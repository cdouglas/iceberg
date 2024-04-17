/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.gcp.gcs;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.FFS;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.zip.Checksum;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The GCSOutputStream leverages native streaming channels from the GCS API for streaming uploads.
 * See <a href="https://cloud.google.com/storage/docs/streaming">Streaming Transfers</a>
 */
class GCSOutputStream extends PositionOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(GCSOutputStream.class);

  private final StackTraceElement[] createStack;
  private final Storage storage;
  private final BlobId blobId;
  private final GCPProperties gcpProperties;
  private final MetricsContext metrics;
  private final Consumer<InputFile> onClose;

  private OutputStream stream;
  private WriteChannel channel;

  private final Counter writeBytes;
  private final Counter writeOperations;

  private long pos = 0;
  private boolean closed = false;

  GCSOutputStream(
      Storage storage, BlobId blobId, GCPProperties gcpProperties, MetricsContext metrics)
      throws IOException {
    this(storage, blobId, gcpProperties, metrics, null, blobInfo -> {});
  }

  GCSOutputStream(
      Storage storage,
      BlobId blobId,
      GCPProperties gcpProperties,
      MetricsContext metrics,
      Checksum checksum,
      Consumer<InputFile> onClose) {
    this.storage = storage;
    this.blobId = blobId;
    this.gcpProperties = gcpProperties;

    createStack = Thread.currentThread().getStackTrace();

    this.metrics = metrics; // XXX hack, save for InputFile extraction
    this.writeBytes = metrics.counter(FileIOMetricsContext.WRITE_BYTES, Unit.BYTES);
    this.writeOperations = metrics.counter(FileIOMetricsContext.WRITE_OPERATIONS);

    this.onClose = onClose;
    openStream(checksum);
  }

  @Override
  public long getPos() {
    return pos;
  }

  @Override
  public void flush() throws IOException {
    stream.flush();
  }

  @Override
  public void write(int b) throws IOException {
    stream.write(b);
    pos += 1;
    writeBytes.increment();
    writeOperations.increment();
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    stream.write(b, off, len);
    pos += len;
    writeBytes.increment(len);
    writeOperations.increment();
  }

  private void openStream(Checksum checksum) {
    List<BlobWriteOption> writeOptions = Lists.newArrayList();

    gcpProperties
        .encryptionKey()
        .ifPresent(key -> writeOptions.add(BlobWriteOption.encryptionKey(key)));
    gcpProperties
        .userProject()
        .ifPresent(userProject -> writeOptions.add(BlobWriteOption.userProject(userProject)));
    if (blobId.getGeneration() != null) {
      writeOptions.add(BlobWriteOption.generationMatch());
    }
    BlobInfo.Builder blobInfoBuilder = BlobInfo.newBuilder(blobId);
    if (checksum != null) {
      final String checksumB64 =
          Base64.getEncoder().encodeToString(Ints.toByteArray((int) checksum.getValue()));
      blobInfoBuilder.setCrc32c(checksumB64);
      writeOptions.add(BlobWriteOption.crc32cMatch());
    }

    try {
      channel =
          storage.writer(blobInfoBuilder.build(), writeOptions.toArray(new BlobWriteOption[0]));

      gcpProperties.channelWriteChunkSize().ifPresent(channel::setChunkSize);

      stream = Channels.newOutputStream(channel);
    } catch (StorageException e) {
      if (e.getCode() == 412) { // precondition failed
        // https://cloud.google.com/storage/docs/json_api/v1/status-codes#412_Precondition_Failed
        // note: LocalStorageHelper throws 404,
        throw new SupportsAtomicOperations.CASException("File was modified during write", e);
      }
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    try {
      super.close();
      closed = true;
      stream.close();
      if (closed) {
        // XXX hack. We need to extract the BlobInfo from the closed channel
        try {
          // TODO verify; that info correctly includes the generation in the BlobId
          final BlobInfo info = FFS.extractFile(channel);
          onClose.accept(
              GCSInputFile.fromBlobId(info.getBlobId(), storage, gcpProperties, metrics));
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
          throw new RuntimeException("Failed to extract BlobInfo from closed stream");
        }
      }
    } catch (StorageException e) {
      if (e.getCode() == 412) { // precondition failed
        // https://cloud.google.com/storage/docs/json_api/v1/status-codes#412_Precondition_Failed
        // note: LocalStorageHelper throws 404,
        throw new SupportsAtomicOperations.CASException("File was modified during write", e);
      }
      throw e;
    }
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning
      String trace = Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed output stream created by:\n\t{}", trace);
    }
  }
}
