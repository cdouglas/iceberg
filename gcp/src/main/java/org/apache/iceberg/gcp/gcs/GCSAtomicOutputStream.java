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

import com.google.api.core.ApiFuture;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Output stream for atomic writes to GCS using BlobWriteSession.
 *
 * <p>This implementation uses the modern GCS SDK's {@link BlobWriteSession} API which provides
 * clean access to the resulting {@link BlobInfo} (including generation) after write completion via
 * {@link BlobWriteSession#getResult()}.
 *
 * <p>The write supports CRC32C checksum verification for compare-and-swap semantics via {@link
 * BlobWriteOption#crc32cMatch()}.
 */
class GCSAtomicOutputStream extends PositionOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(GCSAtomicOutputStream.class);

  private final StackTraceElement[] createStack;
  private final Storage storage;
  private final BlobId blobId;
  private final GCPProperties gcpProperties;
  private final MetricsContext metrics;
  private final BlobId pinnedSnapshot;
  private final Consumer<InputFile> onClose;

  private OutputStream stream;
  private WritableByteChannel channel;
  private ApiFuture<BlobInfo> resultFuture;

  private final Counter writeBytes;
  private final Counter writeOperations;

  private long pos = 0;
  private boolean closed = false;

  GCSAtomicOutputStream(
      Storage storage,
      BlobId blobId,
      GCPProperties gcpProperties,
      MetricsContext metrics,
      CAS token,
      BlobId pinnedSnapshot,
      Consumer<InputFile> onClose) {
    this.storage = storage;
    this.blobId = blobId;
    this.gcpProperties = gcpProperties;
    this.metrics = metrics;
    this.pinnedSnapshot = pinnedSnapshot;
    this.onClose = onClose;

    createStack = Thread.currentThread().getStackTrace();

    this.writeBytes = metrics.counter(FileIOMetricsContext.WRITE_BYTES, Unit.BYTES);
    this.writeOperations = metrics.counter(FileIOMetricsContext.WRITE_OPERATIONS);

    openStream(token);
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

  private void openStream(CAS token) {
    List<BlobWriteOption> writeOptions = Lists.newArrayList();

    gcpProperties
        .encryptionKey()
        .ifPresent(key -> writeOptions.add(BlobWriteOption.encryptionKey(key)));
    gcpProperties
        .userProject()
        .ifPresent(userProject -> writeOptions.add(BlobWriteOption.userProject(userProject)));

    // Apply CAS precondition from the InputFile snapshot. The BlobId's generation drives the
    // ifGenerationMatch precondition: 0 enforces "object must not exist", positive pins the
    // captured live generation. Null snapshot means no precondition (vanilla overwrite).
    if (pinnedSnapshot != null) {
      writeOptions.add(BlobWriteOption.generationMatch(pinnedSnapshot.getGeneration()));
    }

    BlobInfo.Builder blobInfoBuilder = BlobInfo.newBuilder(blobId);
    if (token != null) {
      // Set expected CRC32C checksum for content integrity verification
      blobInfoBuilder.setCrc32c(token.contentHeaderString());
      writeOptions.add(BlobWriteOption.crc32cMatch());
    }

    try {
      // Use BlobWriteSession for clean access to result BlobInfo with generation
      // The getResult() method returns the BlobInfo from the same write operation,
      // ensuring we get the generation from the actual write, not a separate fetch.
      BlobWriteSession writeSession =
          storage.blobWriteSession(
              blobInfoBuilder.build(), writeOptions.toArray(new BlobWriteOption[0]));

      channel = writeSession.open();
      resultFuture = writeSession.getResult();

      stream = Channels.newOutputStream(channel);
    } catch (StorageException e) {
      handleStorageException(e);
      throw e;
    } catch (IOException e) {
      throw new RuntimeException("Failed to open write session for: " + blobId.toGsUtilUri(), e);
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

      // Get the resulting BlobInfo with generation using the clean BlobWriteSession API
      BlobInfo info = resultFuture.get();
      onClose.accept(
          new GCSInputFile(storage, info.getBlobId(), info.getSize(), gcpProperties, metrics));
    } catch (StorageException e) {
      handleStorageException(e);
      throw e;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof StorageException) {
        handleStorageException((StorageException) cause);
      }
      throw new IOException("Failed to complete atomic write to: " + blobId.toGsUtilUri(), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted during atomic write to: " + blobId.toGsUtilUri(), e);
    }
  }

  private void handleStorageException(StorageException e) {
    int code = e.getCode();
    if (code == 412) {
      // https://cloud.google.com/storage/docs/json_api/v1/status-codes#412_Precondition_Failed
      throw new SupportsAtomicOperations.StorageInvariantException("Target modified", e);
    }
    if (code == 429 || (code >= 500 && code < 600)) {
      // 429 = per-object update rate / quota; 5xx = transient server error.
      // Both are recoverable by retrying the same write after a backoff.
      throw new SupportsAtomicOperations.StorageThrottleException(
          "Storage applied backpressure (HTTP " + code + ")", e);
    }
    // Other codes (auth, malformed request) propagate as the original StorageException.
  }

  @SuppressWarnings({"checkstyle:NoFinalizer", "Finalize"})
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning
      String trace = Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed atomic output stream created by:\n\t{}", trace);
    }
  }
}
