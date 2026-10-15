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

import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobAppendableUploadConfig.CloseAction;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streams a regular (non-atomic) write to a zonal/Rapid bucket as an appendable upload, finalized
 * on close.
 *
 * <p>Zonal buckets reject the JSON API's resumable and single-request uploads ({@link
 * GCSOutputStream} uses the former) and the XML API's resumable upload; an appendable upload (gRPC
 * {@code BidiWriteObject}) is the only streaming write they accept. (A single XML PUT would need
 * the whole file in memory; {@link GCSXmlPutWriter} uses one for CAS, where the payload is small
 * and already buffered.)
 *
 * <p>The object is visible under its name, unfinalized, while it is written, and a writer that
 * fails part way leaves the bytes it flushed, finalized if {@link #close()} runs. Iceberg reads
 * only files that a successful commit references, under names unique to each attempt, so such a
 * file is an orphan, as a failed write leaves on other stores. {@link #close()} checks that the
 * finalized object holds every byte written.
 */
class GCSAppendableOutputStream extends PositionOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(GCSAppendableOutputStream.class);
  private static final int BUFFER_SIZE = 1 << 20;

  private final StackTraceElement[] createStack;
  private final BlobId blobId;
  private final BlobAppendableUpload upload;
  private final BlobAppendableUpload.AppendableUploadWriteableByteChannel channel;
  // Batches small writes (metadata JSON is written a few bytes at a time) into channel writes.
  private final OutputStream stream;

  private final Counter writeBytes;
  private final Counter writeOperations;

  private long pos = 0;
  private boolean closed = false;

  GCSAppendableOutputStream(
      Storage grpcStorage, BlobId blobId, GCPProperties gcpProperties, MetricsContext metrics)
      throws IOException {
    this.blobId = blobId;
    this.createStack = Thread.currentThread().getStackTrace();
    this.writeBytes = metrics.counter(FileIOMetricsContext.WRITE_BYTES, Unit.BYTES);
    this.writeOperations = metrics.counter(FileIOMetricsContext.WRITE_OPERATIONS);

    List<BlobWriteOption> writeOptions = Lists.newArrayList();
    gcpProperties
        .encryptionKey()
        .ifPresent(key -> writeOptions.add(BlobWriteOption.encryptionKey(key)));
    gcpProperties
        .userProject()
        .ifPresent(userProject -> writeOptions.add(BlobWriteOption.userProject(userProject)));

    this.upload =
        grpcStorage.blobAppendableUpload(
            BlobInfo.newBuilder(blobId).build(),
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING),
            writeOptions.toArray(new BlobWriteOption[0]));
    this.channel = upload.open();
    this.stream = new BufferedOutputStream(new ChannelSink(), BUFFER_SIZE);
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

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    super.close();
    closed = true;
    stream.close(); // drains the buffer, then closes the channel, which finalizes the object

    BlobInfo written;
    try {
      written = upload.getResult().get();
    } catch (ExecutionException e) {
      throw new IOException("Failed to finalize " + blobId.toGsUtilUri(), e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted finalizing " + blobId.toGsUtilUri(), e);
    }
    if (written.getSize() == null || written.getSize() != pos) {
      throw new IOException(
          "Finalized "
              + blobId.toGsUtilUri()
              + " with "
              + written.getSize()
              + " bytes, but "
              + pos
              + " were written");
    }
  }

  /**
   * Writes through to the channel. The channel's {@code write} is non-blocking: it takes what fits
   * in its outbound queue and returns 0 when the queue is full. Rather than spin until it takes
   * more, wait for GCS to acknowledge what is queued.
   */
  private class ChannelSink extends OutputStream {
    @Override
    public void write(int b) throws IOException {
      write(new byte[] {(byte) b}, 0, 1);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      ByteBuffer buf = ByteBuffer.wrap(b, off, len);
      while (buf.hasRemaining()) {
        if (channel.write(buf) == 0) {
          channel.flush();
        }
      }
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }

  @SuppressWarnings({"checkstyle:NoFinalizer", "Finalize", "deprecation"})
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
