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
package org.apache.iceberg.aws.s3;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.net.ssl.SSLException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

class S3InputStream extends SeekableInputStream implements RangeReadable {
  private static final Logger LOG = LoggerFactory.getLogger(S3InputStream.class);

  private static final List<Class<? extends Throwable>> RETRYABLE_EXCEPTIONS =
      ImmutableList.of(SSLException.class, SocketTimeoutException.class, SocketException.class);

  private final StackTraceElement[] createStack;
  private final S3Client s3;
  private final S3URI location;
  private final S3FileIOProperties s3FileIOProperties;
  // The version this stream reads: its input file's, once that has one, else the one its own first
  // request found. Every request asks for it (If-Match), so a write that lands between two of them
  // fails the read instead of splicing another version's bytes into it.
  private final Supplier<String> fileVersion;
  private final Consumer<GetObjectResponse> observer;
  private String version = null;

  private InputStream stream;
  private long pos = 0;
  private long next = 0;
  private boolean closed = false;

  private final Counter readBytes;
  private final Counter readOperations;

  private int skipSize = 1024 * 1024;
  private RetryPolicy<Object> retryPolicy =
      RetryPolicy.builder()
          .handle(RETRYABLE_EXCEPTIONS)
          .onRetry(
              e -> {
                LOG.warn(
                    "Retrying read from S3, reopening stream (attempt {})", e.getAttemptCount());
                resetForRetry();
              })
          .onFailure(
              e ->
                  LOG.error(
                      "Failed to read from S3 input stream after exhausting all retries",
                      e.getException()))
          .withMaxRetries(3)
          .build();

  S3InputStream(S3Client s3, S3URI location) {
    this(s3, location, new S3FileIOProperties(), MetricsContext.nullMetrics());
  }

  S3InputStream(
      S3Client s3, S3URI location, S3FileIOProperties s3FileIOProperties, MetricsContext metrics) {
    this(s3, location, s3FileIOProperties, metrics, () -> null, response -> {});
  }

  /**
   * A stream of the version {@code fileVersion} names, or, while it names none, of the one this
   * stream's first request finds, which {@code observer} hears of.
   */
  S3InputStream(
      S3Client s3,
      S3URI location,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics,
      Supplier<String> fileVersion,
      Consumer<GetObjectResponse> observer) {
    this.s3 = s3;
    this.location = location;
    this.s3FileIOProperties = s3FileIOProperties;
    this.fileVersion = fileVersion;
    this.observer = observer;

    this.readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, Unit.BYTES);
    this.readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    this.createStack = Thread.currentThread().getStackTrace();
  }

  @Override
  public long getPos() {
    return next;
  }

  @Override
  public void seek(long newPos) {
    Preconditions.checkState(!closed, "already closed");
    Preconditions.checkArgument(newPos >= 0, "position is negative: %s", newPos);

    // this allows a seek beyond the end of the stream but the next read will fail
    next = newPos;
  }

  @Override
  public int read() throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();
    try {
      int bytesRead = Failsafe.with(retryPolicy).get(() -> openedStream().read());
      if (bytesRead == -1) {
        return -1;
      }

      pos += 1;
      next += 1;
      readBytes.increment();
      readOperations.increment();

      return bytesRead;
    } catch (FailsafeException ex) {
      if (ex.getCause() instanceof IOException) {
        throw (IOException) ex.getCause();
      }

      throw ex;
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();

    try {
      int bytesRead = Failsafe.with(retryPolicy).get(() -> openedStream().read(b, off, len));
      if (bytesRead == -1) {
        return -1;
      }

      pos += bytesRead;
      next += bytesRead;
      readBytes.increment(bytesRead);
      readOperations.increment();

      return bytesRead;
    } catch (FailsafeException ex) {
      if (ex.getCause() instanceof IOException) {
        throw (IOException) ex.getCause();
      }

      throw ex;
    }
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    Preconditions.checkPositionIndexes(offset, offset + length, buffer.length);

    String range = String.format("bytes=%s-%s", position, position + length - 1);

    try (InputStream rangeStream = readRange(range)) {
      IOUtil.readFully(rangeStream, buffer, offset, length);
      if (length > 0) {
        readBytes.increment(length);
        readOperations.increment();
      }
    }
  }

  @Override
  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    Preconditions.checkPositionIndexes(offset, offset + length, buffer.length);

    String range = String.format("bytes=-%s", length);

    try (InputStream rangeStream = readRange(range)) {
      int bytesRead = IOUtil.readRemaining(rangeStream, buffer, offset, length);
      if (bytesRead > 0) {
        readBytes.increment(bytesRead);
        readOperations.increment();
      }
      return bytesRead;
    }
  }

  private InputStream readRange(String range) {
    GetObjectRequest.Builder requestBuilder =
        GetObjectRequest.builder().bucket(location.bucket()).key(location.key()).range(range);

    S3RequestUtil.configureEncryption(s3FileIOProperties, requestBuilder);

    return get(requestBuilder);
  }

  /** The version requests ask for, or null while neither the file nor this stream has one. */
  private String version() {
    String chosen = fileVersion.get();
    return chosen != null ? chosen : version;
  }

  /**
   * Sends {@code request} for the version this stream reads, and takes the one it found if none.
   */
  private InputStream get(GetObjectRequest.Builder request) {
    String asked = version();
    if (asked != null) {
      request.ifMatch(asked);
    }
    InputStream response;
    try {
      response = s3.getObject(request.build(), ResponseTransformer.toInputStream());
    } catch (S3Exception e) {
      if (asked != null && e.statusCode() == BaseS3File.PRECONDITION_FAILED) {
        throw new SupportsAtomicOperations.StaleReadException(
            String.format("Version %s of %s is no longer the object's", asked, location), e);
      }
      throw e;
    }
    if (response instanceof ResponseInputStream<?> withResponse
        && withResponse.response() instanceof GetObjectResponse found
        && version == null) {
      this.version = asked != null ? asked : found.eTag();
      observer.accept(found);
    }
    return response;
  }

  /** The open stream; a reopen that failed in a retry's listener is tried again here. */
  private InputStream openedStream() throws IOException {
    if (stream == null) {
      openStream();
    }
    return stream;
  }

  @Override
  public void close() throws IOException {
    super.close();
    closed = true;
    closeStream(false);
  }

  private void positionStream() throws IOException {
    if ((stream != null) && (next == pos)) {
      // already at specified position
      return;
    }

    if ((stream != null) && (next > pos)) {
      // seeking forwards
      long skip = next - pos;
      if (skip <= Math.max(stream.available(), skipSize)) {
        // already buffered or seek is small enough
        LOG.debug("Read-through seek for {} to offset {}", location, next);
        try {
          ByteStreams.skipFully(stream, skip);
          pos = next;
          return;
        } catch (IOException ignored) {
          // will retry by re-opening the stream
        }
      }
    }

    // close the stream and open at desired position
    LOG.debug("Seek with new stream for {} to offset {}", location, next);
    pos = next;
    openStream();
  }

  private void openStream() throws IOException {
    openStream(false);
  }

  private void openStream(boolean closeQuietly) throws IOException {
    GetObjectRequest.Builder requestBuilder =
        GetObjectRequest.builder()
            .bucket(location.bucket())
            .key(location.key())
            .range(String.format("bytes=%s-", pos));

    S3RequestUtil.configureEncryption(s3FileIOProperties, requestBuilder);

    closeStream(closeQuietly);

    try {
      stream = get(requestBuilder);
    } catch (NoSuchKeyException e) {
      throw new NotFoundException(e, "Location does not exist: %s", location);
    }
  }

  @VisibleForTesting
  void resetForRetry() throws IOException {
    openStream(true);
  }

  private void closeStream(boolean closeQuietly) throws IOException {
    if (stream != null) {
      // if we aren't at the end of the stream, and the stream is abortable, then
      // call abort() so we don't read the remaining data with the Apache HTTP client
      abortStream();
      try {
        stream.close();
      } catch (IOException e) {
        if (closeQuietly) {
          stream = null;
          LOG.warn("An error occurred while closing the stream", e);
          return;
        }

        // the Apache HTTP client will throw a ConnectionClosedException
        // when closing an aborted stream, which is expected
        if (!e.getClass().getSimpleName().equals("ConnectionClosedException")) {
          throw e;
        }
      }
      stream = null;
    }
  }

  private void abortStream() {
    try {
      if (stream instanceof Abortable && stream.read() != -1) {
        ((Abortable) stream).abort();
      }
    } catch (Exception e) {
      LOG.warn("An error occurred while aborting the stream", e);
    }
  }

  public void setSkipSize(int skipSize) {
    this.skipSize = skipSize;
  }

  @SuppressWarnings({"checkstyle:NoFinalizer", "Finalize", "deprecation"})
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning
      String trace = Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed input stream created by:\n\t{}", trace);
    }
  }
}
