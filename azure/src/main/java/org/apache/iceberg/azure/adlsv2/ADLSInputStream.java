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
package org.apache.iceberg.azure.adlsv2;

import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeFileOpenInputStreamResult;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.FileRange;
import com.azure.storage.file.datalake.models.PathProperties;
import com.azure.storage.file.datalake.options.DataLakeFileInputStreamOptions;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ADLSInputStream extends SeekableInputStream implements RangeReadable {
  private static final Logger LOG = LoggerFactory.getLogger(ADLSInputStream.class);

  // max skip size, if skip is larger, then the stream will be reopened at new position
  private static final int SKIP_SIZE = 1024 * 1024;

  // the SDK's input stream downloads in chunks of this size unless told otherwise, one request each
  private static final int SDK_DEFAULT_BLOCK_SIZE = 4 * 1024 * 1024;

  private final StackTraceElement[] createStack;
  private final String location;
  private final DataLakeFileClient fileClient;
  private Long fileSize;
  private final AzureProperties azureProperties;
  // The version of the file this stream reads: its input file's, once that has one, else the one
  // its first request finds, which each later request asks for by ETag, so that every read sees the
  // version the first one did, though the file may be replaced in between
  private final Supplier<DataLakeRequestConditions> fileVersion;
  private final Consumer<PathProperties> observer;
  private DataLakeRequestConditions version;

  private InputStream stream;
  private long pos;
  private long next;
  private boolean closed;

  private final Counter readBytes;
  private final Counter readOperations;

  ADLSInputStream(
      String location,
      DataLakeFileClient fileClient,
      Long fileSize,
      AzureProperties azureProperties,
      MetricsContext metrics) {
    this(location, fileClient, fileSize, azureProperties, () -> null, properties -> {}, metrics);
  }

  /**
   * A stream that reads the version {@code fileVersion} gives, once it gives one, and tells {@code
   * observer} what each of its requests found.
   */
  ADLSInputStream(
      String location,
      DataLakeFileClient fileClient,
      Long fileSize,
      AzureProperties azureProperties,
      Supplier<DataLakeRequestConditions> fileVersion,
      Consumer<PathProperties> observer,
      MetricsContext metrics) {
    this.location = location;
    this.fileClient = fileClient;
    this.fileSize = fileSize;
    this.azureProperties = azureProperties;
    this.fileVersion = fileVersion;
    this.observer = observer;

    this.readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, Unit.BYTES);
    this.readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    this.createStack = Thread.currentThread().getStackTrace();
  }

  private void openStream() {
    DataLakeFileOpenInputStreamResult result = openRange(new FileRange(pos));
    if (fileSize == null) {
      this.fileSize = result.getProperties().getFileSize();
    }
    this.stream = result.getInputStream();
  }

  /** The conditions that select the version this stream reads; null until it has one. */
  private DataLakeRequestConditions version() {
    DataLakeRequestConditions chosen = fileVersion.get();
    return chosen != null ? chosen : version;
  }

  /** Takes the version {@code properties} describe, if the stream has none yet. */
  private void observe(PathProperties properties) {
    if (version == null && properties != null && properties.getETag() != null) {
      this.version = new DataLakeRequestConditions().setIfMatch(properties.getETag());
    }
    observer.accept(properties);
  }

  /** Options for {@code range}, downloading at least {@code minBlockSize} bytes per request. */
  private DataLakeFileInputStreamOptions getInputOptions(FileRange range, int minBlockSize) {
    DataLakeFileInputStreamOptions options = new DataLakeFileInputStreamOptions();
    Integer blockSize = azureProperties.adlsReadBlockSize().orElse(null);
    if (minBlockSize > (blockSize != null ? blockSize : SDK_DEFAULT_BLOCK_SIZE)) {
      blockSize = minBlockSize;
    }
    if (blockSize != null) {
      options.setBlockSize(blockSize);
    }
    options.setRange(range);
    DataLakeRequestConditions conditions = version();
    if (conditions != null) {
      options.setRequestConditions(conditions);
    }
    return options;
  }

  @Override
  public long getPos() {
    return next;
  }

  @Override
  public void seek(long newPos) {
    Preconditions.checkState(!closed, "Cannot seek: already closed");
    Preconditions.checkArgument(newPos >= 0, "Cannot seek: position %s is negative", newPos);

    // this allows a seek beyond the end of the stream but the next read will fail
    this.next = newPos;
  }

  @Override
  public int read() throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();

    int bytesRead = stream.read();
    if (bytesRead == -1) {
      return -1;
    }

    pos += 1;
    next += 1;
    readBytes.increment();
    readOperations.increment();

    return bytesRead;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();

    int bytesRead = stream.read(b, off, len);
    if (bytesRead == -1) {
      return -1;
    }

    pos += bytesRead;
    next += bytesRead;
    readBytes.increment(bytesRead);
    readOperations.increment();

    return bytesRead;
  }

  private void positionStream() throws IOException {
    if ((stream != null) && (next == pos)) {
      // already at specified position
      return;
    }

    if ((stream != null) && (next > pos)) {
      // seeking forwards
      long skip = next - pos;
      if (skip <= Math.max(stream.available(), SKIP_SIZE)) {
        // already buffered or seek is small enough
        try {
          ByteStreams.skipFully(stream, skip);
          this.pos = next;
          return;
        } catch (IOException ignored) {
          // will retry by re-opening the stream
        }
      }
    }

    // close the stream and open at desired position
    this.pos = next;
    openStream();
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    Preconditions.checkPositionIndexes(offset, offset + length, buffer.length);
    if (length == 0) {
      return;
    }

    // a FileRange is an offset and a count; one request of exactly the range
    FileRange range = new FileRange(position, (long) length);

    try (InputStream inputStream = openRange(range, length).getInputStream()) {
      IOUtil.readFully(inputStream, buffer, offset, length);
      readBytes.increment(length);
      readOperations.increment();
    }
  }

  @Override
  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    Preconditions.checkPositionIndexes(offset, offset + length, buffer.length);

    if (this.fileSize == null) {
      PathProperties properties = fileClient.getProperties();
      observe(properties);
      this.fileSize = properties.getFileSize();
    }
    long readStart = Math.max(0, fileSize - length);
    int count = (int) (fileSize - readStart);
    if (count == 0) {
      return 0;
    }

    FileRange range = new FileRange(readStart, (long) count);
    try (InputStream inputStream = openRange(range, count).getInputStream()) {
      int bytesRead = IOUtil.readRemaining(inputStream, buffer, offset, count);
      if (bytesRead > 0) {
        readBytes.increment(bytesRead);
        readOperations.increment();
      }
      return bytesRead;
    }
  }

  private DataLakeFileOpenInputStreamResult openRange(FileRange range) {
    return openRange(range, 0);
  }

  private DataLakeFileOpenInputStreamResult openRange(FileRange range, int minBlockSize) {
    try {
      DataLakeFileOpenInputStreamResult result =
          fileClient.openInputStream(getInputOptions(range, minBlockSize));
      observe(result.getProperties());
      return result;
    } catch (RuntimeException e) {
      throwNotFoundIfNotPresent(e, location);
      LOG.error(
          "Failed to open input stream for file {}, range {}", fileClient.getFilePath(), range, e);
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.closed = true;
    if (stream != null) {
      stream.close();
    }
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

  private static void throwNotFoundIfNotPresent(Throwable throwable, String location) {
    if (isFileNotFoundException(throwable)) {
      throw new NotFoundException(throwable, "Location does not exist: %s", location);
    }
  }

  private static boolean isFileNotFoundException(Throwable exception) {
    if (exception instanceof BlobStorageException blobStorageException) {
      return BlobErrorCode.BLOB_NOT_FOUND.equals(blobStorageException.getErrorCode());
    }
    if (exception instanceof DataLakeStorageException dataLakeStorageException) {
      return "PathNotFound".equals(dataLakeStorageException.getErrorCode());
    }
    return false;
  }
}
