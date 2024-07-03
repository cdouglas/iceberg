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

import com.azure.storage.common.ParallelTransferOptions;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.PathProperties;
import com.azure.storage.file.datalake.options.DataLakeFileOutputStreamOptions;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.function.Consumer;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.io.FileChecksum;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ADLSOutputStream extends PositionOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(ADLSOutputStream.class);

  private final StackTraceElement[] createStack;
  private final DataLakeFileClient fileClient;
  private final AzureProperties azureProperties;
  private final Consumer<InputFile> onClose;

  private OutputStream stream;

  private final MetricsContext metrics;
  private final Counter writeBytes;
  private final Counter writeOperations;

  private long pos;
  private boolean closed;

  ADLSOutputStream(
      DataLakeFileClient fileClient, AzureProperties azureProperties, MetricsContext metrics) {
    this(fileClient, azureProperties, metrics, null, info -> {});
  }

  ADLSOutputStream(
      DataLakeFileClient fileClient,
      AzureProperties azureProperties,
      MetricsContext metrics,
      FileChecksum checksum,
      Consumer<InputFile> onClose) {
    this.fileClient = fileClient;
    this.azureProperties = azureProperties;

    this.createStack = Thread.currentThread().getStackTrace();

    this.metrics = metrics;
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

  private void openStream(FileChecksum checksum) {
    DataLakeFileOutputStreamOptions options = new DataLakeFileOutputStreamOptions();
    if (checksum != null) {
      options.setRequestConditions(
          new DataLakeRequestConditions().setIfMatch(checksum.toHeaderString()));
    }
    ParallelTransferOptions transferOptions = new ParallelTransferOptions();
    azureProperties.adlsWriteBlockSize().ifPresent(transferOptions::setBlockSizeLong);
    this.stream = fileClient.getOutputStream(options);
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    super.close();
    this.closed = true;
    if (stream != null) {
      stream.close();
    }
    // !#! TODO extract from close; find something to return a Result<>
    // This is wrong, but GCP is broken, too?
    PathProperties properties = fileClient.getProperties();
    DataLakeRequestConditions invariant = new DataLakeRequestConditions();
    invariant.setIfMatch(properties.getETag());
    ADLSInputFile etagIF =
        new ADLSInputFile(
            fileClient.getFilePath(),
            properties.getFileSize(),
            fileClient,
            azureProperties,
            metrics,
            invariant);
    onClose.accept(etagIF);
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
