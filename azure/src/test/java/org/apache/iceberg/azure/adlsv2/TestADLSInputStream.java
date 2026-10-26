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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.implementation.models.InternalDataLakeFileOpenInputStreamResult;
import com.azure.storage.file.datalake.models.FileRange;
import com.azure.storage.file.datalake.models.PathProperties;
import com.azure.storage.file.datalake.options.DataLakeFileInputStreamOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.CachingMetricsContext;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TestADLSInputStream {
  private static final int EOF = -1;

  @Mock private DataLakeFileClient fileClient;
  @Mock private InputStream inputStream;

  private ADLSInputStream adlsInputStream;

  @BeforeEach
  void before() {
    InternalDataLakeFileOpenInputStreamResult openInputStreamResult =
        new InternalDataLakeFileOpenInputStreamResult(inputStream, mock());
    lenient().when(fileClient.openInputStream(any())).thenReturn(openInputStreamResult);
    adlsInputStream =
        new ADLSInputStream(
            "abfs://container@account.dfs.core.windows.net/path/to/file",
            fileClient,
            0L,
            mock(),
            MetricsContext.nullMetrics());
  }

  @Test
  void testReadSingle() throws IOException {
    int i0 = 1;
    int i1 = 255;
    byte[] data = {(byte) i0, (byte) i1};
    InputStream byteStream = new ByteArrayInputStream(data);
    InternalDataLakeFileOpenInputStreamResult openInputStreamResult =
        new InternalDataLakeFileOpenInputStreamResult(byteStream, mock());
    when(fileClient.openInputStream(any())).thenReturn(openInputStreamResult);

    try (ADLSInputStream in =
        new ADLSInputStream(
            "abfs://container@account.dfs.core.windows.net/path/to/file",
            fileClient,
            2L,
            mock(),
            MetricsContext.nullMetrics())) {

      assertThat(in.read()).isEqualTo(i0);
      assertThat(in.read()).isEqualTo(i1);
      assertThat(in.read()).isEqualTo(EOF);
    }
  }

  @Test
  void testReadBufferedEOF() throws IOException {
    byte[] data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
    InputStream byteStream = new ByteArrayInputStream(data);
    InternalDataLakeFileOpenInputStreamResult openInputStreamResult =
        new InternalDataLakeFileOpenInputStreamResult(byteStream, mock());
    when(fileClient.openInputStream(any())).thenReturn(openInputStreamResult);

    try (ADLSInputStream in =
        new ADLSInputStream(
            "abfs://container@account.dfs.core.windows.net/path/to/file",
            fileClient,
            8L,
            mock(),
            MetricsContext.nullMetrics())) {

      byte[] actual = new byte[10];
      int bytesRead = in.read(actual, 0, 10);
      assertThat(bytesRead).isEqualTo(8);
      assertThat(Arrays.copyOfRange(actual, 0, bytesRead)).isEqualTo(data);

      assertThat(in.read(actual, 0, 10)).isEqualTo(EOF);
      assertThat(in.getPos()).isEqualTo(8);
    }
  }

  @Test
  void testOpeningSendsNoRequest() throws IOException {
    try (ADLSInputStream in =
        new ADLSInputStream(
            "abfs://container@account.dfs.core.windows.net/path/to/file",
            fileClient,
            8L,
            mock(),
            MetricsContext.nullMetrics())) {
      verify(fileClient, never()).openInputStream(any());
    }
  }

  @Test
  void testFirstReadOpensAtItsPosition() throws IOException {
    ArgumentCaptor<DataLakeFileInputStreamOptions> options =
        ArgumentCaptor.forClass(DataLakeFileInputStreamOptions.class);
    InternalDataLakeFileOpenInputStreamResult opened =
        result(new ByteArrayInputStream(new byte[] {7, 8}), 10);
    when(fileClient.openInputStream(options.capture())).thenReturn(opened);
    try (ADLSInputStream in =
        new ADLSInputStream(
            "abfs://container@account.dfs.core.windows.net/path/to/file",
            fileClient,
            10L,
            mock(),
            MetricsContext.nullMetrics())) {
      in.seek(8);
      assertThat(in.read()).isEqualTo(7);
    }

    assertThat(options.getAllValues()).hasSize(1);
    assertThat(options.getValue().getRange().getOffset()).isEqualTo(8);
  }

  @Test
  void testLaterRequestsAskForTheVersionTheFirstFound() throws IOException {
    ArgumentCaptor<DataLakeFileInputStreamOptions> options =
        ArgumentCaptor.forClass(DataLakeFileInputStreamOptions.class);
    InternalDataLakeFileOpenInputStreamResult first =
        result(new ByteArrayInputStream(new byte[] {1, 2}), 10, "e1");
    InternalDataLakeFileOpenInputStreamResult second =
        result(new ByteArrayInputStream(new byte[] {3}), 10, "e1");
    when(fileClient.openInputStream(options.capture())).thenReturn(first).thenReturn(second);
    try (ADLSInputStream in =
        new ADLSInputStream(
            "abfs://container@account.dfs.core.windows.net/path/to/file",
            fileClient,
            null,
            mock(),
            MetricsContext.nullMetrics())) {
      assertThat(in.read()).isEqualTo(1);
      in.readFully(9, new byte[1]);
    }

    // the first request asks for no version; the second, for the one the first found
    assertThat(options.getAllValues()).hasSize(2);
    assertThat(options.getAllValues().get(0).getRequestConditions()).isNull();
    assertThat(options.getAllValues().get(1).getRequestConditions().getIfMatch()).isEqualTo("e1");
  }

  @Test
  void testAFilesFirstReadChoosesItsVersionWithoutALookup() throws IOException {
    InternalDataLakeFileOpenInputStreamResult opened =
        result(new ByteArrayInputStream(new byte[] {1, 2}), 2, "e1");
    when(fileClient.openInputStream(any())).thenReturn(opened);
    ADLSInputFile file =
        new ADLSInputFile(
            "abfs://container@account.dfs.core.windows.net/path/to/file",
            fileClient,
            mock(),
            MetricsContext.nullMetrics());

    try (SeekableInputStream in = file.newStream()) {
      verify(fileClient, never()).openInputStream(any());
      assertThat(in.read()).isEqualTo(1);
    }

    // a replace of the file expects the version it read, and its length is that version's
    assertThat(file.conditions().getIfMatch()).isEqualTo("e1");
    assertThat(file.getLength()).isEqualTo(2);
    verify(fileClient, never()).getProperties();
  }

  @Test
  void testAFilesLengthChoosesTheVersionItsReadsAskFor() throws IOException {
    PathProperties properties = mock(PathProperties.class);
    when(properties.getETag()).thenReturn("e0");
    when(properties.getFileSize()).thenReturn(2L);
    when(fileClient.getProperties()).thenReturn(properties);
    ArgumentCaptor<DataLakeFileInputStreamOptions> options =
        ArgumentCaptor.forClass(DataLakeFileInputStreamOptions.class);
    InternalDataLakeFileOpenInputStreamResult opened =
        result(new ByteArrayInputStream(new byte[] {1, 2}), 2, "e0");
    when(fileClient.openInputStream(options.capture())).thenReturn(opened);
    ADLSInputFile file =
        new ADLSInputFile(
            "abfs://container@account.dfs.core.windows.net/path/to/file",
            fileClient,
            mock(),
            MetricsContext.nullMetrics());

    // as AtomicFileLog reads: a stream, the length, then the bytes
    try (SeekableInputStream in = file.newStream()) {
      assertThat(file.getLength()).isEqualTo(2);
      assertThat(in.read()).isEqualTo(1);
    }

    // the read asks for the version the length came from, so a file replaced in between fails it
    assertThat(options.getValue().getRequestConditions().getIfMatch()).isEqualTo("e0");
    assertThat(file.conditions().getIfMatch()).isEqualTo("e0");
  }

  private static InternalDataLakeFileOpenInputStreamResult result(
      InputStream in, long fileSize, String eTag) {
    PathProperties properties = mock(PathProperties.class);
    lenient().when(properties.getFileSize()).thenReturn(fileSize);
    lenient().when(properties.getETag()).thenReturn(eTag);
    return new InternalDataLakeFileOpenInputStreamResult(in, properties);
  }

  private static InternalDataLakeFileOpenInputStreamResult result(InputStream in, long fileSize) {
    PathProperties properties = mock(PathProperties.class);
    lenient().when(properties.getFileSize()).thenReturn(fileSize);
    return new InternalDataLakeFileOpenInputStreamResult(in, properties);
  }

  @Test
  void testReadFullyClosesTheStream() throws IOException {
    InputStream data = spy(new ByteArrayInputStream(new byte[4]));
    InternalDataLakeFileOpenInputStreamResult opened = result(data, 8);
    when(fileClient.openInputStream(any())).thenReturn(opened);
    adlsInputStream.readFully(0, new byte[4]);

    verify(data).close();
  }

  @Test
  void testReadTailClosesTheStream() throws IOException {
    InputStream data = spy(new ByteArrayInputStream(new byte[4]));
    InternalDataLakeFileOpenInputStreamResult opened = result(data, 8);
    when(fileClient.openInputStream(any())).thenReturn(opened);
    try (ADLSInputStream in =
        new ADLSInputStream(
            "abfs://container@account.dfs.core.windows.net/path/to/file",
            fileClient,
            8L,
            mock(),
            MetricsContext.nullMetrics())) {
      in.readTail(new byte[4], 0, 4);

      verify(data).close();
    }
  }

  @Test
  void testReadFullyRequestsExactlyItsRange() throws IOException {
    ArgumentCaptor<DataLakeFileInputStreamOptions> options =
        ArgumentCaptor.forClass(DataLakeFileInputStreamOptions.class);
    InternalDataLakeFileOpenInputStreamResult opened =
        result(new ByteArrayInputStream(new byte[64]), 10 << 20);
    when(fileClient.openInputStream(options.capture())).thenReturn(opened);
    adlsInputStream.readFully(6 << 20, new byte[64], 0, 64);

    // a FileRange is an offset and a count, not an offset and an end
    FileRange range = options.getValue().getRange();
    assertThat(range.getOffset()).isEqualTo(6 << 20);
    assertThat(range.getCount()).isEqualTo(64);
  }

  @Test
  void testReadFullyLargerThanAChunkIsOneRequest() throws IOException {
    int length = 5 << 20; // above the SDK's 4 MiB chunks, each its own request
    ArgumentCaptor<DataLakeFileInputStreamOptions> options =
        ArgumentCaptor.forClass(DataLakeFileInputStreamOptions.class);
    InternalDataLakeFileOpenInputStreamResult opened =
        result(new ByteArrayInputStream(new byte[length]), 8 << 20);
    when(fileClient.openInputStream(options.capture())).thenReturn(opened);
    adlsInputStream.readFully(1 << 20, new byte[length], 0, length);

    assertThat(options.getValue().getRange().getCount()).isEqualTo(length);
    assertThat(options.getValue().getBlockSize()).isGreaterThanOrEqualTo(length);
  }

  @Test
  void testReadTailRequestsExactlyTheTail() throws IOException {
    ArgumentCaptor<DataLakeFileInputStreamOptions> options =
        ArgumentCaptor.forClass(DataLakeFileInputStreamOptions.class);
    InternalDataLakeFileOpenInputStreamResult opened =
        result(new ByteArrayInputStream(new byte[16]), 100);
    when(fileClient.openInputStream(options.capture())).thenReturn(opened);
    try (ADLSInputStream in =
        new ADLSInputStream(
            "abfs://container@account.dfs.core.windows.net/path/to/file",
            fileClient,
            100L,
            mock(),
            MetricsContext.nullMetrics())) {
      assertThat(in.readTail(new byte[16], 0, 16)).isEqualTo(16);
    }

    FileRange range = options.getValue().getRange();
    assertThat(range.getOffset()).isEqualTo(84);
    assertThat(range.getCount()).isEqualTo(16);
  }

  @Test
  void testReadFullyTracksMetrics() throws IOException {
    byte[] data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
    InputStream byteStream = new ByteArrayInputStream(data);
    InternalDataLakeFileOpenInputStreamResult openInputStreamResult =
        new InternalDataLakeFileOpenInputStreamResult(byteStream, mock());
    when(fileClient.openInputStream(any())).thenReturn(openInputStreamResult);

    CachingMetricsContext metrics = new CachingMetricsContext();
    Counter readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    Counter readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    try (ADLSInputStream in =
        new ADLSInputStream(
            "abfs://container@account.dfs.core.windows.net/path/to/file",
            fileClient,
            (long) data.length,
            mock(),
            metrics)) {
      in.readFully(0, new byte[data.length], 0, data.length);

      assertThat(readBytes.value()).isEqualTo(data.length);
      assertThat(readOperations.value()).isEqualTo(1);
    }
  }

  @Test
  void testReadTailTracksMetrics() throws IOException {
    byte[] data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
    InputStream byteStream = new ByteArrayInputStream(data);
    PathProperties properties = mock(PathProperties.class);
    lenient().when(properties.getFileSize()).thenReturn((long) data.length);
    InternalDataLakeFileOpenInputStreamResult openInputStreamResult =
        new InternalDataLakeFileOpenInputStreamResult(byteStream, properties);
    when(fileClient.openInputStream(any())).thenReturn(openInputStreamResult);

    CachingMetricsContext metrics = new CachingMetricsContext();
    Counter readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    Counter readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    try (ADLSInputStream in =
        new ADLSInputStream(
            "abfs://container@account.dfs.core.windows.net/path/to/file",
            fileClient,
            (long) data.length,
            mock(),
            metrics)) {
      int tailLength = 4;
      int bytesRead = in.readTail(new byte[tailLength], 0, tailLength);

      assertThat(bytesRead).isEqualTo(tailLength);
      assertThat(readBytes.value()).isEqualTo(tailLength);
      assertThat(readOperations.value()).isEqualTo(1);
    }
  }

  @Test
  void testReadTailEmptyObjectDoesNotCountMetrics() throws IOException {
    InputStream byteStream = new ByteArrayInputStream(new byte[0]);
    PathProperties properties = mock(PathProperties.class);
    lenient().when(properties.getFileSize()).thenReturn(0L);
    InternalDataLakeFileOpenInputStreamResult openInputStreamResult =
        new InternalDataLakeFileOpenInputStreamResult(byteStream, properties);
    // a tail of an empty object needs no request
    lenient().when(fileClient.openInputStream(any())).thenReturn(openInputStreamResult);

    CachingMetricsContext metrics = new CachingMetricsContext();
    Counter readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    Counter readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    try (ADLSInputStream in =
        new ADLSInputStream(
            "abfs://container@account.dfs.core.windows.net/path/to/file",
            fileClient,
            0L,
            mock(),
            metrics)) {
      int bytesRead = in.readTail(new byte[8], 0, 8);

      // an empty object yields no bytes; a no-data read counts neither bytes nor an operation
      assertThat(bytesRead).isEqualTo(0);
      assertThat(readBytes.value()).isEqualTo(0);
      assertThat(readOperations.value()).isEqualTo(0);
    }
  }
}
