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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.exception.UnexpectedLengthException;
import com.azure.core.http.HttpHeader;
import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.PathHttpHeaders;
import com.azure.storage.file.datalake.models.PathInfo;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.options.DataLakeFileAppendOptions;
import com.azure.storage.file.datalake.options.DataLakeFileFlushOptions;
import com.azure.storage.file.datalake.options.FileParallelUploadOptions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.FileChecksum;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ADLSFileIOTest {
  protected static AzuriteContainer azuriteContainer = null;

  private final Random random = new Random(1);
  private static final Logger LOG = LoggerFactory.getLogger(ADLSFileIOTest.class);
  private static String uniqTestRun = UUID.randomUUID().toString();

  private static Map<String, String> azureProperties = null;
  private static LocationResolver az;

  @BeforeAll
  public static void initStorage() throws IOException {
    uniqTestRun = UUID.randomUUID().toString();
    LOG.info("TEST RUN: " + uniqTestRun);
    AzureSAS creds =
        AzureSAS.readCreds(new File("/home/chris/work/.cloud/azure/lstnsgym-20250228.json"));
    if (creds != null) {
      azureProperties = Maps.newHashMap();
      azureProperties.put(
          AzureProperties.ADLS_SAS_TOKEN_PREFIX + "lstnsgym.dfs.core.windows.net", creds.sasToken);
      // sascfg.put(AzureProperties.ADLS_CONNECTION_STRING_PREFIX + "lstnsgym.dfs.core.windows.net",
      // creds.connectionString);
      az = new AzureSAS.SasResolver(creds);
    } else {
      azuriteContainer = new AzuriteContainer();
      azuriteContainer.start();
      az = azuriteContainer;
    }
  }

  @AfterAll
  public static void afterAll() {
    if (azuriteContainer != null) {
      azuriteContainer.stop();
    }
  }

  @BeforeEach
  public void baseBefore() {
    if (azuriteContainer != null) {
      azuriteContainer.createStorageContainer();
    }
  }

  @AfterEach
  public void baseAfter() {
    if (azuriteContainer != null) {
      azuriteContainer.deleteStorageContainer();
    }
  }

  protected ADLSFileIO createFileIO() {
    if (null == azureProperties) {
      AzureProperties azureProps = spy(new AzureProperties());

      doAnswer(
              invoke -> {
                DataLakeFileSystemClientBuilder clientBuilder = invoke.getArgument(1);
                clientBuilder.endpoint(azuriteContainer.endpoint());
                clientBuilder.credential(azuriteContainer.credential());
                return null;
              })
          .when(azureProps)
          .applyClientConfiguration(any(), any());

      return new ADLSFileIO(azureProps);
    }
    return new ADLSFileIO(azureProperties);
  }

  @Test
  public void testFileOperations() throws IOException {
    String path = "path/to/file";
    String location = az.location(path);
    ADLSFileIO io = createFileIO();
    DataLakeFileClient fileClient = az.fileClient(path);

    assertThat(fileClient.exists()).isFalse();
    OutputFile outputFile = io.newOutputFile(location);
    try (OutputStream out = outputFile.create()) {
      out.write(123);
    }
    assertThat(fileClient.exists()).isTrue();

    InputFile inputFile = io.newInputFile(location);
    try (InputStream in = inputFile.newStream()) {
      int byteVal = in.read();
      assertThat(byteVal).isEqualTo(123);
    }

    io.deleteFile(location);
    assertThat(fileClient.exists()).isFalse();
  }

  @Test
  public void newOutputFileMatch() throws IOException {
    final String path = "path/to/file.txt";
    final String location = az.location(path);
    final byte[] expected = new byte[1024 * 1024];
    random.nextBytes(expected);
    ADLSFileIO io = createFileIO();

    // create random blob
    final OutputFile out = io.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtil.writeFully(os, ByteBuffer.wrap(expected));
    }

    // ensure it matches
    final InputFile in = io.newInputFile(location);
    assertThat(in.exists()).isTrue();
    final byte[] actual = new byte[1024 * 1024];

    try (InputStream is = in.newStream()) {
      IOUtil.readFully(is, actual, 0, actual.length);
    }
    assertThat(actual).isEqualTo(expected);

    // overwrite it
    OutputFile overwrite = io.newOutputFile(in);
    final byte[] overbytes = new byte[1024 * 1024];
    random.nextBytes(overbytes);
    try (OutputStream os = overwrite.createOrOverwrite()) {
      IOUtil.writeFully(os, ByteBuffer.wrap(overbytes));
    }
    // fail precondition; contents of InputFile changed
    BlobStorageException etagFailure =
        Assertions.assertThrows(
            BlobStorageException.class,
            () -> {
              try (InputStream is = in.newStream()) {
                IOUtil.readFully(is, actual, 0, actual.length);
              }
            });
    // precondition not met
    assertThat(etagFailure.getErrorCode()).isEqualTo(BlobErrorCode.CONDITION_NOT_MET);

    // newly-resolved InputFile should succeed
    try (InputStream is = io.newInputFile(location).newStream()) {
      IOUtil.readFully(is, actual, 0, actual.length);
    }
    assertThat(actual).isEqualTo(overbytes);
  }

  @Test
  public void newOutputFileMatchFail() throws IOException {
    final String path = "path/to/file.txt";
    final String location = az.location(path);
    final byte[] expected = new byte[1024 * 1024];
    random.nextBytes(expected);
    ADLSFileIO io = createFileIO();

    final OutputFile out = io.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtil.writeFully(os, ByteBuffer.wrap(expected));
    }

    final InputFile in = io.newInputFile(location);
    assertThat(in.exists()).isTrue();
    final byte[] actual = new byte[1024 * 1024];
    try (InputStream is = in.newStream()) {
      IOUtil.readFully(is, actual, 0, actual.length);
    }
    assertThat(actual).isEqualTo(expected);

    // overwrite succeeds, because generation matches InputFile
    final OutputFile overwrite = io.newOutputFile(in);
    final byte[] overbytes = new byte[1024 * 1024];
    random.nextBytes(overbytes);
    try (OutputStream os = overwrite.createOrOverwrite()) {
      IOUtil.writeFully(os, ByteBuffer.wrap(overbytes));
    }
    // overwrite fails, object has been overwritten
    BlobStorageException etagFailure =
        Assertions.assertThrows(
            BlobStorageException.class,
            () -> {
              try (InputStream is = in.newStream()) {
                IOUtil.readFully(is, actual, 0, actual.length);
              }
            });
    // precondition not met
    assertThat(etagFailure.getErrorCode()).isEqualTo(BlobErrorCode.CONDITION_NOT_MET);
  }

  @Test
  public void testAtomicPartialWrite() throws IOException {
    final String path = "path/to/file.txt";
    final String location = az.location(path);
    final byte[] expected = new byte[1024 * 1024];
    random.nextBytes(expected);
    ADLSFileIO io = createFileIO();

    final OutputFile out = io.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtil.writeFully(os, ByteBuffer.wrap(expected));
    }

    final InputFile in = io.newInputFile(location);
    assertThat(in.exists()).isTrue();

    // overwrite fails, checksum does not match
    final AtomicOutputFile<CAS> overwrite = io.newOutputFile(in);
    final byte[] overbytes = new byte[1024 * 1024];
    random.nextBytes(overbytes);
    final CAS chk =
        overwrite.prepare(() -> new ByteArrayInputStream(overbytes), AtomicOutputFile.Strategy.CAS);
    // precondition not met (bad checksum)
    UnexpectedLengthException chkFailure =
        Assertions.assertThrows(
            UnexpectedLengthException.class,
            () -> {
              // partial write
              overwrite.writeAtomic(chk, () -> new ByteArrayInputStream(overbytes, 0, 512 * 1024));
            });
    assertThat(chkFailure.getMessage())
        .isEqualTo(
            String.format(
                "Request body emitted %d bytes, less than the expected %d bytes.",
                512 * 1024, 1024 * 1024));
  }

  private byte[] randBytes(int len) {
    final byte[] bytes = new byte[len];
    random.nextBytes(bytes);
    return bytes;
  }

  private static Response<PathInfo> writeBytes(DataLakeFileClient client, byte[] bytes, DataLakeRequestConditions cond) {
    final FileChecksum chk = new ADLSChecksum(AtomicOutputFile.Strategy.CAS);
    chk.update(bytes, 0, bytes.length);
    final Response<PathInfo> resp =
            client.uploadWithResponse(
                    new FileParallelUploadOptions(new ByteArrayInputStream(bytes), bytes.length)
                        .setRequestConditions(cond)
                        .setHeaders(new PathHttpHeaders()
                            .setContentMd5(chk.contentChecksumBytes())
                            .setContentType("binary")),
                    null, // no timeout
                    Context.NONE);
    return resp;
  }

  private static Response<PathInfo> appendBytes(DataLakeFileClient client, long origLen, byte[] bytes, DataLakeRequestConditions cond) {
    final FileChecksum chk = new ADLSChecksum(AtomicOutputFile.Strategy.CAS);
    chk.update(bytes, 0, bytes.length);
    client.appendWithResponse(
            new ByteArrayInputStream(bytes),
            origLen, // client.getProperties().getFileSize(),
            bytes.length,
            chk.contentChecksumBytes(),
            null,
            null,
            Context.NONE);
    final DataLakeFileFlushOptions flushOpts = new DataLakeFileFlushOptions()
            .setClose(true)
            .setRequestConditions(cond)
            .setUncommittedDataRetained(false);
    return client.flushWithResponse(origLen + bytes.length,
            flushOpts, null, Context.NONE);
  }

  @Test
  public void scratchADLS() {
    // ADLSFileIO io = createFileIO();
    // ADLSLocation loc = new ADLSLocation(AZURITE_CONTAINER.location("path/to/file.txt"));
    // DataLakeFileClient client = io.client(loc).getFileClient(loc.path());
    AzureSAS tok =
        AzureSAS.readCreds(new File("/home/chris/work/.cloud/azure/lstnsgym-20250228.json"));
    DataLakeServiceClient serviceClient =
        new DataLakeServiceClientBuilder()
            .endpoint(tok.endpoint)
            .sasToken(tok.sasToken)
            .buildClient();
    DataLakeFileSystemClient fsClient = serviceClient.getFileSystemClient("lst-ns-consistency");
    DataLakeDirectoryClient dirClient = fsClient.getDirectoryClient("wtf");
    final String objId = UUID.randomUUID().toString();
    DataLakeFileClient client = dirClient.getFileClient(objId);

    final byte[] originalBytes = randBytes(1024 + random.nextInt(1024));
    final Response<PathInfo> origResp = writeBytes(client, originalBytes, new DataLakeRequestConditions());
    System.out.println("DEBUG0: " + client.getProperties().getFileSize() + " + " + origResp.getValue().getETag());

    final byte[] overBytes = randBytes(1024 + random.nextInt(1024));
    final Response<PathInfo> overResp = writeBytes(client, overBytes, new DataLakeRequestConditions().setIfMatch(origResp.getValue().getETag()));
    System.out.println("DEBUG1: " + client.getProperties().getFileSize() + " + " + overResp.getValue().getETag());

    final byte[] appendBytes = randBytes(1024 + random.nextInt(1024));
    final Response<PathInfo> appendResp = appendBytes(client, overBytes.length, appendBytes, new DataLakeRequestConditions().setIfMatch(overResp.getValue().getETag()));
    System.out.println("DEBUG2: " + client.getProperties().getFileSize() + " + " + appendResp.getValue().getETag());

    final byte[] appendBytes2 = randBytes(1024 + random.nextInt(1024));
    // fail, off by 1
    final Response<PathInfo> appendResp2;
    try {
      // final Response<PathInfo> appendResp2 = appendBytes(client, overBytes.length + appendBytes.length - 1, appendBytes2, new DataLakeRequestConditions().setIfMatch(appendResp.getValue().getETag()));
      appendResp2 = appendBytes(client, overBytes.length + appendBytes.length - 1, appendBytes2, new DataLakeRequestConditions().setIfMatch(appendResp.getValue().getETag()));
      System.out.println("DEBUG3: " + client.getProperties().getFileSize() + " + " + appendResp2.getValue().getETag());
    } catch (DataLakeStorageException e) {
      assertThat(e.getResponse().getHeaderValue("x-ms-error-code")).isEqualTo("x-ms-error-code:InvalidFlushPosition");
    }

    // final byte[] overBytes2 = randBytes(1024 + random.nextInt(1024));
    // final Response<PathInfo> overResp2 = writeBytes(client, overBytes2, new DataLakeRequestConditions().setIfMatch(appendResp2.getValue().getETag()));
    // System.out.println("DEBUG4: " + client.getProperties().getFileSize() + " + " + overResp2.getValue().getETag());

    // final byte[] failBytes = randBytes(1024 + random.nextInt(1024));
    // final Response<PathInfo> failResp = writeBytes(client, failBytes, new DataLakeRequestConditions().setIfMatch(origResp.getValue().getETag()));
    // System.out.println("DEBUG3: " + client.getProperties().getFileSize() + " + " + failResp.getValue().getETag());
  }

  @Test
  public void testBulkDeleteFiles() {
    String path1 = "path/to/file1";
    String location1 = az.location(path1);
    az.createFile(path1, new byte[] {123});
    assertThat(az.fileClient(path1).exists()).isTrue();

    String path2 = "path/to/file2";
    String location2 = az.location(path2);
    az.createFile(path2, new byte[] {123});
    assertThat(az.fileClient(path2).exists()).isTrue();

    ADLSFileIO io = createFileIO();
    io.deleteFiles(ImmutableList.of(location1, location2));

    assertThat(az.fileClient(path1).exists()).isFalse();
    assertThat(az.fileClient(path2).exists()).isFalse();
  }

  @Test
  public void testGetClient() {
    String location = az.location("path/to/file");
    ADLSFileIO io = createFileIO();
    DataLakeFileSystemClient client = io.client(location);
    assertThat(client.exists()).isTrue();
  }

  /** Azurite does not support ADLSv2 directory operations yet so use mocks here. */
  @SuppressWarnings("unchecked")
  @Test
  public void testListPrefixOperations() {
    String prefix = "abfs://container@account.dfs.core.windows.net/dir";

    OffsetDateTime now = OffsetDateTime.now();
    PathItem dir =
        new PathItem("tag", now, 0L, "group", true, "dir", "owner", "permissions", now, null);
    PathItem file =
        new PathItem(
            "tag", now, 123L, "group", false, "dir/file", "owner", "permissions", now, null);

    PagedIterable<PathItem> response = mock(PagedIterable.class);
    when(response.stream()).thenReturn(ImmutableList.of(dir, file).stream());

    DataLakeFileSystemClient client = mock(DataLakeFileSystemClient.class);
    when(client.listPaths(any(), any())).thenReturn(response);

    ADLSFileIO io = spy(new ADLSFileIO());
    io.initialize(ImmutableMap.of());
    doReturn(client).when(io).client(any(ADLSLocation.class));

    Iterator<FileInfo> result = io.listPrefix(prefix).iterator();

    verify(client).listPaths(any(), any());

    // assert that only files were returned and not directories
    FileInfo fileInfo = result.next();
    assertThat(fileInfo.location()).isEqualTo("dir/file");
    assertThat(fileInfo.size()).isEqualTo(123L);
    assertThat(fileInfo.createdAtMillis()).isEqualTo(now.toInstant().toEpochMilli());

    assertThat(result.hasNext()).isFalse();
  }

  /** Azurite does not support ADLSv2 directory operations yet so use mocks here. */
  @SuppressWarnings("unchecked")
  @Test
  public void testDeletePrefixOperations() {
    String prefix = "abfs://container@account.dfs.core.windows.net/dir";

    Response<Void> response = mock(Response.class);

    DataLakeFileSystemClient client = mock(DataLakeFileSystemClient.class);
    when(client.deleteDirectoryWithResponse(any(), anyBoolean(), any(), any(), any()))
        .thenReturn(response);

    ADLSFileIO io = spy(new ADLSFileIO());
    io.initialize(ImmutableMap.of());
    doReturn(client).when(io).client(any(ADLSLocation.class));

    io.deletePrefix(prefix);

    // assert that recursive delete was called for the directory
    verify(client).deleteDirectoryWithResponse(eq("dir"), eq(true), any(), any(), any());
  }

  @Test
  public void testKryoSerialization() throws IOException {
    FileIO testFileIO = new ADLSFileIO();

    // gcs fileIO should be serializable when properties are passed as immutable map
    testFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.KryoHelpers.roundTripSerialize(testFileIO);

    assertThat(testFileIO.properties()).isEqualTo(roundTripSerializedFileIO.properties());
  }

  @Test
  public void testJavaSerialization() throws IOException, ClassNotFoundException {
    FileIO testFileIO = new ADLSFileIO();

    // gcs fileIO should be serializable when properties are passed as immutable map
    testFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.roundTripSerialize(testFileIO);

    assertThat(testFileIO.properties()).isEqualTo(roundTripSerializedFileIO.properties());
  }
}
