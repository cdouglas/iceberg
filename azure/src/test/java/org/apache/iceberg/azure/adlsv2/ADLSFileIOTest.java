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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ADLSFileIOTest {
  protected static AzuriteContainer azuriteContainer = null;

  private final Random random = new Random();
  private static final Logger LOG = LoggerFactory.getLogger(ADLSFileIOTest.class);
  private static String uniqTestRun = UUID.randomUUID().toString();

  private static Map<String, String> azureProperties = null;
  private static LocationResolver az;

  @BeforeAll
  public static void initStorage() throws IOException {
    uniqTestRun = UUID.randomUUID().toString();
    LOG.info("TEST RUN: " + uniqTestRun);

    // Check for Azure credentials via environment variable
    String credsPath = System.getenv("AZURE_SAS_CREDENTIALS_FILE");
    AzureSAS creds = credsPath != null ? AzureSAS.readCreds(new File(credsPath)) : null;

    if (creds != null) {
      azureProperties = Maps.newHashMap();
      azureProperties.put(
          AzureProperties.ADLS_SAS_TOKEN_PREFIX + creds.account + ".dfs.core.windows.net",
          creds.sasToken);
      az = new AzureSAS.SasResolver(creds);
      LOG.info("Using remote storage: {}", creds.account);
    } else {
      // Use Azurite container for local testing - requires Docker
      try {
        azuriteContainer = new AzuriteContainer();
        azuriteContainer.start();
        az = azuriteContainer;
        LOG.info("Using local Azurite storage");
      } catch (Exception e) {
        LOG.warn("Could not start Azurite container (Docker not available?): {}", e.getMessage());
        // Tests will be skipped via the assumption in baseBefore
      }
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
    // Skip tests if neither Azure credentials nor Azurite is available
    Assumptions.assumeTrue(
        az != null,
        "Azure storage not available - need AZURE_SAS_CREDENTIALS_FILE env var or Docker for Azurite");
    if (azuriteContainer != null) {
      azuriteContainer.createStorageContainer();
    }
  }

  @AfterEach
  public void baseAfter() {
    if (azuriteContainer != null && azuriteContainer.isRunning()) {
      try {
        azuriteContainer.deleteStorageContainer();
      } catch (Exception e) {
        // Ignore cleanup errors
      }
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

    OutputFile outputFile = io.newOutputFile(location);
    try (OutputStream out = outputFile.create()) {
      out.write(123);
    }

    InputFile inputFile = io.newInputFile(location);
    try (InputStream in = inputFile.newStream()) {
      int byteVal = in.read();
      assertThat(byteVal).isEqualTo(123);
    }

    io.deleteFile(location);
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
  public void testAtomicCAS() throws IOException {
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

    // atomic write succeeds
    final AtomicOutputFile atomicOut = io.newOutputFile(in);
    final byte[] newContent = new byte[1024 * 1024];
    random.nextBytes(newContent);
    final CAS chk =
        atomicOut.prepare(
            () -> new ByteArrayInputStream(newContent), AtomicOutputFile.Strategy.CAS);
    InputFile result = atomicOut.writeAtomic(chk, () -> new ByteArrayInputStream(newContent));

    final byte[] actual = new byte[1024 * 1024];
    try (InputStream is = result.newStream()) {
      IOUtil.readFully(is, actual, 0, actual.length);
    }
    assertThat(actual).isEqualTo(newContent);
  }
}
