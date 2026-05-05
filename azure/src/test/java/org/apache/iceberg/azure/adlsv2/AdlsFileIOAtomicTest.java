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

import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.io.SupportsAtomicOperationsContractTest;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** ADLS implementation of the {@link SupportsAtomicOperationsContractTest}. APPEND is supported. */
public class AdlsFileIOAtomicTest extends SupportsAtomicOperationsContractTest {

  private static String runId;
  private static Map<String, String> properties;
  private static AzureSAS.SasResolver resolver;

  @BeforeAll
  static void initStorage() {
    runId = UUID.randomUUID().toString();
    String credsPath = System.getenv("AZURE_SAS_CREDENTIALS_FILE");
    AzureSAS creds = credsPath != null ? AzureSAS.readCreds(new File(credsPath)) : null;
    if (creds != null) {
      properties = Maps.newHashMap();
      properties.put(
          AzureProperties.ADLS_SAS_TOKEN_PREFIX + creds.account + ".dfs.core.windows.net",
          creds.sasToken);
      resolver = new AzureSAS.SasResolver(creds);
    }
  }

  @BeforeEach
  void requireCredentials() {
    Assumptions.assumeTrue(
        properties != null, "ADLS not available - set AZURE_SAS_CREDENTIALS_FILE");
  }

  @Override
  protected boolean supportsAppend() {
    return true;
  }

  @Override
  protected SupportsAtomicOperations newFileIO() {
    return new ADLSFileIO(properties);
  }

  @Override
  protected String randomLocation(String slug) {
    return resolver.location(runId + "/" + slug + "-" + UUID.randomUUID());
  }

  // ─── ADLS-specific behavior tests (read-side ifMatch on InputFile snapshots) ────────────────

  /**
   * Sanity check the basic non-atomic write/read/delete path through {@link ADLSFileIO}. The atomic
   * path is exercised by the inherited contract tests; this guards against regressions in the
   * plumbing that doesn't depend on {@link org.apache.iceberg.io.AtomicOutputFile.Strategy}.
   */
  @Test
  void basicFileOperations() throws IOException {
    String location = randomLocation("basic");
    SupportsAtomicOperations io = newFileIO();

    OutputFile outputFile = ((ADLSFileIO) io).newOutputFile(location);
    try (OutputStream out = outputFile.create()) {
      out.write(123);
    }

    InputFile inputFile = io.newInputFile(location);
    try (InputStream in = inputFile.newStream()) {
      assertThat(in.read()).isEqualTo(123);
    }

    io.deleteFile(location);
  }

  /**
   * ADLS-specific: an {@link InputFile} snapshot pins the etag it observed; subsequent reads
   * through the same InputFile fail with {@link BlobErrorCode#CONDITION_NOT_MET} once the live
   * object's etag changes. Verifies a stale-snapshot read is detected even when the snapshot was
   * taken before a non-atomic {@code createOrOverwrite} replaces the bytes.
   */
  @Test
  void inputFileSnapshotRejectsReadAfterNonAtomicOverwrite() throws IOException {
    Random random = new Random();
    String location = randomLocation("read-snapshot");
    ADLSFileIO io = (ADLSFileIO) newFileIO();
    byte[] expected = new byte[1024 * 1024];
    random.nextBytes(expected);

    OutputFile out = io.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtil.writeFully(os, ByteBuffer.wrap(expected));
    }

    InputFile in = io.newInputFile(location);
    assertThat(in.exists()).isTrue();
    byte[] actual = new byte[1024 * 1024];
    try (InputStream is = in.newStream()) {
      IOUtil.readFully(is, actual, 0, actual.length);
    }
    assertThat(actual).isEqualTo(expected);

    // Non-atomic overwrite via createOrOverwrite — bypasses CAS but bumps the etag.
    OutputFile overwrite = io.newOutputFile(in);
    byte[] overbytes = new byte[1024 * 1024];
    random.nextBytes(overbytes);
    try (OutputStream os = overwrite.createOrOverwrite()) {
      IOUtil.writeFully(os, ByteBuffer.wrap(overbytes));
    }

    BlobStorageException etagFailure =
        Assertions.assertThrows(
            BlobStorageException.class,
            () -> {
              try (InputStream is = in.newStream()) {
                IOUtil.readFully(is, actual, 0, actual.length);
              }
            });
    assertThat(etagFailure.getErrorCode()).isEqualTo(BlobErrorCode.CONDITION_NOT_MET);

    // A freshly resolved InputFile reads the latest bytes successfully.
    try (InputStream is = io.newInputFile(location).newStream()) {
      IOUtil.readFully(is, actual, 0, actual.length);
    }
    assertThat(actual).isEqualTo(overbytes);
  }
}
