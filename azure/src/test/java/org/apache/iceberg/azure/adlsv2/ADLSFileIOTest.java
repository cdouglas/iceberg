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
import org.apache.iceberg.io.SupportsAtomicOperations;
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

  /**
   * Walk the full atomic lifecycle on ADLS — atomic create, append, CAS replace at the same length,
   * append — and at every stage verify a concurrent writer pinned to the prior snapshot fails with
   * the expected {@link SupportsAtomicOperations.CASException} or {@link
   * SupportsAtomicOperations.AppendException}. Each ghost writer prepares its checksum from the
   * same snapshot the winner uses, then commits *after* the winner has advanced the live object —
   * modeling the canonical "we both observed the same generation; only one of us may commit" race.
   */
  @Test
  public void testCreateAppendCasAppendLifecycle() throws IOException {
    final String path = "lifecycle/" + UUID.randomUUID();
    final String location = az.location(path);
    ADLSFileIO io = createFileIO();

    // ---- Stage 1: atomic create. Both writers pin "object does not exist" (ifNoneMatch=*). ----
    final InputFile snap0 = io.newInputFile(location);
    assertThat(snap0.exists()).isFalse();
    final AtomicOutputFile winnerCreate = io.newOutputFile(snap0);
    final byte[] payload1 = "shaved my kiwis".getBytes();
    final CAS tok1 =
        winnerCreate.prepare(
            () -> new ByteArrayInputStream(payload1), AtomicOutputFile.Strategy.CAS);

    final InputFile ghostSnap0 = io.newInputFile(location);
    assertThat(ghostSnap0.exists()).isFalse();
    final AtomicOutputFile ghostCreate = io.newOutputFile(ghostSnap0);
    final byte[] ghostPayload1 = "ghost create".getBytes();
    final CAS ghostTok1 =
        ghostCreate.prepare(
            () -> new ByteArrayInputStream(ghostPayload1), AtomicOutputFile.Strategy.CAS);

    InputFile after1 = winnerCreate.writeAtomic(tok1, () -> new ByteArrayInputStream(payload1));
    assertThat(readAllBytes(after1)).isEqualTo(payload1);

    // Stale create: object now exists, ifNoneMatch=* must reject the write.
    org.junit.jupiter.api.Assertions.assertThrows(
        SupportsAtomicOperations.CASException.class,
        () -> ghostCreate.writeAtomic(ghostTok1, () -> new ByteArrayInputStream(ghostPayload1)));
    assertThat(readAllBytes(io.newInputFile(location))).isEqualTo(payload1);

    // ---- Stage 2: APPEND on the freshly-created object. ----
    final AtomicOutputFile winnerAppend1 = io.newOutputFile(after1);
    final byte[] payload2 = "shaved my hamster".getBytes();
    final CAS tok2 =
        winnerAppend1.prepare(
            () -> new ByteArrayInputStream(payload2), AtomicOutputFile.Strategy.APPEND);

    final AtomicOutputFile ghostAppend1 = io.newOutputFile(after1);
    final byte[] ghostPayload2 = "ghost append".getBytes();
    final CAS ghostTok2 =
        ghostAppend1.prepare(
            () -> new ByteArrayInputStream(ghostPayload2), AtomicOutputFile.Strategy.APPEND);

    InputFile after2 = winnerAppend1.writeAtomic(tok2, () -> new ByteArrayInputStream(payload2));
    final byte[] expected2 = concat(payload1, payload2);
    assertThat(readAllBytes(after2)).isEqualTo(expected2);

    // Stale append: the etag the ghost pinned is no longer live; flush must fail.
    org.junit.jupiter.api.Assertions.assertThrows(
        SupportsAtomicOperations.AppendException.class,
        () -> ghostAppend1.writeAtomic(ghostTok2, () -> new ByteArrayInputStream(ghostPayload2)));
    assertThat(readAllBytes(io.newInputFile(location))).isEqualTo(expected2);

    // ---- Stage 3: CAS replace at the *same* length as the appended object. ----
    final byte[] payload3 = padTo("shaved my pickles".getBytes(), expected2.length);
    final AtomicOutputFile winnerCas = io.newOutputFile(after2);
    final CAS tok3 =
        winnerCas.prepare(() -> new ByteArrayInputStream(payload3), AtomicOutputFile.Strategy.CAS);

    final byte[] ghostPayload3 = padTo("ghost replace".getBytes(), expected2.length);
    final AtomicOutputFile ghostCas = io.newOutputFile(after2);
    final CAS ghostTok3 =
        ghostCas.prepare(
            () -> new ByteArrayInputStream(ghostPayload3), AtomicOutputFile.Strategy.CAS);

    InputFile after3 = winnerCas.writeAtomic(tok3, () -> new ByteArrayInputStream(payload3));
    assertThat(readAllBytes(after3)).isEqualTo(payload3);

    // Stale CAS: same length but stale etag must fail.
    org.junit.jupiter.api.Assertions.assertThrows(
        SupportsAtomicOperations.CASException.class,
        () -> ghostCas.writeAtomic(ghostTok3, () -> new ByteArrayInputStream(ghostPayload3)));
    assertThat(readAllBytes(io.newInputFile(location))).isEqualTo(payload3);

    // ---- Stage 4: APPEND after CAS replace. ----
    final AtomicOutputFile winnerAppend2 = io.newOutputFile(after3);
    final byte[] payload4 = "shaved my yak".getBytes();
    final CAS tok4 =
        winnerAppend2.prepare(
            () -> new ByteArrayInputStream(payload4), AtomicOutputFile.Strategy.APPEND);

    final AtomicOutputFile ghostAppend2 = io.newOutputFile(after3);
    final byte[] ghostPayload4 = "ghost yak".getBytes();
    final CAS ghostTok4 =
        ghostAppend2.prepare(
            () -> new ByteArrayInputStream(ghostPayload4), AtomicOutputFile.Strategy.APPEND);

    InputFile after4 = winnerAppend2.writeAtomic(tok4, () -> new ByteArrayInputStream(payload4));
    final byte[] expected4 = concat(payload3, payload4);
    assertThat(readAllBytes(after4)).isEqualTo(expected4);

    // Stale append after CAS: ghost still pins after3's etag, which the winner has appended past.
    org.junit.jupiter.api.Assertions.assertThrows(
        SupportsAtomicOperations.AppendException.class,
        () -> ghostAppend2.writeAtomic(ghostTok4, () -> new ByteArrayInputStream(ghostPayload4)));
    assertThat(readAllBytes(io.newInputFile(location))).isEqualTo(expected4);
  }

  private static byte[] padTo(byte[] src, int length) {
    if (src.length >= length) {
      byte[] truncated = new byte[length];
      System.arraycopy(src, 0, truncated, 0, length);
      return truncated;
    }
    byte[] out = new byte[length];
    System.arraycopy(src, 0, out, 0, src.length);
    for (int i = src.length; i < length; i++) {
      out[i] = '.';
    }
    return out;
  }

  private static byte[] concat(byte[] a, byte[] b) {
    byte[] out = new byte[a.length + b.length];
    System.arraycopy(a, 0, out, 0, a.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }

  private static byte[] readAllBytes(InputFile in) throws IOException {
    try (InputStream s = in.newStream()) {
      return s.readAllBytes();
    }
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
