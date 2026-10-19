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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.io.SupportsAtomicOperationsContractTest;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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
  private static String account;
  private static String container;
  private static String sasToken;

  @BeforeAll
  static void initStorage() {
    runId = UUID.randomUUID().toString();
    String acct = System.getenv("AZURE_STORAGE_ACCOUNT");
    if (acct != null && !acct.isBlank()) {
      account = acct;
      container = System.getenv("AZURE_TEST_CONTAINER");
      sasToken = System.getenv("AZURE_STORAGE_SAS_TOKEN");
      if (container == null || container.isBlank() || sasToken == null || sasToken.isBlank()) {
        throw new IllegalStateException(
            "AZURE_STORAGE_ACCOUNT is set but AZURE_TEST_CONTAINER and/or "
                + "AZURE_STORAGE_SAS_TOKEN are not.");
      }
      properties = Maps.newHashMap();
      properties.put(
          AzureProperties.ADLS_SAS_TOKEN_PREFIX + account + ".dfs.core.windows.net", sasToken);
    }
  }

  @BeforeEach
  void requireCredentials() {
    Assumptions.assumeTrue(
        properties != null,
        "ADLS not available - set AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_SAS_TOKEN, AZURE_TEST_CONTAINER");
  }

  @Override
  protected SupportsAtomicOperations newFileIO() {
    return new ADLSFileIO(properties);
  }

  @Override
  protected int segmentLimit() {
    return 50_000;
  }

  @Override
  protected String randomLocation(String slug) {
    return String.format(
        "abfs://%s@%s.dfs.core.windows.net/%s/%s-%s",
        container, account, runId, slug, UUID.randomUUID());
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

  // ─── ADLS Append Blob behavior tests ─────────────────────────────────────────────────────────
  //
  // ADLSOutputFile appends with one conditional AppendBlock and replaces by renaming a staged temp
  // Append Blob onto the target. These pin the properties that design is chosen for; they are not
  // part of the cross-provider contract.

  /**
   * Writers racing from one snapshot, half appending and half replacing: exactly one commits, and
   * the file holds exactly what that writer wrote. Repeated a few rounds.
   */
  @Test
  void mixedAppendAndCasRaceLeavesOneWinner() throws Exception {
    String location = randomLocation("mixed-race");
    SupportsAtomicOperations io = newFileIO();
    int writers = 8;
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      InputFile current = createAtomically(io, location, "base|".getBytes(StandardCharsets.UTF_8));
      for (int round = 0; round < 3; round++) {
        ADLSInputFile snapshot = (ADLSInputFile) io.newInputFile(location);
        byte[] before = read(snapshot);
        snapshot.conditions(); // pin the ETag before the writers share the snapshot
        CyclicBarrier start = new CyclicBarrier(writers);
        List<Future<byte[]>> results = Lists.newArrayList();
        for (int w = 0; w < writers; w++) {
          boolean append = w % 2 == 0;
          byte[] payload =
              ((append ? "app-" : "cas-") + round + "-" + w + "|").getBytes(StandardCharsets.UTF_8);
          results.add(
              pool.submit(
                  () -> {
                    AtomicOutputFile out = io.newOutputFile(snapshot);
                    AtomicOutputFile.Strategy strategy =
                        append ? AtomicOutputFile.Strategy.APPEND : AtomicOutputFile.Strategy.CAS;
                    CAS token = out.prepare(() -> new ByteArrayInputStream(payload), strategy);
                    start.await();
                    try {
                      out.writeAtomic(token, () -> new ByteArrayInputStream(payload));
                    } catch (SupportsAtomicOperations.CASException
                        | SupportsAtomicOperations.AppendException e) {
                      return null;
                    }
                    return append ? concat(before, payload) : payload;
                  }));
        }
        List<byte[]> committed = Lists.newArrayList();
        for (Future<byte[]> result : results) {
          byte[] expected = result.get();
          if (expected != null) {
            committed.add(expected);
          }
        }
        assertThat(committed).as("writers that committed in round %s", round).hasSize(1);
        current = io.newInputFile(location);
        assertThat(read(current)).isEqualTo(committed.get(0));
      }
    } finally {
      pool.shutdownNow();
      io.deleteFile(location);
    }
  }

  /** A CAS that loses its race deletes its staged temp: nothing but the target is left behind. */
  @Test
  void lostCasLeavesNoTemp() throws IOException {
    String location = randomLocation("lost-cas");
    SupportsAtomicOperations io = newFileIO();
    try {
      InputFile created = createAtomically(io, location, "v1".getBytes(StandardCharsets.UTF_8));
      AtomicOutputFile winner = io.newOutputFile(created);
      AtomicOutputFile loser = io.newOutputFile(created);
      byte[] v2 = "v2".getBytes(StandardCharsets.UTF_8);
      byte[] v3 = "v3".getBytes(StandardCharsets.UTF_8);
      CAS winnerToken =
          winner.prepare(() -> new ByteArrayInputStream(v2), AtomicOutputFile.Strategy.CAS);
      CAS loserToken =
          loser.prepare(() -> new ByteArrayInputStream(v3), AtomicOutputFile.Strategy.CAS);
      winner.writeAtomic(winnerToken, () -> new ByteArrayInputStream(v2));
      assertThatThrownBy(() -> loser.writeAtomic(loserToken, () -> new ByteArrayInputStream(v3)))
          .isInstanceOf(SupportsAtomicOperations.CASException.class);

      assertThat(read(io.newInputFile(location))).isEqualTo(v2);

      String dir = location.substring(0, location.lastIndexOf('/'));
      List<String> names = Lists.newArrayList();
      try {
        ((ADLSFileIO) io).listPrefix(dir).forEach(info -> names.add(info.location()));
      } catch (DataLakeStorageException e) {
        // Listing needs a container-scoped SAS (srt=c); an object-scoped one cannot check this.
        Assumptions.assumeTrue(
            e.getStatusCode() != 403, "credentials cannot list " + dir + ": " + e.getErrorCode());
        throw e;
      }
      assertThat(names).noneMatch(name -> name.contains(".cas-"));
    } finally {
      io.deleteFile(location);
    }
  }

  /**
   * A file written through the Data Lake API (createOrOverwrite) is not an Append Blob. An APPEND
   * onto it fails with the service's error rather than {@link
   * SupportsAtomicOperations.AppendException}, so a caller does not retry it forever; a CAS turns
   * it into an Append Blob, and appends work from there.
   */
  @Test
  void dataLakeWrittenFileAcceptsAppendsAfterCas() throws IOException {
    String location = randomLocation("dfs-then-cas");
    ADLSFileIO io = (ADLSFileIO) newFileIO();
    try {
      try (OutputStream out = io.newOutputFile(location).createOrOverwrite()) {
        out.write("seed|".getBytes(StandardCharsets.UTF_8));
      }
      InputFile seeded = io.newInputFile(location);
      byte[] rec = "rec|".getBytes(StandardCharsets.UTF_8);
      AtomicOutputFile append = io.newOutputFile(seeded);
      CAS appendToken =
          append.prepare(() -> new ByteArrayInputStream(rec), AtomicOutputFile.Strategy.APPEND);
      assertThatThrownBy(() -> append.writeAtomic(appendToken, () -> new ByteArrayInputStream(rec)))
          .isInstanceOf(BlobStorageException.class)
          .satisfies(
              e ->
                  assertThat(((BlobStorageException) e).getErrorCode())
                      .isEqualTo(BlobErrorCode.INVALID_BLOB_TYPE));

      byte[] ckpt = "ckpt|".getBytes(StandardCharsets.UTF_8);
      AtomicOutputFile replace = io.newOutputFile(io.newInputFile(location));
      CAS casToken =
          replace.prepare(() -> new ByteArrayInputStream(ckpt), AtomicOutputFile.Strategy.CAS);
      InputFile replaced = replace.writeAtomic(casToken, () -> new ByteArrayInputStream(ckpt));

      AtomicOutputFile next = io.newOutputFile(replaced);
      CAS nextToken =
          next.prepare(() -> new ByteArrayInputStream(rec), AtomicOutputFile.Strategy.APPEND);
      InputFile appended = next.writeAtomic(nextToken, () -> new ByteArrayInputStream(rec));
      assertThat(read(appended)).isEqualTo(concat(ckpt, rec));
    } finally {
      io.deleteFile(location);
    }
  }

  private static byte[] concat(byte[] first, byte[] second) {
    byte[] out = Arrays.copyOf(first, first.length + second.length);
    System.arraycopy(second, 0, out, first.length, second.length);
    return out;
  }
}
