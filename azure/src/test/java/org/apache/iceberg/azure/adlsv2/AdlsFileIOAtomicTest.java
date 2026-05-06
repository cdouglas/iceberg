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
import com.azure.storage.file.datalake.DataLakeFileClient;
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

  // ─── ADLS append/lease behavior tests ───────────────────────────────────────────────────────
  //
  // These tests pin the documented behavior of the lease-based atomic append in
  // ADLSOutputFile.appendDestObj and Azure's underlying append/flush model. They are not part of
  // the cross-provider contract — they assert provider-specific guarantees that justify the lease.

  /**
   * Uncommitted data is invisible to readers; the file's reported length reflects only flushed
   * bytes. After an unflushed append, both {@code newInputFile().getLength()} and a streamed read
   * see only the seed.
   */
  @Test
  void unflushedAppendIsNotVisibleToReaders() throws IOException {
    String location = randomLocation("unflushed-invisible");
    SupportsAtomicOperations io = newFileIO();
    String path = pathOf(location);

    byte[] seed = "seed".getBytes();
    DataLakeFileClient client = resolver.fileClient(path);
    client.upload(new ByteArrayInputStream(seed), seed.length, true);

    // Stage uncommitted bytes via a raw appendWithResponse — never flush.
    byte[] orphan = "orphaned-uncommitted-payload".getBytes();
    client.appendWithResponse(
        new ByteArrayInputStream(orphan), seed.length, orphan.length, null, null, null, null);

    InputFile in = io.newInputFile(location);
    assertThat(in.exists()).isTrue();
    assertThat(in.getLength())
        .as("reported length excludes uncommitted bytes")
        .isEqualTo(seed.length);
    try (InputStream is = in.newStream()) {
      byte[] readBack = is.readAllBytes();
      assertThat(readBack).as("readers see only flushed bytes").isEqualTo(seed);
    }

    io.deleteFile(location);
  }

  /**
   * After a writer abandons an append (lease times out without flush), a subsequent writer can
   * acquire a fresh lease, append at the file's *committed* end, and flush. The orphaned
   * uncommitted bytes don't affect the committed content; the second writer's append at the same
   * offset cleanly overwrites the orphaned uncommitted block, and its flush position matches the
   * uncommitted region's new end.
   */
  @Test
  void abandonedAppendIsOverwrittenByNextWriter() throws IOException, InterruptedException {
    String location = randomLocation("orphan-recovery");
    SupportsAtomicOperations io = newFileIO();
    String path = pathOf(location);

    // Seed the file via the FileIO so subsequent reads see it through the same code path.
    byte[] seed = "seed".getBytes();
    DataLakeFileClient client = resolver.fileClient(path);
    client.upload(new ByteArrayInputStream(seed), seed.length, true);

    // Writer A: stage uncommitted bytes via a raw appendWithResponse holding a short lease, then
    // walk away without flushing. We use the lowest legal lease duration (15s) to keep the test
    // bounded; production uses the same value. The lease is acquired explicitly here so we can
    // observe the recovery without introducing a real concurrent client.
    byte[] orphan = "writer-A-orphan".getBytes();
    String orphanLeaseId = UUID.randomUUID().toString();
    com.azure.storage.file.datalake.options.DataLakeFileAppendOptions appendOpts =
        new com.azure.storage.file.datalake.options.DataLakeFileAppendOptions()
            .setLeaseAction(com.azure.storage.file.datalake.models.LeaseAction.ACQUIRE)
            .setProposedLeaseId(orphanLeaseId)
            .setLeaseDuration(15);
    client.appendWithResponse(
        new ByteArrayInputStream(orphan), seed.length, orphan.length, appendOpts, null, null);

    // Wait for the lease to expire. (15s minimum; pad a couple seconds for clock drift.)
    Thread.sleep(17_000);

    // Writer B: snapshot the file's committed state, append, flush via the FileIO lease path.
    InputFile snap = io.newInputFile(location);
    assertThat(snap.getLength()).isEqualTo(seed.length);
    AtomicOutputFile out = io.newOutputFile(snap);
    byte[] payloadB = "writer-B-real".getBytes();
    CAS tok =
        out.prepare(() -> new ByteArrayInputStream(payloadB), AtomicOutputFile.Strategy.APPEND);
    InputFile after = out.writeAtomic(tok, () -> new ByteArrayInputStream(payloadB));

    // Live file is seed + B's bytes; A's orphaned uncommitted block is gone.
    byte[] expected = new byte[seed.length + payloadB.length];
    System.arraycopy(seed, 0, expected, 0, seed.length);
    System.arraycopy(payloadB, 0, expected, seed.length, payloadB.length);
    try (InputStream is = after.newStream()) {
      assertThat(is.readAllBytes()).isEqualTo(expected);
    }

    io.deleteFile(location);
  }

  /**
   * While writer A holds the append lease, writer B's atomic append fails with {@link
   * SupportsAtomicOperations.AppendException} on lease conflict — the contention is detected at
   * append time, not flush time, so B's bytes never enter the uncommitted buffer.
   */
  @Test
  void concurrentAppendBlockedByActiveLease() throws IOException {
    String location = randomLocation("lease-conflict");
    SupportsAtomicOperations io = newFileIO();
    String path = pathOf(location);

    byte[] seed = "seed".getBytes();
    DataLakeFileClient client = resolver.fileClient(path);
    client.upload(new ByteArrayInputStream(seed), seed.length, true);

    // Writer A: acquire the lease via a manual appendWithResponse and HOLD it (don't flush, don't
    // release). This simulates an in-flight writer mid-append+flush span.
    byte[] payloadA = "writer-A-bytes".getBytes();
    String leaseA = UUID.randomUUID().toString();
    com.azure.storage.file.datalake.options.DataLakeFileAppendOptions appendOpts =
        new com.azure.storage.file.datalake.options.DataLakeFileAppendOptions()
            .setLeaseAction(com.azure.storage.file.datalake.models.LeaseAction.ACQUIRE)
            .setProposedLeaseId(leaseA)
            .setLeaseDuration(60);
    client.appendWithResponse(
        new ByteArrayInputStream(payloadA), seed.length, payloadA.length, appendOpts, null, null);

    try {
      // Writer B: attempt a regular FileIO append. Should fail with AppendException because A
      // holds the lease.
      InputFile snap = io.newInputFile(location);
      AtomicOutputFile out = io.newOutputFile(snap);
      byte[] payloadB = "writer-B-bytes".getBytes();
      CAS tok =
          out.prepare(() -> new ByteArrayInputStream(payloadB), AtomicOutputFile.Strategy.APPEND);
      assertThatThrownBy(() -> out.writeAtomic(tok, () -> new ByteArrayInputStream(payloadB)))
          .isInstanceOf(SupportsAtomicOperations.AppendException.class);

      // Live file is unchanged: A's uncommitted bytes are not visible, B's append never landed.
      InputFile reread = io.newInputFile(location);
      assertThat(reread.getLength()).isEqualTo(seed.length);
    } finally {
      // Clean up A's lease so the file can be deleted.
      try {
        new com.azure.storage.file.datalake.specialized.DataLakeLeaseClientBuilder()
            .fileClient(client)
            .leaseId(leaseA)
            .buildClient()
            .releaseLease();
      } catch (RuntimeException ignored) {
        // Already released, lease expired, or path is gone — fine.
      }
      io.deleteFile(location);
    }
  }

  private static String pathOf(String location) {
    // location is "abfs://<container>@<account>.dfs.core.windows.net/<path>" — strip prefix.
    int idx = location.indexOf(".dfs.core.windows.net/");
    return location.substring(idx + ".dfs.core.windows.net/".length());
  }
}
