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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Provider-agnostic contract test for {@link SupportsAtomicOperations}. Subclasses supply a
 * configured FileIO and a per-test location factory; the suite exercises the atomic write contract
 * via {@link SupportsAtomicOperations}, {@link InputFile}, {@link AtomicOutputFile}, and {@link
 * CAS} only — never the concrete provider classes.
 *
 * <p>Whether the configured backend supports {@link AtomicOutputFile.Strategy#APPEND} is the
 * FileIO's declaration for the suite's locations ({@link #supportsAppend()}), which a subclass may
 * override. Tests gated on APPEND skip via {@link Assumptions#assumeTrue} so providers without it
 * (GCS today; standard S3 buckets) report ignored, and providers with it (S3 Express, ADLS) run
 * them.
 */
public abstract class SupportsAtomicOperationsContractTest {

  // ─── Subclass hooks ─────────────────────────────────────────────────────────────────────────

  /** Construct a fresh, configured FileIO. Called once per test method. */
  protected abstract SupportsAtomicOperations newFileIO();

  /**
   * Generate a unique location for a test write target. {@code slug} identifies the test method;
   * subclasses are expected to namespace under a per-suite UUID.
   */
  protected abstract String randomLocation(String slug);

  /**
   * Whether the backing store supports {@link AtomicOutputFile.Strategy#APPEND} where this suite
   * writes. Defaults to the FileIO's own {@link SupportsAtomicOperations#supportsAppend(String)}
   * declaration for a location of the suite (S3: a directory bucket appends, a general-purpose one
   * does not). Subclasses override only when the FileIO's capability is wrong for this specific
   * configuration.
   */
  protected boolean supportsAppend() {
    return newFileIO().supportsAppend(randomLocation("append"));
  }

  /**
   * The most writes an object of the backing store holds, the one that creates or replaces it
   * included (S3 Express: 10,000 parts; an Azure Append Blob: 50,000 blocks), or 0 for no limit
   * this suite should test. The test that reaches it makes that many requests, so it runs only with
   * the environment variable {@code ATOMIC_SEGMENT_LIMIT_TEST=true}.
   */
  protected int segmentLimit() {
    return 0;
  }

  /** Best-effort cleanup hook called after each successful test. Subclasses may override. */
  protected void cleanup(SupportsAtomicOperations io, String location) {
    try {
      io.deleteFile(location);
    } catch (RuntimeException ignored) {
      // cleanup is best-effort
    }
  }

  // ─── Tests ──────────────────────────────────────────────────────────────────────────────────

  @Test
  void inputFileForMissingPathReportsNotExists() {
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("missing");
    try {
      InputFile in = io.newInputFile(loc);
      assertThat(in.exists()).isFalse();
    } finally {
      cleanup(io, loc);
    }
  }

  @Test
  void atomicCreateSucceeds() throws IOException {
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("create");
    try {
      byte[] payload = bytes("hello world");
      InputFile created = createAtomically(io, loc, payload);
      assertThat(read(created)).isEqualTo(payload);
    } finally {
      cleanup(io, loc);
    }
  }

  @Test
  void atomicCreateRaceLeavesOneWinner() throws IOException {
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("create-race");
    try {
      runRace(io, loc, /* writers */ 4, AtomicOutputFile.Strategy.CAS, /* preExisting */ null);
    } finally {
      cleanup(io, loc);
    }
  }

  /**
   * A snapshot that has seen no version assumes the object does not exist: a CAS from it fails if
   * the object exists, and leaves it as it was.
   */
  @Test
  void casFromASnapshotThatSawNothingFailsIfTheObjectExists() throws IOException {
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("cas-unseen");
    try {
      byte[] first = bytes("first");
      createAtomically(io, loc, first);
      AtomicOutputFile out = io.newOutputFile(io.newInputFile(loc));
      byte[] second = bytes("second");
      CAS tok = out.prepare(supplier(second), AtomicOutputFile.Strategy.CAS);
      assertThatThrownBy(() -> out.writeAtomic(tok, supplier(second)))
          .isInstanceOf(SupportsAtomicOperations.CASException.class);
      assertThat(read(io.newInputFile(loc))).isEqualTo(first);
    } finally {
      cleanup(io, loc);
    }
  }

  /** A CAS from a snapshot that looked the object up replaces the version the lookup saw. */
  @Test
  void casFromASnapshotThatLookedReplacesWhatItSaw() throws IOException {
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("cas-looked");
    try {
      createAtomically(io, loc, bytes("first"));
      InputFile looked = io.newInputFile(loc);
      assertThat(looked.getLength()).isEqualTo(5);
      byte[] second = bytes("second");
      AtomicOutputFile out = io.newOutputFile(looked);
      InputFile replaced =
          out.writeAtomic(
              out.prepare(supplier(second), AtomicOutputFile.Strategy.CAS), supplier(second));
      assertThat(read(replaced)).isEqualTo(second);
    } finally {
      cleanup(io, loc);
    }
  }

  /** An APPEND from a snapshot that has seen no version extends the object's current one. */
  @Test
  void appendFromASnapshotThatSawNothingExtendsTheObject() throws IOException {
    Assumptions.assumeTrue(supportsAppend(), "APPEND not supported by this FileIO");
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("append-unseen");
    try {
      byte[] head = bytes("head");
      byte[] tail = bytes("tail");
      createAtomically(io, loc, head);
      InputFile after = appendAtomically(io, io.newInputFile(loc), tail);
      assertThat(read(after)).isEqualTo(concat(head, tail));
    } finally {
      cleanup(io, loc);
    }
  }

  @Test
  void casReplaceSucceeds() throws IOException {
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("cas-replace");
    try {
      byte[] v1 = bytes("first");
      InputFile created = createAtomically(io, loc, v1);

      byte[] v2 = padTo(bytes("second"), v1.length);
      AtomicOutputFile out = io.newOutputFile(created);
      CAS tok = out.prepare(supplier(v2), AtomicOutputFile.Strategy.CAS);
      InputFile replaced = out.writeAtomic(tok, supplier(v2));
      assertThat(read(replaced)).isEqualTo(v2);
    } finally {
      cleanup(io, loc);
    }
  }

  /**
   * A CAS write whose source fails partway, as when the writer dies mid-commit, must leave the
   * target as it was. A replace that is not one atomic step (for example truncate, then append and
   * flush) leaves an empty or partial object that every later reader sees.
   */
  @Test
  void casReplaceThatFailsMidWriteLeavesTargetIntact() throws IOException {
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("cas-fails-mid-write");
    try {
      byte[] v1 = bytes("the complete first version");
      InputFile created = createAtomically(io, loc, v1);

      byte[] v2 = padTo(bytes("second"), 64 * 1024);
      AtomicOutputFile out = io.newOutputFile(created);
      CAS tok = out.prepare(supplier(v2), AtomicOutputFile.Strategy.CAS);
      assertThatThrownBy(() -> out.writeAtomic(tok, () -> failingAfter(v2, v2.length / 2)))
          .as("writeAtomic whose source fails halfway");

      assertThat(read(io.newInputFile(loc)))
          .as("target after a CAS write that failed mid-write")
          .isEqualTo(v1);
    } finally {
      cleanup(io, loc);
    }
  }

  @Test
  void casReplaceRaceLeavesOneWinner() throws IOException {
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("cas-race");
    try {
      InputFile baseline = createAtomically(io, loc, bytes("baseline"));
      runRace(io, loc, /* writers */ 4, AtomicOutputFile.Strategy.CAS, baseline);
    } finally {
      cleanup(io, loc);
    }
  }

  @Test
  void appendSucceeds() throws IOException {
    Assumptions.assumeTrue(supportsAppend(), "APPEND not supported by this FileIO");
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("append");
    try {
      byte[] head = bytes("head");
      byte[] tail = bytes("tail");
      InputFile created = createAtomically(io, loc, head);
      InputFile after = appendAtomically(io, created, tail);
      assertThat(read(after)).isEqualTo(concat(head, tail));
    } finally {
      cleanup(io, loc);
    }
  }

  /**
   * An append past the store's limit on writes per object fails with {@link
   * SupportsAtomicOperations.SegmentLimitException}, not as an append race (which a caller retries
   * as an append), and lands nothing; a CAS replaces the object, after which appends succeed.
   */
  @Test
  void appendPastTheSegmentLimitFails() throws IOException {
    Assumptions.assumeTrue(supportsAppend() && segmentLimit() > 0, "no segment limit to test");
    Assumptions.assumeTrue(
        "true".equals(System.getenv("ATOMIC_SEGMENT_LIMIT_TEST")),
        "makes segmentLimit() requests: run with ATOMIC_SEGMENT_LIMIT_TEST=true");
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("segment-limit");
    try {
      InputFile current = createAtomically(io, loc, bytes("0"));
      for (int written = 1; written < segmentLimit(); written += 1) {
        current = appendAtomically(io, current, bytes("."));
      }
      InputFile full = current;
      byte[] before = read(io.newInputFile(loc));
      assertThat(before).hasSize(segmentLimit());

      assertThatThrownBy(() -> appendAtomically(io, full, bytes("!")))
          .isInstanceOf(SupportsAtomicOperations.SegmentLimitException.class);
      assertThat(read(io.newInputFile(loc))).isEqualTo(before);

      // a CAS replaces the version its snapshot saw
      InputFile live = io.newInputFile(loc);
      live.getLength();
      AtomicOutputFile out = io.newOutputFile(live);
      byte[] fresh = bytes("fresh");
      InputFile replaced =
          out.writeAtomic(
              out.prepare(supplier(fresh), AtomicOutputFile.Strategy.CAS), supplier(fresh));
      InputFile after = appendAtomically(io, replaced, bytes("more"));
      assertThat(read(after)).isEqualTo(concat(fresh, bytes("more")));
    } finally {
      cleanup(io, loc);
    }
  }

  @Test
  void appendRaceLeavesOneWinner() throws IOException {
    Assumptions.assumeTrue(supportsAppend(), "APPEND not supported by this FileIO");
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("append-race");
    try {
      InputFile baseline = createAtomically(io, loc, bytes("baseline"));
      runRace(io, loc, /* writers */ 4, AtomicOutputFile.Strategy.APPEND, baseline);
    } finally {
      cleanup(io, loc);
    }
  }

  /**
   * True-concurrency CAS race. {@code writers} threads each fetch their own snapshot and prepare
   * their CAS token, then all rendezvous at a barrier and call {@code writeAtomic} simultaneously.
   * Exactly one must succeed; every other must throw {@link SupportsAtomicOperations.CASException};
   * the live object must equal the winner's payload.
   */
  @Test
  void casReplaceRaceConcurrent() throws Exception {
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("cas-race-concurrent");
    try {
      createAtomically(io, loc, bytes("baseline"));
      runConcurrentRace(io, loc, /* writers */ 4, AtomicOutputFile.Strategy.CAS);
    } finally {
      cleanup(io, loc);
    }
  }

  /** True-concurrency APPEND race. Same shape as {@link #casReplaceRaceConcurrent}. */
  @Test
  void appendRaceConcurrent() throws Exception {
    Assumptions.assumeTrue(supportsAppend(), "APPEND not supported by this FileIO");
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("append-race-concurrent");
    try {
      createAtomically(io, loc, bytes("baseline"));
      runConcurrentRace(io, loc, /* writers */ 4, AtomicOutputFile.Strategy.APPEND);
    } finally {
      cleanup(io, loc);
    }
  }

  /**
   * Walk the full atomic lifecycle — create, APPEND, CAS-replace at the same length, APPEND — and
   * at every stage force a concurrent writer pinned to the prior snapshot to surface the
   * appropriate exception. Skipped on providers without APPEND.
   */
  @Test
  void lifecycleCreateAppendCasAppend() throws IOException {
    Assumptions.assumeTrue(supportsAppend(), "APPEND not supported by this FileIO");
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("lifecycle");
    try {
      // ── Stage 1: atomic create. Both writers pin "object does not exist". ──────────────────
      InputFile snap0 = io.newInputFile(loc);
      assertThat(snap0.exists()).isFalse();
      AtomicOutputFile winner1 = io.newOutputFile(snap0);
      byte[] createPayload = bytes("stage-1-create");
      CAS tok1 = winner1.prepare(supplier(createPayload), AtomicOutputFile.Strategy.CAS);

      InputFile ghostSnap0 = io.newInputFile(loc);
      AtomicOutputFile ghost1 = io.newOutputFile(ghostSnap0);
      byte[] ghostCreate = bytes("ghost-create");
      CAS gTok1 = ghost1.prepare(supplier(ghostCreate), AtomicOutputFile.Strategy.CAS);

      InputFile after1 = winner1.writeAtomic(tok1, supplier(createPayload));
      assertThat(read(after1)).isEqualTo(createPayload);
      assertThatThrownBy(() -> ghost1.writeAtomic(gTok1, supplier(ghostCreate)))
          .isInstanceOf(SupportsAtomicOperations.CASException.class);

      // ── Stage 2: APPEND. ───────────────────────────────────────────────────────────────────
      AtomicOutputFile winner2 = io.newOutputFile(after1);
      byte[] appendPayload = bytes("stage-2-append");
      CAS tok2 = winner2.prepare(supplier(appendPayload), AtomicOutputFile.Strategy.APPEND);

      AtomicOutputFile ghost2 = io.newOutputFile(after1);
      byte[] ghostAppend = bytes("ghost-append");
      CAS gTok2 = ghost2.prepare(supplier(ghostAppend), AtomicOutputFile.Strategy.APPEND);

      InputFile after2 = winner2.writeAtomic(tok2, supplier(appendPayload));
      byte[] expected2 = concat(createPayload, appendPayload);
      assertThat(read(after2)).isEqualTo(expected2);
      assertThatThrownBy(() -> ghost2.writeAtomic(gTok2, supplier(ghostAppend)))
          .isInstanceOf(SupportsAtomicOperations.AppendException.class);

      // ── Stage 3: CAS replace at the same length. ──────────────────────────────────────────
      byte[] replacePayload = padTo(bytes("stage-3-cas-replace"), expected2.length);
      AtomicOutputFile winner3 = io.newOutputFile(after2);
      CAS tok3 = winner3.prepare(supplier(replacePayload), AtomicOutputFile.Strategy.CAS);

      AtomicOutputFile ghost3 = io.newOutputFile(after2);
      byte[] ghostReplace = padTo(bytes("ghost-replace"), expected2.length);
      CAS gTok3 = ghost3.prepare(supplier(ghostReplace), AtomicOutputFile.Strategy.CAS);

      InputFile after3 = winner3.writeAtomic(tok3, supplier(replacePayload));
      assertThat(read(after3)).isEqualTo(replacePayload);
      assertThatThrownBy(() -> ghost3.writeAtomic(gTok3, supplier(ghostReplace)))
          .isInstanceOf(SupportsAtomicOperations.CASException.class);

      // ── Stage 4: APPEND after CAS. ─────────────────────────────────────────────────────────
      AtomicOutputFile winner4 = io.newOutputFile(after3);
      byte[] tailPayload = bytes("stage-4-append-after-cas");
      CAS tok4 = winner4.prepare(supplier(tailPayload), AtomicOutputFile.Strategy.APPEND);

      AtomicOutputFile ghost4 = io.newOutputFile(after3);
      byte[] ghostTail = bytes("ghost-tail");
      CAS gTok4 = ghost4.prepare(supplier(ghostTail), AtomicOutputFile.Strategy.APPEND);

      InputFile after4 = winner4.writeAtomic(tok4, supplier(tailPayload));
      assertThat(read(after4)).isEqualTo(concat(replacePayload, tailPayload));
      assertThatThrownBy(() -> ghost4.writeAtomic(gTok4, supplier(ghostTail)))
          .isInstanceOf(SupportsAtomicOperations.AppendException.class);
    } finally {
      cleanup(io, loc);
    }
  }

  /**
   * An APPEND pinned to a version that a CAS has since replaced fails, even when the replacement
   * has the length the append expects: an append is conditioned on the version it extends, not only
   * on its offset. A store that checked only the offset would extend an object the writer never
   * saw, and a reader that trusts the bytes before an offset it already read (reading only what was
   * appended since) could not tell that the object under them changed. Skipped on providers without
   * APPEND.
   */
  @Test
  void appendPinnedToAVersionReplacedAtTheSameLengthFails() throws IOException {
    Assumptions.assumeTrue(supportsAppend(), "APPEND not supported by this FileIO");
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("stale-append");
    try {
      byte[] first = bytes("first-version");
      InputFile v1 = createAtomically(io, loc, first);

      // A writer prepares an append to the version it read
      AtomicOutputFile stale = io.newOutputFile(v1);
      byte[] staleTail = bytes("stale-tail");
      CAS staleTok = stale.prepare(supplier(staleTail), AtomicOutputFile.Strategy.APPEND);

      // Another replaces that version with one of the same length
      byte[] second = padTo(bytes("replaced"), first.length);
      AtomicOutputFile replace = io.newOutputFile(v1);
      InputFile v2 =
          replace.writeAtomic(
              replace.prepare(supplier(second), AtomicOutputFile.Strategy.CAS), supplier(second));
      assertThat(v2.getLength()).isEqualTo(v1.getLength());

      assertThatThrownBy(() -> stale.writeAtomic(staleTok, supplier(staleTail)))
          .isInstanceOf(SupportsAtomicOperations.AppendException.class);
      assertThat(read(io.newInputFile(loc))).isEqualTo(second);

      // An append to the version that replaced it extends it
      InputFile v3 = appendAtomically(io, v2, bytes("tail"));
      assertThat(read(v3)).isEqualTo(concat(second, bytes("tail")));
    } finally {
      cleanup(io, loc);
    }
  }

  /**
   * A CAS from a version that an APPEND has since extended fails, and lands nothing: a replace is
   * conditioned on the version it saw, whatever moved it since, an append as much as another
   * replace. A store that let it land would erase a record another writer appended and was told had
   * landed (a log compacts by replacing the version it read, while writers append to it). Skipped
   * on providers without APPEND.
   */
  @Test
  void casFromAVersionAnAppendExtendedFails() throws IOException {
    Assumptions.assumeTrue(supportsAppend(), "APPEND not supported by this FileIO");
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("cas-over-append");
    try {
      byte[] first = bytes("first-version");
      InputFile v1 = createAtomically(io, loc, first);

      // A writer prepares a replace of the version it read
      AtomicOutputFile stale = io.newOutputFile(v1);
      byte[] replacement = bytes("replacement");
      CAS staleTok = stale.prepare(supplier(replacement), AtomicOutputFile.Strategy.CAS);

      // Another extends that version
      byte[] tail = bytes("appended");
      InputFile v2 = appendAtomically(io, v1, tail);

      assertThatThrownBy(() -> stale.writeAtomic(staleTok, supplier(replacement)))
          .isInstanceOf(SupportsAtomicOperations.CASException.class);
      assertThat(read(io.newInputFile(loc))).isEqualTo(concat(first, tail));

      // A replace of the version the append made replaces it
      AtomicOutputFile fresh = io.newOutputFile(v2);
      InputFile v3 =
          fresh.writeAtomic(
              fresh.prepare(supplier(replacement), AtomicOutputFile.Strategy.CAS),
              supplier(replacement));
      assertThat(read(v3)).isEqualTo(replacement);
    } finally {
      cleanup(io, loc);
    }
  }

  /**
   * True-concurrency race of CASes and APPENDs from one version: exactly one lands, each other
   * fails as its own strategy's lost race (CASException, AppendException), and the object is the
   * winner's: its payload for a CAS, the version extended by its payload for an APPEND. Skipped on
   * providers without APPEND.
   */
  @Test
  void casAndAppendRaceConcurrent() throws Exception {
    Assumptions.assumeTrue(supportsAppend(), "APPEND not supported by this FileIO");
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("cas-append-race-concurrent");
    try {
      createAtomically(io, loc, bytes("baseline"));
      runConcurrentRace(
          io,
          loc,
          AtomicOutputFile.Strategy.CAS,
          AtomicOutputFile.Strategy.APPEND,
          AtomicOutputFile.Strategy.CAS,
          AtomicOutputFile.Strategy.APPEND);
    } finally {
      cleanup(io, loc);
    }
  }

  /**
   * An APPEND of {@link SupportsAtomicOperations#maxAppendBytes} bytes lands; one of a byte more
   * fails before it lands, refused ({@link SupportsAtomicOperations.StorageRefusedException}), not
   * as a lost race, which a caller would send again as it is; and a CAS is not bound by the limit.
   * Skipped where the store sets no limit. A limit past {@link #LARGE_APPEND_BYTES} writes about
   * three times that, so the test then runs only with the environment variable {@code
   * ATOMIC_APPEND_LIMIT_TEST=true}.
   */
  @Test
  void appendPastMaxAppendBytesIsRefusedAndLandsNothing() throws IOException {
    Assumptions.assumeTrue(supportsAppend(), "APPEND not supported by this FileIO");
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("append-limit");
    long max = io.maxAppendBytes(loc);
    Assumptions.assumeTrue(max > 0, "no limit on an append's bytes");
    Assumptions.assumeTrue(
        max <= LARGE_APPEND_BYTES || "true".equals(System.getenv("ATOMIC_APPEND_LIMIT_TEST")),
        "writes about three times the limit: run with ATOMIC_APPEND_LIMIT_TEST=true");
    try {
      byte[] first = bytes("first-version");
      InputFile v1 = createAtomically(io, loc, first);

      byte[] atLimit = filled(Math.toIntExact(max), (byte) 'a');
      InputFile v2 = appendAtomically(io, v1, atLimit);
      byte[] expected = concat(first, atLimit);
      assertThat(v2.getLength()).isEqualTo(expected.length);

      byte[] pastLimit = filled(Math.toIntExact(max + 1), (byte) 'b');
      assertThatThrownBy(() -> appendAtomically(io, v2, pastLimit))
          .isInstanceOf(SupportsAtomicOperations.StorageRefusedException.class);
      assertThat(read(io.newInputFile(loc))).isEqualTo(expected);

      // A CAS is not bound by it
      AtomicOutputFile out = io.newOutputFile(v2);
      InputFile v3 =
          out.writeAtomic(
              out.prepare(supplier(pastLimit), AtomicOutputFile.Strategy.CAS), supplier(pastLimit));
      assertThat(read(v3)).isEqualTo(pastLimit);
    } finally {
      cleanup(io, loc);
    }
  }

  /** The largest append limit the suite reaches without {@code ATOMIC_APPEND_LIMIT_TEST}. */
  protected static final long LARGE_APPEND_BYTES = 16L << 20;

  // ─── Reads ──────────────────────────────────────────────────────────────────────────────────

  /**
   * A file that looked the object up reads the version the lookup found, after a replace too: that
   * version's bytes, or {@link SupportsAtomicOperations.StaleReadException}; never the
   * replacement's. The replacement here is longer, so a read of the length the lookup found that
   * the store does not pin to its version returns the start of the replacement, which looks like
   * the old version followed by appends.
   */
  @Test
  void aFileThatLookedReadsThatVersionAfterAReplace() throws IOException {
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("read-replaced");
    try {
      byte[] first = randomBytes(1, 4 * 1024);
      createAtomically(io, loc, first);
      InputFile looked = io.newInputFile(loc);
      assertThat(looked.getLength()).isEqualTo(first.length);

      byte[] second = randomBytes(2, 8 * 1024);
      replaceAtomically(io, loc, second);

      assertReadsOnly(looked, first, "a whole read after a replace");
      assertRangeReadsOnly(looked, first, 1024, "a range read after a replace");
      assertThat(looked.getLength()).isEqualTo(first.length);
      assertThat(read(io.newInputFile(loc))).isEqualTo(second);
    } finally {
      cleanup(io, loc);
    }
  }

  /**
   * An append makes a new version: a file that looked the object up before it reads the version
   * before it, or fails as stale, and its length is that version's. Skipped on providers without
   * APPEND.
   */
  @Test
  void aFileThatLookedReadsThatVersionAfterAnAppend() throws IOException {
    Assumptions.assumeTrue(supportsAppend(), "APPEND not supported by this FileIO");
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("read-appended");
    try {
      byte[] head = randomBytes(3, 4 * 1024);
      createAtomically(io, loc, head);
      InputFile looked = io.newInputFile(loc);
      assertThat(looked.getLength()).isEqualTo(head.length);

      InputFile other = io.newInputFile(loc);
      other.getLength();
      byte[] tail = randomBytes(4, 1024);
      appendAtomically(io, other, tail);

      assertReadsOnly(looked, head, "a whole read after an append");
      assertRangeReadsOnly(looked, head, 1024, "a range read after an append");
      assertThat(looked.getLength()).isEqualTo(head.length);
      assertThat(read(io.newInputFile(loc))).isEqualTo(concat(head, tail));
    } finally {
      cleanup(io, loc);
    }
  }

  /**
   * A file whose first request was a read has seen the version that read found, on a store whose
   * reads record it: every later request of the stream (a backward seek reopens it, a range read
   * sends its own), a new stream, the file's length and a CAS from it are of that version, after a
   * replace of the same length too. A CAS from it fails rather than replace a version it did not
   * read.
   */
  @Test
  void aFileWhoseFirstRequestReadReadsThatVersion() throws IOException {
    SupportsAtomicOperations io = newFileIO();
    Assumptions.assumeTrue(io.readsRecordVersion(), "reads do not record their version");
    String loc = randomLocation("read-first");
    try {
      byte[] first = randomBytes(5, 64 * 1024);
      createAtomically(io, loc, first);
      InputFile reader = io.newInputFile(loc);
      byte[] second = randomBytes(6, first.length);
      try (SeekableInputStream in = reader.newStream()) {
        byte[] start = new byte[16];
        fill(in, start);
        assertThat(start).isEqualTo(Arrays.copyOfRange(first, 0, start.length));

        replaceAtomically(io, loc, second);

        in.seek(0);
        try {
          byte[] again = new byte[32];
          fill(in, again);
          assertThat(again)
              .as("a reopened stream's bytes")
              .isEqualTo(Arrays.copyOfRange(first, 0, again.length));
        } catch (SupportsAtomicOperations.StaleReadException e) {
          // the store no longer serves the version the stream reads
        }
        if (in instanceof RangeReadable range) {
          try {
            byte[] middle = new byte[64];
            range.readFully(32 * 1024, middle);
            assertThat(middle)
                .as("a stream's range read")
                .isEqualTo(Arrays.copyOfRange(first, 32 * 1024, 32 * 1024 + middle.length));
          } catch (SupportsAtomicOperations.StaleReadException e) {
            // as above
          }
        }
      }

      assertReadsOnly(reader, first, "a new stream of the file");
      assertThat(reader.getLength()).isEqualTo(first.length);
      AtomicOutputFile out = io.newOutputFile(reader);
      byte[] third = randomBytes(7, 1024);
      CAS tok = out.prepare(supplier(third), AtomicOutputFile.Strategy.CAS);
      assertThatThrownBy(() -> out.writeAtomic(tok, supplier(third)))
          .isInstanceOf(SupportsAtomicOperations.CASException.class);
      assertThat(read(io.newInputFile(loc))).isEqualTo(second);
    } finally {
      cleanup(io, loc);
    }
  }

  /**
   * On a store whose reads record their version, a file whose first request reads from an offset to
   * the end (one GET, with no lookup) has seen the version it read, and the whole object's length:
   * after a replace of another length, its length is still the version's, and a CAS from it fails
   * rather than replace a version it did not read.
   */
  @Test
  void aFileWhoseFirstRequestReadsFromAnOffsetHasSeenThatVersion() throws IOException {
    SupportsAtomicOperations io = newFileIO();
    Assumptions.assumeTrue(io.readsRecordVersion(), "reads do not record their version");
    String loc = randomLocation("read-from-offset");
    try {
      byte[] first = randomBytes(8, 8 * 1024);
      createAtomically(io, loc, first);
      InputFile reader = io.newInputFile(loc);
      byte[] rest;
      try (SeekableInputStream in = reader.newStream()) {
        in.seek(5 * 1024);
        rest = in.readAllBytes();
      }
      assertThat(rest).isEqualTo(Arrays.copyOfRange(first, 5 * 1024, first.length));

      byte[] second = randomBytes(9, 2 * 1024);
      replaceAtomically(io, loc, second);

      assertThat(reader.getLength()).as("the length of the version read").isEqualTo(first.length);
      assertReadsOnly(reader, first, "a new stream of the file");
      AtomicOutputFile out = io.newOutputFile(reader);
      byte[] third = randomBytes(10, 1024);
      CAS tok = out.prepare(supplier(third), AtomicOutputFile.Strategy.CAS);
      assertThatThrownBy(() -> out.writeAtomic(tok, supplier(third)))
          .isInstanceOf(SupportsAtomicOperations.CASException.class);
      assertThat(read(io.newInputFile(loc))).isEqualTo(second);
    } finally {
      cleanup(io, loc);
    }
  }

  /**
   * On a store whose reads record their version, a stream's read from the object's end or past it
   * returns the stream's end, whether its file has seen a version or not, and a read from inside
   * the object still reads it after.
   */
  @Test
  void aReadFromTheEndOrPastItIsTheStreamsEnd() throws IOException {
    SupportsAtomicOperations io = newFileIO();
    Assumptions.assumeTrue(io.readsRecordVersion(), "reads do not record their version");
    String loc = randomLocation("read-past-end");
    try {
      byte[] payload = randomBytes(11, 1024);
      createAtomically(io, loc, payload);
      for (long offset : new long[] {payload.length, payload.length + 20, 4 * payload.length}) {
        InputFile fresh = io.newInputFile(loc);
        try (SeekableInputStream in = fresh.newStream()) {
          in.seek(offset);
          assertThat(in.read()).as("a read from " + offset + " by a fresh file").isEqualTo(-1);
          assertThat(in.read(new byte[16], 0, 16)).isEqualTo(-1);
          in.seek(payload.length - 4);
          byte[] last = new byte[4];
          fill(in, last);
          assertThat(last)
              .isEqualTo(Arrays.copyOfRange(payload, payload.length - 4, payload.length));
        }
        InputFile looked = io.newInputFile(loc);
        assertThat(looked.getLength()).isEqualTo(payload.length);
        try (SeekableInputStream in = looked.newStream()) {
          in.seek(offset);
          assertThat(in.read())
              .as("a read from " + offset + " by a file that looked")
              .isEqualTo(-1);
        }
      }
    } finally {
      cleanup(io, loc);
    }
  }

  /**
   * A file that has seen a version reads that version past the end of a shorter replacement, or
   * fails as stale. A stream that opens there (a new stream, or one that reopens after a connection
   * reset) sends a GET from that offset pinned to the version, which the store must refuse as a
   * failed precondition (412) rather than as a range the live object cannot satisfy (416): a stream
   * reports a 416 as its end, so the read would end inside the version, short of it
   * (fileio-catalog's errata U4). Both a file that looked the object up and, where reads record
   * their version, one whose first request was a read; by a stream and by a range read.
   */
  @Test
  void aSeenVersionReadPastTheEndOfAShorterReplacementIsNotCutShort() throws IOException {
    SupportsAtomicOperations io = newFileIO();
    String loc = randomLocation("read-past-replacement");
    try {
      byte[] first = randomBytes(12, 64 * 1024);
      createAtomically(io, loc, first);
      InputFile looked = io.newInputFile(loc);
      assertThat(looked.getLength()).isEqualTo(first.length);
      List<InputFile> seen = new ArrayList<>(List.of(looked));
      if (io.readsRecordVersion()) {
        InputFile reader = io.newInputFile(loc);
        try (SeekableInputStream in = reader.newStream()) {
          byte[] start = new byte[16];
          fill(in, start);
          assertThat(start).isEqualTo(Arrays.copyOfRange(first, 0, start.length));
        }
        seen.add(reader);
      }

      byte[] second = randomBytes(13, 1024);
      replaceAtomically(io, loc, second);

      for (InputFile file : seen) {
        String which = file == looked ? "a file that looked" : "a file whose first request read";
        for (int offset : new int[] {second.length, 32 * 1024, first.length - 64}) {
          byte[] expected = Arrays.copyOfRange(first, offset, offset + 64);
          String what = which + ", from " + offset + " (the replacement has " + second.length + ")";
          try (SeekableInputStream in = file.newStream()) {
            in.seek(offset);
            byte[] bytes = new byte[expected.length];
            fill(in, bytes);
            assertThat(bytes).as(what).isEqualTo(expected);
          } catch (SupportsAtomicOperations.StaleReadException e) {
            // the store no longer serves the version: the precondition failed
          } catch (IOException e) {
            throw new AssertionError(what + ": a read of the version ended short of it", e);
          }
          try (SeekableInputStream in = file.newStream()) {
            if (in instanceof RangeReadable range) {
              byte[] bytes = new byte[expected.length];
              range.readFully(offset, bytes);
              assertThat(bytes).as(what + ", a range read").isEqualTo(expected);
            }
          } catch (SupportsAtomicOperations.StaleReadException e) {
            // as above
          } catch (IOException e) {
            throw new AssertionError(what + ": a range read of the version ended short of it", e);
          }
        }
      }
      assertThat(read(io.newInputFile(loc))).isEqualTo(second);
    } finally {
      cleanup(io, loc);
    }
  }

  /** Reads {@code file} whole: {@code expected}, unless the read fails as stale. */
  private static void assertReadsOnly(InputFile file, byte[] expected, String what)
      throws IOException {
    try {
      assertThat(read(file)).as(what).isEqualTo(expected);
    } catch (SupportsAtomicOperations.StaleReadException e) {
      // the store no longer serves the version the file reads; no other version's bytes came back
    }
  }

  /** As {@link #assertReadsOnly}, by a range read at {@code offset}, if the stream offers one. */
  private static void assertRangeReadsOnly(InputFile file, byte[] expected, int offset, String what)
      throws IOException {
    try (SeekableInputStream in = file.newStream()) {
      if (in instanceof RangeReadable range) {
        byte[] bytes = new byte[expected.length - offset];
        range.readFully(offset, bytes);
        assertThat(bytes).as(what).isEqualTo(Arrays.copyOfRange(expected, offset, expected.length));
      }
    } catch (SupportsAtomicOperations.StaleReadException e) {
      // as above
    }
  }

  /** Replaces the object at {@code location}, through a snapshot that looks it up first. */
  private static InputFile replaceAtomically(
      SupportsAtomicOperations io, String location, byte[] payload) throws IOException {
    InputFile live = io.newInputFile(location);
    live.getLength();
    AtomicOutputFile out = io.newOutputFile(live);
    return out.writeAtomic(
        out.prepare(supplier(payload), AtomicOutputFile.Strategy.CAS), supplier(payload));
  }

  /** Reads {@code buffer.length} bytes from {@code in}, or fails at its end. */
  private static void fill(InputStream in, byte[] buffer) throws IOException {
    int read = 0;
    while (read < buffer.length) {
      int count = in.read(buffer, read, buffer.length - read);
      if (count < 0) {
        throw new IOException("Stream ended after " + read + " of " + buffer.length + " bytes");
      }
      read += count;
    }
  }

  private static byte[] randomBytes(long seed, int length) {
    byte[] bytes = new byte[length];
    new Random(seed).nextBytes(bytes);
    return bytes;
  }

  // ─── Race helper ────────────────────────────────────────────────────────────────────────────

  /**
   * Pin {@code writers} writers to the same snapshot and serialize their writeAtomic invocations.
   * Exactly one must succeed; the rest must throw the strategy-appropriate exception (CASException
   * or AppendException). The final live object equals the winner's payload.
   */
  private void runRace(
      SupportsAtomicOperations io,
      String location,
      int writers,
      AtomicOutputFile.Strategy strategy,
      InputFile preExistingSnapshot)
      throws IOException {
    Class<? extends SupportsAtomicOperations.AtomicOperationException> expected =
        strategy == AtomicOutputFile.Strategy.APPEND
            ? SupportsAtomicOperations.AppendException.class
            : SupportsAtomicOperations.CASException.class;

    Random random = new Random(42);
    List<byte[]> payloads = new ArrayList<>(writers);
    List<AtomicOutputFile> outs = new ArrayList<>(writers);
    List<CAS> tokens = new ArrayList<>(writers);

    for (int i = 0; i < writers; i++) {
      InputFile snap = io.newInputFile(location);
      if (preExistingSnapshot != null) {
        // every writer sees the version they race to replace or extend
        snap.getLength();
      }
      byte[] payload = new byte[1024];
      random.nextBytes(payload);
      payloads.add(payload);
      AtomicOutputFile out = io.newOutputFile(snap);
      outs.add(out);
      tokens.add(out.prepare(supplier(payload), strategy));
    }

    int winner = -1;
    int losses = 0;
    for (int i = 0; i < writers; i++) {
      try {
        outs.get(i).writeAtomic(tokens.get(i), supplier(payloads.get(i)));
        assertThat(winner).as("only one writer may succeed").isEqualTo(-1);
        winner = i;
      } catch (SupportsAtomicOperations.AtomicOperationException expectedFail) {
        assertThat(expectedFail).isInstanceOf(expected);
        losses++;
      }
    }
    assertThat(winner).isNotEqualTo(-1);
    assertThat(losses).isEqualTo(writers - 1);

    if (strategy == AtomicOutputFile.Strategy.APPEND) {
      // appended winner produced "<baseline><payload[winner]>"; we don't make assertions about
      // the prefix here since the baseline content is opaque to runRace.
      // Just verify the live object exists and is at least the winner's length.
      assertThat(io.newInputFile(location).getLength())
          .isGreaterThanOrEqualTo(payloads.get(winner).length);
    } else {
      assertThat(read(io.newInputFile(location))).isEqualTo(payloads.get(winner));
    }
  }

  /**
   * Run {@code writers} writers concurrently against a real executor, rendezvousing at a barrier
   * before each thread invokes {@code writeAtomic}. Exactly one writer must commit; every other
   * must throw the strategy-appropriate exception; the live object must be the winner's: its
   * payload for a CAS, the version it raced on extended by its payload for an APPEND. Each writer
   * carries a distinct random payload so the suite can verify which one actually committed.
   */
  private void runConcurrentRace(
      SupportsAtomicOperations io, String location, int writers, AtomicOutputFile.Strategy strategy)
      throws Exception {
    AtomicOutputFile.Strategy[] strategies = new AtomicOutputFile.Strategy[writers];
    Arrays.fill(strategies, strategy);
    runConcurrentRace(io, location, strategies);
  }

  /** As above, writer {@code i} writing with {@code strategies[i]}. */
  private void runConcurrentRace(
      SupportsAtomicOperations io, String location, AtomicOutputFile.Strategy... strategies)
      throws Exception {
    int writers = strategies.length;
    byte[] baseline = read(io.newInputFile(location));
    Random random = new Random(0xC0FFEE);
    List<byte[]> payloads = new ArrayList<>(writers);
    for (int i = 0; i < writers; i++) {
      // Identical-length payloads. The contract is that exactly one writer commits and the live
      // bytes match that writer's payload, regardless of length collisions. Provider impls are
      // responsible for serializing concurrent writers correctly (e.g. ADLS uses a blob lease over
      // its two-phase append/flush, since neither the position nor the etag check alone prevents
      // an etag-race winner from committing a loser's bytes).
      byte[] payload = new byte[1024];
      random.nextBytes(payload);
      payloads.add(payload);
    }

    CyclicBarrier startGate = new CyclicBarrier(writers);
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      List<Future<Integer>> futures = new ArrayList<>(writers);
      for (int i = 0; i < writers; i++) {
        final int idx = i;
        final byte[] payload = payloads.get(i);
        final AtomicOutputFile.Strategy strategy = strategies[i];
        final Class<? extends SupportsAtomicOperations.AtomicOperationException> expectedFailure =
            strategy == AtomicOutputFile.Strategy.APPEND
                ? SupportsAtomicOperations.AppendException.class
                : SupportsAtomicOperations.CASException.class;
        Callable<Integer> task =
            () -> {
              // Each thread builds its own FileIO (and underlying SDK client) so the race
              // doesn't share client-side state — observable conflicts must come from the
              // backing store, not from cached fields, shared connection state, or block
              // buffers in the client library.
              SupportsAtomicOperations threadIo = newFileIO();
              InputFile snap = threadIo.newInputFile(location);
              // every writer sees the baseline they race to replace or extend
              snap.getLength();
              AtomicOutputFile out = threadIo.newOutputFile(snap);
              CAS tok = out.prepare(supplier(payload), strategy);
              startGate.await(30, TimeUnit.SECONDS);
              try {
                out.writeAtomic(tok, supplier(payload));
                return idx;
              } catch (SupportsAtomicOperations.AtomicOperationException e) {
                assertThat(e).isInstanceOf(expectedFailure);
                return -1;
              }
            };
        futures.add(pool.submit(task));
      }

      int winner = -1;
      int losses = 0;
      for (Future<Integer> f : futures) {
        int result = f.get(60, TimeUnit.SECONDS);
        if (result >= 0) {
          assertThat(winner).as("only one writer may succeed").isEqualTo(-1);
          winner = result;
        } else {
          losses++;
        }
      }
      assertThat(winner).isNotEqualTo(-1);
      assertThat(losses).isEqualTo(writers - 1);

      // The live object is the winner's: the baseline extended by its payload, or its payload
      assertThat(read(io.newInputFile(location)))
          .isEqualTo(
              strategies[winner] == AtomicOutputFile.Strategy.APPEND
                  ? concat(baseline, payloads.get(winner))
                  : payloads.get(winner));
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  // ─── Tiny helpers ───────────────────────────────────────────────────────────────────────────

  protected static InputFile createAtomically(
      SupportsAtomicOperations io, String location, byte[] payload) throws IOException {
    AtomicOutputFile out = io.newOutputFile(io.newInputFile(location));
    CAS tok = out.prepare(supplier(payload), AtomicOutputFile.Strategy.CAS);
    return out.writeAtomic(tok, supplier(payload));
  }

  protected static InputFile appendAtomically(
      SupportsAtomicOperations io, InputFile prior, byte[] payload) throws IOException {
    AtomicOutputFile out = io.newOutputFile(prior);
    CAS tok = out.prepare(supplier(payload), AtomicOutputFile.Strategy.APPEND);
    return out.writeAtomic(tok, supplier(payload));
  }

  protected static byte[] read(InputFile in) throws IOException {
    try (InputStream s = in.newStream()) {
      return s.readAllBytes();
    }
  }

  protected static String randomSlug(String prefix) {
    return prefix + "-" + UUID.randomUUID();
  }

  private static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] concat(byte[] a, byte[] b) {
    byte[] out = new byte[a.length + b.length];
    System.arraycopy(a, 0, out, 0, a.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }

  private static byte[] filled(int length, byte value) {
    byte[] out = new byte[length];
    Arrays.fill(out, value);
    return out;
  }

  private static byte[] padTo(byte[] src, int length) {
    if (src.length == length) {
      return src;
    }
    byte[] out = new byte[length];
    System.arraycopy(src, 0, out, 0, Math.min(src.length, length));
    for (int i = src.length; i < length; i++) {
      out[i] = '.';
    }
    return out;
  }

  private static Supplier<InputStream> supplier(byte[] data) {
    return () -> new ByteArrayInputStream(data);
  }

  /** A stream over {@code data} that fails after {@code okBytes}, like a writer dying mid-write. */
  private static InputStream failingAfter(byte[] data, int okBytes) {
    return new InputStream() {
      private int pos = 0;

      @Override
      public int read() throws IOException {
        if (pos >= okBytes) {
          throw new IOException("writer failed mid-write");
        }
        return data[pos++] & 0xFF;
      }

      @Override
      public int read(byte[] buf, int off, int len) throws IOException {
        if (pos >= okBytes) {
          throw new IOException("writer failed mid-write");
        }
        int n = Math.min(len, okBytes - pos);
        System.arraycopy(data, pos, buf, off, n);
        pos += n;
        return n;
      }
    };
  }
}
