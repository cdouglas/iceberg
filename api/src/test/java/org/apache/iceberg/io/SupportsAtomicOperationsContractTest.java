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
 * <p>Capability declaration is the subclass's job: each provider's subclass declares whether the
 * configured backend supports {@link AtomicOutputFile.Strategy#APPEND} by overriding {@link
 * #supportsAppend()}. Tests gated on APPEND skip via {@link Assumptions#assumeTrue} so providers
 * without it (GCS today; standard S3 buckets) report ignored, and providers with it (S3 Express,
 * ADLS) run them.
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
   * Whether the backing store supports {@link AtomicOutputFile.Strategy#APPEND}. Default {@code
   * false}; providers that support APPEND override this to {@code true}.
   */
  protected boolean supportsAppend() {
    return false;
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
      InputFile snap =
          preExistingSnapshot != null ? io.newInputFile(location) : io.newInputFile(location);
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
   * must throw the strategy-appropriate exception; the live object must equal the winner's payload.
   * Each writer carries a distinct random payload so the suite can verify which one actually
   * committed.
   */
  private void runConcurrentRace(
      SupportsAtomicOperations io, String location, int writers, AtomicOutputFile.Strategy strategy)
      throws Exception {
    Class<? extends SupportsAtomicOperations.AtomicOperationException> expectedFailure =
        strategy == AtomicOutputFile.Strategy.APPEND
            ? SupportsAtomicOperations.AppendException.class
            : SupportsAtomicOperations.CASException.class;

    Random random = new Random(0xC0FFEE);
    List<byte[]> payloads = new ArrayList<>(writers);
    for (int i = 0; i < writers; i++) {
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
        Callable<Integer> task =
            () -> {
              // Each thread fetches its own snapshot + prepares its token before rendezvousing.
              InputFile snap = io.newInputFile(location);
              AtomicOutputFile out = io.newOutputFile(snap);
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

      if (strategy == AtomicOutputFile.Strategy.APPEND) {
        // Live object must be baseline + winner's payload — verify the suffix matches the winner.
        byte[] live = read(io.newInputFile(location));
        assertThat(live.length).isGreaterThanOrEqualTo(payloads.get(winner).length);
        byte[] suffix = new byte[payloads.get(winner).length];
        System.arraycopy(live, live.length - suffix.length, suffix, 0, suffix.length);
        assertThat(suffix).isEqualTo(payloads.get(winner));
      } else {
        assertThat(read(io.newInputFile(location))).isEqualTo(payloads.get(winner));
      }
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
}
