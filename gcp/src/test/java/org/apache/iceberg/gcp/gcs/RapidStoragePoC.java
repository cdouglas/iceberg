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
package org.apache.iceberg.gcp.gcs;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobAppendableUploadConfig.CloseAction;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Empirical probes against a real GCS Rapid Storage (zonal) bucket. The published docs are thin on
 * the per-RPC preconditions of the appendable-object API, so this class runs concrete experiments
 * to learn:
 *
 * <ul>
 *   <li>does the SDK expose {@code blobAppendableUpload} cleanly,
 *   <li>does {@code BlobWriteOption.generationMatch(g)} fence stale openers,
 *   <li>what happens when two writers race on the same object,
 *   <li>can a finalized object be CAS-overwritten via the existing {@code BlobWriteSession} path.
 * </ul>
 *
 * <p>Skips entirely when {@code RAPID_BUCKET} is unset, so it is safe to land before a bucket
 * exists. When run, each probe prints a single-line {@code [poc.<probe>]} report so the answers are
 * visible in the test log without having to reread the assertions.
 *
 * <p>Run:
 *
 * <pre>{@code
 * RAPID_BUCKET=my-zonal-bucket \
 *   ./gradlew :iceberg-gcp:test \
 *   --tests org.apache.iceberg.gcp.gcs.RapidStoragePoC \
 *   -x generateGitProperties
 * }</pre>
 */
public class RapidStoragePoC {

  private static final String BUCKET_ENV = "RAPID_BUCKET";
  private static final String PROJECT_ENV = "GOOGLE_CLOUD_PROJECT";
  private static final String SUITE_PREFIX = "iceberg-rapid-poc/" + UUID.randomUUID() + "/";

  private static String bucket;
  private static Storage storage;
  private final List<BlobId> created = new ArrayList<>();

  @BeforeAll
  static void setupClass() {
    bucket = System.getenv(BUCKET_ENV);
    if (bucket == null || bucket.isEmpty()) {
      return;
    }
    StorageOptions.Builder builder = StorageOptions.grpc();
    String project = System.getenv(PROJECT_ENV);
    if (project != null && !project.isEmpty()) {
      builder.setProjectId(project);
    }
    storage = builder.build().getService();
    System.out.println(
        "[poc.setup] storage="
            + storage.getClass().getSimpleName()
            + " bucket="
            + bucket
            + " prefix="
            + SUITE_PREFIX);
  }

  @AfterAll
  static void teardownClass() throws Exception {
    if (storage != null) {
      storage.close();
    }
  }

  @AfterEach
  void cleanupObjects() {
    if (storage == null) {
      return;
    }
    for (BlobId id : created) {
      try {
        storage.delete(id);
      } catch (RuntimeException ignored) {
        // best-effort
      }
    }
    created.clear();
  }

  // ─── Probe 1: API discovery ─────────────────────────────────────────────────────────────────

  @Test
  void probe01ApiDiscovery() throws IOException {
    requireBucket();
    BlobId id = freshKey("api-discovery");
    BlobInfo info = BlobInfo.newBuilder(id).build();
    BlobAppendableUpload up =
        storage.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING),
            BlobWriteOption.doesNotExist());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up.open()) {
      ch.write(ByteBuffer.wrap(bytes("api-discovery")));
      ch.finalizeAndClose();
    }
    BlobInfo result = unwrap(up);
    System.out.println(
        "[poc.01] uploadType="
            + up.getClass().getName()
            + " channelType="
            + BlobAppendableUpload.AppendableUploadWriteableByteChannel.class.getName()
            + " resultGen="
            + result.getGeneration());
    assertThat(result.getGeneration()).isPositive();
  }

  // ─── Probe 2: happy-path append (single writer, two flushes, finalize) ─────────────────────

  @Test
  void probe02HappyPathAppend() throws IOException {
    requireBucket();
    BlobId id = freshKey("happy");
    BlobInfo info = BlobInfo.newBuilder(id).build();
    BlobAppendableUploadConfig cfg =
        BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING);
    BlobAppendableUpload up =
        storage.blobAppendableUpload(info, cfg, BlobWriteOption.doesNotExist());

    byte[] head = bytes("head");
    byte[] tail = bytes("tail");
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up.open()) {
      ch.write(ByteBuffer.wrap(head));
      ch.flush();
      ch.write(ByteBuffer.wrap(tail));
      ch.finalizeAndClose();
    }
    BlobInfo result = unwrap(up);
    byte[] readBack = readObject(id);
    System.out.println("[poc.02] gen=" + result.getGeneration() + " size=" + readBack.length);
    assertThat(readBack).isEqualTo(concat(head, tail));
  }

  // ─── Probe 3: generation behavior across appends ────────────────────────────────────────────

  @Test
  void probe03GenerationAcrossAppends() throws IOException {
    requireBucket();
    BlobId id = freshKey("gen");
    BlobInfo info = BlobInfo.newBuilder(id).build();

    // First session: open, flush, close-without-finalize so the object remains appendable.
    BlobAppendableUploadConfig cfgKeepOpen =
        BlobAppendableUploadConfig.of().withCloseAction(CloseAction.CLOSE_WITHOUT_FINALIZING);
    BlobAppendableUpload up1 =
        storage.blobAppendableUpload(info, cfgKeepOpen, BlobWriteOption.doesNotExist());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up1.open()) {
      ch.write(ByteBuffer.wrap(bytes("aaaa")));
      ch.flush();
    }
    BlobInfo afterFirst = unwrap(up1);
    long genAfterFirst = afterFirst.getGeneration();

    // Take over via second session. Pass the generation on the BlobId itself so the SDK uses
    // TakeoverAppendableUploadState. Mirror the catalog's actual use-case: append + flush, no
    // finalize. minFlushSize(0) forces each write through to the server so the SDK's buffered-
    // byte counter doesn't drift from the takeover state's expected offset (a 2.68.0 quirk).
    BlobInfo takeoverInfo =
        BlobInfo.newBuilder(BlobId.of(id.getBucket(), id.getName(), genAfterFirst)).build();
    BlobAppendableUploadConfig cfgKeepOpenAgain =
        BlobAppendableUploadConfig.of()
            .withCloseAction(CloseAction.CLOSE_WITHOUT_FINALIZING)
            .withFlushPolicy(com.google.cloud.storage.FlushPolicy.minFlushSize(0));
    BlobAppendableUpload up2 = storage.blobAppendableUpload(takeoverInfo, cfgKeepOpenAgain);
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up2.open()) {
      ch.write(ByteBuffer.wrap(bytes("bbbb")));
      ch.flush();
    }
    long genAfterSecond = storage.get(id).getGeneration();

    System.out.println(
        "[poc.03] genAfterFirstFlush="
            + genAfterFirst
            + " genAfterSecondFlush="
            + genAfterSecond
            + " stable="
            + (genAfterFirst == genAfterSecond));
    assertThat(readObject(id)).isEqualTo(bytes("aaaabbbb"));
  }

  // ─── Probe 4: concurrent-stream takeover semantics ─────────────────────────────────────────

  @Test
  void probe04ConcurrentStreamTakeover() throws IOException {
    requireBucket();
    BlobId id = freshKey("takeover");
    BlobInfo info = BlobInfo.newBuilder(id).build();
    BlobAppendableUploadConfig cfgKeepOpen =
        BlobAppendableUploadConfig.of().withCloseAction(CloseAction.CLOSE_WITHOUT_FINALIZING);

    // Writer A: open, flush, leave channel open. Read the live generation directly from the
    // bucket — calling unwrap(a) here would block until A's stream resolves.
    BlobAppendableUpload a =
        storage.blobAppendableUpload(info, cfgKeepOpen, BlobWriteOption.doesNotExist());
    BlobAppendableUpload.AppendableUploadWriteableByteChannel chA = a.open();
    chA.write(ByteBuffer.wrap(bytes("AAAA")));
    chA.flush();
    long genA = storage.get(id).getGeneration();

    // Writer B: open the same object as a takeover (generation on BlobId) while A's channel is
    // still open. We expect the server to fence one of them.
    String bOutcome;
    BlobInfo takeoverInfo =
        BlobInfo.newBuilder(BlobId.of(id.getBucket(), id.getName(), genA)).build();
    try {
      BlobAppendableUpload b = storage.blobAppendableUpload(takeoverInfo, cfgKeepOpen);
      try (BlobAppendableUpload.AppendableUploadWriteableByteChannel chB = b.open()) {
        chB.write(ByteBuffer.wrap(bytes("BBBB")));
        chB.flush();
      }
      bOutcome = "B opened+flushed";
    } catch (Exception e) {
      bOutcome = "B threw " + classify(e);
    }

    // Now poke writer A to learn whether B fenced it.
    String aOutcome;
    try {
      chA.write(ByteBuffer.wrap(bytes("AAAA-2")));
      chA.flush();
      aOutcome = "A still writeable";
    } catch (Exception e) {
      aOutcome = "A fenced: " + classify(e);
    } finally {
      try {
        chA.closeWithoutFinalizing();
      } catch (Exception ignored) {
        // already errored
      }
    }

    System.out.println("[poc.04] genA=" + genA + " | " + bOutcome + " | " + aOutcome);
  }

  // ─── Probe 5: ifGenerationMatch on stream open with a stale generation ─────────────────────

  @Test
  void probe05IfGenerationMatchStale() throws IOException {
    requireBucket();
    BlobId id = freshKey("stale-gen");
    BlobInfo info = BlobInfo.newBuilder(id).build();

    // Round 1: create + finalize, capture original gen.
    BlobAppendableUpload r1 =
        storage.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING),
            BlobWriteOption.doesNotExist());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = r1.open()) {
      ch.write(ByteBuffer.wrap(bytes("v1")));
      ch.finalizeAndClose();
    }
    long staleGen = unwrap(r1).getGeneration();

    // Round 2: replace via a fresh appendable upload pinned to staleGen (Rapid's CAS-replace).
    BlobAppendableUpload r2 =
        storage.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING),
            BlobWriteOption.generationMatch(staleGen));
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = r2.open()) {
      ch.write(ByteBuffer.wrap(bytes("v2-overwrite")));
      ch.finalizeAndClose();
    }
    long newGen = unwrap(r2).getGeneration();

    // Round 3: another writer pinned to the now-stale gen — must fail.
    String outcome;
    try {
      BlobAppendableUpload r3 =
          storage.blobAppendableUpload(
              info,
              BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING),
              BlobWriteOption.generationMatch(staleGen));
      try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = r3.open()) {
        ch.write(ByteBuffer.wrap(bytes("ghost")));
        ch.finalizeAndClose();
      }
      outcome = "stale open succeeded (BUG for our purposes)";
    } catch (Exception e) {
      outcome = "stale open rejected: " + classify(e);
    }

    System.out.println(
        "[poc.05] staleGen=" + staleGen + " newGen=" + newGen + " outcome=" + outcome);
  }

  // ─── Probe 6: offset / data integrity after takeover ───────────────────────────────────────

  @Test
  void probe06ResumeOffsetIntegrity() throws IOException {
    requireBucket();
    BlobId id = freshKey("resume");
    BlobInfo info = BlobInfo.newBuilder(id).build();

    // Initial write, no finalize.
    BlobAppendableUpload up1 =
        storage.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.CLOSE_WITHOUT_FINALIZING),
            BlobWriteOption.doesNotExist());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up1.open()) {
      ch.write(ByteBuffer.wrap(bytes("XXXXX"))); // 5 bytes
      ch.flush();
    }
    long gen1 = unwrap(up1).getGeneration();
    long sizeAfter1 = readObject(id).length;

    // Takeover via generation on BlobId, append + flush + close-without-finalizing (mirrors
    // catalog's actual append path). minFlushSize(0) forces each write to go to the server so
    // the SDK's takeover state machine doesn't trip its own (totalLength == totalSentBytes)
    // assertion when the buffered bytes haven't been sent yet at close time.
    BlobInfo takeoverInfo =
        BlobInfo.newBuilder(BlobId.of(id.getBucket(), id.getName(), gen1)).build();
    BlobAppendableUpload up2 =
        storage.blobAppendableUpload(
            takeoverInfo,
            BlobAppendableUploadConfig.of()
                .withCloseAction(CloseAction.CLOSE_WITHOUT_FINALIZING)
                .withFlushPolicy(com.google.cloud.storage.FlushPolicy.minFlushSize(0)));
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up2.open()) {
      ch.write(ByteBuffer.wrap(bytes("YYYYY"))); // 5 more bytes
      ch.flush();
    }
    byte[] finalBytes = readObject(id);
    System.out.println(
        "[poc.06] sizeAfter1="
            + sizeAfter1
            + " finalSize="
            + finalBytes.length
            + " final=\""
            + new String(finalBytes, StandardCharsets.UTF_8)
            + "\"");
    // Expect "XXXXXYYYYY" if takeover resumes from the persisted tail; surprising otherwise.
    assertThat(finalBytes).isEqualTo(bytes("XXXXXYYYYY"));
  }

  // ─── Probe 7: finalize → CAS overwrite ────────────────────────────────────────────────────

  @Test
  void probe07FinalizeThenCAS() throws IOException {
    requireBucket();
    BlobId id = freshKey("finalize-then-cas");
    BlobInfo info = BlobInfo.newBuilder(id).build();

    BlobAppendableUpload up =
        storage.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING),
            BlobWriteOption.doesNotExist());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up.open()) {
      ch.write(ByteBuffer.wrap(bytes("appended")));
      ch.finalizeAndClose();
    }
    long finalizedGen = unwrap(up).getGeneration();

    // CAS-replace via a fresh appendable upload pinned to finalizedGen.
    BlobAppendableUpload r2 =
        storage.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING),
            BlobWriteOption.generationMatch(finalizedGen));
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = r2.open()) {
      ch.write(ByteBuffer.wrap(bytes("replaced-via-cas")));
      ch.finalizeAndClose();
    }
    long replacedGen = unwrap(r2).getGeneration();

    // A retry pinned to the now-stale finalizedGen must fail.
    String staleRetry;
    try {
      BlobAppendableUpload retry =
          storage.blobAppendableUpload(
              info,
              BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING),
              BlobWriteOption.generationMatch(finalizedGen));
      try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = retry.open()) {
        ch.write(ByteBuffer.wrap(bytes("ghost")));
        ch.finalizeAndClose();
      }
      staleRetry = "stale retry SUCCEEDED (BUG)";
    } catch (Exception e) {
      staleRetry = "stale retry rejected: " + classify(e);
    }

    System.out.println(
        "[poc.07] finalizedGen="
            + finalizedGen
            + " replacedGen="
            + replacedGen
            + " | "
            + staleRetry);
    assertThat(readObject(id)).isEqualTo(bytes("replaced-via-cas"));
  }

  // ─── Probe 8: race harness (N concurrent appenders pinned to the same generation) ─────────

  @Test
  void probe08AppendRace() throws Exception {
    requireBucket();
    BlobId id = freshKey("race");
    BlobInfo info = BlobInfo.newBuilder(id).build();

    // Establish baseline + capture its generation.
    BlobAppendableUpload baseline =
        storage.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.CLOSE_WITHOUT_FINALIZING),
            BlobWriteOption.doesNotExist());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = baseline.open()) {
      ch.write(ByteBuffer.wrap(bytes("BASE-")));
      ch.flush();
    }
    long baseGen = unwrap(baseline).getGeneration();

    int writers = 4;
    CyclicBarrier gate = new CyclicBarrier(writers);
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      List<Future<String>> futures = new ArrayList<>(writers);
      for (int i = 0; i < writers; i++) {
        final int idx = i;
        Callable<String> task =
            () -> {
              // Each thread builds its own Storage client so the race relies on backend
              // serialization, not in-process state.
              try (Storage threadStorage = StorageOptions.grpc().build().getService()) {
                BlobAppendableUpload up =
                    threadStorage.blobAppendableUpload(
                        info,
                        BlobAppendableUploadConfig.of()
                            .withCloseAction(CloseAction.CLOSE_WITHOUT_FINALIZING),
                        BlobWriteOption.generationMatch(baseGen));
                gate.await(30, TimeUnit.SECONDS);
                try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up.open()) {
                  ch.write(ByteBuffer.wrap(bytes("W" + idx)));
                  ch.flush();
                }
                return "writer" + idx + ":WIN gen=" + unwrap(up).getGeneration();
              } catch (Exception e) {
                return "writer" + idx + ":LOSE " + classify(e);
              }
            };
        futures.add(pool.submit(task));
      }

      List<String> outcomes = new ArrayList<>();
      int wins = 0;
      for (Future<String> f : futures) {
        String r = f.get(60, TimeUnit.SECONDS);
        outcomes.add(r);
        if (r.contains(":WIN")) {
          wins++;
        }
      }
      System.out.println("[poc.08] wins=" + wins + " outcomes=" + outcomes);
      // We do NOT assert a single winner here: the point of the probe is to learn what GCS
      // actually does. The result classifies the API's contention model.
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  // ─── Probe 9: extract write_handle / routing_token from session 1 via package backdoor ────

  @Test
  void probe09CaptureWriteHandle() throws Exception {
    requireBucket();
    BlobId id = freshKey("capture");
    BlobInfo info = BlobInfo.newBuilder(id).build();

    BlobAppendableUpload up =
        storage.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.CLOSE_WITHOUT_FINALIZING),
            BlobWriteOption.doesNotExist());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up.open()) {
      ch.write(ByteBuffer.wrap(bytes("session1-")));
      ch.flush();
    }

    com.google.cloud.storage.RapidStorageBackdoor.CapturedState captured =
        com.google.cloud.storage.RapidStorageBackdoor.capture(up);
    System.out.println("[poc.09] captured: " + captured);
    // We don't assert that writeHandle != null — the point is to learn what the SDK has stashed.
    // If the field is populated, probe 10 will plumb it into a takeover.
    assertThat(captured.generation).isPositive();
  }

  // ─── Probe 10: raw-gRPC takeover with the captured write_handle / routing_token ───────────

  @Test
  void probe10RawGrpcTakeover() throws Exception {
    requireBucket();
    BlobId id = freshKey("rawgrpc");
    BlobInfo info = BlobInfo.newBuilder(id).build();

    // Session 1 — establish the appendable, leave it unfinalized, capture state.
    BlobAppendableUpload up1 =
        storage.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.CLOSE_WITHOUT_FINALIZING),
            BlobWriteOption.doesNotExist());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up1.open()) {
      ch.write(ByteBuffer.wrap(bytes("AAAA")));
      ch.flush();
    }
    com.google.cloud.storage.RapidStorageBackdoor.CapturedState captured =
        com.google.cloud.storage.RapidStorageBackdoor.capture(up1);
    System.out.println("[poc.10] sess1 captured: " + captured);

    // Session 2 — raw bidi gRPC call carrying the captured write_handle / routing_token.
    com.google.storage.v2.StorageClient client =
        com.google.cloud.storage.RapidStorageBackdoor.storageClient(storage);

    com.google.storage.v2.AppendObjectSpec.Builder specBuilder =
        com.google.storage.v2.AppendObjectSpec.newBuilder()
            .setBucket("projects/_/buckets/" + id.getBucket())
            .setObject(id.getName())
            .setGeneration(captured.generation);
    if (captured.writeHandle != null) {
      specBuilder.setWriteHandle(captured.writeHandle);
    }
    if (captured.routingToken != null) {
      specBuilder.setRoutingToken(captured.routingToken);
    }

    byte[] payload = bytes("BBBB");
    com.google.storage.v2.ChecksummedData data =
        com.google.storage.v2.ChecksummedData.newBuilder()
            .setContent(com.google.protobuf.ByteString.copyFrom(payload))
            .build();

    com.google.storage.v2.BidiWriteObjectRequest first =
        com.google.storage.v2.BidiWriteObjectRequest.newBuilder()
            .setAppendObjectSpec(specBuilder.build())
            .setWriteOffset(captured.confirmedBytes)
            .setChecksummedData(data)
            .setFlush(true)
            .setStateLookup(true)
            .build();

    // Routing header: rather than build a fresh context (which lacks any redirect-driven routing
    // info the server may have set during session 1), reuse the GrpcCallContext the SDK was
    // actively using when session 1 closed.
    com.google.api.gax.grpc.GrpcCallContext callCtx = captured.lastCallContext;
    if (callCtx == null) {
      callCtx =
          com.google.api.gax.grpc.GrpcCallContext.createDefault()
              .withExtraHeaders(
                  java.util.Collections.singletonMap(
                      "x-goog-request-params",
                      java.util.Collections.singletonList(
                          "bucket=projects/_/buckets/" + id.getBucket())));
    }
    com.google.api.gax.rpc.BidiStream<
            com.google.storage.v2.BidiWriteObjectRequest,
            com.google.storage.v2.BidiWriteObjectResponse>
        stream = client.bidiWriteObjectCallable().call(callCtx);
    String outcome;
    try {
      stream.send(first);
      stream.closeSend();
      // Drain responses; we expect at least one ack with persisted_size >= confirmedBytes + 4.
      java.util.List<com.google.storage.v2.BidiWriteObjectResponse> responses =
          new java.util.ArrayList<>();
      for (com.google.storage.v2.BidiWriteObjectResponse resp : stream) {
        responses.add(resp);
      }
      outcome = "ok responses=" + responses.size();
      for (com.google.storage.v2.BidiWriteObjectResponse r : responses) {
        outcome +=
            " persistedSize="
                + (r.hasPersistedSize() ? r.getPersistedSize() : -1)
                + " hasResource="
                + r.hasResource();
      }
    } catch (Exception e) {
      outcome = "rejected: " + classify(e);
    }

    byte[] finalBytes = readObject(id);
    System.out.println(
        "[poc.10] takeover result: "
            + outcome
            + " | final size="
            + finalBytes.length
            + " final=\""
            + new String(finalBytes, StandardCharsets.UTF_8)
            + "\"");
  }

  // ─── Probe 11: concurrent takeover with shared write_handle ────────────────────────────────

  @Test
  void probe11ConcurrentTakeoverRace() throws Exception {
    requireBucket();
    BlobId id = freshKey("race-takeover");
    BlobInfo info = BlobInfo.newBuilder(id).build();

    BlobAppendableUpload up1 =
        storage.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.CLOSE_WITHOUT_FINALIZING),
            BlobWriteOption.doesNotExist());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up1.open()) {
      ch.write(ByteBuffer.wrap(bytes("BASE")));
      ch.flush();
    }
    com.google.cloud.storage.RapidStorageBackdoor.CapturedState captured =
        com.google.cloud.storage.RapidStorageBackdoor.capture(up1);
    System.out.println("[poc.11] sess1 captured: " + captured);

    com.google.storage.v2.StorageClient client =
        com.google.cloud.storage.RapidStorageBackdoor.storageClient(storage);

    int writers = 2;
    CyclicBarrier gate = new CyclicBarrier(writers);
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      List<Future<String>> futures = new ArrayList<>(writers);
      for (int i = 0; i < writers; i++) {
        final int idx = i;
        Callable<String> task =
            () -> {
              try {
                gate.await(30, TimeUnit.SECONDS);
                return rawTakeoverAppend(
                    client,
                    id,
                    captured,
                    bytes("W" + idx + "ZZ"),
                    /* finalize */ false,
                    "writer" + idx);
              } catch (Exception e) {
                return "writer" + idx + ":throw " + classify(e);
              }
            };
        futures.add(pool.submit(task));
      }

      List<String> outcomes = new ArrayList<>();
      int wins = 0;
      for (Future<String> f : futures) {
        String r = f.get(60, TimeUnit.SECONDS);
        outcomes.add(r);
        if (r.contains(":WIN")) {
          wins++;
        }
      }
      byte[] finalBytes = readObject(id);
      System.out.println(
          "[poc.11] wins="
              + wins
              + " outcomes="
              + outcomes
              + " | final size="
              + finalBytes.length
              + " final=\""
              + new String(finalBytes, StandardCharsets.UTF_8)
              + "\"");
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  // ─── Probe 12: write_handle TTL after sleep ────────────────────────────────────────────────

  @Test
  void probe12WriteHandleTtl() throws Exception {
    requireBucket();
    int sleepSeconds = 60;
    String envSleep = System.getenv("POC_TTL_SLEEP_SECONDS");
    if (envSleep != null && !envSleep.isEmpty()) {
      sleepSeconds = Integer.parseInt(envSleep);
    }

    BlobId id = freshKey("ttl");
    BlobInfo info = BlobInfo.newBuilder(id).build();

    BlobAppendableUpload up1 =
        storage.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.CLOSE_WITHOUT_FINALIZING),
            BlobWriteOption.doesNotExist());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up1.open()) {
      ch.write(ByteBuffer.wrap(bytes("HEAD")));
      ch.flush();
    }
    com.google.cloud.storage.RapidStorageBackdoor.CapturedState captured =
        com.google.cloud.storage.RapidStorageBackdoor.capture(up1);
    long captureTime = System.currentTimeMillis();
    System.out.println("[poc.12] captured at t=0s (sleeping " + sleepSeconds + "s): " + captured);

    Thread.sleep(sleepSeconds * 1000L);

    long actualSlept = (System.currentTimeMillis() - captureTime) / 1000;
    com.google.storage.v2.StorageClient client =
        com.google.cloud.storage.RapidStorageBackdoor.storageClient(storage);
    String outcome =
        rawTakeoverAppend(client, id, captured, bytes("TAIL"), /* finalize */ false, "ttl-attempt");
    byte[] finalBytes = readObject(id);
    System.out.println(
        "[poc.12] after "
            + actualSlept
            + "s: "
            + outcome
            + " | final size="
            + finalBytes.length
            + " final=\""
            + new String(finalBytes, StandardCharsets.UTF_8)
            + "\"");
  }

  // ─── Probe 13: fresh-stream contention without write_handle ────────────────────────────────

  /**
   * Probe 13a: does AppendObjectSpec without a write_handle even work? This is the prerequisite for
   * path 1 in the plan — fresh-stream-per-commit fencing. If the server requires a handle, the
   * whole approach is blocked.
   */
  @Test
  void probe13aFreshStreamSerial() throws Exception {
    requireBucket();
    BlobId id = freshKey("fresh-serial");
    BlobInfo info = BlobInfo.newBuilder(id).build();

    BlobAppendableUpload up1 =
        storage.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.CLOSE_WITHOUT_FINALIZING),
            BlobWriteOption.doesNotExist());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up1.open()) {
      ch.write(ByteBuffer.wrap(bytes("BASE")));
      ch.flush();
    }
    com.google.cloud.storage.RapidStorageBackdoor.CapturedState captured =
        com.google.cloud.storage.RapidStorageBackdoor.capture(up1);
    com.google.storage.v2.StorageClient client =
        com.google.cloud.storage.RapidStorageBackdoor.storageClient(storage);

    String outcome =
        rawTakeoverAppend(
            client,
            id,
            captured,
            bytes("TAIL"), /* finalize */
            false,
            "fresh", /* omitHandle */
            true);
    byte[] finalBytes = readObject(id);
    System.out.println(
        "[poc.13a] no-handle outcome="
            + outcome
            + " | final size="
            + finalBytes.length
            + " final=\""
            + new String(finalBytes, StandardCharsets.UTF_8)
            + "\"");
  }

  /**
   * Probe 13b: two concurrent fresh-stream takeovers (no write_handle). If 13a worked, do these two
   * threads fence each other? Goal: prove or refute that "drop the handle, get fencing back."
   */
  @Test
  void probe13bFreshStreamRace() throws Exception {
    requireBucket();
    BlobId id = freshKey("fresh-race");
    BlobInfo info = BlobInfo.newBuilder(id).build();

    BlobAppendableUpload up1 =
        storage.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.CLOSE_WITHOUT_FINALIZING),
            BlobWriteOption.doesNotExist());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up1.open()) {
      ch.write(ByteBuffer.wrap(bytes("BASE")));
      ch.flush();
    }
    com.google.cloud.storage.RapidStorageBackdoor.CapturedState captured =
        com.google.cloud.storage.RapidStorageBackdoor.capture(up1);
    com.google.storage.v2.StorageClient client =
        com.google.cloud.storage.RapidStorageBackdoor.storageClient(storage);

    int writers = 2;
    CyclicBarrier gate = new CyclicBarrier(writers);
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      List<Future<String>> futures = new ArrayList<>(writers);
      for (int i = 0; i < writers; i++) {
        final int idx = i;
        Callable<String> task =
            () -> {
              try {
                gate.await(30, TimeUnit.SECONDS);
                return rawTakeoverAppend(
                    client,
                    id,
                    captured,
                    bytes("W" + idx + "ZZ"),
                    /* finalize */ false,
                    "writer" + idx,
                    /* omitHandle */ true);
              } catch (Exception e) {
                return "writer" + idx + ":throw " + classify(e);
              }
            };
        futures.add(pool.submit(task));
      }
      List<String> outcomes = new ArrayList<>();
      int wins = 0;
      for (Future<String> f : futures) {
        String r = f.get(60, TimeUnit.SECONDS);
        outcomes.add(r);
        if (r.contains(":WIN")) {
          wins++;
        }
      }
      // Probe whether the WIN'd bytes are durable, or whether they vanish (or the read view
      // catches up over time). Read immediately, again at +500ms, again at +5s.
      byte[] readImmediate = readObject(id);
      Thread.sleep(500);
      byte[] read500ms = readObject(id);
      Thread.sleep(4500);
      byte[] read5s = readObject(id);
      long getGeneration = storage.get(id).getGeneration();
      long getSize = storage.get(id).getSize();
      System.out.println(
          "[poc.13b] no-handle wins="
              + wins
              + " outcomes="
              + outcomes
              + " | sizeImmediate="
              + readImmediate.length
              + "(\""
              + new String(readImmediate, StandardCharsets.UTF_8)
              + "\")"
              + " size+500ms="
              + read500ms.length
              + " size+5s="
              + read5s.length
              + "(\""
              + new String(read5s, StandardCharsets.UTF_8)
              + "\")"
              + " | meta gen="
              + getGeneration
              + " size="
              + getSize);
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  /**
   * Probe 13c: same as 13b but with {@code state_lookup=false}. Hypothesis: state_lookup may
   * interfere with the server's exclusive-writer arbitration; without it we should see the clean
   * "winner persists, loser FAILED_PRECONDITION" pattern consistently.
   */
  @Test
  void probe13cFreshStreamRaceNoStateLookup() throws Exception {
    requireBucket();
    BlobId id = freshKey("fresh-race-no-lookup");
    BlobInfo info = BlobInfo.newBuilder(id).build();
    BlobAppendableUpload up1 =
        storage.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.CLOSE_WITHOUT_FINALIZING),
            BlobWriteOption.doesNotExist());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up1.open()) {
      ch.write(ByteBuffer.wrap(bytes("BASE")));
      ch.flush();
    }
    com.google.cloud.storage.RapidStorageBackdoor.CapturedState captured =
        com.google.cloud.storage.RapidStorageBackdoor.capture(up1);
    com.google.storage.v2.StorageClient client =
        com.google.cloud.storage.RapidStorageBackdoor.storageClient(storage);

    int writers = 2;
    CyclicBarrier gate = new CyclicBarrier(writers);
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      List<Future<String>> futures = new ArrayList<>(writers);
      for (int i = 0; i < writers; i++) {
        final int idx = i;
        Callable<String> task =
            () -> {
              try {
                gate.await(30, TimeUnit.SECONDS);
                return rawTakeoverAppend(
                    client,
                    id,
                    captured,
                    bytes("W" + idx + "ZZ"),
                    /* finalize */ false,
                    "writer" + idx,
                    /* omitHandle */ true,
                    /* stateLookup */ false);
              } catch (Exception e) {
                return "writer" + idx + ":throw " + classify(e);
              }
            };
        futures.add(pool.submit(task));
      }
      List<String> outcomes = new ArrayList<>();
      int wins = 0;
      for (Future<String> f : futures) {
        String r = f.get(60, TimeUnit.SECONDS);
        outcomes.add(r);
        if (r.contains(":WIN")) {
          wins++;
        }
      }
      Thread.sleep(2000);
      byte[] finalBytes = readObject(id);
      long getSize = storage.get(id).getSize();
      System.out.println(
          "[poc.13c] wins="
              + wins
              + " outcomes="
              + outcomes
              + " | final size="
              + finalBytes.length
              + "(\""
              + new String(finalBytes, StandardCharsets.UTF_8)
              + "\") metaSize="
              + getSize);
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  // ─── Probe 14: does generation advance per flush (not just per finalize)? ──────────────────

  /**
   * Run a sequence of flushes and record the live object generation between each. If generation
   * moves on every flush, then if_generation_match on AppendObjectSpec could be a real fence: each
   * concurrent writer would carry the same pinned generation, and exactly one would win.
   */
  @Test
  void probe14GenerationPerFlush() throws Exception {
    requireBucket();
    BlobId id = freshKey("gen-per-flush");
    BlobInfo info = BlobInfo.newBuilder(id).build();

    BlobAppendableUpload up1 =
        storage.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.CLOSE_WITHOUT_FINALIZING),
            BlobWriteOption.doesNotExist());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up1.open()) {
      ch.write(ByteBuffer.wrap(bytes("AAAA")));
      ch.flush();
    }
    long genAfterFirstFlush = storage.get(id).getGeneration();
    com.google.cloud.storage.RapidStorageBackdoor.CapturedState captured1 =
        com.google.cloud.storage.RapidStorageBackdoor.capture(up1);
    com.google.storage.v2.StorageClient client =
        com.google.cloud.storage.RapidStorageBackdoor.storageClient(storage);

    String r1 =
        rawTakeoverAppend(client, id, captured1, bytes("BBBB"), /* finalize */ false, "second");
    long genAfterSecondFlush = storage.get(id).getGeneration();

    // Recapture: refresh the write_handle / state after the second flush, then do a third.
    // The catalog's commit loop would reread state per-commit anyway.
    BlobAppendableUpload up3 =
        storage.blobAppendableUpload(
            BlobInfo.newBuilder(BlobId.of(id.getBucket(), id.getName(), genAfterSecondFlush))
                .build(),
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.CLOSE_WITHOUT_FINALIZING));
    String captureFromTakeover;
    try {
      // Write 0 bytes, just to get the SDK to issue an open and capture the resulting state.
      try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up3.open()) {
        // no write — just open and close-without-finalizing
      }
      com.google.cloud.storage.RapidStorageBackdoor.CapturedState captured3 =
          com.google.cloud.storage.RapidStorageBackdoor.capture(up3);
      captureFromTakeover = String.valueOf(captured3.generation);
    } catch (Exception e) {
      captureFromTakeover = "<error: " + e.getClass().getSimpleName() + ">";
    }

    System.out.println(
        "[poc.14] genAfterFirstFlush="
            + genAfterFirstFlush
            + " r1="
            + r1
            + " genAfterSecondFlush="
            + genAfterSecondFlush
            + " genFromSdkTakeover="
            + captureFromTakeover
            + " advanced="
            + (genAfterSecondFlush != genAfterFirstFlush));
  }

  /**
   * Issue one takeover-append round-trip via the raw gRPC bidiWriteObjectCallable. Returns a
   * single-line outcome like "writer0:WIN persistedSize=8" or "writer0:LOSE <classified
   * exception>". Used by the concurrent-race, TTL, and fencing probes.
   */
  private String rawTakeoverAppend(
      com.google.storage.v2.StorageClient client,
      BlobId id,
      com.google.cloud.storage.RapidStorageBackdoor.CapturedState captured,
      byte[] payload,
      boolean finalize,
      String label) {
    return rawTakeoverAppend(
        client, id, captured, payload, finalize, label, /* omitHandle */ false);
  }

  private String rawTakeoverAppend(
      com.google.storage.v2.StorageClient client,
      BlobId id,
      com.google.cloud.storage.RapidStorageBackdoor.CapturedState captured,
      byte[] payload,
      boolean finalize,
      String label,
      boolean omitHandle) {
    return rawTakeoverAppend(
        client, id, captured, payload, finalize, label, omitHandle, /* stateLookup */ true);
  }

  private String rawTakeoverAppend(
      com.google.storage.v2.StorageClient client,
      BlobId id,
      com.google.cloud.storage.RapidStorageBackdoor.CapturedState captured,
      byte[] payload,
      boolean finalize,
      String label,
      boolean omitHandle,
      boolean stateLookup) {
    com.google.storage.v2.AppendObjectSpec.Builder specBuilder =
        com.google.storage.v2.AppendObjectSpec.newBuilder()
            .setBucket("projects/_/buckets/" + id.getBucket())
            .setObject(id.getName())
            .setGeneration(captured.generation);
    if (!omitHandle && captured.writeHandle != null) {
      specBuilder.setWriteHandle(captured.writeHandle);
    }
    if (captured.routingToken != null) {
      specBuilder.setRoutingToken(captured.routingToken);
    }
    com.google.storage.v2.ChecksummedData data =
        com.google.storage.v2.ChecksummedData.newBuilder()
            .setContent(com.google.protobuf.ByteString.copyFrom(payload))
            .build();
    com.google.storage.v2.BidiWriteObjectRequest.Builder reqBuilder =
        com.google.storage.v2.BidiWriteObjectRequest.newBuilder()
            .setAppendObjectSpec(specBuilder.build())
            .setWriteOffset(captured.confirmedBytes)
            .setChecksummedData(data)
            .setFlush(true)
            .setFinishWrite(finalize);
    if (stateLookup) {
      reqBuilder.setStateLookup(true);
    }
    com.google.storage.v2.BidiWriteObjectRequest first = reqBuilder.build();
    com.google.api.gax.grpc.GrpcCallContext callCtx = captured.lastCallContext;
    if (callCtx == null) {
      callCtx =
          com.google.api.gax.grpc.GrpcCallContext.createDefault()
              .withExtraHeaders(
                  java.util.Collections.singletonMap(
                      "x-goog-request-params",
                      java.util.Collections.singletonList(
                          "bucket=projects/_/buckets/" + id.getBucket())));
    }
    com.google.api.gax.rpc.BidiStream<
            com.google.storage.v2.BidiWriteObjectRequest,
            com.google.storage.v2.BidiWriteObjectResponse>
        stream = client.bidiWriteObjectCallable().call(callCtx);
    try {
      stream.send(first);
      stream.closeSend();
      long persisted = -1;
      int responses = 0;
      for (com.google.storage.v2.BidiWriteObjectResponse resp : stream) {
        responses++;
        if (resp.hasPersistedSize()) {
          persisted = resp.getPersistedSize();
        }
      }
      return label + ":WIN responses=" + responses + " persistedSize=" + persisted;
    } catch (Exception e) {
      return label + ":LOSE " + classify(e);
    }
  }

  // ─── helpers ───────────────────────────────────────────────────────────────────────────────

  private void requireBucket() {
    Assumptions.assumeTrue(
        storage != null && bucket != null && !bucket.isEmpty(),
        "RAPID_BUCKET unset; skipping Rapid Storage PoC probe");
  }

  private BlobId freshKey(String slug) {
    BlobId id = BlobId.of(bucket, SUITE_PREFIX + slug + "-" + UUID.randomUUID());
    created.add(id);
    return id;
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

  private static BlobInfo unwrap(BlobAppendableUpload up) throws IOException {
    try {
      return up.getResult().get(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted waiting for upload result", e);
    } catch (ExecutionException | TimeoutException e) {
      throw new IOException("failed waiting for upload result", e);
    }
  }

  private byte[] readObject(BlobId id) throws IOException {
    try (ReadableByteChannel ch = storage.reader(id)) {
      java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
      ByteBuffer buf = ByteBuffer.allocate(8192);
      while (ch.read(buf) > 0) {
        buf.flip();
        out.write(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
        buf.clear();
      }
      return out.toByteArray();
    }
  }

  /**
   * Classify a thrown exception in a single-line string. We don't yet know which failure modes the
   * appendable-object API surfaces, so this captures the class name, gRPC status (when present),
   * and message — enough to map each loser to {@code AppendException} once the patterns are clear.
   */
  private static String classify(Throwable t) {
    StringBuilder sb = new StringBuilder();
    sb.append(t.getClass().getSimpleName()).append('(');
    if (t instanceof StorageException) {
      sb.append("code=").append(((StorageException) t).getCode()).append(' ');
    }
    sb.append('"').append(String.valueOf(t.getMessage())).append('"').append(')');
    if (t.getCause() != null && t.getCause() != t) {
      sb.append(" caused-by ").append(classify(t.getCause()));
    }
    return sb.toString();
  }
}
