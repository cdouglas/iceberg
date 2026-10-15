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

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobAppendableUploadConfig.CloseAction;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Stage-0 follow-up probe: can we implement atomic CAS on a zonal Rapid bucket via a
 * stage-and-rewrite pattern instead of writing directly to the target?
 *
 * <p>The matrix probe ({@link GcsCasTransportProbe}) showed that the only zonal-compatible write
 * surface is {@code storage.blobAppendableUpload(... generationMatch ...)}, and that the server
 * silently drops {@code crc32cMatch} on that path. That combination is unsafe: a writer that
 * catches an exception mid-write and falls through to {@code close()} will publish whatever bytes
 * have been written as the new generation, with no server-side integrity check.
 *
 * <p>This probe tests an alternative: stage the new content into a UUID-named temp object, then use
 * {@code storage.copy(...)} with both {@code ifSourceGenerationMatch} and a destination {@code
 * generationMatch} precondition to atomically replace the target. If the rewrite is server-side and
 * atomic, a partial-temp + skipped-rewrite leaves the target unchanged — i.e., exception safety
 * comes from never having mutated the live object in the first place.
 *
 * <p>Blocks:
 *
 * <ul>
 *   <li><b>A</b> — demonstrate the partial-publish bug in direct {@code blobAppendableUpload}.
 *   <li><b>B</b> — stage-and-rewrite: happy path, stale-gen rejection, partial-temp exception
 *       safety, cross-bucket source.
 *   <li><b>C</b> — reader visibility during rewrite (any intermediate state observable?).
 *   <li><b>D</b> — HNS {@code Storage.moveBlob} if the SDK exposes it.
 * </ul>
 *
 * <p><b>Run:</b>
 *
 * <pre>{@code
 * STANDARD_BUCKET=lst-consistency RAPID_BUCKET=lstx-consistency \
 * GOOGLE_CLOUD_PROJECT=lst-consistency \
 *   ./gradlew :iceberg-gcp:test \
 *   --tests org.apache.iceberg.gcp.gcs.GcsCasViaRewriteProbe \
 *   -x generateGitProperties --info
 * }</pre>
 */
public class GcsCasViaRewriteProbe {

  private static final String STANDARD_BUCKET_ENV = "STANDARD_BUCKET";
  private static final String RAPID_BUCKET_ENV = "RAPID_BUCKET";
  private static final String CREDS_ENV = "GOOGLE_APPLICATION_CREDENTIALS";
  private static final String SUITE_PREFIX = "iceberg-cas-rewrite-probe/" + UUID.randomUUID() + "/";

  private static String standardBucket;
  private static String zonalBucket;
  private static Storage httpClient;
  private static Storage grpcClient;
  private static final Map<String, String> RESULTS = new LinkedHashMap<>();

  private final List<BlobId> created = new ArrayList<>();

  // ─── lifecycle ────────────────────────────────────────────────────────────────────────────

  @BeforeAll
  static void setupClass() throws IOException {
    standardBucket = System.getenv(STANDARD_BUCKET_ENV);
    zonalBucket = System.getenv(RAPID_BUCKET_ENV);
    if ((standardBucket == null || standardBucket.isEmpty())
        && (zonalBucket == null || zonalBucket.isEmpty())) {
      System.out.println("[probe.setup] no buckets configured");
      return;
    }
    Credentials creds = loadCredentials();
    StorageOptions.Builder httpBuilder = StorageOptions.http();
    StorageOptions.Builder grpcBuilder = StorageOptions.grpc();
    if (creds != null) {
      httpBuilder.setCredentials(creds);
      grpcBuilder.setCredentials(creds);
    }
    String project = System.getenv("GOOGLE_CLOUD_PROJECT");
    if (project != null && !project.isEmpty()) {
      httpBuilder.setProjectId(project);
      grpcBuilder.setProjectId(project);
    }
    httpClient = httpBuilder.build().getService();
    grpcClient = grpcBuilder.build().getService();
    System.out.println(
        "[probe.setup] standard="
            + standardBucket
            + " zonal="
            + zonalBucket
            + " prefix="
            + SUITE_PREFIX);
  }

  @AfterAll
  static void teardownClass() throws Exception {
    if (httpClient != null) {
      httpClient.close();
    }
    if (grpcClient != null) {
      grpcClient.close();
    }
    report();
  }

  @AfterEach
  void cleanupObjects() {
    if (httpClient == null) {
      return;
    }
    for (BlobId id : created) {
      try {
        httpClient.delete(id);
      } catch (RuntimeException ignored) {
        // best-effort
      }
    }
    created.clear();
  }

  // ─── Block A: partial-publish bug in direct blobAppendableUpload ──────────────────────────

  /** Sanity: full-payload write with FINALIZE_WHEN_CLOSING produces the expected object. */
  @Test
  void a1_appendableFullFinalize() throws Exception {
    requireZonal();
    BlobId target = freshKey(zonalBucket, "a1-full");
    byte[] payload = randomBytes(4096, 1L);
    BlobInfo info = BlobInfo.newBuilder(target).setCrc32c(b64(crc32c(payload))).build();

    BlobAppendableUpload up =
        grpcClient.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING),
            BlobWriteOption.doesNotExist(),
            BlobWriteOption.crc32cMatch());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up.open()) {
      ch.write(ByteBuffer.wrap(payload));
    } // close() finalizes per CloseAction
    long gen = up.getResult().get().getGeneration();
    byte[] actual = grpcClient.readAllBytes(target);
    record(
        "a1.full.finalize",
        "gen=" + gen + " len=" + actual.length + " match=" + sameBytes(payload, actual));
  }

  /**
   * The partial-publish bug: writer wrote half the bytes, then "exception fell through to close",
   * and close finalized the truncated bytes as the new generation. No server-side defense.
   */
  @Test
  void a2_appendablePartialFinalizeOnZonal() throws Exception {
    requireZonal();
    BlobId target = freshKey(zonalBucket, "a2-partial-finalize");

    // Pre-seed target at g1 with "old bytes".
    byte[] oldBytes = randomBytes(4096, 100L);
    long g1 = appendableWriteFull(grpcClient, target, oldBytes, BlobWriteOption.doesNotExist());

    byte[] full = randomBytes(4096, 200L);
    BlobInfo info = BlobInfo.newBuilder(target).setCrc32c(b64(crc32c(full))).build();
    BlobAppendableUpload up =
        grpcClient.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING),
            BlobWriteOption.generationMatch(g1),
            BlobWriteOption.crc32cMatch());

    // Write only half — simulate "writer caught an exception, fell through to close()"
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up.open()) {
      ch.write(ByteBuffer.wrap(full, 0, full.length / 2));
    } // close() finalizes per CloseAction

    long gen = -1;
    String state;
    try {
      gen = up.getResult().get().getGeneration();
      byte[] actual = grpcClient.readAllBytes(target);
      boolean truncated = actual.length == full.length / 2;
      boolean unchanged = sameBytes(actual, oldBytes);
      state =
          "finalized gen="
              + gen
              + " len="
              + actual.length
              + " truncated="
              + truncated
              + " unchanged="
              + unchanged;
    } catch (Exception e) {
      // If the server rejects close-without-full-content, that's actually safe behavior.
      Blob now = grpcClient.get(target);
      state =
          "close failed: "
              + classify(e)
              + " | target now gen="
              + (now == null ? "deleted" : now.getGeneration())
              + " len="
              + (now == null ? -1 : now.getSize());
    }
    record("a2.partial.finalize.zonal", state);
  }

  /** Candidate in-place mitigation: CLOSE_WITHOUT_FINALIZING. Does it leave target untouched? */
  @Test
  void a3_appendablePartialCloseWithoutFinalize() throws Exception {
    requireZonal();
    BlobId target = freshKey(zonalBucket, "a3-partial-noclose");

    byte[] oldBytes = randomBytes(4096, 300L);
    long g1 = appendableWriteFull(grpcClient, target, oldBytes, BlobWriteOption.doesNotExist());

    byte[] full = randomBytes(4096, 400L);
    BlobInfo info = BlobInfo.newBuilder(target).setCrc32c(b64(crc32c(full))).build();
    BlobAppendableUpload up =
        grpcClient.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.CLOSE_WITHOUT_FINALIZING),
            BlobWriteOption.generationMatch(g1),
            BlobWriteOption.crc32cMatch());

    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up.open()) {
      ch.write(ByteBuffer.wrap(full, 0, full.length / 2));
    } // close() does NOT finalize

    Blob nowBlob = grpcClient.get(target);
    long nowGen = nowBlob == null ? -1 : nowBlob.getGeneration();
    long nowSize = nowBlob == null ? -1 : nowBlob.getSize();
    byte[] actual = grpcClient.readAllBytes(target);
    boolean unchanged = sameBytes(actual, oldBytes);
    record(
        "a3.partial.noFinalize.zonal",
        "g1=" + g1 + " nowGen=" + nowGen + " nowSize=" + nowSize + " unchanged=" + unchanged);
  }

  // ─── Block B: stage-and-rewrite ───────────────────────────────────────────────────────────

  /** Happy path: upload temp via appendable+finalize, copy onto zonal target with both gen pins. */
  @Test
  void b1_rewriteZonalHappy() throws Exception {
    requireZonal();
    BlobId target = freshKey(zonalBucket, "b1-target");
    BlobId temp = freshKey(zonalBucket, "b1-temp");

    byte[] oldBytes = randomBytes(2048, 500L);
    long g1 = appendableWriteFull(grpcClient, target, oldBytes, BlobWriteOption.doesNotExist());

    byte[] newBytes = randomBytes(4096, 600L);
    long tempGen = appendableWriteFull(grpcClient, temp, newBytes, BlobWriteOption.doesNotExist());

    String result;
    try {
      Blob copied = doRewrite(grpcClient, temp, tempGen, target, g1);
      byte[] actual = grpcClient.readAllBytes(target);
      result =
          "copied gen="
              + copied.getGeneration()
              + " size="
              + copied.getSize()
              + " match="
              + sameBytes(newBytes, actual)
              + " crc32c="
              + copied.getCrc32c();
    } catch (Exception e) {
      result = "FAIL " + classify(e);
    }
    record("b1.rewrite.zonal.happy", result);
  }

  /** Stale destination generation must be rejected. */
  @Test
  void b2_rewriteZonalStaleGen() throws Exception {
    requireZonal();
    BlobId target = freshKey(zonalBucket, "b2-target");
    BlobId temp1 = freshKey(zonalBucket, "b2-temp1");
    BlobId temp2 = freshKey(zonalBucket, "b2-temp2");

    long g1 =
        appendableWriteFull(
            grpcClient, target, randomBytes(2048, 700L), BlobWriteOption.doesNotExist());
    long t1 =
        appendableWriteFull(
            grpcClient, temp1, randomBytes(4096, 800L), BlobWriteOption.doesNotExist());
    long g2 = doRewrite(grpcClient, temp1, t1, target, g1).getGeneration();

    // target is now at g2; try a copy pinned to the stale g1
    long t2 =
        appendableWriteFull(
            grpcClient, temp2, randomBytes(4096, 900L), BlobWriteOption.doesNotExist());
    String result;
    try {
      doRewrite(grpcClient, temp2, t2, target, g1);
      result = "BUG accepted stale dest gen";
    } catch (Exception e) {
      result = "rejected " + classify(e);
    }
    record("b2.rewrite.zonal.staleGen", result);
  }

  /**
   * Exception safety: writer caught an exception mid-temp-upload and finalized the temp anyway (so
   * temp is published with truncated bytes). Caller's exception handler then SKIPS the rewrite.
   * Target must remain at g1 with old bytes.
   */
  @Test
  void b3_rewriteSkippedAfterPartialTemp() throws Exception {
    requireZonal();
    BlobId target = freshKey(zonalBucket, "b3-target");
    BlobId temp = freshKey(zonalBucket, "b3-temp");

    byte[] oldBytes = randomBytes(2048, 1000L);
    long g1 = appendableWriteFull(grpcClient, target, oldBytes, BlobWriteOption.doesNotExist());

    byte[] full = randomBytes(4096, 1100L);
    BlobInfo info = BlobInfo.newBuilder(temp).setCrc32c(b64(crc32c(full))).build();
    BlobAppendableUpload up =
        grpcClient.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING),
            BlobWriteOption.doesNotExist(),
            BlobWriteOption.crc32cMatch());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up.open()) {
      ch.write(ByteBuffer.wrap(full, 0, full.length / 2));
    } // truncated temp is now finalized

    // Caller's exception handler skips the rewrite step entirely. Target should still be at g1.
    Blob targetNow = grpcClient.get(target);
    byte[] targetActual = grpcClient.readAllBytes(target);
    Blob tempNow = grpcClient.get(temp);
    record(
        "b3.rewrite.skippedAfterPartialTemp",
        "target gen="
            + targetNow.getGeneration()
            + " unchanged="
            + sameBytes(targetActual, oldBytes)
            + " | temp gen="
            + tempNow.getGeneration()
            + " size="
            + tempNow.getSize()
            + " (truncated to "
            + (full.length / 2)
            + ")");
  }

  /** Cross-bucket source: temp in standard bucket, copy onto zonal target. */
  @Test
  void b4_rewriteCrossBucketIntoZonal() throws Exception {
    requireBoth();
    BlobId target = freshKey(zonalBucket, "b4-target");
    BlobId temp = freshKey(standardBucket, "b4-temp");

    long g1 =
        appendableWriteFull(
            grpcClient, target, randomBytes(2048, 1200L), BlobWriteOption.doesNotExist());

    byte[] newBytes = randomBytes(4096, 1300L);
    BlobInfo tempInfo = BlobInfo.newBuilder(temp).setCrc32c(b64(crc32c(newBytes))).build();
    long tempGen =
        httpClient.createFrom(tempInfo, new java.io.ByteArrayInputStream(newBytes)).getGeneration();

    String result;
    try {
      Blob copied = doRewrite(grpcClient, temp, tempGen, target, g1);
      byte[] actual = grpcClient.readAllBytes(target);
      result = "copied gen=" + copied.getGeneration() + " match=" + sameBytes(newBytes, actual);
    } catch (Exception e) {
      result = "FAIL " + classify(e);
    }
    record("b4.rewrite.crossBucket.std->zonal", result);
  }

  // ─── Block C: reader visibility during rewrite ────────────────────────────────────────────

  /** Spawn parallel readers polling the target while a rewrite is in flight. */
  @Test
  void c1_readerDuringRewrite() throws Exception {
    requireZonal();
    BlobId target = freshKey(zonalBucket, "c1-target");
    BlobId temp = freshKey(zonalBucket, "c1-temp");

    byte[] oldBytes = randomBytes(64 * 1024, 1400L);
    long g1 = appendableWriteFull(grpcClient, target, oldBytes, BlobWriteOption.doesNotExist());

    // Use a larger temp to widen any visibility window.
    byte[] newBytes = randomBytes(2 * 1024 * 1024, 1500L);
    long tempGen = appendableWriteFull(grpcClient, temp, newBytes, BlobWriteOption.doesNotExist());

    int readerCount = 8;
    int durationMs = 3000;
    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService pool = Executors.newFixedThreadPool(readerCount + 1);
    List<Future<List<String>>> readers = new ArrayList<>();
    try {
      for (int i = 0; i < readerCount; i++) {
        final int rid = i;
        readers.add(
            pool.submit(
                () -> {
                  List<String> log = new ArrayList<>();
                  long start = System.nanoTime();
                  while (!stop.get()) {
                    try {
                      Blob b = grpcClient.get(target);
                      byte[] sample = grpcClient.readAllBytes(target);
                      log.add(
                          "r"
                              + rid
                              + " t="
                              + (System.nanoTime() - start) / 1_000_000
                              + "ms gen="
                              + (b == null ? -1 : b.getGeneration())
                              + " size="
                              + sample.length
                              + " head="
                              + (sample.length > 0 ? Integer.toHexString(sample[0] & 0xff) : "-"));
                    } catch (StorageException e) {
                      log.add("r" + rid + " err " + e.getCode());
                    }
                  }
                  return log;
                }));
      }

      // Run the rewrite and signal readers to stop.
      Future<Long> rewriteFuture =
          pool.submit(
              () -> {
                try {
                  Blob copied = doRewrite(grpcClient, temp, tempGen, target, g1);
                  return copied.getGeneration();
                } finally {
                  Thread.sleep(200); // brief tail observation after the rewrite returns
                  stop.set(true);
                }
              });
      long g2 = rewriteFuture.get(durationMs, TimeUnit.MILLISECONDS);

      int totalSamples = 0;
      int g1Samples = 0;
      int g2Samples = 0;
      int otherSamples = 0;
      int g2WrongSize = 0;
      for (Future<List<String>> f : readers) {
        for (String line : f.get(2, TimeUnit.SECONDS)) {
          totalSamples++;
          if (line.contains("gen=" + g1)) {
            g1Samples++;
          } else if (line.contains("gen=" + g2)) {
            g2Samples++;
            if (!line.contains("size=" + newBytes.length)) {
              g2WrongSize++;
            }
          } else {
            otherSamples++;
          }
        }
      }
      record(
          "c1.reader.duringRewrite",
          "g1="
              + g1
              + " g2="
              + g2
              + " samples="
              + totalSamples
              + " atG1="
              + g1Samples
              + " atG2="
              + g2Samples
              + " other="
              + otherSamples
              + " g2WrongSize="
              + g2WrongSize);
    } finally {
      stop.set(true);
      pool.shutdownNow();
      pool.awaitTermination(3, TimeUnit.SECONDS);
    }
  }

  // ─── Block E: HNS Storage.moveBlob with generation preconditions ──────────────────────────
  //
  // MoveBlobRequest accepts BlobSourceOption + BlobTargetOption (same option types as
  // copy/rewrite),
  // so generationMatch is available on both sides. The Rapid Storage incompatibilities doc does not
  // list move/rename, so this is the leading candidate for atomic CAS on Rapid.

  /** Happy path: stage temp via appendable+finalize, moveBlob(temp → target) with both gen pins. */
  @Test
  void e1_moveBlobZonalHappy() throws Exception {
    requireZonal();
    BlobId target = freshKey(zonalBucket, "e1-target");
    BlobId temp = freshKey(zonalBucket, "e1-temp");

    byte[] oldBytes = randomBytes(2048, 1800L);
    long g1 = appendableWriteFull(grpcClient, target, oldBytes, BlobWriteOption.doesNotExist());

    byte[] newBytes = randomBytes(4096, 1900L);
    long tempGen = appendableWriteFull(grpcClient, temp, newBytes, BlobWriteOption.doesNotExist());

    String result;
    try {
      Blob moved = doMove(grpcClient, temp, tempGen, target, g1);
      byte[] actual = grpcClient.readAllBytes(target);
      Blob tempAfter = grpcClient.get(temp);
      result =
          "moved gen="
              + moved.getGeneration()
              + " size="
              + moved.getSize()
              + " bytesMatch="
              + sameBytes(newBytes, actual)
              + " | tempAfter="
              + (tempAfter == null ? "deleted" : "STILL EXISTS gen=" + tempAfter.getGeneration())
              + " crc32c="
              + moved.getCrc32c();
    } catch (Exception e) {
      result = "FAIL " + classify(e);
    }
    record("e1.move.zonal.happy", result);
  }

  /** Stale destination generation must be rejected. */
  @Test
  void e2_moveBlobZonalStaleGen() throws Exception {
    requireZonal();
    BlobId target = freshKey(zonalBucket, "e2-target");
    BlobId temp1 = freshKey(zonalBucket, "e2-temp1");
    BlobId temp2 = freshKey(zonalBucket, "e2-temp2");

    long g1 =
        appendableWriteFull(
            grpcClient, target, randomBytes(2048, 2000L), BlobWriteOption.doesNotExist());
    long t1 =
        appendableWriteFull(
            grpcClient, temp1, randomBytes(4096, 2100L), BlobWriteOption.doesNotExist());
    long g2 = doMove(grpcClient, temp1, t1, target, g1).getGeneration();

    long t2 =
        appendableWriteFull(
            grpcClient, temp2, randomBytes(4096, 2200L), BlobWriteOption.doesNotExist());
    String result;
    try {
      doMove(grpcClient, temp2, t2, target, g1);
      result = "BUG accepted stale dest gen (g1=" + g1 + ", target now at g2=" + g2 + ")";
    } catch (Exception e) {
      result = "rejected " + classify(e);
    }
    record("e2.move.zonal.staleGen", result);
  }

  /** Exception safety: partial temp finalized, move skipped, target unchanged. */
  @Test
  void e3_moveSkippedAfterPartialTemp() throws Exception {
    requireZonal();
    BlobId target = freshKey(zonalBucket, "e3-target");
    BlobId temp = freshKey(zonalBucket, "e3-temp");

    byte[] oldBytes = randomBytes(2048, 2300L);
    long g1 = appendableWriteFull(grpcClient, target, oldBytes, BlobWriteOption.doesNotExist());

    byte[] full = randomBytes(4096, 2400L);
    BlobInfo info = BlobInfo.newBuilder(temp).setCrc32c(b64(crc32c(full))).build();
    BlobAppendableUpload up =
        grpcClient.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING),
            BlobWriteOption.doesNotExist(),
            BlobWriteOption.crc32cMatch());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up.open()) {
      ch.write(ByteBuffer.wrap(full, 0, full.length / 2));
    } // partial temp now finalized

    // caller's exception handler skips moveBlob entirely
    Blob targetNow = grpcClient.get(target);
    byte[] targetActual = grpcClient.readAllBytes(target);
    Blob tempNow = grpcClient.get(temp);
    record(
        "e3.move.skippedAfterPartialTemp",
        "target gen="
            + targetNow.getGeneration()
            + " unchanged="
            + sameBytes(targetActual, oldBytes)
            + " | temp gen="
            + tempNow.getGeneration()
            + " size="
            + tempNow.getSize()
            + " (truncated to "
            + (full.length / 2)
            + ")");
  }

  /** Reader visibility during a move on Rapid. */
  @Test
  void e4_readerDuringMove() throws Exception {
    requireZonal();
    BlobId target = freshKey(zonalBucket, "e4-target");
    BlobId temp = freshKey(zonalBucket, "e4-temp");

    byte[] oldBytes = randomBytes(64 * 1024, 2500L);
    long g1 = appendableWriteFull(grpcClient, target, oldBytes, BlobWriteOption.doesNotExist());
    byte[] newBytes = randomBytes(2 * 1024 * 1024, 2600L);
    long tempGen = appendableWriteFull(grpcClient, temp, newBytes, BlobWriteOption.doesNotExist());

    int readerCount = 8;
    int durationMs = 5000;
    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService pool = Executors.newFixedThreadPool(readerCount + 1);
    List<Future<List<String>>> readers = new ArrayList<>();
    try {
      for (int i = 0; i < readerCount; i++) {
        final int rid = i;
        readers.add(
            pool.submit(
                () -> {
                  List<String> log = new ArrayList<>();
                  long start = System.nanoTime();
                  while (!stop.get()) {
                    try {
                      // Atomic read: get metadata first, then read bytes pinned to the same
                      // generation via BlobId-with-generation. This rules out the get/readAllBytes
                      // race that would otherwise let metadata and content come from different
                      // points in time.
                      Blob b = grpcClient.get(target);
                      if (b == null) {
                        log.add(
                            "r"
                                + rid
                                + " t="
                                + (System.nanoTime() - start) / 1_000_000
                                + "ms gen=null");
                        continue;
                      }
                      long pinnedGen = b.getGeneration();
                      BlobId pinned = BlobId.of(target.getBucket(), target.getName(), pinnedGen);
                      byte[] sample;
                      String readNote = "ok";
                      try {
                        sample = grpcClient.readAllBytes(pinned);
                      } catch (StorageException re) {
                        sample = new byte[0];
                        readNote = "readErr=" + re.getCode();
                      }
                      log.add(
                          "r"
                              + rid
                              + " t="
                              + (System.nanoTime() - start) / 1_000_000
                              + "ms gen="
                              + pinnedGen
                              + " metaSize="
                              + b.getSize()
                              + " readSize="
                              + sample.length
                              + " "
                              + readNote);
                    } catch (StorageException e) {
                      log.add("r" + rid + " getErr code=" + e.getCode());
                    }
                  }
                  return log;
                }));
      }

      Future<Long> moveFuture =
          pool.submit(
              () -> {
                try {
                  return doMove(grpcClient, temp, tempGen, target, g1).getGeneration();
                } finally {
                  Thread.sleep(200);
                  stop.set(true);
                }
              });
      long g2 = moveFuture.get(durationMs, TimeUnit.MILLISECONDS);

      int total = 0;
      int atG1Clean = 0;
      int atG2Clean = 0;
      int atG1Bad = 0;
      int atG2Bad = 0;
      int otherGen = 0;
      List<String> anomalies = new ArrayList<>();
      for (Future<List<String>> f : readers) {
        for (String line : f.get(2, TimeUnit.SECONDS)) {
          total++;
          boolean atG1 = line.contains("gen=" + g1 + " ");
          boolean atG2 = line.contains("gen=" + g2 + " ");
          boolean expectedG1 =
              line.contains("metaSize=" + oldBytes.length)
                  && line.contains("readSize=" + oldBytes.length);
          boolean expectedG2 =
              line.contains("metaSize=" + newBytes.length)
                  && line.contains("readSize=" + newBytes.length);
          if (atG1 && expectedG1) {
            atG1Clean++;
          } else if (atG2 && expectedG2) {
            atG2Clean++;
          } else if (atG1) {
            atG1Bad++;
            if (anomalies.size() < 5) {
              anomalies.add(line);
            }
          } else if (atG2) {
            atG2Bad++;
            if (anomalies.size() < 5) {
              anomalies.add(line);
            }
          } else {
            otherGen++;
            if (anomalies.size() < 5) {
              anomalies.add(line);
            }
          }
        }
      }
      record(
          "e4.reader.duringMove",
          "g1="
              + g1
              + " g2="
              + g2
              + " samples="
              + total
              + " G1-clean="
              + atG1Clean
              + " G2-clean="
              + atG2Clean
              + " G1-bad="
              + atG1Bad
              + " G2-bad="
              + atG2Bad
              + " otherGen="
              + otherGen
              + (anomalies.isEmpty() ? "" : " | anomalies: " + anomalies));
    } finally {
      stop.set(true);
      pool.shutdownNow();
      pool.awaitTermination(3, TimeUnit.SECONDS);
    }
  }

  // ─── Block F: does moveBlob work on standard (non-HNS) buckets? ───────────────────────────

  /** Try moveBlob against a non-HNS standard bucket. If it fails, that's the answer. */
  @Test
  void f1_moveBlobStandardNonHns() throws Exception {
    requireBoth();
    BlobId target = freshKey(standardBucket, "f1-target");
    BlobId temp = freshKey(standardBucket, "f1-temp");

    // Pre-seed via blobWriteSession (the standard-bucket path).
    byte[] oldBytes = randomBytes(2048, 2700L);
    long g1 = sessionWriteFull(httpClient, target, oldBytes, BlobWriteOption.doesNotExist());
    byte[] newBytes = randomBytes(4096, 2800L);
    long tempGen = sessionWriteFull(httpClient, temp, newBytes, BlobWriteOption.doesNotExist());

    String result;
    try {
      Blob moved = doMove(grpcClient, temp, tempGen, target, g1);
      byte[] actual = grpcClient.readAllBytes(target);
      Blob tempAfter = grpcClient.get(temp);
      result =
          "moved gen="
              + moved.getGeneration()
              + " size="
              + moved.getSize()
              + " bytesMatch="
              + sameBytes(newBytes, actual)
              + " | tempAfter="
              + (tempAfter == null ? "deleted" : "STILL EXISTS");
    } catch (Exception e) {
      result = "FAIL " + classify(e);
    }
    record("f1.move.standard.nonHns", result);
  }

  // ─── Block G: latency comparison ──────────────────────────────────────────────────────────
  //
  // Compare the existing single-call CAS path (blobWriteSession on the existing target) against
  // the two-call stage-and-move path (write temp + moveBlob), where applicable. Small payload
  // (~1 KB) and small N to keep the run quick; this is order-of-magnitude, not a benchmark.

  private static final int LATENCY_N = 10;
  private static final int LATENCY_PAYLOAD_BYTES = 1024;

  // Each iteration writes to a fresh target with doesNotExist() to avoid GCS's per-object
  // ~1-update/second rate limit. This measures the write-path cost (network + SDK + server) under
  // unloaded steady state; the SDK code path is identical between doesNotExist() and
  // generationMatch(g), so the comparison is faithful for one CAS-replace's worth of work.

  /** Latency: existing direct path on standard via blobWriteSession (HTTP). */
  @Test
  void g1_latencyStandardDirectSession() throws Exception {
    requireBoth();
    long[] times = new long[LATENCY_N];
    for (int i = 0; i < LATENCY_N; i++) {
      byte[] payload = randomBytes(LATENCY_PAYLOAD_BYTES, 3001L + i);
      BlobId target = freshKey(standardBucket, "g1-target-" + i);
      long t0 = System.nanoTime();
      sessionWriteFull(httpClient, target, payload, BlobWriteOption.doesNotExist());
      times[i] = System.nanoTime() - t0;
    }
    record("g1.latency.standard.directSession.http", summarize(times));
  }

  /** Latency: stage-and-move on standard (session write + moveBlob). */
  @Test
  void g2_latencyStandardStageAndMove() throws Exception {
    requireBoth();
    long[] times = new long[LATENCY_N];
    for (int i = 0; i < LATENCY_N; i++) {
      byte[] payload = randomBytes(LATENCY_PAYLOAD_BYTES, 3101L + i);
      BlobId target = freshKey(standardBucket, "g2-target-" + i);
      BlobId temp = freshKey(standardBucket, "g2-temp-" + i);
      long t0 = System.nanoTime();
      long tempGen = sessionWriteFull(httpClient, temp, payload, BlobWriteOption.doesNotExist());
      doMove(grpcClient, temp, tempGen, target, 0L);
      times[i] = System.nanoTime() - t0;
    }
    record("g2.latency.standard.stageAndMove", summarize(times));
  }

  /** Latency: stage-and-move on Rapid (appendable write + moveBlob, gRPC throughout). */
  @Test
  void g3_latencyRapidStageAndMove() throws Exception {
    requireZonal();
    long[] times = new long[LATENCY_N];
    for (int i = 0; i < LATENCY_N; i++) {
      byte[] payload = randomBytes(LATENCY_PAYLOAD_BYTES, 3201L + i);
      BlobId target = freshKey(zonalBucket, "g3-target-" + i);
      BlobId temp = freshKey(zonalBucket, "g3-temp-" + i);
      long t0 = System.nanoTime();
      long tempGen = appendableWriteFull(grpcClient, temp, payload, BlobWriteOption.doesNotExist());
      doMove(grpcClient, temp, tempGen, target, 0L);
      times[i] = System.nanoTime() - t0;
    }
    record("g3.latency.rapid.stageAndMove", summarize(times));
  }

  /** Latency: direct appendable on Rapid (unsafe under failure, baseline for comparison). */
  @Test
  void g4_latencyRapidDirectAppendable() throws Exception {
    requireZonal();
    long[] times = new long[LATENCY_N];
    for (int i = 0; i < LATENCY_N; i++) {
      byte[] payload = randomBytes(LATENCY_PAYLOAD_BYTES, 3301L + i);
      BlobId target = freshKey(zonalBucket, "g4-target-" + i);
      long t0 = System.nanoTime();
      appendableWriteFull(grpcClient, target, payload, BlobWriteOption.doesNotExist());
      times[i] = System.nanoTime() - t0;
    }
    record("g4.latency.rapid.directAppendable", summarize(times));
  }

  private static String summarize(long[] timesNs) {
    long[] sorted = timesNs.clone();
    java.util.Arrays.sort(sorted);
    long total = 0;
    long min = Long.MAX_VALUE;
    long max = 0;
    for (long t : timesNs) {
      total += t;
      min = Math.min(min, t);
      max = Math.max(max, t);
    }
    long mean = total / timesNs.length;
    long p50 = sorted[sorted.length / 2];
    long p90 = sorted[(int) (sorted.length * 0.9)];
    return String.format(
        java.util.Locale.ROOT,
        "n=%d min=%.1fms p50=%.1fms p90=%.1fms mean=%.1fms max=%.1fms",
        timesNs.length,
        min / 1e6,
        p50 / 1e6,
        p90 / 1e6,
        mean / 1e6,
        max / 1e6);
  }

  // ─── Block H: speculative double-tap move (safety + latency curve) ────────────────────────
  //
  // Pattern: stage the new content via blobAppendableUpload, issue the moveBlob "speculatively"
  // at delay D after starting the finalize (no source-gen-match — we don't have tempGen yet),
  // then once finalize completes issue the moveBlob again with source-gen-match (idempotent
  // retry). Whichever arrives at the server first wins; the loser sees 404 (source already
  // moved). The win is one round-trip when D matches the server-side finalize processing time.

  private static final long NO_SPECULATION = Long.MAX_VALUE;
  private static final long[] H1_DELAYS_MS = {0L, 1L, 5L, 10L, 25L, NO_SPECULATION};
  private static final int[] H1_SIZES_BYTES = {
    2 * 1024, 8 * 1024, 32 * 1024, 128 * 1024, 512 * 1024, 1024 * 1024
  };
  private static final int H1_ITERATIONS = 3;

  /** Outcome of a single speculative-CAS attempt. */
  private static final class CasResult {
    Boolean specOk; // null = not attempted
    String specErr;
    Boolean postFlushOk; // null = not attempted (won't happen in this implementation)
    String postFlushErr;
    Long finalGen;
    long totalNs;
    boolean targetCorrect;

    String summary() {
      String spec =
          specOk == null ? "skipped" : (specOk ? "ok" : "fail(" + truncateMsg(specErr) + ")");
      String pf =
          postFlushOk == null
              ? "skipped"
              : (postFlushOk ? "ok" : "fail(" + truncateMsg(postFlushErr) + ")");
      return "spec=" + spec + " postFlush=" + pf + " gen=" + finalGen + " ok=" + targetCorrect;
    }
  }

  /**
   * Always-double-tap speculative CAS. Issues finalize and the speculative move in parallel; once
   * finalize completes, also issues the post-flush move with source-gen-match. Both moves are sent;
   * the first to be processed server-side moves the source and the second sees 404.
   */
  private CasResult casSpeculativeDoubleTap(
      Storage storage,
      ExecutorService pool,
      BlobId temp,
      byte[] payload,
      BlobId target,
      long expectedTargetGen,
      long delayMs)
      throws Exception {
    CasResult r = new CasResult();
    long t0 = System.nanoTime();
    String expectedCrcB64 = b64(crc32c(payload));

    Future<Long> finalizeFut =
        pool.submit(
            () -> {
              BlobInfo info = BlobInfo.newBuilder(temp).setCrc32c(expectedCrcB64).build();
              BlobAppendableUpload up =
                  storage.blobAppendableUpload(
                      info,
                      BlobAppendableUploadConfig.of()
                          .withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING),
                      BlobWriteOption.doesNotExist(),
                      BlobWriteOption.crc32cMatch());
              try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up.open()) {
                ch.write(ByteBuffer.wrap(payload));
                ch.finalizeAndClose();
              }
              return up.getResult().get().getGeneration();
            });

    Future<Blob> specMoveFut = null;
    if (delayMs != NO_SPECULATION) {
      final long d = delayMs;
      specMoveFut =
          pool.submit(
              () -> {
                if (d > 0) {
                  Thread.sleep(d);
                }
                Storage.MoveBlobRequest req =
                    Storage.MoveBlobRequest.newBuilder()
                        .setSource(temp)
                        .setTarget(target)
                        .setTargetOptions(BlobTargetOption.generationMatch(expectedTargetGen))
                        .build();
                return storage.moveBlob(req);
              });
    }

    long tempGen;
    try {
      tempGen = finalizeFut.get(60, TimeUnit.SECONDS);
    } catch (Exception e) {
      r.totalNs = System.nanoTime() - t0;
      r.specErr = "finalize_failed:" + classify(e);
      return r;
    }

    Future<Blob> postFlushFut =
        pool.submit(
            () -> {
              Storage.MoveBlobRequest req =
                  Storage.MoveBlobRequest.newBuilder()
                      .setSource(temp)
                      .setTarget(target)
                      .setSourceOptions(BlobSourceOption.generationMatch(tempGen))
                      .setTargetOptions(BlobTargetOption.generationMatch(expectedTargetGen))
                      .build();
              return storage.moveBlob(req);
            });

    if (specMoveFut != null) {
      try {
        Blob spec = specMoveFut.get(60, TimeUnit.SECONDS);
        r.specOk = true;
        r.finalGen = spec.getGeneration();
      } catch (ExecutionException e) {
        r.specOk = false;
        r.specErr = classify(e.getCause());
      } catch (Exception e) {
        r.specOk = false;
        r.specErr = classify(e);
      }
    }

    try {
      Blob postFlush = postFlushFut.get(60, TimeUnit.SECONDS);
      r.postFlushOk = true;
      if (r.finalGen == null) {
        r.finalGen = postFlush.getGeneration();
      }
    } catch (ExecutionException e) {
      r.postFlushOk = false;
      r.postFlushErr = classify(e.getCause());
    } catch (Exception e) {
      r.postFlushOk = false;
      r.postFlushErr = classify(e);
    }

    r.totalNs = System.nanoTime() - t0;

    // Verify final state of target. NB: use .equals() — Blob.getGeneration() returns Long, and Long
    // == Long compares references, which is only true for values < 128.
    if (r.finalGen != null) {
      try {
        Blob actualTarget = storage.get(target);
        if (actualTarget != null && r.finalGen.equals(actualTarget.getGeneration())) {
          byte[] actualBytes = storage.readAllBytes(target);
          r.targetCorrect = sameBytes(actualBytes, payload);
        }
      } catch (StorageException ignored) {
        // target may not exist if CAS failed completely
      }
    }
    return r;
  }

  /** h1: matrix of delays × payload sizes. Single-threaded. */
  @Test
  void h1_speculativeMatrix() throws Exception {
    requireZonal();
    ExecutorService pool = Executors.newFixedThreadPool(8);
    StringBuilder summary = new StringBuilder("\n");
    summary.append(
        String.format(
            java.util.Locale.ROOT,
            "%-12s %-10s | %-6s %-6s %-6s | %-22s | %-22s%n",
            "size",
            "delay",
            "spec=ok",
            "pf=ok",
            "byOk",
            "p50/p90/max ms",
            "first-error"));
    try {
      for (int size : H1_SIZES_BYTES) {
        for (long delay : H1_DELAYS_MS) {
          List<CasResult> results = new ArrayList<>();
          for (int i = 0; i < H1_ITERATIONS; i++) {
            BlobId target = freshKey(zonalBucket, "h1-t-" + size + "-" + delay + "-" + i);
            BlobId temp = freshKey(zonalBucket, "h1-s-" + size + "-" + delay + "-" + i);
            byte[] payload = randomBytes(size, 4000L + i + delay);
            CasResult r =
                casSpeculativeDoubleTap(grpcClient, pool, temp, payload, target, 0L, delay);
            results.add(r);
          }
          int specOks = 0;
          int pfOks = 0;
          int byOk = 0;
          String firstErr = "";
          long[] times = new long[results.size()];
          for (int i = 0; i < results.size(); i++) {
            CasResult r = results.get(i);
            times[i] = r.totalNs;
            if (Boolean.TRUE.equals(r.specOk)) specOks++;
            if (Boolean.TRUE.equals(r.postFlushOk)) pfOks++;
            if (r.targetCorrect) byOk++;
            if (firstErr.isEmpty()) {
              if (r.specErr != null) firstErr = "spec:" + truncateMsg(r.specErr);
              else if (r.postFlushErr != null) firstErr = "pf:" + truncateMsg(r.postFlushErr);
            }
          }
          long[] sorted = times.clone();
          java.util.Arrays.sort(sorted);
          long p50 = sorted[sorted.length / 2];
          long p90 = sorted[(int) (sorted.length * 0.9)];
          long max = sorted[sorted.length - 1];
          String row =
              String.format(
                  java.util.Locale.ROOT,
                  "%-12s %-10s | %-6d %-6d %-6d | %-22s | %s",
                  (size / 1024) + "KiB",
                  delay == NO_SPECULATION ? "none" : (delay + "ms"),
                  specOks,
                  pfOks,
                  byOk,
                  String.format(
                      java.util.Locale.ROOT, "%.0f/%.0f/%.0f", p50 / 1e6, p90 / 1e6, max / 1e6),
                  firstErr);
          summary.append(row).append("\n");
          record(
              "h1.size=" + (size / 1024) + "KiB.D=" + (delay == NO_SPECULATION ? "none" : delay),
              row);
        }
      }
      System.out.println(summary.toString());
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(3, TimeUnit.SECONDS);
    }
  }

  /**
   * h2: multi-thread same-target contention with speculation enabled. N writers all pinned to the
   * same baseline generation, each staging its own UUID temp + double-tap moving onto the shared
   * target. Exactly one writer must succeed per round.
   */
  @Test
  void h2_multiThreadSameTargetContention() throws Exception {
    requireZonal();
    int writers = 4;
    int rounds = 3;
    long delayMs = 5L; // moderate speculation
    ExecutorService pool = Executors.newFixedThreadPool(writers * 4);
    BlobId target = freshKey(zonalBucket, "h2-target");
    byte[] seedBytes = randomBytes(8 * 1024, 5000L);
    long currentGen =
        appendableWriteFull(grpcClient, target, seedBytes, BlobWriteOption.doesNotExist());

    int totalWinners = 0;
    int totalLosers = 0;
    int byteCheckFailures = 0;
    List<String> anomalies = new ArrayList<>();

    for (int round = 0; round < rounds; round++) {
      final long baselineGen = currentGen;
      List<Future<CasResult>> futs = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        final int wid = w;
        final int r = round;
        futs.add(
            pool.submit(
                () -> {
                  BlobId temp = freshKey(zonalBucket, "h2-temp-r" + r + "-w" + wid);
                  byte[] payload = randomBytes(8 * 1024, 5100L + wid + r * 100L);
                  return casSpeculativeDoubleTap(
                      grpcClient, pool, temp, payload, target, baselineGen, delayMs);
                }));
      }
      int winners = 0;
      Long winningGen = null;
      for (int w = 0; w < writers; w++) {
        CasResult r = futs.get(w).get(120, TimeUnit.SECONDS);
        boolean specWin = Boolean.TRUE.equals(r.specOk);
        boolean pfWin = Boolean.TRUE.equals(r.postFlushOk);
        if (specWin || pfWin) {
          winners++;
          if (winningGen == null) {
            winningGen = r.finalGen;
          } else if (!winningGen.equals(r.finalGen)) {
            anomalies.add(
                "round "
                    + round
                    + " writer "
                    + w
                    + " says gen="
                    + r.finalGen
                    + " but earlier winner gen="
                    + winningGen);
          }
          if (!r.targetCorrect) {
            byteCheckFailures++;
            anomalies.add(
                "round " + round + " winner-claim w=" + w + " but target bytes don't match");
          }
        } else {
          // Must have failed with 412 / FAILED_PRECONDITION
          String specErr = r.specErr == null ? "" : r.specErr;
          String pfErr = r.postFlushErr == null ? "" : r.postFlushErr;
          if (!specErr.contains("412") && !pfErr.contains("412")) {
            anomalies.add(
                "round "
                    + round
                    + " writer "
                    + w
                    + " lost with non-412: spec="
                    + specErr
                    + " pf="
                    + pfErr);
          }
        }
      }
      totalWinners += winners;
      totalLosers += (writers - winners);
      if (winners != 1) {
        anomalies.add("round " + round + " had " + winners + " winners (expected 1)");
      }
      // Refresh current gen for next round
      Blob nowBlob = grpcClient.get(target);
      currentGen = nowBlob.getGeneration();
    }

    record(
        "h2.multiThread.sameTarget",
        "rounds="
            + rounds
            + " writersPerRound="
            + writers
            + " totalWinners="
            + totalWinners
            + " totalLosers="
            + totalLosers
            + " byteCheckFailures="
            + byteCheckFailures
            + (anomalies.isEmpty() ? "" : " | anomalies: " + anomalies));
    pool.shutdownNow();
    pool.awaitTermination(3, TimeUnit.SECONDS);
  }

  /**
   * h3a: force the "speculation wins" case (large delay so server has finalized by the time spec
   * move arrives). Confirm the post-flush move's 404 is cleanly distinguishable.
   */
  @Test
  void h3a_speculationWinsPostFlush404() throws Exception {
    requireZonal();
    ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      BlobId target = freshKey(zonalBucket, "h3a-target");
      BlobId temp = freshKey(zonalBucket, "h3a-temp");
      byte[] payload = randomBytes(8 * 1024, 6000L);

      // Use a large delay so spec move arrives well after finalize completes.
      String expectedCrc = b64(crc32c(payload));
      CasResult r = casSpeculativeDoubleTap(grpcClient, pool, temp, payload, target, 0L, 500L);

      String diag = "";
      if (r.finalGen != null) {
        Blob actualTarget = grpcClient.get(target);
        byte[] actualBytes = grpcClient.readAllBytes(target);
        diag =
            " | target.gen="
                + (actualTarget == null ? "null" : actualTarget.getGeneration())
                + " target.size="
                + (actualTarget == null ? "null" : actualTarget.getSize())
                + " target.metaCrc="
                + (actualTarget == null ? "null" : actualTarget.getCrc32c())
                + " readSize="
                + actualBytes.length
                + " readCrc="
                + b64(crc32c(actualBytes))
                + " expectedCrc="
                + expectedCrc
                + " expectedSize="
                + payload.length;
      }

      record(
          "h3a.speculationWins.postFlush404",
          "spec="
              + r.specOk
              + " specErr="
              + truncateMsg(r.specErr)
              + " | postFlush="
              + r.postFlushOk
              + " pfErr="
              + truncateMsg(r.postFlushErr)
              + " | finalGen="
              + r.finalGen
              + " bytesMatch="
              + r.targetCorrect
              + diag);
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(3, TimeUnit.SECONDS);
    }
  }

  /** h3b: force CAS contention. Both spec and post-flush moves should 412. */
  @Test
  void h3b_casContentionBoth412() throws Exception {
    requireZonal();
    ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      BlobId target = freshKey(zonalBucket, "h3b-target");
      byte[] seed = randomBytes(2048, 6100L);
      long g1 = appendableWriteFull(grpcClient, target, seed, BlobWriteOption.doesNotExist());
      // Writer B replaces target so writer A's baseline (g1) becomes stale
      BlobId tempB = freshKey(zonalBucket, "h3b-tempB");
      long tempBGen =
          appendableWriteFull(
              grpcClient, tempB, randomBytes(2048, 6200L), BlobWriteOption.doesNotExist());
      doMove(grpcClient, tempB, tempBGen, target, g1);

      // Writer A still pinned to g1 — must fail
      BlobId tempA = freshKey(zonalBucket, "h3b-tempA");
      CasResult r =
          casSpeculativeDoubleTap(
              grpcClient, pool, tempA, randomBytes(2048, 6300L), target, g1, 5L);
      record(
          "h3b.casContention.both412",
          "spec="
              + r.specOk
              + " specErr="
              + truncateMsg(r.specErr)
              + " | postFlush="
              + r.postFlushOk
              + " pfErr="
              + truncateMsg(r.postFlushErr)
              + " | targetReplaced="
              + (r.finalGen != null));
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(3, TimeUnit.SECONDS);
    }
  }

  /** h4: orphan cleanup. After CAS failure, verify the temp is deletable. */
  @Test
  void h4_orphanCleanup() throws Exception {
    requireZonal();
    ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      BlobId target = freshKey(zonalBucket, "h4-target");
      byte[] seed = randomBytes(2048, 7000L);
      long g1 = appendableWriteFull(grpcClient, target, seed, BlobWriteOption.doesNotExist());
      BlobId tempB = freshKey(zonalBucket, "h4-tempB");
      long tempBGen =
          appendableWriteFull(
              grpcClient, tempB, randomBytes(2048, 7100L), BlobWriteOption.doesNotExist());
      doMove(grpcClient, tempB, tempBGen, target, g1);

      // Writer A's CAS fails because g1 is stale
      BlobId tempA = freshKey(zonalBucket, "h4-tempA");
      CasResult r =
          casSpeculativeDoubleTap(
              grpcClient, pool, tempA, randomBytes(2048, 7200L), target, g1, 5L);

      // tempA should still exist (moveBlob failed). Verify it can be deleted.
      Blob tempBefore = grpcClient.get(tempA);
      boolean deletedOk = false;
      String deleteErr = "";
      try {
        deletedOk = grpcClient.delete(tempA);
      } catch (Exception e) {
        deleteErr = classify(e);
      }
      Blob tempAfter = grpcClient.get(tempA);

      // h4b: also verify happy path leaves no orphan
      BlobId target2 = freshKey(zonalBucket, "h4b-target");
      BlobId tempC = freshKey(zonalBucket, "h4b-tempC");
      CasResult happy =
          casSpeculativeDoubleTap(
              grpcClient, pool, tempC, randomBytes(2048, 7300L), target2, 0L, 5L);
      Blob orphan = grpcClient.get(tempC);

      record(
          "h4.orphan.afterCasFailure",
          "casFailed="
              + (r.finalGen == null)
              + " tempBeforeDelete="
              + (tempBefore == null ? "missing" : "size=" + tempBefore.getSize())
              + " deletedOk="
              + deletedOk
              + (deleteErr.isEmpty() ? "" : " deleteErr=" + deleteErr)
              + " tempAfterDelete="
              + (tempAfter == null ? "gone" : "STILL THERE"));
      record(
          "h4b.orphan.afterCasSuccess",
          "happySucceeded="
              + (happy.finalGen != null)
              + " tempCAfterMove="
              + (orphan == null ? "gone (good)" : "STILL THERE (leaked)"));
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(3, TimeUnit.SECONDS);
    }
  }

  /**
   * h5: per-object update rate. Single-thread back-to-back CAS on the same target via the spec
   * pattern. Count 429s and record observed update interval.
   */
  @Test
  void h5_perObjectUpdateRate() throws Exception {
    requireZonal();
    int iterations = 15;
    long delayMs = 5L;
    ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      BlobId target = freshKey(zonalBucket, "h5-target");
      byte[] seed = randomBytes(8 * 1024, 8000L);
      long gen = appendableWriteFull(grpcClient, target, seed, BlobWriteOption.doesNotExist());

      int throttled = 0;
      int succeeded = 0;
      long[] interArrivalMs = new long[iterations];
      long lastT = System.nanoTime();
      for (int i = 0; i < iterations; i++) {
        BlobId temp = freshKey(zonalBucket, "h5-temp-" + i);
        byte[] payload = randomBytes(8 * 1024, 8100L + i);
        CasResult r =
            casSpeculativeDoubleTap(grpcClient, pool, temp, payload, target, gen, delayMs);
        long now = System.nanoTime();
        interArrivalMs[i] = (now - lastT) / 1_000_000;
        lastT = now;
        if (r.finalGen != null) {
          succeeded++;
          gen = r.finalGen;
        }
        boolean was429 =
            (r.specErr != null && r.specErr.contains("429"))
                || (r.postFlushErr != null && r.postFlushErr.contains("429"));
        if (was429) throttled++;
      }
      long[] sorted = interArrivalMs.clone();
      java.util.Arrays.sort(sorted);
      record(
          "h5.perObjectUpdateRate.spec",
          "iterations="
              + iterations
              + " succeeded="
              + succeeded
              + " throttled429="
              + throttled
              + " interArrival(p50/p90/max ms)="
              + sorted[sorted.length / 2]
              + "/"
              + sorted[(int) (sorted.length * 0.9)]
              + "/"
              + sorted[sorted.length - 1]);
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(3, TimeUnit.SECONDS);
    }
  }

  // ─── helpers ──────────────────────────────────────────────────────────────────────────────

  private void requireZonal() {
    Assumptions.assumeTrue(
        zonalBucket != null && !zonalBucket.isEmpty() && grpcClient != null,
        "RAPID_BUCKET not set");
  }

  private void requireBoth() {
    requireZonal();
    Assumptions.assumeTrue(
        standardBucket != null && !standardBucket.isEmpty() && httpClient != null,
        "STANDARD_BUCKET not set");
  }

  private long sessionWriteFull(
      Storage storage, BlobId target, byte[] payload, BlobWriteOption preCondition)
      throws Exception {
    BlobInfo info = BlobInfo.newBuilder(target).setCrc32c(b64(crc32c(payload))).build();
    com.google.cloud.storage.BlobWriteSession sess =
        storage.blobWriteSession(info, preCondition, BlobWriteOption.crc32cMatch());
    try (java.nio.channels.WritableByteChannel ch = sess.open()) {
      try (java.io.OutputStream os = java.nio.channels.Channels.newOutputStream(ch)) {
        os.write(payload);
      }
    }
    return sess.getResult().get().getGeneration();
  }

  private long appendableWriteFull(
      Storage storage, BlobId target, byte[] payload, BlobWriteOption preCondition)
      throws Exception {
    BlobInfo info = BlobInfo.newBuilder(target).setCrc32c(b64(crc32c(payload))).build();
    BlobAppendableUpload up =
        storage.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING),
            preCondition,
            BlobWriteOption.crc32cMatch());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up.open()) {
      ch.write(ByteBuffer.wrap(payload));
      ch.finalizeAndClose();
    }
    return up.getResult().get().getGeneration();
  }

  private Blob doRewrite(
      Storage storage, BlobId source, long sourceGen, BlobId target, long targetGen) {
    CopyRequest req =
        CopyRequest.newBuilder()
            .setSource(source)
            .setSourceOptions(BlobSourceOption.generationMatch(sourceGen))
            .setTarget(
                BlobInfo.newBuilder(target).build(), BlobTargetOption.generationMatch(targetGen))
            .build();
    CopyWriter writer = storage.copy(req);
    return writer.getResult();
  }

  private Blob doMove(
      Storage storage, BlobId source, long sourceGen, BlobId target, long targetGen) {
    Storage.MoveBlobRequest req =
        Storage.MoveBlobRequest.newBuilder()
            .setSource(source)
            .setTarget(target)
            .setSourceOptions(BlobSourceOption.generationMatch(sourceGen))
            .setTargetOptions(BlobTargetOption.generationMatch(targetGen))
            .build();
    return storage.moveBlob(req);
  }

  private BlobId freshKey(String bucket, String slug) {
    BlobId id = BlobId.of(bucket, SUITE_PREFIX + slug + "-" + UUID.randomUUID() + ".bin");
    created.add(BlobId.of(bucket, id.getName()));
    return id;
  }

  private static byte[] randomBytes(int n, long seed) {
    Random r = new Random(seed);
    byte[] b = new byte[n];
    r.nextBytes(b);
    return b;
  }

  private static byte[] crc32c(byte[] data) {
    int crc = Hashing.crc32c().hashBytes(data).asInt();
    return new byte[] {(byte) (crc >>> 24), (byte) (crc >>> 16), (byte) (crc >>> 8), (byte) crc};
  }

  private static String b64(byte[] data) {
    return BaseEncoding.base64().encode(data);
  }

  private static boolean sameBytes(byte[] a, byte[] b) {
    if (a == null || b == null || a.length != b.length) {
      return false;
    }
    for (int i = 0; i < a.length; i++) {
      if (a[i] != b[i]) {
        return false;
      }
    }
    return true;
  }

  private static String classify(Throwable t) {
    Throwable cur = t;
    while (cur != null) {
      if (cur instanceof StorageException) {
        StorageException se = (StorageException) cur;
        String reason = se.getReason() != null ? se.getReason() : "";
        return "StorageException(code="
            + se.getCode()
            + (reason.isEmpty() ? "" : ", reason=" + reason)
            + ", msg="
            + truncateMsg(se.getMessage())
            + ")";
      }
      cur = cur.getCause();
    }
    return t.getClass().getSimpleName() + ": " + truncateMsg(t.getMessage());
  }

  private static String truncateMsg(String m) {
    if (m == null) {
      return "<null>";
    }
    String oneLine = m.replace('\n', ' ').replace('\r', ' ');
    return oneLine.length() <= 100 ? oneLine : oneLine.substring(0, 99) + "…";
  }

  private static Credentials loadCredentials() throws IOException {
    String credPath = System.getenv(CREDS_ENV);
    if (credPath != null && !credPath.isEmpty()) {
      Path p = Paths.get(credPath);
      if (Files.exists(p)) {
        try (FileInputStream in = new FileInputStream(p.toFile())) {
          return GoogleCredentials.fromStream(in);
        }
      }
    }
    try {
      return GoogleCredentials.getApplicationDefault();
    } catch (IOException e) {
      System.out.println("[probe.setup] no application-default credentials: " + e.getMessage());
      return null;
    }
  }

  private void record(String label, String result) {
    System.out.println("[probe." + label + "] " + result);
    RESULTS.put(label, result);
  }

  static void report() {
    if (RESULTS.isEmpty()) {
      return;
    }
    System.out.println();
    System.out.println("=== GcsCasViaRewriteProbe summary ===");
    for (Map.Entry<String, String> e : RESULTS.entrySet()) {
      System.out.printf("  %-44s  %s%n", e.getKey(), e.getValue());
    }
  }

  // assertThat is imported but not used directly in records; keep here so the import doesn't dangle
  // when adding richer assertions later.
  @SuppressWarnings("unused")
  private static void touchAssertJ() {
    assertThat(1).isEqualTo(1);
  }
}
