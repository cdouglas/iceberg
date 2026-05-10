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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobAppendableUploadConfig.CloseAction;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.metrics.MetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CAS-replace on zonal/Rapid GCS buckets via stage-and-move.
 *
 * <p>Zonal buckets reject every standard write surface ({@code blobWriteSession},
 * {@code storage.create}, {@code storage.copy/rewrite}) and the one surface they accept
 * ({@code blobAppendableUpload} writing directly to the target) silently publishes truncated bytes
 * on any exception path through {@code close()} regardless of {@code CloseAction}. The viable
 * pattern is:
 *
 * <ol>
 *   <li>Write payload to a UUID-named temp via
 *       {@code blobAppendableUpload(doesNotExist) + finalizeAndClose}.
 *   <li>{@code Storage.moveBlob} with both source and target generation preconditions to
 *       atomically replace the live target. The server-side move preserves the temp's bytes,
 *       deletes the temp on success, and rejects stale-target races with {@code HTTP 412}.
 * </ol>
 *
 * <p>Speculative variant (engaged when {@link GCPProperties#rapidMoveSpeculativeDelayMs()} is
 * {@code >= 0}): in parallel with the finalize task, schedule a moveBlob at delay {@code D} that
 * carries only the target-gen-match (the source's generation isn't known until finalize returns).
 * After finalize completes, fire an idempotent post-flush moveBlob with both preconditions.
 * Whichever moveBlob reaches the server first consumes the source; the loser sees {@code 404}.
 * The win is one round-trip when {@code D} matches the server-side finalize processing time. See
 * {@code docs/docs/atomic_io_gcs_rapid.md} for the empirical justification.
 *
 * <p>This class is stateless across calls; instances may be shared across writers.
 */
class GCSRapidStageAndMove {
  private static final Logger LOG = LoggerFactory.getLogger(GCSRapidStageAndMove.class);
  private static final String TEMP_PREFIX = ".cas-tmp/";

  // Daemon-threaded cached pool: scales on demand, idle threads die after 60s, no lifecycle
  // coupling with PrefixedStorage. Used only for the speculative path (parallelizing finalize
  // with the speculative move and the post-flush move).
  private static final AtomicLong THREAD_COUNTER = new AtomicLong();
  private static final ExecutorService SHARED_EXECUTOR =
      Executors.newCachedThreadPool(
          r -> {
            Thread t = new Thread(r, "gcs-rapid-cas-" + THREAD_COUNTER.incrementAndGet());
            t.setDaemon(true);
            return t;
          });

  private final Storage grpcStorage;
  private final GCPProperties gcpProperties;
  private final MetricsContext metrics;

  GCSRapidStageAndMove(Storage grpcStorage, GCPProperties gcpProperties, MetricsContext metrics) {
    this.grpcStorage = grpcStorage;
    this.gcpProperties = gcpProperties;
    this.metrics = metrics;
  }

  /**
   * Atomically replace {@code target} with {@code payload}, gated on {@code pinnedSnapshot}'s
   * generation. Returns an {@link InputFile} pointing at the new generation. Throws
   * {@link SupportsAtomicOperations.StorageInvariantException} on stale-gen rejection (HTTP 412).
   */
  InputFile writeAtomic(
      BlobId target, BlobId pinnedSnapshot, String expectedCrc32cB64, byte[] payload)
      throws IOException {
    long expectedTargetGen = pinnedSnapshot == null ? 0L : pinnedSnapshot.getGeneration();
    BlobId temp =
        BlobId.of(target.getBucket(), TEMP_PREFIX + UUID.randomUUID() + ".tmp");
    long speculativeDelayMs = gcpProperties.rapidMoveSpeculativeDelayMs();

    if (speculativeDelayMs < 0) {
      return writeSerial(target, expectedTargetGen, temp, expectedCrc32cB64, payload);
    }
    return writeSpeculative(
        target, expectedTargetGen, temp, expectedCrc32cB64, payload, speculativeDelayMs);
  }

  // ─── serial path (no speculation) ──────────────────────────────────────────────────────────

  private InputFile writeSerial(
      BlobId target, long expectedTargetGen, BlobId temp, String expectedCrc, byte[] payload)
      throws IOException {
    long tempGen;
    try {
      tempGen = stageTemp(temp, expectedCrc, payload);
    } catch (StorageException e) {
      throw mapStorageException(e);
    } catch (IOException e) {
      // best-effort: any partial state remains on the server but never reaches target
      bestEffortDelete(temp);
      throw e;
    }

    try {
      Blob moved = doMove(temp, tempGen, target, expectedTargetGen);
      return inputFileFor(moved);
    } catch (StorageException e) {
      // temp still exists on stale-gen rejection; clean it up so we don't accumulate orphans
      bestEffortDelete(temp);
      throw mapStorageException(e);
    }
  }

  // ─── speculative path (always-double-tap moveBlob) ─────────────────────────────────────────

  private InputFile writeSpeculative(
      BlobId target,
      long expectedTargetGen,
      BlobId temp,
      String expectedCrc,
      byte[] payload,
      long speculativeDelayMs)
      throws IOException {
    Future<Long> finalizeFut =
        SHARED_EXECUTOR.submit(() -> stageTemp(temp, expectedCrc, payload));

    Future<Blob> specMoveFut =
        SHARED_EXECUTOR.submit(
            () -> {
              if (speculativeDelayMs > 0) {
                Thread.sleep(speculativeDelayMs);
              }
              return doMoveNoSrcGen(temp, target, expectedTargetGen);
            });

    long tempGen;
    try {
      tempGen = finalizeFut.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      drainQuietly(specMoveFut);
      bestEffortDelete(temp);
      throw new IOException("Interrupted during temp upload to " + temp.toGsUtilUri(), e);
    } catch (ExecutionException e) {
      drainQuietly(specMoveFut);
      bestEffortDelete(temp);
      throw rethrowExecution(e);
    }

    Future<Blob> postFlushFut =
        SHARED_EXECUTOR.submit(() -> doMove(temp, tempGen, target, expectedTargetGen));

    Blob moved = null;
    StorageException specErr = null;
    StorageException postErr = null;
    try {
      moved = specMoveFut.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      drainQuietly(postFlushFut);
      bestEffortDelete(temp);
      throw new IOException("Interrupted awaiting speculative move", e);
    } catch (ExecutionException e) {
      specErr = asStorageException(e.getCause());
    }

    try {
      Blob postFlushMoved = postFlushFut.get();
      if (moved == null) {
        moved = postFlushMoved;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      bestEffortDelete(temp);
      throw new IOException("Interrupted awaiting post-flush move", e);
    } catch (ExecutionException e) {
      postErr = asStorageException(e.getCause());
    }

    if (moved != null) {
      // At least one move succeeded — the loser's 404 / 412 is fine.
      return inputFileFor(moved);
    }

    // Both moves failed. Prefer the 412 (legitimate CAS failure) over the 404 (timing miss).
    StorageException propagate = pickReal(specErr, postErr);
    bestEffortDelete(temp);
    if (propagate != null) {
      throw mapStorageException(propagate);
    }
    throw new IOException("Both speculative and post-flush moveBlob calls failed without a recognized cause");
  }

  // ─── primitives ────────────────────────────────────────────────────────────────────────────

  /** Append-upload {@code payload} into {@code temp} and finalize. Returns the temp's generation. */
  private long stageTemp(BlobId temp, String expectedCrc32cB64, byte[] payload) throws IOException {
    BlobInfo info = BlobInfo.newBuilder(temp).setCrc32c(expectedCrc32cB64).build();
    BlobAppendableUpload up =
        grpcStorage.blobAppendableUpload(
            info,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING),
            BlobWriteOption.doesNotExist(),
            BlobWriteOption.crc32cMatch());
    try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up.open()) {
      ch.write(ByteBuffer.wrap(payload));
      ch.finalizeAndClose();
    }
    try {
      return up.getResult().get().getGeneration();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted awaiting append-upload result for " + temp.toGsUtilUri(), e);
    } catch (ExecutionException e) {
      throw rethrowExecution(e);
    }
  }

  private Blob doMove(BlobId source, long sourceGen, BlobId target, long targetGen) {
    Storage.MoveBlobRequest.Builder b =
        Storage.MoveBlobRequest.newBuilder()
            .setSource(source)
            .setTarget(target)
            .setSourceOptions(BlobSourceOption.generationMatch(sourceGen));
    b.setTargetOptions(BlobTargetOption.generationMatch(targetGen));
    return grpcStorage.moveBlob(b.build());
  }

  /**
   * Speculative move: no source-gen-match (we don't know the temp's gen yet). The temp's UUID name
   * guarantees only this writer can have created it, so the "wrong source bytes" risk is bounded
   * by the assumption that no other process is writing to this UUID path.
   */
  private Blob doMoveNoSrcGen(BlobId source, BlobId target, long targetGen) {
    Storage.MoveBlobRequest req =
        Storage.MoveBlobRequest.newBuilder()
            .setSource(source)
            .setTarget(target)
            .setTargetOptions(BlobTargetOption.generationMatch(targetGen))
            .build();
    return grpcStorage.moveBlob(req);
  }

  private InputFile inputFileFor(Blob moved) {
    return new GCSInputFile(grpcStorage, moved.getBlobId(), moved.getSize(), gcpProperties, metrics);
  }

  // ─── helpers ───────────────────────────────────────────────────────────────────────────────

  private static IOException rethrowExecution(ExecutionException e) throws IOException {
    Throwable cause = e.getCause();
    if (cause instanceof StorageException) {
      throw mapStorageException((StorageException) cause);
    }
    if (cause instanceof IOException) {
      throw (IOException) cause;
    }
    if (cause instanceof RuntimeException) {
      throw (RuntimeException) cause;
    }
    throw new IOException(cause);
  }

  private static IOException mapStorageException(StorageException e) {
    int code = e.getCode();
    if (code == 412) {
      throw new SupportsAtomicOperations.StorageInvariantException("Target modified", e);
    }
    if (code == 429 || (code >= 500 && code < 600)) {
      throw new SupportsAtomicOperations.StorageThrottleException(
          "Storage applied backpressure (HTTP " + code + ")", e);
    }
    throw e;
  }

  private static StorageException asStorageException(Throwable t) {
    Throwable cur = t;
    while (cur != null) {
      if (cur instanceof StorageException) {
        return (StorageException) cur;
      }
      cur = cur.getCause();
    }
    return null;
  }

  /** Prefer a 412 (real CAS failure) over a 404 (speculation arrived too early). */
  private static StorageException pickReal(StorageException a, StorageException b) {
    if (a != null && a.getCode() == 412) return a;
    if (b != null && b.getCode() == 412) return b;
    if (a != null && a.getCode() != 404) return a;
    if (b != null && b.getCode() != 404) return b;
    return a != null ? a : b;
  }

  private static <T> void drainQuietly(Future<T> fut) {
    try {
      fut.get();
    } catch (Exception ignored) {
      // best-effort
    }
  }

  private void bestEffortDelete(BlobId blob) {
    try {
      grpcStorage.delete(blob);
    } catch (Exception e) {
      LOG.debug("Best-effort delete of {} failed", blob, e);
    }
  }
}
