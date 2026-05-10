# Atomic FileIO on GCS Rapid Storage

This document records two rounds of empirical investigation into how to
implement `SupportsAtomicOperations` on GCS Rapid Storage. The
short answer:

- **Atomic APPEND is not safely implementable** on Rapid. The original
  PoC (recorded below as the "April 2026" investigation) tried to use
  the appendable-object protocol's bidi API to fence concurrent
  writers and found silent data loss patterns under contention.
- **Atomic CAS-replace IS implementable**, but not via the obvious
  paths (`blobWriteSession`, `storage.create`, `copy/rewrite` are all
  rejected by the server on Rapid; direct `blobAppendableUpload` writes
  to the target are unsafe under exception paths). The viable pattern
  is **stage-and-move**: write to a UUID-named temp via
  `blobAppendableUpload`, then `Storage.moveBlob` with both source and
  target generation preconditions to atomically replace the live
  object. This was found in the "May 2026" investigation below.

`GCSFileIO.supportsAppend()` stays `false`. `GCSFileIO` will gain a
zonal-aware CAS path that uses stage-and-move when the bucket is
zonal/Rapid, and keeps the existing `blobWriteSession` path for
standard buckets (where it remains the fastest option, see the
performance section).

## May 2026: stage-and-move CAS

After the April 2026 PoC ruled out atomic APPEND on Rapid, the next
question was whether the FileIO catalog can run on Rapid at all — i.e.,
whether atomic CAS-replace has a working implementation. This required
surveying the SDK and server behavior across all zonal-compatible
write surfaces, since the obvious one (`blobWriteSession`) is rejected
by zonal buckets with HTTP 400 ("Zonal buckets are incompatible with
resumable upload").

The probe code lives at:

- `iceberg/gcp/src/test/java/org/apache/iceberg/gcp/gcs/GcsCasTransportProbe.java`
  — matrix probe across `{HTTP, gRPC} × {blobWriteSession, blobAppendableUpload, storage.create} × {standard, zonal}`.
- `iceberg/gcp/src/test/java/org/apache/iceberg/gcp/gcs/GcsCasViaRewriteProbe.java`
  — focused probe on the partial-publish bug, `storage.copy/rewrite`,
  `Storage.moveBlob`, and a small latency comparison.

### Findings

- **Direct write to the target is unsafe on Rapid.** The only
  zonal-compatible write surface is `storage.blobAppendableUpload(...
  generationMatch(g))`. Probes confirmed it accepts the `generationMatch`
  precondition and rejects stale generations cleanly with
  `StorageException(412, FAILED_PRECONDITION)`. But: the server
  **silently drops `crc32cMatch`** on this path, and a writer that
  encounters an exception mid-write and falls through to `close()`
  publishes whatever bytes have been written as the new generation —
  with no server-side integrity check. Setting
  `BlobAppendableUploadConfig.CloseAction.CLOSE_WITHOUT_FINALIZING`
  does **not** prevent the partial publish: the channel close still
  exposes flushed bytes at a new generation. There is no in-place
  defense against partial-write publication on the appendable path.
- **`storage.copy` / `rewrite` are rejected on Rapid.** Both source-
  and target-side return `StorageException(INVALID_ARGUMENT: Rapid
  storage class objects do not support rewrite)`. This rules out the
  classic "stage and rewrite" pattern.
- **`storage.create(BlobInfo, byte[])` is rejected on Rapid** with
  `INVALID_ARGUMENT: This bucket requires appendable objects.`
- **`Storage.moveBlob(MoveBlobRequest)` works on Rapid.** The
  `MoveBlobRequest` builder accepts `BlobSourceOption.generationMatch`
  and `BlobTargetOption.generationMatch`, so it implements full
  CAS-replace semantics. Probes confirmed: happy path replaces the
  target with the temp's bytes at a new generation and **deletes the
  temp atomically as part of the move**; stale destination generation
  is rejected with HTTP 412; concurrent readers polling the target
  during a move never observe an intermediate state (only `g_old` with
  full old bytes or `g_new` with full new bytes).
- **`moveBlob` also works on standard non-HNS buckets.** The API is
  not gated on hierarchical namespace; the SDK accepts a non-HNS bucket
  and the server performs the move-replacement atomically. So in
  principle the same code path could serve both bucket types.

### The pattern

```
1. write payload to bucket/<uuid>.tmp via blobAppendableUpload(doesNotExist) + finalizeAndClose
   - on any exception: target is untouched; clean up the temp best-effort and propagate
2. verify the temp's CRC32C client-side
   - read up.getResult().get().getCrc32c() and compare to the expected CRC
   - if mismatched (the partial-publish vector that bypasses server-side checks):
     abandon, target is still untouched
3. storage.moveBlob(MoveBlobRequest{
       source           = bucket/<uuid>.tmp,
       target           = bucket/catalog,
       sourceOptions    = [generationMatch(tempGen)],
       targetOptions    = [generationMatch(g_expected)]})
   - 412 → CAS failure → caller refreshes target, retries from step 1
   - 2xx → target is now at g_new with the temp's bytes; temp is deleted
4. (no step 4 — moveBlob handles cleanup)
```

The pattern's exception safety comes from step 2's "never publish to
the live target until we've confirmed the staged content is what we
intended." Any failure before step 3 leaves the target unchanged. A
failure during step 3 either rejects the move atomically (412 or other
non-2xx) or completes it; the server does not expose intermediate
state to readers.

### Performance

Latency comparison, N=10 fresh-target writes per cell, ~1 KB payload,
single-threaded, run from a workstation against the US-WEST1 standard
bucket and US-WEST4 zonal Rapid bucket used by the catalog-bench
harness. The workstation is well outside the bucket regions, so the
absolute numbers are dominated by WAN RTT; the *ratios* between cells
on the same connection are the meaningful signal. This is order-of-
magnitude characterization, not a proper benchmark.

| path                                         | min   | p50   | p90   | mean  | max   |
| -------------------------------------------- | ----- | ----- | ----- | ----- | ----- |
| standard, direct `blobWriteSession` (HTTP)   | 157ms | 173ms | 241ms | 178ms | 241ms |
| standard, stage-and-move (HTTP+gRPC)         | 224ms | 271ms | 360ms | 273ms | 360ms |
| Rapid, stage-and-move (gRPC throughout)      | 386ms | 444ms | 840ms | 495ms | 840ms |
| Rapid, direct `blobAppendableUpload` (gRPC)* | 234ms | 266ms | 875ms | 324ms | 875ms |

\* Direct appendable on Rapid is unsafe under exception paths (see the
findings above) — this row is included only as a baseline for the cost
of the moveBlob round trip.

Reading the table:

- **Standard buckets should keep the existing direct path.**
  Stage-and-move works on standard, but the extra `moveBlob` round trip
  costs roughly one additional RTT (~100ms in this measurement) and we
  gain nothing in correctness over what `blobWriteSession` already
  provides. Keep the existing code on standard.
- **Rapid stage-and-move is the safe path** at roughly 2.5× the p50
  latency of standard direct (444ms vs 173ms). The extra cost is one
  `moveBlob` round trip plus appendable-upload setup overhead; both
  scale with RTT, so the ratio holds across regions even if the
  absolute numbers shrink dramatically when run from in-region clients.
- **Rapid is currently the slowest substrate for the FileIO catalog**
  in single-threaded steady-state writes, on top of being the most
  complex to implement safely. Its sub-millisecond *read* latency is
  still the differentiator that justifies it for some workloads — but
  if catalog-write throughput is the bottleneck, standard GCS or one
  of the other providers is the better fit.

### Production plan (sketch)

`GCSFileIO` will detect bucket type lazily in `PrefixedStorage` (via
`storage.get(bucket).getLocationType().equals("zonal")`, cached
per-bucket) and fork in `newOutputFile(InputFile replace)`:

- standard buckets: existing `GCSAtomicOutputStream` path through
  `blobWriteSession`, untouched.
- zonal buckets: new write path that performs the four-step
  stage-and-move described above.

The gRPC `Storage` client is required for both `blobAppendableUpload`
and `moveBlob`, so zonal prefixes will hold a second client built via
`StorageOptions.grpc()`. The two probe classes will be deleted once
the production path lands and is covered by a `GcsFileIOAtomicRapidTest`
that extends the existing contract test.

`GCSFileIO.supportsAppend()` stays `false`. The catalog file format on
Rapid follows the CAS-only commit policy, just as it does on standard
GCS today.

## April 2026: appendable-object PoC (atomic APPEND ruled out)

### Background

`SupportsAtomicOperations` requires backends to implement two
strategies:

- **`CAS`** — atomically replace an object with a new payload, gated on
  the prior object's ETag/generation.
- **`APPEND`** — atomically append bytes to an existing object at a
  specific offset, with the same single-winner property under
  contention. Optional; backends may report `supportsAppend() = false`
  and the catalog will coerce its commit policy to CAS-only.

Standard GCS objects are immutable; `GCSFileIO.supportsAppend()`
returns `false` today. The ADLS provider implements APPEND via a blob
lease around a conditional append+flush pair (see
`docs/atomic_io.md`). S3 Express implements it via PutObject with
`writeOffsetBytes` plus `if-match` ETag. Rapid Storage's appendable
objects looked like the GCS analog.

### The contract under test

The contract test that matters for this investigation is in
`api/src/test/java/org/apache/iceberg/io/SupportsAtomicOperationsContractTest.java`:

```
@Test
void appendRaceConcurrent() {
  // 4 writers, each with its own FileIO/SDK client, all pinned to the
  // same baseline snapshot, rendezvous at a barrier and call
  // writeAtomic simultaneously. Exactly one must commit; the other
  // three must throw AppendException.
}
```

Plus a lifecycle test that walks `create -> APPEND -> CAS-replace ->
APPEND` and at every stage asserts that a concurrent writer pinned to
the prior snapshot surfaces the appropriate exception. The atomicity
guarantee is "exactly one of N concurrent writers commits, the rest
fail visibly" — silent data loss for the apparent winner is a contract
violation.

### What was probed

A self-contained PoC class
(`gcp/src/test/java/org/apache/iceberg/gcp/gcs/RapidStoragePoC.java`,
preserved at the SHA referenced in [Reproducibility](#reproducibility))
exercised the API directly against a zonal Rapid bucket. The probes:

| # | Probe | Outcome |
|---|---|---|
| 01 | API discovery — `Storage.blobAppendableUpload` exists and returns a real generation | ok |
| 02 | Happy-path append: single writer, two flushes, finalize | ok |
| 03 | Generation across appends: open, flush, close-without-finalize, take over, finalize | initially failed (replace, not append); fixed by carrying generation on `BlobId` |
| 04 | Two streams open concurrently against the same object via the public SDK | the SDK rejects the second open locally with an internal `IllegalArgumentException` before the server arbitrates |
| 05 | `BlobWriteOption.generationMatch(staleGen)` after a CAS-replace | rejected with `FAILED_PRECONDITION` (HTTP 412) — clean signal |
| 06 | Resume offset integrity after takeover (public SDK) | replaces rather than appends; SDK lacks a way to plumb `write_handle` cross-session |
| 07 | Finalize, then CAS-replace via fresh `blobAppendableUpload(generationMatch(finalizedGen))` | ok; stale-gen retry rejected with `FAILED_PRECONDITION` |
| 08 | Fresh-stream race: 4 writers, each `BlobWriteOption.doesNotExist()` or `generationMatch(baseGen)`, barrier-rendezvoused | exactly 1 winner / 3 losers; losers throw `IllegalStateException("…TERMINAL_ERROR")`. **Server-side fence works for fresh streams** |
| 09 | Capture `write_handle` and `routing_token` from session 1 via package-friend access to `BidiUploadState.BaseUploadState` | ok; ~961-byte `BidiWriteHandle` populated, `routingToken` empty, `lastOpenArguments.ctx` carries routing headers |
| 10 | Raw gRPC takeover with the captured `write_handle` (path bypasses the SDK's missing public surface) | ok; bytes land at the expected offset, returns `persistedSize` updated |
| 11 | Two concurrent takeovers presenting the **same** `write_handle` at the same `write_offset` | **silent last-writer-wins.** Both clients receive `WIN persistedSize=N`; only one writer's bytes persist; the loser has no signal |
| 12 | `write_handle` durability: capture, sleep 127 s, take over | ok; handle survives idle gap of at least minutes |
| 13a | Fresh-stream takeover **without** `write_handle`, single writer | ok; server accepts `AppendObjectSpec(bucket, object, generation)` without a handle |
| 13b/13c | Two concurrent fresh-stream takeovers (no handle), with and without `state_lookup` | non-deterministic. ~1 in 3 runs fence cleanly with `FAILED_PRECONDITION: A different writer has become the exclusive writer of this object.`; the rest produce **silent data loss** — winner reports `WIN persistedSize=8`, loser reports `OUT_OF_RANGE current size '8'`, post-read `metaSize=4` |
| 14 | Does generation advance per flush? | no. `genAfterFirstFlush == genAfterSecondFlush`; only finalize advances generation |

### Findings in detail

#### `write_handle` is a resume token, not a fence

The Java SDK's public surface (`BlobAppendableUpload.getResult()`)
returns only a `BlobInfo`. The server actually returns a
`BidiWriteObjectResponse` per write, and that response contains a
`write_handle` field — an opaque ~961-byte token. The SDK stores it
internally on `BidiUploadState.BaseUploadState.writeHandle` but does
not expose it.

A package-friend backdoor class in test sources can read that field
and feed it into a fresh `bidiWriteObjectCallable().call(ctx)` so a
new process can resume an upload at the persisted offset (probe 10).
The handle survives at least minutes of idle (probe 12). It is what
its name says: a **resumable-upload session token**, identical in
spirit to the resume tokens that have always existed for standard GCS
resumable uploads.

What the handle does **not** do is fence. The protocol is built around
the assumption that a client presenting `(write_handle,
write_offset=K)` is *the same logical writer retrying* — so when the
server's persisted size is already at or past K, the request is
treated as an idempotent replay and the server ack'd it without
rewriting. Two different clients presenting the same handle at the
same offset both receive success responses; only one client's bytes
actually land. The loser cannot tell from the response that they lost.

This is correct behavior for "single client retrying through a flaky
network" and incorrect for "multiple clients fencing each other".

#### Generation does not advance per flush

`BlobInfo.getGeneration()` is stable across flushes within an
unfinalized appendable's lifetime; only finalize advances it
(probe 14). So `AppendObjectSpec.if_generation_match` cannot
distinguish two writers that opened against the same captured state —
they both match.

#### Concurrent fresh-stream takeovers are unsafe

Removing `write_handle` from the request was the most plausible way
to recover the fence — it forces the server back into a "fresh
stream" code path where probe 08's 4-writer race produced clean 1/3
arbitration. But the same shape with takeover semantics
(`AppendObjectSpec(bucket, object, generation)` with no handle) is
non-deterministic:

- ~1 in 3 runs: the server selects an exclusive writer; that writer's
  bytes commit; the loser receives `FAILED_PRECONDITION: A different
  writer has become the exclusive writer of this object.`. This is
  the outcome we'd build a fence around.
- ~2 in 3 runs: both streams stay tentatively open; one writer's
  flushed bytes are visible to the other (which observes them as
  "current size"); when neither stream cleanly wins exclusivity, both
  writers' tentative bytes are rolled back. The "winning" writer's
  response — `WIN persistedSize=8` — is a **lie**: by the time the
  test re-reads the object, `metaSize` has reverted to its value
  before the contention.

`state_lookup=true` mildly affects the frequency but not the
existence of the bad pattern (probe 13c, 4/5 silent-loss without it
versus 2/3 with it). The protocol does not guarantee that contention
between two fresh-stream takeovers resolves with a winning side.

#### CAS-replace works for the happy path only — see May 2026

Probe 07 confirms `blobAppendableUpload(... generationMatch(g)) +
finalize` reliably replaces an object atomically *under happy-path
single-writer conditions*, with stale-gen retries cleanly rejected as
`FAILED_PRECONDITION` (HTTP 412). At the time this was written, the
intent was to use that pattern as the production CAS-replace path on
Rapid.

The May 2026 follow-up showed this is **not safe** for our use case:
when a writer encounters an exception mid-write and falls through to
`close()`, the truncated bytes are published as the new generation
with no server-side integrity check (CRC32C is silently dropped on
this path). `CloseAction.CLOSE_WITHOUT_FINALIZING` does not prevent
this. The production path on Rapid uses stage-and-move via
`Storage.moveBlob` instead; see the May 2026 section above.

### Why the design is what it is

Rapid Storage's appendable objects are designed for **one long-lived
writer per object**. The `write_handle` mechanism, the lack of
per-flush generation advancement, the protocol's idempotent treatment
of replayed offsets — all of these make sense if you assume the
client is a single logging or telemetry pipeline whose biggest
operational concern is recovering from network errors mid-upload. The
docs' line "Appendable objects can only have one writer at a time"
expresses that assumption directly.

A multi-writer fencing primitive would require something the protocol
does not currently expose: per-flush generation advancement, an
explicit `claim_exclusive` flag on the request, or some other
server-side mechanism that converts the "exclusive writer" arbitration
we observe ~1 in 3 races into a deterministic outcome. None of these
is in the public proto today.

## April-2026 recommendation (superseded)

The original recommendation at the bottom of the April 2026 PoC was
"keep `supportsAppend()` false; treat Rapid like standard GCS for
writes." The first half (no append) still stands. The second half is
**superseded** by the May 2026 finding that "writes on Rapid go through
the same CAS-replace pattern as standard GCS" was incorrect: the SDK
surfaces standard GCS uses (`blobWriteSession`, `storage.create`,
`copy/rewrite`) are all rejected by zonal Rapid buckets. The actual
production path on Rapid is stage-and-move via `Storage.moveBlob`;
see the May 2026 section at the top of this document.

The April 2026 recommendation about incremental commits requiring an
external serializer remains correct — `supportsAppend()` cannot be
flipped to `true` without one, regardless of how the CAS-replace path
is implemented.

## Reproducibility

### May 2026 probes (CAS-replace surface survey + stage-and-move)

`GcsCasTransportProbe` and `GcsCasViaRewriteProbe` live in
`iceberg/gcp/src/test/java/org/apache/iceberg/gcp/gcs/` and the
test-only `google-cloud-storage:2.68.0` pin lives in `iceberg/build.gradle`
on the `iceberg-gcp` module. They will be deleted (or pared down to a
single regression smoke test) when the production stage-and-move
path lands; until then they are runnable as:

```
STANDARD_BUCKET=<std-bucket> RAPID_BUCKET=<zonal-bucket> \
GOOGLE_CLOUD_PROJECT=<project> \
  ./gradlew :iceberg-gcp:test \
  --tests 'org.apache.iceberg.gcp.gcs.GcsCasTransportProbe' \
  --tests 'org.apache.iceberg.gcp.gcs.GcsCasViaRewriteProbe' \
  -x generateGitProperties --info
```

The `[probe.*]` lines in stdout and the per-class summary tables are
the load-bearing observations. Cells whose required bucket env var is
unset are skipped via `Assumptions.assumeTrue`.

### April 2026 PoC (atomic-APPEND investigation)

The PoC class
(`iceberg/gcp/src/test/java/org/apache/iceberg/gcp/gcs/RapidStoragePoC.java`),
the package-friend backdoor
(`iceberg/gcp/src/test/java/com/google/cloud/storage/RapidStorageBackdoor.java`),
and the SDK version bump
(`iceberg/build.gradle` testImplementation pin to
`google-cloud-storage:2.68.0`) live at commit
`d6fd260bb` in this repository's history. They are
not present in `HEAD` because they include intentionally-failing tests
that document the silent-data-loss outcome and would block CI; they
are preserved in history so the experiments are reproducible against
a real Rapid bucket if the protocol's behavior ever changes.

To rerun (requires a Rapid bucket and `gcloud auth
application-default login`):

```
git checkout d6fd260bb -- \
  iceberg/build.gradle \
  iceberg/gcp/src/test/java/com/google/cloud/storage/RapidStorageBackdoor.java \
  iceberg/gcp/src/test/java/org/apache/iceberg/gcp/gcs/RapidStoragePoC.java

RAPID_BUCKET=<bucket> GOOGLE_CLOUD_PROJECT=<project> \
  ./gradlew :iceberg-gcp:test \
  --tests org.apache.iceberg.gcp.gcs.RapidStoragePoC \
  -x generateGitProperties --info
```

The `[poc.NN]` lines in stdout are the load-bearing observations. Probes
03/06/11/13b/13c are intentionally-failing under the current GCS
protocol; their failure messages are the data points cited above.
