# Atomic FileIO on GCS Rapid Storage

This document records an empirical investigation into whether GCS Rapid
Storage's appendable-object API can implement the `Strategy.APPEND`
half of `SupportsAtomicOperations`. The short answer is **no, not safely
under concurrent writers**. The longer answer — what works, what
doesn't, and why — is below.

## Summary

Google Cloud Storage shipped *Rapid Storage* in early 2026: zonal
buckets with a `RAPID` storage class and a new "appendable objects"
gRPC API (`Storage.blobAppendableUpload`). The API exposes
`open() / write() / flush() / finalizeAndClose()` semantics over a
mutable-tail object that other clients can read while it grows. On
paper this looked like the missing primitive that would let
`GCSFileIO.supportsAppend()` return `true` — letting the FileIO catalog
use the same checkpoint+log commit protocol on GCS that ADLS and S3
Express already enable.

After running 14 probes against a real Rapid bucket, the conclusion is:

- **Single-writer happy paths work cleanly.** Open, write, flush,
  finalize. No surprises.
- **`write_handle` is a resumable-upload session token, not a fence
  token.** Two clients presenting the same handle at the same
  `write_offset` silently last-writer-wins; the loser receives a
  success response but its bytes never persist.
- **Generation does not advance per flush.** It only moves at finalize.
  So `if_generation_match` on `AppendObjectSpec` cannot fence two
  appenders pinned to the same captured state — both will match.
- **Fresh-stream concurrent contention is non-deterministic.** Some
  races fence cleanly with `FAILED_PRECONDITION: A different writer has
  become the exclusive writer of this object.`; others produce silent
  data loss in which both writers' tentative bytes are visible to the
  server, the loser sees `OUT_OF_RANGE` quoting them, and then the
  bytes evaporate without committing.
- **CAS-replace via the appendable API works.**
  `blobAppendableUpload(... generationMatch(g)) + finalize` is reliable
  and gives us the same atomic-replace semantics we already have on
  standard GCS — at the cost of full-object rewrites per commit, which
  defeats the point of using an appendable in the first place.

The protocol's docs say "Appendable objects can only have one writer at
a time." We had read that as a constraint to navigate; the experiments
show it is the *design assumption* the entire protocol is built around.
A single long-lived writer with `write_handle` to recover from network
blips is the supported model. Multiple short-lived writers fencing each
other through the storage layer is not.

**Recommendation:** keep `GCSFileIO.supportsAppend()` returning `false`.
The catalog falls back to CAS-only on Rapid the same way it does on
standard GCS. Rapid is still useful for its read characteristics
(sub-millisecond latency, ~15 TB/s aggregate) but offers no incremental-
append benefit for our atomic write protocol.

## Background

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

## The contract under test

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

## What was probed

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

## Findings in detail

### `write_handle` is a resume token, not a fence

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

### Generation does not advance per flush

`BlobInfo.getGeneration()` is stable across flushes within an
unfinalized appendable's lifetime; only finalize advances it
(probe 14). So `AppendObjectSpec.if_generation_match` cannot
distinguish two writers that opened against the same captured state —
they both match.

### Concurrent fresh-stream takeovers are unsafe

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

### CAS-replace works

Probe 07 confirms `blobAppendableUpload(... generationMatch(g)) +
finalize` reliably replaces an object atomically, with stale-gen
retries cleanly rejected as `FAILED_PRECONDITION` (HTTP 412). This is
the same shape as standard GCS CAS via
`storage.create(... generationMatch(g))` — except that on Rapid the
non-appendable `storage.create` path is forbidden (the bucket rejects
it with `INVALID_ARGUMENT: This bucket requires appendable objects.`),
so even CAS must go through the appendable API. Either way: every
commit pays for a full-object rewrite. The Rapid-specific advantage
of "incremental-cost growth" does not survive contention.

## Why the design is what it is

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

## Recommendation

- **`GCSFileIO.supportsAppend()` stays `false`.** No change to the
  main code is warranted from this work. The FileIO catalog already
  coerces to CAS-only when `supportsAppend()` is `false`; that path
  works on Rapid, as it does on standard GCS.
- **Rapid Storage is still worth using for its read characteristics**
  in catalogs that read the object hot — sub-millisecond latency and
  high aggregate throughput are real wins for catalog-file fetches.
  Writes go through the same CAS-replace pattern as standard GCS.
- **If we ever want incremental commits on Rapid**, the prerequisite
  is an external serializer — a leader-elected catalog process, or a
  separate lock service — that ensures Rapid's protocol sees only one
  client at a time. That is an architectural change, not a config
  flag.

## Reproducibility

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
