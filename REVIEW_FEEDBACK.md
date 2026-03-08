# Review Feedback Remediation Plan

## Summary

Two rounds of reviews identified issues on the `cmpmap` branch. This document
tracks each finding, its verification status, and the fix applied.

All findings verified against source code. Last updated: 2026-03-08.

---

## Round 1 Findings (original reviews)

### Fix 1: `RemapFunctionWithRemapper` serialization — **DONE**

**Severity:** High
**Files:** `SparkCompactionConflictResolver.java` in `spark/v3.5/` and `spark/v4.0/`

**Problem:** `RemapFunctionWithRemapper` captured a non-serializable
`PositionDeleteRemapper` field. In distributed Spark execution, Java serialization
of the closure fails with `NotSerializableException`.

**Fix:** Field marked `transient`, constructor serializes `CompactionMap` objects to
`byte[][]` via `CompactionMaps.toBytes()`, `call()` lazily reconstructs the remapper.
Both `RemapFunction` and `RemapFunctionWithRemapper` use this pattern.

**Tests:** `TestRemapFunctionSerializability.java` (4 tests)

---

### Fix 2: Multi-file position delete validation — **DONE**

**Severity:** Medium-High
**File:** `CompactionMapValidator.java`

**Problem:** `findConflicts()` only checked `referencedDataFile != null`, silently
skipping multi-file position deletes. Stale physical addresses could commit
unchallenged, causing missed deletions.

**Fix:** Conservative conflict detection: when a multi-file position delete exists
and compacted files are present, treat all compacted files as conflicts. Position
deletes are writes (physical addresses), not reads; SERIALIZABLE isolation's
structural-change exemption applies only to reads.

**Tests:** `TestCompactionMapValidatorMultiFileDeletes.java` (2 tests), updated
assertions in `TestSerializableIsolationWithCompaction.java` (V2 expects conflict,
V3 DVs targeting non-compacted files succeed).

---

### Fix 3 + Fix 7: Multi-file position tracking — **DONE**

**Severity:** Critical
**Files:** `PositionTrackingDataWriter.java` in `spark/v3.5/` and `spark/v4.0/`

**Problem:** `recordBufferedMappingsWithActualPaths()` assigned all mappings to
`files[0].location()`, producing wrong compaction maps when the writer rolled over.

**Fix:** Build cumulative record-count boundaries from output files array,
binary-search each mapping's `targetPos` to the correct output file, adjust
positions to be file-local. TODO removed.

**Tests:** `TestPositionTrackingMultiFileAssignment.java` (4 tests)

---

### Fix 4: Fallback `sourceSnapshotId` uses sequence number — **DONE**

**Severity:** Medium
**File:** `BaseRewriteFiles.java`

**Problem:** `generateAndWriteCompactionMap()` used `base.lastSequenceNumber()` as
fallback `sourceSnapshotId`. Sequence numbers and snapshot IDs are distinct domains.

**Fix:** Use `base.currentSnapshot().snapshotId()` when available, -1L sentinel
when no snapshots exist.

**Tests:** `TestCompactionMapSnapshotIdFallback.java` (2 tests)

---

### Fix 5 + Fix 6: Spark 4.0 skipped tests and phantom doc — **DONE**

**Severity:** High
**File:** `TestBinPackWithPositionTracking.java` (Spark 4.0)

**Problem:** Two tests unconditionally skipped via `assumeThat(false).isTrue()`,
stub helper returned empty list, class Javadoc referenced nonexistent doc file.

**Fix:** Ported working helpers from Spark 3.5, removed skips, removed stub,
removed phantom doc reference.

---

## Round 2 Findings (reviews of fe7fe00a9)

### Fix 8: `deleteFileCount()` excludes multi-file deletes from safety cap — **DONE**

**Severity:** Medium-High (review1 §2.2)
**File:** `DeleteConflictInfo.java`

**Problem:** `deleteFileCount()` returned only `conflictingDeleteFiles.size()`,
excluding multi-file position deletes. But `SparkCompactionConflictResolver.resolve()`
appends `multiFilePositionDeletes()` to the resolver work. An operator setting
`write.compaction.resolve-delete-conflicts.max-files=50` would cap only file-scoped
conflicts while multi-file deletes are resolved without limit.

**Fix:** `deleteFileCount()` now returns
`conflictingDeleteFiles.size() + multiFilePositionDeletes.size()`, reflecting the
actual work the resolver will perform.

**Tests:** Updated assertions in `TestCompactionConflictDetector.java` (2 tests).

---

### Fix 9: Dead and inconsistent `findConflictingDeletes` — **DONE**

**Severity:** Low (review1 §3, review2 §remaining concern)
**File:** `CompactionMapValidator.java`

**Problem:** `findConflictingDeletes()` only checked `referencedDataFile != null`,
inconsistent with the now-conservative `findConflicts()`. Both reviewers flagged this.

**Fix:** Deleted. Zero callers in production code (confirmed by grep). Dead code
with inconsistent semantics that could mislead future developers.

---

### Fix 10: Fallback map generation soundness precondition — **DONE**

**Severity:** Medium (review1 §2.3)
**Files:** `RewriteDataFilesCommitManager.java`, `SparkRewriteDataFilesCommitManager.java`
(Spark 3.5 + 4.0), `BaseRewriteFiles.java`

**Problem:** Fallback map generation (no explicit position tracking) assumes source
files are concatenated in iteration order into the target file. This is only sound
for bin-pack rewrites; sort/z-order rewrites rearrange rows and would produce
incorrect maps. The assumption was undocumented and unenforced.

**Fix:** Added `Preconditions.checkState()` verifying that the target file's record
count equals the sum of source record counts. A mismatch indicates the writer
filtered, reordered, or duplicated rows, which invalidates the sequential-offset
assumption. Added documentation of the soundness precondition.

---

### Fix 11: Misleading test method name — **DONE**

**Severity:** Low (review1 §3)
**File:** `TestCompactionMapValidatorMultiFileDeletes.java`

**Problem:** Method named `testMultiFilePositionDeleteConflictNotDetected` while
asserting that the conflict IS detected.

**Fix:** Renamed to `testMultiFilePositionDeleteConflictDetectedConservatively`.

---

### Review1 §2.1 — `RemapFunctionWithRemapper` serialization "still present" — **FALSE**

The reviewer cited lines 489-504 (the `call()` method) but did not check the field
declaration at line 468 (`private transient PositionDeleteRemapper remapper`) or the
constructor at lines 470-488 (which serializes to `byte[][]`). Fix 1 was already
applied and is correct.

---

## Not actionable

### Review1 §4: Duplication across Spark 3.5/4.0

Both reviewers noted that `SparkCompactionConflictResolver` and the fallback map
generation are duplicated across Spark versions. This is inherent to the Iceberg
project structure (each Spark version is a separate module with its own copy).
Consolidation would require cross-version abstraction that the project does not
currently support.

### Review1 §6: Benchmark coupling

Benchmarks directly use core strategy implementations. This is intentional — the
benchmarks measure the actual code, not a parallel implementation.
