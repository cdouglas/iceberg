# Review Feedback Remediation Plan

## Summary

Two independent reviews identified four confirmed issues on the `cmpmap` branch.
A subsequent audit found additional deferred work (TODOs, skipped tests, stub helpers,
phantom documentation) that must be resolved before this branch is shippable.

All findings verified against source code on 2026-03-08.

---

## Review Findings

### Fix 1: `RemapFunctionWithRemapper` serialization (Review 1, Finding 1)

**Severity:** High
**Files:** `SparkCompactionConflictResolver.java` in `spark/v3.5/` and `spark/v4.0/`

**Problem:** `RemapFunctionWithRemapper` implements `Serializable` but captures a
`PositionDeleteRemapper` field that does not implement `Serializable`. In distributed
Spark execution the closure is serialized to executors, causing `NotSerializableException`.

**Fix:** Mirror the pattern already used by `RemapFunction` in the same file:
- Change `remapper` field to `transient`
- Store the `CompactionMap` (which is Avro-serializable) instead
- Lazily initialize `PositionDeleteRemapper` on first `call()` invocation

**Tests added:** `TestRemapFunctionSerializability.java` (3 tests)

---

### Fix 2: Multi-file position delete validation gap (Review 1, Finding 2)

**Severity:** Medium-High (documented gap, not fixable in validator without content scanning)
**File:** `CompactionMapValidator.java`

**Problem:** `findConflicts()` only checks `referencedDataFile != null`, silently
skipping multi-file position deletes. `BaseRowDelta` inherits this gap, allowing
transactions with multi-file position deletes to pass validation even when they
reference compacted files.

**Resolution:** A conservative approach (flag all compacted files as conflicts when
multi-file position deletes exist) was implemented and then **reverted** because it
produces false positives that break SERIALIZABLE isolation for V2 tables. Position
deletes that do NOT reference compacted files are incorrectly flagged, causing
`CompactionConflictException` where the commit should succeed.

The gap cannot be fixed in the validator without reading delete file content (expensive
I/O during validation). Instead, `CompactionConflictDetector` (which scans manifest
content) handles this case correctly at the Spark action level.

**Status:** Documented gap with test. The `findConflicts()` Javadoc explains the gap
and the reason the conservative approach was rejected.

**Tests added:** `TestCompactionMapValidatorMultiFileDeletes.java` (2 tests: one
verifies file-scoped detection works, one documents the multi-file gap)

---

### Fix 3: Multi-file position tracking assigns all mappings to first output file (Review 2, Finding 1)

**Severity:** Critical
**Files:** `PositionTrackingDataWriter.java` in `spark/v3.5/` and `spark/v4.0/`

**Problem:** `recordBufferedMappingsWithActualPaths()` uses `files[0].location()` for
all buffered mappings. If the writer rolls over to multiple output files, mappings for
rows in later files are incorrectly attributed to the first file.

**Fix:** Track per-output-file row boundaries. In `recordBufferedMappingsWithActualPaths`,
assign each buffered mapping to the correct output file by matching its `targetPos`
against cumulative row counts of each output file.

**Tests added:** `TestPositionTrackingMultiFileAssignment.java` (4 tests)

---

### Fix 4: Fallback `sourceSnapshotId` uses sequence number (Review 2, Finding 2)

**Severity:** Medium
**File:** `BaseRewriteFiles.java`

**Problem:** `generateAndWriteCompactionMap()` line 205 uses `base.lastSequenceNumber()`
as fallback for `sourceSnapshotId`. Sequence numbers and snapshot IDs are distinct
identifier domains; this produces semantically invalid compaction map metadata.

**Fix:** Use `base.currentSnapshot().snapshotId()` as fallback when available, or a
sentinel value (-1L) when there is truly no snapshot. Never use a sequence number.

**Tests added:** `TestCompactionMapSnapshotIdFallback.java` (2 tests)

---

## Deferred Work Audit (TODOs, Skipped Tests, Stubs)

The following items were found during a sweep of the codebase. Every one of these
represents deferred work that must be completed or explicitly removed. There is no
"later" — this branch ships complete or not at all.

### Fix 5: Spark 4.0 `TestBinPackWithPositionTracking` — two tests permanently skipped

**Severity:** High
**File:** `spark/v4.0/spark/src/test/java/org/apache/iceberg/spark/actions/TestBinPackWithPositionTracking.java`

**Problem:** Two tests use `assumeThat(false).isTrue()` to unconditionally skip:

1. `testBinPackGeneratesCompactionMapWithPositionDeletes()` (line 158-161)
   — TODO says "Implement position delete helper for comprehensive testing"
2. `testNToMCompactionScenario()` (line 196-199)
   — TODO says "Complex partitioned write scenario - requires proper partition value generation"

Meanwhile, the Spark 3.5 version of this file has **fully working implementations** of
both the `writePosDeletesToFile` helper and the test logic. The v4.0 file has a stub
helper that returns an empty list (line 429-434).

**Fix:**
- Port the working `writePosDeletesToFile` / `writePosDeletes` helper from the Spark 3.5
  version to v4.0 (it uses `GenericAppenderFactory` + `PositionDeleteWriter`, which
  should work identically in 4.0)
- Remove the `assumeThat(false).isTrue()` skips
- Remove the stub helper
- If the class-level doc claim about "schema validation issues during Parquet writer
  creation" is real, diagnose and fix the root cause rather than skipping tests around it
- Remove the class-level TODO doc comment (lines 59-61)

---

### Fix 6: Phantom documentation reference

**Severity:** Low (but embarrassing)
**File:** `spark/v4.0/spark/src/test/java/org/apache/iceberg/spark/actions/TestBinPackWithPositionTracking.java` line 61

**Problem:** The class Javadoc references `spark/v4.0/docs/position_tracking_challenges.md`
which does not exist. The `spark/v4.0/docs/` directory does not exist either. This is
a citation to a document that was never written.

**Fix:** Remove the reference. If there are genuine Spark 4.0 challenges worth
documenting, write the document; otherwise delete the dead link.

---

### Fix 7: TODO in `PositionTrackingDataWriter` (both Spark versions)

**Severity:** Critical (same as Fix 3 — this is the implementation site)
**Files:**
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/PositionTrackingDataWriter.java:192`
- `spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/source/PositionTrackingDataWriter.java:192`

**Problem:** `// TODO: Handle multiple target files if writer rolled over` marks the
exact line where `files[0].location()` is used for all mappings. This is Fix 3's
implementation site.

**Fix:** Implement multi-file assignment (see Fix 3). After fix, remove the TODO.
The implementation should:
1. Iterate `files[]` and build cumulative record-count boundaries
2. For each buffered mapping, binary-search `targetPos` to find the correct output file
3. Record each mapping with the correct target file path

---

## Priority Order

1. **Fix 3 + Fix 7** (same issue) — silently produces wrong compaction maps — **DONE**
2. **Fix 5** — entire test class is a facade in Spark 4.0; port working v3.5 code — **DONE**
3. **Fix 1** — runtime failure in distributed Spark execution — **DONE**
4. **Fix 2** — documented gap (conservative fix reverted, breaks SERIALIZABLE isolation)
5. **Fix 4** — metadata inconsistency (snapshot ID vs sequence number) — **DONE**
6. **Fix 6** — phantom doc reference — **DONE**

## Test Inventory (added for review gaps)

| File | Tests | Covers |
|------|-------|--------|
| `TestRemapFunctionSerializability.java` | 4 | Fix 1: Serialization gap + Avro bytes round-trip |
| `TestCompactionMapValidatorMultiFileDeletes.java` | 2 | Fix 2: Documents validator gap |
| `TestPositionTrackingMultiFileAssignment.java` | 4 | Fix 3/7: Multi-file assignment |
| `TestCompactionMapSnapshotIdFallback.java` | 2 | Fix 4: Snapshot ID fallback |

All 12 tests pass. All compaction-related tests pass (256+ tests).
