# Compaction maps review (cmpmap branch)

## Scope and limitations

- Reviewed implementation and tests in `api/core` and `spark` relevant to compaction maps and delete remapping.
- Reviewed benchmark coupling only (not benchmark performance code quality).
- Could not fetch upstream tag `apache-iceberg-1.10.1` in this environment due network restrictions (GitHub access returned HTTP 403), so this review is against the checked-out branch contents only.

## Findings

### 1) **Critical correctness issue: position tracking assigns all mappings to the first output file when a task writes multiple files**

In Spark position-tracking writers (both 3.5 and 4.0), buffered `(sourceFile, sourcePos, targetPos)` mappings are all committed to `files[0].location()` from `TaskCommit`.

This is explicitly documented as TODO, but currently active behavior. If file rollover/splitting occurs inside one writer task, mappings for rows written to later files are incorrectly attributed to the first file, producing an invalid compaction map and incorrect delete remapping.

- Evidence: `recordBufferedMappingsWithActualPaths` uses `files[0]` and applies it to all buffered mappings.
- Affected files:
  - `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/PositionTrackingDataWriter.java`
  - `spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/source/PositionTrackingDataWriter.java`

**Impact:** wrong target file in remapped deletes under multi-output-per-task writes.

**Suggested fix:** track per-output-file row ranges (or get row-to-file boundaries from writer commit metadata) and assign buffered mappings by output position interval, not a single file.

---

### 2) **Correctness issue: fallback source snapshot ID uses sequence number, not snapshot ID**

`BaseRewriteFiles.generateAndWriteCompactionMap` derives `sourceSnapshotId` as:

- `startingSnapshotId` if set, else
- `snapshot.snapshotId()` if available, else
- `base.lastSequenceNumber()`

The fallback uses a sequence number in a field intended to store snapshot IDs. This can encode invalid identity in compaction maps and break chain interpretation/debugging in cases where `snapshot` is null and `startingSnapshotId` is unset.

- Evidence: `base.lastSequenceNumber()` is used to populate `sourceSnapshotId`.
- Affected file:
  - `core/src/main/java/org/apache/iceberg/BaseRewriteFiles.java`

**Impact:** metadata inconsistency and potential mis-linking of map provenance.

**Suggested fix:** use an actual snapshot ID fallback (e.g., current snapshot id when available), or require an explicit starting snapshot for map generation.

---

## Test completeness assessment

### What is strong

- Core has broad compaction-map/remapping coverage including builder, serialization, integration, chain behavior, conflict detection/resolution, DV paths, and strategy selection.
- Spark includes integration tests for rewrite actions with compaction maps and conflict-resolution flows.
- Benchmark coupling is guarded by dedicated tests that assert benchmark path calls production APIs (`PositionDeleteRemapper.remapPositionsBulkPrimitive`) rather than a parallel implementation.

### Gaps identified

1. **No regression test for multi-file-per-task position-tracking commit path**
   - There should be a Spark integration/unit test forcing writer rollover (single task emits multiple data files), then asserting per-row mapping targets correct output files.

2. **No test asserting snapshot-ID semantics in compaction map metadata under fallback path**
   - A targeted core test should verify `sourceSnapshotId` is always a valid snapshot ID, never a sequence number.

## Benchmark coupling verification

- Benchmark-core coupling is present and explicit in `benchmark/remapping-microbenchmark/src/test/java/org/apache/iceberg/benchmark/remapping/TestCoreApiCoupling.java`.
- The test suite validates that benchmark extraction/remapping paths are compatible with and routed through core remapper APIs, reducing risk of accidentally benchmarking a divergent in-benchmark implementation.


