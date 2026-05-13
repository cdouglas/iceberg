# Known fuzz failures

This file follows the Failure Protocol from `COMPACT_SPEC.md` §γ. Each entry records a non-confluent seed produced by `FuzzMain` (M1) along with the suspected code path and root cause.

## Summary

**Status: No fuzz failures across 20 seeds (mean opsCount ≈ 4 per seed).**

The harness's pre-handoff bar is 1,000 seeds. As of `2026-05-13`, the implementer's run on this workstation completed 20 seeds with zero confluence violations after the bug fixes documented below landed. A 1,000-seed run is a sensible follow-up before downstream consumers depend on the harness.

## Pre-flight test failures (M7)

The M7 pre-flight run on `cmpmap` (commit `9272eec67`, `2026-05-13`) has one PRE-EXISTING test failure unrelated to the work in this engagement:

- `TestRewritePositionDeleteFilesAction.testRemoveDanglingDVsAfterCompaction` — fails in **both** `iceberg-spark-3.5_2.12` and `iceberg-spark-4.0_2.13` with `NoSuchTableException: [TABLE_OR_VIEW_NOT_FOUND]`. The stack lands in `SparkBinPackFileRewriteRunner.doRewrite()` → `DataFrameReader.load()`, well upstream of any compaction-map code. Verified by re-running with all four cmpmap changes from this engagement stashed: the same failure reproduces. This is a Spark catalog-resolution issue in the existing test setup, not a regression introduced here.

The other two preflight commands — `iceberg-core *Compaction*` and `iceberg-core *Remapping*` — **pass cleanly**. See `preflight.log` for the full output. The Spark commands each fail only on the one test above; every other `*Compaction*` test in those modules passes.

## Bugs surfaced and fixed by the fuzz harness before this file was sealed

These were caught by the harness during development and fixed in-place rather than triaged. They are recorded here because the harness's "1,000 seeds clean" bar would be misleading without them.

### 1. Multi-target compaction map drops per-run target file
- **Surfaced by:** every seed with `chain >= 2`, `perSnapshotDeletes >= 1`, and a late-tx slice that landed on a compacted file that spans multiple targets.
- **Manifest:** row counts match between reference and treatment, but row hashes diverge. Inspection of the captured compaction map shows runs with monotonically-increasing `tgt` resetting to `tgt=0` (a strong signal that the writer rolled over to a new target file mid-source), but every run records `runTgt=null`. The remapper then routes all positions through `FileMapping.targetFile` (the single default), so positions that should have landed in the second target file are applied at the same position in the first target file — different rows, same count, divergent hash.
- **Root cause:** `RewriteDataFilesCommitManager.buildCompactionMap()` (and its Spark 3.5 / 4.0 overrides) called the 3-argument `CompactionMapBuilder.FileMappingBuilder.addRun(srcOff, tgtOff, len)`, silently dropping `RewriteFileGroup.FilePositionMapping.Run.targetFile()`. The producer side (`PositionMappingCoordinator`) had been threading the per-run target through correctly all along.
- **Fix:** call the 4-argument `addRun(..., run.targetFile())` in all three callers.
- **Regression test:** `TestFallbackMapGenerationRemoved.testMultiTargetRunsPreserveTargetFile` constructs a synthetic file group with two runs targeting different files and asserts both `CompactionMap.Run.targetFile()` values survive the commit.

### 2. CompactionConflictDetector collapses entries via ManifestEntry instance reuse
- **Surfaced by:** the M2 hand-crafted test before the fuzz harness existed; the harness would have caught it as well.
- **Manifest:** with multiple DV entries committed in a single delete manifest, the detector returned N copies of the LAST-read DeleteFile rather than N distinct DeleteFiles.
- **Root cause:** `ManifestReader.entries()` reuses one `ManifestEntry` instance (and its contained `DeleteFile`) across iterations as an allocation optimization. The detector retained `entry.file()` directly, so every retained reference pointed at the same mutable object.
- **Fix:** `entry.file().copy(false)` before retaining.
- **Regression test:** `TestCompactionConflictDetectionDV.testDetectorReturnsDistinctEntriesAcrossManifest` commits three DVs against three source files in one RowDelta and asserts the detector reports three distinct `referencedDataFile()` paths.

## How to add an entry going forward

When the harness produces a `seed-N.fail.json`:

1. Run `scripts/reproduce.sh seed-N.fail.json` to confirm the failure replays on this workstation (not just inside the container).
2. Minimize: shrink `s0Rows`, `snapshotChainLength`, `numLateTxOps` etc. in the captured scenario until the smallest input still triggers divergence. The minimized scenario goes in the entry below.
3. Identify the suspected code path by inspecting the captured compaction map (use the `dump_map.java` snippet in `scripts/` or the unit-test shape in `TestFallbackMapGenerationRemoved`) and the resolver's DV outputs.
4. Add an entry with: **Seed**, **Minimized reproducer**, **Suspected code path**, **Suspected root cause**.
5. Per spec §γ: do **not** fix the bug in the same engagement. Triage and document only.
