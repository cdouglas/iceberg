# Known fuzz failures (active)

This file tracks **active** failures only — entries are removed once a fix lands and the
linkage is captured in git history. See "Lifecycle" at the bottom of this file.

## Open failures from FuzzMain

### 1. Row-replacement deletes don't get remapped on v2 tables
- **Seeds**: 1 (12-row diff), 4 (39-row diff) on the default adversarial config (smoke run
  2026-05-14, seeds 0..4). Both are V2-only format with one or more `rowReplacement` ops in
  the late-tx schedule.
- **Manifest**: treatment has more rows than reference by exactly the cumulative
  `deletesPerOp` of the row-replacement op(s) in the scenario. Smell:
  `CompactionConflictDetector` either skips or misclassifies the delete portion of
  OVERWRITE-operation snapshots.
- **Suspected code path**: `CompactionConflictDetector.detectConflicts` interacts with
  `RowDelta`s that carry both `addRows` and `addDeletes` (snapshot operation = OVERWRITE)
  differently than DELETE-only snapshots. The data-file portion is a known accepted gap
  (documented in `RowReplacementOp` Javadoc); the surprise is that the *delete-file* portion
  also escapes detection in the V2 PD-file case.
- **Open question**: the detector should still find the PD files regardless of the parent
  snapshot's operation type. Confirm by inspecting `seed-1.warehouse.tar`'s
  conflict-detector output.

### 2. V2 chain deletes drop after upgrade-then-compact on v2→v3 scenarios
- **Seed**: 2 on the default adversarial config (smoke run 2026-05-14). Format =
  `V2_THEN_UPGRADE_TO_V3`; chain commits 7 V2 PD-file deletes, table upgrades to v3, late-tx
  writes a DV.
- **Manifest**: 7-row divergence between reference and treatment — exactly
  `chain * perSnapshotDeletes` (1 × 7 = 7).
- **Suspected code path**: after upgrade to v3, `SparkActions.rewriteDataFiles` may not honor
  pre-upgrade V2 position-delete files during the rewrite scan. Cross-references the
  explore-agent finding that v3 scan planning ignores PD files
  (`TestRowDelta.java:1875–1883`). Whether compaction's internal MoR scan is also affected is
  the open question.
- **Open question**: confirm by inspecting `seed-2.warehouse.tar`'s pre-compaction state vs.
  the post-compaction snapshot's data files — the 7 rows the chain PD files were supposed to
  delete should be missing post-compact in both paths if compaction honors PD files; if
  they're present in *both* paths the bug is upstream of the harness; if they're present in
  only *one* path the bug is in the harness's path-asymmetry handling.

## Open pre-flight test failures (M7)

- `TestRewritePositionDeleteFilesAction.testRemoveDanglingDVsAfterCompaction` — fails in
  **both** `iceberg-spark-3.5_2.12` and `iceberg-spark-4.0_2.13` with `NoSuchTableException:
  [TABLE_OR_VIEW_NOT_FOUND]`. The stack lands in `SparkBinPackFileRewriteRunner.doRewrite()`
  → `DataFrameReader.load()`, well upstream of any compaction-map code. Verified pre-existing
  by re-running with the engagement's changes stashed (commit `9272eec67`, 2026-05-13). This
  is a Spark catalog-resolution issue in the existing test setup, not a regression.

The other two pre-flight commands — `iceberg-core *Compaction*` and `iceberg-core
*Remapping*` — pass cleanly.

## How to triage a new failure

When the harness produces a `seed-N.fail.json`:

1. Run `scripts/reproduce.sh seed-N.fail.json` to confirm the failure replays on this
   workstation (not just inside the container).
2. Untar the warehouse: `tar -xf seed-N.warehouse.tar` — inspect both `reference/` and
   `treatment/` Iceberg metadata. The compaction-map Avro file in
   `treatment/db/treatment/metadata/compaction-map-*.avro` is readable via
   `CompactionMaps.read`.
3. Distinguish: is the divergence (a) a workload-shape gap the harness shouldn't be hitting
   (file it here and consider tightening the config), or (b) a real bug in the resolver /
   detector / remapper (file it here with the suspected code path)?
4. Per spec §γ: do **not** fix the bug in the same engagement that surfaces it. Triage and
   document only.
5. Add an entry under "Open failures" with: **Seed(s)**, **Manifest**, **Suspected code
   path**, **Open question**.

## Lifecycle

This file is an **active-only** ledger. When a failure is resolved:

1. **Same commit as the fix**: add a `**Resolution:**` paragraph to the existing entry
   describing the root cause, the fix, and the regression test that pins it. The entry stays
   in the file in this commit so the history preserves the bug → fix linkage in one place.
2. **Subsequent commit**: delete the entry from this file. The full record (entry + fix) is
   now preserved in `git log` against the commit from step 1; the file stays a tidy list of
   currently-open issues.

`git log -p benchmark/compaction-baseline/KNOWN_FAILURES.md` reconstructs the full history of
every failure that ever lived here.
