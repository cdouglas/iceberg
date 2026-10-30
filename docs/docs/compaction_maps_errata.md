---
title: "Compaction Maps - Implementation Errata"
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Compaction Maps - Implementation Errata

This document lists expedient implementation choices and simplifications made during development that we intend to revisit and improve later. These are not bugs but pragmatic decisions to ship functionality faster while documenting technical debt.

## 1. Design Scope: Order-Preserving Compactions

### Supported Operations

Compaction maps support **order-preserving** compaction operations:

- ✅ **Bin-pack rewrites**: Combining multiple files without reordering (simple concatenation)
- ✅ **Merge compactions**: Bin-pack with position deletes applied during scan

These operations preserve the relative order of rows within each source file, allowing position mappings to be represented efficiently as runs.

### How Merge Compactions Work

When position deletes exist, they're applied during the scan phase (standard Iceberg behavior). The compaction map correctly tracks the resulting gaps:

```
Source file A: positions 0, 1, 2, 3, 4
Position delete: delete row 2 from file A
Scan phase: Iceberg applies deletes, returns rows with _pos = 0, 1, 3, 4
Write phase: PositionTrackingDataWriter records mappings for _pos = 0, 1, 3, 4
Target file: positions 0, 1, 2, 3
Compaction map: Run(0, 0, 2), Run(3, 2, 2)  // Gap at source position 2
```

### Out of Scope: Order-Changing Operations

Compaction maps are **not appropriate** for operations that reorder rows:

- **Sorted compactions**: Rewriting data sorted by column(s)
- **Z-ordered compactions**: Reorganizing data along a space-filling curve for query locality

This is a **design boundary**, not a missing feature. Order-changing operations are inappropriate for compaction maps because:

1. **Degenerate mappings**: Reordering produces maps with runs of length 1 (every row maps individually), defeating run-length encoding and creating maps as large as the data itself.

2. **Semantic mismatch**: Position deletes identify rows by `(file_path, position)`. After reordering, position N refers to a different logical row. Remapping position deletes through a reorder operation would delete wrong rows.

3. **Better alternatives exist**: For sorted/Z-ordered compactions, concurrent transactions should use equality deletes (content-based) rather than position deletes, or accept that position deletes against old files are invalidated by the reorder.

**Note**: A table with sorted base data and unsorted recent changes is fine—the unsorted changes can be compacted (bin-packed) with position tracking, and later merged into sorted runs without tracking (since the merge applies deletes during scan).

---

## 2. Automatic Conflict Resolution (Partial)

### Issue

Automatic conflict resolution is **now available for compaction operations** (Spark 3.5 and 4.0), but **application transactions still require manual resolution**.

### Current Status

**✅ Compaction Operations (Spark 3.5 and 4.0):**

Automatic conflict resolution is implemented via `SparkRewriteDataFilesCommitManager`:
```java
// Enable compaction maps AND conflict resolution
table.updateProperties()
    .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
    .set(TableProperties.COMPACTION_RESOLVE_DELETE_CONFLICTS, "true")
    .commit();

// Compaction automatically resolves conflicts with concurrent position deletes
SparkActions.get(spark).rewriteDataFiles(table).execute();
```

**Supported:**
- V2 format tables (position delete files)
- V3 format tables (deletion vectors)
- Subject to `max-files` limit (default: 100)

**❌ Application Transactions:**

Position delete conflicts from application transactions (e.g., RowDelta) still require manual handling:
```java
try {
  rowDelta.addDeletes(deleteFile);
  rowDelta.commit();
} catch (CompactionConflictException e) {
  // Manual resolution required:
  // 1. Get compaction map locations from exception
  // 2. Load maps and create remapper
  // 3. Remap position deletes
  // 4. Retry commit with remapped deletes
}
```

### What's Remaining

**Application Transaction Conflict Resolution:**

Add opt-in automatic remapping to `BaseRowDelta`:
```java
// Enable automatic conflict resolution
rowDelta.enableAutomaticRemapping(true);
rowDelta.addDeletes(deleteFile);
rowDelta.commit();
// If compaction conflict detected:
//   1. Automatically load compaction maps
//   2. Remap position deletes
//   3. Retry commit transparently
```

### Code Location

**Compaction Resolution (Implemented for Spark 3.5 and 4.0):**
```
spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkRewriteDataFilesCommitManager.java
spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/actions/SparkRewriteDataFilesCommitManager.java
  detectAndResolveConflicts() - Detects and resolves conflicts during commit

spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkCompactionConflictResolver.java
spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/actions/SparkCompactionConflictResolver.java
  resolve() - Reads, remaps, and writes conflicting position deletes and DVs

core/src/main/java/org/apache/iceberg/CompactionConflictDetector.java
  detectConflicts() - Scans manifests to find conflicting delete files
```

**Application Resolution (Manual):**
```
core/src/main/java/org/apache/iceberg/BaseRowDelta.java
  validateNoCompactionConflicts() - Throws CompactionConflictException

core/src/main/java/org/apache/iceberg/CompactionMapValidator.java
  validateNoCompactedReferences() - Conflict detection logic

core/src/main/java/org/apache/iceberg/PositionDeleteRemapper.java
  remapDelete() - Manual remapping API
```

### Validation

```bash
# Test compaction conflict resolution (Spark 3.5)
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test \
  --tests "TestSparkCompactionConflictResolution"

# Test compaction conflict resolution (Spark 4.0)
./gradlew :iceberg-spark:iceberg-spark-4.0_2.13:test \
  --tests "TestSparkCompactionConflictResolution"

# Test manual resolution (application transactions)
./gradlew :iceberg-core:test --tests "TestCompactionConflictResolution"
```

---

## 4. The Validator Collected Carried-Forward Maps, Out of Order ✅ FIXED

### Issue

`CompactionMapValidator` decides whether a row delta's position deletes reference files that a
compaction removed after the transaction started. It walked from the current snapshot back to the
starting one and collected the map location of every data manifest it saw. But a manifest keeps its
location when a later commit carries, filters (`ManifestFilterManager`) or merges
(`ManifestMergeManager`) it, so each snapshot names the map of every compaction whose manifests are
still live. Three bugs followed. Each is reproduced in `TestCompactionMapValidatorHistory`.

- **Scope.** The validator collected the maps of compactions from before the starting snapshot
  whenever any commit landed after the transaction started. A position delete file that covers several
  data files (no `referencedDataFile`, as the default partition delete granularity writes) conflicts
  with every compacted file the validator knows. Such a delete failed with a
  `CompactionConflictException` naming files that were compacted before the transaction began.
  Remapping does nothing for those files, so a retry from the same starting snapshot failed the same
  way.
- **Order.** Maps were ordered by where their locations first appear in manifest lists. Manifest lists
  group manifests by spec id, highest first. Suppose a later compaction writes into a spec with a lower
  id than an earlier compaction's output, for example after rolling a partition spec back. Then the
  chain came out newest first. `CompactionMapChain` composes a file only through the maps after the one
  that compacted it. So a delete on F1, compacted to F2 and then to F3, was remapped to F2, which the
  table no longer holds.
- **Composition.** `CompactionMaps.compose` required the first map's target snapshot to equal the
  second map's source snapshot. A map's source snapshot is the snapshot its compaction planned
  against, so any commit between two chained compactions broke the equality. A map that
  `RewriteDataFilesCommitManager` writes (core or Spark) records `startingSnapshotId + 1` as a
  placeholder target, so its chains never satisfied it. Remapping a chained conflict threw
  `IllegalArgumentException`.

### Resolution

- The validator attributes each map to the snapshot that wrote it. A REPLACE snapshot's own map is on
  a manifest that the snapshot wrote, and lists source files that the snapshot removed. Only the maps
  of snapshots after the starting one are collected, in commit order.
- `compose` follows files only. The snapshot id check, which never affected the result, is removed.
  `TestCompactionMapComposition` now composes across snapshot ids that do not match, instead of
  expecting an exception.

### Still Open

- SERIALIZABLE validation (`MergingSnapshotProducer.validateCompactionAwareConflicts`) treats a replace
  as layout-only if any of its data manifests names a map. Carried locations make every replace after a
  mapped compaction look mapped. That falls back to upstream's behavior, which never validates a
  replace's added files, rather than to anything less safe. Whether to keep the exemption at all is
  undecided.
- An action-built map's `targetSnapshotId` is a placeholder. Nothing should rely on it.

### Code Location

- `core/src/main/java/org/apache/iceberg/CompactionMapValidator.java` -- `loadCompactionMaps`,
  `addOwnMaps`
- `core/src/main/java/org/apache/iceberg/CompactionMaps.java` -- `compose`
- `core/src/test/java/org/apache/iceberg/TestCompactionMapValidatorHistory.java`

---

## 5. Position-Tracked Compactions Dropped Row Lineage on v3 ✅ FIXED

### Issue

The v3 spec requires that a row a compaction moves keeps its `_row_id` and
`_last_updated_sequence_number`. Spark's bin-pack compaction tracks source positions whenever
`write.compaction-map.enabled` is true, and with tracking on it kept neither:

- **Spark 3.5 wrote no lineage.** For a position-tracked rewrite, `SparkTable` read `_file` and `_pos`
  *instead of* the lineage columns, and `SparkWriteBuilder` left lineage out of the write schema
  (6a14ae978). Readers then gave every output row a new `_row_id` from the output file's
  `first_row_id`, and took the compaction's sequence number as its `_last_updated_sequence_number`.
  The outputs had no lineage metrics either.
- **Spark 4.0 wrote garbage.** Its write schema added the lineage columns (65dedde35), but the rows did
  not carry them, and the writer reads columns by ordinal. It wrote `_file` and `_pos` in their place.
  Every row's `_row_id` was the internal offset word of the `_file` string, the same value for every
  row in a task, and its `_last_updated_sequence_number` was its source position.

Both earlier fixes made a v3 write succeed by changing the write schema, rather than by reading
lineage with the rows. The v3 tests checked maps, not lineage, so they passed. With maps disabled,
both versions kept lineage. The bug was found by a snapshot-rewrite test onto a Spark compaction
(`remap` branch), which refuses such compactions.

### Impact

- Lineage is wrong for every row that a position-tracked compaction moved in a v3 table. Change
  tracking by `_row_id` sees each moved row as deleted and re-inserted. `_last_updated_sequence_number`
  claims the row changed at the compaction (3.5), or holds a file position (4.0, where ids also
  collide).
- Files written before the fix keep the wrong values, and the fix does not repair them. The original
  ids exist only in the source files' `first_row_id`, which older snapshots' manifests record, and in
  the compaction map that leads back to them.
- ORC never had lineage to keep. Iceberg's ORC readers do not read row lineage at all: every row reads
  its file's `first_row_id`. That gap is upstream and unrelated to compaction maps, so the test below
  covers Parquet only.

### Resolution

- A position-tracked rewrite reads `_row_id` and `_last_updated_sequence_number` with the rows, ahead
  of `_file` and `_pos` (`SparkTable.addLineageIfRequired`, `SparkBinPackFileRewriteRunner`), and
  writes them like any other v3 compaction (`SparkWriteBuilder`).
- `SparkWriteBuilder.checkLineageRead` refuses a position-tracked v3 rewrite whose input lacks
  lineage. `PositionTrackingDataWriter` refuses input where `_file` and `_pos` are not the last
  columns, so the delegate can never write them as other columns.
- Spark 4.0's `addRowLineageColumnsIfMissing`, which produced the misaligned write, is removed.
- `TestBinPackWithPositionTracking.testBinPackKeepsRowLineage` (3.5 and 4.0, Parquet) deletes a row,
  compacts, and checks two things. Every surviving row keeps both values, and the outputs record both
  columns in their metrics, with no nulls.

### Code Location

- `spark/v{3.5,4.0}/spark/src/main/java/org/apache/iceberg/spark/source/SparkTable.java` --
  `addLineageIfRequired`
- `spark/v{3.5,4.0}/spark/src/main/java/org/apache/iceberg/spark/source/SparkWriteBuilder.java` --
  `build`, `checkLineageRead`
- `spark/v{3.5,4.0}/spark/src/main/java/org/apache/iceberg/spark/source/PositionTrackingDataWriter.java`
- `spark/v{3.5,4.0}/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackFileRewriteRunner.java`
  -- `doRewrite`

---

## Summary

| # | Issue | Impact | Status | Priority |
|---|-------|--------|--------|----------|
| 1 | Order-preserving compactions only | Order-changing ops (sort, Z-order) out of scope | By design | - |
| 2 | Automatic conflict resolution | Compactions ✅, Application transactions ❌ | Partially implemented | Low |
| 4 | Validator collected carried-forward maps, out of order | Spurious conflicts for multi-file deletes; chained remaps stopped early or threw | Fixed | - |
| 5 | Position-tracked compactions dropped v3 row lineage | Moved rows got new `_row_id`s; files written before the fix keep them | Fixed | - |

**Fixed Issues (Removed from Active List):**
- ~~Chained compaction maps~~ - ✅ FIXED: Lazy composition via CompactionMapChain (Feb 2, 2026, commit 8cb1e6156)
- ~~Source files spanning multiple targets~~ - ✅ FIXED: Per-run target files now supported (Jan 24, 2026)
- ~~Normal scans vs staged scans~~ - ✅ FIXED: Staged scans now work with explicit metadata column selection
- ~~V3 Deletion Vector conflict resolution~~ - ✅ FIXED: SparkCompactionConflictResolver now supports DVs
- ~~Spark 4.0 Conflict Resolution Parity~~ - ✅ FIXED in commit 539432b51
- ~~Compaction map location not in manifests~~ - ✅ FIXED in commit 41324b697
- ~~Target-pending placeholder bug~~ - ✅ FIXED in commit e8287a752
- ~~Spark 3.5 format v3 + position tracking~~ - ✅ FIXED in commits e8287a752 and 6a14ae978, the latter by dropping row lineage (see 5)
- ~~Spark 3.5/4.0 partitioned table position tracking~~ - ✅ FIXED in commit 8b811d951
- ~~Spark 4.0 format v3 + position tracking~~ - ✅ FIXED in commit 65dedde35, but by writing `_file` and `_pos` as row lineage (see 5)

## How to Contribute

If you'd like to help address the remaining issues:

**Application Transaction Conflict Resolution (Priority: Low):** Implement opt-in automatic remapping in `BaseRowDelta` for application-level position delete conflicts. The compaction-level resolution (`SparkRewriteDataFilesCommitManager`) is already complete for both Spark 3.5 and 4.0.

## References

- [Main Compaction Maps Documentation](compaction_maps.md)
