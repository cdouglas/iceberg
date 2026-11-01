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

A bin-pack into another partition spec is not one of these: it shuffles rows into their new
partitions, and writes no map (errata 15).

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

## 3. Compaction Map Files Were Invisible to Table Maintenance ✅ FIXED

### Issue

A compaction map is a file in the table's metadata directory (`CompactionMaps.newCompactionMapFile`),
and the only thing that names it is a manifest's `compaction_map_location` (field 550). No maintenance
operation read that field:

- **Orphan-file removal deleted maps that were still in use.** `DeleteOrphanFilesSparkAction` (Spark
  3.5 and 4.0) and Flink's `DeleteOrphanFiles` treated only content files, manifests, manifest lists,
  metadata JSON files, the version hint, and statistics files as reachable. Every map older than the
  cutoff (three days by default in Spark) was deleted, including maps a live manifest still named.
- **Snapshot expiry never deleted maps.** `IncrementalFileCleanup`, `ReachableFileCleanup` and
  `ExpireSnapshotsSparkAction` deleted unreachable manifests, manifest lists and content files, but
  never looked at field 550. A map outlived the last snapshot that named it, indefinitely.

### Impact

- Remapping through a map that orphan removal had deleted fails when it reads the map. A concurrent
  commit needs a map only until it retries, normally well inside the cutoff. Anything that reads maps
  later was exposed for every map older than the cutoff.
- Expiry leaked one small Avro file per compaction.

### Resolution

A map is reachable while a manifest of a reachable snapshot names it. That includes a filtered or
merged copy, which keeps the location (`ManifestFilterManager`, `ManifestMergeManager`).

- `ReachableFileUtil.compactionMapLocations` lists the maps that data manifests name.
- Orphan removal treats those maps as reachable, in Spark's `BaseSparkAction.otherMetadataFileDS` and
  Flink's `ListMetadataFiles` (1.19, 1.20 and 2.0). `DeleteReachableFilesSparkAction` now deletes them
  with the rest of a table.
- Expiry deletes a map that a manifest of an expired snapshot names and no retained manifest does.
  Both core cleanup strategies read the location from the manifest list, which is now in their
  projection, and `ExpireSnapshotsSparkAction` compares maps along with the other files.

Listing maps reads every snapshot's manifest list on the driver. Spark reads manifest lists
distributed elsewhere, through the `all_manifests` metadata table, but that table has no compaction
map column.

### Code Location

- `core/src/main/java/org/apache/iceberg/ReachableFileUtil.java` -- `compactionMapLocations`
- `core/src/main/java/org/apache/iceberg/FileCleanupStrategy.java`, `IncrementalFileCleanup.java`,
  `ReachableFileCleanup.java`
- `spark/v{3.5,4.0}/spark/src/main/java/org/apache/iceberg/spark/actions/BaseSparkAction.java` --
  `otherMetadataFileDS`, `compactionMapDS`; `ExpireSnapshotsSparkAction.java` -- `fileDS`
- `flink/v{1.19,1.20,2.0}/flink/src/main/java/org/apache/iceberg/flink/maintenance/operator/ListMetadataFiles.java`
- `core/src/test/java/org/apache/iceberg/TestCompactionMapCleanup.java`
- `spark/v{3.5,4.0}/spark/src/test/java/org/apache/iceberg/spark/actions/TestCompactionMapMaintenance.java`

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
  `RewriteDataFilesCommitManager` wrote (core or Spark) recorded `startingSnapshotId + 1` as a
  placeholder target, so its chains never satisfied it. Remapping a chained conflict threw
  `IllegalArgumentException`.

### Resolution

- The validator attributes each map to the snapshot that wrote it. A REPLACE snapshot's own map is on
  a manifest that the snapshot wrote, and lists source files that the snapshot removed. Only the maps
  of snapshots after the starting one are collected, in commit order.
- `compose` follows files only. The snapshot id check, which never affected the result, is removed.
  `TestCompactionMapComposition` now composes across snapshot ids that do not match, instead of
  expecting an exception.
- Action-built maps record the snapshot their compaction committed as their target. The commit
  managers hand the map to `BaseRewriteFiles.setCompactionMap`, which writes it when the rewrite is
  applied, under the rewrite's own snapshot id. That id is kept across retries, so a retry reuses
  the map already written.

### Follow-up

- SERIALIZABLE validation also read map locations, and carried locations made every replace after a
  mapped compaction look mapped. That check is removed; see 6.

### Code Location

- `core/src/main/java/org/apache/iceberg/CompactionMapValidator.java` -- `loadCompactionMaps`,
  `addOwnMaps`
- `core/src/main/java/org/apache/iceberg/CompactionMaps.java` -- `compose`
- `core/src/main/java/org/apache/iceberg/BaseRewriteFiles.java` -- `setCompactionMap`
- `core/src/test/java/org/apache/iceberg/TestCompactionMapValidatorHistory.java`
- `core/src/test/java/org/apache/iceberg/TestCompactionMapCommitFlow.java` --
  `testRewriteRecordsItsOwnSnapshotAsTheMapTarget`

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

## 6. SERIALIZABLE Row Deltas Conflicted With Compactions Without Maps ✅ FIXED

### Issue

`MergingSnapshotProducer.validateCompactionAwareConflicts` extended SERIALIZABLE validation to replace
snapshots. A SERIALIZABLE row delta failed if a concurrent replace added files matching its conflict
filter, unless one of the replace's data manifests named a compaction map. Upstream's
`validateAddedDataFiles` checks only appends and overwrites, because a replace adds no rows. The check
could only add failures to upstream's.

### Impact

- The check ran whether or not `write.compaction-map.enabled` was set. On a table without maps, the
  default, a SERIALIZABLE merge-on-read DELETE, UPDATE or MERGE failed against any concurrent
  compaction of files matching its filter, even when its deletes touched no compacted file.
  Copy-on-write operations were unaffected; only `BaseRowDelta` ran the check.
- Sorted and Z-order compactions, which write no maps by design, conflicted with every such row delta.
- A map is not evidence that a compaction kept its rows, so the check did not catch a compaction that
  lost or duplicated rows, with or without a map.
- Carried map locations (see 4) made every replace after a mapped compaction look mapped, so the same
  history passed or failed depending on manifests left by earlier compactions.

### Resolution

The check is removed, and SERIALIZABLE row deltas validate added data files as upstream does. A delete
on a file that a compaction removed still fails. `CompactionMapValidator` throws
`CompactionConflictException` when maps are enabled and the compaction wrote one, and
`validateDataFilesExist` fails for any removed file the row delta declares.

### Code Location

- `core/src/main/java/org/apache/iceberg/BaseRowDelta.java` -- `validate`
- `core/src/test/java/org/apache/iceberg/TestSerializableIsolationWithCompaction.java` --
  `testSerializableIsolationAllowsCompactionWithoutMap`

---

## 7. Direct Rewrites Recorded a Map That Assumed Their Layout ✅ FIXED

### Issue

`BaseRewriteFiles` wrote a fallback compaction map for a direct `table.newRewrite()` that was given
none. It did so on any table with `write.compaction-map.enabled`, when the rewrite added one data
file. The map assumed the target held the replaced files concatenated in the order `deleteFile()` was
called. The only check was that record counts added up. Its justification was that sort and Z-order
rewrites never enable compaction maps, but maps are enabled per table, not per rewrite. The commit
managers turned the fallback off, so it reached only direct callers: other engines, maintenance tools,
and applications.

### Impact

- **Silent:** a sort rewrite into one file passed the check and got a wrong map, and so did a
  concatenation in a different order. A delete remapped through it lands on the wrong row. That covers
  an application retrying after a `CompactionConflictException`, chained maps, and a v2 snapshot
  rewrite, which trusts maps. In the reproduction, deleting id 10 after a sort rewrite removed id 0.
- **Loud:** a direct rewrite that applied deletes wrote fewer rows than its sources, so the check threw
  `IllegalStateException` and the compaction could not commit on a map-enabled table.

### Resolution

The fallback is removed, with `disableAutoCompactionMap`. A rewrite records a map only when its caller
passes one with `setCompactionMap` or `setCompactionMapLocation`, which the commit managers do from
explicit position tracking. A delete on a file a mapless rewrite removed fails
`validateDataFilesExist`, and the transaction retries from the new layout. Core tests that relied on
the fallback state their layout through a test helper, `ConcatenationMaps`.

### Code Location

- `core/src/main/java/org/apache/iceberg/BaseRewriteFiles.java` -- `apply`
- `data/src/test/java/org/apache/iceberg/data/TestDirectRewriteCompactionMap.java`
- `core/src/test/java/org/apache/iceberg/TestFallbackMapGenerationRemoved.java` --
  `testDirectRewriteRecordsNoMap`

---

## 8. Compaction Maps Are Written in Plaintext

### Issue

Iceberg encrypts a table's manifests with its encryption manager. A compaction map is a
metadata-directory file that `BaseRewriteFiles` writes, and the prototype writes it through
`FileIO.newOutputFile`, in plaintext. No key is recorded for it, and every reader opens it with
`FileIO.newInputFile`.

### Impact

On an encrypted table a map exposes what an encrypted manifest hides: the paths of the files a
compaction replaced and wrote, and which row positions moved where. It holds no row values. Deletes
the conflict resolver remaps are written through `OutputFileFactory` and are encrypted. Writers and
readers agree, so nothing fails: the gap is confidentiality only.

### Status

Open, out of scope for the prototype. Encrypting maps needs key metadata recorded beside the map's
location, which manifest field 550 does not have room for. Maps are written plaintext whether or not
the table is encrypted.

### Code Location

- `core/src/main/java/org/apache/iceberg/BaseRewriteFiles.java` -- the map written at `apply`
- `core/src/main/java/org/apache/iceberg/CompactionMaps.java` -- `newCompactionMapFile`, `read`

---

## 9. Commit-Time Conflict Resolution Could Never Commit ✅ FIXED

### Issue

With `write.compaction.resolve-delete-conflicts` set, `SparkRewriteDataFilesCommitManager` remapped
the deletes committed to a rewrite's files since it started. It then added the remapped copies to its
`RewriteFiles` without removing the originals. `RewriteFiles` accepts new delete files only alongside
removed ones, and validates from the starting snapshot that no new delete applies to a rewritten
file. So the commit always failed. No test committed a delete while a rewrite was running.

### Impact

The option never worked. A compaction that met a concurrent delete failed, as it would have without
the option, and left its written files behind.

### Resolution

The rewrite resolves through the branch head it read, and in one commit removes each conflicting
deletion vector or file-scoped position delete file and adds its remapped copy. It validates from
that head, so a delete committed after resolution still fails it.

- DVs remapped into one target file are merged, since a data file may have one DV.
- The detector tells DVs apart by content offset as well as location, since one Puffin file holds
  the DVs of several data files. It does not count a delete a later commit already replaced.
- A position delete file that also references files outside the rewrite fails it with a
  `ValidationException`.
- A rewrite that does not use its starting sequence number does not resolve.
- The remapped files are deleted if the commit fails cleanly.

### Code Location

- `spark/*/spark/src/main/java/org/apache/iceberg/spark/actions/SparkRewriteDataFilesCommitManager.java`
- `spark/*/spark/src/main/java/org/apache/iceberg/spark/actions/SparkCompactionConflictResolver.java`
- `core/src/main/java/org/apache/iceberg/CompactionConflictDetector.java`, `DeleteConflictInfo.java`
- Tests: `TestSparkCompactionConflictResolution`, `TestCompactionConflictDetector`

---

## 10. Spark 4.0 Copy-on-Write Writes Failed on v3 ✅ FIXED

### Issue

Position tracking made Spark 4.0's `SparkWriteBuilder` validate the write schema against the incoming
Spark schema instead of the one it writes, which adds `_row_id` and `_last_updated_sequence_number`
when the write must carry row lineage. A copy-on-write MERGE, UPDATE or DELETE on a v3 table wrote
rows with lineage through a writer built without it.

### Impact

Every such write failed with `IndexOutOfBoundsException` in `ParquetWithSparkSchemaVisitor`, on any v3
table, with or without compaction maps. Spark 3.5 was not affected.

### Resolution

`SparkWriteBuilder` validates against the schema it writes, as Spark 3.5 and upstream do.

### Code Location

- `spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/source/SparkWriteBuilder.java`
- Tests: `TestCopyOnWriteWithLineage` (failed 27 of 30 cases before)

---

## 11. Bin-Pack Rewrites Failed Outside Hadoop Tables ✅ FIXED

### Issue

Position tracking made `SparkBinPackFileRewriteRunner` load and save the rewrite through the table's
location instead of the rewrite group's id, whether or not the table kept maps. Only a Hadoop table
loads by path. Every fork test used Hadoop tables.

### Impact

A bin-pack rewrite of a table in a Hive, REST or session catalog failed with
`NoSuchTableException`. That made `rewrite_data_files` and dangling-delete removal fail on those
catalogs, with maps on or off.

### Resolution

The runner loads and saves the group id, which resolves through the table cache to the same rewrite
table.

### Code Location

- `spark/*/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackFileRewriteRunner.java`
- Tests: `TestRewriteDataFilesProcedureWithCompactionMaps` (v2 and v3 in every test catalog)

---

## 12. Spark 4.0 Replaced Tracked Mappings With Concatenations ✅ FIXED

### Issue

After the runner attached the position mappings the writers recorded, Spark 4.0's
`RewriteDataFilesSparkAction` replaced them with mappings that concatenated the source files by
record count. A record count includes rows the rewrite drops as deleted.

### Impact

The map of a source file with deletes was one gapless run, so positions after its first deleted row
remapped to the wrong rows. A concurrent delete remapped through such a map deleted another row.
Spark 3.5 was not affected. Errata 7 removed the same assumption from `BaseRewriteFiles`.

### Resolution

The concatenation is removed; Spark 4.0 keeps the recorded mappings, as Spark 3.5 does.

### Code Location

- `spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java`
- Tests: `TestSparkBinPackWithPositionDeletes`, which had run only on Spark 3.5 (failed 19 of 20
  cases on 4.0 before)

---

## 13. Sort and Z-Order Rewrites Failed on Tables With Maps ✅ FIXED

### Issue

Errata 1 still holds: only order-preserving compactions write maps. To track positions, `SparkTable`
added `_file` and `_pos` to the rewrite schema of every table that keeps maps. The write path dropped
them only from a write that tracks positions, which only bin-pack asks for.

### Impact

Every sort or Z-order rewrite of a table with maps failed with "Field _file not found in source
schema". Errata 1 expects such a table to be sorted without tracking; it could not be compacted by
anything but bin-pack. No test rewrote a table with maps by anything but bin-pack.

### Resolution

Every rewrite of a table with maps drops the two columns before writing, and only bin-pack records
them. Sort and Z-order rewrites run and write no map.

### Code Location

- `spark/v3.5`, `spark/v4.0`: `SparkTable.readsSourcePositions`, `SparkWriteBuilder`, `SparkWrite`
- Tests: `TestRewriteDataFilesActionWithCompactionMaps` -- `testSortRewriteWritesNoMap`,
  `testZOrderRewriteWritesNoMap`

---

## 14. Position Mappings Reached the Driver Only in Local Mode ✅ FIXED

### Issue

`PositionTrackingDataWriter` runs in each Spark task and recorded the positions it moved in
`PositionMappingCoordinator`, a per-JVM singleton. The rewrite runner reads the mappings from the
driver's singleton when the group finishes. Nothing carried them from an executor to the driver: the
task's commit message held only its data files. Every test and benchmark ran Spark in local mode,
where tasks share the driver's JVM.

### Impact

On a cluster the driver saw no mappings, so a compaction committed without a map, silently. Nothing
was wrong as a result: a concurrent delete on a replaced file conflicted as it would on a table
without maps, and the transaction retried. But the feature did nothing.

### Resolution

Each task returns its mappings with its commit message. The writer groups rows into runs of
consecutive source positions as it writes them, and on commit splits the runs where it rolled to a
new file. It returns them in a `TrackedTaskCommit`, a `TaskCommit` that also carries the runs. The
rewrite's commit, on the driver, records the runs of the messages it commits in the driver's
coordinator, where the runner reads them as before. An accumulator would also have reached the
driver, but it counts a retried task's rows twice; the commit messages are exactly the tasks whose
files the rewrite commits.

A task holds one entry per run rather than one per row. A task whose files do not hold exactly the
rows it wrote now fails, since a map that left out rows would read as if they had been deleted.

### Code Location

- `spark/*/spark/src/main/java/org/apache/iceberg/spark/source/PositionTrackingDataWriter.java` --
  `withSourcePositions`, `recordSourcePositions`, `TrackedTaskCommit`
- `spark/*/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java` -- `RewriteFiles`
- `spark/*/spark/src/main/java/org/apache/iceberg/spark/PositionMappingCoordinator.java` --
  `recordRun`
- Tests: `TestCompactionMapsOnCluster` bin-packs and merges on a `local-cluster` master, whose
  executors run in their own JVMs, and checks the map against the rows (failed all 4 cases before,
  on Spark 3.5 and 4.0). `TestPositionTrackingDataWriter` and `TestPositionMappingCoordinator` (now
  also on 3.5) cover the commit message and its recording.

---

## 15. Bin-Packs Into a New Partition Spec Recorded Wrong Maps ✅ FIXED

### Issue

A bin-pack of files written under an older partition spec rewrites them into the current spec,
shuffling rows into their new partitions with a range distribution. Errata 1 still holds, and that
shuffle reorders rows as a sort does, but the rewrite tracked positions anyway. On a table with no
sort order, Iceberg also drops the sort within each task when fanout writers are enabled, which is
the default. A task then wrote several partitions with rows interleaved, while the tracking writer
assumed each task wrote its files one after another. It also took the files in the order the
fanout writer reported them, grouped by partition in hash order.

### Impact

After partition evolution, compacting old files on a table with maps recorded a map that sent rows
to the wrong target positions, whenever a task wrote more than one partition. The record-count check
added with errata 14 does not catch it, since the counts still add up. A concurrent delete remapped
through such a map deleted other rows. A correct map would still have been degenerate, runs of a row
or a few, which errata 1 excludes.

### Resolution

A bin-pack that repartitions tracks nothing and writes no map, as sort and Z-order rewrites do;
concurrent deletes conflict as on a table without maps. As a backstop, a write that tracks positions
never gets a fanout writer, so a tracked task that wrote interleaved partitions would fail instead
of recording a wrong map.

### Code Location

- `spark/*/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackFileRewriteRunner.java`
  -- `requiresRepartition`
- `spark/*/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java` --
  `createWriterFactory`
- Tests: `TestRewriteDataFilesActionWithCompactionMaps` -- `testBinPackIntoANewSpecWritesNoMap`
  (failed on Spark 3.5 and 4.0 before the fix) and
  `testBinPackOfAPartitionedTableMapsEveryRow`

---

## Summary

| # | Issue | Impact | Status | Priority |
|---|-------|--------|--------|----------|
| 1 | Order-preserving compactions only | Order-changing ops (sort, Z-order) out of scope | By design | - |
| 2 | Automatic conflict resolution | Compactions ✅, Application transactions ❌ | Partially implemented | Low |
| 3 | Compaction map files were invisible to maintenance | Orphan removal deleted maps in use; expiry never deleted them | Fixed | - |
| 4 | Validator collected carried-forward maps, out of order | Spurious conflicts for multi-file deletes; chained remaps stopped early or threw | Fixed | - |
| 5 | Position-tracked compactions dropped v3 row lineage | Moved rows got new `_row_id`s; files written before the fix keep them | Fixed | - |
| 6 | SERIALIZABLE row deltas conflicted with compactions without maps | Spurious conflicts, including on tables without maps | Fixed | - |
| 7 | Direct rewrites recorded a map that assumed their layout | Remapped deletes hit the wrong rows after a sort rewrite; rewrites that applied deletes could not commit | Fixed | - |
| 8 | Compaction maps are written in plaintext | An encrypted table's maps expose file paths and moved positions | Open | Low |
| 9 | Commit-time conflict resolution could never commit | Compactions that met a concurrent delete failed with resolution on | Fixed | - |
| 10 | Spark 4.0 copy-on-write writes failed on v3 | MERGE, UPDATE and DELETE failed on every v3 table | Fixed | - |
| 11 | Bin-pack rewrites failed outside Hadoop tables | `rewrite_data_files` failed in Hive, REST and session catalogs | Fixed | - |
| 12 | Spark 4.0 replaced tracked mappings with concatenations | Maps of files with deletes remapped positions to the wrong rows | Fixed | - |
| 13 | Sort and Z-order rewrites failed on tables with maps | Only bin-pack could compact a table with maps | Fixed | - |
| 14 | Position mappings reached the driver only in local mode | On a cluster, compactions wrote no map | Fixed | - |
| 15 | Bin-packs into a new partition spec recorded wrong maps | Remapped deletes hit the wrong rows after partition evolution | Fixed | - |

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
