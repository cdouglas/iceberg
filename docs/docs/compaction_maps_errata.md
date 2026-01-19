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

## 1. Normal Scans Instead of Staged Scans

### Issue

Position tracking uses **normal scans** instead of **staged scans** for reading data during bin-pack rewrites when compaction map generation is enabled.

### Impact

**Performance: ~10-20% slower**
- Normal scans re-scan manifests that were already scanned during planning
- File path filter evaluation adds overhead
- Manifest reader instantiation repeated

### Why This Choice Was Made

Staged scans don't properly expose metadata columns (`_file`, `_pos`) to Spark's physical planner:

```
Staged Scan Behavior:
1. Metadata columns added to scan schema ✅
2. Spark V2ScanRelationPushDown optimizer phase ❌
3. PushDownUtils.toOutputAttrs fails with "key not found" error
4. Metadata columns pruned away before reaching executor
```

Normal scans fully support metadata columns through Spark's `SupportsMetadataColumns` interface and preserve them through the entire query planning pipeline.

### Investigation

See [`docs/staged_scan_investigation.md`](../../docs/staged_scan_investigation.md) for detailed investigation including:
- Stack traces showing where staged scans fail
- Analysis of Spark's optimization phases
- Why `toOutputAttrs` can't map metadata column names to field IDs
- Multiple attempted solutions and their outcomes

### What Needs to Be Done

**Option 1: Fix Staged Scan Metadata Column Support (Preferred)**

Investigate and fix the root cause in Iceberg's staged scan implementation:
- Ensure metadata columns survive Spark's `V2ScanRelationPushDown` optimization
- Fix `PushDownUtils.toOutputAttrs` mapping for metadata columns
- Maintain backward compatibility with existing staged scan usage

**Option 2: Optimize Normal Scan Path (Workaround)**

If fixing staged scans proves complex:
- Cache manifest scan results between planning and execution
- Optimize file path filter evaluation
- Consider pre-computed file sets similar to staged scans

**Option 3: Alternative Metadata Propagation**

Explore alternative mechanisms for propagating metadata:
- Custom Spark physical plan nodes that inject metadata
- Partition-level metadata aggregation instead of row-level
- Broadcast variables containing file-to-position mappings

### Current Workaround

The ~10-20% performance overhead is acceptable for the initial implementation because:
- Compaction map generation is an opt-in feature (disabled by default)
- Used primarily for high-concurrency workloads where conflict resolution matters more than raw throughput
- Overhead only applies when `write.compaction-map.enabled=true`
- Most production workloads can absorb this cost for the safety guarantees

### Code Location

```
spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackFileRewriteRunner.java

Lines 55-116: Scan type selection logic
Lines 57-67: Documentation explaining the tradeoff
Lines 69-104: Normal scan path with metadata columns
Lines 106-116: Staged scan fallback when position tracking disabled
```

### Validation

To verify the performance impact:
```bash
# Run bin-pack without position tracking (staged scans)
./gradlew :iceberg-spark:iceberg-spark-3.5_2.13:test \
  --tests "TestRewriteDataFilesAction.testBinPackWithoutCompactionMaps"

# Run bin-pack with position tracking (normal scans)
./gradlew :iceberg-spark:iceberg-spark-3.5_2.13:test \
  --tests "TestRewriteDataFilesAction.testBinPackWithCompactionMaps"

# Compare execution times and manifest read counts
```

---

## 2. Position Tracking Limited to Bin-Pack Rewrites

### Issue

Position tracking is only implemented for **bin-pack rewrites** (combining multiple data files without reordering). Rewrite operations that reorder rows are not supported.

### Impact

**Functionality: Rewrite-time reordering not supported**
- ✅ **Bin-pack rewrites**: Multiple files → single/multiple files (simple concatenation)
- ✅ **Merge compactions**: Combining data files with position deletes (deletes applied during scan)
- ❌ **Sorted rewrites**: Row order changes during rewrite operation (e.g., SORT BY column)
- ❌ **Z-ordered rewrites**: Data reorganization changes positions

### Why This Choice Was Made

Bin-pack is the most common compaction pattern and has simple position semantics:
- Source rows map sequentially to target: `source[0..N] → target[offset..offset+N]`
- When position deletes exist, they're applied during scan (standard Iceberg behavior)
- Only surviving rows appear in DataFrame with `_file` and `_pos` metadata
- Gaps in runs automatically represent deleted positions

### How Merge Compactions Work

The implementation **DOES support merge compactions** (bin-pack with position deletes):

**Example:**
```
Source file A: positions 0, 1, 2, 3, 4
Position delete: delete row 2 from file A
Scan phase: Iceberg applies deletes, returns rows with _pos = 0, 1, 3, 4
Write phase: PositionTrackingDataWriter records mappings for _pos = 0, 1, 3, 4
Target file: positions 0, 1, 2, 3
Compaction map: Run(0, 0, 2), Run(3, 2, 2)  // Gap at source position 2
```

**Why It Works:**
1. Position deletes applied during scan (before position tracking sees the data)
2. Only surviving rows get position mappings
3. Gap at source position 2 automatically represented by non-consecutive runs
4. No special instrumentation needed for delete handling

### What Needs to Be Done

**For Sorted Rewrites:**
- Track position transformations as rows are reordered during sort operation
- Record which source position maps to which target position after sorting
- Instrument Spark's sort operator to capture position changes

**For Z-Ordered Rewrites:**
- Track position changes as data is reorganized
- Handle complex reordering patterns
- Coordinate with Z-order implementation

**Current Status:**
- Rewrite-time sorting not supported (would require tracking through Spark's sort operator)
- Scan-time filtering fully supported (position deletes applied during scan)

### Validation

Comprehensive tests needed to verify merge compactions:
```bash
# Test bin-pack with position deletes
./gradlew :iceberg-spark:iceberg-spark-3.5_2.13:test \
  --tests "TestBinPackWithPositionTracking.testBinPackWithPositionDeletes"

# Verify compaction maps have correct gaps
# Verify position delete remapping works end-to-end
```

---

## 3. Automatic Conflict Resolution (Partial)

### Issue

Automatic conflict resolution is **now available for compaction operations** (Spark 3.5), but **application transactions still require manual resolution**.

### Current Status

**✅ Compaction Operations (Spark 3.5):**

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

**Limitations:**
- Only V2 format tables (position delete files)
- V3+ uses Deletion Vectors with different semantics
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

**V3 Deletion Vector Support:**

Extend compaction conflict resolution to V3 format tables with Deletion Vectors.

### Code Location

**Compaction Resolution (Implemented):**
```
spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkRewriteDataFilesCommitManager.java
  detectAndResolveConflicts() - Detects and resolves conflicts during commit

spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkCompactionConflictResolver.java
  resolve() - Reads, remaps, and writes conflicting position deletes

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
# Test compaction conflict resolution
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test \
  --tests "TestSparkCompactionConflictResolution"

# Test manual resolution (application transactions)
./gradlew :iceberg-core:test --tests "TestCompactionConflictResolution"
```

---

## Summary

| # | Issue | Impact | Status | Priority |
|---|-------|--------|--------|----------|
| 1 | Normal scans vs staged scans | 10-20% performance overhead | Documented, acceptable | Medium |
| 2 | Bin-pack only position tracking | Rewrite-time reordering unsupported (sorted/Z-ordered) | Merge compactions work | Low |
| 3 | Automatic conflict resolution | Compactions ✅, Application transactions ❌ | Partially implemented | Low |

**Fixed Issues (Removed from Active List):**
- ~~Compaction map location not in manifests~~ - ✅ FIXED in commit 41324b697
- ~~Target-pending placeholder bug~~ - ✅ FIXED in commit e8287a752
- ~~Spark 3.5 format v3 + position tracking~~ - ✅ FIXED in commit e8287a752
- ~~Spark 3.5/4.0 partitioned table position tracking~~ - ✅ FIXED in commit 8b811d951
- ~~Spark 4.0 format v3 + position tracking~~ - ✅ FIXED in commit 65dedde35

## How to Contribute

If you'd like to help address any of these issues:

1. **Normal Scans Performance:** Start with `docs/staged_scan_investigation.md` to understand why staged scans fail, then investigate fixes in Iceberg's staged scan implementation.

2. **Sorted Rewrite Position Tracking:** Design position tracking framework that instruments Spark's sort operator to track position transformations through reordering operations.

3. **Application Transaction Conflict Resolution:** Implement opt-in automatic remapping in `BaseRowDelta` for application-level position delete conflicts. The compaction-level resolution (`SparkRewriteDataFilesCommitManager`) is already complete.

4. **V3 Deletion Vector Conflict Resolution:** Extend `SparkCompactionConflictResolver` to support V3 format tables with Deletion Vectors.

5. **Spark 4.0 Conflict Resolution Parity:** Port `SparkCompactionConflictResolver` and `SparkRewriteDataFilesCommitManager` from Spark 3.5 to Spark 4.0, along with corresponding tests.

## References

- [Main Compaction Maps Documentation](compaction_maps.md)
- [Staged Scan Investigation](../../docs/staged_scan_investigation.md)
