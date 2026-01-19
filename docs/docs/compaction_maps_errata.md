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
| 1 | Order-preserving compactions only | Order-changing ops (sort, Z-order) out of scope | By design | - |
| 2 | Automatic conflict resolution | Compactions ✅, Application transactions ❌ | Partially implemented | Low |

**Fixed Issues (Removed from Active List):**
- ~~Normal scans vs staged scans~~ - ✅ FIXED: Staged scans now work with explicit metadata column selection
- ~~Compaction map location not in manifests~~ - ✅ FIXED in commit 41324b697
- ~~Target-pending placeholder bug~~ - ✅ FIXED in commit e8287a752
- ~~Spark 3.5 format v3 + position tracking~~ - ✅ FIXED in commit e8287a752
- ~~Spark 3.5/4.0 partitioned table position tracking~~ - ✅ FIXED in commit 8b811d951
- ~~Spark 4.0 format v3 + position tracking~~ - ✅ FIXED in commit 65dedde35

## How to Contribute

If you'd like to help address any of these issues:

1. **Application Transaction Conflict Resolution:** Implement opt-in automatic remapping in `BaseRowDelta` for application-level position delete conflicts. The compaction-level resolution (`SparkRewriteDataFilesCommitManager`) is already complete.

2. **V3 Deletion Vector Conflict Resolution:** Extend `SparkCompactionConflictResolver` to support V3 format tables with Deletion Vectors.

3. **Spark 4.0 Conflict Resolution Parity:** Port `SparkCompactionConflictResolver` and `SparkRewriteDataFilesCommitManager` from Spark 3.5 to Spark 4.0, along with corresponding tests.

## References

- [Main Compaction Maps Documentation](compaction_maps.md)
- [Staged Scan Investigation](../../docs/staged_scan_investigation.md)
