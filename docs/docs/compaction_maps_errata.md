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

## 2. Chained Compaction Maps Not Supported

### Issue

When multiple compactions occur between a transaction's start and commit, the current implementation does not compose (chain) compaction maps. This can result in **incorrect or incomplete remapping** when position deletes reference files that were compacted through multiple intermediate states.

### Scenario

Consider a transaction T1 that starts at snapshot S1 with position deletes for file F1:

```
Timeline:
─────────────────────────────────────────────────────────────────────────────
S1: File F1 exists
    │
    │  Transaction T1 starts (base = S1)
    │  T1 creates position deletes for F1
    │
    ▼
S2: Compaction C1 rewrites F1 → F2
    Compaction map M1: source=S1, target=S2
    FileMapping: F1 → F2 with runs [(0,0,1000), ...]
    │
    ▼
S3: Compaction C2 rewrites F2 → F3
    Compaction map M2: source=S2, target=S3
    FileMapping: F2 → F3 with runs [(0,0,1000), ...]
    │
    │  T1 attempts to commit at S3
    │
─────────────────────────────────────────────────────────────────────────────

Required remapping: F1 → F2 → F3 (chain M1 and M2)
Current behavior: Only M1 is found (F1 is sourceFile in M1)
                  Remapping produces deletes for F2, which no longer exists!
```

### Impact

**Severity: Data Correctness**

When this scenario occurs:

1. **CompactionMapValidator** finds M1 because F1 appears as a source file
2. **PositionDeleteRemapper** remaps F1 positions to F2 positions using M1
3. The remapped deletes reference F2, but F2 was compacted to F3
4. The commit may succeed with deletes pointing to a non-existent file, or fail validation

**When This Can Happen:**

- High-frequency compaction schedules (multiple compactions between transaction retries)
- Long-running transactions that span multiple compaction cycles
- Batch jobs that retry after failures, encountering accumulated compactions

**When This Cannot Happen:**

- Single compaction between transaction start and commit (the common case)
- Compactions that don't touch the same files (disjoint file sets)

### Current Behavior

The `CompactionMapValidator.findCompactionMaps()` method walks the snapshot history and finds all compaction maps, but:

1. It indexes maps by **source file path** only
2. When a file F1 is found in map M1, it returns M1's location
3. It does **not** check if M1's target (F2) was subsequently compacted
4. The `PositionDeleteRemapper` takes a single map—no composition logic exists

```java
// CompactionMapValidator.java:119-143
Map<String, String> findCompactionMaps() {
    Map<String, String> compactionMaps = Maps.newHashMap();
    Snapshot snapshot = currentSnapshot;
    while (snapshot != null && snapshot.snapshotId() != startingSnapshotId) {
        for (ManifestFile manifest : snapshot.dataManifests(io)) {
            String mapLocation = manifest.compactionMapLocation();
            if (mapLocation != null) {
                CompactionMap map = CompactionMaps.read(io.newInputFile(mapLocation));
                for (CompactionMap.FileMapping mapping : map.fileMappings()) {
                    // BUG: Only indexes by source, doesn't track chains
                    compactionMaps.put(mapping.sourceFile(), mapLocation);
                }
            }
        }
        // ... walk to parent snapshot
    }
    return compactionMaps;
}
```

### Source Snapshot ID Purpose

The `source_snapshot_id` field in compaction maps exists precisely to enable chain detection:

- **M1**: `source_snapshot_id = S1`, `target_snapshot_id = S2`
- **M2**: `source_snapshot_id = S2`, `target_snapshot_id = S3`

A correct implementation could use these to:
1. Verify `M1.target_snapshot_id == M2.source_snapshot_id` (chain continuity)
2. Compose the mappings: F1 → F2 (via M1) → F3 (via M2)

Currently, these IDs are stored but **not used for validation or chaining**.

### Possible Remediations

#### Option A: Eager Map Composition at Commit Time

When a new compaction commits, compose it with any existing maps that reference its source files.

**Approach:**
```
When committing compaction C2 (F2 → F3):
1. Find any existing maps where targetFile = F2
2. For each such map M1 (F1 → F2):
   - Create composed mapping: F1 → F3
   - Store composed map alongside or instead of M2
3. Transactions only ever need a single map lookup
```

**Pros:**
- Simple consumer logic (no chaining needed at read time)
- Single map lookup during conflict resolution

**Cons:**
- Increases commit complexity
- Maps grow larger over time (accumulate all historical mappings)
- Requires loading and modifying maps during commit

#### Option B: Lazy Map Chaining at Resolution Time

Compose maps on-demand when resolving conflicts.

**Approach:**
```
When resolving conflict for T1 (deletes for F1):
1. Find M1 where F1 is sourceFile → F1 maps to F2
2. Check if F2 appears as sourceFile in any map
3. If yes, find M2 where F2 is sourceFile → F2 maps to F3
4. Compose: F1 → F3
5. Repeat until target file exists in current snapshot
```

**Pros:**
- No change to commit path
- Maps stay small (only track immediate transformations)
- Composition only done when needed

**Cons:**
- More complex resolution logic
- Multiple map loads during resolution
- Must handle cycles (detect infinite loops from corrupted maps)

#### Option C: Limit Compaction Frequency

Operational mitigation: ensure at most one compaction occurs between any transaction's start and commit.

**Approach:**
- Document the limitation clearly
- Recommend compaction scheduling that avoids rapid successive compactions
- Add validation that warns or fails if chained compactions are detected

**Pros:**
- No code changes required
- Simple to understand

**Cons:**
- Limits operational flexibility
- Doesn't fix the underlying issue
- Hard to enforce in distributed systems

#### Option D: Validate and Reject Chained Scenarios

Detect when chaining would be required and fail fast with a clear error.

**Approach:**
```
During conflict detection:
1. Find map M1 for source file F1
2. Check if M1's target file F2 was also compacted
3. If yes, throw an error explaining the limitation
4. User must manually resolve or wait for compaction to settle
```

**Pros:**
- Prevents silent data corruption
- Clear error message guides users
- Minimal implementation effort

**Cons:**
- Degrades to failure instead of handling the case
- May cause spurious failures in high-compaction environments

### Recommended Approach

**Short-term (Option D):** Add validation to detect and reject chained compaction scenarios with a clear error message. This prevents data corruption while we develop a complete solution.

**Long-term (Option B):** Implement lazy map chaining at resolution time. This keeps the commit path simple and only adds complexity where needed. The `source_snapshot_id` and `target_snapshot_id` fields already support this—we just need to use them.

### Code Locations

```
core/src/main/java/org/apache/iceberg/CompactionMapValidator.java
  findCompactionMaps() - Needs chain detection logic

core/src/main/java/org/apache/iceberg/PositionDeleteRemapper.java
  - Needs to accept multiple maps or a composed map

core/src/main/java/org/apache/iceberg/CompactionMaps.java
  - Add compose(map1, map2) method for map composition

api/src/main/java/org/apache/iceberg/CompactionMap.java
  - sourceSnapshotId() and targetSnapshotId() exist but unused
```

### Validation

```bash
# Once fixed, add test for chained compaction scenario
./gradlew :iceberg-core:test --tests "TestCompactionMapChaining"
```

---

## 3. Automatic Conflict Resolution (Partial)

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

## Summary

| # | Issue | Impact | Status | Priority |
|---|-------|--------|--------|----------|
| 1 | Order-preserving compactions only | Order-changing ops (sort, Z-order) out of scope | By design | - |
| 2 | Chained compaction maps not supported | Data correctness risk with multiple compactions | Not implemented | **High** |
| 3 | Automatic conflict resolution | Compactions ✅, Application transactions ❌ | Partially implemented | Low |

**Fixed Issues (Removed from Active List):**
- ~~Source files spanning multiple targets~~ - ✅ FIXED: Per-run target files now supported (Jan 24, 2026)
- ~~Normal scans vs staged scans~~ - ✅ FIXED: Staged scans now work with explicit metadata column selection
- ~~V3 Deletion Vector conflict resolution~~ - ✅ FIXED: SparkCompactionConflictResolver now supports DVs
- ~~Spark 4.0 Conflict Resolution Parity~~ - ✅ FIXED in commit 539432b51
- ~~Compaction map location not in manifests~~ - ✅ FIXED in commit 41324b697
- ~~Target-pending placeholder bug~~ - ✅ FIXED in commit e8287a752
- ~~Spark 3.5 format v3 + position tracking~~ - ✅ FIXED in commit e8287a752
- ~~Spark 3.5/4.0 partitioned table position tracking~~ - ✅ FIXED in commit 8b811d951
- ~~Spark 4.0 format v3 + position tracking~~ - ✅ FIXED in commit 65dedde35

## How to Contribute

If you'd like to help address the remaining issues:

**Chained Compaction Maps (Priority: High):** Implement detection and handling of chained compaction scenarios. Start with Option D (validation/rejection) for safety, then implement Option B (lazy chaining) for full support. Key files: `CompactionMapValidator.java`, `PositionDeleteRemapper.java`, `CompactionMaps.java`.

**Application Transaction Conflict Resolution (Priority: Low):** Implement opt-in automatic remapping in `BaseRowDelta` for application-level position delete conflicts. The compaction-level resolution (`SparkRewriteDataFilesCommitManager`) is already complete for both Spark 3.5 and 4.0.

## References

- [Main Compaction Maps Documentation](compaction_maps.md)
