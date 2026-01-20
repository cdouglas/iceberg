---
title: "Compaction Maps"
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

# Compaction Maps

## Overview

Compaction maps enable concurrent transactions writing position deletes to coexist with compaction operations. When data files are compacted, existing position deletes that reference the old files become invalid. Compaction maps track the position transformations from source to target files, allowing transactions to automatically remap their position deletes when conflicts are detected.

## Motivation

In Apache Iceberg, **position deletes** identify deleted rows using `(file_path, row_position)` tuples. When a data file is compacted (rewritten with other files), the original file is removed and positions change in the new files. This creates a conflict:

**Without Compaction Maps:**
1. Transaction A reads snapshot S1 and writes position deletes referencing file F1
2. Transaction B compacts F1 into F2, creating snapshot S2
3. Transaction A tries to commit on top of S2
4. **Result:** Transaction A fails because F1 no longer exists

**With Compaction Maps:**
1. Transaction A reads snapshot S1 and writes position deletes referencing file F1
2. Transaction B compacts F1 into F2, creating snapshot S2 with a compaction map
3. Transaction A tries to commit on top of S2
4. Iceberg detects the conflict and provides compaction map for remapping
5. **Result:** Transaction A can remap deletes from F1 → F2 and retry

## Current State

### What's Complete

✅ **Core Infrastructure (Phases 1-5)**
- CompactionMap data structures with Avro serialization and run-length encoding
- CompactionMapBuilder with automatic run merging for consecutive position ranges
- Storage utilities, configuration properties, and ManifestFile schema extension
- PositionDeleteRemapper for remapping position deletes to new file locations
- CompactionMapValidator for detecting conflicts during transaction commit
- SERIALIZABLE isolation enhancements with compaction awareness
- 61+ unit and integration tests covering all components (full V2/V3 parity)

✅ **Deletion Vector (DV) Support**
- DVPositionWriter utility for writing DVs from position collections
- RemappedDVWriter helper for N:M remapping scenarios (multiple target files)
- PositionDeleteRemapper supports both position delete files and deletion vectors
- DVPositionReader for reading positions from DV files
- Comprehensive end-to-end integration tests across V2 (position deletes) and V3 (deletion vectors)

✅ **Spark 3.5 Position Tracking**
- Position tracking fully implemented for bin-pack rewrites
- PositionTrackingDataWriter extracts source metadata during write
- PositionMappingCoordinator aggregates mappings across distributed executors
- Automatic compaction map generation during commit
- Accurate run-based tracking with gaps for deleted rows

✅ **Compaction Conflict Resolution (Phase 3)**
- SparkRewriteDataFilesCommitManager detects conflicts with concurrent position delete transactions
- SparkCompactionConflictResolver remaps conflicting deletes using Spark infrastructure
- Opt-in via `write.compaction.resolve-delete-conflicts` table property
- Configurable max-files limit for safety
- 6 integration tests covering all conflict resolution scenarios

✅ **Merge Compaction Support**
- Position deletes applied during scan (standard Iceberg behavior)
- Only surviving rows tracked in position mappings
- Gaps in runs automatically represent deleted positions
- Works for bin-pack operations combining data files with position deletes

### Test Coverage

✅ **Comprehensive Test Suite Complete (150+ tests passing)**

The compaction maps feature has comprehensive test coverage across all components:
- Core infrastructure (serialization, builder, storage, remapping)
- Position delete remapping with both position delete files and deletion vectors
- **Remapping algorithm optimization (59 tests)** - Binary search, interval tree, stream join, range query, smart selector
- Conflict detection and resolution workflows (full V2/V3 parity)
- SERIALIZABLE isolation with compaction awareness (full V2/V3 parity)
- End-to-end Spark integration tests for conflict detection and resolution
- **Compaction conflict resolution (6 tests)** - TestSparkCompactionConflictResolution
- Format version compatibility (v2 position deletes, v3 deletion vectors)
- File format support (Parquet, ORC, Puffin for DVs)
- **JMH performance benchmarks (324 configurations)** - Empirical validation across 54 workload scenarios

See [Implementation Details](compaction_maps_impl.md#test-coverage) for detailed test descriptions and execution commands

✅ **Spark 4.0 Position Tracking**

Position tracking is fully functional in Spark 4.0 for both V2 and V3 format tables with Parquet and ORC file formats.

### Implementation Shortcuts

See [Compaction Maps Errata](compaction_maps_errata.md) for documented implementation shortcuts including:
- Position tracking limited to bin-pack rewrites
- Automatic conflict resolution: compactions ✅, application transactions ❌

## Configuration

### Table Properties

**`write.compaction-map.enabled`** (default: `false`)
- Controls whether compaction maps are generated during compaction operations
- Set to `true` to enable compaction map generation for bin-pack rewrites
- **Note:** Position tracking is supported in Spark 3.5 and Spark 4.0

**`write.compaction-map.target-size-bytes`** (default: `8388608` / 8 MB)
- Target size for compaction map files (currently not enforced)
- Used for monitoring and documentation
- Run-length encoding keeps maps compact for typical workloads

**`write.delete.isolation-level`** (default: `"serializable"`)
- Controls isolation level for DELETE/UPDATE/MERGE operations
- `"serializable"`: Validates concurrent data changes with compaction awareness
- `"snapshot"`: No validation of concurrent operations

**`write.compaction.resolve-delete-conflicts`** (default: `false`)
- Enables automatic resolution of conflicts with concurrent position delete transactions during compaction
- When enabled, compaction operations can detect and remap conflicting position deletes
- Only applies to V2 format tables with position delete files (not DVs)

**`write.compaction.resolve-delete-conflicts.max-files`** (default: `100`)
- Maximum number of conflicting delete files to resolve automatically
- Safety limit to prevent excessive overhead from large conflict sets
- If exceeded, compaction throws `ValidationException` requiring manual intervention

### Example Configuration

```java
// Enable compaction maps for a table (Spark 3.5 and 4.0)
table.updateProperties()
    .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
    .set(TableProperties.DELETE_ISOLATION_LEVEL, "serializable")
    .commit();

// Run bin-pack compaction (maps generated automatically)
RewriteFiles rewrite = table.newRewrite();
sourceFiles.forEach(rewrite::deleteFile);
targetFiles.forEach(rewrite::addFile);
rewrite.commit();
```

### Compaction with Automatic Conflict Resolution

```java
// Enable compaction maps AND conflict resolution
table.updateProperties()
    .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
    .set(TableProperties.COMPACTION_RESOLVE_DELETE_CONFLICTS, "true")
    .set(TableProperties.COMPACTION_RESOLVE_DELETE_CONFLICTS_MAX_FILES, "50")
    .commit();

// Run compaction via Spark action
// If concurrent deletes occurred, they are automatically remapped
SparkActions.get(spark)
    .rewriteDataFiles(table)
    .execute();
```

**What Happens During Conflict Resolution:**

1. Compaction starts at snapshot S1 and rewrites files
2. Concurrent transaction adds position deletes, creating S2
3. Compaction detects conflicts with S2's deletes during commit
4. SparkCompactionConflictResolver reads the conflicting delete files
5. Uses PositionDeleteRemapper to remap positions from source → target files
6. Writes new delete files referencing the compacted files
7. Commits compaction with remapped deletes included

### SERIALIZABLE Isolation with Compaction Awareness

Compaction maps enable SERIALIZABLE isolation to distinguish between structural and logical data changes:

```java
// Start DELETE transaction with SERIALIZABLE isolation
RowDelta rowDelta = table.newRowDelta()
    .validateFromSnapshot(startingSnapshotId)
    .conflictDetectionFilter(Expressions.equal("region", "us-west"))
    .validateNoConflictingDataFiles();  // Enable SERIALIZABLE

rowDelta.addDeletes(deleteFile);

// Concurrent compaction occurred:
// - WITH compaction map: commit succeeds (structural change only)
// - WITHOUT compaction map: ValidationException (potential data change)
rowDelta.commit();
```

## Limitations

### 1. Spark Version Support

- **Spark 3.5:** ✅ Position tracking fully implemented and functional
- **Spark 4.0:** ✅ Position tracking fully implemented and functional
- **Other engines:** Compaction map infrastructure works (read/validate/remap), but generation requires Spark-specific position tracking

### 2. Design Scope: Order-Preserving Compactions

Compaction maps support **order-preserving** compaction operations:

- ✅ **Bin-pack rewrites**: Multiple small files → larger files (simple concatenation)
- ✅ **Merge compactions**: Combining data files with position deletes applied during scan

**Out of scope by design:**
- **Sorted compactions**: Rewriting data sorted by column(s)
- **Z-ordered compactions**: Reorganizing data along a space-filling curve

Order-changing operations are **not appropriate** for compaction maps because:
1. Reordering produces degenerate maps (runs of length 1), defeating run-length encoding
2. Position deletes identify rows by position—after reordering, position N refers to a different logical row
3. For sorted/Z-ordered compactions, use equality deletes or accept that position deletes are invalidated

**How Merge Compactions Work:**

When compacting files with position deletes:
1. Normal scan reads data files and applies position deletes (standard Iceberg behavior)
2. Only surviving rows appear in the DataFrame with `_file` and `_pos` metadata
3. PositionTrackingDataWriter records mappings only for surviving rows
4. Gaps in compaction map runs automatically represent deleted positions

Example:
```
Source file: positions 0, 1, 2, 3, 4
Position delete: row 2
Scan output: rows with _pos = 0, 1, 3, 4 (row 2 filtered)
Target positions: 0, 1, 2, 3
Compaction map: Run(0, 0, 2), Run(3, 2, 2)  // Gap at source position 2
```

### 3. Conflict Resolution Options

**For Compaction Operations (Spark 3.5):**

Automatic conflict resolution is available via `write.compaction.resolve-delete-conflicts=true`. When enabled, compactions automatically detect and remap conflicting position deletes from concurrent transactions. This is the recommended approach for high-concurrency workloads.

**Supported Delete Types:**
- ✅ **File-scoped position deletes**: Deletes with `referencedDataFile` set (created with `DeleteGranularity.FILE`)
- ✅ **Multi-file position deletes**: Deletes spanning multiple data files (created with `DeleteGranularity.PARTITION`) - these are detected as potential conflicts and resolved by reading delete content
- ❌ **Equality deletes**: Not file-scoped, handled by standard Iceberg semantics (not conflicts)

**Limitations:**
- Only available for V2 format tables (position delete files)
- V3+ uses Deletion Vectors which have different semantics
- Subject to `max-files` limit for safety
- Multi-file position deletes require reading delete file content for resolution (additional I/O)

**For Application Transactions:**

Position delete conflicts from application transactions (e.g., RowDelta) require handling with `PositionDeleteRemapper`. The complete workflow is:

```java
try {
    rowDelta.addDeletes(deleteFile);
    rowDelta.commit();
} catch (CompactionConflictException e) {
    // 1. Load remappers from exception (handles multiple compaction maps)
    Map<String, PositionDeleteRemapper> remappers =
        PositionDeleteRemapper.fromConflict(e, table.io());

    // 2. Read original position deletes and remap
    List<PositionDelete<?>> remappedDeletes = new ArrayList<>();
    int skippedCount = 0;

    for (PositionDelete<?> delete : readPositionDeletes(deleteFile)) {
        String path = delete.path().toString();
        PositionDeleteRemapper remapper = remappers.get(path);

        if (remapper == null) {
            // File was not compacted, keep original
            remappedDeletes.add(delete);
        } else {
            // Remap using lenient mode (returns null if row was filtered)
            PositionDelete<?> remapped = remapper.remapDeleteOrNull(delete);
            if (remapped != null) {
                remappedDeletes.add(remapped);
            } else {
                // Row was filtered during merge compaction - skip (it's a no-op)
                skippedCount++;
            }
        }
    }

    // 3. Write remapped deletes to new file
    DeleteFile remappedDeleteFile = writePositionDeletes(remappedDeletes, table);

    // 4. Retry with remapped deletes
    RowDelta retry = table.newRowDelta();
    retry.addDeletes(remappedDeleteFile);
    retry.commit();

    LOG.info("Remapped {} deletes, skipped {} filtered positions",
        remappedDeletes.size(), skippedCount);
}
```

**Key API Methods:**
- `PositionDeleteRemapper.fromConflict(exception, io)` - Loads remappers from exception
- `remapper.remapDeleteOrNull(delete)` - Remaps delete, returns null if position was filtered (lenient mode)
- `remapper.remapDelete(delete)` - Remaps delete, throws exception if position not found (strict mode)
- `remapper.mayNeedRemapping(deleteFile)` - Conservative check for multi-file position deletes

**Handling Unmapped Positions:**
When a position is not found in the compaction map, it typically means the row was filtered during merge compaction (position deletes were applied during the scan). Using `remapDeleteOrNull()` handles this gracefully by returning `null`, allowing you to safely skip these positions.

**Note:** SERIALIZABLE isolation provides automatic handling for read conflicts (distinguishes structural vs data changes), but position delete conflicts from application transactions still require manual remapping.

### 4. Performance Overhead

When position tracking is enabled:
- Uses efficient staged scans with explicit metadata column selection
- Minimal overhead compared to standard bin-pack rewrites
- Available in Spark 3.5 and Spark 4.0

### 5. Remapping Algorithm Efficiency

The remapping implementation includes multiple optimized algorithms with automatic selection:

**Implemented Strategies:**

| Strategy | Complexity | Best For | Status |
|----------|------------|----------|--------|
| Linear Search | O(n*m) | m < 10 (baseline) | ✅ Complete |
| Binary Search | O(n log m) | 10 ≤ m < 100 | ✅ Complete |
| Interval Tree | O(n log m) | m ≥ 100 | ✅ Complete |
| Stream Join | O(n + m) | Sorted positions | ✅ Complete |
| Range Query | O(m log n) | High fan-in (n >> m) | ✅ Complete |

**Performance Improvements:**

- **Single-position lookup**: 15-100x speedup (binary search and interval tree)
- **Bulk sorted remapping**: 100-750x speedup (stream join)
- **High fan-in scenarios**: 250x speedup (range query)
- **Automatic selection**: Smart algorithm selector chooses optimal strategy based on workload characteristics (run count, position count, sortedness, gap ratio)

**Example Performance (n=10,000 positions):**

| Runs (m) | Linear | Binary | Stream Join | Range Query | Best Strategy |
|----------|--------|--------|-------------|-------------|---------------|
| 10 | 100K ops | 33K ops | 10K ops | **133 ops** | Range Query (750x) |
| 100 | 1M ops | 67K ops | **10K ops** | 1.3K ops | Stream Join (100x) |
| 1000 | 10M ops | 100K ops | **11K ops** | 13K ops | Stream Join (900x) |

**Smart Selection:**

The `RemappingAlgorithmSelector` automatically chooses the optimal strategy based on:
- Run count (m): Number of runs in compaction map
- Position count (n): Number of positions to remap
- Sortedness: Whether positions are sorted (detected via sampling)
- Gap ratio: Percentage of source range not covered by runs

Selection overhead is <5% and provides near-optimal performance across all workload types.

**Benchmarking:**

Comprehensive JMH benchmark suite validates performance across 54 scenarios. See `REMAPPING_BENCHMARKS.md` for detailed benchmarking documentation and instructions.

## References

- **[Implementation Details](compaction_maps_impl.md)** - Architecture, API usage, testing, and Spark implementation
- **[Implementation Errata](compaction_maps_errata.md)** - Known shortcuts and technical debt
- **[Staged Scan Investigation](../../docs/staged_scan_investigation.md)** - Historical investigation of staged scan metadata columns (now resolved)
- [Iceberg Position Deletes Specification](https://iceberg.apache.org/spec/#position-delete-files)
- [Iceberg Manifest Format](https://iceberg.apache.org/spec/#manifests)

## Future Work

1. **Spark 4.0 Conflict Resolution Parity** - Port `SparkCompactionConflictResolver` and `SparkRewriteDataFilesCommitManager` from Spark 3.5 to Spark 4.0
2. **Application Transaction Conflict Resolution** - Automatic remapping in BaseRowDelta for application-level position delete conflicts
3. **Other Engine Integration** - Extend position tracking to Flink, Trino, etc.
