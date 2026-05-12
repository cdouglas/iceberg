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

## Configuration

### Table Properties

**`write.compaction-map.enabled`** (default: `false`)
- Controls whether compaction maps are generated during compaction operations
- Set to `true` to enable compaction map generation for bin-pack rewrites
- Position tracking is supported in Spark 3.5 and Spark 4.0

**`write.compaction-map.target-size-bytes`** (default: `8388608` / 8 MB)
- Target size for compaction map files (currently not enforced)
- Run-length encoding keeps maps compact for typical workloads

**`write.delete.isolation-level`** (default: `"serializable"`)
- Controls isolation level for DELETE/UPDATE/MERGE operations
- `"serializable"`: Validates concurrent data changes with compaction awareness
- `"snapshot"`: No validation of concurrent operations

**`write.compaction.resolve-delete-conflicts`** (default: `false`)
- Enables automatic resolution of conflicts with concurrent position delete transactions during compaction
- When enabled, compaction operations detect and remap conflicting position deletes
- Applies to both V2 position delete files and V3 deletion vectors

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

## Conflict Detection: Writes vs Reads

Compaction maps participate in two independent validation checks during commit. Understanding both is essential for correct integration.

### Write-Conflict Checking (Always Active)

Position deletes are **writes** — they contain physical `(file_path, row_position)` tuples that become stale when files are compacted. This check runs for **all** position delete transactions, regardless of isolation level:

```java
RowDelta rowDelta = table.newRowDelta()
    .validateFromSnapshot(startingSnapshotId);

rowDelta.addDeletes(deleteFile);

// If deleteFile references files that were compacted since startingSnapshotId:
// → CompactionConflictException is thrown
// → Application MUST remap and retry (see next section)
rowDelta.commit();
```

This is **mandatory for correctness**: committing stale position deletes without remapping would silently delete wrong rows or fail to delete intended rows.

### SERIALIZABLE Read-Conflict Optimization

Separately, SERIALIZABLE isolation checks whether concurrent data changes conflict with a transaction's reads. Compaction maps allow this check to distinguish structural changes (compaction) from logical changes (inserts/deletes):

```java
// Start DELETE transaction with SERIALIZABLE isolation
RowDelta rowDelta = table.newRowDelta()
    .validateFromSnapshot(startingSnapshotId)
    .conflictDetectionFilter(Expressions.equal("region", "us-west"))
    .validateNoConflictingDataFiles();  // Enable SERIALIZABLE read check

rowDelta.addDeletes(deleteFile);

// Concurrent compaction occurred (REPLACE operation):
// - WITH compaction map: no read conflict (structural change only)
// - WITHOUT compaction map: ValidationException (potential data change)
rowDelta.commit();
```

**Note:** Both checks run independently. A transaction can pass the SERIALIZABLE read check (compaction is structural) but still fail the write check (position deletes reference compacted files). The write-conflict `CompactionConflictException` must always be handled.

## Handling Compaction Conflicts in Application Transactions

When an application transaction (e.g., RowDelta) conflicts with a compaction that rewrote referenced files, a `CompactionConflictException` is thrown. The application **must** remap its position deletes and retry — this is a correctness requirement, not an optimization. Committing stale position deletes would silently corrupt the table by deleting wrong rows or missing intended deletions.

If multiple sequential compactions occurred between the transaction's start and commit (chained compactions), a `ChainedCompactionMapsException` subtype is thrown. The `PositionDeleteRemapper.fromConflict()` helper handles both cases transparently.

### Complete Example: Recovering from a Compaction Conflict

```java
import org.apache.iceberg.PositionDeleteRemapper;
import org.apache.iceberg.exceptions.CompactionConflictException;

public class DeleteTransactionWithConflictRecovery {

    private static final int MAX_RETRIES = 3;
    private static final Logger LOG = LoggerFactory.getLogger(DeleteTransactionWithConflictRecovery.class);

    /**
     * Executes a delete transaction with automatic conflict recovery.
     *
     * @param table the Iceberg table
     * @param deletePositions map of file path to positions to delete
     * @return the committed snapshot ID
     */
    public long executeDeleteWithRecovery(Table table, Map<String, long[]> deletePositions) {
        int attempt = 0;
        Map<String, long[]> currentDeletes = deletePositions;

        while (attempt < MAX_RETRIES) {
            attempt++;
            try {
                return commitDeletes(table, currentDeletes);
            } catch (CompactionConflictException e) {
                LOG.info("Compaction conflict detected on attempt {}, remapping deletes", attempt);
                currentDeletes = remapDeletes(table, currentDeletes, e);
            }
        }

        throw new RuntimeException("Failed to commit after " + MAX_RETRIES + " attempts");
    }

    private long commitDeletes(Table table, Map<String, long[]> deletePositions) {
        // Create delete file from positions
        DeleteFile deleteFile = writePositionDeletes(table, deletePositions);

        // Commit with SERIALIZABLE isolation
        RowDelta rowDelta = table.newRowDelta()
            .validateFromSnapshot(table.currentSnapshot().snapshotId())
            .validateNoConflictingDataFiles();

        rowDelta.addDeletes(deleteFile);
        rowDelta.commit();

        return table.currentSnapshot().snapshotId();
    }

    private Map<String, long[]> remapDeletes(
            Table table,
            Map<String, long[]> originalDeletes,
            CompactionConflictException conflict) {

        // Load remappers from the conflict exception
        // Handles chained compactions (multiple maps if several compactions occurred)
        Map<String, PositionDeleteRemapper> remappers =
            PositionDeleteRemapper.fromConflict(conflict, table.io());

        Map<String, long[]> remappedDeletes = new HashMap<>();
        int totalRemapped = 0;
        int totalSkipped = 0;

        for (Map.Entry<String, long[]> entry : originalDeletes.entrySet()) {
            String sourcePath = entry.getKey();
            long[] sourcePositions = entry.getValue();

            PositionDeleteRemapper remapper = remappers.get(sourcePath);

            if (remapper == null) {
                // File was not compacted, keep original deletes
                remappedDeletes.put(sourcePath, sourcePositions);
                continue;
            }

            // Remap positions to target file(s)
            // Returns Map<targetPath, targetPositions[]>
            Map<String, long[]> mapped = remapper.remapPositionsBulkPrimitive(sourcePath, sourcePositions);

            for (Map.Entry<String, long[]> targetEntry : mapped.entrySet()) {
                String targetPath = targetEntry.getKey();
                long[] targetPositions = targetEntry.getValue();

                // Merge with existing positions for this target file
                remappedDeletes.merge(targetPath, targetPositions, this::mergePositions);
                totalRemapped += targetPositions.length;
            }

            // Count positions that couldn't be remapped (deleted during merge compaction)
            int mappedCount = mapped.values().stream().mapToInt(arr -> arr.length).sum();
            totalSkipped += sourcePositions.length - mappedCount;
        }

        LOG.info("Remapped {} positions, skipped {} (already deleted during compaction)",
            totalRemapped, totalSkipped);

        return remappedDeletes;
    }

    private long[] mergePositions(long[] a, long[] b) {
        long[] merged = new long[a.length + b.length];
        System.arraycopy(a, 0, merged, 0, a.length);
        System.arraycopy(b, 0, merged, a.length, b.length);
        Arrays.sort(merged);
        return merged;
    }

    private DeleteFile writePositionDeletes(Table table, Map<String, long[]> deletePositions) {
        // Implementation depends on table format version and engine
        // For V2: Write Parquet position delete file
        // For V3: Write deletion vector (Puffin file with roaring bitmap)
        // ...
    }
}
```

### Key Points for Conflict Recovery

1. **Catch `CompactionConflictException`**: This exception contains the compaction maps needed for remapping.

2. **Load remappers**: `PositionDeleteRemapper.fromConflict(exception, io)` handles loading and composing multiple compaction maps if several compactions occurred between the transaction's start and commit.

3. **Handle multi-target remapping**: A single source file may map to multiple target files (when file size limits cause splits). The `remapPositionsBulkPrimitive()` method returns a map from target paths to position arrays.

4. **Handle filtered positions**: During merge compaction, some rows are deleted by position deletes applied during the scan. These positions won't appear in the compaction map—they're no-ops and can be safely skipped.

5. **Retry with remapped deletes**: After remapping, create new delete files targeting the compacted files and retry the commit.

### Simplified Pattern for Single-Position Remapping

For simpler cases with individual position deletes:

```java
try {
    rowDelta.addDeletes(deleteFile);
    rowDelta.commit();
} catch (CompactionConflictException e) {
    Map<String, PositionDeleteRemapper> remappers =
        PositionDeleteRemapper.fromConflict(e, table.io());

    List<PositionDelete<?>> remappedDeletes = new ArrayList<>();

    for (PositionDelete<?> delete : readPositionDeletes(deleteFile)) {
        String path = delete.path().toString();
        PositionDeleteRemapper remapper = remappers.get(path);

        if (remapper == null) {
            // File not compacted, keep original
            remappedDeletes.add(delete);
        } else {
            // remapDeleteOrNull returns null if position was filtered during merge
            PositionDelete<?> remapped = remapper.remapDeleteOrNull(delete);
            if (remapped != null) {
                remappedDeletes.add(remapped);
            }
        }
    }

    // Write and commit remapped deletes — validate from current snapshot
    // to detect any further compactions during the retry
    table.refresh();
    DeleteFile remappedFile = writePositionDeletes(remappedDeletes, table);
    table.newRowDelta()
        .validateFromSnapshot(table.currentSnapshot().snapshotId())
        .addDeletes(remappedFile)
        .commit();
}
```

## Limitations

### 1. Spark Version Support

- **Spark 3.5:** Position tracking fully implemented
- **Spark 4.0:** Position tracking fully implemented
- **Other engines:** Compaction map infrastructure works (read/validate/remap), but generation requires Spark-specific position tracking

### 2. Design Scope: Order-Preserving Compactions

Compaction maps support **order-preserving** compaction operations:

- **Bin-pack rewrites**: Multiple small files → larger files (simple concatenation)
- **Merge compactions**: Combining data files with position deletes applied during scan

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

### 3. Conflict Resolution Scope

**Automatic resolution (compaction operations):**
- Enabled via `write.compaction.resolve-delete-conflicts=true`
- Compactions detect and remap conflicting position deletes from concurrent transactions
- Subject to `max-files` limit for safety

**Manual resolution (application transactions):**
- Application transactions must catch `CompactionConflictException` and remap using `PositionDeleteRemapper`
- See [Handling Compaction Conflicts](#handling-compaction-conflicts-in-application-transactions) for complete example

## Performance

### End-to-End Remapping Performance

Benchmarks measured across AWS, GCP, and Azure with cloud storage (S3, GCS, ADLS) show the complete read-remap-write cycle:

**At 1M deletes (production-scale workload):**

| Format | Avg Latency | Throughput | Notes |
|--------|-------------|------------|-------|
| Position Delete Files | 2039ms | 0.5M deletes/sec | Parquet I/O dominates |
| Deletion Vectors | 379ms | 2.9M deletes/sec | RoaringBitmap + Puffin |

**Deletion vectors are 5.4x faster** than position delete files at scale, primarily due to:
- Compact RoaringBitmap representation vs row-per-delete Parquet
- Efficient bulk iteration (always sorted)
- Smaller I/O footprint

**By Cloud Provider (1M deletes, 100 runs):**

| Cloud | Position Delete | Deletion Vector |
|-------|-----------------|-----------------|
| AWS   | 2089ms          | 424ms           |
| Azure | 2241ms          | 285ms           |
| GCP   | 1787ms          | 430ms           |

### Remapping Algorithm Performance

The remapping implementation uses automatic algorithm selection based on workload characteristics:

| Strategy | Complexity | Best For |
|----------|------------|----------|
| Interval Tree | O(n log m) | Unsorted data (most scenarios) |
| Stream Join | O(n + m) | Sorted, dense, many runs (m ≥ 100) |
| Range Query | O(m log n) | Sorted, sparse or few runs |

**Measured speedup vs linear baseline (Feb 2026 hyperparallel JMH run, 72 configurations):**

| Scale | Best strategy | Speedup vs Linear |
|-------|---------------|-------------------|
| n=1K, m=100 (sorted, dense) | StreamJoin / IntervalTree | 1.6–6.9× |
| n=10K, m=1K  (sorted, dense) | StreamJoin | 60× |
| n=100K, m=10 (sorted, dense) | RangeQuery | 6.5× |
| n=100K, m=1K (sorted, dense) | IntervalTree | 68× |
| n=1M, m=10K  (sorted, dense) | IntervalTree | 832× |

The smart selector automatically chooses an optimal strategy with ~5% average overhead. When the caller has structural knowledge that positions are sorted (e.g., positions extracted from a Deletion Vector bitmap), `RemappingAlgorithmSelector.selectOptimal(mapping, positions, Boolean.TRUE)` skips the sortedness sample entirely.

### Bitmap Construction Performance

When materializing remapped positions into a `RoaringPositionBitmap` (the engine type behind V3 Deletion Vectors and `BitmapPositionDeleteIndex`), construction now uses bulk APIs that group positions by the bitmap's upper-32-bit key and call `RoaringBitmap.addN` per sub-bitmap, avoiding the per-call routing and container-locating cost that dominated the previous per-position `set(long)` loop.

**Measured speedup of `RoaringPositionBitmap.setAll(long[])` vs `set(long)` per value (DVRemappingPhaseBenchmark, Feb 2026):**

| numDeletes | `set(long)` per value | `setAll(long[])` | speedup |
|-----------:|----------------------:|-----------------:|--------:|
|       1,000 |   5.93 µs |  3.29 µs | 1.80× |
|      10,000 |  64.2 µs  | 41.8 µs  | 1.54× |
|     100,000 |   648 µs  |  446 µs  | 1.45× |
|   1,000,000 | 6447 µs   | 4624 µs  | 1.39× |

This speedup propagates to:

- **V2 position-delete reads.** `Deletes.toPositionIndexes` and `toPositionIndex` accumulate per-data-file positions into a small primitive buffer and flush via `PositionDeleteIndex.delete(long[])`, which dispatches to the bulk path.
- **V3 deletion-vector remapping output.** `PositionDeleteRemapper.remapPositionsBulkDV(String, int[])` keeps the output side in `int[]`, avoiding the long→int narrowing pass callers used to do.

## References

- **[Implementation Details](compaction_maps_impl.md)** - Architecture, API reference, and Spark internals
- **[Implementation Errata](compaction_maps_errata.md)** - Known limitations and design decisions
- **[Benchmarking Guide](compaction_maps_bench.md)** - JMH and microbenchmark suites
- [Iceberg Position Deletes Specification](https://iceberg.apache.org/spec/#position-delete-files)
- [Iceberg Manifest Format](https://iceberg.apache.org/spec/#manifests)

## Future Work

1. **Application Transaction Conflict Resolution** - Automatic remapping in BaseRowDelta for application-level position delete conflicts
2. **Other Engine Integration** - Extend position tracking to Flink, Trino, etc.
