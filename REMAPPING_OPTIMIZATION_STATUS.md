# Remapping Optimization Status

**Date:** February 3, 2026
**Branch:** `cmpmap`
**Last Commit:** `0577b6eea` (cloud-runner refactor)

## Summary

Position delete remapping optimization is **complete and validated**. Deletion Vector (DV) remapping optimization is now **implemented** (pending benchmark validation).

## DV Optimization (Implemented Feb 3, 2026)

Optimizations added to both the **production API** and **benchmark** to avoid boxing overhead:

### Core Changes

1. **`DVPositionReader.readDeletedPositionsPrimitive(DeleteFile)`** - Returns `long[]` instead of `CloseableIterable<Long>`
   - Uses `PositionDeleteIndex.cardinality()` to pre-allocate array
   - Fills array directly via `forEach(pos -> positions[idx[0]++] = pos)`
   - **Location:** `core/src/main/java/org/apache/iceberg/deletes/DVPositionReader.java`

2. **`PositionDeleteRemapper.remapPositionsBulkPrimitive(String, long[])`** - Returns `Map<String, long[]>`
   - Avoids Set wrapper overhead
   - Returns sorted arrays ready for RoaringBitmap construction
   - **Location:** `core/src/main/java/org/apache/iceberg/PositionDeleteRemapper.java`

3. **`PositionDeleteRemapper.remapDVBulk(DeleteFile, FileIO)`** - Updated to use primitive APIs
   - Now calls `readDeletedPositionsPrimitive()` instead of boxed `readDeletedPositions()`
   - Internally uses `remapPositionsBulkPrimitive()` for efficient remapping
   - Wraps result in `SortedLongArraySet` for backward-compatible API
   - **This is the production code path used by Spark conflict resolution**

### Benchmark Changes

- `RemappingBenchmarkRunner.remapDeletionVectors()` - Uses primitive APIs and `RoaringBitmap` aggregation
  - Extracts positions as `long[]` directly from RoaringBitmap
  - Calls `remapPositionsBulkPrimitive()`
  - Aggregates into `RoaringBitmap` instead of `HashSet`

### Expected Impact

Based on the boxing overhead analysis:
- Old path: 1M positions → 1M Long objects (16 bytes each) + HashSet hashing
- New path: 1M positions → `long[]` (8 bytes each, contiguous) + direct RoaringBitmap add

**Expected remap% reduction:** From 45-61% to ~5-10% (matching Position Delete performance)

## Completed Work

### 1. Bulk Remapping API (`PositionDeleteRemapper.java`)

Added new public method that accepts raw positions without requiring `DeleteFile` objects:

```java
public Map<String, Set<Long>> remapPositionsBulk(String sourceFile, Iterable<Long> positions)
```

**Key features:**
- Uses `RemappingAlgorithmSelector` to choose optimal strategy (IntervalTree, StreamJoin, RangeQuery)
- Returns `SortedLongArraySet` instead of `HashSet` (O(1) construction vs O(n))
- Handles non-compacted files by returning original positions

**Location:** `core/src/main/java/org/apache/iceberg/PositionDeleteRemapper.java:97-130`

### 2. SortedLongArraySet

Custom `Set<Long>` implementation optimized for remapping results:

```java
public class SortedLongArraySet extends AbstractSet<Long> {
    private final long[] sortedPositions;

    // O(1) construction - just stores reference
    public SortedLongArraySet(long[] sortedPositions) {
        this.sortedPositions = sortedPositions;
    }

    // O(log n) lookup via binary search
    public boolean contains(Object o) {
        return Arrays.binarySearch(sortedPositions, (Long) o) >= 0;
    }
}
```

**Location:** `core/src/main/java/org/apache/iceberg/SortedLongArraySet.java`

### 3. Benchmark Integration

Updated `RemappingBenchmarkRunner` to use bulk API:

```java
// OLD - slow path
for (int pos : sourceBitmap) {
    PositionDelete<Record> delete = PositionDelete.create();
    delete.set(sourceFile, pos, null);
    remapper.remapDeleteOrNull(delete);
}

// NEW - bulk path
List<Long> positions = new ArrayList<>(sourceBitmap.getCardinality());
for (int pos : sourceBitmap) {
    positions.add((long) pos);
}
Map<String, Set<Long>> remapped = remapper.remapPositionsBulk(sourceFile, positions);
```

**Location:** `benchmark/remapping-microbenchmark/src/main/java/org/apache/iceberg/benchmark/remapping/RemappingBenchmarkRunner.java`

### 4. Test Coverage

Added 5 new tests in `TestBulkRemappingIntegration.java`:
- `testRemapPositionsBulkBasic`
- `testRemapPositionsBulkNonCompacted`
- `testRemapPositionsBulkWithGaps`
- `testRemapPositionsBulkEmpty`
- `testRemapPositionsBulkLargeScale` (100K positions, <1 second)

**Location:** `core/src/test/java/org/apache/iceberg/TestBulkRemappingIntegration.java:194-312`

## Benchmark Results (February 3, 2026)

All three clouds (AWS us-west-2, GCP us-west1, Azure westus2) with co-located storage.

### Position Delete Files (uses bulk API)

| Scale | AWS remap% | GCP remap% | Azure remap% |
|-------|------------|------------|--------------|
| 1K deletes | 0.02-0.15% | 0.01-0.13% | 0.02-0.26% |
| 10K deletes | 0.15-0.46% | 0.16-0.39% | 0.13-0.73% |
| 100K deletes | 0.91-1.98% | 0.82-1.98% | 1.01-2.52% |
| 1M deletes | 1.58-5.78% | 1.74-3.59% | 1.76-3.96% |
| **Average** | **1.34%** | **1.21%** | **1.42%** |

### Deletion Vectors (per-position API - NOT optimized)

| Scale | AWS remap% | GCP remap% | Azure remap% |
|-------|------------|------------|--------------|
| 1K deletes | 0.3-2.8% | 0.3-7.8% | 0.4-4.9% |
| 10K deletes | 1.5-4.4% | 1.2-3.4% | 2.7-7.8% |
| 100K deletes | 11-32% | 12-37% | 20-53% |
| 1M deletes | 49-63% | 58-73% | 65-78% |
| **Average** | **19.19%** | **22.29%** | **26.96%** |

**Results location:** `/workspace/benchmark/remapping-microbenchmark/results/cloud_comparison_20260203/`

## Remaining Work: DV Optimization

### Problem

The `remapDVBulk()` method in `PositionDeleteRemapper.java` still uses inefficient per-position processing:

```java
// Current implementation (simplified) - PositionDeleteRemapper.java:132-180
public Map<String, Set<Long>> remapDVBulk(DeleteFile dv, FileIO io) {
    PositionDeleteIndex deleteIndex = Deletes.toPositionIndex(dv.location(), io);

    // Problem 1: Iterates positions one at a time
    for (long pos = 0; pos < maxPosition; pos++) {
        if (deleteIndex.isDeleted(pos)) {
            // Problem 2: Calls single-position remap for each
            Long remapped = remapPosition(sourceFile, pos);
            if (remapped != null) {
                // Problem 3: Builds HashSet incrementally
                results.computeIfAbsent(targetFile, k -> new HashSet<>()).add(remapped);
            }
        }
    }
    return results;
}
```

### Three Issues to Fix

#### Issue 1: Reading positions one at a time

**Current:** Iterates through all possible positions and checks `isDeleted(pos)` for each.

**Problem:** For sparse DVs, this is wasteful. For dense DVs with 1M deletes, we're doing 1M `isDeleted()` calls.

**Fix:** Extract all positions from the DV in bulk. The `PositionDeleteIndex` interface needs a method to iterate deleted positions:

```java
// Option A: Add to PositionDeleteIndex interface
public interface PositionDeleteIndex {
    boolean isDeleted(long position);

    // NEW: Iterate all deleted positions efficiently
    LongIterator deletedPositions();  // or PrimitiveIterator.OfLong
}

// Option B: Use RoaringBitmap directly if available
// DVs are stored as RoaringBitmaps internally - expose iteration
RoaringBitmap bitmap = ((RoaringPositionDeleteIndex) deleteIndex).bitmap();
for (int pos : bitmap) {
    positions.add((long) pos);
}
```

**Files to modify:**
- `api/src/main/java/org/apache/iceberg/deletes/PositionDeleteIndex.java` - Add iteration method
- `core/src/main/java/org/apache/iceberg/deletes/RoaringPositionDeleteIndex.java` - Implement iteration
- `core/src/main/java/org/apache/iceberg/PositionDeleteRemapper.java` - Use bulk extraction

#### Issue 2: Not using bulk remapping API

**Current:** Calls `remapPosition()` for each position individually.

**Fix:** Collect all positions first, then call `remapPositionsBulk()`:

```java
// Collect all deleted positions
List<Long> positions = new ArrayList<>();
for (long pos : deleteIndex.deletedPositions()) {  // New method from Issue 1
    positions.add(pos);
}

// Remap in bulk using optimized algorithm
return remapPositionsBulk(sourceFile, positions);
```

#### Issue 3: Building HashSets incrementally

**Current:** Uses `computeIfAbsent(k -> new HashSet<>()).add(pos)` which:
- Creates HashSet objects (allocation overhead)
- Hashes each position on add (O(1) but with overhead)
- Rehashes when capacity exceeded

**Fix:** Already solved by Issue 2 - `remapPositionsBulk()` returns `SortedLongArraySet` which has O(1) construction.

### Proposed Implementation

```java
// PositionDeleteRemapper.java - updated remapDVBulk()
public Map<String, Set<Long>> remapDVBulk(DeleteFile dv, FileIO io) {
    String sourceFile = dv.referencedDataFile();
    FileMapping mapping = getMapping(sourceFile);

    if (mapping == null) {
        // File wasn't compacted - return original positions
        return extractPositionsAsMap(dv, sourceFile, io);
    }

    // Extract all positions from DV efficiently
    List<Long> positions = extractPositions(dv, io);

    if (positions.isEmpty()) {
        return Collections.emptyMap();
    }

    // Use bulk remapping API (handles algorithm selection, returns SortedLongArraySet)
    return remapPositionsBulk(sourceFile, positions);
}

private List<Long> extractPositions(DeleteFile dv, FileIO io) {
    PositionDeleteIndex deleteIndex = Deletes.toPositionIndex(dv.location(), io);

    // Option A: If PositionDeleteIndex supports iteration
    List<Long> positions = new ArrayList<>();
    deleteIndex.forEachDeletedPosition(positions::add);
    return positions;

    // Option B: If we can access RoaringBitmap directly
    // RoaringBitmap bitmap = ((RoaringPositionDeleteIndex) deleteIndex).bitmap();
    // return bitmap.stream().mapToLong(i -> i).boxed().collect(toList());
}
```

### Expected Impact

Based on Position Delete results, DV remap% should drop from **19-27% average to ~1.3% average** after optimization.

At 1M deletes scale:
- Current: 49-78% remap time
- Expected: 1.6-6% remap time (matching Position Delete Files)

### Files to Modify

1. **`api/src/main/java/org/apache/iceberg/deletes/PositionDeleteIndex.java`**
   - Add `forEachDeletedPosition(LongConsumer consumer)` or similar

2. **`core/src/main/java/org/apache/iceberg/deletes/BitmapPositionDeleteIndex.java`**
   - Implement efficient iteration over RoaringBitmap

3. **`core/src/main/java/org/apache/iceberg/PositionDeleteRemapper.java`**
   - Update `remapDVBulk()` to use bulk extraction and `remapPositionsBulk()`

4. **`core/src/test/java/org/apache/iceberg/TestBulkRemappingIntegration.java`**
   - Add tests for optimized DV remapping

### Testing

Run existing tests:
```bash
./gradlew :iceberg-core:test --tests "*Remapping*"
./gradlew :iceberg-core:test --tests "*BulkRemapping*"
```

Run benchmarks:
```bash
cd benchmark/cloud-runner/aws
source setup.conf
./run.sh all --config ../remapping-microbenchmark/configs/quick.yaml
```

## Cloud Runner Status

Infrastructure setup/teardown scripts added:
- `setup.sh` - One-time infrastructure creation (idempotent)
- `run.sh` - Per-run VM lifecycle (loads setup.conf automatically)
- `teardown.sh` - Remove all infrastructure

**Commit:** `0577b6eea` - refactor(cloud-runner): Separate infrastructure setup from VM lifecycle

## Related Commits

- `9a0647c66` - feat(benchmark): Integrate bulk remapping optimization for position deletes
- `0577b6eea` - refactor(cloud-runner): Separate infrastructure setup from VM lifecycle

## Next Steps

1. Add `forEachDeletedPosition()` to `PositionDeleteIndex` interface
2. Implement in `BitmapPositionDeleteIndex` using RoaringBitmap iteration
3. Update `remapDVBulk()` to extract positions in bulk
4. Update `remapDVBulk()` to call `remapPositionsBulk()` instead of per-position remapping
5. Run benchmarks to validate DV remap% drops to ~1-2%
