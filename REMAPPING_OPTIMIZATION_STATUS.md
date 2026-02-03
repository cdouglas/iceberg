# Remapping Optimization Status

**Date:** February 3, 2026
**Branch:** `cmpmap`
**Last Commit:** `0868ec759` (Add PositionDeleteIndex overload and document shared API)

## Summary

Both Position Delete and Deletion Vector (DV) remapping optimizations are **complete**. Both paths now use the same shared primitive API (`remapPositionsBulkPrimitive`), ensuring benchmark measurements accurately reflect production performance.

## DV Optimization (Completed Feb 3, 2026)

### Problem Solved

DV remapping was 15-40x slower than Position Deletes due to boxing overhead:
- **Before:** 45-61% remap time at 1M deletes scale
- **After:** Expected ~5-10% (pending benchmark validation)

### Architecture

Both production and benchmark now share the same code path:

```
Production (remapDVBulk):              Benchmark:
┌─────────────────────────┐           ┌─────────────────────────┐
│     DeleteFile          │           │     RoaringBitmap       │
└───────────┬─────────────┘           └───────────┬─────────────┘
            │                                     │
            ▼                                     ▼
┌─────────────────────────┐           ┌─────────────────────────┐
│ DVPositionReader        │           │ Direct iteration        │
│ .readDeletedPositions   │           │ for (int pos : bitmap)  │
│ Primitive()             │           │                         │
└───────────┬─────────────┘           └───────────┬─────────────┘
            │                                     │
            ▼                                     ▼
            └─────────────┬───────────────────────┘
                          │
                          ▼
          ┌───────────────────────────────┐
          │ remapPositionsBulkPrimitive   │  ← SHARED API
          │ (String sourceFile, long[])   │
          └───────────────┬───────────────┘
                          │
                          ▼
          ┌───────────────────────────────┐
          │ RemappingAlgorithmSelector    │
          │ + Strategy execution          │
          └───────────────────────────────┘
```

### Core Changes

1. **`DVPositionReader.readDeletedPositionsPrimitive(DeleteFile)`** - Returns `long[]`
   - Uses `PositionDeleteIndex.cardinality()` to pre-allocate array
   - Fills array directly via `forEach(pos -> positions[idx[0]++] = pos)` (no boxing)
   - **Location:** `core/src/main/java/org/apache/iceberg/deletes/DVPositionReader.java`

2. **`PositionDeleteRemapper.remapPositionsBulkPrimitive(String, long[])`** - Returns `Map<String, long[]>`
   - Avoids Set wrapper overhead
   - Returns sorted arrays ready for RoaringBitmap construction
   - **Location:** `core/src/main/java/org/apache/iceberg/PositionDeleteRemapper.java`

3. **`PositionDeleteRemapper.remapPositionsBulkPrimitive(String, PositionDeleteIndex)`** - Overload
   - Accepts PositionDeleteIndex for callers who have that interface
   - Extracts positions and delegates to the `long[]` version
   - **Location:** `core/src/main/java/org/apache/iceberg/PositionDeleteRemapper.java`

4. **`PositionDeleteRemapper.remapDVBulk(DeleteFile, FileIO)`** - Updated
   - Now calls `readDeletedPositionsPrimitive()` instead of boxed `readDeletedPositions()`
   - Internally uses `remapPositionsBulkPrimitive()` for efficient remapping
   - Wraps result in `SortedLongArraySet` for backward-compatible API
   - **This is the production code path used by Spark conflict resolution**

### Benchmark Changes

- `RemappingBenchmarkRunner.remapDeletionVectors()` - Uses same API as production
  - Extracts positions as `long[]` directly from RoaringBitmap
  - Calls `remapPositionsBulkPrimitive()`
  - Aggregates into `RoaringBitmap` instead of `HashSet`

### Test Coverage

Added `TestCoreApiCoupling.java` to ensure benchmark stays coupled with core:
- `testBenchmarkPositionExtractionCompatibleWithCoreApi` - Verifies position extraction works
- `testPrimitiveAndBoxedApisProduceSameResults` - Ensures API consistency
- `testBenchmarkAggregationPreservesPositions` - Verifies aggregation correctness
- `testNonCompactedFileHandling` - Edge case for non-compacted files
- `testEmptyPositionsHandling` - Edge case for empty input
- `testGapPositionsFiltered` - Verifies gap handling in merge compaction
- `testLargePositionCount` - Tests for overflow issues
- `testApiSignatureStability` - Compile-time API contract verification

**Location:** `benchmark/remapping-microbenchmark/src/test/java/org/apache/iceberg/benchmark/remapping/TestCoreApiCoupling.java`

### Expected Impact

Based on the boxing overhead analysis:
- Old path: 1M positions → 1M Long objects (16 bytes each) + HashSet hashing
- New path: 1M positions → `long[]` (8 bytes each, contiguous) + direct RoaringBitmap add

**Expected remap% reduction:** From 45-61% to ~5-10% (matching Position Delete performance)

## Previous Work: Position Delete Optimization

### Bulk Remapping API

Added `remapPositionsBulk(String sourceFile, Iterable<Long> positions)`:
- Uses `RemappingAlgorithmSelector` to choose optimal strategy
- Returns `SortedLongArraySet` instead of `HashSet` (O(1) construction vs O(n))

### SortedLongArraySet

Custom `Set<Long>` implementation optimized for remapping results:
- O(1) construction (just stores array reference)
- O(log n) lookup via binary search

**Location:** `core/src/main/java/org/apache/iceberg/SortedLongArraySet.java`

## Benchmark Results (February 3, 2026) - Pre-DV-Optimization

All three clouds (AWS us-west-2, GCP us-west1, Azure westus2) with co-located storage.

### Position Delete Files (optimized)

| Scale | AWS remap% | GCP remap% | Azure remap% |
|-------|------------|------------|--------------|
| 1K deletes | 0.02-0.15% | 0.01-0.13% | 0.02-0.26% |
| 10K deletes | 0.15-0.46% | 0.16-0.39% | 0.13-0.73% |
| 100K deletes | 0.91-1.98% | 0.82-1.98% | 1.01-2.52% |
| 1M deletes | 1.58-5.78% | 1.74-3.59% | 1.76-3.96% |
| **Average** | **1.34%** | **1.21%** | **1.42%** |

### Deletion Vectors (pre-optimization baseline)

| Scale | AWS remap% | GCP remap% | Azure remap% |
|-------|------------|------------|--------------|
| 1K deletes | 0.3-2.8% | 0.3-7.8% | 0.4-4.9% |
| 10K deletes | 1.5-4.4% | 1.2-3.4% | 2.7-7.8% |
| 100K deletes | 11-32% | 12-37% | 20-53% |
| 1M deletes | 49-63% | 58-73% | 65-78% |
| **Average** | **19.19%** | **22.29%** | **26.96%** |

**Results location:** `/workspace/benchmark/remapping-microbenchmark/results/cloud_comparison_20260203/`

## Related Commits

- `6c4c0df1b` - feat(remapping): Add primitive APIs for DV remapping to avoid boxing overhead
- `0868ec759` - feat(remapping): Add PositionDeleteIndex overload and document shared API
- `9a0647c66` - feat(benchmark): Integrate bulk remapping optimization for position deletes
- `0577b6eea` - refactor(cloud-runner): Separate infrastructure setup from VM lifecycle

## Next Steps

1. **Run cloud benchmarks** to validate DV optimization produces expected ~5-10% remap time
2. Compare optimized DV results against Position Delete baseline

## Testing

Run tests:
```bash
# Core remapping tests
./gradlew :iceberg-core:test --tests "*Remapping*"

# Benchmark coupling tests
./gradlew :benchmark:remapping-microbenchmark:test --tests "TestCoreApiCoupling"

# All benchmark tests
./gradlew :benchmark:remapping-microbenchmark:test
```

Run benchmarks:
```bash
cd benchmark/cloud-runner/aws
source setup.conf
./run.sh all --config ../remapping-microbenchmark/configs/quick.yaml
```
