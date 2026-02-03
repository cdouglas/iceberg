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

### Measured Impact (February 3, 2026)

AWS benchmark results comparing pre-optimization baseline to optimized DV remapping:

| Scale | DV Pre-Optimization | DV Optimized (Phase 1) | Position Delete (Reference) |
|-------|---------------------|------------------------|----------------------------|
| 1K deletes | 0.3-7.8% | **0.61-0.72%** | 0.07-0.08% |
| 10K deletes | 1.2-7.8% | **0.66-0.94%** | 0.32-0.39% |
| 100K deletes | 11-53% | **4.86-4.88%** | 1.58-1.64% |
| 1M deletes | 45-78% | **29.5-32.7%** | 3.73-4.99% |

**Phase 1 results** (before primitive API refactoring):
- **1K-10K scale**: DV remap% now comparable to Position Deletes (under 1%)
- **100K scale**: Major improvement from 11-53% down to ~5% (55-91% reduction)
- **1M scale**: Improved from 45-78% to 29-33% (40-57% reduction), still higher than PD

### Phase 2: Primitive API Refactoring (February 3, 2026)

**Problem**: Phase 1 still had boxing overhead at 1M scale because `RemappingStrategy.runForPositions()` used `List<Long>`.

**Solution**: Refactored the entire remapping pipeline to use primitive `long[]` arrays:

1. **`RemappingStrategy.runForPositions(long[])`** - New primitive bulk lookup method
2. **`StreamJoinStrategy.streamJoinPrimitive(long[])`** - Primitive stream join algorithm
3. **`RangeQueryStrategy.rangeQueryPrimitive(long[])`** - Primitive range query algorithm
4. **`RemappingAlgorithmSelector.selectOptimal(FileMapping, long[])`** - Primitive strategy selection
5. **`PositionDeleteRemapper.remapPositionsBulkPrimitive()`** - Now fully primitive, no boxing

**Data flow (after refactoring)**:
```
long[] positions -> selector.selectOptimal(mapping, positions) -> strategy.runForPositions(positions) -> Map<Long, Run>
```

Boxing only occurs in the output `Map<Long, Run>`, which is typically much smaller than input (gaps filtered).

**Expected impact at 1M scale**: DV remap% should drop from 29-33% to ~5-10%, matching Position Delete performance.

**Results location:** `/workspace/benchmark/remapping-microbenchmark/results/aws_20260203_183208/`

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

## Benchmark Results (Pre-DV-Optimization Baseline)

Multi-cloud results (AWS us-west-2, GCP us-west1, Azure westus2) with co-located storage.

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

- `6b87de4dc` - perf(remapping): Eliminate boxing overhead with primitive long[] APIs
- `6c4c0df1b` - feat(remapping): Add primitive APIs for DV remapping to avoid boxing overhead
- `0868ec759` - feat(remapping): Add PositionDeleteIndex overload and document shared API
- `9a0647c66` - feat(benchmark): Integrate bulk remapping optimization for position deletes
- `0577b6eea` - refactor(cloud-runner): Separate infrastructure setup from VM lifecycle

## Next Steps

1. ✅ **Cloud benchmarks validated** - DV optimization reduces remap% significantly at all scales
2. ✅ **Primitive API refactoring complete** - `RemappingStrategy` interface and all implementations now support primitive `long[]` arrays
3. **Pending**: Run cloud benchmarks to validate primitive API improvement at 1M scale (expected: 29-33% → ~5-10%)

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
export AWS_SSH_KEY_FILE="$HOME/.ssh/iceberg-benchmark-aws.pem"
./run.sh all --config /workspace/benchmark/remapping-microbenchmark/src/main/resources/configs/quick_dv_test.yaml
```
