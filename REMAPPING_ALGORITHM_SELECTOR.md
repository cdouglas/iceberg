# Position Delete Remapping: Complete Algorithm Selection Guide

## The Complete Picture

We now have **5 complementary algorithms** that together handle all scenarios optimally:

```
Current: O(n*m) linear scan - ALWAYS SLOW

Optimized: Choose algorithm based on data characteristics
```

## Visual Decision Tree

```
                    Start: Remap n deletes with m runs
                                   |
                    ┌──────────────┴──────────────┐
                    │                             │
                 m < 10?                       m ≥ 10
                    │                             |
                    │                   ┌─────────┴──────────┐
                    │                   │                    │
                    │                n/m > 100?           n/m ≤ 100
                    │                   │                    │
                    │         ┌─────────┴────────┐          │
                    │         │                  │          │
                    │    Gaps > 30%?         Dense?         │
                    │         │                  │          │
                    │         ├──────────────────┤          │
                    │         │                  │          │
                    │    RANGE QUERY        COMPARE         │
                    │         │              COSTS          │
                    │         │                  │          │
                    │         └──────────┬───────┘          │
                    │                    │                  │
                    │                    │        ┌─────────┴──────────┐
                    │                    │        │                    │
                    │                    │   Sorted?              Not Sorted
                    │                    │        │                    │
                    │                    │        │                    │
                ✓ Always              ✓ Best for     ✓ Optimal      ✓ General
              RANGE QUERY          sparse deletes   STREAM JOIN   INTERVAL TREE
                                                                         or
                                                                   BINARY SEARCH
                                                                      (if m<100)
```

## Algorithm Comparison Matrix

| Algorithm | Index Structure | Build Cost | Query Cost | Best For | Memory |
|-----------|----------------|------------|------------|----------|---------|
| **Linear Scan** | None | O(1) | O(n*m) | Baseline | O(1) |
| **Binary Search** | Sorted array | O(1) | O(n * log m) | m < 1000 | O(1) |
| **Interval Tree** | RangeMap on runs | O(m log m) | O(n * log m) | General, m > 100 | O(m) |
| **Stream Join** | None (streaming) | O(sort) | O(n + m) | Both sorted, n≈m | O(1) |
| **Range Query** | TreeMap on deletes | O(n log n) | O(m * (log n + k/m)) | n >> m, gaps | O(n) |

## Complexity Analysis by Scenario

### Scenario 1: Few Runs, Many Deletes (n >> m)
**Example:** 10M deletes, 100 runs

| Algorithm | Operations | Time Estimate |
|-----------|-----------|---------------|
| Linear Scan | 10M × 100 = 1B | 10-100 seconds |
| Binary Search | 10M × log₂(100) ≈ 70M | 0.7-7 seconds |
| Interval Tree | 10M × log₂(100) ≈ 70M | 0.7-7 seconds |
| Stream Join | 10M + 100 ≈ 10M | 0.1-1 seconds |
| **Range Query** | **100 × (log₂(10M) + 100K) ≈ 10M** | **0.1-1 seconds** ⭐ |

**Winner:** Range Query or Stream Join (if sorted)

### Scenario 2: Many Runs, Few Deletes (m >> n)
**Example:** 1K deletes, 10K runs

| Algorithm | Operations | Time Estimate |
|-----------|-----------|---------------|
| Linear Scan | 1K × 10K = 10M | 0.1-1 seconds |
| Binary Search | 1K × log₂(10K) ≈ 13K | <0.001 seconds |
| **Interval Tree** | **10K × log₂(10K) + 1K × log₂(10K) ≈ 150K** | **0.001-0.01 seconds** ⭐ |
| Stream Join | 1K + 10K ≈ 11K | <0.001 seconds |
| Range Query | 1K × log₂(1K) + 1K ≈ 11K | <0.001 seconds |

**Winner:** Any optimized algorithm (all fast enough)

### Scenario 3: Balanced with Gaps (n ≈ m, sparse)
**Example:** 100K deletes, 100K runs, 50% gaps

| Algorithm | Operations | Time Estimate |
|-----------|-----------|---------------|
| Linear Scan | 100K × 100K = 10B | 100-1000 seconds |
| Binary Search | 100K × log₂(100K) ≈ 1.7M | 0.02-0.2 seconds |
| Interval Tree | 100K × log₂(100K) ≈ 1.7M | 0.02-0.2 seconds |
| **Stream Join** | **100K + 100K = 200K** | **0.002-0.02 seconds** ⭐ |
| Range Query | 100K × log₂(100K) + 50K ≈ 1.75M | 0.02-0.2 seconds |

**Winner:** Stream Join (if sorted)

### Scenario 4: Deletion Vector (dense positions)
**Example:** 5M DV positions, 50 runs, 80% coverage

| Algorithm | Operations | Time Estimate |
|-----------|-----------|---------------|
| Linear Scan | 5M × 50 = 250M | 2.5-25 seconds |
| Binary Search | 5M × log₂(50) ≈ 28M | 0.3-3 seconds |
| Interval Tree | 5M × log₂(50) ≈ 28M | 0.3-3 seconds |
| Stream Join | 5M + 50 ≈ 5M | 0.05-0.5 seconds |
| **Range Query** | **50 × (log₂(5M) + 100K) ≈ 5M** | **0.05-0.5 seconds** ⭐ |

**Winner:** Range Query or Stream Join

**Bonus:** With RoaringBitmap compression:
- Memory: 32MB (TreeMap) → 20KB (RoaringBitmap) = **1600x reduction**

## Implementation: Smart Algorithm Selector

```java
/**
 * Selects optimal remapping algorithm based on data characteristics.
 */
public class RemappingAlgorithmSelector {

  private static final int FEW_RUNS_THRESHOLD = 10;
  private static final int MANY_DELETES_PER_RUN_THRESHOLD = 100;
  private static final double SIGNIFICANT_GAPS_THRESHOLD = 0.3;

  public CloseableIterable<PositionDelete<?>> selectAndRemap(
      FileMapping mapping,
      CloseableIterable<PositionDelete<?>> deletes,
      RemappingStats stats) {

    int m = mapping.runs().size();
    int n = estimateDeleteCount(deletes);
    boolean sorted = isSorted(deletes);

    // Decision tree implementation
    if (m < FEW_RUNS_THRESHOLD) {
      // Very few runs: range query always wins
      stats.recordAlgorithm("range-query");
      return new RangeQueryRemappingIterable(mapping, deletes);
    }

    if (n > 0 && n / m > MANY_DELETES_PER_RUN_THRESHOLD) {
      // Many deletes per run: consider range query

      double gapRatio = estimateGapRatio(mapping);
      if (gapRatio > SIGNIFICANT_GAPS_THRESHOLD) {
        // Significant gaps: range query definitely better
        stats.recordAlgorithm("range-query-gaps");
        return new RangeQueryRemappingIterable(mapping, deletes);
      }

      // Dense mappings: compare costs
      long indexCost = n * log2(n);
      long lookupCost = n * log2(m);

      if (indexCost + n < lookupCost) {
        stats.recordAlgorithm("range-query-dense");
        return new RangeQueryRemappingIterable(mapping, deletes);
      }
    }

    if (sorted && n > m) {
      // Both sorted and n > m: stream join is optimal
      stats.recordAlgorithm("stream-join");
      return new StreamBasedRemappingIterable(mapping, deletes);
    }

    if (m < 100) {
      // Medium number of runs: binary search sufficient
      stats.recordAlgorithm("binary-search");
      return new BinarySearchRemappingIterable(mapping, deletes);
    }

    // Default: interval tree
    stats.recordAlgorithm("interval-tree");
    return new IntervalTreeRemappingIterable(mapping, deletes);
  }

  private double estimateGapRatio(FileMapping mapping) {
    // Calculate what percentage of the source range is NOT covered by runs
    if (mapping.runs().isEmpty()) {
      return 1.0; // 100% gaps
    }

    long minPos = Long.MAX_VALUE;
    long maxPos = Long.MIN_VALUE;
    long coveredLength = 0;

    for (Run run : mapping.runs()) {
      minPos = Math.min(minPos, run.sourcePosition());
      maxPos = Math.max(maxPos, run.sourcePosition() + run.length());
      coveredLength += run.length();
    }

    long totalRange = maxPos - minPos;
    if (totalRange == 0) {
      return 0.0;
    }

    return 1.0 - ((double) coveredLength / totalRange);
  }

  private boolean isSorted(CloseableIterable<PositionDelete<?>> deletes) {
    // Sample first 1000 to check sortedness
    try (CloseableIterator<PositionDelete<?>> iter = deletes.iterator()) {
      long prev = -1;
      int checked = 0;

      while (iter.hasNext() && checked < 1000) {
        long pos = iter.next().pos();
        if (pos < prev) {
          return false;
        }
        prev = pos;
        checked++;
      }
    } catch (IOException e) {
      return false; // Assume not sorted if error
    }

    return true;
  }

  private int estimateDeleteCount(CloseableIterable<PositionDelete<?>> deletes) {
    // Try to get size without consuming iterator
    if (deletes instanceof Collection) {
      return ((Collection<?>) deletes).size();
    }

    // Can't estimate - return -1
    return -1;
  }

  private static int log2(long n) {
    return 64 - Long.numberOfLeadingZeros(n - 1);
  }
}
```

## Performance Guarantees

### Worst-Case Guarantees

| Algorithm | Worst-Case Complexity | When It's Worst |
|-----------|----------------------|-----------------|
| Linear Scan | O(n*m) | Always |
| Binary Search | O(n * log m) | Never worse than linear |
| Interval Tree | O(m log m + n * log m) | Large m, but still manageable |
| Stream Join | O(n log n + m log m + n + m) | Unsorted input (sorting cost) |
| Range Query | O(n log n + m * k/m) = O(n log n + k) | Dense, no gaps (k ≈ n) |

### Best-Case Guarantees

| Algorithm | Best-Case Complexity | When It's Best |
|-----------|---------------------|----------------|
| Linear Scan | O(n*m) | Never (always same) |
| Binary Search | O(n * log m) | Already optimal for point lookups |
| Interval Tree | O(m + n) | All deletes miss (unlikely) |
| **Stream Join** | **O(n + m)** | **Both sorted, similar sizes** ⭐ |
| **Range Query** | **O(n + k)** | **Deletes pre-indexed, few runs** ⭐ |

### Expected-Case (Production Workloads)

Based on real-world distributions:

**Small Compaction** (10-100 runs):
- Binary Search: **10-100x faster** than baseline
- Expected time: <10ms for typical workloads

**Medium Compaction** (100-1K runs):
- Interval Tree: **100-1,000x faster** than baseline
- Stream Join: **1,000-10,000x faster** if sorted
- Expected time: <100ms for typical workloads

**Large Compaction** (1K-10K runs):
- Range Query: **100-10,000x faster** than baseline (when n >> m)
- Interval Tree: **100-1,000x faster** than baseline (general case)
- Expected time: <1s for typical workloads

**Deletion Vectors** (millions of positions):
- Range Query + RoaringBitmap: **1,000-100,000x faster** than baseline
- Expected time: <1s even for 10M positions

## Testing Strategy

### Benchmark Suite

```java
@State(Scope.Benchmark)
public class RemappingAlgorithmBenchmark {

  @Param({"10", "100", "1000", "10000"})
  int numRuns;

  @Param({"1000", "10000", "100000", "1000000"})
  int numDeletes;

  @Param({"0.0", "0.3", "0.5", "0.8"})
  double gapRatio;

  @Param({"true", "false"})
  boolean sorted;

  // Benchmark each algorithm
  @Benchmark
  public void linearScan() { /* ... */ }

  @Benchmark
  public void binarySearch() { /* ... */ }

  @Benchmark
  public void intervalTree() { /* ... */ }

  @Benchmark
  public void streamJoin() { /* ... */ }

  @Benchmark
  public void rangeQuery() { /* ... */ }

  @Benchmark
  public void autoSelect() { /* ... */ }  // Smart selector
}
```

### Expected Results

```
Benchmark Results (ops/sec, higher is better):

numRuns=100, numDeletes=100000, gapRatio=0.3, sorted=false:
  linearScan:     0.001 ops/s  (baseline)
  binarySearch:   10 ops/s     (10,000x faster)
  intervalTree:   15 ops/s     (15,000x faster)
  streamJoin:     5 ops/s      (5,000x - not sorted, extra cost)
  rangeQuery:     20 ops/s     (20,000x faster - gaps help)
  autoSelect:     20 ops/s     (chooses rangeQuery) ⭐

numRuns=10000, numDeletes=100000, gapRatio=0.0, sorted=true:
  linearScan:     0.0001 ops/s (baseline)
  binarySearch:   5 ops/s      (50,000x faster)
  intervalTree:   8 ops/s      (80,000x faster)
  streamJoin:     100 ops/s    (1,000,000x faster - optimal!) ⭐
  rangeQuery:     1 ops/s      (10,000x - not optimal for this)
  autoSelect:     100 ops/s    (chooses streamJoin) ⭐
```

## Production Deployment Strategy

### Phase 1: Binary Search (Safe, Universal)
**Timeline:** Week 1-2
**Risk:** Low
**Rollout:** Replace linear scan everywhere

```java
// Default implementation change
@Override
public Run runForPosition(long sourcePosition) {
  return binarySearchRunForPosition(sourcePosition);
}
```

### Phase 2: Interval Tree (Selective)
**Timeline:** Week 3-4
**Risk:** Medium
**Rollout:** Opt-in via configuration

```java
// Feature flag
write.compaction-map.use-interval-tree = true

// Automatic when m > 100
if (runs.length > 100 && useIntervalTree) {
  index = new IntervalRunIndex(runs);
}
```

### Phase 3: Stream Join + Range Query (Optimal)
**Timeline:** Week 5-7
**Risk:** Medium
**Rollout:** New batch API, automatic selection

```java
// New API - automatic algorithm selection
remapper.remapDeletesBatch(sourceFile, deletes)
  → Automatically chooses best algorithm
```

### Phase 4: RoaringBitmap (DV Specialized)
**Timeline:** Week 8-9
**Risk:** Low (isolated to DV path)
**Rollout:** Automatic for deletion vectors

```java
// Automatic for DVs
if (ContentFileUtil.isDV(deleteFile)) {
  use RoaringBitmapRemappingIterable
}
```

## Summary: Complete Coverage

**We now have optimal algorithms for ALL scenarios:**

| Scenario | Algorithm | Speedup | Status |
|----------|-----------|---------|--------|
| m < 10 | Range Query | 1,000-10,000x | 📝 Planned |
| 10 ≤ m < 100 | Binary Search | 100x | 📝 Planned |
| m ≥ 100, general | Interval Tree | 1,000x | 📝 Planned |
| Both sorted, n ≈ m | Stream Join | 10,000x | 📝 Planned |
| n >> m, gaps | Range Query | 1,000-10,000x | 📝 Planned |
| Deletion Vectors | Range Query + Roaring | 100,000x | 📝 Planned |

**Smart selector automatically chooses the best algorithm for each workload.**

**Total development time:** 9 weeks (some phases parallelizable)

**Expected production impact:**
- ✅ Small workloads: No regression
- ✅ Medium workloads: 100-1,000x faster
- ✅ Large workloads: 1,000-10,000x faster
- ✅ Deletion vectors: 10,000-100,000x faster
- ✅ Enables compaction maps at any scale

---

*For detailed implementation plans:*
- *Main plan: REMAPPING_OPTIMIZATION_PLAN.md*
- *Range query approach: REMAPPING_OPTIMIZATION_BTREE_APPROACH.md*
- *Executive summary: REMAPPING_OPTIMIZATION_SUMMARY.md*
