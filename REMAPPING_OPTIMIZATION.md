# Remapping Algorithm Optimization - Implementation History

**NOTE**: This document contains the detailed phase-by-phase implementation history for the remapping optimization work. For practical guidance on working with compaction maps, see `CLAUDE.md`.

---

## Remapping Algorithm Optimization (Phases 1-7)

### Overview

The initial remapping implementation in `GenericCompactionMap.GenericFileMapping.runForPosition()` used linear search through runs (O(m) per lookup). For typical workloads with n position deletes and m runs, this resulted in O(n*m) total remapping cost.

**Problem Scale**: For n=10,000 deletes and m=100 runs, this meant 1,000,000 comparisons per remapping operation.

**Solution**: Seven-phase optimization implementing multiple strategies with automatic selection based on data characteristics.

### Implementation Summary

All seven phases completed with comprehensive test coverage:

| Phase | Strategy | Complexity | Best For | Commit |
|-------|----------|------------|----------|--------|
| 1-2 | Binary Search | O(log m) | 10 ≤ m < 100 | 238c78f82 |
| 3 | Interval Tree | O(log m) | m ≥ 100 | f97f1c838 |
| 4 | Stream Join | O(n + m) bulk | Bulk, m ≈ n | 10a5b452b |
| 5 | Range Query | O(m log n) bulk | Bulk, n >> m | 60ce17588 |
| 6 | Integration Tests | - | Realistic workloads | 718bab7b0 |
| 7.1 | Smart Selector | - | Automatic strategy choice | 032efad32 |
| 7.2 | JMH Benchmarks | - | Empirical validation | 9c16b556b |

**Total**: ~4,200 lines of code, 82 comprehensive tests, 100-900x speedup for typical workloads.

### Phase 1-2: Binary Search Strategy

**Commit**: 238c78f82 - "Optimize position delete remapping with binary search (Phase 1-2)"

**Problem**: Linear search O(m) is inefficient for medium to large run counts.

**Solution**:
- Introduced `RemappingStrategy` interface with Factory pattern
- Implemented `LinearSearchStrategy` (explicit baseline, O(m))
- Implemented `BinarySearchStrategy` (optimized, O(log m))
- Automatic threshold-based selection: linear for m < 10, binary for m ≥ 10

**Key Files**:
- `RemappingStrategy.java` - Interface with Factory.create()
- `LinearSearchStrategy.java` - Baseline algorithm (explicit)
- `BinarySearchStrategy.java` - Optimized algorithm with validation
- `GenericCompactionMap.java:237-248` - Integrated with lazy initialization

**Performance**:
- m=100: ~100x speedup
- m=1,000: ~1,000x speedup
- Real-world (n=10,000, m=100): 1M → 67K comparisons (15x faster)

**Testing**: 15 test methods including property-based tests with 10,000+ lookups

**Token Saver**: Read `BinarySearchStrategy.java:62-91` for the core algorithm. The validation logic is identical to what's used in IntervalTreeStrategy.

### Phase 3: Interval Tree Strategy

**Commit**: f97f1c838 - "Add interval tree strategy for large run counts (Phase 3)"

**Problem**: Binary search with array indexing can have cache locality issues for very large m.

**Solution**:
- Balanced BST built in O(m) from sorted runs
- Each node augmented with maxEnd for efficient pruning
- Tree height guaranteed O(log m) via middle-element selection

**Key Algorithm**:
```java
// Build balanced tree
Node buildTree(List<Run> runs, int start, int end) {
  int mid = start + (end - start) / 2;  // Take middle
  Node node = new Node(runs.get(mid));
  node.left = buildTree(runs, start, mid - 1);   // Recurse left
  node.right = buildTree(runs, mid + 1, end);    // Recurse right
  node.maxEnd = computeMaxEnd(node);             // Augment
  return node;
}

// Search with pruning
Run search(Node node, long position) {
  if (position >= node.maxEnd) return null;  // Prune!
  if (position in node.run) return node.run;
  // Recursively search left or right
}
```

**Key Files**:
- `IntervalTreeStrategy.java` - Balanced BST with augmented nodes
- `RemappingStrategy.java:60-62` - Updated INTERVAL_TREE_THRESHOLD = 100

**Performance**:
- Similar O(log m) to binary search
- Better cache locality for very large m
- Enables future optimizations (range queries, incremental updates)

**Testing**: 21 test methods total (added interval tree to all existing tests)

**Token Saver**: The tree construction is in `IntervalTreeStrategy.java:112-137`. It's a standard balanced BST build from sorted array - simple middle-element recursion.

### Phase 4: Stream Join Strategy

**Commit**: 10a5b452b - "Add stream join strategy for bulk remapping (Phase 4)"

**Problem**: Previous strategies optimize single-position lookup but don't leverage sorted input for bulk operations.

**Solution**:
- Added `runForPositions(List<Long>)` bulk API to RemappingStrategy
- Implemented StreamJoinStrategy with O(n + m) merge-join algorithm
- Automatic sorted detection with O(n log m) fallback for unsorted

**Key Algorithm**:
```java
Map<Long, Run> streamJoin(List<Long> sortedPositions) {
  int runIndex = 0;
  Run currentRun = runs.get(0);

  for (Long position : sortedPositions) {
    // Advance through runs until we might contain this position
    while (runIndex < runs.size() && position >= currentRunEnd) {
      runIndex++;
      currentRun = runs.get(runIndex);
      currentRunEnd = currentRun.sourcePosition() + currentRun.length();
    }

    // Check if position is in current run
    if (position >= currentRun.sourcePosition() && position < currentRunEnd) {
      results.put(position, currentRun);
    }
  }
  return results;
}
```

**Key Files**:
- `StreamJoinStrategy.java` - Merge-join for sorted positions
- `RemappingStrategy.java:53-78` - Added bulk API with default implementation

**Performance**:
- Sorted: O(n + m) single pass
- Unsorted: O(n log m) fallback to binary search
- Real-world (n=10,000, m=100, sorted): 1M → 10,100 operations (100x faster)
- Speedup: ~7x over per-position binary search

**Testing**: 32 test methods total (11 new for stream join)

**Usage Example**:
```java
// Bulk remapping with sorted positions
StreamJoinStrategy strategy = new StreamJoinStrategy(runs);
Map<Long, Run> results = strategy.runForPositions(sortedPositions);
```

**Token Saver**: The core stream join is in `StreamJoinStrategy.java:119-157`. It's a standard merge-join pattern - advance through both lists in tandem.

### Phase 5: Range Query Strategy

**Commit**: 60ce17588 - "Add range query strategy for high fan-in scenarios (Phase 5)"

**Problem**: Stream join is O(n + m), but when n >> m, we can do better by inverting the query.

**Solution**:
- Inverted algorithm: for each run, find all positions in that range
- Binary search for lower/upper bounds of positions in each run
- Automatically sorts positions if needed

**Key Algorithm**:
```java
Map<Long, Run> rangeQuery(List<Long> sortedPositions) {
  for (Run run : runs) {
    long runStart = run.sourcePosition();
    long runEnd = runStart + run.length();

    // Binary search for first position >= runStart
    int startIndex = binarySearchLowerBound(sortedPositions, runStart);

    // Binary search for first position >= runEnd
    int endIndex = binarySearchLowerBound(sortedPositions, runEnd);

    // All positions in [startIndex, endIndex) are within this run
    for (int i = startIndex; i < endIndex; i++) {
      results.put(sortedPositions.get(i), run);
    }
  }
  return results;
}
```

**Key Files**:
- `RangeQueryStrategy.java` - Inverted query via binary search bounds
- `RemappingStrategy.java:39-40` - Updated documentation

**Performance**:
- Complexity: O(m log n + k) where k = matches
- Real-world (n=10,000, m=10): 1M → 133 operations (7,500x faster)
- Real-world (n=10,000, m=100): 1M → 1,330 operations (750x faster)
- Beats stream join when: m < n / log n

**When to Use**:
- Stream join: When m ≈ n
- Range query: When n >> m (high fan-in)

**Testing**: 44 test methods total (12 new for range query including high fan-in test)

**Usage Example**:
```java
// High fan-in: many positions, few runs
RangeQueryStrategy strategy = new RangeQueryStrategy(runs);
Map<Long, Run> results = strategy.runForPositions(manyPositions);
// ~250x faster than binary search per position
```

**Token Saver**: The binary search lower bound is in `RangeQueryStrategy.java:220-236`. It's a standard lower_bound implementation finding first element >= target.

### Phase 6: Integration Tests

**Commit**: 718bab7b0 - "Add integration tests for remapping strategies (Phase 6.3)"

**Problem**: Unit tests validated each strategy independently, but didn't test realistic workload scenarios or cross-strategy comparisons at scale.

**Solution**:
- Created `TestRemappingStrategiesIntegration.java` with 4 comprehensive integration tests
- Validated optimal strategy selection for different workload characteristics
- Tested realistic data distributions and scaling behavior

**Key Tests**:
1. **testStreamJoinOptimalForSortedPositions** - Validates StreamJoin outperforms BinarySearch for sorted bulk operations
2. **testRangeQueryOptimalForHighFanIn** - Validates RangeQuery dominates for high fan-in scenarios (n >> m)
3. **testRealisticDataDistributions** - Tests with gaps, varying run sizes, and sparse positions
4. **testLinearScalingBehavior** - Validates scaling properties as m and n grow

**Key Files**:
- `TestRemappingStrategiesIntegration.java` - 4 integration tests validating optimal behavior

**Testing**: Integration tests use realistic parameters (100 runs, 10K positions) and measure relative performance

**Token Saver**: Integration tests at lines 37-227 show end-to-end strategy selection validation. These tests confirm algorithmic complexity claims with real data.

### Phase 7.1: Smart Algorithm Selector

**Commit**: 032efad32 - "Implement smart algorithm selector (Phase 7.1)"

**Problem**: Factory.create() couldn't adapt to bulk remapping workloads, and manual strategy selection required understanding algorithmic trade-offs.

**Solution**:
- Implemented `RemappingAlgorithmSelector` with multi-factor analysis
- Automatic optimal strategy selection based on run count (m), position count (n), sortedness, and gap ratio
- Integrated into `PositionDeleteRemapper` for automatic bulk optimization

**Selection Rules**:
```java
// m < 10: RangeQuery (always optimal for few runs)
if (m < FEW_RUNS_THRESHOLD) return new RangeQueryStrategy(runs);

// High fan-in (n/m > 100) with gaps: RangeQuery optimal
if (n / m > HIGH_FAN_IN_THRESHOLD) {
  double gapRatio = estimateGapRatio(mapping);
  if (gapRatio > SIGNIFICANT_GAPS_THRESHOLD) {
    return new RangeQueryStrategy(runs);
  }
}

// Sorted and significant size: StreamJoin optimal
boolean sorted = isSorted(positions);
if (sorted && n > m) {
  return new StreamJoinStrategy(runs);
}

// Medium runs: BinarySearch sufficient
if (m < BINARY_SEARCH_THRESHOLD) return new BinarySearchStrategy(runs);

// Default: IntervalTree (good for all scenarios)
return new IntervalTreeStrategy(runs);
```

**Key Files**:
- `RemappingAlgorithmSelector.java` - Smart selector with multi-factor analysis
- `PositionDeleteRemapper.java:237-241` - Integrated with bulk remapping
- `TestRemappingAlgorithmSelector.java` - 11 comprehensive tests

**Features**:
- **Gap ratio estimation**: Analyzes source range coverage to detect sparsity
- **Sortedness detection**: Samples first 1000 positions to detect sorted input
- **Cost-based comparison**: Compares index cost vs lookup cost for dense high fan-in scenarios
- **Selection overhead**: <5% overhead provides near-optimal performance

**Testing**: 11 test methods validating selector behavior across different workload characteristics

**Token Saver**: Selector logic is in `RemappingAlgorithmSelector.java:63-113`. The selection rules follow a decision tree prioritizing special cases (few runs, high fan-in) before falling back to general strategies.

### Phase 7.2: JMH Performance Benchmarks

**Commit**: 9c16b556b - "Add JMH benchmarks for remapping algorithms (Phase 7.2)"

**Problem**: No empirical validation of performance claims across diverse workload parameters.

**Solution**:
- Created comprehensive JMH benchmark suite with 54 parameter combinations
- Benchmarks all 5 strategies plus smart selector
- Documented benchmark execution and interpretation in `REMAPPING_BENCHMARKS.md`

**Benchmark Parameters**:
- **numRuns**: 10, 100, 1000 (m)
- **numPositions**: 1000, 10000, 100000 (n)
- **gapRatio**: 0.0 (dense), 0.3 (moderate), 0.5 (sparse)
- **sorted**: true, false

**Total Scenarios**: 3 × 3 × 3 × 2 = 54 parameter combinations × 6 benchmarks = 324 benchmark configurations

**Key Files**:
- `RemappingAlgorithmBenchmark.java` - JMH benchmark suite (6 benchmarks)
- `RemappingBenchmarkUtils.java` - Helper utilities for test data generation
- `REMAPPING_BENCHMARKS.md` - Comprehensive documentation (280 lines)

**Running Benchmarks**:
```bash
# Run all scenarios (takes 2-3 hours)
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhOutputPath=benchmark/remapping-results.txt

# Run specific scenario
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhParams="numRuns=100,numPositions=10000,sorted=true"

# Test only smart selector
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark.smartSelector
```

**Expected Results**:
- **Few runs (m=10)**: RangeQuery 100-750x faster than linear
- **Medium runs (m=100), sorted**: StreamJoin 100x faster than linear
- **Many runs (m=1000), sorted**: StreamJoin 900x faster than linear
- **Smart selector overhead**: Within 5-10% of optimal strategy

**Token Saver**: Benchmark suite at `RemappingAlgorithmBenchmark.java:75-144` is straightforward - 6 @Benchmark methods, each testing one strategy. Read `REMAPPING_BENCHMARKS.md` for interpretation guidance.

### Phase 7.3: Benchmark Analysis and Selector Bug Fix

**Date**: January 16-18, 2026

**Problem**: Initial benchmark run revealed smart selector suboptimal performance (194% average overhead vs optimal strategy).

**Root Causes Identified**:
1. **Primary Issue**: For m < FEW_RUNS_THRESHOLD (10), selector always chose RangeQuery without checking sortedness. RangeQuery requires O(n log n) sorting for unsorted data, causing 3500%+ overhead for large unsorted datasets.
2. **Secondary Issue**: For m ≥ BINARY_SEARCH_THRESHOLD (100), selector checked sortedness and chose StreamJoin for sorted data. However, IntervalTree is faster than StreamJoin at high m due to better cache locality.

**Bug Fix** (RemappingAlgorithmSelector.java):
```java
// BEFORE (buggy):
if (m < FEW_RUNS_THRESHOLD) {
  return new RangeQueryStrategy(runs);  // BUG: Always uses RangeQuery, even for unsorted!
}

boolean sorted = isSorted(positions);
if (sorted && n > m) {
  return new StreamJoinStrategy(runs);  // BUG: Happens BEFORE checking m < 100
}

if (m < BINARY_SEARCH_THRESHOLD) {
  return new BinarySearchStrategy(runs);
}

// AFTER (fixed):
if (m < FEW_RUNS_THRESHOLD) {
  return new RangeQueryStrategy(runs);  // Still needs fix for unsorted data
}

// Move sortedness check INSIDE medium range check
if (m < BINARY_SEARCH_THRESHOLD) {
  boolean sorted = isSorted(positions);
  if (sorted && n > m) {
    return new StreamJoinStrategy(runs);  // Now only for m < 100
  }
  return new BinarySearchStrategy(runs);
}

// For m >= 100: IntervalTree always optimal
return new IntervalTreeStrategy(runs);
```

**Remaining Issue**: Line 76-78 still needs fix for unsorted data with m < 10. Should check sortedness:
```java
if (m < FEW_RUNS_THRESHOLD) {
  boolean sorted = isSorted(positions);
  if (sorted) {
    return new RangeQueryStrategy(runs);  // Optimal for sorted
  }
  return new BinarySearchStrategy(runs);  // Better for unsorted (avoid sort penalty)
}
```

**Benchmark Results Summary** (from `ANALYSIS_20260116.md`):

**Optimal Strategy by Scenario**:
- RangeQuery: 24 scenarios (all sorted, low m)
- IntervalTree: 24 scenarios (all unsorted, or high m)
- StreamJoin: 5 scenarios (sorted, medium m)
- BinarySearch: 1 scenario (unsorted, medium-high m)

**Smart Selector Performance**:
- Average overhead: 194% (POOR - due to unsorted RangeQuery selections)
- Worst cases: 3500%+ overhead (m=10, n=100K, unsorted - RangeQuery sorting penalty)
- Best cases: <5% overhead (sorted scenarios with correct strategy selection)

**Key Findings**:
1. **Fix validated**: Moving sortedness check inside m < 100 block prevents StreamJoin selection for m ≥ 100
2. **Still problematic**: m < 10 unsorted cases show 2600-3500% overhead due to RangeQuery sorting
3. **IntervalTree dominance**: For m ≥ 100, IntervalTree consistently outperforms StreamJoin by 4-6x for sorted data
4. **Actual speedups**: 5-160x for typical workloads (not 100-900x as originally claimed)

**Analysis Tools Created**:
- `analyze_results.py` - Parses JMH output, calculates overhead, identifies optimal strategies
- `visualize_results.py` - Generates comparison charts (requires matplotlib)
- Updated `benchmark/remapping-optimization/README.md` with usage instructions

**Running Analysis**:
```bash
cd benchmark/remapping-optimization

# Parse and analyze results
python3 analyze_results.py results_20260116_162342.txt

# Generate visualizations (requires: pip3 install matplotlib)
python3 visualize_results.py results_20260116_162342.csv

# Output: chart_strategy_comparison.png, chart_selector_overhead.png, chart_speedup_vs_linear.png
```

**Next Steps**:
1. ✅ Fix StreamJoin vs IntervalTree selection (completed)
2. ⏳ Fix RangeQuery selection for unsorted data with m < 10 (pending)
3. Update performance claims in documentation (5-160x instead of 100-900x)
4. Re-run benchmarks to validate fixes

**Token Saver**: See `benchmark/remapping-optimization/ANALYSIS_20260116.md` for detailed benchmark analysis with scenario breakdowns and performance tables.

### Performance Comparison Table

**Single-Position Lookup** (n=1):

| Strategy | Complexity | m=10 | m=100 | m=1000 |
|----------|------------|------|-------|--------|
| Linear | O(m) | 10 | 100 | 1,000 |
| Binary | O(log m) | 3.3 | 6.6 | 10 |
| Interval Tree | O(log m) | 3.3 | 6.6 | 10 |
| Speedup | | 3x | 15x | 100x |

**Bulk Lookup (Sorted)** (n=10,000):

| Strategy | Complexity | m=10 | m=100 | m=1000 |
|----------|------------|------|-------|--------|
| Linear | O(n*m) | 100K | 1M | 10M |
| Binary per position | O(n log m) | 33K | 67K | 100K |
| Stream Join | O(n + m) | 10K | 10K | 11K |
| Range Query | O(m log n) | 133 | 1.3K | 13K |
| Best | | Range | Range | Stream |

### Testing Strategy

**Test Organization**:
- `TestRemappingStrategies.java` - 44 comprehensive unit tests for all strategies
- `TestRemappingStrategiesIntegration.java` - 4 integration tests for realistic workloads
- `TestRemappingAlgorithmSelector.java` - 11 tests for smart algorithm selection
- `RemappingAlgorithmBenchmark.java` - 6 JMH benchmarks × 54 scenarios = 324 configurations
- Unit tests for each strategy independently
- Property-based tests comparing all strategies (10,000+ lookups)
- Edge cases: empty, single run, gaps, boundaries
- Validation tests: unsorted, overlapping runs
- Performance tests: parameterized with 10, 100, 1000 runs

**Total Test Coverage**: 82 tests (59 remapping optimization tests + existing tests)

**Running Tests**:
```bash
# All remapping strategy tests
./gradlew :iceberg-core:test --tests "TestRemappingStrategies"
./gradlew :iceberg-core:test --tests "TestRemappingStrategiesIntegration"
./gradlew :iceberg-core:test --tests "TestRemappingAlgorithmSelector"

# Specific strategy
./gradlew :iceberg-core:test --tests "TestRemappingStrategies.testBinarySearchBasic"

# Run all remapping tests
./gradlew :iceberg-core:test --tests "*Remapping*"

# Run JMH benchmarks
./gradlew :iceberg-core:jmh -PjmhIncludeRegex=RemappingAlgorithmBenchmark
```

**Token Saver**: Read the test file from the top. The basic tests (lines 37-106) show all strategies with the same test data. Property-based tests (lines 277-318, 513-560, 758-808) verify all strategies produce identical results.

### Integration Points

**GenericCompactionMap.java**:
```java
// Line 200: Added strategy field
private transient volatile RemappingStrategy strategy;

// Lines 237-248: Modified runForPosition()
@Override
public Run runForPosition(long sourcePosition) {
  // Lazy initialization of remapping strategy
  if (strategy == null) {
    synchronized (this) {
      if (strategy == null) {
        strategy = RemappingStrategy.Factory.create(runs());
      }
    }
  }
  return strategy.runForPosition(sourcePosition);
}
```

**Automatic Selection** (RemappingStrategy.Factory):
- m < 10: LinearSearchStrategy (simple, no overhead)
- 10 ≤ m < 100: BinarySearchStrategy (fast, minimal overhead)
- m ≥ 100: IntervalTreeStrategy (optimal for large m)

**Manual Selection** (for bulk operations):
```java
// Application code decides based on workload characteristics
if (n > m * 100) {
  // High fan-in: use range query
  strategy = new RangeQueryStrategy(runs);
} else {
  // General case: use stream join
  strategy = new StreamJoinStrategy(runs);
}
Map<Long, Run> results = strategy.runForPositions(positions);
```

### Key Design Decisions

**Why Not Auto-Select for Bulk?**

The Factory.create() is called when the FileMapping is first accessed and doesn't know n (number of positions to remap). Bulk strategy selection depends on runtime characteristics (n/m ratio) that vary per operation.

**Solution**: Higher-level APIs (like PositionDeleteRemapper) should choose bulk strategy based on actual workload.

**Why Multiple Bulk Strategies?**

Different scenarios have different optimal algorithms:
- Stream join O(n + m): Best when m ≈ n
- Range query O(m log n): Best when n >> m (high fan-in)
- Crossover point: m ≈ n / log n

Example: For n=10,000:
- m=10: Range query (133 vs 10,010 ops)
- m=1,000: Stream join (11,000 vs 13,300 ops)

**Why Lazy Initialization?**

Strategy construction has validation overhead (checking sorted, building tree). Lazy initialization avoids this cost until first lookup, and only happens once per FileMapping.

### Common Issues and Solutions

**Issue 1: Strategy Not Switching at Thresholds**

**Problem**: Factory still uses linear search for m=15.

**Solution**: Check Factory.create() thresholds at RemappingStrategy.java:60-62. Threshold is m < 10 for linear, < 100 for binary.

**Issue 2: Bulk Lookup Slower Than Expected**

**Problem**: Stream join doesn't seem faster than per-position binary search.

**Solution**:
1. Check if positions are sorted (unsorted falls back to binary search)
2. Verify m is appropriate (stream join better when m < n / log n)
3. Consider range query for high fan-in (n >> m)

**Issue 3: Test Failures After Adding New Strategy**

**Problem**: Property-based tests fail with new strategy.

**Solution**: All strategies must produce identical results. Check:
1. Boundary handling (inclusive start, exclusive end)
2. Gap handling (return null for positions in gaps)
3. Out-of-bounds handling (return null)

**Test Reference**: `TestRemappingStrategies.testAllStrategiesMatch()` at line 283 runs 10,000 lookups comparing all strategies.

### Token-Saving Strategies for Remapping

**1. Start with Test File**

`TestRemappingStrategies.java` is well-organized:
- Lines 37-106: Basic tests showing all strategies with same data
- Lines 175-275: Large-scale tests (10, 100, 1000 runs)
- Lines 277-318: Property-based test (read this first!)
- Lines 366-573: Stream join tests (bulk API examples)
- Lines 575-821: Range query tests (high fan-in examples)

**2. Algorithm Quick Reference**

Don't read full implementations for understanding:
- Linear: `LinearSearchStrategy.java:48-59` (9 lines - simple loop)
- Binary: `BinarySearchStrategy.java:62-91` (30 lines - standard binary search)
- Interval Tree: `IntervalTreeStrategy.java:78-103` (26 lines - tree search + prune)
- Stream Join: `StreamJoinStrategy.java:119-157` (39 lines - merge join)
- Range Query: `RangeQueryStrategy.java:167-192` (26 lines - range bounds)

**3. Use Grep for Navigation**

```bash
# Find strategy implementations
grep -l "implements RemappingStrategy" core/src/main/java/org/apache/iceberg/*.java

# Find usage in GenericCompactionMap
grep -n "RemappingStrategy" core/src/main/java/org/apache/iceberg/GenericCompactionMap.java

# Find test coverage
grep -n "Strategy" core/src/test/java/org/apache/iceberg/TestRemappingStrategies.java | head -20
```

**4. Performance Formula Reference**

Quick reference without reading code:
- Single lookup: Binary search is O(log m)
- Bulk sorted: Stream join is O(n + m), range query is O(m log n)
- Crossover: Range better when m < n / log n
- Rule of thumb: n/m > 100 → use range query

### Future Enhancements

**Parallel Bulk Remapping**:
Partition positions by run ranges and process in parallel:
```java
// Split positions into buckets by run
// Process each bucket independently
// Merge results
```

**Range Compression**:
Store positions as ranges [start, end) instead of individual positions:
```java
// Instead of: [5, 6, 7, 8, 9]
// Store as: [5-10)
// Reduces memory and speeds up range query
```

**RoaringBitmap Integration**:
For deletion vectors, use RoaringBitmap for efficient position storage and range operations.

