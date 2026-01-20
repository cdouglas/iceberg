# Remapping Algorithm Benchmark Report

**Date**: January 19, 2026
**Benchmark Run**: January 16, 2026
**Total Configurations**: 324 (6 strategies × 54 parameter combinations)

## Executive Summary

The remapping algorithm optimization provides significant performance improvements for position delete remapping during compaction operations. For sorted workloads at scale, the optimized algorithms achieve **5-23x speedup** over the baseline linear search. However, the smart selector has a **known issue** with unsorted data when m < 100, causing up to **3500% overhead** in worst cases.

### Key Results

| Workload Scale | Best Strategy | Speedup vs Linear |
|----------------|---------------|-------------------|
| n=1,000 (small) | rangeQuery/streamJoin | 1.2-1.5x |
| n=10,000 (medium) | rangeQuery | 4-5x |
| n=100,000 (large) | streamJoin/rangeQuery | 6-23x |

### Recommendations

1. **For production use**: Enable compaction maps - the speedups are substantial for typical workloads
2. **Known issue**: Smart selector overhead for unsorted data with m < 100 needs fixing (Phase 7.4)
3. **Safe defaults**: Current implementation works well for sorted bulk remapping (the common case)

---

## Benchmark Configuration

### Parameters Tested

- **numRuns (m)**: 10, 100, 1000 - Number of runs in compaction map
- **numPositions (n)**: 1,000, 10,000, 100,000 - Number of positions to remap
- **gapRatio**: 0.0 (dense), 0.3 (moderate), 0.5 (sparse)
- **sorted**: true, false - Whether positions are pre-sorted

### Strategies Benchmarked

| Strategy | Complexity | Best For |
|----------|------------|----------|
| linearSearch | O(n×m) | Baseline only |
| binarySearch | O(n log m) | Medium m, unsorted |
| intervalTree | O(n log m) | Large m, unsorted |
| streamJoin | O(n + m) | Sorted positions |
| rangeQuery | O(m log n) | Few runs (m small) |
| smartSelector | Adaptive | Automatic selection |

---

## Results Analysis

### Strategy Performance Comparison

![Strategy Comparison](chart_strategy_comparison.png)

**Figure 1**: Performance comparison across all strategies for sorted data (gap=0.0). Log-log scale shows how strategies scale with increasing runs (m).

**Key observations**:

1. **At small scale (n=1,000)**: All strategies perform similarly (~15-50 μs). Linear search is competitive because the data fits in cache.

2. **At medium scale (n=10,000)**: Clear separation emerges. RangeQuery and smartSelector lead at m=10 (~33 μs), while linear search degrades to 180-730 μs.

3. **At large scale (n=100,000)**: Dramatic differences appear:
   - Best strategies: 300-3,000 μs
   - Linear search: 2,000-63,000 μs
   - **23x speedup** for streamJoin at m=1000

### Speedup vs Linear Search

![Speedup Chart](chart_speedup_vs_linear.png)

**Figure 2**: Speedup multiplier compared to linear search baseline. Values above the red dashed line (1x) indicate improvement.

**Key findings**:

| Scale | m=10 | m=100 | m=1000 |
|-------|------|-------|--------|
| n=1,000 | 1.4x | 1.5x | 1.2x |
| n=10,000 | 5.4x | 4.0x | 3.7x |
| n=100,000 | 6.4x | 22.3x | **23.5x** |

The speedup increases dramatically with scale, reaching **23.5x** for the most demanding workload (n=100,000, m=1000).

### Smart Selector Overhead Analysis

![Selector Overhead](chart_selector_overhead.png)

**Figure 3**: Smart selector overhead vs optimal strategy for top 20 worst cases. Red bars indicate unsorted data, blue bars indicate sorted data.

**Critical finding**: The selector has severe overhead (2600-3500%) for three specific scenarios:
- n=10,000, m=10, sorted=false: **3531% overhead**
- n=100,000, m=100, sorted=false: **2660% overhead**
- n=100,000, m=10, sorted=false: **2622% overhead**

**Root cause**: When m < 100 and data is unsorted, the selector incorrectly chooses RangeQuery, which requires O(n log n) sorting. For unsorted data, IntervalTree (O(n log m) without sorting) is optimal.

### Optimal Strategy Distribution

| Strategy | Optimal Scenarios | Characteristics |
|----------|-------------------|-----------------|
| rangeQuery | 24 | All sorted scenarios |
| intervalTree | 24 | All unsorted scenarios |
| streamJoin | 5 | Mixed (high n, moderate m) |
| binarySearch | 1 | Edge case only |

**Pattern**:
- **Sorted data** → RangeQuery wins
- **Unsorted data** → IntervalTree wins

---

## Performance by Scale

### Small Scale (n=1,000 positions)

```
n=1000, m=1000, sorted=true:
  rangeQuery     :    20.75 μs  ← Best
  streamJoin     :    22.16 μs
  linearSearch   :    25.80 μs
  binarySearch   :    38.73 μs
  smartSelector  :    49.36 μs  ← 138% overhead (known issue)
  intervalTree   :    49.78 μs

  Speedup vs linear: 1.2x
```

At small scale, the overhead of tree structures and sorting outweighs benefits. Linear search remains competitive.

### Medium Scale (n=10,000 positions)

```
n=10000, m=100, sorted=true:
  rangeQuery     :   177.12 μs  ← Best
  smartSelector  :   185.88 μs  ← 5% overhead (good)
  streamJoin     :   269.08 μs
  binarySearch   :   332.46 μs
  intervalTree   :   359.95 μs
  linearSearch   :   702.94 μs

  Speedup vs linear: 4.0x
```

Smart selector performs well for sorted medium-scale workloads.

### Large Scale (n=100,000 positions)

```
n=100000, m=1000, sorted=true:
  streamJoin     :  2,685 μs  ← Best
  smartSelector  :  2,869 μs  ← 7% overhead (good)
  rangeQuery     :  2,874 μs
  intervalTree   :  5,272 μs
  binarySearch   :  5,409 μs
  linearSearch   : 63,050 μs

  Speedup vs linear: 23.5x
```

At scale, optimized algorithms provide dramatic improvements. Smart selector overhead is acceptable (~7%).

---

## Known Issues

### Issue 1: Smart Selector Overhead for Unsorted Data (m < 100)

**Severity**: High for affected workloads
**Status**: Pending fix (Phase 7.4)

**Problem**: Selector doesn't check sortedness when m < 100, choosing RangeQuery which requires expensive O(n log n) sorting.

**Affected scenarios**:
- m=10, sorted=false: Up to 3531% overhead
- m=100, sorted=false: Up to 2660% overhead

**Workaround**: For unsorted workloads, manually use IntervalTree:
```java
RemappingStrategy strategy = new IntervalTreeStrategy(runs);
Map<Long, Run> results = strategy.runForPositions(positions);
```

**Fix**: Add sortedness check to selector for m < 100 cases.

### Issue 2: RangeQuery Performance on Unsorted Data

**Severity**: Informational
**Status**: By design

RangeQuery requires sorted positions for its O(m log n) complexity. When positions are unsorted, it must sort them first (O(n log n)), making it suboptimal.

---

## Recommendations

### For Typical Compaction Workloads

1. **Enable compaction maps** (`write.compaction-map.enabled=true`) - the performance benefits are substantial
2. **Use default smart selector** - works well for sorted bulk remapping (the common case)
3. **Expected performance**: 4-23x speedup at scale

### For Known Problem Scenarios

If experiencing poor performance with unsorted position deletes:
1. Check if positions are sorted - if not, consider sorting before remapping
2. For small m (< 100), manually select IntervalTree strategy
3. Wait for Phase 7.4 fix

### For Future Development

1. **Phase 7.4**: Fix smart selector for m < 100 unsorted cases
2. **Consider**: Add sortedness detection with sampling
3. **Consider**: Add runtime metrics to detect suboptimal strategy selection

---

## Appendix: Raw Data Summary

### Measurement Statistics

- **Total scenarios**: 54
- **Total measurements**: 324
- **Strategies**: 6
- **JMH iterations**: 5 per measurement

### Smart Selector Overhead Distribution

| Overhead Range | Count | % of Scenarios |
|---------------|-------|----------------|
| < 0% (faster) | 14 | 26% |
| 0-10% | 18 | 33% |
| 10-100% | 12 | 22% |
| 100-500% | 7 | 13% |
| > 500% | 3 | 6% |

### Files Generated

- `results_20260116_162342.txt` - Raw JMH output
- `results_20260116_162342.json` - Structured JSON results
- `results_20260116_162342.csv` - Tabular data for analysis
- `chart_strategy_comparison.png` - Strategy performance comparison
- `chart_selector_overhead.png` - Selector overhead analysis
- `chart_speedup_vs_linear.png` - Speedup visualization

---

*Report generated: January 19, 2026*
*Benchmark suite: RemappingAlgorithmBenchmark*
*Analysis tools: analyze_results.py, visualize_results.py*
