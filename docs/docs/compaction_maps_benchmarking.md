# Remapping Algorithm Performance Benchmarks

This document describes the JMH performance benchmarks for position delete remapping algorithms.

## Overview

The benchmarks measure the performance of five remapping strategies plus the smart selector across different workload characteristics:

**Strategies Benchmarked:**
- LinearSearchStrategy - O(m) per position lookup
- BinarySearchStrategy - O(log m) per position lookup
- IntervalTreeStrategy - O(log m) with tree structure
- StreamJoinStrategy - O(n + m) for sorted positions
- RangeQueryStrategy - O(m log n) for high fan-in
- SmartSelector - Automatic strategy selection

**Workload Parameters:**
- **numRuns** (m): 10, 100, 1000 - Number of runs in compaction map
- **numPositions** (n): 1000, 10000, 100000 - Number of positions to remap
- **gapRatio**: 0.0 (dense), 0.3 (moderate), 0.5 (sparse) - Percentage of gaps
- **sorted**: true/false - Whether positions are sorted

## Running Benchmarks

### Run All Scenarios

Run all combinations of parameters (takes 2-3 hours):

```bash
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhOutputPath=benchmark/remapping-algorithm-benchmark-results.txt
```

### Run Specific Scenarios

Run specific parameter combinations:

```bash
# Test sorted positions with 100 runs and 10k positions
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhParams="numRuns=100,numPositions=10000,gapRatio=0.3,sorted=true"
```

```bash
# Test high fan-in scenario (many positions, few runs)
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhParams="numRuns=10,numPositions=100000,gapRatio=0.0,sorted=true"
```

```bash
# Test large run count scenario
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhParams="numRuns=1000,numPositions=10000,gapRatio=0.0,sorted=false"
```

### Run Specific Benchmarks

Run only specific strategies:

```bash
# Compare StreamJoin vs RangeQuery for sorted positions
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark.streamJoin|RemappingAlgorithmBenchmark.rangeQuery \
    -PjmhParams="sorted=true"
```

```bash
# Test only the smart selector
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark.smartSelector
```

### Additional JMH Options

```bash
# Run with custom JMH options
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhArgs="-wi 5 -i 10 -f 2 -rf json -rff results.json"
```

JMH options:
- `-wi N` - Number of warmup iterations (default: 3)
- `-i N` - Number of measurement iterations (default: 5)
- `-f N` - Number of forks (default: 1)
- `-rf FORMAT` - Result format (text, csv, json)
- `-rff FILE` - Result output file

## Measured Performance Characteristics

*Based on January 22, 2026 benchmark results (324 configurations)*

### Few Runs (m = 10)

**RangeQuery** is optimal for sorted data:
- Measured: 48µs for n=10k, 529µs for n=100k (sorted, gap=0.0)
- 6.5-6.7x speedup vs LinearSearch

### Medium Runs (m = 100)

**Sorted positions**: StreamJoin optimal
- Measured: 361µs for n=10k, 889µs for n=100k (sorted, gap=0.0)
- 3.3-23.6x speedup vs LinearSearch

**Unsorted positions**: IntervalTree optimal
- IntervalTree wins all unsorted scenarios

### Many Runs (m = 1000)

**Sorted**: StreamJoin optimal
- Measured: 371µs for n=10k, 5380µs for n=100k (sorted, gap=0.0)
- 3.4-32.4x speedup vs LinearSearch

**Unsorted**: IntervalTree optimal

### Selection Summary

| Condition | Best Strategy |
|-----------|---------------|
| Unsorted | IntervalTree |
| Sorted + gapRatio > 0.3 | RangeQuery |
| Sorted + n >= 10000 + m >= 100 | StreamJoin |
| Sorted + other | RangeQuery |

## Interpreting Results

### Sample Output

```
Benchmark                                (numRuns)  (numPositions)  (gapRatio)  (sorted)  Mode  Cnt    Score    Error  Units
RemappingAlgorithmBenchmark.linearSearch        10            1000         0.0      true  avgt    5   12.345 ±  1.234  us/op
RemappingAlgorithmBenchmark.binarySearch        10            1000         0.0      true  avgt    5    1.234 ±  0.123  us/op
RemappingAlgorithmBenchmark.streamJoin          10            1000         0.0      true  avgt    5    0.987 ±  0.098  us/op
RemappingAlgorithmBenchmark.rangeQuery          10            1000         0.0      true  avgt    5    0.456 ±  0.045  us/op
RemappingAlgorithmBenchmark.smartSelector       10            1000         0.0      true  avgt    5    0.478 ±  0.047  us/op
```

### Analysis Guidelines

1. **Verify Linear Search is slowest**: Baseline for comparison
2. **Check BinarySearch vs IntervalTree**: BinarySearch should be faster for m < 100
3. **Validate StreamJoin for sorted**: Should beat BinarySearch for sorted positions
4. **Confirm RangeQuery for few runs**: Should be fastest when m < 10
5. **Smart Selector overhead**: Should be within 5-10% of optimal strategy

### Speedup Calculations

Compare strategies relative to LinearSearch:

```
Speedup = LinearSearch_Time / Strategy_Time

Measured speedups (January 2026 benchmarks):
- Small scale (n=1000): 1.1-1.4x
- Medium scale (n=10000): 3.3-6.5x
- Large scale (n=100000): 6.7-32.4x
- Smart selector overhead: ~5% average
```

## Benchmark Scenarios Matrix

Total scenarios: 3 (numRuns) × 3 (numPositions) × 3 (gapRatio) × 2 (sorted) = 54 configurations

**Key Scenarios to Review:**

1. **Few runs, many positions (sorted)**
   - numRuns=10, numPositions=100000, sorted=true
   - Winner: RangeQuery (6.7x speedup)

2. **Many runs, sorted positions**
   - numRuns=1000, numPositions=10000, sorted=true
   - Winner: StreamJoin (3.4x speedup)

3. **Medium runs, unsorted**
   - numRuns=100, numPositions=10000, sorted=false
   - Winner: IntervalTree (unsorted always uses IntervalTree)

4. **Large scale, sorted**
   - numRuns=1000, numPositions=100000, sorted=true
   - Winner: StreamJoin (32.4x speedup)

5. **Large scale, unsorted**
   - numRuns=1000, numPositions=100000, sorted=false
   - Winner: IntervalTree

## Troubleshooting

### Benchmarks Taking Too Long

Reduce parameter combinations:

```bash
# Test only key scenarios
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhParams="numRuns=100,numPositions=10000"
```

### Inconsistent Results

Increase warmup and measurement iterations:

```bash
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhArgs="-wi 10 -i 20"
```

### Out of Memory

Reduce position count or run with more heap:

```bash
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhJvmArgs="-Xmx4g"
```

## Files

- `core/src/jmh/java/org/apache/iceberg/RemappingAlgorithmBenchmark.java` - Main benchmark
- `core/src/jmh/java/org/apache/iceberg/RemappingBenchmarkUtils.java` - Helper utilities
- `REMAPPING_BENCHMARKS.md` - This documentation

## Benchmark Results

### January 22, 2026 Run (Latest - 324 Configurations)

**Run ID**: `results_20260122_140158`
**Location**: `benchmark/remapping-optimization/results/`
**Duration**: ~2 hours
**VM**: OpenJDK 17, containerized

**Key Findings**:

1. **Actual Speedups** (vs LinearSearch baseline, sorted=true, gap=0.0):
   - Small scale (n=1000): 1.1-1.4x speedup
   - Medium scale (n=10000): 3.3-6.5x speedup
   - Large scale (n=100000): 6.7-32.4x speedup

2. **Optimal Strategy by Scenario**:
   - IntervalTree: 24 scenarios (all unsorted)
   - RangeQuery: 17 scenarios (sorted, sparse or small m)
   - StreamJoin: 10 scenarios (sorted, dense, m ≥ 100)
   - BinarySearch: 3 scenarios (unsorted edge cases with m=1000)

3. **Smart Selector Performance**:
   - **Average overhead**: 4.68%
   - **Max overhead**: 33.3% (unsorted edge case where BinarySearch wins)
   - **Selector faster than optimal**: 20 cases (measurement variance)

4. **Key Insight**: StreamJoin requires m ≥ 100
   - For small m (e.g., m=10), RangeQuery wins even with large n
   - Updated selector to require both n >= 10000 AND m >= 100 for StreamJoin

### January 16, 2026 Run (Historical - Pre-Fix)

**Run ID**: `results_20260116_162342`
**Status**: Historical reference only (selector has been significantly updated since)

**Issues Found** (now fixed):
- Initial selector had 194% average overhead
- 3500%+ overhead for unsorted data with m < 10
- Selection logic flaws corrected in subsequent commits

### Analysis Tools

Use the provided Python scripts to analyze benchmark results:

```bash
cd benchmark/remapping-optimization

# Parse and analyze JMH output
python3 analyze_results.py results_20260116_162342.txt

# Generate visualization charts (requires matplotlib)
python3 visualize_results.py results_20260116_162342.csv
```

**Output**:
- Console summary: optimal strategies, selector overhead, performance tables
- CSV file: `results_20260116_162342.csv` (for spreadsheets)
- Charts (if matplotlib installed):
  - `chart_strategy_comparison.png` - Performance across all strategies
  - `chart_selector_overhead.png` - Selector overhead vs optimal
  - `chart_speedup_vs_linear.png` - Speedup comparison

See `benchmark/remapping-optimization/ANALYSIS_20260116.md` for detailed analysis including:
- Scenario-by-scenario breakdown
- Performance validation against claims
- Insights and recommendations
- Selector logic improvements

## References

- JMH Documentation: https://github.com/openjdk/jmh
- Iceberg Benchmarks: `site/docs/benchmarks.md`
- Remapping Optimization Plan: `REMAPPING_OPTIMIZATION_IMPLEMENTATION_PLAN.md`
- Benchmark Analysis: `benchmark/remapping-optimization/ANALYSIS_20260116.md`
