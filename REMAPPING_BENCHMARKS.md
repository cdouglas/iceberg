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

## Expected Performance Characteristics

### Few Runs (m = 10)

**RangeQuery** is optimal:
- O(m log n) complexity
- Fast for few runs regardless of position count
- Expected: 100-1000µs for 100k positions

### Medium Runs (m = 100)

**Sorted positions**: StreamJoin optimal
- O(n + m) single pass
- Expected: 500-2000µs for 10k positions

**Unsorted positions**: BinarySearch optimal
- O(n log m) complexity
- Expected: 1000-5000µs for 10k positions

### Many Runs (m = 1000)

**IntervalTree** optimal:
- O(n log m) with good cache locality
- Expected: 5000-20000µs for 10k positions

### High Fan-in (n >> m)

**RangeQuery** optimal when n/m > 100:
- O(m log n) complexity
- Expected: 1000-5000µs for 100k positions, 10 runs

### With Gaps (gapRatio > 0.3)

**Predicate pushdown** benefits:
- Filters irrelevant runs
- 30-50% speedup for sparse scenarios
- RangeQuery benefits most from sparsity

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

Expected speedups for typical workloads:
- BinarySearch: 10-15x (m=100)
- IntervalTree: 15-20x (m=1000)
- StreamJoin: 20-50x (sorted, m=100)
- RangeQuery: 100-250x (m=10, n=100k)
```

## Benchmark Scenarios Matrix

Total scenarios: 3 (numRuns) × 3 (numPositions) × 3 (gapRatio) × 2 (sorted) = 54 configurations

**Key Scenarios to Review:**

1. **Few runs, many positions**
   - numRuns=10, numPositions=100000, sorted=true
   - Expected winner: RangeQuery

2. **Many runs, sorted positions**
   - numRuns=1000, numPositions=10000, sorted=true
   - Expected winner: StreamJoin

3. **Medium runs, unsorted**
   - numRuns=100, numPositions=10000, sorted=false
   - Expected winner: BinarySearch

4. **High gaps, high fan-in**
   - numRuns=10, numPositions=100000, gapRatio=0.5
   - Expected winner: RangeQuery (predicate pushdown benefit)

5. **Large scale**
   - numRuns=1000, numPositions=100000
   - Expected winner: IntervalTree or StreamJoin (if sorted)

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

## References

- JMH Documentation: https://github.com/openjdk/jmh
- Iceberg Benchmarks: `site/docs/benchmarks.md`
- Remapping Optimization Plan: `REMAPPING_OPTIMIZATION_IMPLEMENTATION_PLAN.md`
