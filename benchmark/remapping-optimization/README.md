# Remapping Algorithm Benchmark Results

This directory contains JMH benchmark results for the remapping algorithm optimization.

## Benchmark Organization

### File Naming Convention

```
results_YYYYMMDD_HHMMSS.<format>
```

- **YYYYMMDD**: Date in year-month-day format
- **HHMMSS**: Time in hour-minute-second format (24-hour)
- **format**: Output format (txt, json, csv)

### Output Formats

**Text Format (`.txt`)**
- Human-readable output with full JMH logs
- Includes warmup iterations, measurement iterations, and final results
- Best for understanding what happened during the benchmark run

**JSON Format (`.json`)**
- Machine-readable structured output
- Contains all benchmark results with full metadata
- Best for programmatic analysis and visualization
- Can be parsed with tools like `jq`

**CSV Format (`.csv`)** (if generated)
- Spreadsheet-friendly tabular format
- Easy to import into Excel, Google Sheets, or data analysis tools
- Best for creating charts and comparative analysis

## Benchmark Parameters

The full benchmark suite tests 54 parameter combinations:

**Parameters:**
- **numRuns**: 10, 100, 1000 (m = number of runs in compaction map)
- **numPositions**: 1000, 10000, 100000 (n = number of positions to remap)
- **gapRatio**: 0.0 (dense), 0.3 (moderate gaps), 0.5 (sparse)
- **sorted**: true, false

**Strategies Benchmarked:**
1. `linearSearch` - O(m) baseline
2. `binarySearch` - O(log m) optimized
3. `intervalTree` - O(log m) with tree structure
4. `streamJoin` - O(n + m) for sorted positions
5. `rangeQuery` - O(m log n) for high fan-in
6. `smartSelector` - Automatic optimal selection

**Total Configurations**: 6 strategies × 54 parameter combinations = 324 benchmark runs

## Running Benchmarks

### Full Suite

```bash
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhArgs="-rf json -rff benchmark/remapping-optimization/results_TIMESTAMP.json"
```

**Duration**: Approximately 2-3 hours

### Specific Scenarios

```bash
# Test high fan-in scenario (many positions, few runs)
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhParams="numRuns=10,numPositions=100000,gapRatio=0.0,sorted=true"

# Test sorted bulk remapping
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhParams="numRuns=100,numPositions=10000,sorted=true"

# Test smart selector only
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark.smartSelector
```

## Analyzing Results

### Quick Analysis (Text Output)

```bash
# View full results
cat results_20260116_162342.txt

# Extract summary statistics
grep "Benchmark" results_20260116_162342.txt | grep -A 1 "Mode"

# Find fastest strategies
grep "avgt" results_20260116_162342.txt | sort -k 6 -n
```

### Detailed Analysis (JSON Output)

```bash
# Pretty-print JSON
jq '.' results_20260116_162342.json

# Extract specific benchmark results
jq '.[] | select(.benchmark | contains("smartSelector"))' results_20260116_162342.json

# Compare strategies for specific parameters
jq '.[] | select(.params.numRuns == "100" and .params.numPositions == "10000")' results_20260116_162342.json
```

## Expected Results

### Few Runs (m=10)
- **Winner**: RangeQuery
- **Speedup**: 100-750x over linear search
- **Why**: O(m log n) optimal when m is very small

### Medium Runs (m=100), Sorted
- **Winner**: StreamJoin
- **Speedup**: 100x over linear search
- **Why**: O(n + m) single pass beats O(n log m)

### Many Runs (m=1000), Sorted
- **Winner**: StreamJoin
- **Speedup**: 900x over linear search
- **Why**: Linear scan dominates over repeated tree lookups

### Smart Selector Overhead
- **Expected**: Within 5-10% of optimal strategy
- **Validation**: Compare `smartSelector` results to best strategy for each scenario

## Interpreting Scores

JMH reports scores in **microseconds per operation** (us/op) with Mode=AverageTime:
- **Lower is better**
- Score represents average time to remap all positions
- Error margin (±) indicates measurement variance

Example:
```
RemappingAlgorithmBenchmark.streamJoin  avgt  5  123.456 ± 10.234  us/op
```
- Average: 123.456 microseconds per operation
- Standard deviation: ±10.234 microseconds
- 5 measurement iterations

## References

- **Benchmark Implementation**: `core/src/jmh/java/org/apache/iceberg/RemappingAlgorithmBenchmark.java`
- **Test Data Generation**: `core/src/jmh/java/org/apache/iceberg/RemappingBenchmarkUtils.java`
- **Documentation**: `REMAPPING_BENCHMARKS.md`
- **Strategy Implementations**: `core/src/main/java/org/apache/iceberg/*Strategy.java`
