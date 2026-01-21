# Remapping Algorithm Benchmark Results

This directory contains JMH benchmark results for the remapping algorithm optimization.

## Quick Start

### Local Execution

```bash
# Quick benchmark (~30 min)
./run_full_benchmark.sh --quick

# Full benchmark suite (~2-3 hours)
./run_full_benchmark.sh
```

### Docker Compose (Recommended)

Docker Compose provides an isolated environment with persistent results storage.

```bash
cd benchmark/remapping-optimization

# Build the image
docker compose build

# Run quick benchmark (~10-15 min)
docker compose run --rm benchmark-quick

# Run default benchmark (~45 min)
docker compose run --rm benchmark

# Run full suite (~2-3 hours)
docker compose run --rm benchmark-full

# Run only smart selector tests
docker compose run --rm benchmark-selector

# View results
docker compose run --rm shell ls -la /benchmark/results

# Copy results to local directory
docker compose cp benchmark:/benchmark/results ./local-results

# Run analysis on results
docker compose run --rm analyze

# Interactive shell for exploration
docker compose run --rm shell

# Clean up (removes volume with results)
docker compose down -v
```

### Docker (Manual)

For more control, use Docker directly:

```bash
# Build from repository root
docker build -t iceberg-jmh-benchmark -f benchmark/remapping-optimization/Dockerfile .

# Run with results mounted to local directory
docker run --rm --cpus=4 --memory=8g \
    -v $(pwd)/benchmark/remapping-optimization/results:/benchmark/results \
    iceberg-jmh-benchmark

# Quick benchmark
docker run --rm iceberg-jmh-benchmark RemappingAlgorithmBenchmark -wi 1 -i 2 -f 1
```

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

### Automated Analysis Script

The `analyze_results.py` script provides comprehensive analysis of benchmark results:

```bash
# Run analysis on text output
python3 analyze_results.py results_20260116_162342.txt
```

**Output includes:**
- Summary statistics (total scenarios, measurements)
- Optimal strategy breakdown by scenario characteristics
- Smart selector overhead analysis
- Performance comparison by scale
- CSV export for further analysis

**Example output:**
```
BENCHMARK SUMMARY
- Total scenarios: 54
- Total measurements: 324

OPTIMAL STRATEGY BY SCENARIO
rangeQuery: 24 scenarios (all sorted)
intervalTree: 24 scenarios (all unsorted)
streamJoin: 5 scenarios

SMART SELECTOR OVERHEAD ANALYSIS
- Average overhead: 5-10% (expected)
- High overhead cases flagged for investigation
```

### Visualization

The `visualize_results.py` script generates charts from benchmark data:

```bash
# Generate all charts from CSV
python3 visualize_results.py results_20260116_162342.csv
```

**Requirements:**
```bash
pip3 install matplotlib
```

**Generated charts:**
1. `chart_strategy_comparison.png` - Performance comparison across all strategies
2. `chart_selector_overhead.png` - Smart selector overhead vs optimal (top 20 worst cases)
3. `chart_speedup_vs_linear.png` - Speedup comparison against linear search baseline

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

## Key Findings (January 2026)

Based on 324-configuration benchmarks:

### Unsorted Data
- **Winner**: IntervalTree (wins 46/54 unsorted scenarios)
- **Speedup**: 2-7x over BinarySearch
- **Why**: O(log m) lookups without sorting overhead

### Sorted Data
- **Winner**: RangeQuery or StreamJoin (IntervalTree never wins)
- **Speedup**: 1.5-3x over IntervalTree
- **Why**: Can exploit sorted order for efficient scanning

### BinarySearch
- **Never wins** any scenario (removed from selector)

### Smart Selector Decision Tree

```
if unsorted:
    return IntervalTree        # Wins 85% of unsorted scenarios

if gapRatio > 0.3:
    return RangeQuery          # Sparse: skip gaps efficiently

if m >= 100 and n >= 10000:
    return StreamJoin          # Bulk sorted workloads

return RangeQuery              # Default for sorted
```

### Smart Selector Overhead
- **Expected**: <10% compared to manually selecting optimal
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

## Troubleshooting

### High Error Margins

If error margins exceed 20% of measurements:
- Close other applications
- Use AC power (not battery)
- Use Docker with dedicated resources (`--cpus=4 --memory=8g`)
- Increase fork count (`-f 5`)

### Out of Memory

```bash
# Increase heap in Docker
docker run --rm -e JAVA_OPTS="-Xms8g -Xmx8g" ...

# Or locally
./gradlew :iceberg-core:jmh -PjmhJvmArgs="-Xmx16g"
```

### Build Failures

```bash
# Clean and rebuild
./gradlew clean :iceberg-core:jmhJar

# Skip checks during build
./gradlew :iceberg-core:jmhJar -x test -x spotlessCheck
```

## References

- **Benchmark Implementation**: `core/src/jmh/java/org/apache/iceberg/RemappingAlgorithmBenchmark.java`
- **Test Data Generation**: `core/src/jmh/java/org/apache/iceberg/RemappingBenchmarkUtils.java`
- **Documentation**: `REMAPPING_BENCHMARKS.md`
- **Strategy Implementations**: `core/src/main/java/org/apache/iceberg/*Strategy.java`
