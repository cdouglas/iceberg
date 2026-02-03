# Remapping Algorithm Benchmark

JMH benchmarks for comparing remapping algorithm strategies used in compaction map position remapping.

**Purpose:** Measure pure algorithm performance (CPU-bound, no I/O) to tune strategy selection thresholds.

**Related:** For end-to-end benchmarks including I/O costs, see [`remapping-microbenchmark`](../remapping-microbenchmark/).

## Quick Start

### Parallel Execution (Recommended for Large Machines)

For machines with many cores, run all 6 strategies in parallel containers:

```bash
cd benchmark/remapping-optimization

# Default settings (~2 hours with 6 parallel containers)
./run_parallel_benchmark.sh

# Quick test (~20 min)
./run_parallel_benchmark.sh quick

# Full suite with more iterations (~4 hours)
./run_parallel_benchmark.sh full

# Check status of running benchmark
./run_parallel_benchmark.sh status

# Cancel running benchmark
./run_parallel_benchmark.sh cancel
```

**Features:**
- Launches 8 containers simultaneously (one per strategy)
- Reduces full benchmark from ~11 hours to ~2 hours
- **Reentrant**: Safe to disconnect and reconnect - running again waits for existing containers
- Automatically merges results, runs analysis, and generates visualizations

### Hyper-Parallel Execution (100+ Core Machines)

For very large machines (100+ cores, 1TB+ RAM), maximize parallelization by partitioning each strategy by JMH parameters:

```bash
cd benchmark/remapping-optimization

# Default settings (~30 min with 36 parallel containers)
./run_hyperparallel_benchmark.sh

# Quick test (~10 min)
./run_hyperparallel_benchmark.sh quick

# Full suite (~1-2 hours instead of 7-14 hours)
./run_hyperparallel_benchmark.sh full

# Check status
./run_hyperparallel_benchmark.sh status

# Cancel
./run_hyperparallel_benchmark.sh cancel
```

**Partitioning:**
- Heavy strategies (streamJoin, rangeQuery): 6 containers each (by sorted × numPositions)
- Light strategies (6 others): 4 containers each (by sorted × numPositions groups)
- Total: 36 containers

**Speedup:** ~6x over parallel, ~36x over serial for full suite.

### Single Container Execution

For simpler execution or resource-constrained environments:

```bash
cd benchmark/remapping-optimization

# Default benchmark (~45 min)
./run_docker_benchmark.sh

# Quick test (~15 min)
./run_docker_benchmark.sh quick

# Full suite (~2-3 hours)
./run_docker_benchmark.sh full

# Smart selector only
./run_docker_benchmark.sh selector

# Custom JMH arguments
./run_docker_benchmark.sh RemappingAlgorithmBenchmark.intervalTree -wi 2 -i 3 -f 1
```

### Local Execution (No Docker)

Run directly with Gradle:

```bash
# From repository root
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhArgs="-wi 3 -i 5 -f 2 -rf json -rff benchmark/remapping-optimization/results/results.json"
```

## Building the Docker Image

The scripts automatically build the image if needed. To build manually:

```bash
# From repository root
docker build -t iceberg-jmh-benchmark -f benchmark/remapping-optimization/Dockerfile .
```

The Dockerfile uses a multi-stage build:
1. **Builder stage**: Compiles `iceberg-core:jmhJar` with minimal dependencies
2. **Runtime stage**: JRE with the JMH jar and Python for analysis

Build time is ~5-10 minutes depending on network and cache state.

## Results

Results are written to the `results/` directory (bind-mounted from the container):

```
results/
├── results-merged-TIMESTAMP.json    # Combined results from parallel run
├── results-merged-TIMESTAMP.csv     # CSV for analysis
├── results-linearSearch-TIMESTAMP.json
├── results-binarySearch-TIMESTAMP.json
├── results-intervalTree-TIMESTAMP.json
├── results-streamJoin-TIMESTAMP.json
├── results-rangeQuery-TIMESTAMP.json
└── results-smartSelector-TIMESTAMP.json
```

Charts are generated in the script directory:
```
benchmark/remapping-optimization/
├── chart_strategy_comparison.png
├── chart_selector_overhead.png
└── chart_speedup_vs_linear.png
```

## Benchmark Parameters

The full benchmark suite tests 54 parameter combinations per strategy:

| Parameter | Values | Description |
|-----------|--------|-------------|
| `numRuns` | 10, 100, 1000 | Number of runs in compaction map (m) |
| `numPositions` | 1000, 10000, 100000 | Number of positions to remap (n) |
| `gapRatio` | 0.0, 0.3, 0.5 | Sparsity: dense, moderate, sparse |
| `sorted` | true, false | Whether positions are pre-sorted |

**Total**: 6 strategies × 54 combinations = 324 benchmark configurations

**Note:** Some parameter combinations are mathematically impossible (e.g., 100000 unique positions in a range of 2000). The benchmark automatically detects and skips these invalid combinations.

### Strategies

| Strategy | Complexity | Best For |
|----------|------------|----------|
| `linearSearch` | O(m) | Baseline only |
| `binarySearch` | O(log m) | Never optimal (kept for comparison) |
| `intervalTree` | O(log m) | Unsorted data |
| `streamJoin` | O(n + m) | Large sorted workloads |
| `rangeQuery` | O(m log n) | Sorted data with gaps |
| `smartSelector` | Varies | Automatic selection |

## Analysis

### Automatic Analysis

The parallel benchmark script runs analysis automatically. For manual analysis:

```bash
# Analyze JSON results
python3 analyze_results.py results/results-merged-TIMESTAMP.json

# Generate charts from CSV
python3 visualize_results.py results/results-merged-TIMESTAMP.csv
```

### Analysis Output

```
BENCHMARK SUMMARY
- Total scenarios: 54
- Total measurements: 324

OPTIMAL STRATEGY BY SCENARIO
intervalTree: 27 scenarios (unsorted data)
rangeQuery: 20 scenarios (sorted with gaps)
streamJoin: 7 scenarios (large sorted workloads)

SMART SELECTOR OVERHEAD
- Average overhead: <10% vs manually selecting optimal
- Overhead > 20% flagged for investigation
```

### Manual Analysis

```bash
# Pretty-print JSON
jq '.' results/results-merged-TIMESTAMP.json

# Find smartSelector results
jq '.[] | select(.benchmark | contains("smartSelector"))' results.json

# Compare strategies for specific parameters
jq '.[] | select(.params.numRuns == "100" and .params.numPositions == "10000")' results.json
```

## Key Findings

Based on empirical benchmarks (January 2026):

### Unsorted Data
- **Winner**: IntervalTree (wins ~85% of unsorted scenarios)
- **Speedup**: 2-7x over BinarySearch
- **Why**: O(log m) lookups without O(n log n) sorting overhead

### Sorted Data
- **Winner**: RangeQuery or StreamJoin
- **Speedup**: 1.5-3x over IntervalTree
- **Why**: Exploits sorted order for efficient scanning

### Smart Selector Logic

```
if unsorted:
    return IntervalTree

if gapRatio > 0.3:
    return RangeQuery      # Skip gaps efficiently

if m >= 100 and n >= 10000:
    return StreamJoin      # Bulk sorted workloads

return RangeQuery          # Default for sorted
```

## Troubleshooting

### Container Exits Immediately (Exit Code 137)

OOM killed. Reduce heap size or remove memory limits:

```bash
# Use smaller heap
docker run --rm -e JAVA_OPTS="-Xms2g -Xmx2g -XX:+UseG1GC" iceberg-jmh-benchmark
```

### High Error Margins (>20%)

Indicates noisy measurements. Use Docker for isolation:
- Close other applications
- Use AC power (not battery)
- Increase fork count: `-f 5`

### Build Failures

```bash
# Clean rebuild
docker build --no-cache -t iceberg-jmh-benchmark -f benchmark/remapping-optimization/Dockerfile .

# Or locally
./gradlew clean :iceberg-core:jmhJar -x test -x spotlessCheck
```

## JMH Parameters

| Flag | Default | Description |
|------|---------|-------------|
| `-wi` | 3 | Warmup iterations |
| `-i` | 5 | Measurement iterations |
| `-f` | 2 | Forks (separate JVM runs) |
| `-rf` | json | Result format |
| `-rff` | - | Result file path |

**Presets:**
- `quick`: `-wi 1 -i 2 -f 1` (~15 min single, ~20 min parallel)
- `default`: `-wi 3 -i 5 -f 2` (~45 min single, ~2 hours parallel)
- `full`: `-wi 5 -i 10 -f 3` (~3 hours single, ~4 hours parallel)

## References

- **Benchmark Implementation**: `core/src/jmh/java/org/apache/iceberg/RemappingAlgorithmBenchmark.java`
- **Strategy Implementations**: `core/src/main/java/org/apache/iceberg/*Strategy.java`
- **Smart Selector**: `core/src/main/java/org/apache/iceberg/RemappingAlgorithmSelector.java`
- **Detailed Documentation**: `REMAPPING_BENCHMARKS.md` (repository root)
