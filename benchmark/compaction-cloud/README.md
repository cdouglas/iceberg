# Compaction Cloud Benchmark

A synthetic benchmark framework for evaluating compaction map efficacy under realistic concurrent workloads in Apache Iceberg.

## Overview

This benchmark evaluates how compaction maps help resolve conflicts between concurrent position delete transactions and compaction operations. It uses metadata-only simulation to test at scale without requiring actual data files or storage I/O.

### Key Features

- **Metadata-only simulation**: Row counts encoded in metadata, no actual data files
- **Deterministic reproduction**: Support for both trace replay and seeded random generation
- **Realistic conflicts**: Transactions commit to create real compaction conflicts
- **Configurable parameters**: Workload shape, concurrency, conflict rates
- **Comprehensive statistics**: Conflict rates, remapping overhead, map sizes

## Quick Start

### Running Locally

```bash
# Run with defaults
./scripts/run-benchmark.sh

# Run with custom config
./scripts/run-benchmark.sh --config src/main/resources/configs/default.yaml

# Run comparison (with and without maps)
./scripts/run-benchmark.sh --output-dir results-with-maps
./scripts/run-benchmark.sh --no-maps --output-dir results-no-maps
```

### Running on Cloud Storage

```bash
# AWS S3
./scripts/run-cloud.sh aws

# Google Cloud Storage
./scripts/run-cloud.sh gcp

# Azure Blob Storage
./scripts/run-cloud.sh azure
```

### Analyzing Results

```bash
# Analyze single run
python scripts/analyze-results.py results/20260122_120000/

# Compare two runs
python scripts/analyze-results.py --compare results-with-maps results-no-maps

# Generate charts
python scripts/analyze-results.py results/ --charts
```

## Configuration

### Default Configuration

```yaml
# Table configuration
table-location: benchmark-tables
format-version: 2
compaction-maps-enabled: true

# Workload configuration
workload-mode: RANDOM    # or TRACE_REPLAY
random-seed: 42

# Simulation parameters
base-row-count: 1000000
merge-on-read-penalty: 0.1
compaction-speedup: 10.0
rows-per-ms: 10000

# Execution parameters
num-iterations: 10
concurrent-writers: 4
concurrent-compactors: 1

# Random workload parameters
num-files: 100
avg-rows-per-file: 100000
row-count-variance: 0.2
delete-selectivity: 0.001
compaction-frequency: 0.1
conflict-probability: 0.3

# Statistics
collect-detailed-stats: true
output-dir: benchmark-results
```

### Workload Modes

#### Random Mode (default)

Generates random workload events based on configuration parameters:
- Initial data load
- Periodic delete operations with configurable selectivity
- Compaction operations at configured frequency
- Concurrent deletes targeting compacting files

#### Trace Replay Mode

Replays events from a YAML trace file for reproducible testing:

```yaml
name: "TPC-H-like workload"
events:
  - type: INITIAL_LOAD
    timestamp: 0
    table: benchmark

  - type: DELETE_ROWS
    timestamp: 1000
    table: benchmark
    selectivity: 0.001
    pattern: RANDOM

  - type: COMPACTION
    timestamp: 2000
    table: benchmark
    file_count: 10

  - type: CONCURRENT_DELETE
    timestamp: 2000
    table: benchmark
    selectivity: 0.0005
    target: COMPACTING
```

## Architecture

```
benchmark/compaction-cloud/
├── src/main/java/org/apache/iceberg/benchmark/cloud/
│   ├── CompactionCloudBenchmark.java      # Main entry point
│   ├── WorkloadGenerator.java             # Workload generation
│   ├── SimulatedTable.java                # Metadata-only table wrapper
│   ├── SimulatedDataFile.java             # Fake data file factory
│   ├── SimulatedDeleteFile.java           # Fake delete file factory
│   ├── TransactionSimulator.java          # Transaction execution
│   ├── CompactionSimulator.java           # Compaction execution
│   ├── ConflictStatistics.java            # Statistics aggregation
│   ├── config/
│   │   ├── BenchmarkConfig.java           # Configuration POJO
│   │   ├── WorkloadConfig.java            # Workload parameters
│   │   └── TraceParser.java               # Trace file parser
│   └── metrics/
│       ├── MetricsCollector.java          # Real-time metrics
│       └── BenchmarkReport.java           # Report generation
├── src/main/resources/
│   ├── traces/                            # Sample trace files
│   │   ├── tpc-h-like.yaml
│   │   └── high-conflict.yaml
│   └── configs/
│       └── default.yaml
├── scripts/
│   ├── run-benchmark.sh                   # Local execution
│   ├── run-cloud.sh                       # Cloud deployment
│   └── analyze-results.py                 # Results analysis
└── README.md
```

## Metrics Collected

### Conflict Metrics
- Total transactions, conflicts, successful remaps
- Conflict rate over time
- Conflicts by file count, row count

### Performance Metrics
- Delete latency (avg, p50, p95, p99)
- Remap latency distribution
- Compaction map generation time
- Strategy selection distribution

### Efficiency Metrics
- Compaction map size vs row count
- Run count vs file count
- Map compression ratio

## Sample Trace Files

### TPC-H-like (`traces/tpc-h-like.yaml`)

Simulates TPC-H style analytics with periodic batch updates:
- Initial data load (60 files, 6M rows)
- Daily batch deletes (0.1% selectivity)
- Periodic compaction with concurrent conflicts

### High Conflict (`traces/high-conflict.yaml`)

Stress test for conflict handling:
- Multiple concurrent delete transactions
- Frequent compaction with many concurrent conflicts
- Tests remapping performance under load

## Output Files

After running, the output directory contains:

```
benchmark-results/20260122_120000/
├── statistics_20260122_120000.json    # JSON metrics
├── config_20260122_120000.yaml        # Configuration used
├── report_20260122_120000.txt         # Human-readable report
└── charts/                            # Visualization (if generated)
    ├── latency_distribution.png
    └── transaction_outcomes.png
```

## Dependencies

### Required
- Java 17+
- Iceberg core libraries

### Optional
- Python 3.8+ with pyyaml, matplotlib (for analysis)
- AWS CLI / gcloud / az CLI (for cloud runs)

## Building

```bash
# From Iceberg root
./gradlew :iceberg-benchmark-compaction-cloud:build

# Create shadow JAR
./gradlew :iceberg-benchmark-compaction-cloud:shadowJar
```

## Example Results

```
═══════════════════════════════════════════════════════════════
           Benchmark Complete
═══════════════════════════════════════════════════════════════
Elapsed Time:         45.23 seconds
Total Deletes:        100
Conflict Rate:        12.00%
Remap Success Rate:   100.00%
Total Compactions:    10
Avg Delete Latency:   15.67 ms
Avg Remap Latency:    2.34 ms
═══════════════════════════════════════════════════════════════
```

## Contributing

See the main Iceberg contribution guidelines. This benchmark is part of the compaction maps feature implementation on the `cmpmap` branch.
