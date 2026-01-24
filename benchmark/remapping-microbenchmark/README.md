# Remapping Microbenchmark

This benchmark measures the cost of rebasing position deletes on top of a compaction commit, which occurs in conflict scenarios where:

1. A transaction with position deletes conflicts with a concurrent compaction
2. A compaction needs to rebase concurrent transaction deletes onto its compacted state

These scenarios are symmetric—the remapping algorithm is the same.

## Overview

The benchmark measures three phases of the remapping operation:

1. **Read**: Load position deletes (Parquet) or deletion vectors (Puffin) from storage
2. **Remap**: Apply compaction map to transform positions using various strategies
3. **Write**: Write remapped deletes back to storage

This captures the full cost of conflict resolution, including cloud storage latency.

## Delete Formats

- **Position Delete Files (V2/V3)**: Parquet files containing `(file_path, position)` tuples
- **Deletion Vectors (V3)**: Puffin files containing Roaring bitmaps referencing single data files

## Remapping Strategies

| Strategy | Complexity | Best For |
|----------|------------|----------|
| LINEAR | O(n×m) | Baseline comparison |
| BINARY_SEARCH | O(n log m) | Few positions, many runs |
| INTERVAL_TREE | O(n log m) | Unsorted data |
| STREAM_JOIN | O(n+m) | Sorted bulk operations |
| RANGE_QUERY | O(m log n) | Sparse deletes, many runs |
| SMART | Varies | Automatic selection |

Where `n` = number of positions, `m` = number of runs in compaction map.

## Quick Start

### Local Testing

```bash
# Run with quick configuration (1-2 minutes)
./scripts/run-benchmark.sh configs/quick.yaml

# Run with default configuration (10-15 minutes)
./scripts/run-benchmark.sh
```

### Cloud Benchmarking

```bash
# AWS S3
export BENCHMARK_S3_BUCKET=my-benchmark-bucket
./scripts/run-cloud.sh aws configs/sigmod.yaml

# Google Cloud Storage
export BENCHMARK_GCS_BUCKET=my-benchmark-bucket
./scripts/run-cloud.sh gcp configs/sigmod.yaml

# Azure Blob Storage
export BENCHMARK_AZURE_CONTAINER=my-container
export AZURE_STORAGE_ACCOUNT=mystorageaccount
./scripts/run-cloud.sh azure configs/sigmod.yaml
```

### Analyzing Results

```bash
# Generate Markdown report
python3 scripts/analyze-results.py results/run_*/results.json -o report.md

# Generate charts (requires matplotlib)
python3 scripts/analyze-results.py results/run_*/results.json --charts

# Output raw analysis as JSON
python3 scripts/analyze-results.py results/run_*/results.json --json
```

## Configuration

Configuration is specified via YAML files. See `configs/` for examples.

### Key Parameters

```yaml
# Delete counts to test (number of position deletes)
delete-counts:
  - 1000
  - 10000
  - 100000
  - 1000000

# Run counts to test (number of runs in compaction map)
run-counts:
  - 10
  - 100
  - 1000
  - 10000

# Density patterns
densities:
  - SPARSE   # ~1% deleted, scattered positions
  - DENSE    # ~50% deleted, clustered positions

# Delete file formats
formats:
  - POSITION_DELETE_FILE  # V2/V3 Parquet files
  - DELETION_VECTOR       # V3 Puffin files

# Strategies to benchmark
strategies:
  - LINEAR
  - BINARY_SEARCH
  - INTERVAL_TREE
  - STREAM_JOIN
  - RANGE_QUERY
  - SMART
```

### Configurations

| Config | Purpose | Duration |
|--------|---------|----------|
| `quick.yaml` | Verify benchmark works | 1-2 min |
| `default.yaml` | Local development testing | 10-15 min |
| `sigmod.yaml` | SIGMOD-ready results | 4-6 hours |

## Output

Results are saved to the output directory:

```
results/run_YYYYMMDD_HHMMSS/
├── config.yaml      # Configuration used
├── results.json     # Raw measurements
└── summary.json     # Aggregated statistics
```

### Result Format

Each measurement includes:

```json
{
  "format": "POSITION_DELETE_FILE",
  "density": "SPARSE",
  "strategy": "INTERVAL_TREE",
  "num-deletes": 10000,
  "num-runs": 100,
  "read-latency-ns": 12345678,
  "remap-latency-ns": 1234567,
  "write-latency-ns": 23456789,
  "total-latency-ns": 37036034,
  "input-size-bytes": 102400,
  "output-size-bytes": 98304,
  "iteration": 0,
  "warmup": false
}
```

## Building

```bash
# Build the benchmark module
./gradlew :benchmark:remapping-microbenchmark:build

# Create fat JAR for distribution
./gradlew :benchmark:remapping-microbenchmark:fatJar
```

## Architecture

```
remapping-microbenchmark/
├── src/main/java/org/apache/iceberg/benchmark/remapping/
│   ├── RemappingMicrobenchmark.java    # Main entry point
│   ├── RemappingBenchmarkRunner.java   # Benchmark execution
│   ├── BenchmarkConfig.java            # Configuration
│   ├── generators/
│   │   ├── PositionDeleteGenerator.java  # Generate Parquet deletes
│   │   ├── DeletionVectorGenerator.java  # Generate Puffin DVs
│   │   └── CompactionMapGenerator.java   # Generate compaction maps
│   └── metrics/
│       └── BenchmarkMetrics.java       # Metrics collection
├── configs/
│   ├── default.yaml                    # Default configuration
│   ├── quick.yaml                      # Quick test configuration
│   └── sigmod.yaml                     # SIGMOD-ready configuration
├── scripts/
│   ├── run-benchmark.sh                # Local execution
│   ├── run-cloud.sh                    # Cloud execution
│   └── analyze-results.py              # Results analysis
└── README.md                           # This file
```

## Key Findings

From empirical benchmarks (January 2026):

- **UNSORTED data**: IntervalTree wins in most scenarios regardless of scale
- **SORTED data**: RangeQuery or StreamJoin optimal depending on gap ratio
- **Cloud I/O**: Read and write phases dominate; remap CPU time is typically <10%
- **Deletion Vectors**: More efficient than position delete files for dense deletes
- **Smart Selector**: <5% overhead compared to optimal strategy selection

## Dependencies

- Apache Iceberg core (compaction map APIs)
- Apache Parquet (position delete files)
- Roaring Bitmaps (deletion vectors)
- Hadoop FileSystem (cloud storage access)
- Jackson (YAML/JSON parsing)

## License

Licensed under the Apache License, Version 2.0.
