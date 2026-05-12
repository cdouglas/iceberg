---
title: "Compaction Maps - Benchmarking"
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Compaction Maps - Benchmarking

This document provides an overview of the benchmarking infrastructure for compaction map remapping operations.

## Benchmark Suites

Two complementary benchmark suites measure different aspects of remapping performance:

### 1. remapping-optimization (JMH)

**Purpose:** Compare remapping algorithm strategies in isolation (CPU-bound)

**Location:** `benchmark/remapping-optimization/`

**What it measures:**
- Pure algorithm performance without I/O
- Strategy comparison: LinearSearch, BinarySearch, IntervalTree, StreamJoin, RangeQuery
- Smart selector accuracy and overhead

**When to use:**
- Tuning algorithm selection thresholds
- Validating new strategy implementations
- Understanding scaling characteristics

**Quick start:**
```bash
cd benchmark/remapping-optimization

# Parallel execution (recommended, ~2 hours)
./run_parallel_benchmark.sh

# Quick test (~20 min)
./run_parallel_benchmark.sh quick

# Single container
./run_docker_benchmark.sh
```

See [`benchmark/remapping-optimization/README.md`](../../benchmark/remapping-optimization/README.md) for detailed documentation.

### 2. remapping-microbenchmark

**Purpose:** Measure end-to-end remapping cost including I/O (realistic workloads)

**Location:** `benchmark/remapping-microbenchmark/`

**What it measures:**
- Read phase: Loading position deletes or DVs from storage
- Remap phase: Applying compaction map transformations
- Write phase: Writing remapped deletes back to storage
- Cloud storage latency (S3, GCS, Azure)

**When to use:**
- Estimating real-world conflict resolution costs
- Comparing position delete files vs deletion vectors
- Understanding I/O vs CPU cost breakdown

**Quick start:**
```bash
cd benchmark/remapping-microbenchmark

# Local testing (~2 min)
./scripts/run-benchmark.sh configs/quick.yaml

# Cloud benchmarking
export BENCHMARK_S3_BUCKET=my-bucket
./scripts/run-cloud.sh aws configs/sigmod.yaml
```

See [`benchmark/remapping-microbenchmark/README.md`](../../benchmark/remapping-microbenchmark/README.md) for detailed documentation.

## Key Findings

Based on empirical benchmarks (January-February 2026):

### Algorithm Performance

| Condition | Best Strategy | Speedup vs Linear |
|-----------|---------------|-------------------|
| Unsorted data, any scale | IntervalTree | 2–264× (one outlier at 832× for n=1M, m=10K) |
| Sorted + sparse (gaps > 30%) | RangeQuery | 1.5–6× |
| Sorted + dense + large scale | StreamJoin | 3–138× |
| Small scale (n=1K) | Any optimized | 1.4–6.9× |

The high-end multipliers (≥100×) come from the Feb 2026 hyperparallel run, which extends the parameter sweep up to `n=1M, m=10K`. Earlier runs that capped at `n=100K` reported up to 32×.

### Smart Selector

- **Average overhead:** ~5% vs manually selecting optimal strategy.
- **Selection logic:** Four-branch decision tree on sortedness, gap ratio, and scale; see [implementation pseudocode](compaction_maps_impl_pseudocode.md).
- **Sorted-hint API:** `selectOptimal(mapping, positions, Boolean.TRUE)` skips the up-to-1000-element sortedness sample when the caller has structural knowledge (e.g., positions extracted from a Roaring bitmap are sorted by construction).
- **Validation:** 324 JMH configurations in the standard sweep, 72 in the hyperparallel sweep.

### Cloud Benchmark Results (May 12, 2026)

The data below is from `RemappingBenchmarkRunner` against `sigmod.yaml` on the `cmpmap` branch (commit `49cc6dcdb` family), pooling AWS (us-west-2, `m5.xlarge`), GCP (us-west1, `n2-standard-4`), and Azure (westus2, `Standard_D4s_v3`). The runner only exercises the `SMART` strategy through the public API, so each cloud produces 1600 records (4 delete counts × 4 run counts × 2 densities × 2 formats × 25 iterations); 3840 measurement records pooled after dropping warmups. Result files and regenerated plots are committed under [`benchmark/remapping-microbenchmark/results/`](../../benchmark/remapping-microbenchmark/results/).

**Pooled phase latency (all clouds, all scenarios):**

| Phase | V2 Position Delete median (mean) | V3 Deletion Vector median (mean) |
|-------|--------------------------------:|---------------------------------:|
| read  |  96 ms (165 ms) |  79 ms ( 80 ms) |
| remap | 1.8 ms ( 17 ms) | 5.0 ms ( 29 ms) |
| write | 161 ms (503 ms) |  95 ms (100 ms) |

See [`results/plots/latency_heatmap_pd.png`](../../benchmark/remapping-microbenchmark/results/plots/latency_heatmap_pd.png) and [`latency_heatmap_dv.png`](../../benchmark/remapping-microbenchmark/results/plots/latency_heatmap_dv.png) for per-cloud × delete-count × run-count breakdowns of the remap phase. Total-latency heatmaps live alongside as [`total_latency_heatmap_{pd,dv}.png`](../../benchmark/remapping-microbenchmark/results/plots/).

**End-to-end totals by scale (pooled, median of `read + remap + write`):**

| numDeletes | V2 PD total | V3 DV total | PD / DV |
|-----------:|------------:|------------:|--------:|
|       1,000 |   161 ms |   162 ms | 1.0× |
|      10,000 |   171 ms |   157 ms | 1.1× |
|     100,000 |   355 ms |   197 ms | 1.8× |
|   1,000,000 |  2006 ms |   313 ms | **6.4×** |

DV's compactness advantage only matters at scale: at 1K–10K deletes the two formats are tied, and DV's structural cost on the remap phase (bitmap reconstruction) is almost exactly cancelled by its smaller read/write. At 1M deletes the write-side gap dominates and DV is 6× faster end-to-end.

**Remap phase isolated by scale (where PD has the structural advantage):**

| numDeletes | V2 PD remap | V3 DV remap | DV / PD |
|-----------:|------------:|------------:|--------:|
|       1,000 |   134 µs |   390 µs | 2.9× |
|      10,000 |   630 µs |   829 µs | 1.3× |
|     100,000 |  5.93 ms |  6.69 ms | 1.1× |
|   1,000,000 |  59.6 ms |  75.5 ms | 1.3× |

The DV-over-PD remap penalty is real but small in absolute terms — at most ~15 ms median at 1M deletes — and is overwhelmed by DV's read/write savings on the same scenario.

**Caveats on run-to-run noise:**

Cloud-side variability dominates differences between runs of the same benchmark. As a concrete example, the prior baseline (Feb 3, 2026, preserved in commit `a6615d438` for reference) reported DV write at 1M deletes as 283 ms on GCP and 110 ms on Azure; the current run reports 144 ms and 69 ms for the same scenario. AWS in the same window moved barely (178 → 186 ms). Those swings track cloud-capacity fluctuations, not code changes.

The in-process bulk-construct optimization committed in May 2026 (`RoaringPositionBitmap.setAll(long[])`, `PositionDeleteIndex.delete(long[])`) was independently verified at 1.4–1.8× on `DVRemappingPhaseBenchmark`, but the speedup is on a small absolute component of an already-small phase and is not visible above the I/O noise floor at the cloud-benchmark scale.

**Takeaway for users:** treat the cloud-benchmark medians as order-of-magnitude indicators of relative format cost. Don't read a 10–20% shift in cloud-benchmark medians as a meaningful code-change signal — that resolution belongs to the JMH benchmarks under `core/src/jmh/`.

## Running Benchmarks

### Prerequisites

- Docker (for containerized execution)
- Python 3.8+ with matplotlib (for analysis/visualization)
- Cloud credentials (for cloud benchmarks)

### remapping-optimization

```bash
# Full suite with parallel containers
cd benchmark/remapping-optimization
./run_parallel_benchmark.sh

# Results in results/ directory
# Charts generated automatically
```

### remapping-microbenchmark

```bash
# Local quick test
cd benchmark/remapping-microbenchmark
./scripts/run-benchmark.sh configs/quick.yaml

# Analyze results
python3 scripts/analyze-results.py results/run_*/results.json -o report.md
```

## Benchmark Parameters

### remapping-optimization

| Parameter | Values | Description |
|-----------|--------|-------------|
| `numRuns` (m) | 10, 100, 1000 | Runs in compaction map |
| `numPositions` (n) | 1000, 10000, 100000 | Positions to remap |
| `gapRatio` | 0.0, 0.3, 0.5 | Sparsity of runs |
| `sorted` | true, false | Position ordering |

**Note:** Some parameter combinations are invalid (e.g., 100000 positions with only 10 runs at 10% coverage). The benchmark automatically skips these.

### remapping-microbenchmark

| Parameter | Values | Description |
|-----------|--------|-------------|
| `delete-counts` | 1K - 1M | Number of position deletes |
| `run-counts` | 10 - 10K | Runs in compaction map |
| `densities` | SPARSE, DENSE | Delete clustering pattern |
| `formats` | POSITION_DELETE_FILE, DELETION_VECTOR | V2/V3 delete format |

## Result Analysis

Both benchmark suites include Python analysis scripts:

```bash
# remapping-optimization
cd benchmark/remapping-optimization
python3 analyze_results.py results/results-merged-*.json
python3 visualize_results.py results/results-merged-*.csv

# remapping-microbenchmark
cd benchmark/remapping-microbenchmark
python3 scripts/analyze-results.py results/run_*/results.json --charts
```

## References

- [Compaction Maps Overview](compaction_maps.md)
- [Implementation Details](compaction_maps_impl.md)
- [remapping-optimization README](../../benchmark/remapping-optimization/README.md)
- [remapping-microbenchmark README](../../benchmark/remapping-microbenchmark/README.md)
