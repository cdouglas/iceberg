# Plotting Errata

This document describes issues with the visualization code in `scripts/plot_results.py` that produced misleading plots, and the fixes applied.

## Benchmark Overview

The remapping microbenchmark measures the end-to-end latency of remapping position deletes when data files are compacted. Each benchmark iteration performs three phases: **read** (load positions from a delete file), **remap** (apply compaction map to translate positions), and **write** (output remapped positions to new delete files). The benchmark runs across three cloud providers (AWS S3, GCP Cloud Storage, Azure Blob) to measure I/O latency variation.

The benchmark explores a parameter space with four primary dimensions: **delete count** (n = 1K, 10K, 100K, 1M positions), **run count** (m = 10, 100, 1K, 10K runs in the compaction map), **density** (sparse vs dense position distribution), and **format** (position delete files vs deletion vectors). Additional parameters include fanout factor (2, 10, 50, 100) and split factor (1, 2, 5, 10) for multi-target mapping scenarios. Each configuration runs 5 warmup iterations followed by 20 measurement iterations.

The run count parameter (m) is critical because remapping algorithm performance scales differently with m depending on the strategy: LinearSearch is O(n*m), BinarySearch is O(n*log(m)), IntervalTree is O(n*log(m)), StreamJoin is O(n+m), and RangeQuery is O(m*log(n)). Averaging latencies across different run counts (e.g., m=10 and m=10000) produces meaningless results since the remapping phase dominates at high m but is negligible at low m.

## Removed Plots

The following plots were removed because they averaged across run counts, making their results statistically meaningless:

| Plot Function | File | Problem |
|---------------|------|---------|
| `plot_combined_latency_breakdown` | combined_latency_breakdown.png | Averaged across ALL parameters (n, m, density) |
| `plot_compact_latency_breakdown` | latency_breakdown_compact.png | Averaged across ALL parameters (n, m, density) |
| `plot_compact_cloud_comparison` | cloud_comparison_compact.png | Averaged across m |
| `plot_latency_breakdown_1m` | latency_breakdown_1m.png | Averaged across m=10..10000 |
| `plot_latency_breakdown` | latency_breakdown.png | Averaged across n and m |
| `plot_throughput_comparison` | throughput_comparison.png | Averaged across m |
| `plot_scaling` | scaling.png | Error bars conflated run count variation with noise |
| `plot_format_comparison` | format_comparison.png | Averaged across ALL parameters |
| `plot_boxplots` | latency_boxplots.png | Mixed incomparable distributions |
| `plot_cloud_comparison` | cloud_comparison.png | Averaged across m |
| `generate_summary_table` | summary_table.png | Averaged across ALL parameters |

## Current Plots

All plots are stratified by run count to avoid meaningless averaging:

| Function | Output | Description |
|----------|--------|-------------|
| `plot_cloud_comparison_by_runs` | cloud_comparison_m{10,100,1k,10k}.png | Latency by delete count, separate plot per m |
| `plot_latency_breakdown_by_runs` | latency_breakdown_1m_m{10,100,1k,10k}.png | Read/remap/write breakdown per m (1M deletes) |
| `plot_latency_heatmap` | latency_heatmap_{pd,dv}.png | Heatmap of n × m with common scale |

## Generated Files

After running `python scripts/plot_results.py`:

```
results/plots/
├── cloud_comparison_m10.png/pdf      # m=10 runs
├── cloud_comparison_m100.png/pdf     # m=100 runs
├── cloud_comparison_m1k.png/pdf      # m=1000 runs
├── cloud_comparison_m10k.png/pdf     # m=10000 runs
├── latency_breakdown_1m_m10.png/pdf  # 1M deletes, m=10
├── latency_breakdown_1m_m100.png/pdf # 1M deletes, m=100
├── latency_breakdown_1m_m1k.png/pdf  # 1M deletes, m=1000
├── latency_breakdown_1m_m10k.png/pdf # 1M deletes, m=10000
├── latency_heatmap_pd.png/pdf        # Position deletes heatmap
└── latency_heatmap_dv.png/pdf        # Deletion vectors heatmap
```

## Recommended Usage

For meaningful analysis:

1. **Use stratified plots** (`_by_runs` variants) to see how latency varies with run count
2. **Use heatmaps** to visualize the full n × m parameter space
3. **Compare across m values** to understand algorithm scaling behavior

The heatmap is particularly useful because it shows:
- How remap latency scales with both n and m
- Which (n, m) combinations are performance-critical
- Differences between cloud providers on identical workloads
