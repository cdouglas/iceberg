# Plotting Errata

This document describes issues with the visualization code in `scripts/plot_results.py` that produced misleading plots, and the fixes applied.

## Benchmark Overview

The remapping microbenchmark measures the end-to-end latency of remapping position deletes when data files are compacted. Each benchmark iteration performs three phases: **read** (load positions from a delete file), **remap** (apply compaction map to translate positions), and **write** (output remapped positions to new delete files). The benchmark runs across three cloud providers (AWS S3, GCP Cloud Storage, Azure Blob) to measure I/O latency variation.

The benchmark explores a parameter space with four primary dimensions: **delete count** (p = 1K, 10K, 100K, 1M positions), **run count** (r = 10, 100, 1K, 10K runs in the compaction map), **density** (sparse vs dense position distribution), and **format** (position delete files vs deletion vectors). Additional parameters include fanout factor (2, 10, 50, 100) and split factor (1, 2, 5, 10) for multi-target mapping scenarios. Each configuration runs 5 warmup iterations followed by 20 measurement iterations.

The run count parameter (r) is critical because remapping algorithm performance scales differently with r depending on the strategy: LinearSearch is O(p*r), BinarySearch is O(p*log(r)), IntervalTree is O(p*log(r)), StreamJoin is O(p+r), and RangeQuery is O(r*log(p)). Averaging latencies across different run counts (e.g., r=10 and r=10000) produces meaningless results since the remapping phase dominates at high r but is negligible at low r.

## Removed Plots

The following plots were removed because they averaged across run counts, making their results statistically meaningless:

| Plot Function | File | Problem |
|---------------|------|---------|
| `plot_combined_latency_breakdown` | combined_latency_breakdown.png | Averaged across ALL parameters (p, r, density) |
| `plot_compact_latency_breakdown` | latency_breakdown_compact.png | Averaged across ALL parameters (p, r, density) |
| `plot_compact_cloud_comparison` | cloud_comparison_compact.png | Averaged across r |
| `plot_latency_breakdown_1m` | latency_breakdown_1m.png | Averaged across r=10..10000 |
| `plot_latency_breakdown` | latency_breakdown.png | Averaged across p and r |
| `plot_throughput_comparison` | throughput_comparison.png | Averaged across r |
| `plot_scaling` | scaling.png | Error bars conflated run count variation with noise |
| `plot_format_comparison` | format_comparison.png | Averaged across ALL parameters |
| `plot_boxplots` | latency_boxplots.png | Mixed incomparable distributions |
| `plot_cloud_comparison` | cloud_comparison.png | Averaged across r |
| `generate_summary_table` | summary_table.png | Averaged across ALL parameters |

## Current Plots

All plots are stratified by run count to avoid meaningless averaging:

| Function | Output | Description |
|----------|--------|-------------|
| `plot_cloud_comparison_by_runs` | cloud_comparison_r{10,100,1000,10000}.png | Latency by delete count, separate plot per r |
| `plot_cloud_comparison_by_runs_clipped` | cloud_comparison_r*_detail.png | Broken-axis variant; bottom panel sized for non-outlier bars, top panel shows outlier bar tops with annotated latency |
| `plot_latency_breakdown_by_runs` | latency_breakdown_1m_r{10,100,1000,10000}.png | Read/remap/write breakdown per r (1M deletes) |
| `plot_latency_heatmap` | latency_heatmap_{pd,dv}.png | Remap-only latency heatmap of p × r |
| `plot_latency_heatmap` | total_latency_heatmap_{pd,dv}.png | Total latency (read+remap+write) heatmap of p × r |

## Generated Files

After running `python scripts/plot_results.py`:

```
results/plots/
├── cloud_comparison_r10.png/pdf        # r=10 runs
├── cloud_comparison_r100.png/pdf       # r=100 runs
├── cloud_comparison_r1000.png/pdf      # r=1000 runs
├── cloud_comparison_r10000.png/pdf     # r=10000 runs
├── cloud_comparison_r*_detail.png/pdf  # Broken-axis variant (only when outliers exist)
├── latency_breakdown_1m_r10.png/pdf    # 1M deletes, r=10
├── latency_breakdown_1m_r100.png/pdf   # 1M deletes, r=100
├── latency_breakdown_1m_r1000.png/pdf  # 1M deletes, r=1000
├── latency_breakdown_1m_r10000.png/pdf # 1M deletes, r=10000
├── latency_heatmap_pd.png/pdf          # Remap-only latency heatmap (position deletes)
├── latency_heatmap_dv.png/pdf          # Remap-only latency heatmap (deletion vectors)
├── total_latency_heatmap_pd.png/pdf    # Total latency heatmap (position deletes)
└── total_latency_heatmap_dv.png/pdf    # Total latency heatmap (deletion vectors)
```

## Recommended Usage

For meaningful analysis:

1. **Use stratified plots** (`_by_runs` variants) to see how latency varies with run count
2. **Use heatmaps** to visualize the full p × r parameter space
3. **Compare across r values** to understand algorithm scaling behavior

The heatmap is particularly useful because it shows:
- How remap latency scales with both p and r
- Which (p, r) combinations are performance-critical
- Differences between cloud providers on identical workloads
