#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""
Analyze compaction cloud benchmark results.

Usage:
    python analyze-results.py <results_dir>
    python analyze-results.py --compare <dir1> <dir2>

Dependencies:
    pip install pyyaml matplotlib pandas (optional for charts)
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False
    print("Note: Install pyyaml for YAML support (pip install pyyaml)")

try:
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("Note: Install matplotlib for charts (pip install matplotlib)")


def load_json(path: Path) -> Dict:
    """Load JSON file."""
    with open(path) as f:
        return json.load(f)


def load_yaml(path: Path) -> Dict:
    """Load YAML file."""
    if not HAS_YAML:
        return {}
    with open(path) as f:
        return yaml.safe_load(f)


def find_latest_files(results_dir: Path) -> tuple:
    """Find the latest statistics and config files."""
    stats_files = sorted(results_dir.glob("statistics_*.json"))
    config_files = sorted(results_dir.glob("config_*.yaml"))

    stats_file = stats_files[-1] if stats_files else None
    config_file = config_files[-1] if config_files else None

    return stats_file, config_file


def analyze_single(results_dir: Path) -> Dict:
    """Analyze a single benchmark run."""
    stats_file, config_file = find_latest_files(results_dir)

    if not stats_file:
        print(f"No statistics file found in {results_dir}")
        return {}

    stats = load_json(stats_file)
    config = load_yaml(config_file) if config_file else {}

    return {
        "stats": stats,
        "config": config,
        "dir": str(results_dir)
    }


def print_summary(data: Dict) -> None:
    """Print a summary of the benchmark results."""
    stats = data.get("stats", {})
    config = data.get("config", {})

    print("\n" + "=" * 65)
    print("           Benchmark Analysis Summary")
    print("=" * 65)

    # Config info
    if config:
        print(f"\nConfiguration:")
        print(f"  Compaction Maps:      {config.get('compaction-maps-enabled', 'N/A')}")
        print(f"  Format Version:       {config.get('format-version', 'N/A')}")
        print(f"  Iterations:           {config.get('num-iterations', 'N/A')}")

    # Summary stats
    summary = stats.get("summary", {})
    print(f"\nTransaction Summary:")
    print(f"  Total Deletes:        {summary.get('total_deletes', 0):,}")
    print(f"  Successful:           {summary.get('successful_deletes', 0):,}")
    print(f"  Failed:               {summary.get('failed_deletes', 0):,}")
    print(f"  Conflict Rate:        {summary.get('conflict_rate', 0) * 100:.2f}%")
    print(f"  Remap Success Rate:   {summary.get('remap_success_rate', 0) * 100:.2f}%")

    print(f"\nCompaction Summary:")
    print(f"  Total Compactions:    {summary.get('total_compactions', 0):,}")
    print(f"  With Maps:            {summary.get('compactions_with_maps', 0):,}")

    # Latency stats
    latency = stats.get("latency", {})
    print(f"\nLatency Statistics:")
    print(f"  Avg Delete:           {latency.get('avg_delete_latency_ms', 0):.2f} ms")
    print(f"  P99 Delete:           {latency.get('p99_delete_latency_ms', 0):.2f} ms")
    print(f"  Avg Remap:            {latency.get('avg_remap_latency_ms', 0):.2f} ms")
    print(f"  P99 Remap:            {latency.get('p99_remap_latency_ms', 0):.2f} ms")

    # Map efficiency
    map_eff = stats.get("map_efficiency", {})
    print(f"\nMap Efficiency:")
    print(f"  Avg Map Size:         {map_eff.get('avg_map_size_kb', 0):.2f} KB")
    print(f"  Avg Run Count:        {map_eff.get('avg_run_count', 0):.1f}")
    print(f"  Avg Files/Compaction: {map_eff.get('avg_files_per_compaction', 0):.1f}")

    # Strategy metrics
    strategies = stats.get("strategy_metrics", {})
    if strategies:
        print(f"\nStrategy Usage:")
        for name, metrics in strategies.items():
            print(f"  {name}: {metrics.get('count', 0):,} uses, {metrics.get('avg_latency_ms', 0):.2f} ms avg")

    print("\n" + "=" * 65)


def compare_runs(dir1: Path, dir2: Path) -> None:
    """Compare two benchmark runs."""
    data1 = analyze_single(dir1)
    data2 = analyze_single(dir2)

    if not data1.get("stats") or not data2.get("stats"):
        print("Cannot compare: missing data")
        return

    stats1 = data1["stats"]
    stats2 = data2["stats"]
    config1 = data1.get("config", {})
    config2 = data2.get("config", {})

    print("\n" + "=" * 65)
    print("           Benchmark Comparison")
    print("=" * 65)

    # Determine which has maps enabled
    maps1 = config1.get("compaction-maps-enabled", True)
    maps2 = config2.get("compaction-maps-enabled", True)

    label1 = "With Maps" if maps1 else "Without Maps"
    label2 = "With Maps" if maps2 else "Without Maps"

    print(f"\n{'Metric':<30} {label1:>15} {label2:>15} {'Diff':>12}")
    print("-" * 72)

    # Compare key metrics
    def compare_metric(name: str, path: List[str], fmt: str = ".2f", pct: bool = False) -> None:
        v1 = stats1
        v2 = stats2
        for p in path:
            v1 = v1.get(p, {}) if isinstance(v1, dict) else 0
            v2 = v2.get(p, {}) if isinstance(v2, dict) else 0

        if pct:
            v1 = v1 * 100 if v1 else 0
            v2 = v2 * 100 if v2 else 0

        diff = v2 - v1 if v1 and v2 else 0
        diff_pct = (diff / v1 * 100) if v1 else 0

        fmt_str = f"{{:{fmt}}}"
        v1_str = fmt_str.format(v1) if v1 else "N/A"
        v2_str = fmt_str.format(v2) if v2 else "N/A"
        diff_str = f"{diff_pct:+.1f}%" if v1 else "N/A"

        unit = "%" if pct else ""
        print(f"{name:<30} {v1_str:>14}{unit} {v2_str:>14}{unit} {diff_str:>12}")

    compare_metric("Total Deletes", ["summary", "total_deletes"], ",d")
    compare_metric("Conflict Rate", ["summary", "conflict_rate"], ".2f", pct=True)
    compare_metric("Remap Success Rate", ["summary", "remap_success_rate"], ".2f", pct=True)
    compare_metric("Avg Delete Latency (ms)", ["latency", "avg_delete_latency_ms"])
    compare_metric("P99 Delete Latency (ms)", ["latency", "p99_delete_latency_ms"])
    compare_metric("Avg Remap Latency (ms)", ["latency", "avg_remap_latency_ms"])
    compare_metric("Avg Map Size (KB)", ["map_efficiency", "avg_map_size_kb"])

    print("\n" + "=" * 65)


def generate_charts(data: Dict, output_dir: Path) -> None:
    """Generate visualization charts."""
    if not HAS_MATPLOTLIB:
        print("Skipping charts: matplotlib not installed")
        return

    stats = data.get("stats", {})

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Chart 1: Latency distribution
    fig, ax = plt.subplots(figsize=(10, 6))
    latency = stats.get("latency", {})

    metrics = ["avg", "p50", "p95", "p99"]
    delete_values = [latency.get(f"{m}_delete_latency_ms", 0) for m in metrics]
    remap_values = [latency.get(f"{m}_remap_latency_ms", 0) for m in metrics]

    x = range(len(metrics))
    width = 0.35

    ax.bar([i - width/2 for i in x], delete_values, width, label="Delete Latency")
    ax.bar([i + width/2 for i in x], remap_values, width, label="Remap Latency")

    ax.set_ylabel("Latency (ms)")
    ax.set_title("Latency Distribution")
    ax.set_xticks(x)
    ax.set_xticklabels(["Average", "P50", "P95", "P99"])
    ax.legend()

    plt.tight_layout()
    plt.savefig(output_dir / "latency_distribution.png", dpi=150)
    plt.close()

    # Chart 2: Transaction outcomes
    fig, ax = plt.subplots(figsize=(8, 8))
    summary = stats.get("summary", {})

    successful = summary.get("successful_deletes", 0)
    failed = summary.get("failed_deletes", 0)
    conflicts = summary.get("conflict_rate", 0) * summary.get("total_deletes", 0)

    if successful + failed > 0:
        sizes = [successful, failed]
        labels = [f"Successful\n({successful:,})", f"Failed\n({failed:,})"]
        colors = ["#4CAF50", "#F44336"]

        ax.pie(sizes, labels=labels, colors=colors, autopct="%1.1f%%", startangle=90)
        ax.set_title("Transaction Outcomes")

        plt.tight_layout()
        plt.savefig(output_dir / "transaction_outcomes.png", dpi=150)
    plt.close()

    print(f"Charts saved to: {output_dir}")


def main():
    parser = argparse.ArgumentParser(description="Analyze compaction cloud benchmark results")
    parser.add_argument("results_dir", nargs="?", help="Results directory to analyze")
    parser.add_argument("--compare", nargs=2, metavar=("DIR1", "DIR2"),
                       help="Compare two result directories")
    parser.add_argument("--charts", action="store_true", help="Generate visualization charts")
    parser.add_argument("--output", help="Output directory for charts")

    args = parser.parse_args()

    if args.compare:
        compare_runs(Path(args.compare[0]), Path(args.compare[1]))
    elif args.results_dir:
        results_dir = Path(args.results_dir)
        data = analyze_single(results_dir)

        if data:
            print_summary(data)

            if args.charts:
                output_dir = Path(args.output) if args.output else results_dir / "charts"
                generate_charts(data, output_dir)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
