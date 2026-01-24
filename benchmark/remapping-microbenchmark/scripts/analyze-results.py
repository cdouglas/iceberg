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
Analyze remapping microbenchmark results.

Usage:
    python3 analyze-results.py results.json [--output report.md] [--charts]
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

try:
    import matplotlib.pyplot as plt
    import numpy as np
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


def load_results(path: str) -> list:
    """Load benchmark results from JSON file."""
    with open(path) as f:
        return json.load(f)


def filter_warmup(results: list) -> list:
    """Filter out warmup iterations."""
    return [r for r in results if not r.get('warmup', False)]


def group_by_scenario(results: list) -> dict:
    """Group results by scenario parameters."""
    groups = defaultdict(list)
    for r in results:
        key = (
            r['format'],
            r['density'],
            r['strategy'],
            r['num-deletes'],
            r['num-runs']
        )
        groups[key].append(r)
    return dict(groups)


def compute_stats(values: list) -> dict:
    """Compute summary statistics for a list of values."""
    if not values:
        return {'count': 0}

    sorted_vals = sorted(values)
    n = len(sorted_vals)

    return {
        'count': n,
        'mean': sum(values) / n,
        'min': min(values),
        'max': max(values),
        'p50': sorted_vals[n // 2],
        'p95': sorted_vals[int(n * 0.95)] if n >= 20 else sorted_vals[-1],
        'p99': sorted_vals[int(n * 0.99)] if n >= 100 else sorted_vals[-1],
    }


def analyze_results(results: list) -> dict:
    """Analyze benchmark results and compute summaries."""
    filtered = filter_warmup(results)
    groups = group_by_scenario(filtered)

    analysis = {
        'total_measurements': len(filtered),
        'total_scenarios': len(groups),
        'scenarios': {},
        'strategy_comparison': {},
        'format_comparison': {},
    }

    # Per-scenario statistics
    for key, group in groups.items():
        format_, density, strategy, num_deletes, num_runs = key
        latencies_ns = [r['total-latency-ns'] for r in group]
        latencies_ms = [ns / 1_000_000 for ns in latencies_ns]

        scenario_key = f"{format_}_{density}_{strategy}_d{num_deletes}_r{num_runs}"
        analysis['scenarios'][scenario_key] = {
            'format': format_,
            'density': density,
            'strategy': strategy,
            'num_deletes': num_deletes,
            'num_runs': num_runs,
            'latency_ms': compute_stats(latencies_ms),
            'read_pct': compute_stats([100 * r['read-latency-ns'] / r['total-latency-ns'] for r in group]),
            'remap_pct': compute_stats([100 * r['remap-latency-ns'] / r['total-latency-ns'] for r in group]),
            'write_pct': compute_stats([100 * r['write-latency-ns'] / r['total-latency-ns'] for r in group]),
        }

    # Strategy comparison (averaged across configurations)
    by_strategy = defaultdict(list)
    for key, group in groups.items():
        _, _, strategy, _, _ = key
        for r in group:
            by_strategy[strategy].append(r['total-latency-ns'] / 1_000_000)

    for strategy, latencies in by_strategy.items():
        analysis['strategy_comparison'][strategy] = compute_stats(latencies)

    # Format comparison
    by_format = defaultdict(list)
    for key, group in groups.items():
        format_, _, _, _, _ = key
        for r in group:
            by_format[format_].append(r['total-latency-ns'] / 1_000_000)

    for format_, latencies in by_format.items():
        analysis['format_comparison'][format_] = compute_stats(latencies)

    return analysis


def generate_markdown_report(analysis: dict, output_path: str = None):
    """Generate a Markdown report from the analysis."""
    lines = [
        "# Remapping Microbenchmark Results",
        "",
        f"Total measurements: {analysis['total_measurements']}",
        f"Total scenarios: {analysis['total_scenarios']}",
        "",
        "## Strategy Comparison",
        "",
        "| Strategy | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |",
        "|----------|-----------|----------|----------|----------|",
    ]

    for strategy in ['LINEAR', 'BINARY_SEARCH', 'INTERVAL_TREE', 'STREAM_JOIN', 'RANGE_QUERY', 'SMART']:
        stats = analysis['strategy_comparison'].get(strategy, {})
        if stats:
            lines.append(f"| {strategy} | {stats['mean']:.2f} | {stats['p50']:.2f} | {stats['p95']:.2f} | {stats['p99']:.2f} |")

    lines.extend([
        "",
        "## Format Comparison",
        "",
        "| Format | Mean (ms) | P50 (ms) | P95 (ms) |",
        "|--------|-----------|----------|----------|",
    ])

    for format_ in ['POSITION_DELETE_FILE', 'DELETION_VECTOR']:
        stats = analysis['format_comparison'].get(format_, {})
        if stats:
            lines.append(f"| {format_} | {stats['mean']:.2f} | {stats['p50']:.2f} | {stats['p95']:.2f} |")

    lines.extend([
        "",
        "## Phase Breakdown (Average)",
        "",
        "| Scenario | Read % | Remap % | Write % |",
        "|----------|--------|---------|---------|",
    ])

    # Show a few representative scenarios
    shown = 0
    for key, data in sorted(analysis['scenarios'].items()):
        if shown >= 10:
            break
        read_pct = data['read_pct']['mean']
        remap_pct = data['remap_pct']['mean']
        write_pct = data['write_pct']['mean']
        lines.append(f"| {key[:50]} | {read_pct:.1f} | {remap_pct:.1f} | {write_pct:.1f} |")
        shown += 1

    report = "\n".join(lines)

    if output_path:
        with open(output_path, 'w') as f:
            f.write(report)
        print(f"Report written to: {output_path}")
    else:
        print(report)

    return report


def generate_charts(analysis: dict, output_dir: str):
    """Generate visualization charts (requires matplotlib)."""
    if not HAS_MATPLOTLIB:
        print("Warning: matplotlib not available, skipping charts")
        return

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Strategy comparison bar chart
    strategies = ['LINEAR', 'BINARY_SEARCH', 'INTERVAL_TREE', 'STREAM_JOIN', 'RANGE_QUERY', 'SMART']
    means = [analysis['strategy_comparison'].get(s, {}).get('mean', 0) for s in strategies]

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(strategies, means, color=['#4e79a7', '#f28e2b', '#e15759', '#76b7b2', '#59a14f', '#edc948'])
    ax.set_ylabel('Mean Latency (ms)')
    ax.set_xlabel('Strategy')
    ax.set_title('Remapping Strategy Comparison')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(output_path / 'strategy_comparison.png', dpi=150)
    plt.close()

    # Format comparison
    formats = ['POSITION_DELETE_FILE', 'DELETION_VECTOR']
    format_means = [analysis['format_comparison'].get(f, {}).get('mean', 0) for f in formats]

    fig, ax = plt.subplots(figsize=(8, 6))
    bars = ax.bar(formats, format_means, color=['#4e79a7', '#e15759'])
    ax.set_ylabel('Mean Latency (ms)')
    ax.set_title('Delete Format Comparison')
    plt.tight_layout()
    plt.savefig(output_path / 'format_comparison.png', dpi=150)
    plt.close()

    # Phase breakdown pie chart
    read_pcts = []
    remap_pcts = []
    write_pcts = []

    for data in analysis['scenarios'].values():
        read_pcts.append(data['read_pct']['mean'])
        remap_pcts.append(data['remap_pct']['mean'])
        write_pcts.append(data['write_pct']['mean'])

    avg_read = sum(read_pcts) / len(read_pcts) if read_pcts else 0
    avg_remap = sum(remap_pcts) / len(remap_pcts) if remap_pcts else 0
    avg_write = sum(write_pcts) / len(write_pcts) if write_pcts else 0

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.pie([avg_read, avg_remap, avg_write],
           labels=['Read', 'Remap', 'Write'],
           autopct='%1.1f%%',
           colors=['#4e79a7', '#f28e2b', '#e15759'])
    ax.set_title('Average Phase Breakdown')
    plt.tight_layout()
    plt.savefig(output_path / 'phase_breakdown.png', dpi=150)
    plt.close()

    print(f"Charts saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Analyze remapping microbenchmark results')
    parser.add_argument('results', help='Path to results.json file')
    parser.add_argument('--output', '-o', help='Output path for Markdown report')
    parser.add_argument('--charts', '-c', action='store_true', help='Generate visualization charts')
    parser.add_argument('--charts-dir', default='charts', help='Directory for chart output')
    parser.add_argument('--json', action='store_true', help='Output raw analysis as JSON')

    args = parser.parse_args()

    # Load and analyze results
    results = load_results(args.results)
    analysis = analyze_results(results)

    if args.json:
        print(json.dumps(analysis, indent=2))
        return

    # Generate report
    generate_markdown_report(analysis, args.output)

    # Generate charts if requested
    if args.charts:
        generate_charts(analysis, args.charts_dir)


if __name__ == '__main__':
    main()
