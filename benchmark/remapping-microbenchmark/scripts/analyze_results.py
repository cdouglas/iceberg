#!/usr/bin/env python3
"""
Analyze remapping benchmark results from multiple cloud providers.

Usage:
    python analyze_results.py [results_dir]

    results_dir: Directory containing cloud result subdirectories (aws_*, gcp_*, azure_*)
                 Default: ../results
"""

import json
import sys
import os
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any
import statistics


def load_results(results_dir: Path) -> Dict[str, List[Dict]]:
    """Load results from all cloud providers."""
    all_results = {}

    for cloud_dir in results_dir.iterdir():
        if not cloud_dir.is_dir():
            continue

        cloud_name = cloud_dir.name.split('_')[0]  # aws, gcp, azure

        # Find results.json files
        for results_file in cloud_dir.rglob('results.json'):
            if cloud_name not in all_results:
                all_results[cloud_name] = []

            with open(results_file) as f:
                data = json.load(f)
                # Add cloud provider to each record
                for record in data:
                    record['cloud'] = cloud_name
                all_results[cloud_name].extend(data)

    return all_results


def filter_measurement_only(results: List[Dict]) -> List[Dict]:
    """Filter out warmup iterations."""
    return [r for r in results if not r.get('warmup', False)]


def group_by_scenario(results: List[Dict]) -> Dict[str, List[Dict]]:
    """Group results by scenario key."""
    grouped = defaultdict(list)
    for r in results:
        key = f"{r['format']}_{r['density']}_{r['strategy']}_d{r['num-deletes']}_r{r['num-runs']}"
        grouped[key].append(r)
    return grouped


def compute_statistics(values: List[float]) -> Dict[str, float]:
    """Compute statistics for a list of values."""
    if not values:
        return {}

    sorted_vals = sorted(values)
    n = len(sorted_vals)

    return {
        'count': n,
        'mean': statistics.mean(values),
        'median': statistics.median(values),
        'stddev': statistics.stdev(values) if n > 1 else 0,
        'min': min(values),
        'max': max(values),
        'p50': sorted_vals[int(n * 0.5)],
        'p95': sorted_vals[int(n * 0.95)] if n >= 20 else sorted_vals[-1],
        'p99': sorted_vals[int(n * 0.99)] if n >= 100 else sorted_vals[-1],
    }


def analyze_by_cloud(all_results: Dict[str, List[Dict]]) -> Dict[str, Dict]:
    """Analyze results grouped by cloud provider."""
    analysis = {}

    for cloud, results in all_results.items():
        measurements = filter_measurement_only(results)
        grouped = group_by_scenario(measurements)

        cloud_stats = {}
        for scenario, scenario_results in grouped.items():
            latencies_ms = [r['total-latency-ns'] / 1_000_000 for r in scenario_results]
            read_pct = [r['read-latency-ns'] / r['total-latency-ns'] * 100 for r in scenario_results]
            remap_pct = [r['remap-latency-ns'] / r['total-latency-ns'] * 100 for r in scenario_results]
            write_pct = [r['write-latency-ns'] / r['total-latency-ns'] * 100 for r in scenario_results]

            num_deletes = scenario_results[0]['num-deletes']
            throughput = [num_deletes / (r['total-latency-ns'] / 1_000_000_000) for r in scenario_results]

            cloud_stats[scenario] = {
                'latency_ms': compute_statistics(latencies_ms),
                'read_pct': compute_statistics(read_pct),
                'remap_pct': compute_statistics(remap_pct),
                'write_pct': compute_statistics(write_pct),
                'throughput': compute_statistics(throughput),
                'num_deletes': num_deletes,
                'num_runs': scenario_results[0]['num-runs'],
                'format': scenario_results[0]['format'],
                'density': scenario_results[0]['density'],
            }

        analysis[cloud] = cloud_stats

    return analysis


def compare_clouds(analysis: Dict[str, Dict]) -> None:
    """Print cross-cloud comparison."""
    print("\n" + "=" * 100)
    print("CROSS-CLOUD COMPARISON")
    print("=" * 100)

    # Find common scenarios
    all_scenarios = set()
    for cloud_stats in analysis.values():
        all_scenarios.update(cloud_stats.keys())

    # Print header
    clouds = sorted(analysis.keys())
    print(f"\n{'Scenario':<50} " + " ".join(f"{c.upper():>15}" for c in clouds))
    print("-" * (50 + 16 * len(clouds)))

    # Group scenarios by format
    for format_type in ['POSITION_DELETE_FILE', 'DELETION_VECTOR']:
        print(f"\n--- {format_type} ---")

        for scenario in sorted(all_scenarios):
            if format_type not in scenario:
                continue

            # Extract short name
            parts = scenario.split('_')
            density = parts[2] if len(parts) > 2 else ''
            deletes = parts[3] if len(parts) > 3 else ''
            runs = parts[4] if len(parts) > 4 else ''
            short_name = f"{density} {deletes} {runs}"

            row = f"{short_name:<50}"
            for cloud in clouds:
                if scenario in analysis.get(cloud, {}):
                    latency = analysis[cloud][scenario]['latency_ms']['mean']
                    row += f" {latency:>14.2f}ms"
                else:
                    row += f" {'N/A':>15}"
            print(row)


def print_summary(analysis: Dict[str, Dict]) -> None:
    """Print summary statistics."""
    print("\n" + "=" * 100)
    print("SUMMARY STATISTICS BY CLOUD")
    print("=" * 100)

    for cloud, scenarios in analysis.items():
        print(f"\n### {cloud.upper()} ###")

        # Aggregate by format
        for format_type in ['POSITION_DELETE_FILE', 'DELETION_VECTOR']:
            format_scenarios = {k: v for k, v in scenarios.items() if format_type in k}
            if not format_scenarios:
                continue

            all_latencies = []
            all_throughputs = []
            for stats in format_scenarios.values():
                all_latencies.append(stats['latency_ms']['mean'])
                all_throughputs.append(stats['throughput']['mean'])

            print(f"\n  {format_type}:")
            print(f"    Avg Latency: {statistics.mean(all_latencies):.2f}ms")
            print(f"    Min Latency: {min(all_latencies):.2f}ms")
            print(f"    Max Latency: {max(all_latencies):.2f}ms")
            print(f"    Avg Throughput: {statistics.mean(all_throughputs):,.0f} deletes/sec")


def export_csv(analysis: Dict[str, Dict], output_path: Path) -> None:
    """Export results to CSV for further analysis."""
    with open(output_path, 'w') as f:
        # Header
        f.write("cloud,format,density,num_deletes,num_runs,")
        f.write("latency_mean_ms,latency_p50_ms,latency_p95_ms,latency_p99_ms,")
        f.write("read_pct,remap_pct,write_pct,throughput\n")

        for cloud, scenarios in analysis.items():
            for scenario, stats in scenarios.items():
                f.write(f"{cloud},{stats['format']},{stats['density']},")
                f.write(f"{stats['num_deletes']},{stats['num_runs']},")
                f.write(f"{stats['latency_ms']['mean']:.2f},")
                f.write(f"{stats['latency_ms']['p50']:.2f},")
                f.write(f"{stats['latency_ms'].get('p95', stats['latency_ms']['max']):.2f},")
                f.write(f"{stats['latency_ms'].get('p99', stats['latency_ms']['max']):.2f},")
                f.write(f"{stats['read_pct']['mean']:.1f},")
                f.write(f"{stats['remap_pct']['mean']:.1f},")
                f.write(f"{stats['write_pct']['mean']:.1f},")
                f.write(f"{stats['throughput']['mean']:.0f}\n")

    print(f"\nCSV exported to: {output_path}")


def main():
    # Determine results directory
    if len(sys.argv) > 1:
        results_dir = Path(sys.argv[1])
    else:
        script_dir = Path(__file__).parent
        results_dir = script_dir.parent / 'results'

    if not results_dir.exists():
        print(f"Error: Results directory not found: {results_dir}")
        sys.exit(1)

    print(f"Loading results from: {results_dir}")

    # Load and analyze
    all_results = load_results(results_dir)

    if not all_results:
        print("No results found!")
        sys.exit(1)

    print(f"Found results from: {', '.join(all_results.keys())}")
    for cloud, results in all_results.items():
        measurements = filter_measurement_only(results)
        print(f"  {cloud}: {len(measurements)} measurements ({len(results)} total)")

    # Analyze
    analysis = analyze_by_cloud(all_results)

    # Print reports
    print_summary(analysis)
    compare_clouds(analysis)

    # Export CSV
    csv_path = results_dir / 'combined_results.csv'
    export_csv(analysis, csv_path)

    # Save JSON analysis
    json_path = results_dir / 'analysis.json'
    with open(json_path, 'w') as f:
        # Convert for JSON serialization
        json_analysis = {}
        for cloud, scenarios in analysis.items():
            json_analysis[cloud] = {}
            for scenario, stats in scenarios.items():
                json_analysis[cloud][scenario] = stats
        json.dump(json_analysis, f, indent=2)
    print(f"JSON analysis saved to: {json_path}")


if __name__ == '__main__':
    main()
