#!/usr/bin/env python3
"""
Generate visualization charts from JMH benchmark results.

Requires: matplotlib
Install: pip3 install matplotlib

Usage:
    python3 visualize_results.py <results.csv>

Example:
    python3 visualize_results.py results_20260116_162342.csv
"""

import sys
import csv
from collections import defaultdict
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


def load_results(csv_file):
    """Load results from CSV file."""
    results = []
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            results.append({
                'strategy': row['strategy'],
                'gap_ratio': float(row['gap_ratio']),
                'num_positions': int(row['num_positions']),
                'num_runs': int(row['num_runs']),
                'sorted': row['sorted'].lower() == 'true',
                'avg_time_us': float(row['avg_time_us']),
                'error_us': float(row['error_us'])
            })
    return results


def group_by_scenario(results):
    """Group results by scenario."""
    scenarios = defaultdict(dict)
    for r in results:
        key = (r['gap_ratio'], r['num_positions'], r['num_runs'], r['sorted'])
        scenarios[key][r['strategy']] = (r['avg_time_us'], r['error_us'])
    return scenarios


def create_strategy_comparison_chart(results, output_file):
    """Create comparison chart showing all strategies across scales."""
    # Filter to sorted, gap=0.0 scenarios for cleaner visualization
    filtered = [r for r in results if r['sorted'] and r['gap_ratio'] == 0.0]

    if not filtered:
        print("No sorted, gap=0.0 results found")
        return

    # Group by (n, m)
    scenarios = defaultdict(dict)
    for r in filtered:
        key = (r['num_positions'], r['num_runs'])
        scenarios[key][r['strategy']] = r['avg_time_us']

    # Create subplots for different n values
    n_values = sorted(set(r['num_positions'] for r in filtered))
    m_values = sorted(set(r['num_runs'] for r in filtered))

    fig, axes = plt.subplots(1, len(n_values), figsize=(18, 5))
    if len(n_values) == 1:
        axes = [axes]

    strategies = ['linearSearch', 'binarySearch', 'intervalTree',
                  'streamJoin', 'rangeQuery', 'smartSelector']
    colors = {'linearSearch': '#e74c3c', 'binarySearch': '#3498db',
              'intervalTree': '#2ecc71', 'streamJoin': '#f39c12',
              'rangeQuery': '#9b59b6', 'smartSelector': '#1abc9c'}

    for ax, n in zip(axes, n_values):
        times_by_m = defaultdict(dict)

        for m in m_values:
            key = (n, m)
            if key in scenarios:
                for strategy in strategies:
                    if strategy in scenarios[key]:
                        times_by_m[m][strategy] = scenarios[key][strategy]

        # Plot each strategy
        for strategy in strategies:
            m_vals = []
            time_vals = []
            for m in sorted(times_by_m.keys()):
                if strategy in times_by_m[m]:
                    m_vals.append(m)
                    time_vals.append(times_by_m[m][strategy])

            if m_vals:
                ax.plot(m_vals, time_vals, marker='o',
                        label=strategy, color=colors.get(strategy, '#34495e'),
                        linewidth=2, markersize=6)

        ax.set_xlabel('Number of Runs (m)', fontsize=11, fontweight='bold')
        ax.set_ylabel('Time (μs)', fontsize=11, fontweight='bold')
        ax.set_title(f'n = {n:,} positions', fontsize=12, fontweight='bold')
        ax.set_xscale('log')
        ax.set_yscale('log')
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.legend(fontsize=8, loc='best')

        # Format axes
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{int(x)}'))
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{x:.0f}'))

    plt.suptitle('Remapping Strategy Performance (sorted=true, gap=0.0)',
                 fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"Strategy comparison chart saved to: {output_file}")
    plt.close()


def create_selector_overhead_chart(results, output_file):
    """Create chart showing selector overhead vs optimal."""
    scenarios = group_by_scenario(results)

    overhead_data = []

    for scenario_key, strategies in scenarios.items():
        if 'smartSelector' not in strategies:
            continue

        gap, n, m, sorted_flag = scenario_key

        # Find optimal (excluding smartSelector)
        non_selector = {k: v[0] for k, v in strategies.items() if k != 'smartSelector'}
        if not non_selector:
            continue

        optimal_strat, optimal_time = min(non_selector.items(), key=lambda x: x[1])
        selector_time = strategies['smartSelector'][0]

        overhead_pct = ((selector_time - optimal_time) / optimal_time) * 100

        overhead_data.append({
            'scenario': f"g={gap},n={n},m={m},s={int(sorted_flag)}",
            'gap': gap,
            'n': n,
            'm': m,
            'sorted': sorted_flag,
            'overhead_pct': overhead_pct,
            'optimal': optimal_strat
        })

    if not overhead_data:
        print("No selector overhead data found")
        return

    # Sort by overhead (worst first)
    overhead_data.sort(key=lambda x: x['overhead_pct'], reverse=True)

    # Create bar chart for top 20 worst cases
    top_n = min(20, len(overhead_data))
    top_cases = overhead_data[:top_n]

    fig, ax = plt.subplots(figsize=(14, 8))

    scenarios_labels = [f"n={d['n']},m={d['m']},s={int(d['sorted'])}" for d in top_cases]
    overheads = [d['overhead_pct'] for d in top_cases]

    # Color by sorted status
    colors_list = ['#e74c3c' if not d['sorted'] else '#3498db' for d in top_cases]

    bars = ax.barh(range(len(scenarios_labels)), overheads, color=colors_list, alpha=0.7)

    ax.set_yticks(range(len(scenarios_labels)))
    ax.set_yticklabels(scenarios_labels, fontsize=9)
    ax.set_xlabel('Overhead vs Optimal (%)', fontsize=12, fontweight='bold')
    ax.set_title('Smart Selector Overhead - Top 20 Worst Cases',
                 fontsize=14, fontweight='bold')
    ax.grid(True, axis='x', alpha=0.3, linestyle='--')

    # Add legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='#e74c3c', alpha=0.7, label='Unsorted'),
        Patch(facecolor='#3498db', alpha=0.7, label='Sorted')
    ]
    ax.legend(handles=legend_elements, loc='lower right', fontsize=10)

    # Add value labels on bars
    for i, (bar, overhead) in enumerate(zip(bars, overheads)):
        width = bar.get_width()
        ax.text(width + max(overheads) * 0.01, bar.get_y() + bar.get_height()/2,
                f'{overhead:.0f}%', ha='left', va='center', fontsize=8)

    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"Selector overhead chart saved to: {output_file}")
    plt.close()


def create_speedup_chart(results, output_file):
    """Create chart showing speedup vs linear search."""
    # Filter to sorted, gap=0.0 scenarios
    filtered = [r for r in results if r['sorted'] and r['gap_ratio'] == 0.0]

    scenarios = group_by_scenario(filtered)

    speedup_data = defaultdict(lambda: defaultdict(dict))

    for scenario_key, strategies in scenarios.items():
        if 'linearSearch' not in strategies:
            continue

        gap, n, m, sorted_flag = scenario_key
        linear_time = strategies['linearSearch'][0]

        for strategy, (time, _) in strategies.items():
            if strategy != 'linearSearch':
                speedup = linear_time / time
                speedup_data[n][m][strategy] = speedup

    # Create subplots for each n
    n_values = sorted(speedup_data.keys())
    fig, axes = plt.subplots(1, len(n_values), figsize=(18, 5))
    if len(n_values) == 1:
        axes = [axes]

    strategies = ['binarySearch', 'intervalTree', 'streamJoin', 'rangeQuery', 'smartSelector']
    colors = {'binarySearch': '#3498db', 'intervalTree': '#2ecc71',
              'streamJoin': '#f39c12', 'rangeQuery': '#9b59b6', 'smartSelector': '#1abc9c'}

    for ax, n in zip(axes, n_values):
        m_values = sorted(speedup_data[n].keys())

        for strategy in strategies:
            m_vals = []
            speedup_vals = []

            for m in m_values:
                if strategy in speedup_data[n][m]:
                    m_vals.append(m)
                    speedup_vals.append(speedup_data[n][m][strategy])

            if m_vals:
                ax.plot(m_vals, speedup_vals, marker='o', label=strategy,
                        color=colors.get(strategy, '#34495e'),
                        linewidth=2, markersize=6)

        ax.set_xlabel('Number of Runs (m)', fontsize=11, fontweight='bold')
        ax.set_ylabel('Speedup vs Linear Search', fontsize=11, fontweight='bold')
        ax.set_title(f'n = {n:,} positions', fontsize=12, fontweight='bold')
        ax.set_xscale('log')
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.legend(fontsize=8, loc='best')
        ax.axhline(y=1, color='r', linestyle='--', alpha=0.5, linewidth=1)

        # Format x-axis
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{int(x)}'))

    plt.suptitle('Speedup vs Linear Search (sorted=true, gap=0.0)',
                 fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"Speedup chart saved to: {output_file}")
    plt.close()


def create_all_charts(csv_file):
    """Create all visualization charts."""
    print(f"Loading results from: {csv_file}")
    results = load_results(csv_file)
    print(f"Loaded {len(results)} measurements")

    output_dir = csv_file.rsplit('/', 1)[0] if '/' in csv_file else '.'

    # Generate charts
    create_strategy_comparison_chart(
        results,
        f"{output_dir}/chart_strategy_comparison.png"
    )

    create_selector_overhead_chart(
        results,
        f"{output_dir}/chart_selector_overhead.png"
    )

    create_speedup_chart(
        results,
        f"{output_dir}/chart_speedup_vs_linear.png"
    )

    print("\nAll charts generated successfully!")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 visualize_results.py <results.csv>")
        print("\nRequires: matplotlib")
        print("Install: pip3 install matplotlib")
        sys.exit(1)

    csv_file = sys.argv[1]

    try:
        create_all_charts(csv_file)
    except ImportError:
        print("\nError: matplotlib not installed")
        print("Install with: pip3 install matplotlib")
        sys.exit(1)
    except Exception as e:
        print(f"\nError generating charts: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
