#!/usr/bin/env python3
"""
Enhanced analysis for hyperparallel benchmark results.

Handles:
- Multiple iterations per scenario (averaging)
- Filtering measurement artifacts (sub-microsecond times)
- Sorted vs unsorted comparison
- Strategy selection accuracy analysis
"""

import sys
import csv
from collections import defaultdict
import statistics

def load_and_filter_results(csv_file, min_time_us=1.0):
    """Load results, filtering out measurement artifacts."""
    results = []
    filtered_count = 0

    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            avg_time = float(row['avg_time_us'])
            if avg_time < min_time_us:
                filtered_count += 1
                continue
            results.append({
                'strategy': row['strategy'],
                'gap_ratio': float(row['gap_ratio']),
                'num_positions': int(row['num_positions']),
                'num_runs': int(row['num_runs']),
                'sorted': row['sorted'].lower() == 'true',
                'avg_time_us': avg_time,
                'error_us': float(row['error_us'])
            })

    print(f"Loaded {len(results)} measurements (filtered {filtered_count} sub-{min_time_us}us artifacts)")
    return results


def aggregate_by_scenario(results):
    """Average multiple iterations per scenario."""
    scenario_times = defaultdict(lambda: defaultdict(list))

    for r in results:
        key = (r['gap_ratio'], r['num_positions'], r['num_runs'], r['sorted'])
        scenario_times[key][r['strategy']].append(r['avg_time_us'])

    # Average the times
    scenarios = {}
    for key, strategies in scenario_times.items():
        scenarios[key] = {}
        for strategy, times in strategies.items():
            scenarios[key][strategy] = {
                'mean': statistics.mean(times),
                'stdev': statistics.stdev(times) if len(times) > 1 else 0,
                'n': len(times)
            }

    return scenarios


def analyze_optimal_strategies(scenarios):
    """Find optimal strategy for each scenario."""
    # Base strategies (excluding NoPushdown variants and smartSelector)
    base_strategies = ['linearSearch', 'binarySearch', 'intervalTree', 'streamJoin', 'rangeQuery']

    results = {'sorted': [], 'unsorted': []}

    for scenario_key, strategies in scenarios.items():
        gap, n, m, sorted_flag = scenario_key
        category = 'sorted' if sorted_flag else 'unsorted'

        # Find optimal among base strategies
        best = None
        best_time = float('inf')
        for strat in base_strategies:
            if strat in strategies and strategies[strat]['mean'] < best_time:
                best = strat
                best_time = strategies[strat]['mean']

        if best is None:
            continue

        # Get selector time
        selector_time = strategies.get('smartSelector', {}).get('mean')

        results[category].append({
            'scenario': scenario_key,
            'optimal': best,
            'optimal_time': best_time,
            'selector_time': selector_time,
            'overhead_pct': ((selector_time - best_time) / best_time * 100) if selector_time else None,
            'all_strategies': {k: v['mean'] for k, v in strategies.items()}
        })

    return results


def print_analysis(analysis):
    """Print comprehensive analysis."""
    print("\n" + "=" * 80)
    print("HYPERPARALLEL BENCHMARK ANALYSIS")
    print("=" * 80)

    for category in ['sorted', 'unsorted']:
        data = analysis[category]
        if not data:
            continue

        print(f"\n{'=' * 40}")
        print(f"{category.upper()} DATA ({len(data)} scenarios)")
        print('=' * 40)

        # Count wins per strategy
        wins = defaultdict(int)
        for d in data:
            wins[d['optimal']] += 1

        print("\nOptimal Strategy Wins:")
        for strat, count in sorted(wins.items(), key=lambda x: -x[1]):
            pct = count / len(data) * 100
            print(f"  {strat:20s}: {count:3d} ({pct:5.1f}%)")

        # Selector overhead
        overheads = [d['overhead_pct'] for d in data if d['overhead_pct'] is not None]
        if overheads:
            print(f"\nSmart Selector Overhead:")
            print(f"  Mean:   {statistics.mean(overheads):6.1f}%")
            print(f"  Median: {statistics.median(overheads):6.1f}%")
            print(f"  Min:    {min(overheads):6.1f}%")
            print(f"  Max:    {max(overheads):6.1f}%")

            # Worst cases
            worst = sorted(data, key=lambda x: x['overhead_pct'] or 0, reverse=True)[:5]
            print(f"\n  Worst 5 cases:")
            for d in worst:
                gap, n, m, s = d['scenario']
                print(f"    {d['overhead_pct']:5.1f}%: gap={gap}, n={n:6d}, m={m:5d}")
                print(f"           optimal={d['optimal']} ({d['optimal_time']:.1f}us) vs selector ({d['selector_time']:.1f}us)")

        # Performance by scale
        print(f"\n  Performance by Scale (gap=0.0):")
        for n in [1000, 10000, 100000]:
            scenarios_at_n = [d for d in data if d['scenario'][1] == n and d['scenario'][0] == 0.0]
            if scenarios_at_n:
                for d in sorted(scenarios_at_n, key=lambda x: x['scenario'][2]):
                    gap, n_val, m, s = d['scenario']
                    strats = d['all_strategies']
                    linear = strats.get('linearSearch', 0)
                    best_time = d['optimal_time']
                    speedup = linear / best_time if best_time > 0 else 0
                    print(f"    n={n:6d}, m={m:5d}: {d['optimal']:15s} ({best_time:8.1f}us) {speedup:5.1f}x vs linear")


def create_plots(scenarios, output_dir):
    """Generate visualization plots."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np

    # 1. Strategy comparison heatmap (sorted vs unsorted)
    fig, axes = plt.subplots(1, 2, figsize=(16, 8))

    strategies = ['linearSearch', 'binarySearch', 'intervalTree', 'streamJoin', 'rangeQuery', 'smartSelector']
    strategy_colors = {
        'linearSearch': 0, 'binarySearch': 1, 'intervalTree': 2,
        'streamJoin': 3, 'rangeQuery': 4, 'smartSelector': 5
    }

    for ax_idx, sorted_flag in enumerate([True, False]):
        ax = axes[ax_idx]

        # Build grid: rows=scenarios, cols=strategies
        filtered = [(k, v) for k, v in scenarios.items() if k[3] == sorted_flag]
        filtered.sort(key=lambda x: (x[0][1], x[0][2], x[0][0]))  # Sort by n, m, gap

        if not filtered:
            continue

        n_scenarios = len(filtered)
        n_strats = len(strategies)

        # Find optimal for each scenario
        heatmap_data = []
        labels = []

        for scenario_key, strats in filtered:
            gap, n, m, s = scenario_key
            labels.append(f"n={n},m={m},g={gap}")

            row = []
            times = {k: v['mean'] for k, v in strats.items() if k in strategies}
            min_time = min(times.values()) if times else 1

            for strat in strategies:
                if strat in times:
                    # Ratio to best (1.0 = optimal)
                    ratio = times[strat] / min_time
                    row.append(ratio)
                else:
                    row.append(np.nan)

            heatmap_data.append(row)

        data = np.array(heatmap_data)

        im = ax.imshow(data, aspect='auto', cmap='RdYlGn_r', vmin=1.0, vmax=3.0)

        ax.set_xticks(range(n_strats))
        ax.set_xticklabels([s.replace('Search', '').replace('Tree', 'Tree').replace('Selector', 'Select')
                          for s in strategies], rotation=45, ha='right', fontsize=8)

        # Only show every 3rd y label to reduce clutter
        ax.set_yticks(range(0, n_scenarios, 3))
        ax.set_yticklabels([labels[i] for i in range(0, n_scenarios, 3)], fontsize=7)

        ax.set_title(f"{'Sorted' if sorted_flag else 'Unsorted'} Data\n(1.0=optimal, darker=slower)", fontsize=11)

        # Mark optimal cells
        for i in range(len(heatmap_data)):
            row = heatmap_data[i]
            if row:
                min_idx = np.nanargmin(row)
                ax.add_patch(plt.Rectangle((min_idx-0.5, i-0.5), 1, 1,
                            fill=False, edgecolor='black', linewidth=2))

    plt.colorbar(im, ax=axes, label='Ratio to Optimal', shrink=0.8)
    plt.suptitle('Strategy Performance Comparison (Hyperparallel Benchmark)', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/hyperparallel_strategy_heatmap.png', dpi=150, bbox_inches='tight')
    print(f"Saved: {output_dir}/hyperparallel_strategy_heatmap.png")
    plt.close()

    # 2. Selector overhead distribution
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    for ax_idx, sorted_flag in enumerate([True, False]):
        ax = axes[ax_idx]

        overheads = []
        for scenario_key, strats in scenarios.items():
            if scenario_key[3] != sorted_flag:
                continue
            if 'smartSelector' not in strats:
                continue

            selector_time = strats['smartSelector']['mean']
            base = {k: v['mean'] for k, v in strats.items()
                   if k in ['linearSearch', 'binarySearch', 'intervalTree', 'streamJoin', 'rangeQuery']}
            if not base:
                continue
            optimal_time = min(base.values())
            overhead = (selector_time - optimal_time) / optimal_time * 100
            overheads.append(overhead)

        if overheads:
            ax.hist(overheads, bins=20, color='steelblue', edgecolor='black', alpha=0.7)
            ax.axvline(statistics.mean(overheads), color='red', linestyle='--',
                      label=f'Mean: {statistics.mean(overheads):.1f}%')
            ax.axvline(statistics.median(overheads), color='green', linestyle='--',
                      label=f'Median: {statistics.median(overheads):.1f}%')
            ax.set_xlabel('Overhead vs Optimal (%)')
            ax.set_ylabel('Count')
            ax.set_title(f"{'Sorted' if sorted_flag else 'Unsorted'} Data")
            ax.legend()
            ax.grid(True, alpha=0.3)

    plt.suptitle('Smart Selector Overhead Distribution', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/hyperparallel_selector_overhead.png', dpi=150, bbox_inches='tight')
    print(f"Saved: {output_dir}/hyperparallel_selector_overhead.png")
    plt.close()

    # 3. Speedup vs Linear by scale
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))

    for row_idx, sorted_flag in enumerate([True, False]):
        for col_idx, n in enumerate([1000, 10000, 100000]):
            ax = axes[row_idx, col_idx]

            data_by_m = defaultdict(dict)
            for scenario_key, strats in scenarios.items():
                gap, n_val, m, s = scenario_key
                if s != sorted_flag or n_val != n or gap != 0.0:
                    continue
                if 'linearSearch' not in strats:
                    continue

                linear_time = strats['linearSearch']['mean']
                for strategy in ['binarySearch', 'intervalTree', 'streamJoin', 'rangeQuery', 'smartSelector']:
                    if strategy in strats:
                        speedup = linear_time / strats[strategy]['mean']
                        data_by_m[m][strategy] = speedup

            colors = {'binarySearch': '#3498db', 'intervalTree': '#2ecc71',
                     'streamJoin': '#f39c12', 'rangeQuery': '#9b59b6', 'smartSelector': '#e74c3c'}

            for strategy in ['binarySearch', 'intervalTree', 'streamJoin', 'rangeQuery', 'smartSelector']:
                m_vals = sorted(data_by_m.keys())
                speedups = [data_by_m[m].get(strategy) for m in m_vals]
                if any(s is not None for s in speedups):
                    valid_m = [m for m, s in zip(m_vals, speedups) if s is not None]
                    valid_s = [s for s in speedups if s is not None]
                    ax.plot(valid_m, valid_s, marker='o', label=strategy,
                           color=colors.get(strategy, 'gray'), linewidth=2)

            ax.axhline(y=1, color='gray', linestyle='--', alpha=0.5)
            ax.set_xscale('log')
            ax.set_xlabel('Number of Runs (m)')
            ax.set_ylabel('Speedup vs Linear')
            ax.set_title(f"n={n:,} ({'Sorted' if sorted_flag else 'Unsorted'})")
            ax.legend(fontsize=7, loc='best')
            ax.grid(True, alpha=0.3)

    plt.suptitle('Speedup vs Linear Search by Scale (gap=0.0)', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/hyperparallel_speedup_by_scale.png', dpi=150, bbox_inches='tight')
    print(f"Saved: {output_dir}/hyperparallel_speedup_by_scale.png")
    plt.close()


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 analyze_hyperparallel.py <results.csv>")
        sys.exit(1)

    csv_file = sys.argv[1]

    # Load and filter
    results = load_and_filter_results(csv_file, min_time_us=1.0)

    # Aggregate by scenario
    scenarios = aggregate_by_scenario(results)
    print(f"Aggregated to {len(scenarios)} unique scenarios")

    # Analyze
    analysis = analyze_optimal_strategies(scenarios)
    print_analysis(analysis)

    # Generate plots
    import os
    output_dir = os.path.dirname(csv_file) or '.'

    try:
        create_plots(scenarios, output_dir)
    except ImportError:
        print("\nNote: matplotlib not available, skipping plots")


if __name__ == '__main__':
    main()
