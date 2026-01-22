#!/usr/bin/env python3
"""
Analyze JMH benchmark results for remapping strategies.

Usage:
    python3 analyze_results.py <results_file>

Example:
    python3 analyze_results.py results_20260116_162342.txt
"""

import sys
import re
from collections import defaultdict
from typing import Dict, List, Tuple


def parse_jmh_results(filename: str) -> List[Dict]:
    """Parse JMH benchmark results into structured data.

    Supports both text format and JSON format.
    """
    results = []

    # Try JSON format first
    if filename.endswith('.json'):
        import json
        with open(filename, 'r') as f:
            data = json.load(f)

        for item in data:
            benchmark = item['benchmark'].split('.')[-1]
            params = item.get('params', {})
            score = item['primaryMetric']['score']
            error = item['primaryMetric']['scoreError']

            results.append({
                'strategy': benchmark,
                'gap_ratio': float(params.get('gapRatio', 0)),
                'num_positions': int(params.get('numPositions', 0)),
                'num_runs': int(params.get('numRuns', 0)),
                'sorted': params.get('sorted', 'false') == 'true',
                'avg_time_us': float(score),
                'error_us': float(error)
            })
        return results

    # Try CSV format
    if filename.endswith('.csv'):
        import csv
        with open(filename, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                results.append({
                    'strategy': row['strategy'],
                    'gap_ratio': float(row['gap_ratio']),
                    'num_positions': int(row['num_positions']),
                    'num_runs': int(row['num_runs']),
                    'sorted': row['sorted'] == 'true',
                    'avg_time_us': float(row['avg_time_us']),
                    'error_us': float(row['error_us'])
                })
        return results

    # Fall back to text format
    with open(filename, 'r') as f:
        for line in f:
            # Match result lines like:
            # RemappingAlgorithmBenchmark.binarySearch 0.0 1000 10 true avgt 5 22.410 ± 2.370 us/op
            match = re.match(
                r'RemappingAlgorithmBenchmark\.(\w+)\s+'
                r'([\d.]+)\s+'  # gapRatio
                r'(\d+)\s+'     # numPositions
                r'(\d+)\s+'     # numRuns
                r'(true|false)\s+'  # sorted
                r'avgt\s+\d+\s+'
                r'([\d.]+)\s+±\s+([\d.]+)\s+us/op',
                line
            )

            if match:
                strategy, gap_ratio, num_pos, num_runs, sorted_str, avg_time, error = match.groups()
                results.append({
                    'strategy': strategy,
                    'gap_ratio': float(gap_ratio),
                    'num_positions': int(num_pos),
                    'num_runs': int(num_runs),
                    'sorted': sorted_str == 'true',
                    'avg_time_us': float(avg_time),
                    'error_us': float(error)
                })

    return results


def group_by_scenario(results: List[Dict]) -> Dict:
    """Group results by scenario parameters."""
    scenarios = defaultdict(dict)

    for result in results:
        key = (
            result['gap_ratio'],
            result['num_positions'],
            result['num_runs'],
            result['sorted']
        )
        scenarios[key][result['strategy']] = result['avg_time_us']

    return scenarios


def find_optimal_strategy(strategies: Dict[str, float]) -> Tuple[str, float]:
    """Find the optimal (fastest) strategy for a scenario."""
    return min(strategies.items(), key=lambda x: x[1])


def calculate_selector_overhead(scenarios: Dict) -> List[Dict]:
    """Calculate smart selector overhead vs optimal strategy."""
    overhead_analysis = []

    for scenario_key, strategies in scenarios.items():
        if 'smartSelector' not in strategies:
            continue

        gap_ratio, num_pos, num_runs, sorted_flag = scenario_key

        # Find optimal strategy (excluding smartSelector)
        non_selector_strategies = {
            k: v for k, v in strategies.items()
            if k != 'smartSelector'
        }

        if not non_selector_strategies:
            continue

        optimal_strategy, optimal_time = find_optimal_strategy(non_selector_strategies)
        selector_time = strategies['smartSelector']

        overhead_pct = ((selector_time - optimal_time) / optimal_time) * 100

        overhead_analysis.append({
            'gap_ratio': gap_ratio,
            'num_positions': num_pos,
            'num_runs': num_runs,
            'sorted': sorted_flag,
            'optimal_strategy': optimal_strategy,
            'optimal_time': optimal_time,
            'selector_time': selector_time,
            'overhead_pct': overhead_pct
        })

    return overhead_analysis


def analyze_strategy_performance(scenarios: Dict) -> Dict:
    """Analyze when each strategy is optimal."""
    strategy_wins = defaultdict(list)

    for scenario_key, strategies in scenarios.items():
        # Exclude smartSelector from optimal comparison
        non_selector = {
            k: v for k, v in strategies.items()
            if k != 'smartSelector'
        }

        if not non_selector:
            continue

        optimal, _ = find_optimal_strategy(non_selector)
        strategy_wins[optimal].append(scenario_key)

    return strategy_wins


def print_summary(results: List[Dict]):
    """Print summary statistics."""
    scenarios = group_by_scenario(results)

    print("=" * 80)
    print("BENCHMARK SUMMARY")
    print("=" * 80)
    print(f"\nTotal scenarios: {len(scenarios)}")
    print(f"Total measurements: {len(results)}")

    # Strategy performance analysis
    print("\n" + "=" * 80)
    print("OPTIMAL STRATEGY BY SCENARIO")
    print("=" * 80)

    strategy_wins = analyze_strategy_performance(scenarios)

    for strategy in sorted(strategy_wins.keys()):
        wins = strategy_wins[strategy]
        print(f"\n{strategy}: {len(wins)} scenarios")

        # Group by characteristics
        sorted_wins = [s for s in wins if s[3]]
        unsorted_wins = [s for s in wins if not s[3]]

        print(f"  - Sorted: {len(sorted_wins)}")
        print(f"  - Unsorted: {len(unsorted_wins)}")

        if len(wins) <= 10:
            for scenario in wins:
                gap, n, m, sorted_flag = scenario
                print(f"    gap={gap}, n={n}, m={m}, sorted={sorted_flag}")

    # Smart selector overhead analysis
    print("\n" + "=" * 80)
    print("SMART SELECTOR OVERHEAD ANALYSIS")
    print("=" * 80)

    overhead_analysis = calculate_selector_overhead(scenarios)

    if overhead_analysis:
        overheads = [o['overhead_pct'] for o in overhead_analysis]
        avg_overhead = sum(overheads) / len(overheads)
        max_overhead = max(overheads)
        min_overhead = min(overheads)

        print(f"\nAverage overhead: {avg_overhead:.2f}%")
        print(f"Max overhead: {max_overhead:.2f}%")
        print(f"Min overhead: {min_overhead:.2f}%")

        # Cases with high overhead (>10%)
        high_overhead = [o for o in overhead_analysis if o['overhead_pct'] > 10]

        if high_overhead:
            print(f"\nHigh overhead cases (>{10}%): {len(high_overhead)}")
            print("\nTop 10 worst cases:")
            high_overhead.sort(key=lambda x: x['overhead_pct'], reverse=True)

            for i, case in enumerate(high_overhead[:10], 1):
                print(f"\n{i}. Overhead: {case['overhead_pct']:.1f}%")
                print(f"   Scenario: gap={case['gap_ratio']}, n={case['num_positions']}, "
                      f"m={case['num_runs']}, sorted={case['sorted']}")
                print(f"   Optimal: {case['optimal_strategy']} ({case['optimal_time']:.2f} us)")
                print(f"   Selector: {case['selector_time']:.2f} us")

        # Cases with negative overhead (selector faster than optimal)
        negative_overhead = [o for o in overhead_analysis if o['overhead_pct'] < 0]
        if negative_overhead:
            print(f"\nSelector faster than 'optimal': {len(negative_overhead)} cases")
            print("(Likely due to measurement variance)")

    # Performance comparison by scale
    print("\n" + "=" * 80)
    print("PERFORMANCE BY SCALE (sorted=true, gap=0.0)")
    print("=" * 80)

    for n in [1000, 10000, 100000]:
        for m in [10, 100, 1000]:
            key = (0.0, n, m, True)
            if key in scenarios:
                strats = scenarios[key]
                print(f"\nn={n}, m={m}:")

                # Sort by time
                sorted_strats = sorted(strats.items(), key=lambda x: x[1])
                for strat, time in sorted_strats:
                    print(f"  {strat:20s}: {time:8.2f} us")

                # Calculate speedup vs linear
                if 'linearSearch' in strats:
                    linear_time = strats['linearSearch']
                    optimal_strat, optimal_time = sorted_strats[0]
                    speedup = linear_time / optimal_time
                    print(f"  Speedup vs linear: {speedup:.1f}x")


def generate_csv(results: List[Dict], output_file: str):
    """Generate CSV file for further analysis."""
    import csv

    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'strategy', 'gap_ratio', 'num_positions', 'num_runs', 'sorted',
            'avg_time_us', 'error_us'
        ])
        writer.writeheader()
        writer.writerows(results)

    print(f"\nCSV output written to: {output_file}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 analyze_results.py <results_file>")
        sys.exit(1)

    results_file = sys.argv[1]

    print(f"Parsing results from: {results_file}")
    results = parse_jmh_results(results_file)

    if not results:
        print("No results found in file!")
        sys.exit(1)

    print(f"Parsed {len(results)} measurements")

    # Print summary
    print_summary(results)

    # Generate CSV with explicit derived filename
    import os
    base, ext = os.path.splitext(results_file)
    csv_file = f"{base}_analysis.csv"
    generate_csv(results, csv_file)


if __name__ == '__main__':
    main()
