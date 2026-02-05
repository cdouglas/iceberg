#!/usr/bin/env python3
"""
Generate plots from remapping benchmark results.

Usage:
    python plot_results.py [results_dir] [output_dir]

Requirements:
    pip install matplotlib pandas numpy
"""

import json
import sys
from pathlib import Path

try:
    import matplotlib.pyplot as plt
    import pandas as pd
    import numpy as np
except ImportError:
    print("Required packages not installed. Run:")
    print("  pip install matplotlib pandas numpy")
    sys.exit(1)

# Set style
plt.style.use('seaborn-v0_8-whitegrid')

# Colorblind-safe palette (Wong palette)
# Orange/Blue/Teal are distinguishable by all forms of color vision deficiency
COLORS = {
    'aws': '#E69F00',      # Orange (colorblind-safe)
    'azure': '#0072B2',    # Blue (colorblind-safe)
    'gcp': '#009E73',      # Teal/Bluish-green (colorblind-safe)
}


def load_raw_results(results_dir: Path) -> pd.DataFrame:
    """Load raw results from JSON files."""
    records = []

    for cloud_dir in results_dir.iterdir():
        if not cloud_dir.is_dir():
            continue

        cloud_name = cloud_dir.name.split('_')[0]

        for results_file in cloud_dir.rglob('results.json'):
            with open(results_file) as f:
                data = json.load(f)
                for record in data:
                    record['cloud'] = cloud_name
                    record['latency_ms'] = record['total-latency-ns'] / 1_000_000
                    record['read_ms'] = record['read-latency-ns'] / 1_000_000
                    record['remap_ms'] = record['remap-latency-ns'] / 1_000_000
                    record['write_ms'] = record['write-latency-ns'] / 1_000_000
                    records.append(record)

    return pd.DataFrame(records)


def plot_cloud_comparison_by_runs(raw_df: pd.DataFrame, output_dir: Path) -> None:
    """Generate cloud comparison plots stratified by run count.

    Creates a separate plot for each run count value (m=10, 100, 1000, 10000),
    avoiding the meaningless averaging across different run counts.
    """
    df = raw_df[~raw_df['warmup']].copy()

    if 'num-runs' not in df.columns:
        print("No num-runs column found, skipping cloud_comparison_by_runs")
        return

    run_counts = sorted(df['num-runs'].unique())
    clouds = sorted(df['cloud'].unique())
    formats = ['DELETION_VECTOR', 'POSITION_DELETE_FILE']
    format_hatches = {'POSITION_DELETE_FILE': '///', 'DELETION_VECTOR': ''}

    for num_runs in run_counts:
        run_df = df[df['num-runs'] == num_runs]

        if run_df.empty:
            continue

        fig, ax = plt.subplots(figsize=(5, 3.5))

        delete_counts = sorted(run_df['num-deletes'].unique())

        # Aggregate by cloud, format, and num_deletes (for this specific run count)
        agg = run_df.groupby(['cloud', 'format', 'num-deletes'])['latency_ms'].mean()

        n_clouds = len(clouds)
        n_formats = len(formats)
        n_bars = n_clouds * n_formats
        width = 0.06
        group_width = n_bars * width + 0.08

        x = np.arange(len(delete_counts)) * group_width

        for cloud_idx, cloud in enumerate(clouds):
            for fmt_idx, fmt in enumerate(formats):
                bar_offset = cloud_idx * n_formats + fmt_idx
                positions = x + (bar_offset - n_bars/2 + 0.5) * width

                values = []
                for dc in delete_counts:
                    try:
                        values.append(agg.loc[(cloud, fmt, dc)])
                    except KeyError:
                        values.append(0)

                ax.bar(positions, values, width,
                      color=COLORS.get(cloud, '#666'),
                      hatch=format_hatches[fmt],
                      edgecolor='white', linewidth=0.5)

        # Legend
        from matplotlib.patches import Patch
        cloud_legend = [Patch(facecolor=COLORS.get(c, '#666'), label=c.upper()) for c in clouds]
        format_legend = [
            Patch(facecolor='gray', hatch='', edgecolor='white', label='Deletion Vector'),
            Patch(facecolor='gray', hatch='///', edgecolor='white', label='Position Delete'),
        ]
        leg1 = ax.legend(handles=cloud_legend, loc='upper left', title='Cloud', fontsize=8)
        ax.add_artist(leg1)
        ax.legend(handles=format_legend, loc='upper left', title='Format', fontsize=8,
                  bbox_to_anchor=(0, 0.62))

        ax.set_xlabel('Number of Deletes', fontsize=10)
        ax.set_ylabel('Average Latency (ms)', fontsize=10)

        # Format run count for title
        run_label = f'{num_runs//1000}K' if num_runs >= 1000 else str(num_runs)
        ax.set_title(f'Remapping Latency (m={run_label} runs)', fontsize=11, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels([f'{d//1000}K' if d >= 1000 else str(d) for d in delete_counts], fontsize=9)
        ax.grid(axis='y', alpha=0.3)

        plt.tight_layout()

        # Filename includes run count
        run_suffix = f'{num_runs//1000}k' if num_runs >= 1000 else str(num_runs)
        plt.savefig(output_dir / f'cloud_comparison_m{run_suffix}.png', dpi=150, bbox_inches='tight')
        plt.savefig(output_dir / f'cloud_comparison_m{run_suffix}.pdf', bbox_inches='tight')
        plt.close()

    print(f"Saved: cloud_comparison_m*.png/pdf for {len(run_counts)} run counts")


def plot_latency_breakdown_by_runs(raw_df: pd.DataFrame, output_dir: Path) -> None:
    """Generate latency breakdown plots stratified by run count.

    Creates a separate plot for each run count value (m=10, 100, 1000, 10000),
    filtered to 1M deletes. Avoids meaningless averaging across run counts.
    """
    df = raw_df[~raw_df['warmup']].copy()

    # Filter to 1M deletes
    df = df[df['num-deletes'] == 1000000]

    if df.empty:
        print("No 1M delete data found, skipping latency_breakdown_by_runs")
        return

    if 'num-runs' not in df.columns:
        print("No num-runs column found, skipping latency_breakdown_by_runs")
        return

    run_counts = sorted(df['num-runs'].unique())
    clouds = sorted(df['cloud'].unique())
    formats = ['DELETION_VECTOR', 'POSITION_DELETE_FILE']
    format_labels = ['DV', 'Pos Del']

    phase_hatches = {
        'read': '',
        'remap': '///',
        'write': '...',
    }

    for num_runs in run_counts:
        run_df = df[df['num-runs'] == num_runs]

        if run_df.empty:
            continue

        fig, ax = plt.subplots(figsize=(8, 5))

        x = np.arange(len(clouds))
        width = 0.38
        bar_positions = [x - width/2, x + width/2]

        max_height = 0

        for fmt_idx, (fmt, label) in enumerate(zip(formats, format_labels)):
            format_df = run_df[run_df['format'] == fmt]

            agg_data = format_df.groupby('cloud').agg({
                'read_ms': 'mean',
                'remap_ms': 'mean',
                'write_ms': 'mean',
                'latency_ms': 'mean'
            }).reindex(clouds)

            positions = bar_positions[fmt_idx]
            bottom = np.zeros(len(clouds))

            for phase, hatch in phase_hatches.items():
                phase_vals = agg_data[f'{phase}_ms'].values
                # Handle NaN values
                phase_vals = np.nan_to_num(phase_vals, nan=0.0)

                bars = ax.bar(positions, phase_vals, width, bottom=bottom, hatch=hatch,
                             edgecolor='white', linewidth=0.5)

                for bar, cloud in zip(bars, clouds):
                    bar.set_facecolor(COLORS.get(cloud, '#666'))

                bottom += phase_vals

            total_vals = agg_data['latency_ms'].values
            total_vals = np.nan_to_num(total_vals, nan=0.0)
            if len(total_vals) > 0 and max(total_vals) > 0:
                max_height = max(max_height, max(total_vals))
            for i, total in enumerate(total_vals):
                if total > 0:
                    ax.annotate(f'{total:.0f}',
                               xy=(positions[i], total + max(20, max_height * 0.02)),
                               ha='center', va='bottom',
                               fontsize=12, fontweight='bold')

        # Legend
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor='gray', edgecolor='white', hatch='', label='Read'),
            Patch(facecolor='gray', edgecolor='white', hatch='///', label='Remap'),
            Patch(facecolor='gray', edgecolor='white', hatch='...', label='Write'),
        ]
        leg1 = ax.legend(handles=legend_elements, loc='upper right', title='Phase', fontsize=12,
                         title_fontsize=12)
        ax.add_artist(leg1)

        ax.set_xticks(x)
        ax.set_xticklabels([c.upper() for c in clouds], fontsize=14, fontweight='bold')

        if max_height > 0:
            for i, cloud in enumerate(clouds):
                ax.text(x[i] - width/2, -max_height * 0.08, 'DV', ha='center', va='top', fontsize=11)
                ax.text(x[i] + width/2, -max_height * 0.08, 'PD', ha='center', va='top', fontsize=11)

        ax.set_ylabel('Latency (ms)', fontsize=14)

        run_label = f'{num_runs//1000}K' if num_runs >= 1000 else str(num_runs)
        ax.set_title(f'Latency Breakdown: 1M Deletes, m={run_label} runs', fontsize=14, fontweight='bold')
        ax.tick_params(axis='y', labelsize=12)
        ax.grid(axis='y', alpha=0.3)
        if max_height > 0:
            ax.set_ylim(0, max_height * 1.15)

        plt.tight_layout()

        run_suffix = f'{num_runs//1000}k' if num_runs >= 1000 else str(num_runs)
        plt.savefig(output_dir / f'latency_breakdown_1m_m{run_suffix}.png', dpi=150, bbox_inches='tight')
        plt.savefig(output_dir / f'latency_breakdown_1m_m{run_suffix}.pdf', bbox_inches='tight')
        plt.close()

    print(f"Saved: latency_breakdown_1m_m*.png/pdf for {len(run_counts)} run counts")


def plot_latency_heatmap(raw_df: pd.DataFrame, output_dir: Path) -> None:
    """Heatmap of remap latency by delete count × run count.

    Creates a separate subplot for each cloud provider with a common color scale.
    Uses a colorblind-friendly colormap (viridis).
    """
    df = raw_df[~raw_df['warmup']].copy()

    if 'num-runs' not in df.columns:
        print("No num-runs column found, skipping heatmap")
        return

    clouds = sorted(df['cloud'].unique())
    formats = ['POSITION_DELETE_FILE', 'DELETION_VECTOR']
    format_labels = {'POSITION_DELETE_FILE': 'Position Deletes', 'DELETION_VECTOR': 'Deletion Vectors'}

    for fmt in formats:
        format_df = df[df['format'] == fmt]

        if format_df.empty:
            continue

        # Get unique values for axes
        delete_counts = sorted(format_df['num-deletes'].unique())
        run_counts = sorted(format_df['num-runs'].unique())

        if len(delete_counts) < 2 or len(run_counts) < 2:
            print(f"Insufficient data for heatmap ({fmt}), skipping")
            continue

        # Create figure with subplots for each cloud
        # Use constrained_layout instead of tight_layout to handle colorbar properly
        n_clouds = len(clouds)
        fig, axes = plt.subplots(1, n_clouds, figsize=(5 * n_clouds, 4), squeeze=False,
                                 constrained_layout=True)
        axes = axes[0]  # Flatten to 1D

        # Compute global min/max for common color scale
        global_min = float('inf')
        global_max = float('-inf')

        # First pass: compute global range
        pivot_data = {}
        for cloud in clouds:
            cloud_df = format_df[format_df['cloud'] == cloud]
            pivot = cloud_df.pivot_table(
                values='remap_ms',
                index='num-runs',
                columns='num-deletes',
                aggfunc='mean'
            )
            pivot_data[cloud] = pivot
            if not pivot.empty:
                global_min = min(global_min, pivot.min().min())
                global_max = max(global_max, pivot.max().max())

        if global_min == float('inf'):
            print(f"No data for heatmap ({fmt}), skipping")
            continue

        # Second pass: create heatmaps with common scale
        for idx, cloud in enumerate(clouds):
            ax = axes[idx]
            pivot = pivot_data[cloud]

            if pivot.empty:
                ax.text(0.5, 0.5, 'No data', ha='center', va='center', transform=ax.transAxes)
                ax.set_title(f'{cloud.upper()}')
                ax.grid(False)
                continue

            # Reindex to ensure consistent ordering
            pivot = pivot.reindex(index=run_counts, columns=delete_counts)

            # Create heatmap using viridis (colorblind-friendly)
            im = ax.imshow(pivot.values, cmap='viridis', aspect='auto',
                          vmin=global_min, vmax=global_max)

            # Set tick labels
            ax.set_xticks(range(len(delete_counts)))
            ax.set_xticklabels([f'{d//1000}K' if d >= 1000 else str(d) for d in delete_counts],
                              fontsize=9)
            ax.set_yticks(range(len(run_counts)))
            ax.set_yticklabels([f'{r//1000}K' if r >= 1000 else str(r) for r in run_counts],
                              fontsize=9)

            ax.set_xlabel('Delete Count (n)', fontsize=10)
            ax.set_ylabel('Run Count (m)', fontsize=10)
            ax.set_title(f'{cloud.upper()}', fontsize=12, fontweight='bold')
            ax.grid(False)

            # Add value annotations
            for i in range(len(run_counts)):
                for j in range(len(delete_counts)):
                    val = pivot.values[i, j]
                    if not np.isnan(val):
                        # Choose text color based on background brightness
                        text_color = 'white' if val < (global_min + global_max) / 2 else 'black'
                        ax.text(j, i, f'{val:.1f}', ha='center', va='center',
                               fontsize=8, color=text_color)

        # Add shared colorbar
        cbar = fig.colorbar(im, ax=axes, shrink=0.8, pad=0.02)
        cbar.set_label('Remap Latency (ms)', fontsize=10)

        fmt_label = format_labels[fmt]
        fig.suptitle(f'Remap Latency Heatmap: {fmt_label}', fontsize=14, fontweight='bold')

        # Generate filename from format
        fmt_suffix = 'pd' if fmt == 'POSITION_DELETE_FILE' else 'dv'
        plt.savefig(output_dir / f'latency_heatmap_{fmt_suffix}.png', dpi=150, bbox_inches='tight')
        plt.savefig(output_dir / f'latency_heatmap_{fmt_suffix}.pdf', bbox_inches='tight')
        plt.close()
        print(f"Saved: latency_heatmap_{fmt_suffix}.png/pdf")


def main():
    # Determine directories
    if len(sys.argv) > 1:
        results_dir = Path(sys.argv[1])
    else:
        script_dir = Path(__file__).parent
        results_dir = script_dir.parent / 'results'

    if len(sys.argv) > 2:
        output_dir = Path(sys.argv[2])
    else:
        output_dir = results_dir / 'plots'

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading results from: {results_dir}")
    print(f"Saving plots to: {output_dir}")

    # Load data
    try:
        raw_df = load_raw_results(results_dir)
    except Exception as e:
        print(f"Error loading data: {e}")
        sys.exit(1)

    print(f"Loaded {len(raw_df)} raw measurements")

    if len(raw_df) == 0:
        print("No data found. Ensure results directory contains cloud provider subdirectories with results.json files.")
        sys.exit(1)

    # Generate plots (all stratified by run count to avoid meaningless averaging)
    print("\nGenerating plots...")
    plot_cloud_comparison_by_runs(raw_df, output_dir)
    plot_latency_breakdown_by_runs(raw_df, output_dir)
    plot_latency_heatmap(raw_df, output_dir)

    print(f"\nAll plots saved to: {output_dir}")


if __name__ == '__main__':
    main()
