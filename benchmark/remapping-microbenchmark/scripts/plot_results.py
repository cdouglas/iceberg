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

    Creates a separate plot for each run count value (r=10, 100, 1000, 10000),
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
        ax.set_title(f'Remapping Latency (r={run_label} runs)', fontsize=11, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels([f'{d//1000}K' if d >= 1000 else str(d) for d in delete_counts], fontsize=9)
        ax.grid(False)
        plt.tight_layout()

        # Filename includes run count
        run_suffix = str(num_runs)
        plt.savefig(output_dir / f'cloud_comparison_r{run_suffix}.png', dpi=150, bbox_inches='tight')
        plt.savefig(output_dir / f'cloud_comparison_r{run_suffix}.pdf', bbox_inches='tight')
        plt.close()

    print(f"Saved: cloud_comparison_r*.png/pdf for {len(run_counts)} run counts")


def plot_cloud_comparison_by_runs_clipped(raw_df: pd.DataFrame, output_dir: Path) -> None:
    """Generate cloud comparison plots with broken y-axis for outlier bars.

    For run counts where some bars (typically PD at high delete counts) are much
    taller than the rest, creates a variant with a broken y-axis: a tall bottom
    panel sized for the non-outlier data and a short top panel showing just the
    outlier bar tops with annotated values.

    Outliers are detected using the IQR method (> Q3 + 1.5*IQR). A plot is only
    generated when at least one outlier exists.
    """
    df = raw_df[~raw_df['warmup']].copy()

    if 'num-runs' not in df.columns:
        print("No num-runs column found, skipping cloud_comparison_by_runs_clipped")
        return

    run_counts = sorted(df['num-runs'].unique())
    clouds = sorted(df['cloud'].unique())
    formats = ['DELETION_VECTOR', 'POSITION_DELETE_FILE']
    format_hatches = {'POSITION_DELETE_FILE': '///', 'DELETION_VECTOR': ''}

    generated = 0

    for num_runs in run_counts:
        run_df = df[df['num-runs'] == num_runs]

        if run_df.empty:
            continue

        delete_counts = sorted(run_df['num-deletes'].unique())
        agg = run_df.groupby(['cloud', 'format', 'num-deletes'])['latency_ms'].mean()

        # Collect all bar values to detect outliers
        all_values = []
        bar_data = []  # (cloud, fmt, dc, value, x_pos)
        n_clouds = len(clouds)
        n_formats = len(formats)
        n_bars = n_clouds * n_formats
        width = 0.06
        group_width = n_bars * width + 0.08
        x = np.arange(len(delete_counts)) * group_width

        for cloud_idx, cloud in enumerate(clouds):
            for fmt_idx, fmt in enumerate(formats):
                bar_offset = cloud_idx * n_formats + fmt_idx
                for dc_idx, dc in enumerate(delete_counts):
                    try:
                        val = agg.loc[(cloud, fmt, dc)]
                    except KeyError:
                        val = 0
                    pos = x[dc_idx] + (bar_offset - n_bars/2 + 0.5) * width
                    bar_data.append((cloud, fmt, dc, val, pos))
                    if val > 0:
                        all_values.append(val)

        if len(all_values) < 4:
            continue

        # IQR outlier detection
        sorted_vals = sorted(all_values)
        q1 = sorted_vals[len(sorted_vals) // 4]
        q3 = sorted_vals[3 * len(sorted_vals) // 4]
        iqr = q3 - q1
        outlier_threshold = q3 + 1.5 * iqr

        outlier_values = [v for v in all_values if v > outlier_threshold]
        if not outlier_values:
            continue

        # Axis limits
        non_outlier_max = max(v for v in all_values if v <= outlier_threshold)
        bottom_top = non_outlier_max * 1.15          # top of bottom panel
        top_bottom = min(outlier_values) * 0.92       # bottom of top panel
        top_top = max(outlier_values) * 1.12          # top of top panel

        # Create broken-axis figure: short top panel, tall bottom panel
        fig, (ax_top, ax_bot) = plt.subplots(
            2, 1, sharex=True, figsize=(5, 4.0),
            gridspec_kw={'height_ratios': [1, 3], 'hspace': 0.06})

        # Draw identical bars on both axes; each clips to its own ylim
        for cloud, fmt, dc, val, pos in bar_data:
            for ax in (ax_top, ax_bot):
                ax.bar(pos, val, width,
                       color=COLORS.get(cloud, '#666'),
                       hatch=format_hatches[fmt],
                       edgecolor='white', linewidth=0.5)

        # Set axis limits
        ax_bot.set_ylim(0, bottom_top)
        ax_top.set_ylim(top_bottom, top_top)

        # Annotate outlier bars in the top panel
        for cloud, fmt, dc, val, pos in bar_data:
            if val > outlier_threshold:
                ax_top.annotate(f'{val:.0f}',
                                xy=(pos, val), xytext=(0, 3),
                                textcoords='offset points',
                                ha='center', va='bottom',
                                fontsize=7, fontweight='bold')

        # Hide the facing spines to create the break
        ax_top.spines['bottom'].set_visible(False)
        ax_bot.spines['top'].set_visible(False)
        ax_top.tick_params(bottom=False)

        # Draw diagonal break marks
        d = 0.012  # size of break marks
        kwargs = dict(color='k', clip_on=False, linewidth=0.8)
        # Top-panel break marks (bottom edge)
        ax_top.plot((-d, +d), (-d, +d), transform=ax_top.transAxes, **kwargs)
        ax_top.plot((1 - d, 1 + d), (-d, +d), transform=ax_top.transAxes, **kwargs)
        # Bottom-panel break marks (top edge)
        ax_bot.plot((-d, +d), (1 - d, 1 + d), transform=ax_bot.transAxes, **kwargs)
        ax_bot.plot((1 - d, 1 + d), (1 - d, 1 + d), transform=ax_bot.transAxes, **kwargs)

        # Legend — Cloud in top panel (mostly empty), Format in bottom panel
        from matplotlib.patches import Patch
        cloud_legend = [Patch(facecolor=COLORS.get(c, '#666'), label=c.upper()) for c in clouds]
        format_legend = [
            Patch(facecolor='gray', hatch='', edgecolor='white', label='Deletion Vector'),
            Patch(facecolor='gray', hatch='///', edgecolor='white', label='Position Delete'),
        ]
        ax_top.legend(handles=cloud_legend, loc='upper left', title='Cloud', fontsize=8)
        ax_bot.legend(handles=format_legend, loc='upper left', title='Format', fontsize=8)

        # Labels and title
        ax_bot.set_xlabel('Number of Deletes', fontsize=10)
        fig.text(0.01, 0.5, 'Average Latency (ms)', va='center', rotation='vertical', fontsize=10)

        run_label = f'{num_runs//1000}K' if num_runs >= 1000 else str(num_runs)
        ax_top.set_title(f'Remapping Latency (r={run_label} runs)', fontsize=11, fontweight='bold')
        ax_bot.set_xticks(x)
        ax_bot.set_xticklabels([f'{d//1000}K' if d >= 1000 else str(d) for d in delete_counts], fontsize=9)
        ax_top.grid(False)
        ax_bot.grid(False)

        plt.savefig(output_dir / f'cloud_comparison_r{str(num_runs)}_detail.png',
                    dpi=150, bbox_inches='tight')
        plt.savefig(output_dir / f'cloud_comparison_r{str(num_runs)}_detail.pdf',
                    bbox_inches='tight')
        plt.close()
        generated += 1

    if generated > 0:
        print(f"Saved: cloud_comparison_r*_detail.png/pdf for {generated} run counts with outliers")
    else:
        print("No run counts had outlier bars; no detail plots generated")


def plot_latency_breakdown_by_runs(raw_df: pd.DataFrame, output_dir: Path) -> None:
    """Generate latency breakdown plots stratified by run count.

    Creates a separate plot for each run count value (r=10, 100, 1000, 10000),
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
        leg1 = ax.legend(handles=legend_elements, loc='upper left', title='Phase', fontsize=12,
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
        ax.set_title(f'Latency Breakdown: 1000K Deletes, r={run_label} runs', fontsize=14, fontweight='bold')
        ax.tick_params(axis='y', labelsize=12)
        ax.grid(False)
        if max_height > 0:
            ax.set_ylim(0, max_height * 1.15)

        plt.tight_layout()

        run_suffix = str(num_runs)
        plt.savefig(output_dir / f'latency_breakdown_1m_r{run_suffix}.png', dpi=150, bbox_inches='tight')
        plt.savefig(output_dir / f'latency_breakdown_1m_r{run_suffix}.pdf', bbox_inches='tight')
        plt.close()

    print(f"Saved: latency_breakdown_1m_r*.png/pdf for {len(run_counts)} run counts")


def plot_latency_heatmap(raw_df: pd.DataFrame, output_dir: Path, metric: str = 'remap_ms') -> None:
    """Heatmap of latency by delete count (p) × run count (r).

    Args:
        raw_df: Raw benchmark results DataFrame
        output_dir: Directory to save plots
        metric: Column to plot - 'remap_ms' for remap only, 'latency_ms' for total

    Creates a separate subplot for each cloud provider with a common color scale.
    Uses a colorblind-friendly colormap (viridis).
    """
    df = raw_df[~raw_df['warmup']].copy()

    if 'num-runs' not in df.columns:
        print("No num-runs column found, skipping heatmap")
        return

    # Configure labels based on metric
    if metric == 'remap_ms':
        metric_label = 'Remap Latency'
        file_prefix = 'latency_heatmap'
    else:
        metric_label = 'Total Latency'
        file_prefix = 'total_latency_heatmap'

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
                values=metric,
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
            # origin='lower' puts lowest values (row 0) at the bottom, closer to origin
            im = ax.imshow(pivot.values, cmap='viridis', aspect='auto',
                          vmin=global_min, vmax=global_max, origin='lower')

            # Set tick labels
            ax.set_xticks(range(len(delete_counts)))
            ax.set_xticklabels([f'{d//1000}K' if d >= 1000 else str(d) for d in delete_counts],
                              fontsize=9)
            ax.set_yticks(range(len(run_counts)))
            ax.set_yticklabels([f'{r//1000}K' if r >= 1000 else str(r) for r in run_counts],
                              fontsize=9)

            ax.set_xlabel('Delete Count (p)', fontsize=10)
            ax.set_ylabel('Run Count (r)', fontsize=10)
            ax.set_title(f'{cloud.upper()}', fontsize=12, fontweight='bold')
            ax.grid(False)

            # Add value annotations (note: with origin='lower', row i is at y=i from bottom)
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
        cbar.set_label(f'{metric_label} (ms)', fontsize=10)

        fmt_label = format_labels[fmt]
        fig.suptitle(f'{metric_label} Heatmap: {fmt_label}', fontsize=14, fontweight='bold')

        # Generate filename from format
        fmt_suffix = 'pd' if fmt == 'POSITION_DELETE_FILE' else 'dv'
        plt.savefig(output_dir / f'{file_prefix}_{fmt_suffix}.png', dpi=150, bbox_inches='tight')
        plt.savefig(output_dir / f'{file_prefix}_{fmt_suffix}.pdf', bbox_inches='tight')
        plt.close()
        print(f"Saved: {file_prefix}_{fmt_suffix}.png/pdf")


def plot_latency_heatmap_per_cloud(raw_df: pd.DataFrame, output_dir: Path, metric: str = 'latency_ms') -> None:
    """Generate separate heatmap for each cloud provider with consistent scale.

    Args:
        raw_df: Raw benchmark results DataFrame
        output_dir: Directory to save plots
        metric: Column to plot - 'remap_ms' for remap only, 'latency_ms' for total

    Creates individual plots for each cloud, all using the same color scale
    so they can be compared when placed side-by-side or on separate pages.
    """
    df = raw_df[~raw_df['warmup']].copy()

    if 'num-runs' not in df.columns:
        print("No num-runs column found, skipping per-cloud heatmaps")
        return

    # Configure labels based on metric
    if metric == 'remap_ms':
        metric_label = 'Remap Latency'
        file_prefix = 'latency_heatmap'
    else:
        metric_label = 'Total Latency'
        file_prefix = 'total_latency_heatmap'

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
            print(f"Insufficient data for per-cloud heatmap ({fmt}), skipping")
            continue

        # Compute global min/max across ALL clouds for consistent scale
        global_min = float('inf')
        global_max = float('-inf')

        pivot_data = {}
        for cloud in clouds:
            cloud_df = format_df[format_df['cloud'] == cloud]
            pivot = cloud_df.pivot_table(
                values=metric,
                index='num-runs',
                columns='num-deletes',
                aggfunc='mean'
            )
            pivot_data[cloud] = pivot
            if not pivot.empty:
                global_min = min(global_min, pivot.min().min())
                global_max = max(global_max, pivot.max().max())

        if global_min == float('inf'):
            print(f"No data for per-cloud heatmap ({fmt}), skipping")
            continue

        fmt_suffix = 'pd' if fmt == 'POSITION_DELETE_FILE' else 'dv'
        fmt_label = format_labels[fmt]

        # Generate separate figure for each cloud
        for cloud in clouds:
            pivot = pivot_data[cloud]

            if pivot.empty:
                continue

            # Reindex to ensure consistent ordering
            pivot = pivot.reindex(index=run_counts, columns=delete_counts)

            # Create single-cloud figure
            fig, ax = plt.subplots(figsize=(6, 5), constrained_layout=True)

            # Create heatmap using viridis (colorblind-friendly)
            # origin='lower' puts lowest values (row 0) at the bottom, closer to origin
            im = ax.imshow(pivot.values, cmap='viridis', aspect='auto',
                          vmin=global_min, vmax=global_max, origin='lower')

            # Set tick labels
            ax.set_xticks(range(len(delete_counts)))
            ax.set_xticklabels([f'{d//1000}K' if d >= 1000 else str(d) for d in delete_counts],
                              fontsize=10)
            ax.set_yticks(range(len(run_counts)))
            ax.set_yticklabels([f'{r//1000}K' if r >= 1000 else str(r) for r in run_counts],
                              fontsize=10)

            ax.set_xlabel('Delete Count (p)', fontsize=11)
            ax.set_ylabel('Run Count (r)', fontsize=11)
            ax.set_title(f'{metric_label}: {cloud.upper()} ({fmt_label})', fontsize=12, fontweight='bold')
            ax.grid(False)

            # Add value annotations (note: with origin='lower', row i is at y=i from bottom)
            for i in range(len(run_counts)):
                for j in range(len(delete_counts)):
                    val = pivot.values[i, j]
                    if not np.isnan(val):
                        text_color = 'white' if val < (global_min + global_max) / 2 else 'black'
                        ax.text(j, i, f'{val:.0f}', ha='center', va='center',
                               fontsize=9, color=text_color)

            # Add colorbar
            cbar = fig.colorbar(im, ax=ax, shrink=0.8)
            cbar.set_label(f'{metric_label} (ms)', fontsize=10)

            # Save
            plt.savefig(output_dir / f'{file_prefix}_{fmt_suffix}_{cloud}.png', dpi=150, bbox_inches='tight')
            plt.savefig(output_dir / f'{file_prefix}_{fmt_suffix}_{cloud}.pdf', bbox_inches='tight')
            plt.close()

        print(f"Saved: {file_prefix}_{fmt_suffix}_{{cloud}}.png/pdf for {len(clouds)} clouds")


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
    plot_cloud_comparison_by_runs_clipped(raw_df, output_dir)
    plot_latency_breakdown_by_runs(raw_df, output_dir)
    plot_latency_heatmap(raw_df, output_dir, metric='remap_ms')
    plot_latency_heatmap(raw_df, output_dir, metric='latency_ms')
    plot_latency_heatmap_per_cloud(raw_df, output_dir, metric='latency_ms')

    print(f"\nAll plots saved to: {output_dir}")


if __name__ == '__main__':
    main()
