#!/usr/bin/env python3
"""
Generate plots from remapping benchmark results.

Usage:
    python plot_results.py [results_dir] [output_dir]

Requirements:
    pip install matplotlib pandas seaborn
"""

import json
import sys
import os
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any, Tuple

try:
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
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
# Note: Original request was orange/blue/red, but orange+red are confused by
# red-green colorblind individuals. Teal provides better distinction.
FORMAT_COLORS = {
    'POSITION_DELETE_FILE': '#2ecc71',  # Green
    'DELETION_VECTOR': '#9b59b6',       # Purple
}

# Latency breakdown colors (colorblind-safe)
PHASE_COLORS = {
    'read': '#56B4E9',   # Sky blue
    'remap': '#D55E00',  # Vermillion
    'write': '#F0E442',  # Yellow
}


def load_csv(results_dir: Path) -> pd.DataFrame:
    """Load combined results CSV."""
    csv_path = results_dir / 'combined_results.csv'
    if not csv_path.exists():
        # Run analyze_results.py first
        print(f"CSV not found. Run analyze_results.py first.")
        sys.exit(1)
    return pd.read_csv(csv_path)


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


def plot_cloud_comparison(df: pd.DataFrame, output_dir: Path) -> None:
    """Plot latency comparison across clouds."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    for idx, format_type in enumerate(['POSITION_DELETE_FILE', 'DELETION_VECTOR']):
        ax = axes[idx]
        format_df = df[df['format'] == format_type]

        # Group by cloud and num_deletes
        clouds = sorted(format_df['cloud'].unique())
        delete_counts = sorted(format_df['num_deletes'].unique())

        x = np.arange(len(delete_counts))
        width = 0.25

        for i, cloud in enumerate(clouds):
            cloud_df = format_df[format_df['cloud'] == cloud]
            means = []
            for dc in delete_counts:
                subset = cloud_df[cloud_df['num_deletes'] == dc]
                means.append(subset['latency_mean_ms'].mean() if len(subset) > 0 else 0)

            bars = ax.bar(x + i * width, means, width, label=cloud.upper(),
                         color=COLORS.get(cloud, '#666'))

        ax.set_xlabel('Number of Deletes')
        ax.set_ylabel('Latency (ms)')
        ax.set_title(format_type.replace('_', ' ').title())
        ax.set_xticks(x + width)
        ax.set_xticklabels([f'{d//1000}K' if d >= 1000 else str(d) for d in delete_counts])
        ax.legend()
        ax.grid(axis='y', alpha=0.3)

    plt.suptitle('Remapping Latency by Cloud Provider', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output_dir / 'cloud_comparison.png', dpi=150, bbox_inches='tight')
    plt.savefig(output_dir / 'cloud_comparison.pdf', bbox_inches='tight')
    plt.close()
    print(f"Saved: cloud_comparison.png/pdf")


def plot_combined_latency_breakdown(raw_df: pd.DataFrame, output_dir: Path) -> None:
    """Plot combined latency by provider with read/remap/write breakdown as stacked bars."""
    # Filter out warmup iterations
    df = raw_df[~raw_df['warmup']].copy()

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    clouds = sorted(df['cloud'].unique())

    for idx, format_type in enumerate(['POSITION_DELETE_FILE', 'DELETION_VECTOR']):
        ax = axes[idx]
        format_df = df[df['format'] == format_type]

        # Aggregate by cloud: mean of read/remap/write times in ms
        agg_data = format_df.groupby('cloud').agg({
            'read_ms': 'mean',
            'remap_ms': 'mean',
            'write_ms': 'mean',
            'latency_ms': 'mean'
        }).reindex(clouds)

        x = np.arange(len(clouds))
        width = 0.6

        # Create stacked bars
        bottom = np.zeros(len(clouds))

        # Read phase
        read_vals = agg_data['read_ms'].values
        bars_read = ax.bar(x, read_vals, width, label='Read',
                          color=PHASE_COLORS['read'], bottom=bottom)
        bottom += read_vals

        # Remap phase
        remap_vals = agg_data['remap_ms'].values
        bars_remap = ax.bar(x, remap_vals, width, label='Remap',
                           color=PHASE_COLORS['remap'], bottom=bottom)
        bottom += remap_vals

        # Write phase
        write_vals = agg_data['write_ms'].values
        bars_write = ax.bar(x, write_vals, width, label='Write',
                           color=PHASE_COLORS['write'], bottom=bottom)

        # Add total latency labels on top of bars
        total_vals = agg_data['latency_ms'].values
        for i, (total, cloud) in enumerate(zip(total_vals, clouds)):
            ax.annotate(f'{total:.0f}ms',
                       xy=(i, total + 10),
                       ha='center', va='bottom',
                       fontsize=10, fontweight='bold')

        # Customize appearance
        ax.set_xlabel('Cloud Provider', fontsize=11)
        ax.set_ylabel('Latency (ms)', fontsize=11)

        format_label = 'Position Delete Files' if format_type == 'POSITION_DELETE_FILE' else 'Deletion Vectors'
        ax.set_title(format_label, fontsize=12, fontweight='bold')

        ax.set_xticks(x)
        ax.set_xticklabels([c.upper() for c in clouds], fontsize=11)

        # Add subtle cloud color indicators at bottom
        for i, cloud in enumerate(clouds):
            ax.axhline(y=0, xmin=(i/len(clouds)) + 0.05, xmax=((i+1)/len(clouds)) - 0.05,
                      color=COLORS.get(cloud, '#666'), linewidth=4, alpha=0.8)

        if idx == 1:
            ax.legend(loc='upper right', fontsize=10)

        ax.grid(axis='y', alpha=0.3)
        ax.set_ylim(0, max(total_vals) * 1.15)  # Add headroom for labels

    plt.suptitle('Remapping Latency Breakdown by Cloud Provider', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output_dir / 'combined_latency_breakdown.png', dpi=150, bbox_inches='tight')
    plt.savefig(output_dir / 'combined_latency_breakdown.pdf', bbox_inches='tight')
    plt.close()
    print(f"Saved: combined_latency_breakdown.png/pdf")


def plot_compact_cloud_comparison(raw_df: pd.DataFrame, output_dir: Path) -> None:
    """Compact plot: grouped by delete count, then cloud, then format (DV first).

    Bar order within each group: AWS_DV, AWS_DF, Azure_DV, Azure_DF, GCP_DV, GCP_DF
    Single-column figure with narrower bars.
    """
    df = raw_df[~raw_df['warmup']].copy()

    # Single-column figure with landscape aspect ratio
    fig, ax = plt.subplots(figsize=(5, 3.5))

    clouds = sorted(df['cloud'].unique())
    # DV first, then DF
    formats = ['DELETION_VECTOR', 'POSITION_DELETE_FILE']
    format_hatches = {'POSITION_DELETE_FILE': '///', 'DELETION_VECTOR': ''}
    format_labels = {'POSITION_DELETE_FILE': 'DF', 'DELETION_VECTOR': 'DV'}

    delete_counts = sorted(df['num-deletes'].unique())

    # Aggregate by cloud, format, and num_deletes
    agg = df.groupby(['cloud', 'format', 'num-deletes'])['latency_ms'].mean()

    # Number of bars per delete count group: 3 clouds × 2 formats = 6
    n_clouds = len(clouds)
    n_formats = len(formats)
    n_bars = n_clouds * n_formats
    width = 0.06  # Half the previous width for single-column figure
    group_width = n_bars * width + 0.08  # Extra spacing between groups

    x = np.arange(len(delete_counts)) * group_width

    # Create bars: for each cloud, then each format (DV, DF)
    # Order: AWS_DV, AWS_DF, Azure_DV, Azure_DF, GCP_DV, GCP_DF
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

            bars = ax.bar(positions, values, width,
                         color=COLORS.get(cloud, '#666'),
                         hatch=format_hatches[fmt],
                         edgecolor='white', linewidth=0.5)

    # Custom legend: cloud colors and format patterns, both on left side
    from matplotlib.patches import Patch
    cloud_legend = [Patch(facecolor=COLORS.get(c, '#666'), label=c.upper()) for c in clouds]
    format_legend = [
        Patch(facecolor='gray', hatch='', edgecolor='white', label='Deletion Vector'),
        Patch(facecolor='gray', hatch='///', edgecolor='white', label='Position Delete'),
    ]
    # Cloud legend at top-left
    leg1 = ax.legend(handles=cloud_legend, loc='upper left', title='Cloud', fontsize=8)
    ax.add_artist(leg1)
    # Format legend below cloud legend (use bbox_to_anchor for positioning)
    ax.legend(handles=format_legend, loc='upper left', title='Format', fontsize=8,
              bbox_to_anchor=(0, 0.62))

    ax.set_xlabel('Number of Deletes', fontsize=10)
    ax.set_ylabel('Average Latency (ms)', fontsize=10)
    ax.set_title('Remapping Latency by Delete Count', fontsize=11, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([f'{d//1000}K' if d >= 1000 else str(d) for d in delete_counts], fontsize=9)
    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / 'cloud_comparison_compact.png', dpi=150, bbox_inches='tight')
    plt.savefig(output_dir / 'cloud_comparison_compact.pdf', bbox_inches='tight')
    plt.close()
    print(f"Saved: cloud_comparison_compact.png/pdf")


def plot_compact_latency_breakdown(raw_df: pd.DataFrame, output_dir: Path) -> None:
    """Compact plot: latency breakdown with both formats side-by-side, grouped by cloud.
    Uses cloud colors with hatching patterns for read/remap/write phases.
    Optimized for single-column figure with larger text."""
    df = raw_df[~raw_df['warmup']].copy()

    fig, ax = plt.subplots(figsize=(8, 5))

    clouds = sorted(df['cloud'].unique())
    formats = ['DELETION_VECTOR', 'POSITION_DELETE_FILE']
    format_labels = ['DV', 'Pos Del']

    # Hatching patterns for phases
    phase_hatches = {
        'read': '',       # Solid
        'remap': '///',   # Diagonal lines
        'write': '...',   # Dots
    }

    x = np.arange(len(clouds))
    width = 0.38
    bar_positions = [x - width/2, x + width/2]

    # Track max height for y-axis
    max_height = 0

    for fmt_idx, (fmt, label) in enumerate(zip(formats, format_labels)):
        format_df = df[df['format'] == fmt]

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

            bars = ax.bar(positions, phase_vals, width, bottom=bottom, hatch=hatch,
                         edgecolor='white', linewidth=0.5)

            # Color by cloud
            for bar, cloud in zip(bars, clouds):
                bar.set_facecolor(COLORS.get(cloud, '#666'))

            bottom += phase_vals

        # Add total labels
        total_vals = agg_data['latency_ms'].values
        max_height = max(max_height, max(total_vals))
        for i, total in enumerate(total_vals):
            ax.annotate(f'{total:.0f}',
                       xy=(positions[i], total + 5),
                       ha='center', va='bottom',
                       fontsize=12, fontweight='bold')

    # Custom legend for phases (using hatching)
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='gray', edgecolor='white', hatch='', label='Read'),
        Patch(facecolor='gray', edgecolor='white', hatch='///', label='Remap'),
        Patch(facecolor='gray', edgecolor='white', hatch='...', label='Write'),
    ]
    leg1 = ax.legend(handles=legend_elements, loc='upper right', title='Phase', fontsize=12,
                     title_fontsize=12)
    ax.add_artist(leg1)

    # Add format labels below x-axis
    ax.set_xticks(x)
    ax.set_xticklabels([c.upper() for c in clouds], fontsize=14, fontweight='bold')

    # Add format indicators
    for i, cloud in enumerate(clouds):
        ax.text(x[i] - width/2, -max_height * 0.08, 'DV', ha='center', va='top', fontsize=11)
        ax.text(x[i] + width/2, -max_height * 0.08, 'PD', ha='center', va='top', fontsize=11)

    ax.set_ylabel('Latency (ms)', fontsize=14)
    ax.set_title('Remapping Latency Breakdown', fontsize=14, fontweight='bold')
    ax.tick_params(axis='y', labelsize=12)
    ax.grid(axis='y', alpha=0.3)
    ax.set_ylim(0, max_height * 1.15)

    plt.tight_layout()
    plt.savefig(output_dir / 'latency_breakdown_compact.png', dpi=150, bbox_inches='tight')
    plt.savefig(output_dir / 'latency_breakdown_compact.pdf', bbox_inches='tight')
    plt.close()
    print(f"Saved: latency_breakdown_compact.png/pdf")


def plot_latency_breakdown_1m(raw_df: pd.DataFrame, output_dir: Path) -> None:
    """Latency breakdown filtered to 1M deletes only.

    Shows read/remap/write phases for the largest workload size,
    which is more representative of production scenarios.
    """
    df = raw_df[~raw_df['warmup']].copy()

    # Filter to 1M deletes only
    df = df[df['num-deletes'] == 1000000]

    if df.empty:
        print("No 1M delete data found, skipping latency_breakdown_1m plot")
        return

    fig, ax = plt.subplots(figsize=(8, 5))

    clouds = sorted(df['cloud'].unique())
    formats = ['DELETION_VECTOR', 'POSITION_DELETE_FILE']
    format_labels = ['DV', 'Pos Del']

    # Hatching patterns for phases
    phase_hatches = {
        'read': '',       # Solid
        'remap': '///',   # Diagonal lines
        'write': '...',   # Dots
    }

    x = np.arange(len(clouds))
    width = 0.38
    bar_positions = [x - width/2, x + width/2]

    # Track max height for y-axis
    max_height = 0

    for fmt_idx, (fmt, label) in enumerate(zip(formats, format_labels)):
        format_df = df[df['format'] == fmt]

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

            bars = ax.bar(positions, phase_vals, width, bottom=bottom, hatch=hatch,
                         edgecolor='white', linewidth=0.5)

            # Color by cloud
            for bar, cloud in zip(bars, clouds):
                bar.set_facecolor(COLORS.get(cloud, '#666'))

            bottom += phase_vals

        # Add total labels
        total_vals = agg_data['latency_ms'].values
        max_height = max(max_height, max(total_vals))
        for i, total in enumerate(total_vals):
            ax.annotate(f'{total:.0f}',
                       xy=(positions[i], total + 20),
                       ha='center', va='bottom',
                       fontsize=12, fontweight='bold')

    # Custom legend for phases (using hatching)
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='gray', edgecolor='white', hatch='', label='Read'),
        Patch(facecolor='gray', edgecolor='white', hatch='///', label='Remap'),
        Patch(facecolor='gray', edgecolor='white', hatch='...', label='Write'),
    ]
    leg1 = ax.legend(handles=legend_elements, loc='upper right', title='Phase', fontsize=12,
                     title_fontsize=12)
    ax.add_artist(leg1)

    # Add format labels below x-axis
    ax.set_xticks(x)
    ax.set_xticklabels([c.upper() for c in clouds], fontsize=14, fontweight='bold')

    # Add format indicators
    for i, cloud in enumerate(clouds):
        ax.text(x[i] - width/2, -max_height * 0.08, 'DV', ha='center', va='top', fontsize=11)
        ax.text(x[i] + width/2, -max_height * 0.08, 'PD', ha='center', va='top', fontsize=11)

    ax.set_ylabel('Latency (ms)', fontsize=14)
    ax.set_title('Remapping Latency Breakdown (1M Deletes)', fontsize=14, fontweight='bold')
    ax.tick_params(axis='y', labelsize=12)
    ax.grid(axis='y', alpha=0.3)
    ax.set_ylim(0, max_height * 1.15)

    plt.tight_layout()
    plt.savefig(output_dir / 'latency_breakdown_1m.png', dpi=150, bbox_inches='tight')
    plt.savefig(output_dir / 'latency_breakdown_1m.pdf', bbox_inches='tight')
    plt.close()
    print(f"Saved: latency_breakdown_1m.png/pdf")


def plot_throughput_comparison(df: pd.DataFrame, output_dir: Path) -> None:
    """Plot throughput comparison across clouds."""
    fig, ax = plt.subplots(figsize=(12, 6))

    # Pivot for grouped bar chart
    pivot_df = df.pivot_table(
        values='throughput',
        index=['format', 'num_deletes'],
        columns='cloud',
        aggfunc='mean'
    ).reset_index()

    clouds = sorted(df['cloud'].unique())
    formats = df['format'].unique()

    x_labels = []
    x_pos = []
    pos = 0

    for fmt in formats:
        fmt_df = pivot_df[pivot_df['format'] == fmt]
        for _, row in fmt_df.iterrows():
            x_labels.append(f"{row['num_deletes']//1000}K")
            x_pos.append(pos)
            pos += 1
        pos += 0.5  # Gap between formats

    width = 0.25
    for i, cloud in enumerate(clouds):
        cloud_data = []
        idx = 0
        for fmt in formats:
            fmt_df = pivot_df[pivot_df['format'] == fmt]
            for _, row in fmt_df.iterrows():
                cloud_data.append(row.get(cloud, 0) if pd.notna(row.get(cloud)) else 0)
                idx += 1
            idx += 0  # Account for gap

        positions = [p + i * width for p in x_pos]
        ax.bar(positions, cloud_data, width, label=cloud.upper(), color=COLORS.get(cloud, '#666'))

    ax.set_xlabel('Number of Deletes')
    ax.set_ylabel('Throughput (deletes/sec)')
    ax.set_title('Remapping Throughput by Cloud Provider')
    ax.set_xticks([p + width for p in x_pos])
    ax.set_xticklabels(x_labels, rotation=45)
    ax.legend()
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{x/1000:.0f}K'))
    ax.grid(axis='y', alpha=0.3)

    # Add format labels
    pos = 0
    for fmt in formats:
        fmt_count = len(pivot_df[pivot_df['format'] == fmt])
        mid = pos + fmt_count / 2 - 0.5
        ax.text(mid + width, ax.get_ylim()[1] * 0.95,
                fmt.replace('_', '\n').replace('FILE', '').strip(),
                ha='center', fontsize=9, style='italic')
        pos += fmt_count + 0.5

    plt.tight_layout()
    plt.savefig(output_dir / 'throughput_comparison.png', dpi=150, bbox_inches='tight')
    plt.savefig(output_dir / 'throughput_comparison.pdf', bbox_inches='tight')
    plt.close()
    print(f"Saved: throughput_comparison.png/pdf")


def plot_latency_breakdown(df: pd.DataFrame, output_dir: Path) -> None:
    """Plot latency breakdown (read/remap/write) by cloud."""
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    clouds = sorted(df['cloud'].unique())

    for idx, cloud in enumerate(clouds):
        ax = axes[idx]
        cloud_df = df[df['cloud'] == cloud]

        # Average percentages by format
        breakdown = cloud_df.groupby('format')[['read_pct', 'remap_pct', 'write_pct']].mean()

        categories = ['Read', 'Remap', 'Write']
        x = np.arange(len(breakdown.index))
        width = 0.6

        bottom = np.zeros(len(breakdown.index))
        colors = ['#3498db', '#e74c3c', '#2ecc71']

        for i, (col, color) in enumerate(zip(['read_pct', 'remap_pct', 'write_pct'], colors)):
            values = breakdown[col].values
            ax.bar(x, values, width, bottom=bottom, label=categories[i], color=color)
            bottom += values

        ax.set_ylabel('Percentage')
        ax.set_title(f'{cloud.upper()}')
        ax.set_xticks(x)
        ax.set_xticklabels([fmt.replace('_', '\n') for fmt in breakdown.index], fontsize=8)
        ax.set_ylim(0, 100)
        if idx == 2:
            ax.legend(loc='upper right')

    plt.suptitle('Latency Breakdown by Phase', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output_dir / 'latency_breakdown.png', dpi=150, bbox_inches='tight')
    plt.savefig(output_dir / 'latency_breakdown.pdf', bbox_inches='tight')
    plt.close()
    print(f"Saved: latency_breakdown.png/pdf")


def plot_scaling(raw_df: pd.DataFrame, output_dir: Path) -> None:
    """Plot how latency scales with number of deletes."""
    # Filter out warmup
    df = raw_df[~raw_df['warmup']]

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    for idx, format_type in enumerate(['POSITION_DELETE_FILE', 'DELETION_VECTOR']):
        ax = axes[idx]
        format_df = df[df['format'] == format_type]

        for cloud in sorted(format_df['cloud'].unique()):
            cloud_df = format_df[format_df['cloud'] == cloud]

            # Group by num_deletes
            grouped = cloud_df.groupby('num-deletes')['latency_ms'].agg(['mean', 'std'])

            ax.errorbar(grouped.index, grouped['mean'],
                       yerr=grouped['std'],
                       marker='o', label=cloud.upper(),
                       color=COLORS.get(cloud, '#666'),
                       capsize=3, capthick=1, linewidth=2, markersize=6)

        ax.set_xlabel('Number of Deletes')
        ax.set_ylabel('Latency (ms)')
        ax.set_title(format_type.replace('_', ' ').title())
        ax.set_xscale('log')
        ax.legend()
        ax.grid(True, alpha=0.3)

    plt.suptitle('Latency Scaling with Delete Count', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output_dir / 'scaling.png', dpi=150, bbox_inches='tight')
    plt.savefig(output_dir / 'scaling.pdf', bbox_inches='tight')
    plt.close()
    print(f"Saved: scaling.png/pdf")


def plot_format_comparison(df: pd.DataFrame, output_dir: Path) -> None:
    """Compare Position Delete Files vs Deletion Vectors."""
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    # Latency comparison
    ax = axes[0]
    pivot = df.pivot_table(values='latency_mean_ms', index='cloud', columns='format', aggfunc='mean')

    x = np.arange(len(pivot.index))
    width = 0.35

    for i, fmt in enumerate(pivot.columns):
        color = FORMAT_COLORS.get(fmt, '#666')
        ax.bar(x + i * width, pivot[fmt], width,
               label=fmt.replace('_', ' ').title(), color=color)

    ax.set_ylabel('Average Latency (ms)')
    ax.set_title('Latency by Format')
    ax.set_xticks(x + width / 2)
    ax.set_xticklabels([c.upper() for c in pivot.index])
    ax.legend()
    ax.grid(axis='y', alpha=0.3)

    # Throughput comparison
    ax = axes[1]
    pivot = df.pivot_table(values='throughput', index='cloud', columns='format', aggfunc='mean')

    for i, fmt in enumerate(pivot.columns):
        color = FORMAT_COLORS.get(fmt, '#666')
        ax.bar(x + i * width, pivot[fmt], width,
               label=fmt.replace('_', ' ').title(), color=color)

    ax.set_ylabel('Throughput (deletes/sec)')
    ax.set_title('Throughput by Format')
    ax.set_xticks(x + width / 2)
    ax.set_xticklabels([c.upper() for c in pivot.index])
    ax.legend()
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{x/1000:.0f}K'))
    ax.grid(axis='y', alpha=0.3)

    plt.suptitle('Position Delete Files vs Deletion Vectors', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output_dir / 'format_comparison.png', dpi=150, bbox_inches='tight')
    plt.savefig(output_dir / 'format_comparison.pdf', bbox_inches='tight')
    plt.close()
    print(f"Saved: format_comparison.png/pdf")


def plot_boxplots(raw_df: pd.DataFrame, output_dir: Path) -> None:
    """Plot distribution of latencies as boxplots."""
    df = raw_df[~raw_df['warmup']]

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    for idx, format_type in enumerate(['POSITION_DELETE_FILE', 'DELETION_VECTOR']):
        ax = axes[idx]
        format_df = df[df['format'] == format_type]

        # Prepare data for boxplot
        clouds = sorted(format_df['cloud'].unique())
        data = [format_df[format_df['cloud'] == c]['latency_ms'].values for c in clouds]

        bp = ax.boxplot(data, labels=[c.upper() for c in clouds], patch_artist=True)

        for patch, cloud in zip(bp['boxes'], clouds):
            patch.set_facecolor(COLORS.get(cloud, '#666'))
            patch.set_alpha(0.7)

        ax.set_ylabel('Latency (ms)')
        ax.set_title(format_type.replace('_', ' ').title())
        ax.grid(axis='y', alpha=0.3)

    plt.suptitle('Latency Distribution by Cloud Provider', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output_dir / 'latency_boxplots.png', dpi=150, bbox_inches='tight')
    plt.savefig(output_dir / 'latency_boxplots.pdf', bbox_inches='tight')
    plt.close()
    print(f"Saved: latency_boxplots.png/pdf")


def generate_summary_table(df: pd.DataFrame, output_dir: Path) -> None:
    """Generate a summary table as an image."""
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.axis('off')

    # Create summary table
    summary = df.groupby(['cloud', 'format']).agg({
        'latency_mean_ms': 'mean',
        'throughput': 'mean',
        'read_pct': 'mean',
        'remap_pct': 'mean',
        'write_pct': 'mean'
    }).round(1)

    # Format for display
    table_data = []
    headers = ['Cloud', 'Format', 'Avg Latency (ms)', 'Throughput', 'Read %', 'Remap %', 'Write %']

    for (cloud, fmt), row in summary.iterrows():
        table_data.append([
            cloud.upper(),
            fmt.replace('_', ' ').replace('FILE', ''),
            f"{row['latency_mean_ms']:.1f}",
            f"{row['throughput']:,.0f}",
            f"{row['read_pct']:.1f}%",
            f"{row['remap_pct']:.1f}%",
            f"{row['write_pct']:.1f}%"
        ])

    table = ax.table(cellText=table_data, colLabels=headers,
                     cellLoc='center', loc='center',
                     colColours=['#4472C4'] * len(headers))

    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.5)

    # Style header
    for i in range(len(headers)):
        table[(0, i)].set_text_props(color='white', fontweight='bold')

    plt.title('Benchmark Results Summary', fontsize=14, fontweight='bold', pad=20)
    plt.tight_layout()
    plt.savefig(output_dir / 'summary_table.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: summary_table.png")


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
        df = load_csv(results_dir)
        raw_df = load_raw_results(results_dir)
    except Exception as e:
        print(f"Error loading data: {e}")
        sys.exit(1)

    print(f"Loaded {len(df)} summary records, {len(raw_df)} raw measurements")

    # Generate plots
    print("\nGenerating plots...")
    plot_cloud_comparison(df, output_dir)
    plot_throughput_comparison(df, output_dir)
    plot_latency_breakdown(df, output_dir)
    plot_format_comparison(df, output_dir)

    if len(raw_df) > 0:
        plot_scaling(raw_df, output_dir)
        plot_boxplots(raw_df, output_dir)
        plot_combined_latency_breakdown(raw_df, output_dir)
        plot_compact_cloud_comparison(raw_df, output_dir)
        plot_compact_latency_breakdown(raw_df, output_dir)
        plot_latency_breakdown_1m(raw_df, output_dir)

    generate_summary_table(df, output_dir)

    print(f"\nAll plots saved to: {output_dir}")


if __name__ == '__main__':
    main()
