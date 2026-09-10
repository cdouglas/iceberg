#!/usr/bin/env python3
"""
Generate plots from the snapshot-rewrite space sweeps.

Usage:
    python plot_results.py [results_dir] [output_dir]

Requirements:
    pip install matplotlib pandas

Figures are deliberately spare: what each one shows, and the parameters behind
it, are written up in EXPERIMENTS.md rather than crowded onto the axes.
"""

import sys
from pathlib import Path

try:
    import matplotlib
    import matplotlib.pyplot as plt
    import pandas as pd
except ImportError:
    print("Required packages not installed. Run:")
    print("  pip install matplotlib pandas")
    sys.exit(1)

# Embed TrueType outlines instead of matplotlib's default Type 3 bitmaps
# (ACM proceedings require scalable fonts).
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

# Set style
plt.style.use('seaborn-v0_8-whitegrid')

# Colorblind-safe palette (Wong palette), matching the remapping benchmark's plots.
# Blue is the space you end up holding and orange is what recovery costs, in both
# of the mix figures, so those two hues carry the same meaning across the pair.
BLUE = '#0072B2'
ORANGE = '#E69F00'
TEAL = '#009E73'
PURPLE = '#CC79A7'

MIB = 1024.0 * 1024.0


def save(fig, output_dir: Path, name: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_dir / f'{name}.png', dpi=150, bbox_inches='tight')
    fig.savefig(output_dir / f'{name}.pdf', bbox_inches='tight')
    plt.close(fig)
    print(f"  {output_dir / name}.{{png,pdf}}")


def plot_mix_reclaimed(df: pd.DataFrame, output_dir: Path) -> None:
    """Of the bytes the rewrite frees, what it keeps and what recovery costs."""
    saved = df.saved_bytes / MIB
    recovered = (df.resurrected_bytes + df.delete_bytes + df.metadata_bytes) / MIB
    x = list(range(len(df)))

    fig, ax = plt.subplots(figsize=(5, 3.5))
    bar = dict(width=0.7, edgecolor='white', linewidth=0.5)
    ax.bar(x, saved, label='reclaimed', color=BLUE, **bar)
    ax.bar(x, recovered, bottom=saved, label='spent recovering dead rows',
           color=ORANGE, **bar)

    ax.set_ylabel('Space freed by the rewrite (MiB)', fontsize=10)
    ax.set_xlabel('Share of window row-changes that are deletions', fontsize=10)
    ax.set_title('Space Reclaimed by Deletion Share', fontsize=11, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([f'{s:.2f}' for s in df.delete_share], fontsize=9)
    ax.legend(loc='upper right', fontsize=8)
    ax.grid(False)
    ax.tick_params(labelsize=9)

    # Direct-label the ends only; the axis carries the rest.
    for i in (0, len(df) - 1):
        ax.annotate(f'{saved.iloc[i]:.0f}', (i, saved.iloc[i]), ha='center', va='bottom',
                    xytext=(0, 3), textcoords='offset points', fontsize=8,
                    fontweight='bold')

    save(fig, output_dir, 'mix_space_reclaimed')


def plot_mix_before_after(df: pd.DataFrame, output_dir: Path) -> None:
    """What the history holds down before the rewrite and after it.

    Before, the table pins the earlier compaction, everything the window wrote on
    top of it, and the new compaction. After, C_A and the original interstitial
    files are unreachable; what stands in their place is the rows the window
    deleted, written back out, plus the delete vectors.
    """
    x = list(range(len(df)))
    width = 0.36
    before = [i - width / 2 for i in x]
    after = [i + width / 2 for i in x]

    ca = df.source_compaction_bytes / MIB
    inter = df.interstitial_bytes / MIB
    target = df.target_bytes / MIB
    replacement = (df.resurrected_bytes + df.delete_bytes + df.metadata_bytes) / MIB

    fig, ax = plt.subplots(figsize=(6.5, 3.6))
    bar = dict(width=width, edgecolor='white', linewidth=0.5)

    ax.bar(before, ca, label='$C_A$ (earlier compaction)', color=PURPLE, **bar)
    ax.bar(before, inter, bottom=ca, label='interstitial data + deletes',
           color=TEAL, **bar)
    ax.bar(before, target, bottom=ca + inter, label='$C_B$ (new compaction)',
           color=BLUE, **bar)

    ax.bar(after, replacement, label='recovered rows + vectors', color=ORANGE, **bar)
    ax.bar(after, target, bottom=replacement, color=BLUE, **bar)

    ax.set_ylabel('Bytes the history pins (MiB)', fontsize=10)
    ax.set_xlabel('Share of window row-changes that are deletions\n'
                  '(left bar: before the rewrite, right bar: after)', fontsize=10)
    ax.set_title('Storage Before and After the Rewrite', fontsize=11, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([f'{s:.2f}' for s in df.delete_share], fontsize=9)
    ax.legend(loc='upper right', fontsize=8)
    ax.grid(False)
    ax.tick_params(labelsize=9)

    # Which bar is which goes in the axis label rather than as in-plot text: at eight
    # groups there is no room beside the bars that does not collide with one.
    ax.set_ylim(0, max(ca + inter + target) * 1.08)

    save(fig, output_dir, 'mix_before_after')


def plot_depth(df: pd.DataFrame, output_dir: Path) -> None:
    """Whether an insert-only window's win survives depth, by row width.

    Small multiples rather than one axis: the two schemas differ by two orders of
    magnitude in bytes, and sharing a scale would hide the crossing entirely.
    Stratified by format version and row width rather than averaged over them, per
    PLOTTING_ERRATA.md in the remapping benchmark.
    """
    fig, axes = plt.subplots(1, 2, figsize=(7, 3.2), sharex=True)

    for ax, cols, label in zip(axes, (20, 2), ('20-column rows', '2-column rows')):
        for version, colour in ((3, BLUE), (2, ORANGE)):
            series = df[(df.format_version == version)
                        & (df.schema_cols == cols)].sort_values('window_commits')
            ax.plot(series.window_commits, series.saved_bytes / MIB, color=colour,
                    linewidth=1.5, marker='o', markersize=4, label=f'v{version}')

        ax.axhline(0, color='k', linewidth=0.8, zorder=1)
        ax.set_xscale('log', base=2)
        ax.set_xticks(sorted(df.window_commits.unique()))
        ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
        ax.get_xaxis().set_minor_formatter(matplotlib.ticker.NullFormatter())
        ax.set_title(label, fontsize=10)
        ax.set_xlabel('Commits retained (m)', fontsize=10)
        ax.legend(loc='lower left', fontsize=8)
        ax.grid(False)
        ax.tick_params(labelsize=9)

    axes[0].set_ylabel('Space reclaimed (MiB)', fontsize=10)
    fig.suptitle('Insert-Only Window: Reclaim vs Window Depth', fontsize=11,
                 fontweight='bold')
    fig.tight_layout()
    save(fig, output_dir, 'depth_reclaim_by_format_version')


def main() -> None:
    results_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / 'results'
    output_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else results_dir / 'plots'

    print(f"Reading {results_dir}")
    mix = results_dir / 'mix.csv'
    if mix.exists():
        df = pd.read_csv(mix).sort_values('delete_share')
        plot_mix_reclaimed(df, output_dir)
        plot_mix_before_after(df, output_dir)
    else:
        print("  no mix.csv, skipping")

    depth = results_dir / 'depth.csv'
    if depth.exists():
        plot_depth(pd.read_csv(depth), output_dir)
    else:
        print("  no depth.csv, skipping")


if __name__ == '__main__':
    main()
