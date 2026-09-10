#!/usr/bin/env python3
"""
Generate plots from the snapshot-rewrite space sweeps.

Usage:
    python plot_results.py [results_dir] [output_dir]

Requirements:
    pip install matplotlib pandas
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
COLORS = {
    'kept': '#0072B2',       # Blue
    'recovered': '#E69F00',  # Orange
    'metadata': '#009E73',   # Teal/Bluish-green
}

# One hue per format version, one dash pattern per schema width: the claim is that
# the delete term is a v2 problem and not a row-width problem, so the two factors
# have to be readable apart.
VERSION_COLORS = {3: '#0072B2', 2: '#E69F00'}
WIDTH_STYLES = {20: '-', 2: '--'}

MIB = 1024.0 * 1024.0


def save(fig, output_dir: Path, name: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_dir / f'{name}.png', dpi=150, bbox_inches='tight')
    fig.savefig(output_dir / f'{name}.pdf', bbox_inches='tight')
    plt.close(fig)
    print(f"  {output_dir / name}.{{png,pdf}}")


def plot_mix(csv: Path, output_dir: Path) -> None:
    """What the rewrite frees and what it spends, as the window shifts to deletions.

    Of the bytes the rewrite frees, the two segments are what it keeps and what it
    spends recovering rows that died in the window and so are absent from the
    compaction. Delete vectors and metadata are the third term and are left out of
    the bars because they would not render; the note states their bound instead.

    Percentage is a separate panel rather than a second y-axis: two scales on one
    plot invent an alignment that is not in the data.
    """
    df = pd.read_csv(csv).sort_values('delete_share')

    saved = df.saved_bytes / MIB
    recovered = df.resurrected_bytes / MIB
    overhead = (df.delete_bytes + df.metadata_bytes) / MIB
    predicted = df.predicted_saved_bytes / MIB
    pct = 100.0 * df.saved_bytes / df.pinned_before_bytes
    x = list(range(len(df)))

    fig, (top, bot) = plt.subplots(
        2, 1, sharex=True, figsize=(5, 4.4),
        gridspec_kw={'height_ratios': [2.2, 1], 'hspace': 0.12})

    bar = dict(width=0.7, edgecolor='white', linewidth=0.5)
    top.bar(x, saved, label='kept (net reclaimed)', color=COLORS['kept'], **bar)
    top.bar(x, recovered, bottom=saved, label='spent recovering dead rows',
            color=COLORS['recovered'], **bar)

    # The design predicts the saving is one copy of the compacted table. Drawing it
    # per bar makes the claim falsifiable on the slide rather than asserted beside it.
    for i, value in enumerate(predicted):
        top.plot([i - 0.35, i + 0.35], [value, value], color='k', linewidth=0.9,
                 zorder=5, label='predicted (one table copy)' if i == 0 else None)

    top.set_ylabel('Reclaimed (MiB)', fontsize=10)
    top.set_title('Snapshot Rewrite Savings by Deletion Share', fontsize=11,
                  fontweight='bold', pad=16)
    top.annotate(
        f'{int(df.base_rows.iloc[0]) // 1000}K base rows, '
        f'{int(df.window_commits.iloc[0])}-commit window, '
        f'{int(df.base_rows.iloc[0]) // 1000}K row-changes, v3, 20-column',
        (0.0, 1.02), xycoords='axes fraction', va='bottom', fontsize=7.5, color='grey')
    top.legend(loc='upper right', fontsize=8)
    top.grid(False)
    top.tick_params(labelsize=9)

    # Direct-label the ends only; the axis carries the rest.
    for i in (0, len(df) - 1):
        top.annotate(f'{saved.iloc[i]:.0f}', (i, saved.iloc[i]), ha='center',
                     va='bottom', xytext=(0, 3), textcoords='offset points',
                     fontsize=8, fontweight='bold')

    bot.plot(x, pct, color=COLORS['kept'], linewidth=1.5, marker='o', markersize=4)
    bot.set_ylabel('% of pinned\nhistory', fontsize=10)
    bot.set_xlabel('Share of window row-changes that are deletions', fontsize=10)
    bot.set_xticks(x)
    bot.set_xticklabels([f'{s:.2f}' for s in df.delete_share], fontsize=9)
    bot.set_ylim(0, max(pct) * 1.45)
    bot.grid(False)
    bot.tick_params(labelsize=9)

    # deadRatio = 0.5d / (1 - d) here, so the 0.5 default is reached at exactly d=0.5.
    shares = list(df.delete_share)
    if 0.5 in shares:
        guard = shares.index(0.5)
        bot.axvline(guard, color='grey', linewidth=0.8, zorder=0)
        bot.annotate('dead-ratio 0.5:\ndefault guard refuses past here',
                     (guard, max(pct) * 1.42), ha='center', va='top', fontsize=7,
                     color='grey')

    top.annotate(
        f'delete vectors + metadata: {overhead.min():.2f}-{overhead.max():.2f} MiB, under '
        f'{100 * (overhead / (df.reclaimable_bytes / MIB)).max():.1f}% of what is freed',
        (0.0, -0.03), xycoords='axes fraction', va='top', fontsize=7, color='grey')

    fig.align_ylabels([top, bot])
    save(fig, output_dir, 'mix_savings_by_deletion_share')


def plot_depth(csv: Path, output_dir: Path) -> None:
    """What retaining one more snapshot costs, by format version and row width.

    The window is insert-only, so nothing is recovered and the delete files plus the
    per-snapshot metadata are the entire cost. Plotted as a share of pinned history
    rather than MiB so all four series share one axis: the 2-column and 20-column
    tables differ by two orders of magnitude in bytes and the crossing would vanish.

    Series are stratified by format version and row width rather than averaged over
    them, per PLOTTING_ERRATA.md in the remapping benchmark: averaging across a
    parameter that changes the cost class produces a meaningless curve.
    """
    df = pd.read_csv(csv)
    df['pct'] = 100.0 * df.saved_bytes / df.pinned_before_bytes

    fig, (top, bot) = plt.subplots(
        2, 1, sharex=True, figsize=(5, 5.0),
        gridspec_kw={'height_ratios': [1.3, 1], 'hspace': 0.35})

    for version in (3, 2):
        for cols in (20, 2):
            series = df[(df.format_version == version)
                        & (df.schema_cols == cols)].sort_values('window_commits')
            top.plot(series.window_commits, series.pct, color=VERSION_COLORS[version],
                     linestyle=WIDTH_STYLES[cols], linewidth=1.5, marker='o',
                     markersize=4, label=f'v{version}, {cols}-column')
            # Label only the two series that move; the flat pair would collide.
            if cols == 2:
                end = series.iloc[-1]
                top.annotate(f'{end.pct:.0f}%', (end.window_commits, end.pct),
                             xytext=(6, -3), textcoords='offset points', fontsize=8,
                             fontweight='bold')

    top.axhline(0, color='k', linewidth=0.8, zorder=1)
    top.set_ylabel('% of pinned\nhistory', fontsize=10)
    top.set_title('Cost of Window Depth by Format Version', fontsize=11,
                  fontweight='bold', pad=16)
    top.annotate(
        f'insert-only window, {int(df.base_rows.iloc[0]) // 1000}K base rows, '
        f'{int(df.window_inserts.iloc[0] / df.window_commits.iloc[0])} rows per commit',
        (0.0, 1.02), xycoords='axes fraction', va='bottom', fontsize=7.5, color='grey')
    top.legend(loc='lower left', fontsize=8, ncol=2)
    top.grid(False)
    top.tick_params(labelsize=9)
    top.annotate('below zero the rewrite costs more than it frees',
                 (df.window_commits.min(), 0), xytext=(4, 5),
                 textcoords='offset points', ha='left', fontsize=7, color='grey')

    # Per rewritten snapshot rather than per window: that is the unit in which a v3
    # vector is genuinely flat, and it is the number a reader can carry away.
    wide = df[df.schema_cols == 20].sort_values('window_commits')
    for version in (2, 3):
        series = wide[wide.format_version == version]
        bot.plot(series.window_commits,
                 series.delete_bytes / (series.window_commits + 1) / 1024.0,
                 color=VERSION_COLORS[version], linewidth=1.5, marker='o',
                 markersize=4, label=f'v{version} delete file')

    meta = wide[wide.format_version == 3]
    bot.plot(meta.window_commits, meta.metadata_bytes / (meta.window_commits + 1) / 1024.0,
             color=COLORS['metadata'], linewidth=1.5, marker='o', markersize=4,
             label='manifest + manifest list')

    bot.set_yscale('log')
    bot.set_xscale('log', base=2)
    bot.set_ylabel('KiB per\nsnapshot', fontsize=10)
    bot.set_xlabel('Commits retained in the window (m)', fontsize=10)
    bot.set_xticks(sorted(df.window_commits.unique()))
    bot.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    bot.get_xaxis().set_minor_formatter(matplotlib.ticker.NullFormatter())
    bot.set_xlim(df.window_commits.min() * 0.85, df.window_commits.max() * 1.4)
    lower, upper = bot.get_ylim()
    bot.set_ylim(lower, upper * 12)
    bot.legend(loc='upper left', fontsize=8)
    bot.grid(False)
    bot.tick_params(labelsize=9)
    bot.set_title('Per-snapshot cost, 20-column rows', fontsize=9)

    fig.align_ylabels([top, bot])
    save(fig, output_dir, 'depth_cost_by_format_version')


def main() -> None:
    results_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / 'results'
    output_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else results_dir / 'plots'

    print(f"Reading {results_dir}")
    for name, fn in (('mix.csv', plot_mix), ('depth.csv', plot_depth)):
        csv = results_dir / name
        if csv.exists():
            fn(csv, output_dir)
        else:
            print(f"  no {name}, skipping")


if __name__ == '__main__':
    main()
