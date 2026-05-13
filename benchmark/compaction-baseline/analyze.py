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

"""Parse compaction-baseline results.jsonl and emit summary CSV + log-log plot.

Per COMPACT_SPEC.md §Reporting, the analyzer reads `results.jsonl` (one line per
timed iteration written by RunMain) and produces:

- `summary.csv` with min / median / max wall-clock per (variant, K) cell, plus
  iteration counts and validity flags.
- `wallclock.png` — log-log plot of K vs median wall-clock ms, baseline and
  treatment as separate series with min/max error bars.

Warmup iterations (`warmup: true`) are excluded from the aggregation. Invalid
iterations (`valid: false`) are reported but not aggregated — the count of
invalid rows per cell is included in the CSV so a reviewer can see if a cell's
median is trustworthy.

Usage:
    python3 analyze.py <results.jsonl> [--output-dir DIR]

The plot generation requires matplotlib. If it is not available the CSV is
still produced and the script exits 0 with a warning.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


def read_jsonl(path: Path) -> List[dict]:
    """Read a JSON Lines file. Skips blank lines; raises on malformed lines."""
    records: List[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise SystemExit(
                    f"{path}:{lineno}: malformed JSON: {e.msg}"
                ) from e
    return records


def aggregate(
    records: Iterable[dict],
) -> Dict[Tuple[str, int], Dict[str, object]]:
    """Group records by (variant, k) and compute min/median/max of wall_clock_ms.

    Excludes warmup iterations. Tracks valid vs invalid count separately so a
    reviewer can see whether a cell's median is trustworthy.
    """
    cells: Dict[Tuple[str, int], Dict[str, object]] = defaultdict(
        lambda: {
            "iterations": 0,
            "valid_iterations": 0,
            "invalid_iterations": 0,
            "wall_clock_ms_values": [],
        }
    )

    for r in records:
        if r.get("warmup"):
            # Warmup is by-design excluded from the headline numbers.
            continue
        key = (r["variant"], r["k"])
        cell = cells[key]
        cell["iterations"] += 1
        if r.get("valid", True):
            cell["valid_iterations"] += 1
            cell["wall_clock_ms_values"].append(r["wall_clock_ms"])
        else:
            cell["invalid_iterations"] += 1

    summary: Dict[Tuple[str, int], Dict[str, object]] = {}
    for key, cell in sorted(cells.items()):
        values = cell["wall_clock_ms_values"]
        if values:
            summary[key] = {
                "iterations": cell["iterations"],
                "valid_iterations": cell["valid_iterations"],
                "invalid_iterations": cell["invalid_iterations"],
                "min_ms": min(values),
                "median_ms": statistics.median(values),
                "max_ms": max(values),
            }
        else:
            summary[key] = {
                "iterations": cell["iterations"],
                "valid_iterations": 0,
                "invalid_iterations": cell["invalid_iterations"],
                "min_ms": None,
                "median_ms": None,
                "max_ms": None,
            }
    return summary


def write_summary_csv(
    summary: Dict[Tuple[str, int], Dict[str, object]], out_path: Path
) -> None:
    fieldnames = [
        "variant",
        "k",
        "iterations",
        "valid_iterations",
        "invalid_iterations",
        "min_ms",
        "median_ms",
        "max_ms",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for (variant, k), row in summary.items():
            writer.writerow({"variant": variant, "k": k, **row})


def write_plot(
    summary: Dict[Tuple[str, int], Dict[str, object]], out_path: Path
) -> None:
    """Log-log plot of K vs median wall-clock per variant, with min/max error bars."""
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print(
            "matplotlib not installed — skipping plot. Install with:",
            "  pip install matplotlib",
            sep="\n  ",
            file=sys.stderr,
        )
        return

    # Build per-variant series.
    series: Dict[str, Dict[str, List[float]]] = defaultdict(
        lambda: {"k": [], "median": [], "min": [], "max": []}
    )
    for (variant, k), row in summary.items():
        if row["median_ms"] is None:
            continue
        series[variant]["k"].append(k)
        series[variant]["median"].append(row["median_ms"])
        series[variant]["min"].append(row["min_ms"])
        series[variant]["max"].append(row["max_ms"])

    if not series:
        print(
            "No valid iterations found; not generating plot.",
            file=sys.stderr,
        )
        return

    fig, ax = plt.subplots(figsize=(7, 5))
    for variant, data in sorted(series.items()):
        ks = data["k"]
        medians = data["median"]
        # Error bars: distance from median to min/max, in median's own units.
        # matplotlib expects [(median - min), (max - median)].
        lower_err = [m - lo for m, lo in zip(medians, data["min"])]
        upper_err = [hi - m for m, hi in zip(medians, data["max"])]
        ax.errorbar(
            ks,
            medians,
            yerr=[lower_err, upper_err],
            marker="o",
            capsize=4,
            label=variant,
        )
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Late-transaction deletes (K)")
    ax.set_ylabel("Wall-clock (ms, log scale)")
    ax.set_title("Compaction reconciliation: baseline vs. treatment")
    ax.grid(True, which="both", linestyle="--", alpha=0.4)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("results", type=Path, help="Path to results.jsonl")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for summary.csv and wallclock.png (default: results' parent)",
    )
    args = parser.parse_args()

    if not args.results.is_file():
        print(f"Not a file: {args.results}", file=sys.stderr)
        return 1

    out_dir = args.output_dir or args.results.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    records = read_jsonl(args.results)
    summary = aggregate(records)

    if not summary:
        print(
            f"No non-warmup iterations found in {args.results}; nothing to summarize.",
            file=sys.stderr,
        )
        return 0

    csv_path = out_dir / "summary.csv"
    write_summary_csv(summary, csv_path)
    print(f"Wrote {csv_path}")

    plot_path = out_dir / "wallclock.png"
    write_plot(summary, plot_path)
    if plot_path.is_file():
        print(f"Wrote {plot_path}")

    # Print a one-line summary per cell to stdout so a reviewer can spot-check
    # the headline numbers without opening the CSV.
    print("\nPer-cell summary (variant, k, median_ms [min..max], iterations):")
    for (variant, k), row in summary.items():
        if row["median_ms"] is None:
            print(
                f"  {variant} k={k}: no valid iterations "
                f"({row['invalid_iterations']} invalid of {row['iterations']})"
            )
        else:
            print(
                f"  {variant} k={k}: {row['median_ms']:.1f} ms "
                f"[{row['min_ms']:.1f}..{row['max_ms']:.1f}] "
                f"({row['valid_iterations']}/{row['iterations']} valid)"
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
