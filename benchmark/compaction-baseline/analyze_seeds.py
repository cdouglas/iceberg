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

"""Audit fuzz sweep coverage: are we testing all the variations FuzzConfig says we should?

Reads every seed-*.{ok,fail}.json under a directory, parses the `describe` field for
the scenario's format bucket and per-op kinds, and compares the actual distribution
to FuzzConfig.defaults()'s intended distribution (uniform over 3 formats × 4 op kinds,
chain length 1..3 uniform, late-tx count 1..8 uniform). Flags:

- Any format bucket or op kind that never appeared in the sweep.
- Any format × op-kind cell with disproportionately low coverage (<25% of expected).
- Any chain length or late-tx count value with zero hits (suggests an RNG range bug).

This is a coverage audit, not a correctness check — every input here is presumed
already-passing. Run after a green sweep to confirm the seeds you ran actually
exercised the workload variants the config intended, not just one corner of the
seed space.

Usage:
    python3 analyze_seeds.py /path/to/seeds/
"""
import collections
import json
import pathlib
import re
import sys


# Intended distributions per FuzzConfig.defaults() and FuzzScenario.forSeed.
EXPECTED_FORMATS = ["V2", "V3", "V2_THEN_UPGRADE_TO_V3"]
EXPECTED_OP_KINDS = ["positionDelete", "append", "rowReplacement", "equalityDelete"]
EXPECTED_CHAIN_RANGE = range(1, 4)        # FuzzScenario.forSeed: 1 + rng.nextInt(3)
EXPECTED_LATE_TX_RANGE = range(1, 9)      # FuzzConfig.defaults: lateTxCount IntRange(1, 8)

DESCRIBE_HEADER_RE = re.compile(
    r"format=(?P<format>\S+).*\bchain=(?P<chain>\d+).*\bnumLateTxOps=(?P<ops>\d+)"
)
OP_KIND_RE = re.compile(r"op\[\d+\]:\s*kind=(?P<kind>\w+)")


def load_describes(seeds_dir: pathlib.Path):
    """Yield (seed, describe) tuples for every seed-*.ok.json / seed-*.fail.json."""
    for path in sorted(seeds_dir.glob("seed-*.*.json")):
        with path.open() as f:
            try:
                record = json.load(f)
            except json.JSONDecodeError:
                print(f"warn: {path.name} is not valid JSON, skipping", file=sys.stderr)
                continue
        describe = record.get("describe")
        if not describe:
            print(f"warn: {path.name} has no 'describe' field, skipping", file=sys.stderr)
            continue
        yield record.get("seed"), describe


def parse_describe(describe: str):
    header = DESCRIBE_HEADER_RE.search(describe)
    if not header:
        return None
    op_kinds = OP_KIND_RE.findall(describe)
    return {
        "format": header["format"],
        "chain": int(header["chain"]),
        "num_late_tx_ops": int(header["ops"]),
        "op_kinds": op_kinds,
    }


def hbar(label: str, count: int, total: int, width: int = 40):
    """Render a simple horizontal bar: label  ##########  count (pct%)."""
    pct = (count / total * 100) if total else 0.0
    filled = int(round(width * count / total)) if total else 0
    bar = "█" * filled + "·" * (width - filled)
    return f"  {label:<28} {bar} {count:>6}  ({pct:5.1f}%)"


def print_section(title: str):
    print()
    print(f"=== {title} ===")


def main():
    if len(sys.argv) != 2:
        print(__doc__.splitlines()[-2], file=sys.stderr)
        sys.exit(2)
    seeds_dir = pathlib.Path(sys.argv[1])
    if not seeds_dir.is_dir():
        print(f"error: {seeds_dir} is not a directory", file=sys.stderr)
        sys.exit(2)

    format_counts = collections.Counter()
    op_kind_counts = collections.Counter()
    chain_counts = collections.Counter()
    late_tx_counts = collections.Counter()
    # Per-format op-kind exposure, to catch e.g. "rowReplacement never paired with V3".
    format_op_pairs = collections.Counter()
    total = 0
    op_total = 0
    skipped = 0

    for _seed, describe in load_describes(seeds_dir):
        parsed = parse_describe(describe)
        if parsed is None:
            skipped += 1
            continue
        total += 1
        fmt = parsed["format"]
        format_counts[fmt] += 1
        chain_counts[parsed["chain"]] += 1
        late_tx_counts[parsed["num_late_tx_ops"]] += 1
        for kind in parsed["op_kinds"]:
            op_kind_counts[kind] += 1
            format_op_pairs[(fmt, kind)] += 1
            op_total += 1

    if total == 0:
        print(f"error: no parseable seed records found under {seeds_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Audited {total} seed records from {seeds_dir} ({skipped} skipped)")
    print(f"Total late-tx ops across the sweep: {op_total}")

    # 1. Format bucket coverage.
    print_section("Format buckets (expected uniform over V2/V3/V2_THEN_UPGRADE_TO_V3)")
    for fmt in EXPECTED_FORMATS:
        print(hbar(fmt, format_counts[fmt], total))
    extra = set(format_counts) - set(EXPECTED_FORMATS)
    for fmt in sorted(extra):
        print(hbar(f"{fmt} (UNEXPECTED)", format_counts[fmt], total))

    # 2. Op-kind coverage.
    print_section("Late-tx op kinds (expected uniform over 4 kinds)")
    for kind in EXPECTED_OP_KINDS:
        print(hbar(kind, op_kind_counts[kind], op_total))
    extra_kinds = set(op_kind_counts) - set(EXPECTED_OP_KINDS)
    for kind in sorted(extra_kinds):
        print(hbar(f"{kind} (UNEXPECTED)", op_kind_counts[kind], op_total))

    # 3. Chain length distribution.
    print_section("Snapshot chain length (expected uniform 1..3)")
    for chain in EXPECTED_CHAIN_RANGE:
        print(hbar(f"chain={chain}", chain_counts[chain], total))
    extra_chains = set(chain_counts) - set(EXPECTED_CHAIN_RANGE)
    for chain in sorted(extra_chains):
        print(hbar(f"chain={chain} (OUT OF RANGE)", chain_counts[chain], total))

    # 4. Late-tx op count distribution.
    print_section("Late-tx op count per scenario (expected uniform 1..8)")
    for n in EXPECTED_LATE_TX_RANGE:
        print(hbar(f"numLateTxOps={n}", late_tx_counts[n], total))
    extra_n = set(late_tx_counts) - set(EXPECTED_LATE_TX_RANGE)
    for n in sorted(extra_n):
        print(hbar(f"numLateTxOps={n} (OUT OF RANGE)", late_tx_counts[n], total))

    # 5. Format × op-kind cross-coverage. With uniform priors each cell expects
    #    op_total / (3 * 4) = op_total / 12 hits. Anything below 25% of that is flagged.
    print_section("Format × op-kind matrix (cells flagged if <25% of expected count)")
    expected_per_cell = op_total / (len(EXPECTED_FORMATS) * len(EXPECTED_OP_KINDS))
    floor = expected_per_cell * 0.25
    header = f"  {'':<24}" + "".join(f"{k:>16}" for k in EXPECTED_OP_KINDS)
    print(header)
    flagged_cells = []
    for fmt in EXPECTED_FORMATS:
        row_cells = []
        for kind in EXPECTED_OP_KINDS:
            count = format_op_pairs[(fmt, kind)]
            cell = f"{count:>16}"
            if count == 0:
                cell = f"{'MISSING':>16}"
                flagged_cells.append((fmt, kind, count, "missing"))
            elif count < floor:
                cell = f"{('<' + str(int(floor))):>16}"
                flagged_cells.append((fmt, kind, count, f"under (<{int(floor)})"))
            row_cells.append(cell)
        print(f"  {fmt:<24}" + "".join(row_cells))
    print(f"  expected per cell ≈ {expected_per_cell:.1f} (flagging threshold: {floor:.1f})")

    # 6. Final verdict.
    print_section("Summary")
    issues = []
    for fmt in EXPECTED_FORMATS:
        if format_counts[fmt] == 0:
            issues.append(f"format {fmt} never appeared")
    for kind in EXPECTED_OP_KINDS:
        if op_kind_counts[kind] == 0:
            issues.append(f"op kind {kind} never appeared")
    for chain in EXPECTED_CHAIN_RANGE:
        if chain_counts[chain] == 0:
            issues.append(f"chain length {chain} never appeared")
    for n in EXPECTED_LATE_TX_RANGE:
        if late_tx_counts[n] == 0:
            issues.append(f"late-tx op count {n} never appeared")
    for fmt, kind, count, why in flagged_cells:
        issues.append(f"cell ({fmt}, {kind}) {why}: count={count}")
    if issues:
        print(f"  ⚠ {len(issues)} coverage gap(s):")
        for issue in issues:
            print(f"    - {issue}")
        sys.exit(1)
    print("  ✓ all expected variations exercised; no missing or under-covered cells.")


if __name__ == "__main__":
    main()
