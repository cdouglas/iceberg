# Snapshot rewrite: space savings

Two local synthetic sweeps behind the extension slides.

Where this sits among the others: `remapping-microbenchmark/` and `compaction-baseline/` are
gradle modules that measure *latency*, and `remapping-optimization/` holds the analysis and plots
for a JMH benchmark whose Java lives in `core/src/jmh`. This follows that last shape -- the driver
lives next to the harness it extends, in `iceberg-data`'s test sources, and only the results and
plotting live here. It is not JMH because the quantity of interest is bytes on disk, not time, and
JMH measures the wrong thing. Plot style, palette and the `results/plots/` layout follow
`remapping-microbenchmark/scripts/plot_results.py`.

Both sweeps drive the real planner and read
[`SnapshotRewriteReport`](../../core/src/main/java/org/apache/iceberg/snaprewrite/SnapshotRewriteReport.java),
whose byte figures come off the files the rewrite actually wrote. Nothing here simulates a cost.

What this adds over `TestSnapshotRewriteScale` and `TestSnapshotRewriteDeleteCost`, which already
pin the accounting: scale and a curve. Those run 20-80k rows in a single data file and assert
bounds; this runs 500k rows across a multi-file compaction and sweeps the parameters that decide
the outcome.

## Running it

```bash
# ~1 min. Also prices the dry-run estimate against the measurement.
./gradlew :iceberg-data:snapshotRewriteSpaceBench \
    -PbenchArgs="--figure all --preset quick --out benchmark/snaprewrite-space/results"

# The slide data: ~2.5 min for the mix sweep, ~1 min for depth.
./gradlew :iceberg-data:snapshotRewriteSpaceBench \
    -PbenchArgs="--figure mix --preset full --out benchmark/snaprewrite-space/results"
./gradlew :iceberg-data:snapshotRewriteSpaceBench \
    -PbenchArgs="--figure depth --preset full --out benchmark/snaprewrite-space/results"

python3 benchmark/snaprewrite-space/plot_results.py
```

`-PbenchHeap` sets the heap (default 8g). `--tmp` puts the scratch tables somewhere with room:
each sweep point builds, compacts and rewrites its own table, then deletes it.

## The two figures

**`results/plots/mix_savings_by_deletion_share`** — 500k base rows, a window of 8 commits applying 500k row-changes, sweeping the
share of those changes that are deletions. Total changes are held equal to the base row count, so
the only thing moving is the split.

The rewrite frees the old layout and spends part of it recovering the rows that died in the window,
because those rows are absent from the compaction and have to be materialized. The saving lands
within 0.5% of the design's prediction -- one copy of the compacted table -- at every point:

| deletions | net reclaimed | predicted | % of pinned history | dead-ratio |
|---|---|---|---|---|
| 0% | 217.7 MiB | 218.2 | 49.9% | 0.00 |
| 25% | 163.3 MiB | 163.6 | 46.1% | 0.17 |
| 50% | 108.9 MiB | 109.1 | 39.9% | 0.50 |
| 90% | 21.9 MiB | 21.8 | 15.4% | 4.50 |

Delete vectors and metadata together never exceed 0.24 MiB, under 0.2% of what is freed. The
`maxDeadRatio` guard's 0.5 default lands at exactly a 50% deletion share here, since
`deadRatio = 0.5d / (1 - d)` in this parameterization.

**`results/plots/depth_cost_by_format_version`** — an insert-only window, so nothing is recovered and the delete files plus the
per-snapshot metadata are the entire cost. 100k base rows, 2k appended per commit, swept over
window depth for both format versions and both schema widths.

Per rewritten snapshot, measured:

| | m=1 | m=8 | m=32 |
|---|---|---|---|
| v3 deletion vector | 294 B | 525 B | 597 B |
| v2 position deletes | 2.0 KiB | 10.0 KiB | 35.2 KiB |
| manifest + manifest list | 14.2 KiB | 14.0 KiB | 13.9 KiB |

A snapshot masks every row inserted after it, so the positions grow quadratically in the window
depth. Under v3 that costs nothing: an order-preserving compaction lays those rows out
consecutively and Roaring stores the range as a run, so the vector is flat at ~0.5 KiB no matter
how much it masks. Under v2 the count is paid a row at a time. The binding cost either way is the
~14 KiB of manifest per retained snapshot, which is what sends a 2-column table under v2 negative
by m=24 while both 20-column series hold near 50%.

## One thing the sweep turned up

The quick preset runs `estimate()` alongside `materialize()`, which prices the dry run against the
measurement at a scale it had not been checked at. The predicted *saving* holds to within 4%, but
the metadata term does not: `estimate()` charges a flat 10 KiB plus 14 KiB per snapshot, which is
right for an insert-only window (139 KiB predicted, 134 KiB measured) and roughly 40% low for any
window that recovers rows (139 KiB predicted, ~225 KiB measured). A snapshot that gains a
resurrection file needs its own data manifest and cannot share the restamped one, and the estimate
does not account for that. It does not change the verdict on any window measured here -- metadata
is under 0.2% of what is freed at this scale -- but it would matter for a small table, which is
exactly where the dry run is used to decide.

## What these figures do not show

Generated tables, one machine, local files, and validated against the Java reader only. No
object-store latency, no window hundreds of commits deep, no production layout. Errata 9 in
`docs/docs/compaction_maps_rewrite.md` stands: the dry run exists so the measurement can be taken
on a real table, and it has not been. This moves the largest measured configuration from 80k rows
in one file to 500k across a multi-file compaction; it does not settle the question.

## Files

| Path | What it is |
|---|---|
| `data/src/test/java/org/apache/iceberg/data/snaprewrite/SnapshotRewriteSpaceBench.java` | the driver |
| `data/src/test/java/org/apache/iceberg/data/snaprewrite/LiveRuns.java` | disjoint clustered delete runs, so the death count is exact |
| `results/*.csv` | one row per configuration; the contract between driver and plotter |
| `plot_results.py` | matplotlib; writes PNG (150 dpi) and PDF into `results/plots/` |
