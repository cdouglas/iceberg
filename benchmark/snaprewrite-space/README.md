# Snapshot rewrite: space savings

Two local synthetic sweeps measuring what a snapshot rewrite reclaims and what it costs.
**What each experiment shows, and how to read the figures, is in [EXPERIMENTS.md](EXPERIMENTS.md).**
This file is how to run it.

Where this sits among the others: `remapping-microbenchmark/` and `compaction-baseline/` are gradle
modules that measure *latency*, and `remapping-optimization/` holds the analysis and plots for a JMH
benchmark whose Java lives in `core/src/jmh`. This follows that last shape -- the driver lives beside
the harness it extends, in `iceberg-data`'s test sources, and only results and plotting live here. It
is not JMH because the quantity of interest is bytes on disk, not time. Plot palette and the
`results/plots/` layout follow `remapping-microbenchmark/scripts/plot_results.py`.

Both sweeps drive the real planner and read
[`SnapshotRewriteReport`](../../core/src/main/java/org/apache/iceberg/snaprewrite/SnapshotRewriteReport.java),
whose byte figures come off the files the rewrite actually wrote. Nothing here simulates a cost. What
this adds over `TestSnapshotRewriteScale` and `TestSnapshotRewriteDeleteCost`, which already pin the
accounting at 20-80k rows in a single data file: scale, and a curve instead of three points.

## Running it

```bash
# ~1 min. Also runs estimate() alongside materialize(), pricing the dry run
# against the measurement.
./gradlew :iceberg-data:snapshotRewriteSpaceBench \
    -PbenchArgs="--figure all --preset quick --out benchmark/snaprewrite-space/results"

# The committed data: ~2.5 min for mix, ~1 min for depth.
./gradlew :iceberg-data:snapshotRewriteSpaceBench \
    -PbenchArgs="--figure mix --preset full --out benchmark/snaprewrite-space/results"
./gradlew :iceberg-data:snapshotRewriteSpaceBench \
    -PbenchArgs="--figure depth --preset full --out benchmark/snaprewrite-space/results"

python3 benchmark/snaprewrite-space/plot_results.py
```

`-PbenchHeap` sets the heap (default 8g). `--tmp` puts the scratch tables somewhere with room: each
sweep point builds, compacts and rewrites its own table, then deletes it. Runs are deterministic
from `--seed`.

The driver asserts the invariant the whole accounting rests on -- each row that died in the window is
recovered exactly once -- and aborts the configuration if it does not hold. Losslessness is the test
suite's job (`./gradlew :iceberg-data:test --tests "*SnapshotRewrite*"`), which can afford to read
every snapshot of every table.

## Files

| Path | What it is |
|---|---|
| `EXPERIMENTS.md` | the experiments and how to read each figure |
| `data/src/test/java/org/apache/iceberg/data/snaprewrite/SnapshotRewriteSpaceBench.java` | the driver |
| `data/src/test/java/org/apache/iceberg/data/snaprewrite/LiveRuns.java` | disjoint clustered delete runs, so the death count is exact |
| `results/mix.csv`, `results/depth.csv` | one row per configuration; the contract between driver and plotter |
| `plot_results.py` | matplotlib; writes PNG (150 dpi) and PDF into `results/plots/` |

CSV columns of note: `source_compaction_bytes` / `interstitial_bytes` split `reclaimable_bytes` into
the two layers the rewrite detaches, and `target_bytes` is `C_B`'s own size. `saved_bytes` is
`reclaimable_bytes` minus recovered rows, delete files and metadata.
