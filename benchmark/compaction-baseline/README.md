# Compaction Baseline Benchmark

Single-machine benchmark comparing two reconciliation strategies when a position-delete
transaction lands during a compaction in Apache Iceberg V3:

- **Baseline**: re-run the compaction so the late delete is absorbed by the rewrite.
- **Treatment**: use the compaction map to remap the late delete on top of the
  already-compacted state.

The benchmark's primary value is **end-to-end correctness affirmation** under a realistic
workload, not precise performance measurement — see `COMPACT_SPEC.md` at the repo root for the
full design rationale and acceptance criteria.

## Status

| Phase | Component | Status |
|-------|-----------|--------|
| 0 | Module skeleton, build wiring | done |
| 1 | `WorkloadGenerator` + determinism test | done |
| 2 | `SetupMain` (build the eight prebuilt warehouses) | done |
| 3 | `RunMain` (cloud-side perf runner) | done (local-fs path) |
| 4 | Hand-crafted tests (M2 race, M3 concurrent compactions, M4 code-path, M5 mutation) | done |
| 5 | `FuzzMain` (M1 confluence fuzz) + determinism guard + reproducer script | done |
| 6 | `CorrectnessCheck` + `analyze.py` | done |
| 7 | M6 (in-runner) + M7 preflight + docs | done |

## Build

```bash
./gradlew :benchmark:compaction-baseline:compileJava
./gradlew :benchmark:compaction-baseline:test
./gradlew :benchmark:compaction-baseline:shadowJar
```

The shadow JAR's default `Main-Class` is `RunMain`; the wrappers under `scripts/` set
the entry class explicitly. Spark 3.5 on JDK 17/21 needs a long list of `--add-opens`
flags — always invoke the JAR via a wrapper script, not `java -jar` directly.

## Setup (Phase 2)

`SetupMain` builds the eight prebuilt warehouses described in `COMPACT_SPEC.md` §"Two Frozen
Starting States" and (optionally) uploads them to S3.

```bash
# build the shaded jar first
./gradlew :benchmark:compaction-baseline:shadowJar

# tiny plumbing check (≈30 s on a workstation, no S3 required)
scripts/setup.sh --output /tmp/compaction-baseline-out \
  --skip-upload --test-sizes --only-k 1000

# full production setup (multi-hour, needs ~50 GB free + AWS CLI configured)
scripts/setup.sh --output /tmp/compaction-baseline-out --bucket my-bench-bucket
```

Each cell becomes one tarball under `--output`. The script produces `setup_manifest.json` with
snapshot IDs, file counts, compaction-map paths, and run counts so the runner can verify it
restored the right thing.

## Run the perf benchmark (Phase 3)

`RunMain` consumes one tarball produced by setup, restores the warehouse into a working
directory, runs warmup + N timed iterations, and writes one JSON line per iteration:

```bash
# baseline at production K=1M, three timed iterations after one warmup
scripts/run.sh \
  --state-tar /tmp/cmpbench-state/baseline-K1m.tar \
  --working-dir /tmp/cmpbench-work \
  --variant baseline --k 1000000 --iterations 3 --warmup 1 \
  --output /tmp/cmpbench-work/results.jsonl

# treatment
scripts/run.sh \
  --state-tar /tmp/cmpbench-state/treatment-K1m.tar \
  --working-dir /tmp/cmpbench-work \
  --variant treatment --k 1000000 --iterations 5 --warmup 1 \
  --output /tmp/cmpbench-work/results.jsonl
```

The runner enforces the spec's M6 soundness assertions on every baseline iteration
(files_read ≥ 10, files_written ≥ 10, input_bytes_scanned ≥ 5 GB). Iterations failing any of
those are marked invalid in the JSONL, and the scenario aborts if zero iterations pass. Pass
`--skip-m6` for sub-production fixtures where those thresholds are intentionally below the
soundness bar.

**Cloud restoration** is not yet handled in-process; the state tarball must be local. For the
spec's S3 flow, fetch the tar with `aws s3 cp s3://.../states/<cell>.tar /tmp/` before invoking
the runner.

## Docker image (FuzzMain)

A self-contained image for the fuzz harness. Build from the **repo root** (the Dockerfile
needs `gradlew` and the full multi-module source tree, so the build context cannot be the
module directory):

```bash
cd <iceberg-repo-root>
docker build -f benchmark/compaction-baseline/Dockerfile -t cmpmap-fuzz:latest .
```

Notes:
- BuildKit (default in Docker 23+) picks up `benchmark/compaction-baseline/Dockerfile.dockerignore`
  to override the repo-root `.dockerignore` (which is tuned for an iceberg-core-only JMH image
  and would exclude `spark/`, `data/`, `parquet/` — modules this image needs). If your client
  is older and the build pulls in the full repo (multi-GB context), enable BuildKit explicitly:
  `DOCKER_BUILDKIT=1 docker build ...`.
- The Dockerfile injects placeholder `version.txt` and `iceberg-build.properties` so gradle
  succeeds without `.git/` in the build context.
- Build time: ~4 minutes on a workstation, ~310 MB final image (a shaded jar that bundles Spark
  3.5.6 + Iceberg + all transitive dependencies).

Run, mounting an output directory:

```bash
mkdir -p /tmp/fuzz-out
docker run --rm -v /tmp/fuzz-out:/out cmpmap-fuzz:latest \
  --seed-start 0 --seed-count 100 --workers 4 --output /out
```

The image's `ENTRYPOINT` is `FuzzMain` with all the Spark-3.5-on-Java-17 `--add-opens` flags
baked in — additional `docker run` args become CLI args to FuzzMain. The output directory
holds `summary.json`, `seed-N.ok.json` per passing seed, and `seed-N.fail.json` +
`seed-N.warehouse.tar` per failing seed.

The image is intentionally minimal; the perf benchmark (`RunMain`, S3-backed) is invoked
outside Docker.

## Run the fuzz harness (Phase 5, M1)

`FuzzMain` verifies confluence on randomized workloads:
`hash(compact(state) + remap(tx, map)) == hash(compact(state ∪ tx))` for any seeded plan.

```bash
# 100 seeds, single worker, default 300 s per-seed timeout. With no --seed-start, the
# harness picks a random anchor and logs it in summary.json so the sweep is reproducible.
scripts/fuzz.sh --seed-count 100 --workers 1 --output ./fuzz-out
```

### Seed selection

The harness tests pseudorandomly-sampled seeds drawn from a `Random` instance, not the
sequential range `0..seedCount-1`. Adjacent seed values share most of their entropy, so a
sequential sweep produces highly similar scenario shapes and wastes CPU on near-duplicates;
pseudorandom sampling spreads coverage across the seed space.

Three modes, in precedence order:

| Mode | Use when | Reproducibility |
|---|---|---|
| `--seeds N1,N2,...` | Replaying specific failing seeds (used by `reproduce.sh`). | Deterministic. |
| `--seed-start <N>` | Reproducing a prior sweep — the anchor logged in its `summary.json`. | Deterministic. |
| neither | A fresh exploratory sweep. | Anchor is logged in `summary.json.seedAnchor` (with `seedAnchorRandom=true`); rerun with `--seed-start <that>` to reproduce. |

`--seeds` and `--seed-start` are mutually exclusive.

### Adversarial config (--config)

By default the harness randomizes across three format buckets (v2, v3, v2→v3-upgraded), four op
kinds (position delete, append, row replacement, equality delete), probabilistic slice overlap
(p=0.5), and 1..8 late-tx ops per seed. Pass `--config <path.json>` to override any of these:

```bash
scripts/fuzz.sh --seed-count 100 --config my-config.json --output ./fuzz-out
```

Sample config (omit any field to fall back to its default):

```json
{
  "formatWeights":          { "v2": 1.0, "v3": 1.0, "v2ThenUpgradeToV3": 1.0 },
  "opKindWeights":          { "positionDelete": 1.0, "append": 1.0,
                              "rowReplacement": 1.0, "equalityDelete": 1.0 },
  "lateTxCount":            { "min": 1, "max": 8 },
  "overlapProbability":     0.5,
  "deletesPerOp":           { "min": 5,  "max": 14 },
  "appendRowsPerOp":        { "min": 500, "max": 3000 },
  "replacementRows":        { "min": 50,  "max": 500 },
  "equalityDeleteRowsPerOp":{ "min": 1,  "max": 20 }
}
```

To reproduce the harness's pre-adversarial v3-DV-only-disjoint shape (the shape under which
seeds 59 and 101 were originally captured), use:

```json
{
  "formatWeights": { "v3": 1.0 },
  "opKindWeights": { "positionDelete": 1.0 },
  "overlapProbability": 0.0,
  "lateTxCount": { "min": 1, "max": 2 }
}
```

### Concurrent execution

`--workers N` runs up to N seeds concurrently in one Spark JVM (driver mode `local[N]`). Per-seed
wall time scales roughly with N because seeds contend for the same task slots, so the default
per-seed timeout is 300 s — tight enough to flush hangs, loose enough that a 10–15 s solo seed
doesn't get aborted under 8-way contention. Override with `--timeout-seconds`.

Output:
- `summary.json` — `{seedAnchor, seedAnchorRandom, seedsRun, seedsFailed, failedSeedList,
  totalElapsedMs, …}`. When `seedAnchorRandom=true`, `seedAnchor` is the value the harness drew;
  passing it back via `--seed-start <seedAnchor>` reproduces the sweep.
- `seed-<N>.ok.json` — one per passing seed: hashes, ops count, row counts, elapsed ms. `N` is
  the actual seed value (can be a large negative or positive `long`).
- `seed-<N>.fail.json` + `seed-<N>.warehouse.tar` — one per failing seed: the operation sequence,
  both hashes, both row counts, and a tar of both reference and treatment workspaces. Either
  the tarball OR the seed alone is enough to replay (the harness is deterministic).

The pre-handoff bar is **1,000 seeds without unexplained failure**. The current `cmpmap` branch
clears a **2,048-seed sweep** (anchor=0, `default-cmpmap-seeds-0..2047`). Any non-confluent seed
is triaged into `KNOWN_FAILURES.md` per the spec's Failure Protocol.

### Coverage audit

After a green sweep, `analyze_seeds.py` confirms the seeds you drew actually covered the
workload variants the config intended, not just one corner of the seed space:

```bash
python3 analyze_seeds.py ./fuzz-out/
```

It tallies format bucket, op kind, chain length, and late-tx count distributions against
`FuzzConfig.defaults()`'s expected uniform priors, and prints a format × op-kind matrix so
under-paired cells get flagged explicitly (rather than hiding inside an apparently-fine
marginal). Exits non-zero if any expected bucket has zero hits or any matrix cell falls below
25 % of its expected count — suitable as a CI post-sweep gate.

## Reproduce a fuzz failure

Given a `seed-N.fail.json`:

```bash
scripts/reproduce.sh path/to/seed-N.fail.json
```

The script extracts the seed, invokes `FuzzMain` for that one seed in a fresh output directory,
and prints whether the divergence reproduced. Exit codes:

| code | meaning |
|------|---------|
| 0 | seed diverged again — the bug is live |
| 2 | seed passed on replay — either a fix has landed since the failure was captured, or the harness has an unseeded source of nondeterminism the determinism guard missed |
| 3 | replay produced neither `ok.json` nor `fail.json` — manual inspection needed |

The warehouse tarball alongside the `.fail.json` is preserved for post-mortem inspection
(`tar -xf seed-N.warehouse.tar`); because `FuzzScenario` is purely seed-driven, the replay
above does not need it.

## Interpret the results (Phase 6)

`analyze.py` consumes the JSONL written by `RunMain`:

```bash
python3 analyze.py /tmp/cmpbench-work/results.jsonl --output-dir /tmp/cmpbench-work/
```

Output:
- `summary.csv` — one row per `(variant, K)` cell with min / median / max wall-clock ms,
  iteration count, and the number of M6-invalid iterations excluded from the aggregation.
- `wallclock.png` — log-log plot of K vs median wall-clock with min/max error bars, baseline
  and treatment as separate series.

The expected gap is **at least two orders of magnitude** — remap measured in hundreds of ms,
baseline compaction in minutes. Anything closer than that should be questioned before being
written up (see spec §"Expected Headline Numbers").

## Pre-flight (M7)

```bash
scripts/preflight.sh ./preflight.log
```

Runs the four spec-required test commands and captures output to `preflight.log`. The bar is
zero failures across all four. Flaky tests must be reported in the writeup, not retried.

## Layout

```
benchmark/compaction-baseline/
├── build.gradle
├── Dockerfile                    # FuzzMain container
├── README.md                     # this file
├── TODO.md                       # follow-up coverage extensions (not yet implemented)
├── KNOWN_FAILURES.md             # fuzz triage log (Failure Protocol)
├── analyze.py                    # RunMain perf results parser + plot generator
├── analyze_seeds.py              # fuzz-sweep coverage audit
├── scripts/
│   ├── setup.sh                  # SetupMain wrapper
│   ├── run.sh                    # RunMain wrapper
│   ├── fuzz.sh                   # FuzzMain wrapper
│   ├── reproduce.sh              # replay a seed-N.fail.json
│   └── preflight.sh              # M7 preflight runner
└── src/
    ├── main/java/org/apache/iceberg/benchmark/compaction/
    │   ├── SetupMain.java                  # Phase 2 — CLI entry
    │   ├── BuildConfig.java                # Phase 2 — sizes / seeds
    │   ├── BuildResult.java
    │   ├── WarehouseBuilder.java           # chain + compact + late-tx
    │   ├── WorkloadCommitter.java
    │   ├── WorkloadGenerator.java
    │   ├── SetupManifest.java
    │   ├── TarUtils.java
    │   ├── S3Uploader.java
    │   ├── RunMain.java                    # Phase 3 — perf runner CLI
    │   ├── IterationResult.java
    │   ├── StageMetricsCollector.java      # Spark stage listener
    │   ├── BaselineTimedRegion.java
    │   ├── TreatmentTimedRegion.java
    │   ├── CorrectnessCheck.java           # xxhash64 row-multiset hash
    │   ├── FuzzMain.java                   # Phase 5 — M1 confluence fuzz CLI
    │   ├── FuzzConfig.java                 # weighted distributions for adversarial mode
    │   ├── FuzzScenario.java               # deterministic plan per seed
    │   ├── FuzzRunner.java                 # both reconciliation paths
    │   ├── LateTxOp.java                   # base for the four op kinds
    │   ├── PositionDeleteOp.java
    │   ├── AppendOp.java
    │   ├── RowReplacementOp.java
    │   └── EqualityDeleteOp.java
    └── test/java/org/apache/iceberg/benchmark/compaction/
        ├── TestWorkloadGeneratorDeterminism.java
        ├── TestWarehouseBuilder.java
        ├── TestWarehouseBuilderFormatMix.java
        ├── TestTarUtils.java
        ├── TestRunMain.java
        ├── TestFuzzConfig.java
        ├── TestFuzzMainArgs.java                # arg parser (--seeds / --seed-start modes)
        ├── TestFuzzScenarioOpKinds.java
        ├── TestFuzzScenarioOverlap.java
        ├── TestMultiTransactionRace.java        # M2
        ├── TestConcurrentCompactions.java       # M3
        ├── TestCodePathVerification.java        # M4
        ├── TestCorrectnessCheckMutation.java    # M5
        └── TestFuzzDeterminism.java             # M1 determinism guard
```

## Bugs caught by this harness

`git log -p benchmark/compaction-baseline/KNOWN_FAILURES.md` reconstructs the full history —
every failure that has lived in that ledger was added with a Resolution paragraph at fix time
and then deleted in a follow-up commit, so the file itself stays a tidy list of currently-open
issues while git preserves the bug → fix → regression-test linkage.

Possible follow-up coverage extensions (compaction-map chains in the fuzzer, partitioned tables,
SERIALIZABLE soundness checks, Spark 4.0 CI parity) are listed in [TODO.md](TODO.md) — they are
not blockers for any spec milestone, just candidates if the fuzzer is invested in further.
