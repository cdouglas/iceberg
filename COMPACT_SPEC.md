# Compaction Baseline Benchmark — Design Specification

## Goal

Build a single-machine benchmark that compares two reconciliation strategies when a position-delete transaction lands during a compaction in Apache Iceberg V3:

- **Baseline**: re-run the compaction as `compact(S_0..S_{n+1})`. The late delete transaction is absorbed by re-doing the rewrite.
- **Treatment**: use the compaction map produced by `compact(S_0..S_n)` to remap `S_{n+1}` on top of the already-compacted state.

The shared cost of `compact(S_0..S_n)` is **sunk** in both sides and excluded from measurements. What we want to compare is the *incremental* cost of reconciling `S_{n+1}`.

This benchmark is a sanity check on top of a completed workshop paper. The goal is plausibility and order-of-magnitude confidence, not novelty.

## Scope

- Iceberg V3 tables only. Deletion vectors (DV), not V2 position delete files.
- Bin-pack compaction only. No sort/Z-order.
- One concurrent transaction (`S_{n+1}`), one compaction. No chaining.
- `S_{n+1}` is delete-only (no new data files).

## Schema

20 columns, deliberately low compressibility so Snappy ≈ 1:1:

| Column     | Type   | Count |
|------------|--------|-------|
| `uuid_*`   | string | 4     |
| `long_*`   | long   | 8     |
| `short_*`  | string (16 char random) | 4 |
| `dbl_*`    | double | 4     |

Approximate row size: ~304 B uncompressed.

## Workload

Pre-compaction state, built deterministically with a fixed seed:

- **S_0**: 10,000,000-row insert.
- **S_1..S_{10}**: each commits a 1,100,000-row insert *plus* ~1,000 position deletes against files committed in earlier snapshots. Deletes are uniformly scattered (no clustering) — this is what produces ~10,000 runs in the compaction map.

After compaction of `S_0..S_{10}`:

- ~21,000,000 live rows.
- ~6.5 GB on disk (Parquet + Snappy, uncompressible payload).
- ~13 compacted output files at default `write.target-file-size-bytes = 512 MB`.
- Compaction map with ~10,000 runs (verify and report actual count; don't fail if slightly off).

### Concurrent transaction `S_{n+1}`

Four sweep points: `K ∈ {1000, 10000, 100000, 1000000}` position deletes, materialized as DVs against pre-compaction files.

- Deletes are **clustered at ~100 per contiguous run**, giving 10 / 100 / 1,000 / 10,000 runs in `S_{n+1}`'s DV. This matches GDPR-style batch-delete shapes.
- Small K (1k, 10k) targets 1–2 pre-compaction source files. Large K (100k, 1M) spreads across all pre-compaction source files proportionally to file size.

## Two Frozen Starting States

Setup builds **8 prebuilt warehouses**, one per `(variant, K)` cell. Each is fully independent (no shared state at runtime):

- **Baseline-K**: HadoopCatalog containing `S_0..S_{n+1}` committed, no compaction yet. Pre-compaction file layout intact.
- **Treatment-K**: HadoopCatalog containing `compact(S_0..S_n)` already executed and committed, the compaction-map Avro file present at the standard metadata location, and `S_{n+1}`'s DV committed against the (now-orphan) pre-compaction files — i.e., a conflicting commit waiting to be reconciled.

Both starting states share the same physical `S_{n+1}` DV content (only its parent snapshot history differs).

The compaction-map file is produced by enabling `write.compaction-map.enabled = true` on the table prior to the offline `compact(S_0..S_n)` step. Setup must assert that the map file exists and is non-empty before declaring the treatment state "ready."

### Build and ship

Setup runs **locally on the implementer's workstation**, not on the cloud VM:

1. Generate data and commit `S_0..S_{n+1}` for each K.
2. For Treatment-K only: enable compaction-map generation and run `compact(S_0..S_n)` once. Snapshot ID and map-file path are captured in a manifest.
3. Tar each of the 8 warehouses, upload to `s3://<bucket>/states/{baseline,treatment}-K{1k,10k,100k,1m}.tar`.

## Runner

Runs on the cloud VM. For each scenario, the runner:

1. Restores the warehouse for the scenario into the working S3 path (`s3 rm --recursive` + `s3 cp --recursive` from frozen state; server-side copy, no egress).
2. Opens the Iceberg table via HadoopCatalog with `s3a://<bucket>/working/<table>` warehouse and `S3FileIO`.
3. Runs **1 untimed warmup iteration**, then **N timed iterations** (Baseline N=3, Treatment N=5).
4. Records one JSON line per timed iteration.
5. After the last iteration of the scenario, captures the post-state checksum for the correctness check.

### Baseline measurement (timed region)

```java
SparkActions.get(spark).rewriteDataFiles(table).execute();
```

`S_{n+1}` is visible at planning time, so there is no conflict to detect. Iceberg's `rewriteDataFiles` reads the pre-compaction files, applies all deletes (including `S_{n+1}`'s DV), and writes a single compacted snapshot. The commit is part of the action.

### Treatment measurement (timed region)

```java
CompactionMap map = CompactionMaps.read(mapFile, table.io());     // not timed (setup)
DeleteConflictInfo conflicts = buildConflictInfo(table, map);     // not timed (setup)

// START TIMER
SparkCompactionConflictResolver resolver = new SparkCompactionConflictResolver(spark, table);
List<DeleteFile> newDeletes = resolver.resolve(map, conflicts);
RowDelta delta = table.newRowDelta();
newDeletes.forEach(delta::addDeletes);
delta.commit();
// STOP TIMER
```

Loading the compaction map and constructing the `DeleteConflictInfo` are outside the timer because they would, in production, have already happened during the (sunk) compaction commit. We are timing the *additional* reconciliation work.

## Cloud Configuration

| Item        | Value |
|-------------|-------|
| VM          | AWS `m6i.2xlarge` (8 vCPU, 32 GiB) |
| OS          | Ubuntu 22.04 LTS |
| Region      | One region, same as bucket |
| Storage     | S3 standard, single bucket |
| Catalog     | `HadoopCatalog`, warehouse at `s3a://<bucket>/working/` |
| FileIO      | `S3FileIO` |
| Spark       | `master("local[*]")`, in-process, driver heap 24 GiB |
| Iceberg     | Branch `cmpmap` (whatever HEAD is at hand-off) |

S3 reads are in the timing path. No local FS staging of data files. Iceberg's metadata cache may stay warm across iterations (fair: it would be warm in production too).

## Measurement Per Iteration

Each timed iteration writes one JSON line to `results.jsonl`:

```json
{
  "variant": "baseline",
  "k": 100000,
  "iteration": 1,
  "wall_clock_ms": 78421,
  "stage_ms": {
    "plan": 2103,
    "scan_write": 71880,
    "commit": 4438
  },
  "snapshot_id_after": 8123498123,
  "input_data_bytes": 6510000000,
  "output_data_bytes": 6498200000,
  "files_read": 17,
  "files_written": 13,
  "compaction_map_runs": 10142,
  "sn_plus_one_runs": 1000
}
```

Treatment iterations additionally include `stage_ms.{read_dv, remap, write_dv}`.

Stage timings are best-effort. `wall_clock_ms` is authoritative; stage breakdowns come from Spark UI metrics where available and explicit `System.nanoTime()` brackets in the runner code where not.

## Correctness Check

Iceberg outputs are deterministic given identical inputs and a fixed seed. The two variants must produce snapshots with identical row-multisets.

After all iterations finish, the runner:

1. Reads one baseline post-state and one treatment post-state (any iteration, since they're deterministic).
2. Computes an order-independent row hash:
   ```scala
   df.selectExpr("xxhash64(struct(*))").agg(sum("xxhash64")).first()
   ```
3. Asserts the two hashes are equal.
4. Fails the run loudly if not.

This is a single end-of-run check per K, not per-iteration. The compaction-map *generation* during offline setup is verified independently by asserting the map file exists, is well-formed Avro, and contains the expected number of runs (±10%).

## Output

The runner writes:

- `results.jsonl` — one line per timed iteration.
- `setup_manifest.json` — produced by the offline setup phase; lists snapshot IDs, file counts, compaction-map run counts, and seed values for each prebuilt warehouse. Uploaded alongside the tarballs.

The analyzer (`analyze.py`) reads `results.jsonl` and produces:

- A `summary.csv` with min / median / max wall-clock per (variant, K) cell.
- A plot of K (log x) vs. median wall-clock ms (log y), baseline and treatment as separate series, with min/max as error bars.

## Deliverables

A new module at `benchmark/compaction-baseline/`, containing:

1. `src/main/java/.../SetupMain.java` — local builder for the 8 prebuilt warehouses.
2. `src/main/java/.../RunMain.java` — VM-side runner.
3. `src/main/java/.../CorrectnessCheck.java` — order-independent row hash.
4. `analyze.py` — results parser + plot generator.
5. `README.md` — build, setup, run, interpret.
6. Test sources sufficient to verify each piece in isolation (see acceptance criteria, appended below).

## Expected Headline Numbers

Calibration against public reports (OLake, AWS EMR blog, etc.) suggests baseline `compact(~6.5 GB)` on a single `m6i.2xlarge` should land in the **1–5 minute** range. Treatment remap of `S_{n+1}` should land in **seconds** even at K=1M. If observed numbers are an order of magnitude off either side, that is grounds to question the setup before drawing conclusions.

---

## Acceptance Criteria

### Framing

The benchmark's primary value is **end-to-end correctness affirmation** under a realistic workload, not precise performance measurement. The wall-clock gap between baseline and treatment is expected to be at least two orders of magnitude (remap measured in hundreds of ms, baseline compaction in minutes). The interesting question is not "how much faster" — it's "does the implementation actually work."

Correctness rests on **confluence**: any sequence of rewrites starting from the same root yields the same normal form, regardless of order. Specifically, for any state and any concurrent transaction:

```
rows(compact(state) + remap(tx, map))  ==  rows(compact(state ∪ tx))
```

The fuzz harness (M1) is the primary instrument for testing this property. The hand-crafted scenario tests (M2, M3) cover specific claims the design makes that fuzz may not reliably exercise.

### Mandatory Tests

#### M1 — Property Fuzz Harness (Confluence)

**Purpose**: Verify confluence on random workloads.

**Property tested**: for any seeded `(snapshot_chain, late_tx)`,
`hash(compact(state) + remap(tx, map)) == hash(compact(state ∪ tx))`.

**Implementation**:
- Small scale (~100k rows, ~4 files) so each tuple runs in seconds.
- Seeded RNG generates inserts, deletes, and the late transaction.
- Both reconciliation paths executed; row-multisets hashed (order-independent xxhash64 aggregate); equality asserted.

**Containerization**:
- `Dockerfile` in the new module; image self-contained, no runtime network dependencies beyond results upload.
- CLI: `java -jar fuzz.jar --seed-start <N> --seed-count <K> --workers <W> --output <dir>`.
- Per-seed output:
  - Success: `seed-N.ok.json` with `{seed, ops_count, hash_a, hash_b, elapsed_ms}`.
  - Failure: `seed-N.fail.json` with the full operation sequence and a warehouse tarball.
- Per-seed timeout (default 60s) so a hung remap does not poison the worker pool.
- Aggregation: `summary.json` with `{seeds_run, seeds_failed, failed_seed_list}`.

**Determinism guarantee**:
- Given a seed, two consecutive runs MUST produce identical operation sequences and identical hashes.
- Implementer ships a unit test (`TestFuzzDeterminism`) that runs the same seed twice and asserts byte-identical output.
- No unseeded sources of nondeterminism: no `System.currentTimeMillis()` in workload generation, no reliance on HashMap iteration order, no thread-scheduling-dependent commit ordering.

**Reproducer portability**:
- The failure dump must be triageable on a different machine without the harness binary.
- Each `seed-N.fail.json` is accompanied by enough state (warehouse tarball or equivalent) and a `reproduce.sh` script such that, given only the cmpmap branch + `java` + `gradle`, the user can replay the failing operation and observe the same divergence.

**Pre-handoff bar**:
- Implementer must run the harness on their workstation for at least **1,000 seeds without unexplained failure** before declaring it done.
- Any failures are triaged into `KNOWN_FAILURES.md` per the Failure Protocol below.

#### M2 — Multi-Transaction Race Test (Hand-Crafted)

**Purpose**: Verify the implementation's stated claim that more than one transaction committed during a compaction can be rebased.

**Setup**: `S_0..S_n` committed, then `S_{n+1}` and `S_{n+2}` both committed against pre-compaction files (each with its own DV). `compact(S_0..S_n)` already run; compaction map present.

**Action**: A single `SparkCompactionConflictResolver.resolve(map, conflicts)` call with both delete files included in `conflicts.conflictingDeleteFiles()`.

**Assertion**: Resulting row-multiset hash equals that of `compact(S_0..S_n ∪ S_{n+1} ∪ S_{n+2})`.

**Bonus**: Repeat the test with `S_{n+1}` and `S_{n+2}` resolved in opposite order. Assert both orderings yield the same hash (confluence under permutation).

#### M3 — Concurrent Compactions Test (Hand-Crafted)

**Purpose**: Verify the documented behavior when two compactions race on the same files.

**Documented behavior** (implementer must locate where this is specified in the codebase; if undocumented, must propose the contract in the PR description):

- First compaction to commit: succeeds.
- Second compaction to commit, when its input files overlap with the first: fails cleanly with `ValidationException`.
- Compaction maps **do not** auto-repair against other compactions. Only late position-delete transactions are auto-repaired.

**Required cases**:
1. Two compactions on overlapping file sets → second fails with `ValidationException`. No silent data loss.
2. Two compactions on disjoint file sets → both succeed. Final state matches sequential composition of both compactions.

#### M4 — Code-Path Verification

**Purpose**: Prove the timed treatment region actually invokes the production resolver and remapper.

**Implementation**: A test that runs the treatment timed region against a small fixture and verifies — via runtime instrumentation (thread-local flag set inside the resolver and asserted after the timed region, or stack-trace capture at method entry) — that execution flows through:

- `SparkCompactionConflictResolver.resolve`
- `PositionDeleteRemapper.remapDVBulk`

Guards against silent path swaps (e.g., a stub that pre-computes the result in untimed setup).

#### M5 — Correctness-Check Mutation Test

**Purpose**: Prove the correctness check is not vacuous.

**Two cases**:

1. **Compaction-map corruption**: shift a run in the compaction map by one position. Run treatment. Compute the row-multiset hash. Assert it differs from the baseline's hash.
2. **Output-file corruption**: flip one byte in a Parquet output file. Hash both the baseline output and the corrupted output. Assert hashes differ.

Confirms the hash function reads file contents and that the correctness check would catch a real divergence.

#### M6 — Baseline Soundness Assertions (in-runner)

**Purpose**: Guard against trivially-cheap baseline runs (filter excluded files, no-op rewrite, etc.).

Each timed baseline iteration must read from Spark metrics and assert before logging the JSON line:

- `files_read >= 10` (expected ~13–17 source files)
- `files_written >= 10` (expected ~13 output files)
- `input_bytes_scanned >= 5 GB`

Iterations failing any assertion are flagged invalid in the JSON output. The runner aborts the scenario if no iteration passes.

#### M7 — Pre-flight Run of Existing Test Suite

**Purpose**: Demonstrate the underlying implementation is in a known-good state before adding new tests.

**Required commands**, captured to `preflight.log`:

```
./gradlew :iceberg-core:test --tests "*Compaction*"
./gradlew :iceberg-core:test --tests "*Remapping*"
./gradlew :iceberg-spark-3.5:test --tests "*Compaction*"
./gradlew :iceberg-spark-4.0:test --tests "*Compaction*"
```

**Bar**: Zero failures. Flaky tests must be reported in the writeup, not retried-until-green.

### Failure Protocol (γ)

When the fuzz harness produces a non-confluent result:

1. The operation sequence and warehouse dump are saved per M1's output spec.
2. The implementer confirms the reproducer actually reproduces on their workstation (not just inside the container).
3. The implementer adds an entry to `KNOWN_FAILURES.md` containing:
   - Seed
   - Minimized reproducer (smallest input that still triggers the bug)
   - Suspected code path
   - Suspected root cause
4. The implementer **does not fix the bug**. Scope is bounded by intent; bug fixes are a separate engagement.
5. If zero seeds fail: `KNOWN_FAILURES.md` simply records `No failures across N seeds, M operations per seed (mean)`.

### Deliverables Checklist

Under `benchmark/compaction-baseline/`:

**Build and image**:
- [ ] `Dockerfile` — self-contained image with the cmpmap branch built in.
- [ ] `build.gradle` — module declaration, dependencies on iceberg-spark and iceberg-core.
- [ ] `README.md` — build, setup, run perf benchmark, run fuzz, interpret results, reproduce a failure.

**Setup and runners**:
- [ ] `SetupMain.java` — local builder for the 8 prebuilt warehouses + S3 upload.
- [ ] `RunMain.java` — VM-side perf runner.
- [ ] `FuzzMain.java` — confluence fuzz harness.
- [ ] `CorrectnessCheck.java` — xxhash64-based row-multiset hash.

**Reproducer tooling**:
- [ ] `reproduce.sh` — replays a fuzz failure dump from `seed-N.fail.json` + warehouse tarball.

**Tests** (in `src/test/java/`):
- [ ] `TestMultiTransactionRace` — M2.
- [ ] `TestConcurrentCompactions` — M3.
- [ ] `TestCodePathVerification` — M4.
- [ ] `TestCorrectnessCheckMutation` — M5.
- [ ] `TestFuzzDeterminism` — guard for M1's determinism property.

**Analysis**:
- [ ] `analyze.py` — parses `results.jsonl`, emits `summary.csv` and a log-log plot of baseline vs. treatment wall-clock across K.

**Outputs produced by the implementer's pre-handoff run**:
- [ ] `preflight.log` — captured output of M7.
- [ ] `KNOWN_FAILURES.md` — fuzz triage log, even if empty.
- [ ] `setup_manifest.json` — snapshot IDs, file counts, compaction-map run counts, seed values for each prebuilt warehouse.

### Hand-off Criteria

The implementer's deliverable is acceptable when **all** of the following are true:

1. All listed files in the checklist exist and are non-trivial.
2. M2, M3, M4, M5, M7 pass on the implementer's workstation.
3. M1 has been run for at least 1,000 seeds on the implementer's workstation. Any failures are triaged in `KNOWN_FAILURES.md` with reproducers.
4. The Docker image builds and `FuzzMain` runs end-to-end on a fresh machine using only the image + an output directory.
5. One successful run of `RunMain` against S3 has produced a `results.jsonl` whose baseline and treatment numbers fall within the calibration band: baseline 1–10 minutes per iteration, treatment under 5 seconds per iteration at K=1M. Out-of-band numbers must be explained in the writeup (e.g., "S3 region was misconfigured, retried with same-region bucket").
6. A reproducer dump from any single fuzz failure (or a manufactured-failure example, if no real failures occurred) has been verified by the implementer to reproduce on a clean machine without the harness binary.

### Out of Scope

The implementer is **not** responsible for:

- Fixing bugs surfaced by the fuzz harness (cataloged via γ; user triages later).
- Sorted or Z-ordered compactions (excluded by design — see `compaction_maps_errata.md`).
- V2 position delete files (V3 DVs only).
- Application-side auto-remapping (`BaseRowDelta` auto-remap is listed as pending work in `CLAUDE.md`).
- Multi-region or multi-cloud benchmarks.
- Reproducing the workshop paper's existing numbers.
