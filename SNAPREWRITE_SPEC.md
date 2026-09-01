# Snapshot Rewriting with Compaction Maps — Prototype Design Specification

Companion to `COMPACT_SPEC.md`. Implements the design sketched in
[Inverting Iceberg Snapshots with Compaction Maps](https://cdouglas.github.io/posts/2026/08/rewriting-snapshots)
(source: `../cdouglas.github.io/_posts/2026-08-31-snaprewrite.md`).

**Status:** design only. No code written. Awaiting review.

---

## 1. Goal

Given a compaction `C_B` and the compaction map it emitted, rewrite the snapshots in the
window `[C_A, C_B)` so that they reference `C_B`'s layout instead of the layout they were
prepared against. The sequence of *table states* addressable by snapshot id is preserved
exactly; the *files* pinned by those snapshots are replaced by `C_B`'s files plus a small
amount of newly materialized data.

The payoff is that `C_A`'s outputs and every interstitial data/delete file in the window
become unreachable and can be reclaimed.

### Non-goals for the prototype

- Sorted / Z-ordered compactions (out of scope for compaction maps generally).
- Equality deletes.
- Schema or partition-spec evolution inside the rewrite window.
- Preserving CDC / changelog semantics. The rewrite deliberately destroys them
  (see §4.6); this is the design's acknowledged cost, not a bug to fix.
- REST catalog support (see §5.1).

---

## 2. Soundness

### 2.1 The induction

Let the window be `C_A = S_0, T_1, T_2, ..., T_n, C_B`, where `S_k` is the table state
after `T_k`. `C_B` compacts `S_n`, so `state(C_B) = state(S_n)`.

Rows are identified by their reference in the *original* layout: a pair
`r = (origFile, origPos)`. Maintain, going backward, a **locator**

```
loc_k : (origFile, origPos) -> (newFile, newPos)
```

defined for exactly the rows live in `S_k`, where `newFile` is a file in the rewritten
representation `R_k`.

**Base.** `R_n` = `C_B`'s file set; `loc_n` = the compaction map (composed across the
window if `C_B` chains through intermediate maps). Every row live at `S_n` is preserved by
the compaction, so `loc_n` is total on `live(S_n)`.

**Step `k -> k-1`.** With `I_k = live(S_k) \ live(S_{k-1})` (rows `T_k` inserted) and
`D_k = live(S_{k-1}) \ live(S_k)` (rows `T_k` deleted):

- Every row in `I_k` is live at `S_k` by construction, so `loc_k` covers it. Emit a
  position delete at `loc_k(r)` for each. **These are pure map arithmetic — no data is
  read.**
- No row in `D_k` is present in `R_k`: it is absent from `C_B` (dead before the
  compaction) and absent from every resurrection file (each is created by the inverse of
  the single transaction that killed those rows). It must be **materialized**: read from
  the original layout and written to a new data file `g_k`.
- `loc_{k-1} = (loc_k \ I_k) ∪ { r -> (g_k, idx_r) : r ∈ D_k }`.

`R_{k-1} = R_k + g_k + (position deletes for I_k)`, and `live(R_{k-1}) = live(S_{k-1})` as
a multiset. Induction closes at `R_0 = ` the rewritten `C_A` snapshot.

### 2.2 The figure, worked

Using the post's example (`bidirection.png`), with 0-based positions:

| | contents | state |
|---|---|---|
| `C_A` | `f_A = [a, b, c]` | `{a,b,c}` |
| `T_α` | adds `f_α = [d, e]`; deletes `f_A:2` (`c`) | `{a,b,d,e}` |
| `T_β` | adds `f_β = [f, g]`; deletes `f_A:1` (`b`), `f_α:0` (`d`) | `{a,e,f,g}` |
| `C_B` | `f_B = [a, e, f, g]` | `{a,e,f,g}` |

Compaction map: `f_A:0 -> f_B:0`, `f_α:1 -> f_B:1`, `f_β:[0,2) -> f_B:[2,4)`.

- **`T_β⁻¹`**: `I_β = {f, g}` → `loc_n` gives `f_B:2, f_B:3` → DV `{2,3}` on `f_B`.
  `D_β = {b, d}` → materialize `g_β = [b, d]`. `loc_{α} = { a->f_B:0, e->f_B:1,
  b->g_β:0, d->g_β:1 }`. State: `{a,e} ∪ {b,d} = {a,b,d,e}` ✓
- **`T_α⁻¹`**: `I_α = {d, e}` → `loc_α` gives `f_B:1` (for `e`) and `g_β:1` (for `d`) →
  DV `{1,2,3}` on `f_B`, DV `{1}` on `g_β`. This is the figure's `C_B:2, T_β⁻¹:2`.
  `D_α = {c}` → materialize `g_α = [c]`. State: `{a} ∪ {b} ∪ {c} = {a,b,c}` ✓

### 2.3 Storage accounting (a testable invariant)

Each dead row is materialized **exactly once**, by the inverse of the transaction that
deleted it. Therefore, letting `dead` = rows that died inside the window:

```
before:  |C_A| + |window inserts| + |C_B|
after:   |C_B| + |dead| + |position deletes|
and      |C_A| + |window inserts| = |C_B| + |dead|
=>       saving ≈ |C_B| − |position deletes|
```

**Retaining the history costs one full copy of the table less than it did.** This is a
strong result and it is directly assertable in tests (§8.4).

The corollary is the cost model: the rewrite reads and rewrites every row that died in the
window. A window whose transactions delete most of the table is expensive and saves
little. The planner should expose `dead bytes / reclaimed bytes` and refuse above a
configurable ratio.

### 2.4 FastAppend falls out

If `T_k` is insert-only then `D_k = ∅`, no data is read or written, and `T_k⁻¹` is a pure
position-delete vector derived from the compaction map alone — exactly the post's claim.
This is an assertion in the test suite, not a special case in the code: **an insert-only
snapshot whose rows all survive to `C_B` must produce zero new data files.**

---

## 3. Preconditions — when a rewrite is *not* lossless

The planner computes these before writing anything. Any failure aborts the window; nothing
is committed. Per the requirement that only lossless rewrites commit, these are hard
refusals, not warnings.

| # | Condition | Why |
|---|---|---|
| P1 | Format version == 2 | Phase 1 scope; v3 row lineage is unsolved (§5.4) |
| P2 | No equality deletes anywhere in the window | `D_k` is not positionally computable |
| P3 | `schema-id` identical across the window and `C_B` | A column dropped after `S_k` would read as null from `C_B`'s files |
| P4 | `spec-id` identical across the window and `C_B` | Resurrection files must be partition-aligned to the snapshot's spec |
| P5 | Every REPLACE snapshot in the window carries a compaction map | Otherwise `loc` has holes |
| P6 | Every source data file needed for resurrection still exists | Cannot materialize a row from a GC'd file |
| P7 | `C_B` is older than `snapshot-rewrite.min-age-ms` | An in-flight transaction based on a rewritten snapshot would validate against nonsense (§4.6) |
| P8 | `dead-bytes / reclaimable-bytes` below threshold | Cost guard, not correctness |
| P9 | No branch/tag ref points *into* the window in a way the caller excluded | Refs are preserved by id, so this is informational; asserted for clarity |

P3 deserves a note: field-id based reads make *additive* schema change harmless, but a
dropped column is not. The prototype takes the conservative rule (identical `schema-id`)
rather than trying to classify changes.

---

## 4. Design decisions forced by this codebase

### 4.1 There is no "replace snapshot" API

`TableMetadata.Builder.addSnapshot` (`core/.../TableMetadata.java:1232`) rejects a
duplicate snapshot id and requires `sequenceNumber > lastSequenceNumber`. Neither can be
satisfied by an in-place rewrite.

**Decision:** construct `TableMetadata` through its package-private constructor
(`TableMetadata.java:273`) — the same path `TableMetadataParser.fromJson` uses
(`TableMetadataParser.java:565`), so a JSON round-trip does not re-run builder validation.
Commit with `TableOperations.commit(base, rewritten)`.

Consequence: the implementation must live in package `org.apache.iceberg`
(`ManifestLists.write` and `DeleteFileIndex` are package-private too).

### 4.2 Data sequence numbers must be re-stamped

Within a rewritten snapshot `S_k`, the data files are `C_B`'s (data-seq `σ_B`) and the
delete files are new. Iceberg applies a positional delete to a data file only when
`delete.dataSeq >= data.dataSeq`. Stamping the new deletes at `σ_k < σ_B` would leave them
**inert** — deleted rows would silently reappear. This is the single most dangerous failure
mode in the design and it fails *open*.

**Decision:** in every rewritten snapshot, write

- data files (both `C_B`'s and resurrection files) as `EXISTING` entries with
  `dataSequenceNumber = baseSeq`, where `baseSeq` = the sequence number of the oldest
  snapshot in the window;
- delete files with `dataSequenceNumber = σ_k`, the rewritten snapshot's own sequence
  number.

Since `baseSeq <= σ_0 < σ_k` for every rewritten snapshot, every delete applies. This also
handles the cross-step case: resurrection file `g_k` is created by `T_k⁻¹` but deleted from
by `T_j⁻¹` for `j < k`, i.e. by a delete stamped `σ_{j-1} < σ_{k-1}`; stamping all data at
`baseSeq` keeps that ordering valid.

`ManifestWriter.existing(file, snapshotId, dataSeq, fileSeq)` (`ManifestWriter.java:154`)
gives the required control.

**This is a spec deviation** and must be documented: one physical data file carries
different `data_sequence_number` values in different snapshots. It is correct for
single-snapshot reads (readers only compare within one snapshot's file set) and meaningless
for incremental scans across the window — which the design already breaks (§4.6).

### 4.3 `D_k` is a set difference, not the commit delta

A transaction may re-delete an already-dead position; delete files are idempotent and
overlapping delete sets are legal. Taking `D_k` = "positions in the delete files `T_k`
added" would resurrect rows that were *already dead* at `S_{k-1}`, **adding rows that never
existed in that state**. `D_k` must be computed as `live(S_{k-1}) \ live(S_k)` from
materialized live sets, via `DeleteFileIndex` at each snapshot.

Same reasoning for whole-file removal: if `T_k` drops a data file outright, `D_k` gains the
file's rows *live at `S_{k-1}`*, not all of its rows.

### 4.4 Reclaim needs its own step

`ExpireSnapshots`/`RemoveSnapshots` only delete files reachable from *expired* snapshots.
After a rewrite the detached files are reachable from *no* snapshot, so nothing ever deletes
them. Additionally, entries in `TableMetadata.previousFiles` (the metadata log) still point
at metadata JSONs that reference the old manifest lists.

**Decision:** the rewriter returns an explicit reclaim manifest — old data files, delete
files, manifests, and manifest lists — and deletion is a separate, opt-in call. The reclaim
step must refuse to delete anything still referenced by a retained metadata-log entry, or
the rewrite must trim the metadata log; the prototype does the former and reports what it
withheld.

### 4.5 Row lineage blocks v3 (deferred to phase 2)

`TableMetadata.Builder.addSnapshot` requires `firstRowId != null` for format ≥ 3, and
materialized `_row_id` write support exists only in the Spark layer
(`spark/v4.0/.../ExtractRowLineage.java`, `SparkWriteBuilder.java`) — `iceberg-data`'s
writers have no path for it, though the *read* path handles materialized values
(`ParquetValueReaders`, `avro/ValueReaders`).

A resurrected row written without a materialized `_row_id` inherits `first_row_id + pos`
from its new file and therefore **changes identity**. Under v3 row lineage that is a
losslessness violation, so v3 is out of scope for phase 1 (precondition P1).

### 4.6 What the rewrite legitimately destroys

Stated plainly so tests do not chase it:

- `snapshot_id` / `status` on manifest entries no longer describe which commit added a
  file. All entries are written `EXISTING` under the rewritten snapshot.
- Incremental scans (`appendsBetween`, CDC) over the rewritten window return garbage.
- `added-*` / `deleted-*` summary fields describe the new file-set delta, not the original
  transaction.
- `_file` and `_pos` metadata columns change for time-travel reads.

The post accepts all of these. P7's age threshold is the operational mitigation.

---

## 5. Architecture

```
core (package org.apache.iceberg — package-private access required)
  SnapshotRewrite.java            public entry point + Result
  SnapshotRewritePlanner.java     metadata-only planning; produces SnapshotRewritePlan
  SnapshotRewritePlan.java        immutable: per-snapshot deletes, resurrection requests,
                                  manifest layout, reclaim list, cost estimate
  LiveRowIndex.java               live positions per data file at a snapshot (DeleteFileIndex)
  RowLocator.java                 loc_k; run-compressed (start dense, optimize later)
  ResurrectionRequest.java        (spec, partition, schema, ordered List<SourceRowRef>)
  RowResurrector.java             SPI: materialize(ResurrectionRequest) -> DataFile
  SnapshotRewriteWriter.java      writes delete files, manifests, manifest lists
  SnapshotRewriteCommitter.java   rebuilds TableMetadata; TableOperations.commit
  SnapshotRewriteReclaim.java     opt-in deletion of the detached file set

data (iceberg-data)
  data/src/main/java/org/apache/iceberg/data/GenericRowResurrector.java
      raw positional read of source files + GenericAppenderFactory write.
      Reads files directly (not via TableScan) so deletes are NOT applied — we want the
      row at position p regardless of its liveness in the current snapshot.
```

Layering rationale: core cannot read data rows (no Parquet/ORC record reader), so
materialization is an SPI. `GenericRowResurrector` serves the local tests; a Spark
implementation is the obvious phase-3 follow-on for scale.

### 5.1 Catalog compatibility

Direct `TableMetadata` reconstruction works with `HadoopTables`, `HadoopCatalog`,
`TestTables`, and any `TableOperations` that accepts a whole-metadata swap. It is **not**
expressible in the REST catalog protocol, which has no `MetadataUpdate` for replacing a
snapshot. A production version would need a new update type (`replace-snapshot`) — worth
noting in the post as a spec-level ask.

---

## 6. Algorithm

**Phase A — plan (metadata only, no IO beyond manifests):**

1. Locate `C_B`: newest snapshot whose manifests carry a `compactionMapLocation`
   (`ManifestFile.compactionMapLocation()`, field 521). Load via `CompactionMaps.read`.
2. Locate `C_A`: the previous compaction, or the caller-supplied window floor.
3. Check P1–P9. Abort the window on any failure, reporting which.
4. Build `loc_n` from the map, composing through `CompactionMapChain` /
   `CompactionMaps.compose` if the window contains more than one compaction.
5. For `k = n .. 1`: compute `live(S_k)`, `live(S_{k-1})` via `LiveRowIndex`; derive `I_k`,
   `D_k`; emit position deletes at `loc_k(I_k)`; emit a `ResurrectionRequest` per
   (spec, partition) for `D_k`; update `loc`.
6. Accumulate per-snapshot file sets and the reclaim list.

**Phase B — materialize:** call the `RowResurrector` for each request. Resurrection files
are grouped by partition and ordered deterministically (source path, then position) so runs
are reproducible and diffable.

**Phase C — write deletes:** one position-delete file per rewritten snapshot per partition,
sorted by `(path, pos)` via `SortingPositionOnlyDeleteWriter`. (Phase 2: a DV per data file
per snapshot.)

**Phase D — write manifests:** per rewritten snapshot, a data manifest and a delete
manifest, then a manifest list. Sequence-number stamping per §4.2.

**Phase E — swap:** rebuild `TableMetadata`, preserving for every rewritten snapshot its
`snapshot-id`, `parent-snapshot-id`, `sequence-number`, `timestamp-ms`, `schema-id`, and
`operation`; replacing `manifest-list`; recomputing `total-*` summary fields; adding
`snapshot-rewritten-from` = the original manifest-list location. Commit.

**Phase F — reclaim (opt-in, separate call):** delete the detached set, minus anything
still reachable from a retained metadata-log entry.

---

## 7. Configuration

```
snapshot-rewrite.enabled                 = false
snapshot-rewrite.min-age-ms              = 86400000   # P7: don't rewrite recent snapshots
snapshot-rewrite.max-dead-ratio          = 0.5        # P8 cost guard
snapshot-rewrite.reclaim                 = false      # phase F is opt-in
```

---

## 8. Regression tests

Local, no Spark. Implementation tests in `core/src/test/java/org/apache/iceberg/`;
end-to-end row-level tests in `data/src/test/java/org/apache/iceberg/data/snaprewrite/`
(the `data` module already depends on `iceberg-core` `testArtifacts`, so `TestTables` is
available alongside `IcebergGenerics`, `FileHelpers`, and `GenericAppenderHelper`).

### 8.1 Harness

- `LocalCompactor` — test utility performing a real bin-pack compaction: reads each source
  file's live rows in position order, appends survivors to a target writer, records runs,
  and commits through `RewriteDataFilesCommitManager` with `FilePositionMapping`s so a
  **real** compaction map is produced and attached. Not a hand-built map.
- `SnapshotRewriteTestBase` — builds a table, runs a scripted or generated workload,
  compacts, rewrites, and diffs.

### 8.2 The oracle

For **every** snapshot id in the table, not just the window:

```
before = { id -> multiset(IcebergGenerics.read(t).useSnapshot(id)) }   # pre-rewrite
rewrite()
after  = { id -> multiset(IcebergGenerics.read(t).useSnapshot(id)) }   # post-rewrite
assert before.equals(after)
```

Multiset, not set — Iceberg tables may contain duplicate rows and the rewrite preserves
rows by position, not by value. Plus, per snapshot: `total-records` summary unchanged;
`snapshot-id`, `parent-snapshot-id`, `sequence-number`, `timestamp-ms` unchanged.

### 8.3 Transaction-type matrix (`TestSnapshotRewriteLossless`)

| # | Case | What it pins down |
|---|---|---|
| 1 | `figureExample` — the post's exact `C_A/T_α/T_β/C_B` history | The canonical derivation, including the `T_β⁻¹:2` cross-reference |
| 2 | FastAppend only, all rows survive | §2.4: **zero new data files**, deletes only |
| 3 | FastAppend, some rows later deleted | Partial survival; resurrection by a later inverse |
| 4 | Position deletes against `C_A`'s files | Resurrection from the compaction's own output |
| 5 | Position deletes against an interstitial append | The `d` case; `loc` must resolve to a resurrection file |
| 6 | Mixed insert+delete in one `RowDelta` | `I_k` and `D_k` non-empty in the same step |
| 7 | Insert and delete of the *same* rows in one commit | `I_k` must exclude rows dead at `S_k` |
| 8 | `DeleteFiles` removing a whole data file | `D_k` = file's rows live at `S_{k-1}`, not all rows |
| 9 | **Re-delete of an already-dead position** | §4.3 — the set-difference bug; naive impl adds phantom rows |
| 10 | A transaction whose every inserted row dies before `C_B` | Full resurrection of an interstitial file |
| 11 | Delete of every row in a file (file empty in `C_B`) | Empty-mapping edge case |
| 12 | Partial compaction — `C_B` leaves some files untouched | `loc` identity for unmapped files |
| 13 | Two compactions inside the window (chained maps) | `CompactionMapChain` composition (phase 3) |
| 14 | Empty / no-op commit in the window | Degenerate step |
| 15 | Interleaved appends from two writers | Ordering independence |
| 16 | Duplicate rows across snapshots | Multiset oracle |

### 8.4 Structural assertions (`TestSnapshotRewriteReclaim`)

- **No remaining dependency:** for every rewritten snapshot, the set of referenced files
  intersected with (`C_A`'s outputs ∪ interstitial adds) is empty.
- **Storage:** reachable bytes after ≈ `|C_B| + |dead rows| + |deletes|`, and the saving
  vs. before is within tolerance of `|C_B|` (§2.3).
- **Reclaim safety:** every file in the reclaim list is unreachable from every snapshot and
  from every retained metadata-log entry.
- **Expire interop:** `ExpireSnapshots` on the oldest rewritten snapshot deletes nothing
  still needed; re-run the oracle afterward.
- **Round-trip:** `TableMetadataParser.toJson` → `fromJson` → re-run the oracle, proving
  the synthesized metadata survives a real reload.

### 8.5 Refusal tests (`TestSnapshotRewriteSkips`)

One test per precondition P1–P8: construct the violating history, assert the rewrite
refuses, assert **the table is byte-identical to before** (nothing partially committed),
and assert the reported reason names the right precondition.

### 8.6 Fuzz (`TestSnapshotRewriteFuzz`)

Seeded random workloads over the op mix in §8.3, modeled on
`benchmark/compaction-baseline`'s `WorkloadGenerator` but local and v2. Per seed: generate,
compact, rewrite, run the full oracle over all snapshots. Log the anchor seed so failures
reproduce, following the convention already established in the compaction-baseline fuzzer.

---

## 9. Milestones

| Phase | Content | Exit criterion |
|-------|---------|----------------|
| 1 | v2, unpartitioned, single window, no chaining. Planner + committer + `GenericRowResurrector`. Tests 8.3 #1–11, 14–16; 8.4; 8.5 | Oracle green; figure example reproduces |
| 2 | Partitioned tables; partial compaction (#12); reclaim step wired | 8.4 reclaim assertions green |
| 3 | Chained maps inside the window (#13); recursive windows back through older compactions | Multi-window oracle green |
| 4 | v3 / DVs: materialized `_row_id` in the resurrection writer, `first-row-id` reconstruction, P1 lifted | v3 oracle green including `_row_id` stability |
| 5 | Spark `RowResurrector` for scale; cost model measurement | — |

---

## 10. Open questions

1. **Should `total-records` be the invariant, or full summary fidelity?** The prototype
   asserts `total-records` and recomputes the rest. Anything stricter conflicts with §4.6.
2. **Metadata-log trimming.** Reclaim is blocked by retained metadata JSONs. Trim the log
   as part of the rewrite, or accept delayed reclaim? Prototype accepts delay and reports.
3. **REST catalog.** A production design needs a `replace-snapshot` metadata update. Worth
   raising in the post as a concrete spec ask.
4. **Recursion depth.** Rewriting back through many compactions accumulates resurrection
   files; each older window resurrects rows that died in *its* window. The §2.3 accounting
   holds per window, but the aggregate should be measured before recommending deep
   recursion.
