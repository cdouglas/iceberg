# Snapshot Rewriting with Compaction Maps — Prototype Design Specification

Companion to `COMPACT_SPEC.md`. Implements the design sketched in
[Inverting Iceberg Snapshots with Compaction Maps](https://cdouglas.github.io/posts/2026/08/rewriting-snapshots)
(source: `../cdouglas.github.io/_posts/2026-08-31-snaprewrite.md`).

**Status:** phases 1-4 implemented and passing. Core planner/writer in
`core/src/main/java/org/apache/iceberg/snaprewrite/`, generic IO in
`data/src/main/java/org/apache/iceberg/data/GenericSnapshotRewriteIO.java`, tests in
`data/src/test/java/org/apache/iceberg/data/snaprewrite/` (62 cases: 12 lossless, 7 structural,
9 refusal, 7 report, 4 layout, 5 commit/reclaim, 20 fuzz seeds). Section 11 records what the
implementation changed about the design.

---

## 1. Goal

Given a compaction `C_B` and the compaction map it emitted, rewrite the snapshots in the
window `[C_A, C_B)` so that they reference `C_B`'s layout instead of the layout they were
prepared against. The sequence of *table states* addressable by snapshot id is preserved
exactly; the *files* pinned by those snapshots are replaced by `C_B`'s files plus a small
amount of newly materialized data.

The payoff is that `C_A`'s outputs and every interstitial data/delete file in the window
become unreachable and can be reclaimed.

**The prototype is non-destructive** (§4.4): it builds the rewritten snapshots for real and
exposes them as a shadow table for verification, but does not commit them back. Its primary
output is a report of what *would* be reclaimed, since reclaim is the point of the design
and the quantity we most want to establish.

### Non-goals for the prototype

- Sorted / Z-ordered compactions (out of scope for compaction maps generally).
- Equality deletes.
- Schema or partition-spec evolution inside the rewrite window.
- Preserving CDC / changelog semantics. The rewrite deliberately destroys them
  (see §4.6); this is the design's acknowledged cost, not a bug to fix.
- Committing the rewrite back to the source table (deferred to phase 4; see §4.4).
- REST catalog support (see §5.2).

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

**Retaining the history costs one full copy of the table less than it did**, less what the
delete vectors cost.

That second term is not a rounding error, and measuring it changed the picture. Every rewritten
snapshot must delete every row inserted after it, so over a window of `m` transactions each
inserting `r` rows the total is on the order of `r · m² / 2` positions. On a measured 30k-row
window (`TestSnapshotRewriteReport`) the deletes came to well over half the saving. Two
consequences: **rewrite often rather than letting history pile up between compactions**, and the
economics are a per-window question the report has to answer, not a property of the design.

Below a certain scale the rewrite simply loses. Each rewritten snapshot needs a manifest and a
manifest list -- several KiB of Avro apiece -- so a table whose data files are smaller than its
metadata pays more than it reclaims. The report prints the negative rather than hiding it
(`TestSnapshotRewriteReport#insertOnlyWindowReportsNoResurrection`).

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
| P1 | Format version == 2 | Phase 1 scope; v3 row lineage is unsolved (§4.5) |
| P2 | No equality deletes anywhere in the window | `D_k` is not positionally computable |
| P3 | `schema-id` identical across the window and `C_B` | A column dropped after `S_k` would read as null from `C_B`'s files |
| P4 | `spec-id` identical across the window and `C_B` | Resurrection files must be partition-aligned to the snapshot's spec |
| P5 | The *target* compaction carries a compaction map | Without one there is nothing to rewrite onto. A map-less replace *inside* the window is not refused -- see §11.6 |
| P6 | Every source data file needed for resurrection still exists | Cannot materialize a row from a GC'd file |
| P7 | `C_B` is older than `snapshot-rewrite.min-age-ms` | An in-flight transaction based on a rewritten snapshot would validate against nonsense (§4.6) |
| P8 | `dead-bytes / reclaimable-bytes` below threshold | Cost guard, not correctness |
| P9 | No branch/tag ref points *into* the window in a way the caller excluded | Refs are preserved by id, so this is informational; asserted for clarity |
| P10 | Every REPLACE in the window leaves the live row count unchanged | §11.1 skips diffing over a compaction on the assumption it is a logical no-op; a replace that also changed data would break that silently |

P3 deserves a note: field-id based reads make *additive* schema change harmless, but a
dropped column is not. The prototype takes the conservative rule (identical `schema-id`)
rather than trying to classify changes.

---

## 4. Design decisions forced by this codebase

### 4.1 There is no "replace snapshot" API

`TableMetadata.Builder.addSnapshot` (`core/.../TableMetadata.java:1232`) rejects a
duplicate snapshot id and requires `sequenceNumber > lastSequenceNumber`. Neither can be
satisfied by an in-place rewrite.

**Decision:** treat the rewrite as an `unsafe` block in the Rust sense — we assert soundness
rather than satisfying what the framework can prove — and confine every such assertion to a
single accessor:

```
core/src/main/java/org/apache/iceberg/SnapshotRewriteUnsafe.java
```

It wraps the package-private surface the rewrite needs, one method per escape hatch, each
carrying the invariant the caller must uphold:

| Accessor | Wraps | Invariant asserted |
|---|---|---|
| `newMetadata(...)` | `TableMetadata` constructor (`TableMetadata.java:273`) | snapshot identity fields preserved; `nextRowId` self-consistent |
| `writeManifestList(...)` | `ManifestLists.write` | entries reference only files written by this rewrite or by `C_B` |
| `liveEntries(...)` | `DeleteFileIndex` | used only for live-set computation, never for commit validation |
| `stampExisting(...)` | `ManifestWriter.existing` | §4.2 stamping rule |

Nothing outside this class touches package-private state, so the audit surface for "what
did we assert that Iceberg would otherwise check" is one file. The class is deliberately
*not* exported through the public API.

The constructor path is the same one `TableMetadataParser.fromJson` uses
(`TableMetadataParser.java:565`), so a JSON round-trip does not re-run builder validation —
which is why §8.4's round-trip test is a real check and not a formality.

Consequence: the implementation must live in package `org.apache.iceberg`.

### 4.2 Data sequence numbers must be re-stamped

**What the check actually enforces.** A positional delete applies to a data file only when
`data.dataSequenceNumber <= delete.dataSequenceNumber`. This is not a structural check —
it is how Iceberg makes a concurrent append and a concurrent position-delete *commute*. A
delete file written at sequence number `N` was prepared against a snapshot whose data files
all have sequence number `<= N`. Any data file with sequence number `> N` was added by a
transaction the delete's author never saw, so applying the delete to it would tombstone
rows that were never examined. The sequence number is the delete's "as-of" watermark.

The rewrite deliberately inverts that invariant: its delete vectors reference `C_B`'s files,
written long *after* the snapshot they are being stamped into. The check is doing its job
by rejecting them. What licenses the bypass is exactly the compaction map — it proves the
target rows are the *same rows*, relocated, not rows the original transaction never saw.
That is the assertion §4.1 confines to `SnapshotRewriteUnsafe`.

**The failure mode differs by format version, and v2 — our phase-1 target — fails silently:**

- **v2 position deletes**: `DeleteFileIndex.PositionDeletes.filter(seq)`
  (`DeleteFileIndex.java:659`) slices a sequence-sorted array from `findStartIndex`
  (`DeleteFileIndex.java:620`). Delete files with `seq < dataFile.seq` are simply not in
  the returned slice. No exception, no log line — the deletes are dropped and the deleted
  rows reappear in the scan.
- **v3 DVs**: `findDV` (`DeleteFileIndex.java:199`) raises `ValidationException` when
  `dv.dataSequenceNumber() < seq` (`:207`). Loud failure.

Because phase 1 is v2, a mis-stamped rewrite produces a *plausible table that reads wrong*.
This makes the §8.2 oracle and the §8.4 round-trip load-bearing rather than confirmatory,
and it is the single strongest argument for the non-destructive default in §4.4.

**Decision (per-snapshot stamping):** in each rewritten snapshot `S_k`, stamp *everything* —
`C_B`'s data files, the resurrection files, and the delete files — at `σ_k`, that snapshot's
own sequence number. Since the comparison is `<=`, equality passes, and every delete
applies to every data file in the snapshot, which is precisely what a rewritten snapshot
wants. The rule states in one line: **a rewritten snapshot stamps its entire contents at its
own sequence number**, as though every file had been added by it. `ManifestWriter.existing(
file, snapshotId, dataSeq, fileSeq)` (`ManifestWriter.java:154`) provides the control.

This also handles the cross-step case without a special rule: resurrection file `g_k` is
created by `T_k⁻¹` but deleted from by `T_j⁻¹` for `j < k`; in snapshot `S_{j-1}` both are
stamped `σ_{j-1}` and the delete applies.

**This is a spec deviation** and is documented as such: one physical data file carries
different `data_sequence_number` values in different snapshots. It is correct for
single-snapshot reads — readers only compare within one snapshot's file set — and
meaningless for incremental scans across the window, which the design already breaks (§4.6).

### 4.3 `D_k` is a set difference, not the commit delta

*This is reconstruction arithmetic, not a claim about isolation.* Whatever produced the
history, the delete sets recorded in it can overlap, so `D_k` must be computed as
`live(S_{k-1}) \ live(S_k)` from materialized live sets rather than read off the delete
files `T_k` added. Taking the commit delta would resurrect rows that were *already dead* at
`S_{k-1}`, **inserting rows that never existed in that state**. The same reasoning applies
to whole-file removal: if `T_k` drops a data file outright, `D_k` gains the file's rows
*live at `S_{k-1}`*, not all of its rows.

Overlapping delete sets are not exotic. They arise from a single writer (a `MERGE` that
deletes an already-deleted row, a retry after partial failure, a
`DeleteGranularity.PARTITION` rewrite), and they arise from concurrency.

**Background, since the concurrency case invites the question:** Iceberg does not implement
item-level write-write conflict detection for row deletes at any isolation level. Under
`snapshot` isolation a `RowDelta` validates `validateDataFilesExist` — that the data files
it references are still present — and optionally `validateDeletedFiles`; it never compares
positions, so two concurrent deletes of the same row both commit. Textbook SI, under
first-committer-wins on the same item, would abort the second. Iceberg is weaker here.
Under `serializable` it adds `validateNoNewDeleteFiles` / `validateAddedDataFiles`
(`MergingSnapshotProducer.java:658`, `:366`), which are *coarser*, not finer: any delete
file added since the base snapshot that could apply to records matching the conflict filter
aborts the commit, at file/partition granularity. So the row-level overlap is never
detected, and the file-level check over-approximates.

**Validation cost**, since it was asked: it is a manifest scan, not a data scan.
`validationHistory` (`MergingSnapshotProducer.java:972`) walks snapshots from the base to
the current head collecting `DELETES`-content manifests from the relevant operations, then
builds a `DeleteFileIndex` over their entries with partition-set and expression pruning. Cost
is roughly *(snapshots since base) × (delete manifests per snapshot)* manifest reads, with
no data file access. Cheap next to the write itself; it degrades when a writer holds a stale
base across many commits, which is the same condition that makes compaction maps worth
having.

### 4.4 The prototype does not commit — it reports

Reclaim was the motivation for the design, so what the prototype most needs to establish is
*how much there is to reclaim*, not that it can mutate a table in place. Committing a
whole-metadata swap is also where the §4.2 silent-failure mode does its damage.

**Decision: the prototype is non-destructive by default.** It builds the rewritten snapshots
for real — resurrection data files, delete files, manifests, manifest lists, and a complete
`TableMetadata` — but never calls `TableOperations.commit`. The synthesized metadata is
exposed as a read-only `Table` so the full oracle runs against genuine Iceberg scan planning:

```java
SnapshotRewriteResult result = SnapshotRewrite.forTable(table).plan().materialize();
Table shadow = result.asTable();      // BaseTable over synthesized metadata, never committed
result.report();                       // savings, per-snapshot breakdown
result.commit();                       // opt-in, phase 4
```

The source table is untouched, so a failed or wrong rewrite costs only scratch files.

**The report is the phase-1 deliverable.** Per window and per snapshot:

```
window  C_A(snap 8812…) .. C_B(snap 9930…)   6 snapshots
  reclaimable   -412.6 MiB   (C_A outputs 388.1, interstitial data 21.3, deletes 3.2)
  resurrected   + 31.4 MiB   (2 files, 41,802 rows)
  delete files  +  0.9 MiB   (6 files, 118,447 positions)
  net           -380.3 MiB   (92.2% of reclaimable)
  rows          inserted 512,338   deleted 41,802   dead-ratio 0.076
```

`net ≈ |C_B| − |deletes|` is the §2.3 prediction; printing both the prediction and the
measurement makes the accounting claim falsifiable on real tables rather than only in tests.
A `--dry-run` mode stops after planning and estimates from manifest metadata alone
(`record_count`, `file_size_in_bytes`), writing nothing at all — cheap enough to run across
a whole table's history to find windows worth rewriting.

### 4.5 Row lineage blocks v3 (deferred)

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
  SnapshotRewrite.java            public entry point; fluent builder
  SnapshotRewriteUnsafe.java      §4.1 — the ONLY file touching package-private state
  SnapshotRewritePlanner.java     metadata-only planning; produces SnapshotRewritePlan
  SnapshotRewritePlan.java        immutable: per-snapshot deletes, resurrection requests,
                                  manifest layout, reclaim list, cost estimate
  SnapshotRewriteResult.java      materialized output: asTable(), report(), commit()
  SnapshotRewriteReport.java      §4.4 savings accounting, predicted vs measured
  LiveRowIndex.java               live positions per data file at a snapshot (DeleteFileIndex)
  RowLocator.java                 loc_k; run-compressed (start dense, optimize later)
  ResurrectionRequest.java        (spec, partition, schema, ordered List<SourceRowRef>)
  RowResurrector.java             SPI: materialize(ResurrectionRequest) -> DataFile
  SnapshotRewriteWriter.java      writes delete files, manifests, manifest lists
  SnapshotRewriteReclaim.java     opt-in deletion of the detached file set (phase 4)

data (iceberg-data)
  data/src/main/java/org/apache/iceberg/data/GenericRowResurrector.java
      raw positional read of source files + GenericAppenderFactory write.
      Reads files directly (not via TableScan) so deletes are NOT applied — we want the
      row at position p regardless of its liveness in the current snapshot.
```

Layering rationale: core cannot read data rows (no Parquet/ORC record reader), so
materialization is an SPI. `GenericRowResurrector` serves the local tests; a Spark
implementation is the obvious follow-on for scale.

### 5.1 The shadow table

`SnapshotRewriteResult.asTable()` returns a `BaseTable` over a `TableOperations` that serves
the synthesized `TableMetadata` and refuses `commit`. Reads go through unmodified Iceberg
scan planning — `DeleteFileIndex`, manifest filtering, sequence-number matching — so the
oracle exercises exactly the code paths that would run against a committed table, including
the §4.2 stamping rule, without ever mutating the source table.

This is what makes the §4.2 silent-failure mode testable: a mis-stamped delete file is
dropped by `PositionDeletes.filter` during shadow-table planning exactly as it would be on a
real table, and the oracle catches the reappearing rows.

### 5.2 Catalog compatibility

`commit()` (phase 4) needs a whole-metadata swap, which works with `HadoopTables`,
`HadoopCatalog`, `TestTables`, and any `TableOperations` accepting one. It is **not**
expressible in the REST catalog protocol, which has no `MetadataUpdate` for replacing a
snapshot. A production version would need a new update type (`replace-snapshot`) — worth
raising in the post as a concrete spec-level ask. The non-destructive path (§4.4) has no
such constraint and works against any catalog.

---

## 6. Algorithm

**Phase A — plan (metadata only; this is all `--dry-run` executes):**

1. Locate `C_B`: newest snapshot whose manifests carry a `compactionMapLocation`
   (`ManifestFile.compactionMapLocation()`, field 521). Load via `CompactionMaps.read`.
2. Locate `C_A`: the previous compaction, or the caller-supplied window floor.
3. Check P1–P9. Abort the window on any failure, reporting which.
4. Build `loc_n` from the map, composing through `CompactionMapChain` /
   `CompactionMaps.compose` if the window contains more than one compaction.
5. For `k = n .. 1`: compute `live(S_k)`, `live(S_{k-1})` via `LiveRowIndex`; derive `I_k`,
   `D_k` (§4.3); emit position deletes at `loc_k(I_k)`; emit a `ResurrectionRequest` per
   (spec, partition) for `D_k`; update `loc`.
6. Accumulate per-snapshot file sets, the reclaim list, and the estimated report from
   manifest metadata (`record_count`, `file_size_in_bytes`) — no data read.

**Phase B — materialize:** call the `RowResurrector` for each request. Resurrection files
are grouped by partition and ordered deterministically (source path, then position) so runs
are reproducible and diffable.

**Phase C — write deletes:** one position-delete file per rewritten snapshot per partition,
sorted by `(path, pos)` via `SortingPositionOnlyDeleteWriter`. (Later: a DV per data file
per snapshot.)

**Phase D — write manifests:** per rewritten snapshot, a data manifest and a delete
manifest, then a manifest list. Everything stamped at `σ_k` per §4.2.

**Phase E — synthesize metadata (no commit):** build `TableMetadata` preserving for every
rewritten snapshot its `snapshot-id`, `parent-snapshot-id`, `sequence-number`,
`timestamp-ms`, `schema-id`, and `operation`; replacing `manifest-list`; recomputing
`total-*` summary fields; adding `snapshot-rewritten-from` = the original manifest-list
location. Return it as `SnapshotRewriteResult`. **Stop here by default.**

**Phase F — commit + reclaim (opt-in, phase 4):** `TableOperations.commit(base, rewritten)`,
then delete the detached set minus anything still reachable from a retained metadata-log
entry.

---

## 7. Configuration

```
snapshot-rewrite.enabled                 = false
snapshot-rewrite.min-age-ms              = 86400000   # P7: don't rewrite recent snapshots
snapshot-rewrite.max-dead-ratio          = 0.5        # P8 cost guard
snapshot-rewrite.commit                  = false      # §4.4 — non-destructive by default
snapshot-rewrite.reclaim                 = false      # phase F is separately opt-in
```

---

## 8. Regression tests

Local, no Spark. Implementation tests in `core/src/test/java/org/apache/iceberg/`;
end-to-end row-level tests in `data/src/test/java/org/apache/iceberg/data/snaprewrite/`
(the `data` module already depends on `iceberg-core` `testArtifacts`, so `TestTables` is
available alongside `IcebergGenerics`, `FileHelpers`, and `GenericAppenderHelper`).

Because the rewrite is non-destructive, **no test mutates its source table** — every case
compares the original against the shadow table (§5.1). A bug costs scratch files, not a
corrupted fixture, and a failing test leaves both representations intact for inspection.

### 8.1 Harness

- `LocalCompactor` — test utility performing a real bin-pack compaction: reads each source
  file's live rows in position order, appends survivors to a target writer, records runs,
  and commits through `RewriteDataFilesCommitManager` with `FilePositionMapping`s so a
  **real** compaction map is produced and attached. Not a hand-built map.
- `SnapshotRewriteTestBase` — builds a table, runs a scripted or generated workload,
  compacts, rewrites into a shadow table, and diffs.

### 8.2 The oracle

For **every** snapshot id in the table, not just the window:

```java
for (long id : allSnapshotIds(original)) {
  assertThat(multiset(IcebergGenerics.read(shadow).useSnapshot(id).build()))
      .isEqualTo(multiset(IcebergGenerics.read(original).useSnapshot(id).build()));
}
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
| 13 | Two compactions inside the window (chained maps) | Sequential map application (§11.2) |
| 17 | Compaction rolling mid-source-file (multi-target runs) | Per-run `targetFile`; borrowed from the existing compaction-map suite, which pins this at the map level -- here it runs end to end |
| 18 | Rolling on a partitioned table | Each partition rolls independently |
| 19 | A map-less replace inside the window | Falls back to copying rows through it rather than refusing (§11.6) |
| 14 | Empty / no-op commit in the window | Degenerate step |
| 15 | Interleaved appends from two writers | Ordering independence |
| 16 | Duplicate rows across snapshots | Multiset oracle |

### 8.4 Structural assertions (`TestSnapshotRewriteStructure`)

- **Sequence-number stamping** (`TestSnapshotRewriteSequenceNumbers`) — the §4.2 mechanism
  gets its own test rather than relying on the oracle to catch it indirectly:
  - every delete file in a rewritten snapshot is returned by
    `DeleteFileIndex.forDataFile` for every data file it references — asserted directly,
    not inferred from row counts;
  - a deliberately mis-stamped variant (deletes at `σ_k`, data left at `σ_B`) is
    constructed in the test and asserted to **fail** the oracle, proving the oracle can
    see the silent-drop failure mode and is not vacuously green.
- **No remaining dependency:** for every rewritten snapshot, the set of referenced files
  intersected with (`C_A`'s outputs ∪ interstitial adds) is empty.
- **Storage:** measured `net` matches the §2.3 prediction `|C_B| − |deletes|` within
  tolerance; the report's own predicted-vs-measured line is asserted consistent.
- **Reclaim safety:** every file in the reclaim list is unreachable from every snapshot in
  the shadow metadata and from every retained metadata-log entry.
- **Round-trip:** `TableMetadataParser.toJson` → `fromJson` → re-run the oracle, proving the
  synthesized metadata survives a real reload (§4.1: the parser does not re-validate, so
  this is a genuine check).
- **Source untouched:** the original table's metadata file and every reachable file are
  byte-identical after the rewrite.
- **Expire interop** (phase 4, once `commit()` exists): `ExpireSnapshots` on the oldest
  rewritten snapshot deletes nothing still needed; re-run the oracle afterward.

### 8.5 Refusal tests (`TestSnapshotRewriteSkips`)

One test per precondition P1–P8: construct the violating history, assert the rewrite
refuses, assert nothing was written outside the scratch location, and assert the reported
reason names the right precondition.

### 8.6 Report tests (`TestSnapshotRewriteReport`)

- `--dry-run` estimate is within tolerance of the materialized measurement on the same
  workload, across the §8.3 matrix.
- Degenerate windows report honestly: an insert-only window reports `resurrected = 0`; a
  window that deletes nearly everything reports a `net` near zero and trips P8.

### 8.7 Fuzz (`TestSnapshotRewriteFuzz`)

Seeded random workloads over the op mix in §8.3, modeled on
`benchmark/compaction-baseline`'s `WorkloadGenerator` but local and v2. Per seed: generate,
compact, rewrite into a shadow table, run the full oracle over all snapshots, and check the
report's predicted-vs-measured accounting. Log the anchor seed so failures reproduce,
following the convention already established in the compaction-baseline fuzzer.

---

## 9. Milestones

| Phase | Content | Status |
|-------|---------|--------|
| 1 | v2, unpartitioned, single window. Planner + shadow materialization + `GenericRowResurrector` + report. Non-destructive throughout | **done** -- oracle green, figure example reproduces, mis-stamp test fails as designed |
| 2 | Partitioned tables; partial compaction; chained maps in-window; recursive windows | **done** -- `TestSnapshotRewriteLayouts` |
| 3 | Fuzz at volume; `--dry-run` accounting | **done** -- 20 seeds, all rewrite (none trivially refuse), windows of 4-8 snapshots |
| 4 | `commit()` + reclaim; `ExpireSnapshots` interop | **done** -- `TestSnapshotRewriteCommit` |
| 5 | v3 / DVs: materialized `_row_id`, `first-row-id` reconstruction, P1 lifted | not started |
| 6 | Spark `RowResurrector` for scale; cost model on real tables | not started |

---

## 10. Open questions

1. ~~**Should `total-records` be the invariant?**~~ **Resolved, and the premise was wrong.**
   `total-records` counts records in live data files, so it is a property of the *layout*, not the
   state: a rewritten snapshot holds the whole compaction and masks most of it with deletes, and its
   `total-records` legitimately rises. The invariant is the set of rows a scan returns, which only a
   scan can check -- which is why §8.2's oracle reads every snapshot rather than comparing summaries.
   All `total-*` fields are recomputed for the new layout; per-commit `added-*`/`deleted-*` are
   dropped rather than fabricated.
2. **Metadata-log trimming.** Reclaim (phase 4) is blocked by retained metadata JSONs. Trim
   the log as part of the rewrite, or accept delayed reclaim? Prototype accepts delay and
   reports what it withheld.
3. **REST catalog.** A production design needs a `replace-snapshot` metadata update. Worth
   raising in the post as a concrete spec ask.
4. **Recursion depth.** Rewriting back through many compactions accumulates resurrection files,
   and §11.1 keeps the cost linear in dead rows rather than in table copies. The remaining limit is
   the quadratic delete term in §2.3: a long window costs more than proportionally, so deep recursion
   should be done as a series of short windows rather than one long one. Worth measuring on a real
   history.
5. **Is per-snapshot stamping the right choice over a single `baseSeq`?** Both satisfy
   `data.seq <= delete.seq`. Per-snapshot is simpler to state and makes each rewritten
   snapshot internally uniform; a shared `baseSeq` would give each physical file only two
   distinct sequence numbers table-wide instead of one per rewritten snapshot. Neither is
   spec-legal; the choice is about which is easier to audit.

6. **Should there be a minimum-saving guard?** P8 bounds the dead ratio, which is about the cost of
   resurrection, but the metadata and delete terms decide whether a small window is worth anything at
   all. The report answers this per window; whether the planner should also refuse on it is a policy
   question, not a correctness one, so nothing was added.

---

## 11. What the implementation changed

Five things the design got wrong or left out, all found by tests rather than by reading.

### 11.1 Inverting a compaction must use its map, not resurrection

The induction diffs adjacent states by file and position. Applied to a compaction *inside* the
window, that sees every old reference as deleted and every new one as inserted, and copies the entire
live table forward at that boundary -- turning a window that spans three compactions into three full
copies of the table. A compaction is a logical no-op whose map already says where the rows went, so
the step is skipped entirely; the maps of the compactions in a window are what the locator consults.
Guarded by P10, since a REPLACE that also changed data would break the assumption silently.

Without this, deep recursion (§10.4) would have been useless rather than merely expensive.

### 11.2 Chained maps cannot be composed, only applied in sequence

`CompactionMaps.compose` requires the first map's target snapshot to equal the second's source, which
holds only for back-to-back compactions. A rewrite window normally has transactions between its
compactions, so composition rejects exactly the case the rewrite needs. `RowLocator` applies the maps
in order instead: each either relocates a reference or passes it through, and a null result means the
row was already dead when that compaction ran. No snapshot-id agreement required.

### 11.3 A snapshot's compaction map has to be identified semantically

Reading `compactionMapLocation` off any manifest a snapshot holds finds the wrong map. Manifest
rewriting carries the location forward onto copies (`ManifestFilterManager.java:489`), and the copy
is stamped with the rewriting snapshot's id -- so a compaction that wrote no map appears to own one
describing a layout change it had nothing to do with, and checking the snapshot id does not help.
`CompactionMapLookup` tests what the map says instead: a snapshot's own map targets the data files
that snapshot added.

### 11.4 Reachability means all referenced files, not added files

`detachedFiles` first used `Snapshot.addedDataFiles`, which misses files a snapshot references but
did not write -- exactly what a partial compaction leaves behind. A file still held by a later
snapshot looked unreachable. Harmless in the report; on the reclaim path it means deleting live data.
`SnapshotFiles` now enumerates manifests. Reclaim also evaluates reachability *at reclaim time*
rather than as of the rewrite, since how much of the metadata log is retained changes with every
commit.

### 11.5 A full scan cannot see a pruning bug

The oracle originally compared full scans only. But a rewritten snapshot holds a whole compaction
plus resurrection files, with file statistics nothing like the originals, and pruning decisions are
made from those statistics. A rewrite that produced bounds excluding rows the file actually contains
would pass every full-scan comparison and fail the first predicate a user wrote. The oracle now
compares filtered scans as well, with bounds drawn from the data present.

### 11.6 A compaction is identified by its map, not by its operation

`DataOperations.REPLACE` covers rewrites that are not compactions, and what actually matters is
whether rows can be followed through the snapshot -- which is exactly what having a map means. The
window floor, the induction's skip, and map collection are all keyed on map presence now.

Changing this removed a refusal rather than adding one. A map-less replace inside the window used to
abort the whole rewrite; it is now diffed like any other transaction, which is correct: every row it
moved is recovered into a resurrection file and every row it wrote becomes a delete, reconstructing
the preceding state from copies instead of by following a map. That costs a full copy of the live
table at that boundary, which is a question for the dead-ratio guard, not a reason to refuse.

**Redundancy not yet exploited.** A compaction is state-identical to its parent (P10 asserts it), so
their rewritten forms have identical file sets and identical deletes -- and the rewrite still writes
two full manifest sets, roughly 12 KiB of Avro per duplicated snapshot. They cannot simply share
manifests because §4.2 stamps each snapshot's contents at its own sequence number, so the two differ
in exactly that field. Sharing would need either a relaxed stamping rule for this case or two
snapshots pointing at one manifest list. Worth doing on a long history; not done here.

### 11.7 The oracle had to project explicitly

`IcebergGenerics` reads with the schema the delete filter requires and never strips the extra `_pos`
column, so a snapshot carrying deletes yields wider records than one without. A rewrite turns
delete-free snapshots into delete-bearing ones, so comparing raw records measured read plumbing
instead of table contents. The oracle compares the schema's own columns.
