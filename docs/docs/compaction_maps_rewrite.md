---
title: "Rewriting Snapshots with Compaction Maps"
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Rewriting Snapshots with Compaction Maps

Compaction maps let a *concurrent* transaction rebase onto a new layout. The same translation runs
backwards: a snapshot that already committed can be re-expressed against a later compaction, instead
of the layout it was prepared against.

The states a table can address do not change. What changes is which files those states pin, and
therefore what can be reclaimed. Once every snapshot in a window references the newer compaction, the
older compaction's outputs and every interstitial data and delete file become unreachable.

**Prerequisite:** compaction maps, on the `cmpmap` branch. Nothing here works without a compaction
that records what it moved.

## What it costs, and what it saves

Each row that dies inside a window is materialized exactly once, by the inverse of the transaction
that killed it. So the bytes needed to retain the history are the compaction plus the rows that died,
where before they were the previous compaction plus everything inserted since. Those two differ by one
copy of the table:

```
before:  |C_A| + |window inserts| + |C_B|
after:   |C_B| + |dead rows| + |delete vectors|
and      |C_A| + |window inserts| = |C_B| + |dead rows|
=>       saving ~ |C_B| - |delete vectors|
```

Measured on 60k base rows, five transactions inserting 4k each, clustered deletes, 20-column rows
(`TestSnapshotRewriteScale`):

```
reclaimable  18.39 MiB      added  0.58 MiB      saved  17.81 MiB
predicted    18.02 MiB   ->  the saving is 98.8% of one copy of the table
854 rows died, 854 recovered
```

Two places it does not pay:

- **Narrow rows.** Every rewritten snapshot deletes every row inserted after it, so a window of `m`
  transactions each inserting `r` rows carries roughly `r · m² / 2` delete positions. A position costs
  about a byte whatever the row's width, so the term was 0.7% of the saving above and **57%** on a
  2-column table. On narrow rows, keep windows short.
- **Small tables.** Each rewritten snapshot needs a manifest and a manifest list, several KiB of Avro
  apiece. Below roughly that scale a rewrite costs more than it frees, and the report says so with a
  negative rather than hiding it.

Because the answer depends on the table, nothing is assumed. Price it first.

## Dry run

The dry run writes nothing at all. It runs the full induction — so its row counts are exact, not
estimated — and reports what each available reach would cost. File sizes are extrapolated from the
compaction's bytes per row, so byte figures are indicative.

```bash
./gradlew :iceberg-data:jar :iceberg-core:jar

VERSION=1.11.0-SNAPSHOT
java -cp "data/build/libs/iceberg-data-$VERSION.jar:core/build/libs/iceberg-core-$VERSION.jar:$(hadoop classpath)" \
    org.apache.iceberg.data.SnapshotRewriteDryRun \
    --dry-run --table /warehouse/db/orders
```

Name the jars explicitly: a `iceberg-data-*.jar` wildcard also matches the `-tests` jar. The tool
needs Iceberg's runtime dependencies and a Hadoop classpath on top of these two — `hadoop classpath`
is the usual way to get the latter, and any deployment that already reads Hadoop-catalog tables has
it.

`--dry-run` is required. The tool has no mode that modifies a table; the flag is explicit so that
intent is on the command line and a future committing mode has to be asked for.
`TestSnapshotRewriteDryRun` asserts both that the documented invocation works and that it leaves the
table byte-identical.

| Flag | Meaning |
|---|---|
| `--dry-run` | Required. Report only. |
| `--table <location>` | Hadoop table location. |
| `--reach <snapshotId>` | Price only the reach whose floor is this snapshot. Omit for all. |

Output is one block per available reach, cheapest first:

```
table /warehouse/db/orders

reach to 4477239261058490891: 7 snapshots, movable
window        7 snapshots  (estimated -- nothing materialized)
  reclaimable     -412.6 MiB
  resurrected      +31.4 MiB   (2 files, 41802 rows)
  deletes          +0.9 MiB    (7 files, 118447 positions)
  metadata         +0.1 MiB
  net             -380.3 MiB
  predicted       -412.6 MiB   (one copy of the compacted table; residual +32.3 MiB)
  rows         live 512338   resurrected 41802   dead-ratio 0.076

reach to 1180339692382900123: 19 snapshots, movable
  ...
```

`movable` means snapshots in that window still reference files the compaction replaced.
`already current` means the reach is done and rewriting it again would change nothing.

The report goes through SLF4J at INFO. If logging is misconfigured a run prints nothing, which looks
identical to a table with no candidates.

## From Java

```java
SnapshotRewriteIO io = new GenericSnapshotRewriteIO(table);

// Price every reach. Writes nothing.
for (SnapshotRewriteSurvey.Candidate candidate : SnapshotRewriteSurvey.survey(table, io)) {
  System.out.println(candidate);
}

// Build one for real, still without touching the table.
SnapshotRewriteResult result =
    SnapshotRewrite.forTable(table, io)
        .onLatestCompaction()
        .floor(chosenFloorSnapshotId)
        .minAgeMs(TimeUnit.DAYS.toMillis(1))
        .maxDeadRatio(0.5)
        .materialize();

System.out.print(result.report());

// Verify it before deciding: reads go through ordinary Iceberg scan planning.
Table shadow = result.asTable();

result.commit(((HasTableOperations) table).operations());   // the one destructive step
```

`materialize()` writes data files, delete files, manifests, and a complete `TableMetadata`, and
commits none of it. A rewrite that turns out wrong costs scratch files.

### Undoing one

Every rewritten snapshot records the manifest list it came from and the summary it carried, so a
committed rewrite can be put back by pointing at them — byte-identical, no reads, no copies.

```java
TableMetadata current = ((HasTableOperations) table).operations().current();
if (SnapshotRewriteRestore.rewrittenCount(current) > 0) {
  ((HasTableOperations) table)
      .operations()
      .commit(current, SnapshotRewriteRestore.restore(current));
}
```

That window closes at reclaim, which deletes the layout a restore would point back at. Reclaim is the
point of no return.

### Reclaiming

Expiring snapshots does not clean up after a rewrite: it deletes only what the *expired* snapshots
reach, and after a rewrite the old files are reached by no snapshot at all. Reclaim is separate and
explicit, and it withholds anything still reachable — including from a retained metadata-log entry, so
immediately after a commit it will typically withhold everything and delete nothing.

```java
SnapshotRewriteResult.ReclaimResult reclaimed =
    result.reclaim(((HasTableOperations) table).operations());
```

## Running it as a recurring pass

The intended deployment is a pass that runs *after* a compaction and switches the snapshots it
superseded onto the newer layout. Keeping the two apart is what lets the age threshold hold: a
snapshot that could still be an in-flight transaction's base, or that replication and audit have not
consumed, must be left alone for a while, and a pass fused into the compaction would rewrite
immediately.

Two properties such a pass depends on:

- **It runs over its own output.** A snapshot rewritten onto one compaction still pins that
  compaction's files, so a newer compaction means moving it again. Nothing makes it special to the
  planner.
- **Reaches extend, they do not chain.** Rewriting `[C_A, C_B)` and later `[C_B, C_C)` as separate
  windows leaves the first pass's snapshots pinning `C_B`, because the second window starts there.
  Releasing an older layout takes one window with an older floor.

So the condition to skip on is *"already expressed against the newest compaction"*, which is what
`Candidate.alreadyCurrent()` reports — not *"has been rewritten"*, which a snapshot needing another
move also satisfies.

## What is refused

A window that cannot be rewritten losslessly is refused whole, with the reason. Partial success is
not useful: a rewrite lossless for most snapshots and lossy for one still silently changes what a
time-travel read returns.

| Refusal | Condition |
|---|---|
| `FORMAT_VERSION` | Not format 2 or 3 |
| `ROW_LINEAGE` | v3, the window would recover rows, and the `SnapshotRewriteIO` does not preserve row ids |
| `EQUALITY_DELETES` | Equality deletes anywhere in the window |
| `SCHEMA_CHANGED` | `schema-id` differs across the window |
| `SPEC_CHANGED` | More than one partition spec across the window |
| `MISSING_COMPACTION_MAP` | The target compaction carries no map |
| `MISSING_SOURCE_FILE` | A file needed to recover a row is gone |
| `TOO_RECENT` | The compaction is younger than `minAgeMs` |
| `DEAD_RATIO` | Rows to recover exceed `maxDeadRatio` of the rows that survived |
| `REPLACE_CHANGED_DATA` | A replace operation in the window altered the table's contents |
| `UNLOCATABLE_ROW` | A row live in a rewritten snapshot could not be placed (a bug, not an input) |
| `NO_COMPACTION` | No snapshot in the table carries a compaction map |

A *map-less* replace **inside** the window is not refused. It is diffed like any other transaction,
which is correct but costs a full copy of the live table at that boundary — a matter for
`maxDeadRatio`, not grounds to stop.

## Implementation

### The induction

Processing a window backwards from the compaction, each step inverts one transaction. The state
carried between steps is a locator: for every row live in snapshot `S_k`, where that row now lives.

- Rows the transaction **inserted** are live at the following state, so the locator already places
  them. They become position deletes. No data is read.
- Rows the transaction **deleted** are live at the preceding state and present in neither the
  compaction nor any other recovery file — each row is killed by exactly one transaction, so it is
  recovered by exactly one inverse. They are copied out of the original layout.

The interesting case is a row inserted by `T_j` and deleted by `T_k` for `k > j`: it is absent from the
compaction, so inverting `T_j` has to find it in the file that inverting `T_k` wrote. That
cross-reference does not follow from the compaction map alone, and it is why the locator has two
layers.

| Concern | Code |
|---|---|
| Entry point, fluent config, `estimate()`, `materialize()` | `core/.../snaprewrite/SnapshotRewrite.java` |
| The induction, preconditions, window resolution | `core/.../snaprewrite/SnapshotRewritePlanner.java` |
| The locator (`loc_k`): compaction maps plus a recovery overlay | `core/.../snaprewrite/RowLocator.java` |
| Live positions per data file at a snapshot | `core/.../snaprewrite/PositionSet.java` |
| Which map a snapshot actually wrote | `core/.../snaprewrite/CompactionMapLookup.java` |
| Manifests, manifest lists, sequence-number stamping, dropping statistics | `core/.../snaprewrite/SnapshotRewriteWriter.java` |
| Shadow table, `commit()`, `reclaim()`, `discard()` | `core/.../snaprewrite/SnapshotRewriteResult.java` |
| Undo, and re-attaching detached statistics | `core/.../snaprewrite/SnapshotRewriteRestore.java` |
| Pricing every reach | `core/.../snaprewrite/SnapshotRewriteSurvey.java` |
| Savings accounting | `core/.../snaprewrite/SnapshotRewriteReport.java` |
| Package-private escape hatches, one per assertion | `core/.../SnapshotRewriteUnsafe.java` |
| Reading deletes, recovering rows, writing deletes | `data/.../data/GenericSnapshotRewriteIO.java` |
| Materialized `_row_id` | `data/.../data/GenericRowLineage.java` |
| Dry-run CLI | `data/.../data/SnapshotRewriteDryRun.java` |

### Sequence numbers

Iceberg applies a positional delete to a data file only when
`data.dataSequenceNumber <= delete.dataSequenceNumber`. That is not bookkeeping: it is how a
concurrent append and a concurrent delete commute. The sequence number is the delete's as-of
watermark, and a data file added after it holds rows the delete's author never saw.

A rewrite inverts that deliberately — its deletes reference files written long after the snapshot they
land in — so **every file in a rewritten snapshot is stamped at that snapshot's own sequence number**.
The comparison becomes an equality, which passes, and each rewritten snapshot reads as though every
file it holds had been added by it.

Getting this wrong fails differently by version, and the v2 mode is the dangerous one:

- **v2**: `DeleteFileIndex.PositionDeletes.filter` slices a sequence-sorted array. Mis-stamped deletes
  are simply not in the slice. No exception, no log line, deleted rows reappear.
- **v3**: `DeleteFileIndex.findDV` raises `ValidationException`.

`TestSnapshotRewriteStructure#misStampedRewriteIsCaughtByTheOracle` builds the mistake on purpose and
asserts the oracle catches it, because a suite that only ever runs the correct stamping cannot
distinguish "the stamping is right" from "nothing is checking".

### Invariants suspended

A rewrite is unsafe in the Rust sense: it asserts soundness rather than satisfying what the framework
can prove. Three different things get called "the invariant" in that sentence, and they carry very
different risk, so they are listed separately.

#### Satisfied by construction

The check still executes on every read of a rewritten snapshot, and still passes. These are the cheap
ones: if the construction is wrong, Iceberg says so.

| Invariant | Guard | Why it passes |
|---|---|---|
| A delete applies to a data file only when `data.dataSequenceNumber <= delete.dataSequenceNumber` | `DeleteFileIndex.findDV` (v3 DVs, raises `ValidationException`); `DeleteFileIndex.PositionDeletes.filter` via `findStartIndex` (v2 position deletes, silent) | uniform per-snapshot stamping makes the comparison an equality |
| A null entry sequence number is permitted only for status `ADDED`, and only from the committing snapshot | `V3Metadata.IndexedManifestEntry.get` case 2, and the same in `V2Metadata` | the rewrite never emits a null: every entry is `EXISTING` with an explicit sequence number |
| An unassigned *manifest* sequence number is permitted only for a manifest created by the committing snapshot | `V3Metadata.ManifestFileWrapper.get` cases 4 and 5 | the manifest's snapshot id and the manifest list's agree, both being the rewritten snapshot's |
| A manifest's `first_row_id` is assigned exactly once, and a DATA manifest must end up with one | `V3Metadata.ManifestFileWrapper.get` case 15; assignment in `ManifestListWriter.V3Writer.prepare` | fresh manifests carry null and are assigned from a counter seeded with the snapshot's own `firstRowId` |

Writing every entry as `EXISTING` is not evasion — it is the only available encoding. Iceberg offers
exactly two: `ADDED` with an inherited sequence number, or `EXISTING` with an explicit one. A rewrite
needs an explicit sequence number of its own choosing, so `EXISTING` is forced.

#### Stated in prose, enforced nowhere

These are the ones to watch, because no failure is possible: nothing will ever complain.

`ManifestWriter.existing` documents its contract as *"The original data and file sequence numbers,
snapshot ID, which were assigned at commit, must be preserved when adding an existing entry."* Nothing
checks it. The rewrite violates all three fields deliberately: `SnapshotRewriteWriter` passes the
rewritten snapshot's own id as each file's owning snapshot, and that snapshot's sequence number as both
the data and the file sequence number. A rewritten manifest therefore asserts that every file it holds
was added by the snapshot holding it. See errata 2 and 3.

Manifest-level `first_row_id` ranges also overlap across rewritten snapshots: the assignment counter
advances over the existing-row count of the whole compaction, from a seed of the snapshot's own
`firstRowId`. Nothing checks this either. It is benign only because file-level `first_row_id` is what
actually carries identity, which is why recovery files are given one explicitly rather than left to
inherit it from the manifest.

#### Bypassed, so the check never runs

Here the framework would refuse outright, so the code takes a different path and the refusal set stands
in for the guarantee.

| Invariant | Guard skipped | What stands in |
|---|---|---|
| A snapshot id is unique within a table | `TableMetadata.Builder.addSnapshot` | replacement is definitionally a duplicate; `SnapshotRewriteUnsafe.replaceSnapshots` uses the package-private constructor, the same path `TableMetadataParser.fromJson` takes |
| A snapshot's sequence number exceeds `lastSequenceNumber` | same | the original sequence number is reused on purpose |
| v3: `firstRowId >= nextRowId`, and `nextRowId += addedRows` | same | a replacement must *not* advance `nextRowId`; the builder cannot express that |
| Every metadata change is expressible as a `MetadataUpdate` | `addSnapshot` records `MetadataUpdate.AddSnapshot`; a rewrite records nothing | nothing — this is why the operation cannot travel over REST (errata 1) |
| Commit-time conflict validation | all of `SnapshotProducer.apply()`, and every `MergingSnapshotProducer` validation | `commit()` is `ops.commit(base, rewritten)`, a whole-metadata swap; the planner's preconditions run before anything is written |

The last row is the largest suspension. Because the commit is a metadata swap rather than a produced
snapshot, the REPLACE sanity check `addedRecords <= replacedRecords`, the row-lineage `assignedRows`
derivation, and the retry/refresh loop are all absent. Concurrency control reduces to whatever
compare-and-set on base metadata the `TableOperations` implementation performs: a writer that moved
`current` makes the commit fail outright, with no retry and no revalidation, because the plan was
computed against one specific base.

What replaces all of it is a refusal set rather than a proof — the twelve reasons in `RewriteRefusal`,
checked before any file is written (`SnapshotRewritePlanner.checkStaticPreconditions`). Two carry most
of the weight: `UNLOCATABLE_ROW`, which requires every row live in the window to be findable, and
`REPLACE_CHANGED_DATA`, which guards the assumption that an interstitial compaction is a logical no-op.

One boundary is load-bearing for all of the above: **the compaction itself is never rewritten**
(`SnapshotRewritePlan.window()` excludes it), so new commits still branch from a snapshot outside the
window, at a sequence number above `lastSequenceNumber`. The inverted sequence-number semantics stay
confined to history that will never be appended to. If that stopped being true, uniform stamping would
be actively wrong rather than merely unusual.

### Row lineage

Under v3 a row's id is normally derived from its file's `first_row_id` plus its offset, which works
only while a file's rows are one contiguous range. Any operation gathering rows from several files
produces a file whose rows came from unrelated ranges, so ids have to be written per row —
`GenericRowLineage`. This is not specific to rewriting: without it no generic-writer *compaction* can
preserve lineage either.

Two read-path mechanics worth knowing:

- A materialized `_row_id` is honoured only when the file **also** carries a `first_row_id`.
  `ParquetValueReaders.rowIds` returns nulls without one, so the column is written and never read. Any
  value works; nothing derives from it.
- Reading `_row_id` back needs that same value supplied as a constant
  (`GenericParquetReaders.buildReader(schema, fileSchema, idToConstant)`). A plain projection discards
  the column silently.

What the rewrite guarantees is that a rewritten snapshot reports the identities *the compaction*
reports; whether those match a row's original ids is the compaction's obligation.

### Statistics

Statistics are keyed by snapshot id, and a rewrite preserves snapshot ids, so every statistics file
stays attached to the snapshot it names unless something detaches it. Nothing in Iceberg validates a
statistics file against the snapshot it describes -- statistics are advisory, and a reader may ignore
them -- so a stale entry is not rejected. It is believed.

The two kinds part company, and the split is exactly the row/layout split that runs through the rest
of this design:

| Kind | Derived from | A rewrite |
|---|---|---|
| Table-level (`statisticsFiles`) | the rows live at the snapshot -- the standard blob type is a theta sketch, i.e. distinct-value counts | **keeps them.** A rewrite preserves the live row set exactly, which is the property a sketch summarises |
| Partition (`partitionStatisticsFiles`) | the layout: per-partition file counts, byte totals, delete counts, `dvCount` | **drops them.** A rewritten snapshot holds the whole compaction with most of it masked, so every one of those numbers is wrong |

Dropping has to be reversible, and partition statistics live in table metadata rather than in a
snapshot, so there is nowhere in the manifest tree to point back at. Each dropped entry is instead
recorded in the summary of the snapshot that replaced it, under keys deliberately outside the
`snapshot-rewritten-from.` prefix -- everything under that prefix is replayed verbatim into the
restored summary, and these keys describe the rewrite rather than the snapshot it replaced. A
`PartitionStatisticsFile` is three fields and one of them is the snapshot's own id, so a path and a
size are enough to rebuild it exactly. Undo stays self-contained: everything needed to reverse a
rewrite is in the snapshots the rewrite wrote.

Reclaim deletes the detached statistics file, because it describes the old layout as surely as its
manifests do. Reachability for it needs one extra step: statistics hang off table metadata rather
than off a snapshot, so walking snapshots does not find them, and a retained metadata-log entry that
still names a detached file would otherwise not count as a reference to it.

## Errata

Expedient choices and known limitations, not bugs.

### 1. Snapshot replacement is not an Iceberg operation

`TableMetadata.Builder.addSnapshot` rejects a duplicate snapshot id and requires a strictly increasing
sequence number, so an in-place rewrite cannot go through the builder. It goes through the
package-private `TableMetadata` constructor instead — the same path `TableMetadataParser.fromJson`
uses, which is why a JSON round trip is a real check and is tested as one.

Every such call is confined to `SnapshotRewriteUnsafe`, one method per escape hatch, each documenting
the invariant the caller must uphold. The rewrite is "unsafe" in the Rust sense: it asserts soundness
rather than satisfying what the framework can prove.

**This is not expressible in the REST catalog protocol**, which has no metadata update for replacing a
snapshot. A production design needs one. The non-destructive path has no such constraint and works
against any catalog.

### 2. One physical file carries different sequence numbers in different snapshots

A consequence of the stamping rule above. Correct for single-snapshot reads — readers only compare
within one snapshot's file set — and meaningless for incremental scans across the window, which the
design already breaks.

### 3. Provenance is destroyed, deliberately

- Manifest entries are all written `EXISTING` under the rewritten snapshot, so `snapshot_id` and
  `status` no longer say which commit added a file.
- Incremental scans (`appendsBetween`, CDC) over the rewritten window return garbage. The snapshots
  are correctly *sequenced*, but the changesets are wrong: the snapshot written by `T_α` was not
  produced by the delta the new layout describes.
- Per-commit `added-*` and `deleted-*` summary fields are dropped rather than fabricated.
- `_file` and `_pos` change for time-travel reads.

`minAgeMs` is the mitigation: do not rewrite snapshots that downstream consumers have not seen yet.

### 4. `total-records` is not preserved, and should not be

It counts records in live data files, so it is a property of the layout rather than of the state: a
rewritten snapshot holds the whole compaction and masks most of it, so its `total-records` rises. All
`total-*` fields are recomputed for the new layout. The invariant is the set of rows a scan returns,
which only a scan can check — which is why the tests compare every snapshot's contents rather than its
summary.

Partition statistics are the same kind of number and get the same treatment, except that they are
dropped rather than recomputed — see [Statistics](#statistics) and errata 11.

### 5. Reclaim is delayed by the metadata log

Entries in `previousFiles` point at metadata documents whose snapshots describe the old layout, so
anything reachable from a retained entry is withheld. Immediately after committing a rewrite that is
usually everything. The alternative — trimming the metadata log as part of the rewrite — was not taken.

### 6. The induction is row-at-a-time

Every inserted row is located individually through the compaction map, so planning cost is linear in
rows rather than in runs, which is the opposite of what a run-encoded map is for.
`PositionDeleteRemapper.remapPositionsBulk` and the strategy selector already exist;
switching the planner to them is unfinished work and a bigger lever on scale than distribution.

### 7. v3 recovery depends on the `SnapshotRewriteIO`

`preservesRowLineage()` defaults to false, so an implementation has to claim the capability. One that
cannot materialize ids stays usable on v2 and is refused on v3 rather than silently renumbering rows.

### 8. Out of scope

- Sort and z-order rewrites, for the reasons compaction maps exclude them generally.
- Equality deletes.
- Schema and partition-spec evolution inside a window.
- Format v4.
- Distributed execution. The `SnapshotRewriteIO` interface admits it — three methods — but only the
  generic implementation exists.

### 9. No evidence from a real history

Every figure in this document comes from generated tables, the largest 80k rows in one file. Nothing
here exercises object-store latency, a window hundreds of commits deep, or a production layout. The
dry run exists so that the measurement can be taken on a real table; it has not been.

### 10. Undo points back; it does not reconstruct

`SnapshotRewriteRestore` reverses a rewrite by pointing each snapshot at the manifest list it used to
carry. That is exact and free, and it is also entirely dependent on the old layout still existing:
after `reclaim()` there is nothing to point at, which
`TestSnapshotRewriteRoundTrip#reclaimEndsReversibility` asserts.

The stronger property -- reconstructing the pre-compaction layout *from the rewritten layout alone* --
is not implemented, and no test exercises it. It appears feasible, because a compaction map is
invertible: for every row in the compaction it names the pre-compaction file and offset the row came
from, which recovers each original file's surviving rows and their order, and so its boundaries. Two
pieces are missing:

- Rows that died inside the window are absent from the compaction and live in recovery files. Their
  original file and offset is known at rewrite time (`ResurrectionRequest.sources()`, in output order)
  and is never persisted.
- Under v3, a row id that was derived rather than materialized needs its original file's
  `first_row_id` to reproduce. That is also computed at rewrite time
  (`ResurrectionRequest.sourceFirstRowIds()`) and also not persisted.

Persisting both -- a reverse map written alongside the rewrite -- would make the inverse computable
without the old files. Even then it could only be lossless up to file identity: reconstruction writes
new files at new paths, so the result would match the original in rows, per-file grouping, row order,
and row ids, but not in bytes or in manifest-list location.

### 11. Partition statistics are dropped, not recomputed

A rewritten snapshot ends up with no partition statistics at all. Recomputing them is possible --
`PartitionStatsHandler.computeAndWriteStatsFile(table, snapshotId)` does exactly that -- but it costs
a scan per rewritten snapshot, and it lives in `iceberg-data`, which `core` cannot call. If it is
ever wanted it belongs behind the `SnapshotRewriteIO` SPI. Until then a reader that relied on
partition statistics for a rewritten snapshot falls back to planning without them, which is slower
but not wrong.

Table-level statistics are kept on the strength of an argument about the *only blob type that exists*
(a theta sketch, which counts distinct values among live rows). That argument is about the blob's
semantics, not about anything the format enforces: a future layout-derived blob type registered as a
table-level statistic would be carried forward and silently believed. Nothing here inspects blob
types.
