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
| Manifests, manifest lists, sequence-number stamping | `core/.../snaprewrite/SnapshotRewriteWriter.java` |
| Shadow table, `commit()`, `reclaim()`, `discard()` | `core/.../snaprewrite/SnapshotRewriteResult.java` |
| Undo | `core/.../snaprewrite/SnapshotRewriteRestore.java` |
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
