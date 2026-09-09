<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Rewriting Iceberg Snapshots with Compaction Maps

This branch re-expresses **snapshots that already committed** against a later compaction, instead of
the layout they were prepared against. The states a table can address do not change; what changes is
which files those states pin, and therefore what can be reclaimed. Once a window of snapshots
references the newer compaction, the older compaction's outputs and every interstitial data and delete
file become unreachable.

A compaction map repairs a *concurrent* transaction's references. The same translation runs backwards,
over history.

> This branch (and its documentation, including this README but excepting this
> comment) is vibe-coded from the [blog
> post](https://cdouglas.github.io/posts/2026/08/rewriting-snapshots) to learn
> what it would break in the format if snapshots were inverted as described.
> Bluntly: the implementation evades safety invariants by corrupting metadata
> in the rewritten table. To be fair, some safety checks protected the
> committed state whose layout we are (unsafely) rewriting, but it highlights
> how the rewrite is liberally interpreting "undefined behavior" as "satisfies
> the default client".
>
> The current prototype doesn't retain enough information to losslessly restore
> the old layout from the inverted layout. It would need to build a reverse map
> for `_row_id` in the inverted layout (retained only for this purpose), among
> other things discarded during the rewrite. It should be feasible, but it is
> not implemented. Table statistics don't change, but more granular stats are
> dropped and not recomputed as part of the rewrite.
>
> The tradeoff is unclear and depends on the workload. Inverting should save
> space and improve read latency by applying deletion vectors to the compacted
> layout, rather than merging chains of delta commits. Particularly if most
> data survive between compactions. However, it would add a lot of complexity
> to areas where the specification is actively building on the invariants this
> explicitly breaks.
>
> Not to be too morose, but this is a qualified win in the best case, insofar
> as the space savings need to be more important than stamping a region of the
> table as "degraded". Instead of corrupting the table to make it look legit,
> explicitly marking the region as "archived" so readers expect only a subset
> of statistics, features, etc. would be more defensible.

## Prerequisite: compaction maps

Nothing here works without a compaction that records what it moved. That is the
[`cmpmap`](https://github.com/cdouglas/iceberg/tree/cmpmap) branch, which this one builds on:

- the compaction map data structure, builder, Avro storage, and manifest-list reference;
- conflict detection and `SERIALIZABLE` integration in `BaseRowDelta` / `MergingSnapshotProducer`;
- automatic remapping in `RewriteDataFilesCommitManager` for Spark 3.5 and 4.0, v2 position delete
  files and v3 deletion vectors;
- the empirically-tuned remapping policy.

Background: Chris Douglas and Joseph M. Hellerstein, **Commutative Compaction**, FORMATS '26,
<https://doi.org/10.1145/3802514.3809174>.

## What it saves

Each row that dies inside a window is materialized exactly once, so the bytes needed to retain the
history become the compaction plus the rows that died, where before they were the previous compaction
plus everything inserted since. Those differ by one copy of the table.

Measured on 60k base rows, five transactions inserting 4k each, clustered deletes, 20-column rows:

```
reclaimable  18.39 MiB      added  0.58 MiB      saved  17.81 MiB
predicted    18.02 MiB   ->  the saving is 98.8% of one copy of the table
```

It does not always pay. Every rewritten snapshot deletes every row inserted after it, and a delete
position costs about a byte whatever the row's width — so on a 2-column table that term was **57%** of
the saving, against 0.7% above. Small tables lose outright, because a manifest and manifest list per
snapshot outweigh the data they describe. **Price it before running it.**

## Dry run

Writes nothing, reports what each available reach would cost:

```bash
./gradlew :iceberg-data:jar :iceberg-core:jar

VERSION=1.11.0-SNAPSHOT
java -cp "data/build/libs/iceberg-data-$VERSION.jar:core/build/libs/iceberg-core-$VERSION.jar:$(hadoop classpath)" \
    org.apache.iceberg.data.SnapshotRewriteDryRun \
    --dry-run --table /warehouse/db/orders
```

`--dry-run` is required; the tool has no mode that modifies a table. See
[compaction_maps_rewrite.md](docs/docs/compaction_maps_rewrite.md#dry-run) for the flags and how to
read the output.

## Status

Non-destructive by default. `materialize()` writes data files, delete files, manifests, and a complete
`TableMetadata`, and commits none of it — the result is exposed as a read-only table so it can be
verified first. `commit()` and `reclaim()` are separate, explicit steps, and a committed rewrite can be
undone byte-identically until reclaim runs.

Implemented: v2 and v3, partitioned and unpartitioned tables, partial and chained compactions, windows
reaching back through older compactions, deletion vectors, materialized `_row_id`, reversibility,
reclaim accounting, and window pricing. 71 test methods including 20 fuzz seeds over both format
versions.

Not implemented: bulk remapping in the planner (it locates rows one at a time), distributed execution,
and any measurement on a real table history. See
[compaction_maps_rewrite.md#errata](docs/docs/compaction_maps_rewrite.md#errata) — in particular that
snapshot replacement is not expressible in the REST catalog protocol, and that the rewrite destroys
change-log provenance on purpose.

## Documentation

- [compaction_maps_rewrite.md](docs/docs/compaction_maps_rewrite.md) — this feature: usage, dry run,
  recurring passes, implementation details, and errata.
- `SNAPREWRITE_SPEC.md` — design specification, soundness argument, and the reasoning behind each
  decision.

Compaction-map documentation lives on the [`cmpmap`](https://github.com/cdouglas/iceberg/tree/cmpmap)
branch, under `docs/docs/compaction_maps*.md`.

## Building

```bash
./gradlew :iceberg-core:compileJava :iceberg-data:compileJava
./gradlew :iceberg-data:test --tests "org.apache.iceberg.data.snaprewrite.*"
./gradlew spotlessApply
```

For general Iceberg build, engine-compatibility, and contribution information, see the upstream
project at <https://iceberg.apache.org>.
