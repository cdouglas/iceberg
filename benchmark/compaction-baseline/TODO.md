# Compaction-baseline follow-up ideas

Not on any current spec milestone, not blocking handoff. Captured here so future work picking up
this branch has a checklist of "if I'm investing in the fuzzer again, here's what I'd reach for
next." Ranked by value-per-effort within each section.

## Fuzz coverage extensions

### 1. No-conflict scenarios (cheap, locks in the negative case)

Every late-tx in the current adversarial config races against compacted files. The
"detector correctly reports zero conflicts, resolver is never invoked, hashes match" path is
never exercised end-to-end in the fuzzer. Add a `targetsPostCompactionFile` flag to op
generation (or a fifth op kind) that picks slice offsets from data files *added after* the
treatment-path compaction. Expected behaviour: `conflicts.conflictingDeleteFiles().isEmpty()`
and `conflicts.multiFilePositionDeletes().isEmpty()`. Worth a fuzz test because a future
change to `CompactionConflictDetector`'s ancestor walk could regress this silently.

### 2. Mixed delete types in one RowDelta (cheap, hits the resolver's grouping logic)

`RowReplacementOp` covers data + PD; nothing exercises position-deletes + equality-deletes in
the same `RowDelta`, or two PD files referencing different source files in one commit. The
conflict detector iterates both kinds, but the resolver's grouping by `referencedDataFile()`
and DV / non-DV split has different code paths per content type — see the bug 5 fix in
`mergePerTargetFile`. A `mixedDelete` op kind that emits both a PD file and an equality-delete
file in one commit would test these jointly.

### 3. Compaction-map chains (medium, exercises a documented-but-unfuzzed path)

The treatment path runs exactly one compaction. `CompactionMapChain` and
`SparkCompactionConflictResolver.resolve(CompactionMapChain, conflicts)` exist specifically
for "compact, late-tx, compact again, late-tx", and they're unit-tested but never fuzzed
end-to-end. Extend `FuzzRunner.buildTreatment` to optionally run a second
`runCompactionAndCaptureMap` after the late-tx, then apply a second late-tx, then resolve
through the chained map. Single new boolean knob (`chainCompactions`) on `FuzzConfig`. The
chain composition is non-trivial enough that real-world bugs are plausible.

### 4. Empty-edge cases (cheap, easy to forget)

`s0Rows=0`, `perSnapshotDeletes=0`, `lateTxCount=0`. Currently the IntRanges exclude these, but
production tables routinely hit them (idle tables, manual compaction triggers without
deletes). Adding `min=0` to the relevant `IntRange`s is one config-line change; the harder
part is the workload generator gracefully producing a no-op scenario. Probably worth gating
on a `--config` setting rather than burning ~12.5 % of the default sweep on no-op seeds.

### 5. Partitioned tables (large, but high coverage value)

The harness uses `PartitionSpec.unpartitioned()` throughout. Partition-scoped delete files,
partition evolution, and partition-aware planning are tested only by `iceberg-core` unit
tests. The `CompactionMapBuilder`'s partition-handling, the resolver's partition spec lookup,
and `CompactionConflictDetector`'s partition pruning are all untouched by the fuzzer.
Significant lift — requires partitioning the workload, partition-aware slice picking, and
partition-spec-evolution edge cases — but real bugs are plausible (partitioning was the
root cause of multiple `iceberg-core` regressions in unrelated paths).

### 6. Real-content equality deletes (medium)

Per `EqualityDeleteOp`'s Javadoc the deletes "almost certainly do not match real row content";
the op exercises only the commit + conflict-detection paths, never the actual deletion. A
scenario that picks `long_0` values from rows known to exist in the post-chain state would
test the row-removal side of equality deletes against compaction. Confluence still
holds — both reference and treatment paths apply the same equality predicate — but the row
counts on either side would no longer be ignorable, and any divergence would localize bugs
in equality-delete + compaction-map interaction.

### 7. Schema evolution mid-chain (speculative)

Add/drop column, partition-spec change, sort-order change committed between chain snapshots.
`compaction_maps_errata.md` notes most of these are out of scope by design; fuzzing them might
just confirm the design rather than find bugs. Skip unless `errata.md` changes to claim
support.

## Spark version parity

The `SparkCompactionConflictResolver` lives in both `spark/v3.5` and `spark/v4.0`, byte-for-byte
identical except for one `cloneSession` cast — but only because previous engagements kept them
in sync manually. Bug 5 (the null-Row encoder crash) existed in both and required the same fix
in both.

### 8. Run the fuzzer against Spark 4.0 in CI (medium build-system lift)

`benchmark/compaction-baseline/build.gradle` hard-pins `iceberg-spark-3.5` and Scala 2.12. To
run the same fuzz harness against Spark 4.0 we need either a parallel
`benchmark/compaction-baseline-spark4` module (DRY violation, but isolates the dependency tree)
or a parameterized build that swaps the Spark dependency based on a Gradle property. The
cheapest viable path: a CI matrix entry with `-PsparkVersions=3.5` and `-PsparkVersions=4.0`,
each building its own shadow JAR, both running against the same anchor, summaries diffed.

The marginal value: Spark 4.0's Catalyst codegen (`serializefromobject_doConsume_0`) is the
exact pipeline that crashed in bug 5. We have *no* evidence the 4.0 path handles null Rows
identically — only that the source code is the same. A divergence wouldn't be visible until a
production v4.0 user filed a bug.

## SERIALIZABLE optimization soundness

The compaction-map value claim is sharper than what the current fuzzer tests:

> WITH compaction map → no conflict (structural change only).
> WITHOUT compaction map → ValidationException (data change).

The current harness verifies **confluence**: `hash(compact + remap-tx) == hash(tx + compact)`.
That's necessary but not sufficient. The soundness claim is **structural-only**: a compaction
snapshot preserves the row multiset exactly (modulo chain deletes that compaction applies
during scan). If that ever fails, the SERIALIZABLE-isolation exemption is unsound — a reader
could see compaction "appear to delete" a row.

### 9. Verify compaction preserves the row multiset (cheap)

At treatment-path compaction time, capture `hashPre = CorrectnessCheck.hash(table_at_parent)`
and `hashPost = CorrectnessCheck.hash(table_at_replace)`. Assert
`hashPre == hashPost`. Caveat: chain V2 PD files that compaction *applies* during scan
legitimately change the multiset, so the comparison either needs to subtract the chain delete
set from `hashPre`, or run only on `format=V3` scenarios where compaction is bin-pack-only
(no MoR scan during compaction). Adds one hash per scenario; the assertion directly attacks the
soundness invariant the SERIALIZABLE optimization relies on.

### 10. Fuzz the SERIALIZABLE reader path (medium)

Build scenarios where a transaction takes a snapshot, compaction commits with a compaction map,
then the transaction tries to commit a `RowDelta.validateNoConflictingDataFiles().commit()`
against the pre-compaction snapshot. The compaction-map-aware validator should allow the commit
without `ValidationException`. Today this is covered by `TestSerializableIsolationWithCompaction`
as a hand-written unit test but never by fuzzed inputs — race-condition shapes and oddly-positioned
compaction maps don't get tested. Add a `serializableReader` op kind that captures a snapshot id
pre-compaction, post-compaction attempts the no-op `RowDelta`, and asserts no exception. Run
against scenarios *with* a compaction map (must succeed) and *without* (negative control —
should throw `ValidationException`). Catches both false-positive conflicts (the optimization is
too conservative) and false-negative conflicts (the optimization is unsound).

## Ordering by value-per-effort

If picking one or two: **9 (SERIALIZABLE multiset preservation)** first — cheap, directly attacks
the soundness claim, no new op-kind plumbing — then **3 (compaction-map chains)** for the
documented-but-unfuzzed path. **10 (SERIALIZABLE reader)** has the highest invariant value but
needs the most plumbing. **8 (Spark 4.0 CI)** is the only one that's a build-system task rather
than a fuzzer change, so it slots into a different work block.
