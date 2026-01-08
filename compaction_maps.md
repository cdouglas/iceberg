# Compaction Maps

## Problem

Apache Iceberg tables are a tree of immutable objects referenced by a root pointer for that table. At its leaves, updates do not write merged data files on every write, but instead write _delta files_ that- merged with the base data- produce the new state. For example, consider an insert into table `A` by a transaction `T1`. To update a row in a data file `D`, `T1` writes a [position delete file](https://iceberg.apache.org/spec/#position-delete-files) `x1` recording the URI of the affected data file (`D`) and the ordinal position of the row in that file. It writes a separate file `u1` containing the updated rows in Avro. If it commits, readers will merge the base data with these two files to omit deleted rows from `D` per `D_x1` and include inserted rows in `D_u1`.

As updates to `D` accumulate, the [overhead](https://dl.acm.org/doi/abs/10.1145/3639314) impacting readers is significant. A reader may need to merge deletion vectors not only for `D`, but also for rows inserted in delta files. Extending the above example, the reader may not only need to read `D_x2` and `D_u2` from a subsequent transaction `T2`, but also `u1_x2` (i..e, rows inserted by `T1` and deleted by `T2`). The quadratic worst-case is never observed, but it's a significant enough problem for vendors to offer a _compaction service_.

The compaction service materializes the merge as a new commit. It writes the result of a full scan as a new base data file `D'` that includes all committed transactions, then commits the result to the table. However, this presents a problem for concurrent transactions. While transactions commute with compaction by definition- it's the same state, just reorganized- concurrent transactions wrote their position delete files with respect to the old, physical layout of the table, not the unified `D'`. In practice, this causes the transaction to abort and restart, at least in part, to regenerate the positions of deleted rows in the reorganized data.

This is a particular problem for well-tuned workloads with a single writer i.e., the workload Iceberg was designed to handle. Randomly failing expensive or deadline-sensitive transactions from a single writer because the database is optimizing read performance is unsolvable as the user and- anecdotally- a common complaint. 

To repair the physical conflicts, we need a map from the old layout to the new one. Taking inspiration from [REMIX](https://www.usenix.org/conference/fast21/presentation/zhong), we can record the state transitions of the merge during compaction and record it as metadata. A transaction against the pre-compacted state (`D` + delta + delete files) need only follow the transitions of the merge to determine where its referents were relocated in the new, compacted data file `D'`.

Note: V3 of the format represents these data as bit vectors ([roaring bitmaps](https://roaringbitmap.org/)), but the problem remains. Unlike Hive (and some relational databases) that use rowids, we need to provide this ancillary data to the writer for it to remap the physical references required by the specification.

## Proposal

Consider the following layout, which roughly corresponds to a series of updates. on a base table. In this figure, entries in the deletion vector `XTn.p` are deleting from transaction delta file `n` at position `p`. So in this example, `T2` updates `A'->A''`,  updates `D->D'`, inserts `F`, and inserts `G`.

```
Base  T1          T2           T3            T4
A     XT0.1  A'   XT1.1 A''    XT2.1  A'''   XT3.1 A''''
B     XT0.2  B'
C                                            XT0.3 C'     
D                 XT0.4 D'
E                              XT0.5  E'
                        F      XT2.3  F'     XT3.3 F''
                        G                    XT2.4 G''
```

Compaction after `T3` would produce the compacted file:
```
A'''
B'
C
D''
E'
F'
G
```

Assume `T4` wrote its update based on `T3`. To recover, `T4` needs to find the new positions of the tuples it deleted. In short, we need to remap:

`(3,1) (0,3) (3,3) (2,4)` (the deleted values in T4: `A'''`, `C`, `F'`, and `G`)
to
`(0,1) (0,3) (0,6) (0,7)` (the positions of these values in the compacted file, `D'`)

The merge in this example inserts tuples from each delta file in this (transaction) order:
```
3.1, 1.2, 0.3, 2.2, 3.2, 3.3, 2.4
```

i.e., `A'''` is from `T3.1`, `B'` is from `T1.2`, `C` is from `T0.3`, and so on. One possible remapping could record a triple `(T, P, N)` where `T` is the transaction, `P` is the starting position, and `N` is the length of the run at each transition in the merge. This example... could have been better chosen because the runs are short:

```
(3, 1, 1) <- (3,1) to (0,1)
(1, 2, 1)
(0 ,3, 1) <- (0, 3) to (0, 3)
(2, 2, 1)
(3, 2, 2) <- (3, 3) to (0, 6); run starts at (0, 5) ends at (0, 6)
(2, 4, 1) <- (2, 4) to (0, 7)
```

`T4` tracks the position in the merged file as a running sum of runs. When it reaches a run that includes a tuple deleted by `T4` (e.g., `(3, 2, 2)` covers `(3, 3)` in `T4`'s delete vector), it can calculate the new position and emit it. Note that _only_ this compaction artifact and the transaction's old position delete file are used to create the new position delete file.

In Iceberg, runs (particularly in the base data) are likely thousands or tens of thousands of tuples. This should be a trivial amount of data, the merge produces it anyway, and one would be pressed to imagine a non-pathological transaction that could be cheaper to run.

## Challenges and Opportunities

This is really niche. Anecdotally it's an ongoing problem (according to a PM in SQL DW at Microsoft and another engineer at LinkedIn), but to convince an audience not soaking in Iceberg operations minutiae that it's more than a self-inflicted nuisance might be a hard sell. On the other hand, Delay Scheduling has 2041 citations, so maybe we shouldn't be too timid.

There is no reasonable, real-world benchmark where this will show any improvement. We can write a benchmark that isolates the conflict and show that it works to resolve it, but I have no idea what we could compare against. This is a product problem.

One possible path would be to vibe-code an example and show it to folks. If they're interested, we can ask them for support in building an example _including_ motivation from production. I'm happy to add authors from industry, even if it's just war stories and graphs from production. But if we don't get anyone pulling on this, we can write it up for an Iceberg summit or something and forget it.