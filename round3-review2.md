# Compaction maps review for commit ffb3590193314bfbfc410331ded4dd7cfbfd26e6

## Scope
- Reviewed only the current HEAD commit: `ffb3590193314bfbfc410331ded4dd7cfbfd26e6`.
- Cross-checked prior feedback in `round2-review1.md`, `round2-review2.md`, and the claimed fixes in `ROUND2_RESPONSE.md`.

## Assessment of ROUND2_RESPONSE claims

### 1) Claim: fallback compaction-map soundness issue is fixed
**Status: Partially addressed, not fully fixed.**

What changed is a `recordCount` equality precondition in fallback map generation paths.
That catches some bad cases, but it does not guarantee correctness.

A rewrite that reorders rows but preserves row count (for example, a sort rewrite that still emits a single target file with equal record count) will still pass this check and produce an incorrect positional map.

This means the core soundness gap from previous review is reduced but not eliminated.

### 2) Claim: review feedback on docs and integration semantics is fully addressed
**Status: Not fully addressed.**

The top-level docs still contain contradictory guidance for application transactions:
- One section states a RowDelta delete commit "succeeds" after compaction when a map exists.
- A later section correctly says application transactions receive `CompactionConflictException` and must remap/retry.

The first statement is incorrect for position-delete writes and conflicts with both implementation and tests.

## New findings

## Finding A (Correctness): fallback compaction map can still be unsound on row reorder with equal counts
**Severity: High**

Fallback map generation currently assumes positional concatenation order and validates only total row count equality.
That is a necessary guard, but not sufficient to prove source->target position preservation.

Affected paths:
- `RewriteDataFilesCommitManager.buildCompactionMap` (core fallback)
- `SparkRewriteDataFilesCommitManager.buildCompactionMap` (Spark 3.5 and 4.0 fallback)
- `BaseRewriteFiles.generateAndWriteCompactionMap` (core fallback)

All three can still silently emit incorrect maps for reorder-preserving-count rewrites.

**Recommendation:**
- Enforce fallback generation only for known order-preserving rewrite modes (e.g., explicit bin-pack mode flag), or
- Remove fallback map generation entirely for generic rewrites and require explicit position tracking whenever maps are enabled.

## Finding B (Documentation correctness): contradictory behavior for RowDelta conflicts
**Severity: Medium**

`docs/docs/compaction_maps.md` has a SERIALIZABLE example saying RowDelta commit succeeds with compaction map.
This contradicts the documented/implemented conflict flow for position-delete writes.

Implementation/tests show that position-delete writes may throw `CompactionConflictException` and require remap+retry.

**Recommendation:**
- Update the SERIALIZABLE section to distinguish read validation from write rebasing.
- Keep one consistent narrative: compaction maps allow conflict *recovery* for position-delete writes, not automatic success for application RowDelta commits.

## What appears correctly fixed from previous review
- Spark resolver serializability issue appears addressed by storing compaction maps as byte arrays and lazy remapper reconstruction on executors.
- `deleteFileCount()` now includes multi-file position deletes, aligning the `max-files` cap with resolver work.
- `findConflictingDeletes` dead helper inconsistency is resolved by method removal.


