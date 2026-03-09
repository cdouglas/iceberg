# Response to Round 3 Reviews

Reviewers: thank you for the thorough third pass. Both reviews identified the
same three findings. Below is our finding-by-finding response.

---

## Finding 1: `remapPositionsBulk` API mismatch in docs (Review 1 §1, Review 2 §1)

**Disposition: Correct. Already fixed.**

The doc example called `remapper.remapPositionsBulk(sourcePositions)` with one
argument and treated the return as `Map<String, long[]>`. The actual API is
`remapPositionsBulkPrimitive(String sourceFile, long[] positions)` returning
`Map<String, long[]>`.

**Fix (commit 189bb899c, prior to these reviews):** Updated to
`remapper.remapPositionsBulkPrimitive(sourcePath, sourcePositions)` in both
`compaction_maps.md` and `compaction_maps_impl.md`.

---

## Finding 2: `fromConflict()` does not handle chained compactions (Review 1 §2, Review 2 implicit)

**Disposition: Correct. Fixed.**

`ChainedCompactionMapsException` extended `ValidationException`, not
`CompactionConflictException`. This meant:
1. `fromConflict(CompactionConflictException, FileIO)` could not accept chained
   exceptions
2. Integrators catching `CompactionConflictException` would miss chained cases
3. The docs incorrectly claimed `fromConflict()` handled both

**Fix (two parts):**

**A. Hierarchy change:** `ChainedCompactionMapsException` now extends
`CompactionConflictException` instead of `ValidationException`. This means
`catch (CompactionConflictException e)` catches both single and chained cases.

**B. `fromConflict()` updated:** The method now checks for the
`ChainedCompactionMapsException` subtype. When detected, it builds a
`CompactionMapChain` from the pre-loaded maps and creates a chain-based
`PositionDeleteRemapper` for all affected files. Single-map conflicts continue
to use the existing location-based loading path.

The docs (already updated in the prior commit) correctly state that
`fromConflict()` handles both cases transparently. This is now actually true.

---

## Finding 3: Fallback map generation unsound for reorder rewrites (Review 1 §3, Review 2 Finding A)

**Disposition: Correct. Fixed by removing action-level fallback.**

The fallback generated maps assuming bin-pack concatenation order. The
record-count precondition added in round 2 was necessary but not sufficient:
a sort rewrite producing a single target with equal record count would pass
the check but produce an incorrect positional map.

**Fix:** Removed fallback map generation from all three action-level commit
managers:

| File | Change |
|------|--------|
| `RewriteDataFilesCommitManager.buildCompactionMap()` (core) | Skip + warn when no position mappings |
| `SparkRewriteDataFilesCommitManager.buildCompactionMap()` (Spark 3.5) | Skip + warn |
| `SparkRewriteDataFilesCommitManager.buildCompactionMap()` (Spark 4.0) | Skip + warn |

When a file group has no explicit position mappings, the commit manager now
logs a warning and skips map generation for that group. This means compaction
maps are only generated when explicit position tracking (via
`PositionTrackingDataWriter`) provides verified source-to-target mappings.

**`BaseRewriteFiles.generateAndWriteCompactionMap()`** retains its fallback
with the existing record-count precondition. This is the low-level API for
direct callers using `table.newRewrite()` — it has no position tracking
infrastructure and is expected to be used only for bin-pack operations. The
comment now explicitly documents this limitation and directs callers to
action-based rewrites with position tracking for sort/z-order.

---

## Finding B (Review 2 only): Contradictory SERIALIZABLE/write-conflict docs

**Disposition: Correct. Already fixed.**

The SERIALIZABLE section was replaced (commit 189bb899c) with a "Conflict
Detection: Writes vs Reads" section that clearly separates:
- **Write-conflict checking** (always active): position deletes targeting
  compacted files throw `CompactionConflictException`, requiring remap+retry
- **SERIALIZABLE read-conflict optimization**: compaction-aware check treats
  REPLACE-with-map as structural (no read conflict)

Both checks are documented as independent, with a note that a transaction can
pass the read check but still fail the write check.

---

## Summary of changes in this response

| Fix | Files changed | What |
|-----|--------------|------|
| §2 hierarchy | `ChainedCompactionMapsException.java` | Extends `CompactionConflictException` instead of `ValidationException` |
| §2 fromConflict | `PositionDeleteRemapper.java` | Handles `ChainedCompactionMapsException` via `CompactionMapChain` |
| §3 fallback removal | `RewriteDataFilesCommitManager.java`, `SparkRewriteDataFilesCommitManager.java` (3.5+4.0) | Skip map generation without position tracking |
| §3 comment | `BaseRewriteFiles.java` | Document low-level fallback limitations |
| §1 | None | Already fixed in prior commit |
| §B | None | Already fixed in prior commit |
