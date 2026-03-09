# Round 4 Response

Reviewed against `round4-review1.md` and `round4-review2.md`.

## Finding 1 (Review 1): Chain collection is non-deterministic for fan-out rewrites

**Severity:** High
**Verdict:** Valid bug. Fixed.
**Commits:** `172c7229d`, `e5a76e4e1`

### Problem

`CompactionMapValidator.collectChainMaps` followed a single downstream target per hop — whichever `HashSet` iteration returned first — then broke out of the loop. For fan-out rewrites (F1 → {T1, T2}) where both targets are subsequently compacted (T1 → T3, T2 → T4), the method collected at most one of the two downstream maps. Which branch was collected was non-deterministic due to `HashSet` iteration order.

This is a correctness bug: a `ChainedCompactionMapsException` with an incomplete chain would cause the caller to remap only a subset of positions, silently dropping the positions that route through the missed branch.

### Fix

Replaced the single-path while loop with BFS traversal using `ArrayDeque` and a `visited` set. The method now enqueues all targets of each mapping and visits every reachable downstream map.

**Changed file:** `CompactionMapValidator.java:333-368`

Key properties of the fix:
- **Complete:** Every downstream branch is followed, regardless of fan-out degree.
- **Deterministic:** The `visited` set prevents re-traversal; collection order no longer depends on `HashSet` iteration.
- **Cycle-safe:** The `visited` check prevents infinite loops if compaction map metadata were ever to form a cycle (shouldn't happen, but defensive).

### Test

Added `testFanOutChainCollectsAllBranches` in `TestCompactionConflictDetection.java:914-1018`.

The test constructs:
1. File F1 (200 rows)
2. Fan-out compaction: F1 → {T1 (rows 0–99), T2 (rows 100–199)} with an explicit multi-target map
3. Second compaction: T1 → T3 (auto-generated single-target map)
4. Third compaction: T2 → T4 (auto-generated single-target map)
5. A concurrent `RowDelta` adding position deletes against F1

The test asserts that the resulting `ChainedCompactionMapsException` contains all 3 compaction maps. Before the fix, it would non-deterministically collect only 2.

---

## Finding 2 (Review 2): Low-level fallback map generation unsound for reorder rewrites

**Severity:** Medium-High
**Verdict:** Not a bug. Addressed with documentation.

### Why the finding is unsound

The reviewer's concern is that `BaseRewriteFiles.generateAndWriteCompactionMap()` could produce incorrect maps for sort or z-order rewrites that happen to preserve record count. This concern is theoretical — **compaction maps are never enabled for reorder rewrites**:

1. **Sort and z-order rewrites never enable `write.compaction-map.enabled`.** This is by design, not by accident. The feature is scoped exclusively to order-preserving operations (bin-pack and merge). See `CLAUDE.md` §3 "Design Scope: Order-Preserving Compactions".

2. **Action-level commit managers suppress auto-generation anyway.** Both `RewriteDataFilesCommitManager` and `SparkRewriteDataFilesCommitManager` call `disableAutoCompactionMap()` to take explicit control of map generation. The fallback in `BaseRewriteFiles` only executes for direct `table.newRewrite()` callers.

3. **Direct API callers doing bin-pack are the only remaining path.** A user calling `table.newRewrite()` directly with compaction maps enabled is performing a bin-pack concatenation. The record-count check is a necessary condition for this assumption.

Removing the auto-generation would have made the code *more* complex — every direct API caller would need to manually construct and attach compaction maps — while providing no correctness benefit for the actual usage scope.

### What we did instead

Improved documentation so that future reviewers understand the design rationale:

1. **Method-level javadoc** on `generateAndWriteCompactionMap()` (`BaseRewriteFiles.java:215-232`) now explains:
   - Why the fallback is sound (scoped to order-preserving operations only)
   - What the record-count check guards against (filtering/duplication) and what it cannot detect (reordering — which doesn't arise in bin-pack)
   - How action-level rewrites bypass this path via `disableAutoCompactionMap()`

2. **Inline comment** on the `Preconditions.checkState` call (`BaseRewriteFiles.java:265-268`) explains the check is a necessary-not-sufficient condition and references the design scope documentation.

3. **Improved error message** in the precondition (`BaseRewriteFiles.java:269-275`) guides users toward explicit position tracking if the check fails, rather than leaving them with an opaque assertion.

### Initial implementation and revert

We initially accepted this finding and removed auto-generation entirely in `172c7229d`. This required modifying 6 test files to manually construct compaction maps, adding complexity without correctness benefit. After discussion, we restored auto-generation with improved documentation in `e5a76e4e1`.
