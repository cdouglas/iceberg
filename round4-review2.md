# Round 4 review (cmpmap @ abb56f7fd5eb093d03c960bf8d3a8af9e2a4a033)

## Scope
- Re-reviewed cmpmap implementation with focus on correctness and production readiness.
- Ignored `benchmark/` as requested.
- Cross-checked prior review findings (`round1/2/3`) and `ROUND2_RESPONSE.md`/`ROUND3_RESPONSE.md` against current code.

## Status of prior findings

### Resolved
1. **Spark closure serializability risk** (round1): resolved.
   - `SparkCompactionConflictResolver` now serializes compaction maps to bytes and lazily reconstructs remapper on executors (`transient` remapper field).
2. **Validator skipping multi-file position deletes** (round1): resolved.
   - `CompactionMapValidator.findConflicts` now treats multi-file position deletes conservatively as conflicting with compacted files.
3. **Position-tracking multi-file assignment bug** (round2/3): resolved.
   - `PositionTrackingDataWriter` now assigns buffered mappings across multiple output files by cumulative record-count boundaries and adjusts file-local positions.
4. **`fromConflict`/chained exception hierarchy mismatch** (round3): resolved.
   - `ChainedCompactionMapsException` extends `CompactionConflictException`, and `PositionDeleteRemapper.fromConflict` handles chained maps.
5. **Action-level fallback map generation** (round3): resolved in commit managers.
   - Core and Spark rewrite commit managers now skip map generation when explicit position mappings are unavailable.

### Still concerning
The remaining concern from round3 is still present at the low-level API layer (detailed below).

---

## Finding

### 1) Low-level fallback compaction map generation remains unsound for reorder rewrites
**Severity:** Medium-High

`BaseRewriteFiles.generateAndWriteCompactionMap()` still auto-generates a fallback map when there is exactly one target file by concatenating source file record-count runs in iteration order. The code comment explicitly acknowledges this is only sound for concatenation/bin-pack semantics and that sort/z-order rewrites can produce incorrect maps even when record counts match.

Current protections are insufficient for production correctness:
- It only checks total record-count equality.
- It does **not** prove row-order preservation.
- It executes automatically for direct `table.newRewrite()` callers when compaction maps are enabled, unless callers remember to manually disable auto map generation.

This means a direct rewrite API user can still get a silently incorrect compaction map under reorder rewrites.

**Evidence:**
- Fallback logic and explicit unsoundness caveat in comments are still present in `BaseRewriteFiles.generateAndWriteCompactionMap`.
- Trigger condition remains automatic in `apply(...)` when map generation is enabled and not explicitly disabled.

**Why this matters:**
A wrong compaction map leads to wrong position-delete remapping targets, which is a data correctness risk (misapplied or dropped deletes).

**Recommended fix direction:**
- Prefer disabling low-level automatic fallback by default (opt-in only), **or**
- Require explicit caller-provided assertion that rewrite preserves row order before generating fallback map, **or**
- Remove fallback generation entirely and require explicit position tracking/provided map for all automatic map generation paths.

---

## Abstraction assessment
- Some Spark 3.5/4.0 duplication remains in conflict resolver and position-tracking writer paths, but this appears version-module duplication rather than a missing abstraction bug.
- I did not find an additional abstraction gap that is as correctness-critical as the fallback issue above.

## Production-readiness verdict
- Significant progress from earlier rounds; most previously reported correctness issues are fixed.
- **Not yet fully production-ready** due to the remaining unsound fallback path in `BaseRewriteFiles` for direct rewrite API use.
