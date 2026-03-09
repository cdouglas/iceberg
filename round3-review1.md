# Round 3 review of cmpmap @ ffb3590193314bfbfc410331ded4dd7cfbfd26e6

## Scope
- Reviewed only commit `ffb3590193314bfbfc410331ded4dd7cfbfd26e6` as requested.
- Read and evaluated prior review notes (`round2-review1.md`, `round2-review2.md`) and the claimed fixes in `ROUND2_RESPONSE.md`.
- Re-reviewed compaction map correctness/integration and the `docs/compaction_maps*.md` docs for implementation alignment and transaction-engine integration completeness.

## Evaluation of `ROUND2_RESPONSE.md` claims

### Claim status summary
- ✅ **Addressed:** `deleteFileCount()` now includes both file-scoped and multi-file delete work, and `max-files` now gates total resolver work as intended.
- ✅ **Addressed:** Spark remap closures now serialize compaction maps to bytes and keep executor remapper fields transient, avoiding direct serialization of `PositionDeleteRemapper`.
- ⚠️ **Partially addressed / still unsound in edge cases:** fallback compaction-map generation still accepts reorder operations that preserve record counts; this remains insufficient as a soundness proof.
- ❌ **Not accurate (docs/response claim):** response and docs claim `PositionDeleteRemapper.fromConflict(...)` handles chained compactions; code does not. Chained cases surface as `ChainedCompactionMapsException` and require explicit chain handling.

## Findings

### 1) Documentation API example is incorrect for `remapPositionsBulk` (high)
`docs/docs/compaction_maps.md` uses an API call signature that does not exist:
- Docs call `remapper.remapPositionsBulk(sourcePositions)` and treat return type as `Map<String, long[]>`.
- Actual API is `remapPositionsBulk(String sourceFile, Iterable<Long> positions)` and returns `Map<String, Set<Long>>`.

This is a direct integration hazard for transaction engines implementing manual conflict recovery from docs.

### 2) Chained compaction handling is incorrectly documented as `fromConflict` support (high)
The docs and `ROUND2_RESPONSE.md` claim `PositionDeleteRemapper.fromConflict(...)` handles chained compactions, but:
- `fromConflict` accepts only `CompactionConflictException` and reads single-map locations.
- Chained scenarios are raised as `ChainedCompactionMapsException` with `compactionMaps()` and must be handled differently.

This leaves a critical gap in the “exhaustive integrator guidance” requirement: integrators are not told they must also catch and handle `ChainedCompactionMapsException`.

### 3) Fallback compaction-map “soundness fix” remains incomplete (medium)
The added fallback precondition (`target.recordCount == sum(source.recordCount)`) prevents some bad maps but is still not sufficient to prove row-order preservation. Reordering rewrites with equal counts can still pass this check and yield incorrect positional maps.

Given fallback generation remains enabled when explicit position mappings are unavailable, the implementation still relies on an assumption not fully enforced in code.

## Additional notes
- Prior issue about Spark closure serializability appears fixed at this commit (transient remapper + byte[] map transport pattern in both Spark 3.5/4.0 resolver implementations).
- Prior issue about `max-files` excluding multi-file deletes appears fixed by updated `DeleteConflictInfo.deleteFileCount()`.

## Recommended remediation
1. Fix docs/examples in `docs/compaction_maps.md` and `docs/compaction_maps_impl.md` to match actual method signatures and return types.
2. Update integration guidance to explicitly handle both:
   - `CompactionConflictException` (single-map path), and
   - `ChainedCompactionMapsException` (compose via `CompactionMapChain` / `PositionDeleteRemapper(CompactionMapChain)`).
3. Either:
   - hard-disable fallback map generation for non-bin-pack rewrites via explicit rewrite-strategy checks, or
   - require explicit position tracking whenever compaction maps are enabled and strategy is not provably concatenation-preserving.

