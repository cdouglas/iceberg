# Compaction maps review for commit fe7fe00a98e970a63ad0b6899d63db583fee6b73

## Scope and method
- Reviewed **only** the repository state at commit `fe7fe00a98e970a63ad0b6899d63db583fee6b73`.
- Focus: correctness, multi-file position delete semantics, test completeness, Spark compaction soundness, and architecture/abstraction quality.
- Ignored benchmark performance claims; only checked benchmark/core implementation coupling.

## 1) Previous-review follow-up

### 1.1 Multi-file validator semantics (previous misunderstanding) — **fixed in this commit**
The validator now conservatively treats multi-file position deletes as conflicts when compacted files exist.

- New conservative branch in `findConflicts` for `POSITION_DELETES` with `referencedDataFile == null`.【F:core/src/main/java/org/apache/iceberg/CompactionMapValidator.java†L395-L412】
- This aligns with write-correctness semantics: position deletes are physical addresses and must be rebased when compaction may have moved rows.

### 1.2 Test updates for that semantic shift — **partially adequate**
- `TestCompactionMapValidatorMultiFileDeletes` now expects multi-file conflicts to be detected (good direction).【F:core/src/test/java/org/apache/iceberg/TestCompactionMapValidatorMultiFileDeletes.java†L168-L225】
- `TestSerializableIsolationWithCompaction` now distinguishes V2/V3 appropriately for this scenario (V2 conflict required; V3 DV targeting non-compacted file allowed).【F:core/src/test/java/org/apache/iceberg/TestSerializableIsolationWithCompaction.java†L205-L220】

## 2) Remaining correctness issues (soundness/liveness)

### 2.1 Spark resolver serializability issue remains unresolved
`RemapFunctionWithRemapper` still captures non-serializable `PositionDeleteRemapper` in Spark 3.5/4.0. This was a prior high-severity finding and is still present in this commit snapshot.

- 3.5 capture of `PositionDeleteRemapper` field.【F:spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkCompactionConflictResolver.java†L489-L504】
- 4.0 same pattern.【F:spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/actions/SparkCompactionConflictResolver.java†L489-L504】
- `PositionDeleteRemapper` is not `Serializable`.【F:core/src/main/java/org/apache/iceberg/PositionDeleteRemapper.java†L95-L99】

### 2.2 `max-files` safety limit still excludes multi-file deletes
Conflict cap enforcement counts only `deleteFileCount()` (file-scoped conflicts), but multi-file deletes are tracked separately and still fed into resolver.

- Guard checks only `conflicts.deleteFileCount()`.【F:spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkRewriteDataFilesCommitManager.java†L217-L226】
- Resolver appends `multiFilePositionDeletes()` in addition to `conflictingDeleteFiles()`.【F:spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkCompactionConflictResolver.java†L160-L170】
- Multi-file conflicts are intentionally outside `deleteFileCount()`.【F:core/src/main/java/org/apache/iceberg/DeleteConflictInfo.java†L124-L156】

This is a policy/correctness mismatch: operators can configure a safety cap that is not actually enforced for all conflict work.

### 2.3 Compaction-map fallback can still produce unsound maps
Fallback map generation (no explicit position tracking) assumes source concatenation by iteration order and `recordCount` offsets.

- Core fallback builder logic.【F:core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java†L237-L251】
- Spark fallback logic mirrors this assumption.【F:spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkRewriteDataFilesCommitManager.java†L340-L352】

This is only sound when physical output order matches that inferred order exactly; that should be an enforced precondition, not a silent assumption.

## 3) Multi-file position deletes: detailed path analysis

### What is now semantically correct
- Validator now forces rebase/conservative conflict for metadata-ambiguous multi-file position deletes.【F:core/src/main/java/org/apache/iceberg/CompactionMapValidator.java†L405-L410】
- Compaction-side detector already models multi-file deletes separately as content-based resolution candidates.【F:core/src/main/java/org/apache/iceberg/CompactionConflictDetector.java†L235-L302】
- Spark resolver includes these files and filters actual delete rows by compacted source-file membership before remapping (right shape for precision in execution).【F:spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkCompactionConflictResolver.java†L340-L349】

### Still-fragile edges
- `CompactionMapValidator.findConflictingDeletes` remains file-scoped only and does not reflect new conservative multi-file policy, so helper behavior is inconsistent with `findConflicts`/`validateNoCompactedReferences`.【F:core/src/main/java/org/apache/iceberg/CompactionMapValidator.java†L424-L439】
- `TestCompactionMapValidatorMultiFileDeletes` method name still says `...NotDetected` while asserting detection; this hurts maintainability and can mask future semantic regressions in review noise.【F:core/src/test/java/org/apache/iceberg/TestCompactionMapValidatorMultiFileDeletes.java†L168-L225】

## 4) Architecture / abstraction assessment

### Duplication that should be reduced
- `SparkCompactionConflictResolver` is duplicated across Spark 3.5 and 4.0 with near-identical logic (including identical pitfalls), increasing drift risk.
- Compaction-map fallback generation logic is duplicated between core and Spark commit managers.

A shared core abstraction for map-construction policy (including strict soundness preconditions) and a shared Spark remap-function strategy would reduce bug surface.

## 5) Test completeness assessment

### Improvements in this commit
- Validator-path behavior for multi-file position deletes now has direct test assertion coverage.【F:core/src/test/java/org/apache/iceberg/TestCompactionMapValidatorMultiFileDeletes.java†L168-L225】
- Serializable isolation test now captures the v2/v3 semantic distinction for this specific scenario.【F:core/src/test/java/org/apache/iceberg/TestSerializableIsolationWithCompaction.java†L205-L220】

### Missing tests still needed
1. Spark closure serializability test for `RemapFunctionWithRemapper` (Java and/or Kryo serialization).
2. `max-files` enforcement test that includes multi-file deletes (total work cap, not just file-scoped count).
3. Fallback-map soundness test, or explicit test that fallback is disabled unless deterministic ordering guarantees are satisfied.
4. Consistency test for `findConflictingDeletes` vs `findConflicts` semantics when multi-file deletes are present.

## 6) Benchmark coupling check
Benchmarks remain coupled to core strategy implementations, not a parallel implementation.

- Benchmark directly enables and runs core no-pushdown strategies for comparison mode.【F:core/src/jmh/java/org/apache/iceberg/RemappingAlgorithmBenchmark.java†L142-L144】

