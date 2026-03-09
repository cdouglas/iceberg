# Compaction maps review (cmpmap branch vs apache-iceberg-1.10.1 intent)

## Scope reviewed
- API/Core compaction map model, validation, remapping, and composition flow.
- Spark integration for conflict detection/resolution in rewrite data files.
- Benchmarking code only for implementation coupling (whether it measures core implementation).

## Summary
The implementation is substantial and generally coherent, but I found two correctness/completeness issues that should be addressed before calling the feature production-ready.

## Findings

### 1) Spark remap closure captures a non-serializable remapper (correctness risk)
**Severity:** High

`SparkCompactionConflictResolver.RemapFunctionWithRemapper` is declared `Serializable` and stores a `PositionDeleteRemapper` in a non-transient field. `PositionDeleteRemapper` does not implement `Serializable`.

In distributed Spark execution, this closure can be serialized and shipped to executors; capturing a non-serializable field is a classic `NotSerializableException` risk.

**Evidence:**
- `RemapFunctionWithRemapper` captures `private final PositionDeleteRemapper remapper` and uses it in `call`.【F:spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkCompactionConflictResolver.java†L489-L522】
- `PositionDeleteRemapper` does not implement `Serializable`.【F:core/src/main/java/org/apache/iceberg/PositionDeleteRemapper.java†L95-L99】

**Suggested fix direction:**
- Prefer the same pattern used by `RemapFunction`: capture `CompactionMap` (or chain-serializable representation), keep remapper transient, and lazily initialize on executor.
- Apply to both Spark 3.5 and 4.0 variants.

---

### 2) Core validator intentionally skips multi-file position delete conflict detection (correctness/completeness gap)
**Severity:** Medium-High

`CompactionMapValidator.findConflicts(...)` checks only `referencedDataFile != null`. For multi-file position delete files, it explicitly acknowledges it may miss conflicts.

This validator is used by `BaseRowDelta` compaction conflict validation path, so a delete transaction can pass validation without surfacing a remappable compaction conflict when only multi-file position deletes are involved.

**Evidence:**
- Validator only checks `referencedDataFile` and documents that it may miss multi-file conflicts.【F:core/src/main/java/org/apache/iceberg/CompactionMapValidator.java†L392-L410】
- `BaseRowDelta` uses this validator in the commit validation path.【F:core/src/main/java/org/apache/iceberg/BaseRowDelta.java†L222-L241】
- The complementary `CompactionConflictDetector` already has explicit multi-file position-delete handling, underscoring the asymmetry.【F:core/src/main/java/org/apache/iceberg/CompactionConflictDetector.java†L288-L315】

**Suggested fix direction:**
- Extend validator conflict detection to use delete-file bounds or content scanning for multi-file position deletes, consistent with detector behavior.
- At minimum, fail conservatively when multi-file position deletes are present and maps exist (to prevent silent false negatives).

---

## Test completeness assessment

### What is good
- Strong unit coverage across map composition, strategy selection, and no-pushdown benchmark variants.
- Integration tests exist for Spark conflict resolution and chained map scenarios.

### Gaps found
1. **No serialization-focused Spark test for `RemapFunctionWithRemapper`**
   - Current tests validate logical remapping scenarios but do not explicitly guard closure serializability risks.
2. **No validator-level test for multi-file position deletes in `CompactionMapValidator` path**
   - Detector has explicit tests for multi-file deletes.【F:core/src/test/java/org/apache/iceberg/TestCompactionConflictDetector.java†L530-L595】
   - Equivalent coverage is not present for validator commit-validation behavior.

## Benchmark coupling check
Benchmarks under `core/src/jmh` are measuring the same strategy classes in core (including benchmark-only no-pushdown variants guarded by explicit enable flags), not a second parallel implementation.

This is coherent with intent: benchmark code exercises core strategy implementations directly.

