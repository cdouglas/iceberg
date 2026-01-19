# Implementation Plan: Staged Scan Metadata Column Support

## Overview

This plan addresses the ~10-20% performance overhead caused by using normal scans instead of staged scans when position tracking is enabled. The goal is to make staged scans properly support `_file` and `_pos` metadata columns so they can be used for compaction map generation.

## Problem Summary

**Current State:**
- Position tracking for compaction maps requires `_file` and `_pos` metadata columns
- Normal scans work perfectly with metadata columns via `SupportsMetadataColumns` interface
- Staged scans are ~10-20% faster (skip manifest re-scanning) but metadata columns fail
- Error: `java.util.NoSuchElementException: key not found: _file` at `PushDownUtils.toOutputAttrs`

**Root Cause:**
Staged scans bypass Spark's metadata column resolution infrastructure. During physical planning, `PushDownUtils.toOutputAttrs` cannot map metadata column names to output attributes because:
1. Spark's `V2ScanRelationPushDown` optimizer prunes columns not in the logical plan
2. Metadata columns added only to the scan schema (not the logical plan) get pruned
3. Physical planner fails when trying to map pruned columns

## Previous Attempts (from staged_scan_investigation.md)

| Attempt | Approach | Result | Why It Failed |
|---------|----------|--------|---------------|
| 1 | Add columns to `schemaWithMetadataColumns()` | FAILED | Optimizer pruned them (not in logical plan) |
| 2 | Add to `metaColumns` list in `pruneColumns()` | FAILED | `pruneColumns()` receives already-pruned schema |
| 3 | Add columns to base schema before pruning | FAILED | Still pruned during physical planning |

## Analysis: Why Normal Scans Work

Normal scans succeed because they follow Spark's full metadata column protocol:

1. **Declaration:** `SparkTable.metadataColumns()` declares available metadata columns (via `SupportsMetadataColumns`)
2. **Selection:** User explicitly selects `.selectExpr("_file", "_pos")` in DataFrame
3. **Logical Plan:** Spark includes metadata columns in the logical plan's output
4. **Optimization:** `V2ScanRelationPushDown` preserves explicitly-requested metadata columns
5. **Pruning:** `pruneColumns()` receives schema that includes metadata columns
6. **Physical Plan:** `PushDownUtils.toOutputAttrs` successfully maps columns to output attributes

Staged scans skip step 2 (explicit selection) and expect columns to be implicitly added, breaking the chain.

## Proposed Solution

### Strategy: Implicit Metadata Column Selection

Make staged scans behave like normal scans when position tracking is enabled by ensuring metadata columns appear in Spark's logical plan BEFORE optimization.

### Implementation Phases

#### Phase 1: Verify Current Failure Mode

**Objective:** Confirm the exact failure point and validate previous findings still apply.

**Tasks:**
1. Create minimal test case that reproduces `key not found: _file` error with staged scan
2. Add debug logging to `SparkStagedScanBuilder.pruneColumns()` to observe requested schema
3. Trace execution path through Spark's physical planner
4. Document exact call stack and failure point

**Files:**
- `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/source/TestStagedScanMetadataColumns.java` (new)

#### Phase 2: Investigate Scan Infrastructure

**Objective:** Understand differences between normal and staged scan metadata handling.

**Tasks:**
1. Compare `SparkBatchQueryScan` vs `SparkStagedScan` implementations
2. Check if `SparkStagedScan` properly inherits/implements metadata column interfaces
3. Analyze how `SupportsMetadataColumns.metadataColumns()` is used by Spark
4. Identify which Spark classes are involved in metadata column resolution

**Key Questions:**
- Does `SparkStagedScan` implement `SupportsMetadataColumns`?
- How does Spark's `V2ScanRelationPushDown` interact with metadata columns?
- What attribute in the logical plan triggers metadata column preservation?

#### Phase 3: Implement Metadata Column Preservation

**Objective:** Make staged scans preserve metadata columns through optimization.

**Approach A: Explicit Implicit Selection (Recommended)**

When position tracking is enabled, modify the DataFrame creation to include explicit metadata column selection:

```java
// In SparkBinPackDataRewriter or equivalent
if (readConf.trackSourcePositions()) {
  // Add explicit _file/_pos to output columns BEFORE optimization
  df = df.selectExpr(table.schema().columns().stream()
      .map(c -> c.name())
      .toArray(String[]::new))
      .withColumn("_file", functions.col("_file"))
      .withColumn("_pos", functions.col("_pos"));
}
```

**Approach B: Schema Enrichment with Marker**

Mark metadata columns as "required" in the schema so optimizer doesn't prune them:

```java
// In SparkStagedScanBuilder.pruneColumns()
if (readConf.trackSourcePositions()) {
  // Always keep _file and _pos regardless of requested schema
  metaColumns.add("_file");
  metaColumns.add("_pos");
}
```

Combined with making `SparkStagedScan` properly report these in `readSchema()`.

**Approach C: Custom Physical Plan (Complex)**

Implement a custom Spark physical plan node that wraps staged scans and injects metadata columns during execution. This bypasses Spark's optimization but is complex and fragile.

**Recommended: Start with Approach A**, fall back to B if A doesn't work within Spark's constraints.

#### Phase 4: SparkStagedScanBuilder Changes

**File:** `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkStagedScanBuilder.java`

```java
// Add position tracking logic (lines 80-91)
private Schema schemaWithMetadataColumns() {
  List<String> columnsToAdd = Lists.newArrayList(metaColumns);

  // Add _file and _pos metadata columns when position tracking is enabled
  if (readConf.trackSourcePositions()) {
    if (!columnsToAdd.contains("_file")) {
      columnsToAdd.add("_file");
    }
    if (!columnsToAdd.contains("_pos")) {
      columnsToAdd.add("_pos");
    }
  }

  List<Types.NestedField> fields =
      columnsToAdd.stream()
          .distinct()
          .map(name -> MetadataColumns.metadataColumn(table, name))
          .collect(Collectors.toList());
  Schema meta = new Schema(fields);

  return TypeUtil.join(schema, meta);
}
```

**Note:** This alone won't fix the issue (Attempt 1 already tried this). We need to ensure the logical plan includes these columns.

#### Phase 5: DataFrame Builder Changes

**File:** `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBinPackDataRewriter.java`

Modify how the DataFrame is constructed when using staged scans:

```java
// When building scan DataFrame for staged scan with position tracking
protected Dataset<Row> buildScanDF(RewriteFileGroup group) {
  Dataset<Row> df = spark.read()
      .format("iceberg")
      .option(SparkReadOptions.SCAN_TASK_SET_ID, group.groupId())
      .load(table.location());

  if (readConf.trackSourcePositions()) {
    // Explicitly select all columns including metadata
    // This ensures _file and _pos are in the logical plan
    List<String> selectCols = new ArrayList<>();
    for (Types.NestedField field : table.schema().columns()) {
      selectCols.add(field.name());
    }
    selectCols.add("_file");
    selectCols.add("_pos");

    df = df.selectExpr(selectCols.toArray(new String[0]));
  }

  return df;
}
```

#### Phase 6: SparkStagedScan Changes

**File:** `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkStagedScan.java`

Ensure `readSchema()` includes metadata columns when position tracking is enabled:

```java
@Override
public StructType readSchema() {
  // Return schema that includes metadata columns
  // This tells Spark what the scan will output
  return SparkSchemaUtil.convert(expectedSchema);
}
```

This should already be correct since `expectedSchema` comes from `SparkStagedScanBuilder.build()` which calls `schemaWithMetadataColumns()`.

#### Phase 7: Integration Testing

**Tasks:**
1. Create test that uses staged scan with position tracking enabled
2. Verify `_file` and `_pos` columns are populated correctly
3. Compare performance between normal scan and fixed staged scan
4. Test with Parquet, ORC, and Avro file formats
5. Test with V2 (position deletes) and V3 (deletion vectors) format tables

**Files:**
- `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/source/TestStagedScanWithPositionTracking.java` (new)
- `spark/v4.0/spark/src/test/java/org/apache/iceberg/spark/source/TestStagedScanWithPositionTracking.java` (new)

#### Phase 8: Port to Spark 4.0

After Spark 3.5 implementation is complete and tested, port changes to Spark 4.0:
1. `SparkStagedScanBuilder.java`
2. `SparkStagedScan.java`
3. Relevant test files

The files are currently identical between versions, so changes should apply cleanly.

## Risk Assessment

### High Risk
- **Spark version compatibility:** Changes may behave differently across Spark minor versions
- **Optimizer changes:** Future Spark versions may change optimization behavior

### Medium Risk
- **Performance regression:** If fix requires additional DataFrame operations, may reduce performance gain
- **Edge cases:** Complex schemas or nested metadata columns may have issues

### Low Risk
- **Breaking changes:** Changes are additive and behind feature flag (`trackSourcePositions`)
- **Test coverage:** Existing tests provide good baseline

## Success Criteria

1. Staged scans work with position tracking enabled (no `key not found: _file` error)
2. `_file` and `_pos` columns populated correctly in output rows
3. Performance improvement over current normal scan implementation (target: 10-15% faster)
4. All existing compaction map tests continue to pass
5. New integration tests pass for both Spark 3.5 and 4.0

## Estimated Complexity

| Phase | Complexity | Effort |
|-------|------------|--------|
| 1. Verify Failure | Low | Investigation |
| 2. Infrastructure Analysis | Medium | Research |
| 3. Design Solution | Medium | Design |
| 4. SparkStagedScanBuilder | Low | Code |
| 5. DataFrame Builder | Medium | Code |
| 6. SparkStagedScan | Low | Code |
| 7. Testing (Spark 3.5) | Medium | Test |
| 8. Port to Spark 4.0 | Low | Code |

## Alternative Approaches

### Alternative 1: Keep Normal Scans (Current Implementation)
**Pros:** Already working, proven reliable
**Cons:** 10-20% performance overhead

### Alternative 2: Side-Channel Position Tracking
Pass position tracking state through thread-local or scan context instead of DataFrame columns.
**Pros:** Bypasses Spark optimization entirely
**Cons:** Complex, hard to maintain, violates DataFrame semantics

### Alternative 3: Patch Spark DSv2
Submit fix to Apache Spark to properly handle implicit metadata columns in staged scans.
**Pros:** Correct fix at the source
**Cons:** Long timeline, version dependency, may not be accepted

## Recommendation

**Start with Phase 1-2** to verify the exact failure mode and understand the infrastructure. If Approach A (explicit selection) works, it's the simplest fix. If not, Approach B (schema enrichment) may require deeper Spark integration changes.

If all approaches fail, consider whether the 10-20% overhead is acceptable for this advanced feature, and update documentation accordingly.

## References

- `docs/staged_scan_investigation.md` - Previous investigation details
- `docs/docs/compaction_maps_errata.md` - Known limitations documentation
- Spark DSv2 source: `V2ScanRelationPushDown.scala`, `PushDownUtils.scala`
- Iceberg: `SparkScanBuilder.java`, `SparkStagedScanBuilder.java`, `SparkTable.java`
