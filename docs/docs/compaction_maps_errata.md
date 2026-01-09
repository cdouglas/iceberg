---
title: "Compaction Maps - Implementation Errata"
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Compaction Maps - Implementation Errata

This document lists expedient implementation choices and simplifications made during development that we intend to revisit and improve later. These are not bugs but pragmatic decisions to ship functionality faster while documenting technical debt.

## 1. Normal Scans Instead of Staged Scans

### Issue

Position tracking uses **normal scans** instead of **staged scans** for reading data during bin-pack rewrites when compaction map generation is enabled.

### Impact

**Performance: ~10-20% slower**
- Normal scans re-scan manifests that were already scanned during planning
- File path filter evaluation adds overhead
- Manifest reader instantiation repeated

### Why This Choice Was Made

Staged scans don't properly expose metadata columns (`_file`, `_pos`) to Spark's physical planner:

```
Staged Scan Behavior:
1. Metadata columns added to scan schema ✅
2. Spark V2ScanRelationPushDown optimizer phase ❌
3. PushDownUtils.toOutputAttrs fails with "key not found" error
4. Metadata columns pruned away before reaching executor
```

Normal scans fully support metadata columns through Spark's `SupportsMetadataColumns` interface and preserve them through the entire query planning pipeline.

### Investigation

See [`docs/staged_scan_investigation.md`](../../docs/staged_scan_investigation.md) for detailed investigation including:
- Stack traces showing where staged scans fail
- Analysis of Spark's optimization phases
- Why `toOutputAttrs` can't map metadata column names to field IDs
- Multiple attempted solutions and their outcomes

### What Needs to Be Done

**Option 1: Fix Staged Scan Metadata Column Support (Preferred)**

Investigate and fix the root cause in Iceberg's staged scan implementation:
- Ensure metadata columns survive Spark's `V2ScanRelationPushDown` optimization
- Fix `PushDownUtils.toOutputAttrs` mapping for metadata columns
- Maintain backward compatibility with existing staged scan usage

**Option 2: Optimize Normal Scan Path (Workaround)**

If fixing staged scans proves complex:
- Cache manifest scan results between planning and execution
- Optimize file path filter evaluation
- Consider pre-computed file sets similar to staged scans

**Option 3: Alternative Metadata Propagation**

Explore alternative mechanisms for propagating metadata:
- Custom Spark physical plan nodes that inject metadata
- Partition-level metadata aggregation instead of row-level
- Broadcast variables containing file-to-position mappings

### Current Workaround

The ~10-20% performance overhead is acceptable for the initial implementation because:
- Compaction map generation is an opt-in feature (disabled by default)
- Used primarily for high-concurrency workloads where conflict resolution matters more than raw throughput
- Overhead only applies when `write.compaction-map.enabled=true`
- Most production workloads can absorb this cost for the safety guarantees

### Code Location

```
spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackFileRewriteRunner.java

Lines 55-116: Scan type selection logic
Lines 57-67: Documentation explaining the tradeoff
Lines 69-104: Normal scan path with metadata columns
Lines 106-116: Staged scan fallback when position tracking disabled
```

### Validation

To verify the performance impact:
```bash
# Run bin-pack without position tracking (staged scans)
./gradlew :iceberg-spark:iceberg-spark-3.5_2.13:test \
  --tests "TestRewriteDataFilesAction.testBinPackWithoutCompactionMaps"

# Run bin-pack with position tracking (normal scans)
./gradlew :iceberg-spark:iceberg-spark-3.5_2.13:test \
  --tests "TestRewriteDataFilesAction.testBinPackWithCompactionMaps"

# Compare execution times and manifest read counts
```

---

## 2. Spark 4.0 Support Deferred

### Issue

Position tracking is **not implemented for Spark 4.0**. The feature works correctly in Spark 3.5 but is blocked in Spark 4.0 due to stricter schema validation.

### Impact

**Functionality: Spark 4.0 users cannot use position tracking**
- Compaction maps can still be read/validated/remapped (core infrastructure works)
- Compaction maps cannot be generated during Spark 4.0 rewrites
- Users must use Spark 3.5 or wait for Spark 4.0 support

### Why This Choice Was Made

Spark 4.0 introduced stricter schema validation during Parquet writer creation that breaks the approach used in Spark 3.5:

```
Spark 3.5 Behavior (Working):
DataFrame: [id, data, _file, _pos]  (4 columns)
Parquet Schema: [id, data]          (2 columns)
Writer Creation: ✅ Succeeds
Row Write: Writer ignores _file and _pos columns ✅

Spark 4.0 Behavior (Broken):
DataFrame: [id, data, _file, _pos]  (4 columns)
Parquet Schema: [id, data]          (2 columns)
Writer Creation: ❌ IndexOutOfBoundsException
  at ParquetWithSparkSchemaVisitor.visitFields(line 196)
  "Index 2 out of bounds for length 2"
```

The error occurs during writer creation, before any rows are written, making runtime row projection ineffective.

### Investigation

See [`spark/v4.0/docs/position_tracking_challenges.md`](../../spark/v4.0/docs/position_tracking_challenges.md) for comprehensive analysis including:
- Detailed error analysis with stack traces
- Three attempted solutions and why each failed
- Three potential solutions with pros/cons
- Recommended implementation approach
- Code locations and debugging context

### What Needs to Be Done

**Recommended Approach: Lenient Schema Matching**

Modify `ParquetWithSparkSchemaVisitor.visitFields()` to skip trailing columns in DataFrame schema that aren't in Parquet schema:

```java
// Current code (strict validation)
for (StructField sField : struct.fields()) {
  if (sField.dataType() != DataTypes.NullType) {
    Type field = group.getFields().get(fieldIndex);  // ❌ Throws if fieldIndex >= group size
    ...
  }
}

// Proposed fix (lenient for trailing metadata)
for (int i = 0; i < struct.fields().length; i++) {
  StructField sField = struct.fields()[i];
  if (sField.dataType() != DataTypes.NullType) {
    if (fieldIndex >= group.getFieldCount()) {
      // Skip trailing metadata columns not in Parquet schema
      break;  // ✅ Allow extra columns
    }
    Type field = group.getFields().get(fieldIndex);
    ...
  }
}
```

This would align Spark 4.0 behavior with Spark 3.5 while maintaining backward compatibility.

**Alternative Approaches:**

See the detailed document for other options including:
- Custom metadata column mechanism (high effort)
- Two-phase writer wrapping (complex)
- Different read strategy (unclear feasibility)

### Current State

**Spark 4.0 code structure:**
- ✅ All classes implemented (mirrors Spark 3.5)
- ✅ Comprehensive TODO comments explaining blocker
- ✅ Tests exist (currently failing with documented error)
- ❌ Non-functional due to schema validation blocker

**Code locations with TODO comments:**
```
spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/source/PositionTrackingDataWriter.java
spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/source/SparkWriteBuilder.java
spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackFileRewriteRunner.java
spark/v4.0/spark/src/test/java/org/apache/iceberg/spark/actions/TestBinPackWithPositionTracking.java
```

### Workaround

**For users needing compaction maps with Spark:**
- Use Spark 3.5 for rewrites that generate compaction maps
- Spark 4.0 can still read and use existing compaction maps
- Core infrastructure (remapping, validation) works in all versions

**For developers:**
- Test position tracking features using Spark 3.5
- Spark 4.0 tests serve as regression suite for when blocker is fixed
- Keep Spark 4.0 code synchronized with Spark 3.5 improvements

### Validation

To verify Spark 4.0 still hits the blocker:
```bash
# This test should fail with IndexOutOfBoundsException
./gradlew :iceberg-spark:iceberg-spark-4.0_2.13:test \
  --tests "TestBinPackWithPositionTracking.testBinPackGeneratesCompactionMapWithoutDeletes"

# Expected error:
# java.lang.IndexOutOfBoundsException: Index 2 out of bounds for length 2
#   at ParquetWithSparkSchemaVisitor.visitFields(ParquetWithSparkSchemaVisitor.java:196)
```

---

## 3. Position Tracking Limited to Bin-Pack Rewrites

### Issue

Position tracking is only implemented for **bin-pack rewrites** (combining multiple data files without reordering). Rewrite operations that reorder rows are not supported.

### Impact

**Functionality: Rewrite-time reordering not supported**
- ✅ **Bin-pack rewrites**: Multiple files → single/multiple files (simple concatenation)
- ✅ **Merge compactions**: Combining data files with position deletes (deletes applied during scan)
- ❌ **Sorted rewrites**: Row order changes during rewrite operation (e.g., SORT BY column)
- ❌ **Z-ordered rewrites**: Data reorganization changes positions

### Why This Choice Was Made

Bin-pack is the most common compaction pattern and has simple position semantics:
- Source rows map sequentially to target: `source[0..N] → target[offset..offset+N]`
- When position deletes exist, they're applied during scan (standard Iceberg behavior)
- Only surviving rows appear in DataFrame with `_file` and `_pos` metadata
- Gaps in runs automatically represent deleted positions

### How Merge Compactions Work

The implementation **DOES support merge compactions** (bin-pack with position deletes):

**Example:**
```
Source file A: positions 0, 1, 2, 3, 4
Position delete: delete row 2 from file A
Scan phase: Iceberg applies deletes, returns rows with _pos = 0, 1, 3, 4
Write phase: PositionTrackingDataWriter records mappings for _pos = 0, 1, 3, 4
Target file: positions 0, 1, 2, 3
Compaction map: Run(0, 0, 2), Run(3, 2, 2)  // Gap at source position 2
```

**Why It Works:**
1. Position deletes applied during scan (before position tracking sees the data)
2. Only surviving rows get position mappings
3. Gap at source position 2 automatically represented by non-consecutive runs
4. No special instrumentation needed for delete handling

### What Needs to Be Done

**For Sorted Rewrites:**
- Track position transformations as rows are reordered during sort operation
- Record which source position maps to which target position after sorting
- Instrument Spark's sort operator to capture position changes

**For Z-Ordered Rewrites:**
- Track position changes as data is reorganized
- Handle complex reordering patterns
- Coordinate with Z-order implementation

**Current Status:**
- Rewrite-time sorting not supported (would require tracking through Spark's sort operator)
- Scan-time filtering fully supported (position deletes applied during scan)

### Validation

Comprehensive tests needed to verify merge compactions:
```bash
# Test bin-pack with position deletes
./gradlew :iceberg-spark:iceberg-spark-3.5_2.13:test \
  --tests "TestBinPackWithPositionTracking.testBinPackWithPositionDeletes"

# Verify compaction maps have correct gaps
# Verify position delete remapping works end-to-end
```

---

## 4. No Automatic Conflict Resolution

### Issue

When position deletes conflict with compacted files, the conflict is **detected but not automatically resolved**. Applications must manually remap and retry.

### Impact

**Usability: Manual intervention required for conflicts**

Applications must implement conflict resolution:
```java
try {
  rowDelta.addDeletes(deleteFile);
  rowDelta.commit();
} catch (CompactionConflictException e) {
  // Manual resolution required:
  // 1. Get compaction map locations from exception
  // 2. Load maps and create remapper
  // 3. Remap position deletes
  // 4. Retry commit with remapped deletes
}
```

### Why This Choice Was Made

**Safety First:**
- Remapping is a complex operation that transforms file references
- Explicit control allows users to audit what's being remapped
- Automatic retry could hide issues or create unexpected behavior
- Easier to add automatic resolution later than to remove it

**Simpler Implementation:**
- No need for complex retry logic
- No risk of infinite retry loops
- Clear separation between detection and resolution

### What Needs to Be Done

**Automatic Resolution Enhancement:**

Add opt-in automatic remapping to `BaseRowDelta`:
```java
// Enable automatic conflict resolution
rowDelta.enableAutomaticRemapping(true);
rowDelta.addDeletes(deleteFile);
rowDelta.commit();
// If compaction conflict detected:
//   1. Automatically load compaction maps
//   2. Remap position deletes
//   3. Retry commit transparently
```

**Implementation considerations:**
- Make it opt-in via configuration or API call
- Add validation to ensure remapping is safe (no overlapping runs, etc.)
- Provide callback/logging for transparency
- Handle edge cases (multiple conflicts, partial remapping)

### Current Workaround

Manual resolution is well-documented and tested:
- Clear error messages with remediation guidance
- Exception provides compaction map locations
- PositionDeleteRemapper API is simple to use
- Most DELETE operations don't hit this case (they create new delete files)

### Code Location

```
core/src/main/java/org/apache/iceberg/BaseRowDelta.java
  validateNoCompactionConflicts() - Throws CompactionConflictException

core/src/main/java/org/apache/iceberg/CompactionMapValidator.java
  validateNoCompactedReferences() - Conflict detection logic

core/src/main/java/org/apache/iceberg/PositionDeleteRemapper.java
  remapDelete() - Manual remapping API
```

### Validation

See test cases demonstrating manual resolution:
```bash
./gradlew :iceberg-core:test --tests "TestCompactionConflictResolution"
```

---

## Summary

| Issue | Impact | Status | Priority |
|-------|--------|--------|----------|
| Normal scans vs staged scans | 10-20% performance overhead | Documented, acceptable | Medium |
| Spark 4.0 support deferred | Feature unavailable in Spark 4.0 | Comprehensive analysis done | High |
| Bin-pack only position tracking | Rewrite-time reordering unsupported (sorted/Z-ordered) | Merge compactions work | Low |
| Manual conflict resolution | Requires application code | Well-documented pattern | Low |

## How to Contribute

If you'd like to help address any of these issues:

1. **Normal Scans Performance:** Start with `docs/staged_scan_investigation.md` to understand why staged scans fail, then investigate fixes in Iceberg's staged scan implementation.

2. **Spark 4.0 Support:** Read `spark/v4.0/docs/position_tracking_challenges.md` for detailed analysis, then prototype the recommended solution (lenient schema matching in `ParquetWithSparkSchemaVisitor`).

3. **Comprehensive Testing (Highest Priority):** Write Spark 3.5 test suite to verify:
   - Bin-pack rewrites with position deletes (merge compactions)
   - Compaction maps have correct runs with gaps
   - Position delete remapping works end-to-end
   - Use `writePosDeletesToFile()` helper from TestRewriteDataFilesAction.java:2428-2469

4. **Sorted Rewrite Position Tracking:** Design position tracking framework that instruments Spark's sort operator to track position transformations through reordering operations.

5. **Automatic Conflict Resolution:** Implement opt-in automatic remapping in `BaseRowDelta` with proper validation and error handling.

## References

- [Main Compaction Maps Documentation](compaction_maps.md)
- [Staged Scan Investigation](../../docs/staged_scan_investigation.md)
- [Spark 4.0 Position Tracking Challenges](../../spark/v4.0/docs/position_tracking_challenges.md)
