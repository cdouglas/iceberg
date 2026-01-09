# Spark 4.0 Position Tracking Challenges

## Status: INCOMPLETE

Position tracking for compaction maps is **fully implemented and working in Spark 3.5**, but **incomplete in Spark 4.0** due to architectural differences in how Spark 4.0 handles schema matching during file writer creation.

## The Problem

When position tracking is enabled, the DataFrame includes metadata columns `_file` and `_pos`:
```
DataFrame schema: [id, data, _file, _pos]  (4 columns)
Iceberg writeSchema: [id, data]            (2 columns)
```

These metadata columns need to:
1. Be present in rows so `PositionTrackingDataWriter` can extract them
2. NOT be written to data files (only data columns should be written)

## Spark 3.5 Solution (Working)

In Spark 3.5, file writers are lenient about extra columns:
- Pass full row with [id, data, _file, _pos] to writer
- Writer's schema (writeSchema) only has [id, data]
- Writer ignores the extra columns and only writes data columns
- **This works perfectly in Spark 3.5**

See: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/PositionTrackingDataWriter.java`

## Spark 4.0 Blocker (Incomplete)

Spark 4.0 has stricter schema validation during writer creation:
- `ParquetWithSparkSchemaVisitor.visitFields()` validates that dsSchema and Parquet schema match exactly
- When dsSchema has 4 fields but Parquet schema has 2, it throws `IndexOutOfBoundsException`
- Error occurs during writer creation, before any rows are written
- This happens at line 196 in `ParquetWithSparkSchemaVisitor.java`

### Stack Trace
```
java.lang.IndexOutOfBoundsException: Index 2 out of bounds for length 2
    at org.apache.iceberg.spark.data.ParquetWithSparkSchemaVisitor.visitFields(ParquetWithSparkSchemaVisitor.java:196)
    at org.apache.iceberg.spark.data.SparkParquetWriters.buildWriter(SparkParquetWriters.java:81)
    at org.apache.iceberg.spark.source.SparkFileWriterFactory.lambda$configureDataWrite$3(SparkFileWriterFactory.java:117)
```

## Attempted Solutions

### Attempt 1: Filter dsSchema Before File Writers
**Approach:** Remove `_file` and `_pos` from dsSchema before passing to file writers.

**Code Location:** `SparkWriteBuilder.build()` at line 144-151

**Issue:** This filters dsSchema correctly, but then `PositionTrackingDataWriter` needs the original schema to find `_file` and `_pos` column ordinals. We tried passing `originalSchema` separately to `WriterFactory`, but this added significant complexity.

**Result:** Compilation succeeded, but architectural complexity increased significantly.

### Attempt 2: ProjectedInternalRow Wrapper
**Approach:** Keep full dsSchema, use `ProjectedInternalRow` to hide trailing columns during write.

**Code Location:** `PositionTrackingDataWriter.java` (commented out ProjectedInternalRow class)

**Issue:** The projection happens at row write time, but schema validation happens at writer creation time. By the time `ProjectedInternalRow.numFields()` is called, the writer has already failed validation.

**Result:** Same IndexOutOfBoundsException.

### Attempt 3: Pass Full Row (Spark 3.5 Approach)
**Approach:** Just pass the full row and rely on writer to ignore extra columns.

**Code Location:** `PositionTrackingDataWriter.write()` at line 103-106

**Issue:** Works in Spark 3.5 but not Spark 4.0 due to stricter validation.

**Result:** IndexOutOfBoundsException during writer creation.

## Potential Solutions (Not Yet Attempted)

### Option 1: Modify ParquetWithSparkSchemaVisitor (Most Promising)
Update `ParquetWithSparkSchemaVisitor.visitFields()` to be lenient about trailing columns in dsSchema that aren't in the Parquet schema. This would align Spark 4.0 behavior with Spark 3.5.

**Pros:**
- Minimal code changes
- Aligns with Spark 3.5 behavior
- Clean separation: dsSchema can have metadata, writeSchema controls what gets written

**Cons:**
- Modifies core Iceberg-Spark integration code
- May have unintended side effects on other Spark 4.0 features

**Implementation:**
Modify `ParquetWithSparkSchemaVisitor.visitFields()` around line 194-196 to skip fields in `struct` that exceed `group.getFieldCount()`.

### Option 2: Custom Metadata Column Mechanism
Instead of using Spark's standard column mechanism, implement a custom metadata system that doesn't affect DataFrame schema.

**Pros:**
- Doesn't require modifying core visitor code
- Clean separation of concerns

**Cons:**
- Significant implementation effort
- Would need custom reader/writer integration
- May not integrate cleanly with Spark's query planning

### Option 3: Two-Phase Writer Wrapping
Create a custom writer wrapper that presents a filtered schema to the underlying Parquet writer but operates on full rows internally.

**Pros:**
- Isolated change
- Doesn't affect core code

**Cons:**
- Complex implementation
- May have performance overhead
- Requires deep understanding of Spark's writer internals

## Recommended Next Steps

1. **Investigate ParquetWithSparkSchemaVisitor modification** (Option 1)
   - Check if other Spark 4.0 features rely on strict field count matching
   - Create a test case to validate the change doesn't break existing functionality
   - Potentially make this configurable with a flag

2. **Consult Iceberg Community**
   - Open a GitHub issue describing the Spark 4.0 challenge
   - Ask if there's an existing pattern for metadata columns that shouldn't be written
   - Get feedback on modifying ParquetWithSparkSchemaVisitor

3. **Prototype Option 1**
   - Implement lenient field matching in visitFields()
   - Test with position tracking enabled
   - Validate no regressions in existing tests

## Code State in v4.0

The current v4.0 code has:
- ✅ `PositionTrackingDataWriter` class (mirrors v3.5)
- ✅ `SparkWriteBuilder` with position tracking logic
- ✅ `SparkBinPackFileRewriteRunner` with normal scan path
- ❌ Working end-to-end position tracking (blocked by schema validation)

Debug output is left in place (commented) at key points to aid future debugging:
- `SparkWriteBuilder.build()` - schema filtering logic
- `SparkWrite.WriterFactory.createWriter()` - writer creation
- `SparkFileWriterFactory` constructor - schema validation

## References

- Working v3.5 implementation: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/PositionTrackingDataWriter.java`
- Blocked v4.0 implementation: `spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/source/PositionTrackingDataWriter.java`
- Parquet visitor error: `spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/data/ParquetWithSparkSchemaVisitor.java:196`
- Plan document: `/home/chris/.claude/plans/squishy-rolling-cat.md`

## Testing

To test when fixed:
```bash
./gradlew :iceberg-spark:iceberg-spark-4.0_2.13:test --tests "TestBinPackWithPositionTracking.testBinPackGeneratesCompactionMapWithoutDeletes"
```

Expected: Test passes with compaction map generated correctly.
Current: IndexOutOfBoundsException during writer creation.
