# Compaction Maps Implementation Plan

**Target**: Apache Iceberg 1.10.x branch
**Approach**: Alternative 1 - Manifest-Level Metadata (Field ID 521)
**Estimated Timeline**: 8 weeks (6 weeks development + 2 weeks testing/refinement)

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Phase 1: Data Structures and Schema](#phase-1-data-structures-and-schema)
3. [Phase 2: Map Generation Infrastructure](#phase-2-map-generation-infrastructure)
4. [Phase 3: Map Storage and Retrieval](#phase-3-map-storage-and-retrieval)
5. [Phase 4: Transaction Integration](#phase-4-transaction-integration)
6. [Phase 5: Compaction Integration](#phase-5-compaction-integration)
7. [Phase 6: Testing Strategy](#phase-6-testing-strategy)
8. [Phase 7: Edge Cases and Optimization](#phase-7-edge-cases-and-optimization)
9. [Phase 8: Documentation and Examples](#phase-8-documentation-and-examples)

---

## Architecture Overview

### Data Flow: Map Generation (Compaction Path)

```
┌─────────────────────────────────────────────────────────┐
│ RewriteDataFilesSparkAction.execute()                   │
│                                                          │
│ 1. Plan file groups to compact                          │
│ 2. For each group:                                       │
│    ├─ Read source files with TrackingIterator           │
│    ├─ CompactionMapBuilder tracks positions             │
│    ├─ Write compacted files                             │
│    └─ CompactionMapBuilder.build() → CompactionMap      │
│                                                          │
│ 3. CompactionMapWriter.write(map) → file storage        │
│ 4. Set manifest.compactionMapLocation(path)             │
│ 5. Commit with RewriteFiles                             │
└─────────────────────────────────────────────────────────┘
```

### Data Flow: Map Consumption (Transaction Path)

```
┌─────────────────────────────────────────────────────────┐
│ Transaction.commit() with position deletes               │
│                                                          │
│ 1. validateDataFilesExist() called                      │
│ 2. validationHistory() reads manifest lists             │
│ 3. For each ManifestFile:                               │
│    if (manifest.compactionMapLocation() != null):       │
│       ├─ Load CompactionMap from storage                │
│       ├─ Check if transaction's deletes reference        │
│       │   files affected by this compaction             │
│       └─ If yes: trigger remapping                      │
│                                                          │
│ 4. PositionDeleteRemapper:                              │
│    ├─ Read old position delete files                    │
│    ├─ Apply compaction map transformations              │
│    ├─ Write new position delete files                   │
│    └─ Update transaction to use new delete files        │
│                                                          │
│ 5. Retry commit with remapped deletes                   │
└─────────────────────────────────────────────────────────┘
```

---

## Phase 1: Data Structures and Schema

**Duration**: 1 week
**Dependencies**: None
**Branch**: `feature/compaction-maps-schema`

### Task 1.1: Define CompactionMap Data Structure

**File**: `api/src/main/java/org/apache/iceberg/CompactionMap.java` (NEW)

```java
package org.apache.iceberg;

import java.io.Serializable;
import java.util.List;

/**
 * A compaction map describes how position references in data files
 * are transformed during a compaction operation.
 *
 * <p>This map enables concurrent transactions to remap their position
 * deletes when the data layout changes due to compaction.
 */
public interface CompactionMap extends Serializable {

  /** Returns the snapshot ID before compaction. */
  long sourceSnapshotId();

  /** Returns the snapshot ID after compaction. */
  long targetSnapshotId();

  /** Returns the list of file mappings in this compaction. */
  List<FileMapping> fileMappings();

  /** Returns the mapping for a specific source file path, or null if not found. */
  FileMapping mappingForFile(String sourceFilePath);

  /**
   * Represents the mapping for a single data file that was compacted.
   */
  interface FileMapping extends Serializable {
    /** Source file path (pre-compaction). */
    String sourceFile();

    /** Target file path (post-compaction). */
    String targetFile();

    /** List of position mapping runs. */
    List<Run> runs();

    /**
     * Returns the Run containing the given source position, or null if not found.
     */
    Run runForPosition(long sourcePosition);
  }

  /**
   * Represents a contiguous run of rows mapped from source to target.
   *
   * <p>A run describes that rows at positions [sourcePosition, sourcePosition + length)
   * in the source file are mapped to [targetPosition, targetPosition + length)
   * in the target file.
   */
  interface Run extends Serializable {
    /** Starting position in source file. */
    long sourcePosition();

    /** Starting position in target file. */
    long targetPosition();

    /** Number of rows in this run. */
    long length();

    /**
     * Given a position in the source file (must be within this run),
     * returns the corresponding position in the target file.
     */
    default long mapPosition(long sourcePos) {
      if (sourcePos < sourcePosition() || sourcePos >= sourcePosition() + length()) {
        throw new IllegalArgumentException(
            String.format("Position %d is not within run [%d, %d)",
                sourcePos, sourcePosition(), sourcePosition() + length()));
      }
      return targetPosition() + (sourcePos - sourcePosition());
    }
  }
}
```

**Tests**: None yet (interface only)

---

### Task 1.2: Implement Mutable CompactionMap Builder

**File**: `core/src/main/java/org/apache/iceberg/BaseCompactionMap.java` (NEW)

```java
package org.apache.iceberg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

class BaseCompactionMap implements CompactionMap {
  private final long sourceSnapshotId;
  private final long targetSnapshotId;
  private final List<FileMapping> fileMappings;
  private final Map<String, FileMapping> fileMappingIndex;

  private BaseCompactionMap(
      long sourceSnapshotId,
      long targetSnapshotId,
      List<FileMapping> fileMappings) {
    this.sourceSnapshotId = sourceSnapshotId;
    this.targetSnapshotId = targetSnapshotId;
    this.fileMappings = ImmutableList.copyOf(fileMappings);

    // Build index for fast lookup
    this.fileMappingIndex = new HashMap<>();
    for (FileMapping mapping : fileMappings) {
      fileMappingIndex.put(mapping.sourceFile(), mapping);
    }
  }

  @Override
  public long sourceSnapshotId() {
    return sourceSnapshotId;
  }

  @Override
  public long targetSnapshotId() {
    return targetSnapshotId;
  }

  @Override
  public List<FileMapping> fileMappings() {
    return fileMappings;
  }

  @Override
  public FileMapping mappingForFile(String sourceFilePath) {
    return fileMappingIndex.get(sourceFilePath);
  }

  static class Builder {
    private long sourceSnapshotId;
    private long targetSnapshotId;
    private final List<FileMapping> fileMappings = new ArrayList<>();

    Builder sourceSnapshotId(long snapshotId) {
      this.sourceSnapshotId = snapshotId;
      return this;
    }

    Builder targetSnapshotId(long snapshotId) {
      this.targetSnapshotId = snapshotId;
      return this;
    }

    Builder addFileMapping(FileMapping mapping) {
      this.fileMappings.add(mapping);
      return this;
    }

    CompactionMap build() {
      Preconditions.checkArgument(sourceSnapshotId > 0, "Source snapshot ID required");
      Preconditions.checkArgument(targetSnapshotId > 0, "Target snapshot ID required");
      Preconditions.checkArgument(!fileMappings.isEmpty(), "At least one file mapping required");
      return new BaseCompactionMap(sourceSnapshotId, targetSnapshotId, fileMappings);
    }
  }

  static class FileMappingImpl implements FileMapping {
    private final String sourceFile;
    private final String targetFile;
    private final List<Run> runs;

    FileMappingImpl(String sourceFile, String targetFile, List<Run> runs) {
      this.sourceFile = sourceFile;
      this.targetFile = targetFile;
      this.runs = ImmutableList.copyOf(runs);
    }

    @Override
    public String sourceFile() {
      return sourceFile;
    }

    @Override
    public String targetFile() {
      return targetFile;
    }

    @Override
    public List<Run> runs() {
      return runs;
    }

    @Override
    public Run runForPosition(long sourcePosition) {
      // Binary search would be more efficient for large run lists
      for (Run run : runs) {
        if (sourcePosition >= run.sourcePosition() &&
            sourcePosition < run.sourcePosition() + run.length()) {
          return run;
        }
      }
      return null;
    }
  }

  static class RunImpl implements Run {
    private final long sourcePosition;
    private final long targetPosition;
    private final long length;

    RunImpl(long sourcePosition, long targetPosition, long length) {
      this.sourcePosition = sourcePosition;
      this.targetPosition = targetPosition;
      this.length = length;
    }

    @Override
    public long sourcePosition() {
      return sourcePosition;
    }

    @Override
    public long targetPosition() {
      return targetPosition;
    }

    @Override
    public long length() {
      return length;
    }
  }
}
```

**Tests**: `TestBaseCompactionMap.java` (see Phase 6)

---

### Task 1.3: Add compaction_map_location to ManifestFile Schema

**File**: `api/src/main/java/org/apache/iceberg/ManifestFile.java`

**Changes**:

```java
// After KEY_METADATA (line 90), before FIRST_ROW_ID (line 92):
Types.NestedField COMPACTION_MAP_LOCATION =
    optional(521, "compaction_map_location", Types.StringType.get(),
             "Location of compaction map file for this manifest");
Types.NestedField FIRST_ROW_ID =
    optional(520, "first_row_id", Types.LongType.get(),
             "Starting row ID to assign to new rows in ADDED data files");
// next ID to assign: 522

// Update SCHEMA (line 100):
Schema SCHEMA =
    new Schema(
        PATH,
        LENGTH,
        SPEC_ID,
        MANIFEST_CONTENT,
        SEQUENCE_NUMBER,
        MIN_SEQUENCE_NUMBER,
        SNAPSHOT_ID,
        ADDED_FILES_COUNT,
        EXISTING_FILES_COUNT,
        DELETED_FILES_COUNT,
        ADDED_ROWS_COUNT,
        EXISTING_ROWS_COUNT,
        DELETED_ROWS_COUNT,
        PARTITION_SUMMARIES,
        KEY_METADATA,
        COMPACTION_MAP_LOCATION,  // NEW - before FIRST_ROW_ID
        FIRST_ROW_ID);

// Add accessor method (after firstRowId(), around line 210):
/**
 * Returns the location of the compaction map file for this manifest, or null
 * if this manifest was not created by a compaction operation.
 */
default String compactionMapLocation() {
  return null;
}
```

**Tests**: None (schema definition)

---

### Task 1.4: Update GenericManifestFile Implementation

**File**: `core/src/main/java/org/apache/iceberg/GenericManifestFile.java`

**Changes**:

```java
// Line 64: Add field after keyMetadata
private byte[] keyMetadata = null;
private String compactionMapLocation = null;  // NEW
private Long firstRowId = null;

// Line 96: Initialize in constructor
this.keyMetadata = null;
this.compactionMapLocation = null;  // NEW
this.firstRowId = null;

// Line 100-116: Update full constructor signature and body
GenericManifestFile(
    String path,
    long length,
    int specId,
    ManifestContent content,
    long sequenceNumber,
    long minSequenceNumber,
    Long snapshotId,
    List<PartitionFieldSummary> partitions,
    ByteBuffer keyMetadata,
    String compactionMapLocation,  // NEW parameter
    Integer addedFilesCount,
    Long addedRowsCount,
    Integer existingFilesCount,
    Long existingRowsCount,
    Integer deletedFilesCount,
    Long deletedRowsCount,
    Long firstRowId) {
  super(ManifestFile.schema().columns().size());
  this.avroSchema = AVRO_SCHEMA;
  this.manifestPath = path;
  this.length = length;
  this.specId = specId;
  this.content = content;
  this.sequenceNumber = sequenceNumber;
  this.minSequenceNumber = minSequenceNumber;
  this.snapshotId = snapshotId;
  this.addedFilesCount = addedFilesCount;
  this.addedRowsCount = addedRowsCount;
  this.existingFilesCount = existingFilesCount;
  this.existingRowsCount = existingRowsCount;
  this.deletedFilesCount = deletedFilesCount;
  this.deletedRowsCount = deletedRowsCount;
  this.partitions = partitions == null ? null : partitions.toArray(new PartitionFieldSummary[0]);
  this.keyMetadata = ByteBuffers.toByteArray(keyMetadata);
  this.compactionMapLocation = compactionMapLocation;  // NEW
  this.firstRowId = firstRowId;
}

// Line 176: Update copy constructor
this.keyMetadata =
    toCopy.keyMetadata == null
        ? null
        : Arrays.copyOf(toCopy.keyMetadata, toCopy.keyMetadata.length);
this.compactionMapLocation = toCopy.compactionMapLocation;  // NEW
this.firstRowId = toCopy.firstRowId;

// Add accessor method (after firstRowId() accessor):
@Override
public String compactionMapLocation() {
  return compactionMapLocation;
}

// Update StructLike get() method to handle field 521
// (in the switch statement or if-else chain that maps field IDs to values)
```

**Tests**:
- `TestGenericManifestFile.java` - Verify field read/write
- Backward compatibility test (see Phase 6)

---

### Task 1.5: Update ManifestFile Builders

**Files to Update**:
- `core/src/main/java/org/apache/iceberg/ManifestFiles.java` - Update builder patterns
- Any other manifest file creation utilities

**Changes**: Add `compactionMapLocation` parameter to builder methods, default to null.

**Tests**: Verify existing tests still pass with null values.

---

## Phase 2: Map Generation Infrastructure

**Duration**: 1.5 weeks
**Dependencies**: Phase 1
**Branch**: `feature/compaction-maps-generation`

### Task 2.1: Implement CompactionMapBuilder

**File**: `core/src/main/java/org/apache/iceberg/CompactionMapBuilder.java` (NEW)

```java
package org.apache.iceberg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder for creating CompactionMap during data file rewrites.
 *
 * <p>Tracks position mappings as rows are read from source files
 * and written to target files.
 *
 * <p>Usage:
 * <pre>
 * CompactionMapBuilder builder = new CompactionMapBuilder(sourceSnapshotId, targetSnapshotId);
 *
 * // For each source file being read:
 * FileTracker tracker = builder.startSourceFile(sourceFilePath);
 * while (hasMoreRows) {
 *   Row row = readRow();  // sourcePosition increments
 *   tracker.recordSourcePosition(sourcePosition);
 * }
 *
 * // For each target file being written:
 * builder.startTargetFile(targetFilePath);
 * for (Row row : rowsToWrite) {
 *   String sourceFile = getSourceFile(row);
 *   long sourcePosition = getSourcePosition(row);
 *   builder.recordMapping(sourceFile, sourcePosition, targetPosition);
 *   targetPosition++;
 * }
 *
 * CompactionMap map = builder.build();
 * </pre>
 */
public class CompactionMapBuilder {
  private final long sourceSnapshotId;
  private final long targetSnapshotId;
  private final Map<String, FileMappingBuilder> fileMappings = new HashMap<>();

  public CompactionMapBuilder(long sourceSnapshotId, long targetSnapshotId) {
    this.sourceSnapshotId = sourceSnapshotId;
    this.targetSnapshotId = targetSnapshotId;
  }

  /**
   * Records a mapping from a source position to a target position.
   *
   * @param sourceFile the source file path
   * @param sourcePosition the position in the source file
   * @param targetFile the target file path
   * @param targetPosition the position in the target file
   */
  public void recordMapping(
      String sourceFile,
      long sourcePosition,
      String targetFile,
      long targetPosition) {

    FileMappingBuilder mappingBuilder =
        fileMappings.computeIfAbsent(sourceFile, k -> new FileMappingBuilder(sourceFile));

    mappingBuilder.addMapping(sourcePosition, targetFile, targetPosition);
  }

  /**
   * Builds the final CompactionMap.
   *
   * <p>This consolidates consecutive position mappings into runs for efficiency.
   */
  public CompactionMap build() {
    BaseCompactionMap.Builder mapBuilder = new BaseCompactionMap.Builder()
        .sourceSnapshotId(sourceSnapshotId)
        .targetSnapshotId(targetSnapshotId);

    for (FileMappingBuilder mappingBuilder : fileMappings.values()) {
      mapBuilder.addFileMapping(mappingBuilder.build());
    }

    return mapBuilder.build();
  }

  private static class FileMappingBuilder {
    private final String sourceFile;
    private final Map<String, List<PositionPair>> targetFileMappings = new HashMap<>();

    FileMappingBuilder(String sourceFile) {
      this.sourceFile = sourceFile;
    }

    void addMapping(long sourcePosition, String targetFile, long targetPosition) {
      targetFileMappings
          .computeIfAbsent(targetFile, k -> new ArrayList<>())
          .add(new PositionPair(sourcePosition, targetPosition));
    }

    /**
     * Builds the file mapping by consolidating consecutive positions into runs.
     *
     * <p>Note: This assumes a single source file maps to a single target file.
     * If source file is split across multiple targets, this creates multiple
     * FileMapping entries (one per target file).
     */
    CompactionMap.FileMapping build() {
      // For now, assume single target file (most common case)
      // TODO: Handle source file split across multiple targets

      if (targetFileMappings.size() != 1) {
        throw new UnsupportedOperationException(
            "Source file mapped to multiple targets not yet supported: " + sourceFile);
      }

      Map.Entry<String, List<PositionPair>> entry =
          targetFileMappings.entrySet().iterator().next();
      String targetFile = entry.getKey();
      List<PositionPair> positions = entry.getValue();

      // Sort by source position
      positions.sort((a, b) -> Long.compare(a.sourcePosition, b.sourcePosition));

      // Consolidate into runs
      List<CompactionMap.Run> runs = new ArrayList<>();
      long runStartSource = -1;
      long runStartTarget = -1;
      long runLength = 0;

      for (PositionPair pair : positions) {
        if (runStartSource == -1) {
          // Start new run
          runStartSource = pair.sourcePosition;
          runStartTarget = pair.targetPosition;
          runLength = 1;
        } else if (pair.sourcePosition == runStartSource + runLength &&
                   pair.targetPosition == runStartTarget + runLength) {
          // Extend current run
          runLength++;
        } else {
          // Finish current run, start new one
          runs.add(new BaseCompactionMap.RunImpl(runStartSource, runStartTarget, runLength));
          runStartSource = pair.sourcePosition;
          runStartTarget = pair.targetPosition;
          runLength = 1;
        }
      }

      // Add final run
      if (runStartSource != -1) {
        runs.add(new BaseCompactionMap.RunImpl(runStartSource, runStartTarget, runLength));
      }

      return new BaseCompactionMap.FileMappingImpl(sourceFile, targetFile, runs);
    }
  }

  private static class PositionPair {
    final long sourcePosition;
    final long targetPosition;

    PositionPair(long sourcePosition, long targetPosition) {
      this.sourcePosition = sourcePosition;
      this.targetPosition = targetPosition;
    }
  }
}
```

**Tests**: `TestCompactionMapBuilder.java` (see Phase 6)

---

### Task 2.2: Create Position-Tracking Iterator Wrapper

**File**: `core/src/main/java/org/apache/iceberg/io/PositionTrackingIterator.java` (NEW)

```java
package org.apache.iceberg.io;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Wraps an iterator to track the current position for compaction map generation.
 *
 * <p>This iterator invokes a callback with each position as elements are consumed.
 */
public class PositionTrackingIterator<T> implements Iterator<T> {
  private final Iterator<T> inner;
  private final Consumer<Long> positionCallback;
  private long currentPosition;

  public PositionTrackingIterator(Iterator<T> inner, Consumer<Long> positionCallback) {
    this.inner = inner;
    this.positionCallback = positionCallback;
    this.currentPosition = 0;
  }

  @Override
  public boolean hasNext() {
    return inner.hasNext();
  }

  @Override
  public T next() {
    T element = inner.next();
    positionCallback.accept(currentPosition);
    currentPosition++;
    return element;
  }

  public long getCurrentPosition() {
    return currentPosition;
  }
}
```

**Tests**: `TestPositionTrackingIterator.java`

---

### Task 2.3: Implement Compaction Context

**File**: `core/src/main/java/org/apache/iceberg/CompactionContext.java` (NEW)

```java
package org.apache.iceberg;

/**
 * Thread-local context for tracking compaction operations.
 *
 * <p>This allows compaction map builders to be passed through the
 * rewrite operation without changing all method signatures.
 */
public class CompactionContext {
  private static final ThreadLocal<CompactionMapBuilder> BUILDER = new ThreadLocal<>();

  /**
   * Sets the compaction map builder for the current thread.
   *
   * <p>This should be called at the start of a compaction operation.
   */
  public static void setBuilder(CompactionMapBuilder builder) {
    BUILDER.set(builder);
  }

  /**
   * Gets the compaction map builder for the current thread, or null if not set.
   */
  public static CompactionMapBuilder getBuilder() {
    return BUILDER.get();
  }

  /**
   * Clears the compaction map builder for the current thread.
   *
   * <p>This should be called after compaction completes or fails.
   */
  public static void clear() {
    BUILDER.remove();
  }

  /**
   * Returns true if a compaction is currently active on this thread.
   */
  public static boolean isActive() {
    return BUILDER.get() != null;
  }

  /**
   * Records a position mapping if a compaction is active.
   */
  public static void recordMapping(
      String sourceFile, long sourcePosition,
      String targetFile, long targetPosition) {
    CompactionMapBuilder builder = BUILDER.get();
    if (builder != null) {
      builder.recordMapping(sourceFile, sourcePosition, targetFile, targetPosition);
    }
  }
}
```

**Tests**: `TestCompactionContext.java`

---

## Phase 3: Map Storage and Retrieval

**Duration**: 1 week
**Dependencies**: Phase 1, Phase 2
**Branch**: `feature/compaction-maps-storage`

### Task 3.1: Define CompactionMap File Format

**Format**: Avro (consistent with Iceberg metadata)

**Schema**: Define in `api/src/main/java/org/apache/iceberg/CompactionMapFormat.java` (NEW)

```java
package org.apache.iceberg;

import org.apache.iceberg.types.Types;

/**
 * Schema for compaction map files.
 */
public class CompactionMapFormat {

  private CompactionMapFormat() {}

  // CompactionMap record
  public static final Types.NestedField SOURCE_SNAPSHOT_ID =
      Types.NestedField.required(1, "source_snapshot_id", Types.LongType.get());
  public static final Types.NestedField TARGET_SNAPSHOT_ID =
      Types.NestedField.required(2, "target_snapshot_id", Types.LongType.get());
  public static final Types.NestedField FILE_MAPPINGS =
      Types.NestedField.required(3, "file_mappings",
          Types.ListType.ofRequired(4, fileMappingType()));

  // FileMapping record
  private static Types.StructType fileMappingType() {
    return Types.StructType.of(
        Types.NestedField.required(5, "source_file", Types.StringType.get()),
        Types.NestedField.required(6, "target_file", Types.StringType.get()),
        Types.NestedField.required(7, "runs", Types.ListType.ofRequired(8, runType()))
    );
  }

  // Run record
  private static Types.StructType runType() {
    return Types.StructType.of(
        Types.NestedField.required(9, "source_position", Types.LongType.get()),
        Types.NestedField.required(10, "target_position", Types.LongType.get()),
        Types.NestedField.required(11, "length", Types.LongType.get())
    );
  }

  public static Schema schema() {
    return new Schema(SOURCE_SNAPSHOT_ID, TARGET_SNAPSHOT_ID, FILE_MAPPINGS);
  }
}
```

---

### Task 3.2: Implement CompactionMapWriter

**File**: `core/src/main/java/org/apache/iceberg/CompactionMapWriter.java` (NEW)

```java
package org.apache.iceberg;

import java.io.IOException;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.OutputFile;

/**
 * Writes a CompactionMap to storage in Avro format.
 */
public class CompactionMapWriter {

  private CompactionMapWriter() {}

  /**
   * Writes a compaction map to the specified output file.
   *
   * @param map the compaction map to write
   * @param output the output file
   * @throws IOException if writing fails
   */
  public static void write(CompactionMap map, OutputFile output) throws IOException {
    try (org.apache.iceberg.avro.AvroIterable.Writer<CompactionMap> writer =
        Avro.write(output)
            .schema(CompactionMapFormat.schema())
            .createWriterFunc(ignored -> new CompactionMapAvroWriter())
            .build()) {
      writer.write(map);
    }
  }

  /**
   * Custom Avro writer for CompactionMap objects.
   */
  private static class CompactionMapAvroWriter
      implements org.apache.iceberg.avro.ValueWriter<CompactionMap> {

    @Override
    public void write(CompactionMap map, org.apache.avro.io.Encoder encoder)
        throws IOException {
      // Write source_snapshot_id
      encoder.writeLong(map.sourceSnapshotId());

      // Write target_snapshot_id
      encoder.writeLong(map.targetSnapshotId());

      // Write file_mappings array
      encoder.writeArrayStart();
      encoder.setItemCount(map.fileMappings().size());
      for (CompactionMap.FileMapping mapping : map.fileMappings()) {
        encoder.startItem();
        writeFileMapping(mapping, encoder);
      }
      encoder.writeArrayEnd();
    }

    private void writeFileMapping(CompactionMap.FileMapping mapping,
                                   org.apache.avro.io.Encoder encoder)
        throws IOException {
      // Write source_file
      encoder.writeString(mapping.sourceFile());

      // Write target_file
      encoder.writeString(mapping.targetFile());

      // Write runs array
      encoder.writeArrayStart();
      encoder.setItemCount(mapping.runs().size());
      for (CompactionMap.Run run : mapping.runs()) {
        encoder.startItem();
        writeRun(run, encoder);
      }
      encoder.writeArrayEnd();
    }

    private void writeRun(CompactionMap.Run run, org.apache.avro.io.Encoder encoder)
        throws IOException {
      encoder.writeLong(run.sourcePosition());
      encoder.writeLong(run.targetPosition());
      encoder.writeLong(run.length());
    }
  }
}
```

**Tests**: `TestCompactionMapWriter.java`

---

### Task 3.3: Implement CompactionMapReader

**File**: `core/src/main/java/org/apache/iceberg/CompactionMapReader.java` (NEW)

```java
package org.apache.iceberg;

import java.io.IOException;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;

/**
 * Reads a CompactionMap from storage.
 */
public class CompactionMapReader {

  private CompactionMapReader() {}

  /**
   * Reads a compaction map from the specified input file.
   *
   * @param input the input file
   * @return the compaction map
   * @throws IOException if reading fails
   */
  public static CompactionMap read(InputFile input) throws IOException {
    try (CloseableIterable<CompactionMap> reader =
        Avro.read(input)
            .project(CompactionMapFormat.schema())
            .createReaderFunc(ignored -> new CompactionMapAvroReader())
            .build()) {

      // CompactionMap file should contain exactly one record
      return reader.iterator().next();
    }
  }

  /**
   * Custom Avro reader for CompactionMap objects.
   */
  private static class CompactionMapAvroReader
      implements org.apache.iceberg.avro.ValueReader<CompactionMap> {

    @Override
    public CompactionMap read(org.apache.avro.io.Decoder decoder, Object reuse)
        throws IOException {
      // Read source_snapshot_id
      long sourceSnapshotId = decoder.readLong();

      // Read target_snapshot_id
      long targetSnapshotId = decoder.readLong();

      // Read file_mappings array
      BaseCompactionMap.Builder builder = new BaseCompactionMap.Builder()
          .sourceSnapshotId(sourceSnapshotId)
          .targetSnapshotId(targetSnapshotId);

      long arrayLength = decoder.readArrayStart();
      while (arrayLength > 0) {
        for (long i = 0; i < arrayLength; i++) {
          builder.addFileMapping(readFileMapping(decoder));
        }
        arrayLength = decoder.arrayNext();
      }

      return builder.build();
    }

    private CompactionMap.FileMapping readFileMapping(org.apache.avro.io.Decoder decoder)
        throws IOException {
      String sourceFile = decoder.readString();
      String targetFile = decoder.readString();

      List<CompactionMap.Run> runs = new ArrayList<>();
      long arrayLength = decoder.readArrayStart();
      while (arrayLength > 0) {
        for (long i = 0; i < arrayLength; i++) {
          runs.add(readRun(decoder));
        }
        arrayLength = decoder.arrayNext();
      }

      return new BaseCompactionMap.FileMappingImpl(sourceFile, targetFile, runs);
    }

    private CompactionMap.Run readRun(org.apache.avro.io.Decoder decoder)
        throws IOException {
      long sourcePosition = decoder.readLong();
      long targetPosition = decoder.readLong();
      long length = decoder.readLong();

      return new BaseCompactionMap.RunImpl(sourcePosition, targetPosition, length);
    }
  }
}
```

**Tests**: `TestCompactionMapReader.java`, round-trip tests

---

### Task 3.4: Add Table Property for Map Storage Location

**File**: `api/src/main/java/org/apache/iceberg/TableProperties.java`

**Add constants**:

```java
/**
 * Whether to generate compaction maps during data file rewrites.
 * Default: true
 */
public static final String COMPACTION_MAPS_ENABLED = "write.compaction-maps.enabled";
public static final boolean COMPACTION_MAPS_ENABLED_DEFAULT = true;

/**
 * Location to store compaction map files.
 * Default: {table-location}/metadata/compaction-maps/
 */
public static final String COMPACTION_MAPS_LOCATION = "write.compaction-maps.location";
```

---

## Phase 4: Transaction Integration

**Duration**: 1.5 weeks
**Dependencies**: Phase 1, 2, 3
**Branch**: `feature/compaction-maps-transactions`

### Task 4.1: Implement PositionDeleteRemapper

**File**: `core/src/main/java/org/apache/iceberg/PositionDeleteRemapper.java` (NEW)

```java
package org.apache.iceberg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Remaps position delete files using compaction maps.
 *
 * <p>When a transaction writes position deletes against files that were
 * subsequently compacted, this class transforms the delete file references
 * to point to the new compacted files.
 */
public class PositionDeleteRemapper {
  private final FileIO fileIO;
  private final Map<String, CompactionMap> compactionMaps;

  public PositionDeleteRemapper(FileIO fileIO) {
    this.fileIO = fileIO;
    this.compactionMaps = new HashMap<>();
  }

  /**
   * Loads a compaction map for later use in remapping.
   *
   * @param mapLocation the location of the compaction map file
   */
  public void loadCompactionMap(String mapLocation) throws IOException {
    InputFile input = fileIO.newInputFile(mapLocation);
    CompactionMap map = CompactionMapReader.read(input);

    // Index by source file for fast lookup
    for (CompactionMap.FileMapping mapping : map.fileMappings()) {
      compactionMaps.put(mapping.sourceFile(), map);
    }
  }

  /**
   * Determines if a position delete file needs remapping.
   *
   * @param deleteFile the delete file to check
   * @return true if any referenced data files were compacted
   */
  public boolean needsRemapping(DeleteFile deleteFile) {
    // Check if any data files referenced by this delete file have compaction maps
    // Note: This requires reading the delete file to see which data files it references
    // For now, we'll be conservative and check if we have any loaded maps
    return !compactionMaps.isEmpty();
  }

  /**
   * Remaps a position delete file to reference compacted data files.
   *
   * @param deleteFile the original delete file
   * @return list of new delete files (may be multiple if deletes split across files)
   */
  public List<DeleteFile> remapDeleteFile(DeleteFile deleteFile) throws IOException {
    // Read the position deletes
    InputFile input = fileIO.newInputFile(deleteFile.location());

    // Group deletes by target file after remapping
    Map<String, List<PositionDelete<Object>>> remappedDeletes = new HashMap<>();

    try (CloseableIterable<PositionDelete<Object>> deletes =
        readPositionDeletes(input, deleteFile)) {

      for (PositionDelete<Object> delete : deletes) {
        String sourceFile = delete.path().toString();
        long sourcePosition = delete.pos();

        // Find compaction map for this file
        CompactionMap map = compactionMaps.get(sourceFile);

        if (map == null) {
          // No compaction for this file, keep original
          remappedDeletes
              .computeIfAbsent(sourceFile, k -> new ArrayList<>())
              .add(delete);
          continue;
        }

        // Find the file mapping
        CompactionMap.FileMapping mapping = map.mappingForFile(sourceFile);
        if (mapping == null) {
          throw new IllegalStateException(
              "Compaction map exists but no mapping found for file: " + sourceFile);
        }

        // Find the run containing this position
        CompactionMap.Run run = mapping.runForPosition(sourcePosition);
        if (run == null) {
          throw new IllegalStateException(
              String.format("Position %d not found in any run for file %s",
                  sourcePosition, sourceFile));
        }

        // Calculate new position
        long targetPosition = run.mapPosition(sourcePosition);
        String targetFile = mapping.targetFile();

        // Create remapped delete
        PositionDelete<Object> remapped = PositionDelete.create();
        remapped.set(targetFile, targetPosition, delete.row());

        remappedDeletes
            .computeIfAbsent(targetFile, k -> new ArrayList<>())
            .add(remapped);
      }
    }

    // Write new delete files (one per target data file)
    List<DeleteFile> newDeleteFiles = new ArrayList<>();
    for (Map.Entry<String, List<PositionDelete<Object>>> entry : remappedDeletes.entrySet()) {
      DeleteFile newDeleteFile = writePositionDeletes(entry.getValue(), deleteFile);
      newDeleteFiles.add(newDeleteFile);
    }

    return newDeleteFiles;
  }

  private CloseableIterable<PositionDelete<Object>> readPositionDeletes(
      InputFile input, DeleteFile deleteFile) throws IOException {
    // Implementation depends on file format (Parquet/ORC/Avro)
    // Use existing Iceberg readers
    throw new UnsupportedOperationException("TODO: Implement position delete reading");
  }

  private DeleteFile writePositionDeletes(
      List<PositionDelete<Object>> deletes, DeleteFile template) throws IOException {
    // Implementation depends on file format
    // Use existing Iceberg writers
    throw new UnsupportedOperationException("TODO: Implement position delete writing");
  }
}
```

**Tests**: `TestPositionDeleteRemapper.java` (see Phase 6)

---

### Task 4.2: Extend MergingSnapshotProducer with Compaction Detection

**File**: `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java`

**Add to validationHistory() method** (around line 868):

```java
private Pair<List<ManifestFile>, Set<Long>> validationHistory(
    TableMetadata base,
    Long startingSnapshotId,
    Set<String> matchingOperations,
    ManifestContent content,
    Snapshot parent) {
  List<ManifestFile> manifests = Lists.newArrayList();
  Set<Long> newSnapshots = Sets.newHashSet();
  List<String> compactionMapLocations = Lists.newArrayList();  // NEW

  Snapshot lastSnapshot = null;
  Iterable<Snapshot> snapshots =
      SnapshotUtil.ancestorsBetween(parent.snapshotId(), startingSnapshotId, base::snapshot);
  for (Snapshot currentSnapshot : snapshots) {
    lastSnapshot = currentSnapshot;

    if (matchingOperations.contains(currentSnapshot.operation())) {
      newSnapshots.add(currentSnapshot.snapshotId());
      if (content == ManifestContent.DATA) {
        for (ManifestFile manifest : currentSnapshot.dataManifests(ops().io())) {
          if (manifest.snapshotId() == currentSnapshot.snapshotId()) {
            manifests.add(manifest);

            // NEW: Collect compaction map locations
            if (manifest.compactionMapLocation() != null) {
              compactionMapLocations.add(manifest.compactionMapLocation());
            }
          }
        }
      } else {
        // ... existing delete manifest handling
      }
    }
  }

  // NEW: Store compaction maps for later use
  if (!compactionMapLocations.isEmpty()) {
    storeCompactionMapsForValidation(compactionMapLocations);
  }

  ValidationException.check(
      lastSnapshot == null || Objects.equals(lastSnapshot.parentId(), startingSnapshotId),
      "Cannot determine history between starting snapshot %s and the last known ancestor %s",
      startingSnapshotId,
      lastSnapshot != null ? lastSnapshot.snapshotId() : null);

  return Pair.of(manifests, newSnapshots);
}

// NEW: Thread-local storage for compaction maps during validation
private static final ThreadLocal<List<String>> VALIDATION_COMPACTION_MAPS = new ThreadLocal<>();

private void storeCompactionMapsForValidation(List<String> mapLocations) {
  VALIDATION_COMPACTION_MAPS.set(mapLocations);
}

protected List<String> getValidationCompactionMaps() {
  List<String> maps = VALIDATION_COMPACTION_MAPS.get();
  return maps != null ? maps : Collections.emptyList();
}

protected void clearValidationCompactionMaps() {
  VALIDATION_COMPACTION_MAPS.remove();
}
```

---

### Task 4.3: Extend BaseRowDelta with Remapping Logic

**File**: `core/src/main/java/org/apache/iceberg/BaseRowDelta.java`

**Add after validate() method** (around line 137):

```java
@Override
protected void validate(TableMetadata base, Snapshot parent) {
  if (parent != null) {
    if (startingSnapshotId != null) {
      Preconditions.checkArgument(
          SnapshotUtil.isAncestorOf(parent.snapshotId(), startingSnapshotId, base::snapshot),
          "Snapshot %s is not an ancestor of %s",
          startingSnapshotId,
          parent.snapshotId());
    }
    if (!referencedDataFiles.isEmpty()) {
      // Check for compaction maps
      List<String> compactionMaps = getValidationCompactionMaps();
      if (!compactionMaps.isEmpty()) {
        handleCompactionConflict(base, parent, compactionMaps);
      } else {
        validateDataFilesExist(
            base,
            startingSnapshotId,
            referencedDataFiles,
            !validateDeletes,
            conflictDetectionFilter,
            parent);
      }
    }

    if (validateNewDataFiles) {
      validateAddedDataFiles(base, startingSnapshotId, conflictDetectionFilter, parent);
    }

    if (validateNewDeleteFiles) {
      validateNoNewDeleteFiles(base, startingSnapshotId, conflictDetectionFilter, parent);
    }
  }
}

// NEW: Handle compaction conflict by remapping position deletes
private void handleCompactionConflict(
    TableMetadata base,
    Snapshot parent,
    List<String> compactionMapLocations) {

  try {
    LOG.info("Compaction detected, remapping position deletes from {} map(s)",
        compactionMapLocations.size());

    // Create remapper and load maps
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(ops().io());
    for (String mapLocation : compactionMapLocations) {
      remapper.loadCompactionMap(mapLocation);
    }

    // Remap any delete files that need it
    List<DeleteFile> newDeleteFiles = new ArrayList<>();
    List<DeleteFile> oldDeleteFiles = new ArrayList<>();

    for (DeleteFile deleteFile : addedDeleteFiles()) {
      if (remapper.needsRemapping(deleteFile)) {
        List<DeleteFile> remapped = remapper.remapDeleteFile(deleteFile);
        newDeleteFiles.addAll(remapped);
        oldDeleteFiles.add(deleteFile);
      }
    }

    if (!oldDeleteFiles.isEmpty()) {
      LOG.info("Remapped {} position delete files into {} new files",
          oldDeleteFiles.size(), newDeleteFiles.size());

      // Replace old delete files with remapped ones
      for (DeleteFile oldFile : oldDeleteFiles) {
        deleteInternal(oldFile);
      }
      for (DeleteFile newFile : newDeleteFiles) {
        add(newFile);
      }
    }

    // Continue with normal validation
    validateDataFilesExist(
        base,
        startingSnapshotId,
        referencedDataFiles,
        !validateDeletes,
        conflictDetectionFilter,
        parent);

  } catch (IOException e) {
    throw new RuntimeException("Failed to remap position deletes after compaction", e);
  } finally {
    clearValidationCompactionMaps();
  }
}
```

**Tests**: Integration test (see Phase 6)

---

## Phase 5: Compaction Integration

**Duration**: 1.5 weeks
**Dependencies**: Phase 1, 2, 3, 4
**Branch**: `feature/compaction-maps-integration`

### Task 5.1: Integrate Map Generation in RewriteDataFiles

**File**: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java`

**Modify doExecute() method**:

```java
@Override
public RewriteDataFiles.Result execute() {
  // Check if compaction maps are enabled
  boolean compactionMapsEnabled = PropertyUtil.propertyAsBoolean(
      table.properties(),
      TableProperties.COMPACTION_MAPS_ENABLED,
      TableProperties.COMPACTION_MAPS_ENABLED_DEFAULT);

  if (compactionMapsEnabled) {
    LOG.info("Compaction maps enabled, will generate maps during rewrite");
  }

  // ... existing planning code ...

  long startingSnapshotId = table.currentSnapshot().snapshotId();

  // Initialize compaction map builder
  CompactionMapBuilder mapBuilder = null;
  if (compactionMapsEnabled) {
    long targetSnapshotId = SnapshotIdGeneratorUtil.generateSnapshotID();
    mapBuilder = new CompactionMapBuilder(startingSnapshotId, targetSnapshotId);
    CompactionContext.setBuilder(mapBuilder);
  }

  try {
    // Execute rewrites
    RewriteDataFiles.Result result = doRewriteFiles(fileGroups);

    // Generate and store compaction map
    if (mapBuilder != null) {
      CompactionMap map = mapBuilder.build();
      String mapLocation = storeCompactionMap(map);
      attachCompactionMapToManifests(result, mapLocation);
    }

    return result;

  } finally {
    if (compactionMapsEnabled) {
      CompactionContext.clear();
    }
  }
}

private String storeCompactionMap(CompactionMap map) throws IOException {
  String mapLocation = compactionMapLocation(table, map.targetSnapshotId());
  OutputFile output = table.io().newOutputFile(mapLocation);
  CompactionMapWriter.write(map, output);
  LOG.info("Wrote compaction map to {}", mapLocation);
  return mapLocation;
}

private String compactionMapLocation(Table table, long snapshotId) {
  String baseLocation = table.properties().get(TableProperties.COMPACTION_MAPS_LOCATION);
  if (baseLocation == null) {
    baseLocation = String.format("%s/metadata/compaction-maps", table.location());
  }
  return String.format("%s/cmap-%d-%s.avro",
      baseLocation, snapshotId, UUID.randomUUID());
}

private void attachCompactionMapToManifests(
    RewriteDataFiles.Result result, String mapLocation) {
  // This requires storing the map location for use during commit
  // Store in thread-local or pass through to commit manager
  CompactionMapRegistry.setMapLocationForCommit(mapLocation);
}
```

---

### Task 5.2: Track Positions During Rewrite

**File**: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackDataRewriter.java`

**Modify doRewrite() method**:

```java
@Override
public Set<DataFile> rewrite(StructLike partition, List<FileScanTask> group) {
  // ... existing setup ...

  boolean trackingPositions = CompactionContext.isActive();

  // Read source files
  Dataset<Row> sourceData = spark.read().format("iceberg").load(paths);

  if (trackingPositions) {
    // Add source file path and position columns for tracking
    sourceData = sourceData
        .withColumn("_source_file", input_file_name())
        .withColumn("_source_position", monotonically_increasing_id());
  }

  // Bin-pack and write
  Dataset<Row> binPacked = sourceData.repartition(numOutputFiles);

  if (trackingPositions) {
    // Add target position tracking
    binPacked = binPacked
        .withColumn("_target_position", row_number().over(
            Window.partitionBy("_partition").orderBy("_source_file", "_source_position")))
        .foreachPartition(iterator -> {
          String targetFile = getCurrentOutputFile();
          long targetPosition = 0;
          while (iterator.hasNext()) {
            Row row = iterator.next();
            String sourceFile = row.getString("_source_file");
            long sourcePosition = row.getLong("_source_position");

            CompactionContext.recordMapping(sourceFile, sourcePosition,
                                           targetFile, targetPosition);
            targetPosition++;
          }
        });
  }

  // Write output
  binPacked.write()...

  // ... return data files ...
}
```

**Note**: This is simplified. Actual implementation needs careful integration with Spark's physical execution to accurately track positions.

---

### Task 5.3: Store Map Location in Manifests

**File**: `core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java`

**Modify commitOrClean() method**:

```java
@Override
public void commitOrClean(Set<DataFile> rewrittenFiles, Set<DataFile> newFiles) {
  // ... existing validation ...

  // Get compaction map location if available
  String compactionMapLocation = CompactionMapRegistry.getMapLocationForCommit();

  RewriteFiles rewriteFiles = table.newRewrite();

  // Delete old files
  for (DataFile file : rewrittenFiles) {
    rewriteFiles.deleteFile(file);
  }

  // Add new files
  for (DataFile file : newFiles) {
    rewriteFiles.addFile(file);
  }

  // NEW: Set compaction map location if present
  if (compactionMapLocation != null) {
    ((BaseRewriteFiles) rewriteFiles).setCompactionMapLocation(compactionMapLocation);
  }

  rewriteFiles.commit();

  // Clean up
  CompactionMapRegistry.clearMapLocationForCommit();
}
```

---

### Task 5.4: Pass Map Location to Manifest Writers

**File**: `core/src/main/java/org/apache/iceberg/BaseRewriteFiles.java`

**Add field and method**:

```java
class BaseRewriteFiles extends MergingSnapshotProducer<RewriteFiles> implements RewriteFiles {
  private final DataFileSet replacedDataFiles = DataFileSet.create();
  private Long startingSnapshotId = null;
  private String compactionMapLocation = null;  // NEW

  // ... existing methods ...

  // NEW: Set compaction map location
  void setCompactionMapLocation(String location) {
    this.compactionMapLocation = location;
  }

  // NEW: Override manifest creation to include map location
  @Override
  protected ManifestFile writeManifest(
      List<ManifestEntry<DataFile>> entries, int specId) throws IOException {

    // ... existing manifest writing ...

    // If we have a compaction map, attach it to the manifest
    if (compactionMapLocation != null && !entries.isEmpty()) {
      // Check if this manifest contains newly added files (from compaction)
      boolean hasAddedFiles = entries.stream()
          .anyMatch(entry -> entry.status() == ManifestEntry.Status.ADDED);

      if (hasAddedFiles) {
        // Attach compaction map location
        manifestBuilder.withCompactionMapLocation(compactionMapLocation);
      }
    }

    return manifestBuilder.build();
  }
}
```

**Note**: This requires updates to ManifestWriter to support setting compaction map location.

---

## Phase 6: Testing Strategy

**Duration**: 2 weeks
**Dependencies**: Phases 1-5
**Branch**: `feature/compaction-maps-testing`

### Unit Tests

#### Test 6.1: CompactionMap Data Structure

**File**: `core/src/test/java/org/apache/iceberg/TestBaseCompactionMap.java` (NEW)

```java
public class TestBaseCompactionMap {

  @Test
  public void testCreateSimpleMap() {
    // Test creating a map with single file mapping
    CompactionMap.Run run = new BaseCompactionMap.RunImpl(0, 0, 100);
    CompactionMap.FileMapping mapping = new BaseCompactionMap.FileMappingImpl(
        "file1.parquet", "file2.parquet", Lists.newArrayList(run));

    CompactionMap map = new BaseCompactionMap.Builder()
        .sourceSnapshotId(1L)
        .targetSnapshotId(2L)
        .addFileMapping(mapping)
        .build();

    assertEquals(1L, map.sourceSnapshotId());
    assertEquals(2L, map.targetSnapshotId());
    assertEquals(1, map.fileMappings().size());
    assertEquals("file1.parquet", map.fileMappings().get(0).sourceFile());
  }

  @Test
  public void testRunPositionMapping() {
    CompactionMap.Run run = new BaseCompactionMap.RunImpl(10, 20, 5);

    // Test positions within run
    assertEquals(20, run.mapPosition(10));
    assertEquals(21, run.mapPosition(11));
    assertEquals(24, run.mapPosition(14));

    // Test position outside run
    assertThrows(IllegalArgumentException.class, () -> run.mapPosition(9));
    assertThrows(IllegalArgumentException.class, () -> run.mapPosition(15));
  }

  @Test
  public void testFileMappingLookup() {
    CompactionMap.Run run1 = new BaseCompactionMap.RunImpl(0, 0, 50);
    CompactionMap.Run run2 = new BaseCompactionMap.RunImpl(100, 50, 50);

    CompactionMap.FileMapping mapping = new BaseCompactionMap.FileMappingImpl(
        "source.parquet",
        "target.parquet",
        Lists.newArrayList(run1, run2));

    // Test finding runs
    assertEquals(run1, mapping.runForPosition(25));
    assertEquals(run2, mapping.runForPosition(125));
    assertNull(mapping.runForPosition(75)); // Gap between runs
  }

  @Test
  public void testMapFileLookup() {
    CompactionMap.Run run = new BaseCompactionMap.RunImpl(0, 0, 100);
    CompactionMap.FileMapping mapping1 = new BaseCompactionMap.FileMappingImpl(
        "file1.parquet", "target.parquet", Lists.newArrayList(run));
    CompactionMap.FileMapping mapping2 = new BaseCompactionMap.FileMappingImpl(
        "file2.parquet", "target.parquet", Lists.newArrayList(run));

    CompactionMap map = new BaseCompactionMap.Builder()
        .sourceSnapshotId(1L)
        .targetSnapshotId(2L)
        .addFileMapping(mapping1)
        .addFileMapping(mapping2)
        .build();

    assertEquals(mapping1, map.mappingForFile("file1.parquet"));
    assertEquals(mapping2, map.mappingForFile("file2.parquet"));
    assertNull(map.mappingForFile("file3.parquet"));
  }
}
```

---

#### Test 6.2: CompactionMapBuilder

**File**: `core/src/test/java/org/apache/iceberg/TestCompactionMapBuilder.java` (NEW)

```java
public class TestCompactionMapBuilder {

  @Test
  public void testBuildSimpleMap() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    // Record consecutive mappings
    for (int i = 0; i < 100; i++) {
      builder.recordMapping("source.parquet", i, "target.parquet", i);
    }

    CompactionMap map = builder.build();

    // Should consolidate into single run
    assertEquals(1, map.fileMappings().size());
    CompactionMap.FileMapping mapping = map.fileMappings().get(0);
    assertEquals(1, mapping.runs().size());
    CompactionMap.Run run = mapping.runs().get(0);
    assertEquals(0, run.sourcePosition());
    assertEquals(0, run.targetPosition());
    assertEquals(100, run.length());
  }

  @Test
  public void testBuildWithGaps() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    // Record mappings with a gap
    for (int i = 0; i < 50; i++) {
      builder.recordMapping("source.parquet", i, "target.parquet", i);
    }
    for (int i = 100; i < 150; i++) {
      builder.recordMapping("source.parquet", i, "target.parquet", i - 50);
    }

    CompactionMap map = builder.build();

    // Should create two runs
    CompactionMap.FileMapping mapping = map.fileMappings().get(0);
    assertEquals(2, mapping.runs().size());

    CompactionMap.Run run1 = mapping.runs().get(0);
    assertEquals(0, run1.sourcePosition());
    assertEquals(0, run1.targetPosition());
    assertEquals(50, run1.length());

    CompactionMap.Run run2 = mapping.runs().get(1);
    assertEquals(100, run2.sourcePosition());
    assertEquals(50, run2.targetPosition());
    assertEquals(50, run2.length());
  }

  @Test
  public void testBuildWithReordering() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    // Record mappings out of order (should still consolidate)
    builder.recordMapping("source.parquet", 10, "target.parquet", 20);
    builder.recordMapping("source.parquet", 11, "target.parquet", 21);
    builder.recordMapping("source.parquet", 9, "target.parquet", 19);

    CompactionMap map = builder.build();

    // Should consolidate into single run after sorting
    CompactionMap.FileMapping mapping = map.fileMappings().get(0);
    assertEquals(1, mapping.runs().size());
    CompactionMap.Run run = mapping.runs().get(0);
    assertEquals(9, run.sourcePosition());
    assertEquals(19, run.targetPosition());
    assertEquals(3, run.length());
  }

  @Test
  public void testMultipleSourceFiles() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    // Map two source files to one target
    for (int i = 0; i < 50; i++) {
      builder.recordMapping("source1.parquet", i, "target.parquet", i);
    }
    for (int i = 0; i < 50; i++) {
      builder.recordMapping("source2.parquet", i, "target.parquet", i + 50);
    }

    CompactionMap map = builder.build();

    assertEquals(2, map.fileMappings().size());
    assertNotNull(map.mappingForFile("source1.parquet"));
    assertNotNull(map.mappingForFile("source2.parquet"));
  }
}
```

---

#### Test 6.3: Compaction Map Serialization

**File**: `core/src/test/java/org/apache/iceberg/TestCompactionMapSerialization.java` (NEW)

```java
public class TestCompactionMapSerialization extends TestBase {

  @Test
  public void testRoundTrip() throws IOException {
    // Create a compaction map
    CompactionMap.Run run1 = new BaseCompactionMap.RunImpl(0, 0, 100);
    CompactionMap.Run run2 = new BaseCompactionMap.RunImpl(200, 100, 50);
    CompactionMap.FileMapping mapping = new BaseCompactionMap.FileMappingImpl(
        "source.parquet",
        "target.parquet",
        Lists.newArrayList(run1, run2));

    CompactionMap originalMap = new BaseCompactionMap.Builder()
        .sourceSnapshotId(123L)
        .targetSnapshotId(456L)
        .addFileMapping(mapping)
        .build();

    // Write to file
    OutputFile output = Files.localOutput(temp.newFile());
    CompactionMapWriter.write(originalMap, output);

    // Read back
    InputFile input = Files.localInput(output.location());
    CompactionMap loadedMap = CompactionMapReader.read(input);

    // Verify
    assertEquals(originalMap.sourceSnapshotId(), loadedMap.sourceSnapshotId());
    assertEquals(originalMap.targetSnapshotId(), loadedMap.targetSnapshotId());
    assertEquals(1, loadedMap.fileMappings().size());

    CompactionMap.FileMapping loadedMapping = loadedMap.fileMappings().get(0);
    assertEquals("source.parquet", loadedMapping.sourceFile());
    assertEquals("target.parquet", loadedMapping.targetFile());
    assertEquals(2, loadedMapping.runs().size());

    CompactionMap.Run loadedRun1 = loadedMapping.runs().get(0);
    assertEquals(0, loadedRun1.sourcePosition());
    assertEquals(0, loadedRun1.targetPosition());
    assertEquals(100, loadedRun1.length());
  }

  @Test
  public void testLargeMap() throws IOException {
    // Test with many files and runs
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    for (int fileNum = 0; fileNum < 100; fileNum++) {
      String sourceFile = "source" + fileNum + ".parquet";
      String targetFile = "target" + (fileNum / 10) + ".parquet";

      for (int i = 0; i < 1000; i += 100) {
        builder.recordMapping(sourceFile, i, targetFile, fileNum * 1000 + i);
      }
    }

    CompactionMap map = builder.build();

    // Write and read
    OutputFile output = Files.localOutput(temp.newFile());
    CompactionMapWriter.write(map, output);

    InputFile input = Files.localInput(output.location());
    CompactionMap loadedMap = CompactionMapReader.read(input);

    assertEquals(100, loadedMap.fileMappings().size());
  }
}
```

---

#### Test 6.4: Position Delete Remapping

**File**: `core/src/test/java/org/apache/iceberg/TestPositionDeleteRemapper.java` (NEW)

```java
public class TestPositionDeleteRemapper extends TestBase {

  @Test
  public void testSimpleRemapping() throws IOException {
    // Create a compaction map
    CompactionMap.Run run = new BaseCompactionMap.RunImpl(0, 100, 100);
    CompactionMap.FileMapping mapping = new BaseCompactionMap.FileMappingImpl(
        "old.parquet", "new.parquet", Lists.newArrayList(run));
    CompactionMap map = new BaseCompactionMap.Builder()
        .sourceSnapshotId(1L)
        .targetSnapshotId(2L)
        .addFileMapping(mapping)
        .build();

    // Write compaction map
    OutputFile mapOutput = Files.localOutput(temp.newFile());
    CompactionMapWriter.write(map, mapOutput);

    // Create position delete file referencing old file
    List<PositionDelete<Void>> deletes = Lists.newArrayList(
        PositionDelete.create().set("old.parquet", 10L, null),
        PositionDelete.create().set("old.parquet", 50L, null),
        PositionDelete.create().set("old.parquet", 99L, null)
    );

    // TODO: Write position delete file

    // Remap
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(table.io());
    remapper.loadCompactionMap(mapOutput.location());

    // TODO: Test remapping
    // Verify positions are remapped: 10→110, 50→150, 99→199
  }

  @Test
  public void testRemappingWithMultipleTargetFiles() {
    // Test when source file splits into multiple targets
    // This tests the edge case mentioned in compaction_maps_code_analysis.md
  }

  @Test
  public void testPartialRemapping() {
    // Test when only some deletes need remapping
    // (some reference compacted files, some don't)
  }
}
```

---

### Integration Tests

#### Test 6.5: Basic Concurrent Write + Compaction

**File**: `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestCompactionMapsIntegration.java` (NEW)

```java
public class TestCompactionMapsIntegration extends SparkTestBase {

  @Test
  public void testConcurrentWriteAndCompaction() throws Exception {
    // Setup: Create table with multiple small files
    Table table = createTableWithSmallFiles(4);
    long startingSnapshotId = table.currentSnapshot().snapshotId();

    // Transaction T1: Start writing position deletes
    List<PositionDelete<Void>> deletes = Lists.newArrayList();
    for (DataFile file : table.currentSnapshot().addedDataFiles(table.io())) {
      deletes.add(PositionDelete.create().set(file.location(), 0L, null));
    }

    // Meanwhile: Compaction runs
    SparkActions.get()
        .rewriteDataFiles(table)
        .execute();

    // Verify compaction created map
    Snapshot compactionSnapshot = table.currentSnapshot();
    assertEquals(DataOperations.REPLACE, compactionSnapshot.operation());

    ManifestFile manifest = compactionSnapshot.dataManifests(table.io()).get(0);
    assertNotNull(manifest.compactionMapLocation());

    // T1: Commit position deletes
    RowDelta rowDelta = table.newRowDelta()
        .validateFromSnapshot(startingSnapshotId);

    // Write position deletes
    DeleteFile deleteFile = writePositionDeletes(table, deletes);
    rowDelta.addDeletes(deleteFile);

    // This should succeed (with remapping)
    rowDelta.commit();

    // Verify transaction succeeded
    Snapshot finalSnapshot = table.currentSnapshot();
    assertEquals(1, finalSnapshot.deleteManifests(table.io()).size());

    // Verify deletes were remapped correctly
    DeleteFile finalDeleteFile = finalSnapshot.deleteManifests(table.io()).get(0);
    List<PositionDelete<Void>> finalDeletes = readPositionDeletes(table, finalDeleteFile);

    // Deletes should reference new compacted file, not old files
    for (PositionDelete<Void> delete : finalDeletes) {
      DataFile referencedFile = findDataFile(table, delete.path().toString());
      assertNotNull("Delete references non-existent file", referencedFile);
    }
  }

  @Test
  public void testCompactionWithoutConcurrentWrites() {
    // Test that compaction still works when no concurrent writes occur
    // (map is generated but not used)
  }
}
```

---

#### Test 6.6: Edge Case - Multiple Concurrent Compactions

**File**: `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestCompactionMapsEdgeCases.java` (NEW)

```java
public class TestCompactionMapsEdgeCases extends SparkTestBase {

  @Test
  public void testChainedCompactions() throws Exception {
    // Edge case: S1 → S2 (compact) → S3 (compact)
    // Transaction against S1 needs to apply both maps

    Table table = createTableWithSmallFiles(8);
    long s1 = table.currentSnapshot().snapshotId();

    // T1: Start transaction against S1
    List<PositionDelete<Void>> deletes = createDeletesForSnapshot(table, s1);

    // Compaction 1: S1 → S2
    SparkActions.get().rewriteDataFiles(table).execute();
    long s2 = table.currentSnapshot().snapshotId();
    String map1Location = getCompactionMapLocation(table, s2);

    // Compaction 2: S2 → S3
    SparkActions.get().rewriteDataFiles(table).execute();
    long s3 = table.currentSnapshot().snapshotId();
    String map2Location = getCompactionMapLocation(table, s3);

    // T1: Commit against S3 (needs to apply both maps)
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(s1);
    DeleteFile deleteFile = writePositionDeletes(table, deletes);
    rowDelta.addDeletes(deleteFile);

    // Should succeed by chaining S1→S2→S3 maps
    rowDelta.commit();

    // Verify correctness
    verifyDeletesRemapped(table);
  }

  @Test
  public void testPartialCompaction() throws Exception {
    // Edge case: Only some files in partition are compacted

    Table table = createPartitionedTable();

    // Add files to multiple partitions
    addFilesToPartition(table, "2023-01-01", 10);
    addFilesToPartition(table, "2023-01-02", 10);
    long s1 = table.currentSnapshot().snapshotId();

    // T1: Create deletes for both partitions
    List<PositionDelete<Void>> deletes = createDeletesForAllPartitions(table);

    // Compact only partition 2023-01-01
    SparkActions.get()
        .rewriteDataFiles(table)
        .filter(Expressions.equal("date", "2023-01-01"))
        .execute();

    // T1: Commit deletes
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(s1);
    DeleteFile deleteFile = writePositionDeletes(table, deletes);
    rowDelta.addDeletes(deleteFile);
    rowDelta.commit();

    // Verify: Deletes for 2023-01-01 remapped, 2023-01-02 unchanged
    verifyPartialRemapping(table, "2023-01-01", "2023-01-02");
  }

  @Test
  public void testCrossFilePositionDeletes() throws Exception {
    // Edge case: Single delete file references multiple data files
    // Some compacted, some not

    Table table = createTableWithSmallFiles(5);
    long s1 = table.currentSnapshot().snapshotId();

    // T1: Create deletes referencing all 5 files
    List<PositionDelete<Void>> deletes = createDeletesForAllFiles(table);

    // Compact only first 3 files
    List<DataFile> filesToCompact = getFirstNFiles(table, 3);
    SparkActions.get()
        .rewriteDataFiles(table)
        .filter(filesToFilter(filesToCompact))
        .execute();

    // T1: Commit deletes
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(s1);
    DeleteFile deleteFile = writePositionDeletes(table, deletes);
    rowDelta.addDeletes(deleteFile);
    rowDelta.commit();

    // Verify: Mixed remapping
    // - Deletes for first 3 files: remapped to new compacted file
    // - Deletes for last 2 files: unchanged
    verifyMixedRemapping(table, filesToCompact);
  }

  @Test
  public void testEqualityDeletesUnaffected() throws Exception {
    // Edge case: Equality deletes should not be remapped

    Table table = createTable();
    long s1 = table.currentSnapshot().snapshotId();

    // T1: Create equality deletes
    List<Record> equalityDeletes = createEqualityDeletes(table);

    // Compact
    SparkActions.get().rewriteDataFiles(table).execute();

    // T1: Commit equality deletes
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(s1);
    DeleteFile deleteFile = writeEqualityDeletes(table, equalityDeletes);
    rowDelta.addDeletes(deleteFile);

    // Should succeed without remapping (equality deletes are value-based)
    rowDelta.commit();

    // Verify equality deletes work correctly
    verifyEqualityDeletesApplied(table);
  }

  @Test
  public void testDeleteFileCompaction() throws Exception {
    // Edge case: Compacting delete files themselves
    // Should NOT create compaction maps

    Table table = createTableWithManyDeletes();

    // Compact delete files
    SparkActions.get()
        .rewriteDeleteFiles(table) // Different action!
        .execute();

    // Verify: No compaction maps created
    Snapshot snapshot = table.currentSnapshot();
    for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
      assertNull("Delete file compaction should not create maps",
          manifest.compactionMapLocation());
    }
  }
}
```

---

#### Test 6.7: Backward Compatibility

**File**: `core/src/test/java/org/apache/iceberg/TestCompactionMapsBackwardCompatibility.java` (NEW)

```java
public class TestCompactionMapsBackwardCompatibility extends TestBase {

  @Test
  public void testOldReaderIgnoresCompactionMapField() throws IOException {
    // Create manifest with compaction map location
    GenericManifestFile manifest = new GenericManifestFile(
        "manifest.avro", 1000L, 0, ManifestContent.DATA,
        1L, 1L, 100L,
        Lists.newArrayList(),
        null,
        "compaction-map.avro",  // NEW field
        10, 1000L, 0, 0L, 0, 0L, null);

    // Serialize using current format
    OutputFile output = Files.localOutput(temp.newFile());
    ManifestListWriter writer = ManifestLists.write(2, output, 100L, 99L, 1L);
    writer.add(manifest);
    writer.close();

    // Read using old schema (without compaction_map_location field)
    Schema oldSchema = new Schema(
        ManifestFile.PATH,
        ManifestFile.LENGTH,
        ManifestFile.SPEC_ID,
        ManifestFile.MANIFEST_CONTENT,
        ManifestFile.SEQUENCE_NUMBER,
        ManifestFile.MIN_SEQUENCE_NUMBER,
        ManifestFile.SNAPSHOT_ID,
        ManifestFile.ADDED_FILES_COUNT,
        ManifestFile.EXISTING_FILES_COUNT,
        ManifestFile.DELETED_FILES_COUNT,
        ManifestFile.ADDED_ROWS_COUNT,
        ManifestFile.EXISTING_ROWS_COUNT,
        ManifestFile.DELETED_ROWS_COUNT,
        ManifestFile.PARTITION_SUMMARIES,
        ManifestFile.KEY_METADATA,
        ManifestFile.FIRST_ROW_ID
        // Note: COMPACTION_MAP_LOCATION not included
    );

    InputFile input = Files.localInput(output.location());
    List<ManifestFile> manifests = Avro.read(input)
        .project(oldSchema)
        .build()
        .toList();

    // Should succeed - old reader ignores new field
    assertEquals(1, manifests.size());
    ManifestFile loaded = manifests.get(0);

    // Old readers return null for new field (default implementation)
    assertNull(loaded.compactionMapLocation());
  }

  @Test
  public void testNewReaderHandlesOldManifests() throws IOException {
    // Read manifest written by old writer (without compaction map field)
    // Should work correctly with null compaction map location

    // Create manifest using old format
    GenericManifestFile manifest = createManifestWithoutCompactionMap();

    // Write using current writer
    OutputFile output = Files.localOutput(temp.newFile());
    ManifestListWriter writer = ManifestLists.write(2, output, 100L, 99L, 1L);
    writer.add(manifest);
    writer.close();

    // Read using current reader
    InputFile input = Files.localInput(output.location());
    List<ManifestFile> manifests = ManifestLists.read(input);

    assertEquals(1, manifests.size());
    assertNull(manifests.get(0).compactionMapLocation());
  }

  @Test
  public void testMixedManifests() throws IOException {
    // Test manifest list with mix of old and new manifests

    ManifestFile oldManifest = createManifestWithoutCompactionMap();
    ManifestFile newManifest = createManifestWithCompactionMap("map.avro");

    OutputFile output = Files.localOutput(temp.newFile());
    ManifestListWriter writer = ManifestLists.write(2, output, 100L, 99L, 1L);
    writer.add(oldManifest);
    writer.add(newManifest);
    writer.close();

    InputFile input = Files.localInput(output.location());
    List<ManifestFile> manifests = ManifestLists.read(input);

    assertEquals(2, manifests.size());
    assertNull(manifests.get(0).compactionMapLocation());
    assertEquals("map.avro", manifests.get(1).compactionMapLocation());
  }
}
```

---

#### Test 6.8: Performance Benchmarks

**File**: `core/src/test/java/org/apache/iceberg/BenchmarkCompactionMaps.java` (NEW)

```java
@State(Scope.Benchmark)
public class BenchmarkCompactionMaps {

  @Param({"10", "100", "1000"})
  private int numFiles;

  @Param({"1000", "10000", "100000"})
  private int rowsPerFile;

  private CompactionMap map;

  @Setup
  public void setup() {
    // Create compaction map with specified parameters
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    for (int fileNum = 0; fileNum < numFiles; fileNum++) {
      String sourceFile = "file" + fileNum + ".parquet";
      String targetFile = "compacted.parquet";

      for (int row = 0; row < rowsPerFile; row++) {
        builder.recordMapping(sourceFile, row, targetFile, fileNum * rowsPerFile + row);
      }
    }

    map = builder.build();
  }

  @Benchmark
  public long benchmarkMapGeneration() {
    // Already done in setup, measure there
    return map.fileMappings().size();
  }

  @Benchmark
  public long benchmarkMapLookup() {
    // Benchmark random position lookups
    Random random = new Random(42);
    long sum = 0;

    for (int i = 0; i < 1000; i++) {
      int fileNum = random.nextInt(numFiles);
      long position = random.nextInt(rowsPerFile);
      String sourceFile = "file" + fileNum + ".parquet";

      CompactionMap.FileMapping mapping = map.mappingForFile(sourceFile);
      CompactionMap.Run run = mapping.runForPosition(position);
      sum += run.mapPosition(position);
    }

    return sum;
  }

  @Benchmark
  public long benchmarkMapSerialization() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputFile output = new InMemoryOutputFile(baos);
    CompactionMapWriter.write(map, output);
    return baos.size();
  }
}
```

---

## Phase 7: Edge Cases and Optimization

**Duration**: 1 week
**Dependencies**: Phase 6
**Branch**: `feature/compaction-maps-optimization`

### Task 7.1: Handle Chained Compactions

**Implementation**: Extend `PositionDeleteRemapper` to handle multiple maps

```java
public class PositionDeleteRemapper {
  // ... existing code ...

  /**
   * Loads multiple compaction maps and chains them.
   *
   * <p>This handles the case where multiple compactions occurred:
   * S1 → S2 (map1), S2 → S3 (map2), transaction against S1 needs map1 ∘ map2
   */
  public void loadChainedCompactionMaps(List<String> mapLocations) throws IOException {
    // Load all maps
    List<CompactionMap> maps = new ArrayList<>();
    for (String location : mapLocations) {
      InputFile input = fileIO.newInputFile(location);
      maps.add(CompactionMapReader.read(input));
    }

    // Sort by source/target snapshot IDs to establish chain
    maps.sort((m1, m2) -> Long.compare(m1.targetSnapshotId(), m2.targetSnapshotId()));

    // Build transitive mapping
    // For each source file in earliest map, follow chain to latest target
    for (CompactionMap map : maps) {
      for (CompactionMap.FileMapping mapping : map.fileMappings()) {
        String sourceFile = mapping.sourceFile();

        // Transitively map through all subsequent compactions
        String finalTarget = mapping.targetFile();
        List<CompactionMap.Run> finalRuns = new ArrayList<>(mapping.runs());

        for (CompactionMap nextMap : maps) {
          if (nextMap.sourceSnapshotId() == map.targetSnapshotId()) {
            CompactionMap.FileMapping nextMapping = nextMap.mappingForFile(finalTarget);
            if (nextMapping != null) {
              finalTarget = nextMapping.targetFile();
              finalRuns = transitivelyMapRuns(finalRuns, nextMapping.runs());
            }
          }
        }

        // Store final transitive mapping
        // ... implementation ...
      }
    }
  }

  private List<CompactionMap.Run> transitivelyMapRuns(
      List<CompactionMap.Run> runs1,
      List<CompactionMap.Run> runs2) {
    // Compose two run mappings: runs1 ∘ runs2
    // For each position in runs1, map through runs2
    // ... implementation ...
  }
}
```

**Test**: `TestChainedCompactionMaps.java`

---

### Task 7.2: Optimize Map Storage with Compression

**Implementation**: Add compression to map files

```java
public class CompactionMapWriter {

  public static void write(CompactionMap map, OutputFile output) throws IOException {
    try (org.apache.iceberg.avro.AvroIterable.Writer<CompactionMap> writer =
        Avro.write(output)
            .schema(CompactionMapFormat.schema())
            .createWriterFunc(ignored -> new CompactionMapAvroWriter())
            .setCodec(CodecFactory.snappyCodec())  // NEW: Add compression
            .build()) {
      writer.write(map);
    }
  }
}
```

**Test**: Measure compression ratio in benchmarks

---

### Task 7.3: Add Compaction Map Expiration

**File**: `core/src/main/java/org/apache/iceberg/RemoveSnapshots.java`

**Extend snapshot expiration to clean up compaction maps**:

```java
@Override
public RemoveSnapshots deleteWith(Consumer<String> deleteFunc) {
  // ... existing code ...

  // NEW: Collect compaction maps from expired snapshots
  Set<String> compactionMapsToDelete = Sets.newHashSet();

  for (Snapshot snapshot : expiredSnapshots) {
    for (ManifestFile manifest : snapshot.allManifests(ops.io())) {
      String mapLocation = manifest.compactionMapLocation();
      if (mapLocation != null) {
        compactionMapsToDelete.add(mapLocation);
      }
    }
  }

  // Delete compaction maps
  LOG.info("Deleting {} compaction map files", compactionMapsToDelete.size());
  for (String mapLocation : compactionMapsToDelete) {
    try {
      deleteFunc.accept(mapLocation);
    } catch (Exception e) {
      LOG.warn("Failed to delete compaction map: {}", mapLocation, e);
    }
  }

  return this;
}
```

**Test**: `TestCompactionMapExpiration.java`

---

## Phase 8: Documentation and Examples

**Duration**: 1 week
**Dependencies**: Phases 1-7
**Branch**: `feature/compaction-maps-docs`

### Task 8.1: API Documentation

Update Javadocs for all public APIs:
- `CompactionMap` interface
- `ManifestFile.compactionMapLocation()`
- `TableProperties.COMPACTION_MAPS_*`

### Task 8.2: User Guide

**File**: `docs/docs/compaction-maps.md` (NEW)

Write comprehensive guide covering:
- What are compaction maps?
- When are they useful?
- How to enable/disable
- Configuration options
- Troubleshooting

### Task 8.3: Migration Guide

Document upgrade path:
- Enabling compaction maps on existing tables
- Performance impact
- Backward compatibility guarantees

### Task 8.4: Example Code

**File**: `examples/src/main/java/org/apache/iceberg/examples/CompactionMapsExample.java` (NEW)

Provide runnable example demonstrating:
- Enabling compaction maps
- Running compaction
- Concurrent writes
- Verification

---

## Testing Coverage Summary

### Unit Tests (15+ tests)
- ✓ CompactionMap data structure
- ✓ CompactionMapBuilder
- ✓ Map serialization/deserialization
- ✓ Position tracking
- ✓ Remapping logic
- ✓ Backward compatibility

### Integration Tests (8+ tests)
- ✓ Basic concurrent write + compaction
- ✓ Chained compactions
- ✓ Partial compaction
- ✓ Cross-file position deletes
- ✓ Equality deletes (unaffected)
- ✓ Delete file compaction (no maps)
- ✓ Mixed old/new manifests

### Performance Tests
- ✓ Map generation overhead
- ✓ Map size measurements
- ✓ Lookup performance
- ✓ Serialization performance

### Edge Cases Covered (from compaction_maps_code_analysis.md)
1. ✓ Multiple concurrent compactions → Test 6.6: testChainedCompactions
2. ✓ Partial compaction → Test 6.6: testPartialCompaction
3. ✓ Cross-file position deletes → Test 6.6: testCrossFilePositionDeletes
4. ✓ Equality deletes → Test 6.6: testEqualityDeletesUnaffected
5. ✓ Delete file compaction → Test 6.6: testDeleteFileCompaction

---

## Rollout Strategy

### Phase 1: Internal Testing (Week 9)
- Deploy to development environment
- Run with feature flag disabled by default
- Test with production-like workloads

### Phase 2: Opt-In Beta (Week 10)
- Enable feature flag for selected tables
- Monitor performance and correctness
- Gather user feedback

### Phase 3: Opt-Out (Week 11)
- Enable by default with opt-out option
- Broad deployment across tables
- Continue monitoring

### Phase 4: GA (Week 12)
- Remove feature flag
- Compaction maps enabled for all new tables
- Document as stable feature

---

## Success Criteria

### Functional
- ✓ All unit tests pass
- ✓ All integration tests pass
- ✓ Backward compatibility verified
- ✓ No regression in existing functionality

### Performance
- ✓ Map generation overhead < 2% of compaction time
- ✓ Map size < 0.1% of compacted data size
- ✓ Remapping time < 1% of transaction time

### Operational
- ✓ Zero-downtime deployment
- ✓ Graceful degradation if disabled
- ✓ Clear error messages
- ✓ Comprehensive documentation

---

## Risk Mitigation

### Risk: Map generation impacts compaction performance
**Mitigation**: Feature flag to disable, performance benchmarks in CI

### Risk: Map storage accumulates
**Mitigation**: Automatic cleanup with snapshot expiration

### Risk: Remapping logic has bugs
**Mitigation**: Comprehensive unit tests, fuzz testing, verification

### Risk: Backward compatibility issues
**Mitigation**: Extensive compatibility testing, gradual rollout

---

## Implementation Checklist

- [ ] Phase 1: Data Structures and Schema (Week 1)
  - [ ] Task 1.1: CompactionMap interface
  - [ ] Task 1.2: BaseCompactionMap implementation
  - [ ] Task 1.3: ManifestFile schema extension
  - [ ] Task 1.4: GenericManifestFile updates
  - [ ] Task 1.5: Builder updates

- [ ] Phase 2: Map Generation (Week 2)
  - [ ] Task 2.1: CompactionMapBuilder
  - [ ] Task 2.2: Position tracking iterator
  - [ ] Task 2.3: Compaction context

- [ ] Phase 3: Storage and Retrieval (Week 3)
  - [ ] Task 3.1: File format definition
  - [ ] Task 3.2: CompactionMapWriter
  - [ ] Task 3.3: CompactionMapReader
  - [ ] Task 3.4: Table properties

- [ ] Phase 4: Transaction Integration (Weeks 4-5)
  - [ ] Task 4.1: PositionDeleteRemapper
  - [ ] Task 4.2: Validation extension
  - [ ] Task 4.3: BaseRowDelta integration

- [ ] Phase 5: Compaction Integration (Weeks 5-6)
  - [ ] Task 5.1: RewriteDataFiles integration
  - [ ] Task 5.2: Position tracking in rewrite
  - [ ] Task 5.3: Map storage in manifests
  - [ ] Task 5.4: Manifest writer updates

- [ ] Phase 6: Testing (Weeks 7-8)
  - [ ] Unit tests (15+)
  - [ ] Integration tests (8+)
  - [ ] Performance benchmarks
  - [ ] Backward compatibility tests

- [ ] Phase 7: Optimization (Week 9)
  - [ ] Chained compaction handling
  - [ ] Compression
  - [ ] Map expiration

- [ ] Phase 8: Documentation (Week 10)
  - [ ] API docs
  - [ ] User guide
  - [ ] Migration guide
  - [ ] Examples

---

## Estimated LOC

- New files: ~3,000 LOC
- Modified files: ~500 LOC
- Test code: ~2,500 LOC
- **Total: ~6,000 LOC**

---

## Timeline Summary

| Week | Phase | Deliverable |
|------|-------|-------------|
| 1 | Schema | CompactionMap + ManifestFile schema |
| 2 | Generation | Map builder + tracking |
| 3 | Storage | Writer/reader + format |
| 4-5 | Transactions | Remapper + validation |
| 5-6 | Compaction | Integration with rewrites |
| 7-8 | Testing | Full test suite |
| 9 | Optimization | Edge cases + perf |
| 10 | Documentation | Docs + examples |

**Total: 10 weeks (including documentation)**
