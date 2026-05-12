---
title: "Compaction Maps - Implementation Details"
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

# Compaction Maps - Implementation Details

This document provides detailed implementation information for compaction maps. For an overview and usage guide, see [Compaction Maps](compaction_maps.md).

## Architecture

### High-Level Design

```
┌─────────────────────────────────────────────────────────────────┐
│                     Compaction Operation                        │
├─────────────────────────────────────────────────────────────────┤
│  1. Rewrite data files (F1, F2 → F3)                           │
│  2. Track position mappings using CompactionMapBuilder         │
│  3. Write CompactionMap to Avro file                           │
│  4. Attach map location to ManifestFile                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Snapshot Metadata                            │
├─────────────────────────────────────────────────────────────────┤
│  ManifestFile:                                                  │
│    - path: metadata/manifest-abc123.avro                       │
│    - compactionMapLocation: metadata/compaction-map-xyz.avro   │
│    - addedFiles: [F3]                                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              Transaction with Position Deletes                  │
├─────────────────────────────────────────────────────────────────┤
│  1. Detect compacted files via CompactionMapValidator          │
│  2. Load compaction map from metadata location                 │
│  3. Remap position deletes using PositionDeleteRemapper        │
│  4. Commit with remapped deletes                               │
└─────────────────────────────────────────────────────────────────┘
```

### Components

#### 1. Data Structures

**CompactionMap** - Main interface representing position transformations
```java
interface CompactionMap {
  long sourceSnapshotId();
  long targetSnapshotId();
  List<FileMapping> fileMappings();
  FileMapping mappingForFile(String sourceFilePath);
}
```

**FileMapping** - Maps positions from one source file to one target file
```java
interface FileMapping {
  String sourceFile();     // e.g., "s3://bucket/data/old.parquet"
  String targetFile();     // e.g., "s3://bucket/data/new.parquet"
  List<Run> runs();        // Position mappings
  Run runForPosition(long sourcePosition);
}
```

**Run** - Contiguous range of rows mapped from source to target
```java
interface Run {
  long sourcePosition();   // Starting position in source file
  long targetPosition();   // Starting position in target file
  long length();          // Number of rows in this run
  String targetFile();    // Per-run target (null = use parent's targetFile)
  long mapPosition(long sourcePos);  // Transform a single position
}
```

#### 2. Storage Format

Compaction maps are stored as Avro files in the table's metadata directory:

**File Location Pattern:**
```
<metadata-dir>/compaction-map-<snapshotId>-<uuid>.avro
```

**Schema:**
```
record CompactionMap {
  long source_snapshot_id;
  long target_snapshot_id;
  array<FileMapping> file_mappings;
}

record FileMapping {
  string source_file;
  string target_file;      // Default target for backward compat
  array<Run> runs;
}

record Run {
  long source_position;
  long target_position;
  long length;
  optional string target_file;  // Per-run target (null = use parent's)
}
```

**Multi-Target Mapping:**

When a source file's rows span multiple target files (e.g., due to target file size limits), each run specifies its own target file:

```
Source: large-file.parquet (3000 rows)
Targets: target-001.parquet (1500 rows capacity), target-002.parquet

CompactionMap:
  file_mappings: [
    {
      source_file: "s3://bucket/data/large-file.parquet",
      target_file: "s3://bucket/data/target-001.parquet",  // default
      runs: [
        {source_position: 0, target_position: 0, length: 1500, target_file: "target-001.parquet"},
        {source_position: 1500, target_position: 0, length: 1500, target_file: "target-002.parquet"}
      ]
    }
  ]
```

**Example:**
```
Compacting: file1.parquet (rows 0-999) → compacted.parquet (rows 0-999)
            file2.parquet (rows 0-499) → compacted.parquet (rows 1000-1499)

CompactionMap:
  source_snapshot_id: 1
  target_snapshot_id: 2
  file_mappings: [
    {
      source_file: "s3://bucket/data/file1.parquet",
      target_file: "s3://bucket/data/compacted.parquet",
      runs: [{source_position: 0, target_position: 0, length: 1000}]
    },
    {
      source_file: "s3://bucket/data/file2.parquet",
      target_file: "s3://bucket/data/compacted.parquet",
      runs: [{source_position: 0, target_position: 1000, length: 500}]
    }
  ]
```

#### 3. Run-Length Encoding

Compaction maps use run-length encoding to efficiently represent position mappings. Consecutive positions that map linearly are merged into single runs.

**Without Optimization:**
```
Row 0: file1 pos 0 → file2 pos 0
Row 1: file1 pos 1 → file2 pos 1
Row 2: file1 pos 2 → file2 pos 2
... (1000 individual mappings)
```

**With Run-Length Encoding:**
```
Run: file1 [0-999] → file2 [0-999]  (single mapping for 1000 rows)
```

The `CompactionMapBuilder` automatically merges consecutive runs during construction.

## API Usage

### Building Compaction Maps

```java
// During compaction, track position mappings
CompactionMapBuilder builder = new CompactionMapBuilder(
    sourceSnapshotId,  // Snapshot before compaction
    targetSnapshotId   // Snapshot after compaction
);

// Add mapping for each source file
CompactionMapBuilder.FileMappingBuilder fileMapping =
    builder.addFileMapping(
        "s3://bucket/data/file1.parquet",      // Source file
        "s3://bucket/data/compacted.parquet"   // Default target file
    );

// Add runs as data is written
// Rows 0-999 from source → rows 0-999 in target
fileMapping.addRun(0, 0, 1000);

// Add mapping for second source file
builder.addFileMapping(
        "s3://bucket/data/file2.parquet",
        "s3://bucket/data/compacted.parquet"
    )
    .addRun(0, 1000, 500);  // Rows 0-499 → rows 1000-1499

// Build the map
CompactionMap map = builder.build();
```

### Building Multi-Target Compaction Maps

When a source file spans multiple target files:

```java
CompactionMapBuilder builder = new CompactionMapBuilder(sourceSnapshotId, targetSnapshotId);

// Large source file that spans two target files
CompactionMapBuilder.FileMappingBuilder fileMapping =
    builder.addFileMapping(
        "s3://bucket/data/large-source.parquet",
        "s3://bucket/data/target-001.parquet"  // Default target
    );

// First 1500 rows go to target-001
fileMapping.addRun(0, 0, 1500, "s3://bucket/data/target-001.parquet");

// Next 1500 rows go to target-002 (different target file)
fileMapping.addRun(1500, 0, 1500, "s3://bucket/data/target-002.parquet");

CompactionMap map = builder.build();

// Verify multi-target support
FileMapping mapping = map.mappingForFile("s3://bucket/data/large-source.parquet");
for (Run run : mapping.runs()) {
    String target = run.targetFile() != null ? run.targetFile() : mapping.targetFile();
    System.out.println("Source [" + run.sourcePosition() + ", " +
        (run.sourcePosition() + run.length()) + ") -> " + target);
}
```

### Writing Compaction Maps

```java
// Generate output file location
OutputFile outputFile = CompactionMaps.newCompactionMapFile(table, snapshotId);

// Write the map
try (CompactionMaps.CompactionMapWriter writer = CompactionMaps.write(outputFile)) {
    writer.write(map);
}

String mapLocation = outputFile.location();
// e.g., "s3://bucket/metadata/compaction-map-12345-uuid.avro"
```

### Reading Compaction Maps

```java
// Read from metadata location
InputFile inputFile = fileIO.newInputFile(mapLocation);
CompactionMap map = CompactionMaps.read(inputFile);

// Query the map
FileMapping mapping = map.mappingForFile("s3://bucket/data/file1.parquet");
if (mapping != null) {
    Run run = mapping.runForPosition(42);
    long newPosition = run.mapPosition(42);
    System.out.println("Position 42 maps to: " + newPosition);
}
```

### Remapping Position Deletes

```java
// Create remapper from compaction map
PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

// Check if a delete file needs remapping
if (remapper.needsRemapping(deleteFile)) {
    // Remap individual position deletes
    PositionDelete<?> originalDelete = PositionDelete.create()
        .set("s3://bucket/data/file1.parquet", 42);

    PositionDelete<?> remappedDelete = remapper.remapDelete(originalDelete);

    // New delete references the compacted file at the remapped position
    System.out.println("New file: " + remappedDelete.path());
    System.out.println("New position: " + remappedDelete.pos());
}
```

### Remapping Deletion Vectors (DVs)

```java
// Create remapper from compaction map
PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

// Read DV file
DeleteFile sourceDV = ...; // DV that references compacted file

// Remap DV to new target files (use remapDVBulk, not the deprecated remapDV)
Map<String, Set<Long>> remappedPositions = remapper.remapDVBulk(sourceDV, fileIO);

// remappedPositions is a map from target file path to deleted positions
// Example: {"s3://bucket/target.parquet" -> [10, 20, 30, 45, 75]}

// Write new DVs for remapped positions
RemappedDVWriter writer = new RemappedDVWriter(table, table.spec(), null);
List<DeleteFile> newDVs = writer.writeRemappedDVs(remappedPositions);

// newDVs contains one DV per target file, ready to commit
for (DeleteFile dv : newDVs) {
    System.out.println("Created DV: " + dv.path());
    System.out.println("References: " + dv.referencedDataFile());
    System.out.println("Deletes: " + dv.recordCount() + " positions");
}
```

### Writing DVs from Position Collections

For scenarios where you have pre-computed position collections:

```java
// Write a single DV for a specific data file
String dataFilePath = "s3://bucket/data/file.parquet";
Collection<Long> deletedPositions = Arrays.asList(10L, 20L, 30L, 40L);

// Create OutputFileFactory for DV generation
OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1)
    .format(FileFormat.PUFFIN)
    .build();

// Write DV
DVPositionWriter writer = new DVPositionWriter(
    fileFactory,
    table.spec(),
    null,  // partition (null for unpartitioned)
    dataFilePath
);

DeleteFile dv = writer.writePositions(deletedPositions);

if (dv != null) {
    // DV was created (null if positions were empty)
    System.out.println("Created DV: " + dv.path());
    System.out.println("References: " + dv.referencedDataFile());
    System.out.println("Deletes: " + dv.recordCount() + " positions");
}
```

### Validating Transactions

```java
// In transaction validation
CompactionMapValidator validator = new CompactionMapValidator(
    fileIO,
    tableMetadata,
    startingSnapshotId,
    currentSnapshot
);

// Check if any delete files conflict with compactions
validator.validateNoCompactedReferences(deleteFiles);
// Throws ValidationException if conflicts detected
```

### Integration with Rewrites

```java
// In compaction operation
RewriteFiles rewrite = table.newRewrite();

// After building and writing compaction map
if (rewrite instanceof BaseRewriteFiles) {
    BaseRewriteFiles baseRewrite = (BaseRewriteFiles) rewrite;
    baseRewrite.setCompactionMapLocation(mapLocation);
}

// Delete old files
oldFiles.forEach(rewrite::deleteFile);

// Add new files
newFiles.forEach(rewrite::addFile);

// Commit - manifest will include compaction map location
rewrite.commit();
```

### Handling Compaction Conflicts

When position deletes reference compacted files, a `CompactionConflictException` is thrown.
This is a **correctness requirement**: stale position deletes must be remapped before commit.
Use `PositionDeleteRemapper.fromConflict()` which handles both single and chained compactions:

```java
try {
    rowDelta.addDeletes(deleteFile);
    rowDelta.commit();
} catch (CompactionConflictException e) {
    // fromConflict() loads and composes all compaction maps from the exception,
    // including chained maps if multiple sequential compactions occurred.
    Map<String, PositionDeleteRemapper> remappers =
        PositionDeleteRemapper.fromConflict(e, table.io());

    // Remap position deletes using the appropriate remapper for each file
    DeleteFile remappedDelete = remapDeleteFile(deleteFile, remappers);

    // Retry with remapped deletes — validate from current snapshot
    // to detect any further compactions during the retry
    table.refresh();
    RowDelta retry = table.newRowDelta()
        .validateFromSnapshot(table.currentSnapshot().snapshotId());
    retry.addDeletes(remappedDelete);
    retry.commit();
}
```

**Manual map loading** (lower-level alternative, does NOT handle chained compactions):

```java
} catch (CompactionConflictException e) {
    Set<String> compactedFiles = e.compactedFiles();
    Map<String, String> mapLocations = e.compactionMapLocations();

    List<CompactionMap> maps = mapLocations.values().stream()
        .distinct()
        .map(loc -> CompactionMaps.read(fileIO.newInputFile(loc)))
        .collect(Collectors.toList());

    // WARNING: maps.get(0) only works for single compactions.
    // For chained compactions, use PositionDeleteRemapper.fromConflict() instead.
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(maps.get(0));
    // ...
}
```

## Implementation Phases

### Phase 1: Core Data Structures (Completed)

**Files:**
- `api/src/main/java/org/apache/iceberg/CompactionMap.java` - Core interface
- `core/src/main/java/org/apache/iceberg/GenericCompactionMap.java` - Avro implementation
- `core/src/main/java/org/apache/iceberg/CompactionMaps.java` - Storage utilities

**Key Design Decisions:**
- Used Avro serialization (not Java Serializable) to follow Iceberg patterns
- Implemented StructLike, IndexedRecord, SchemaConstructable for Avro compatibility
- Used InternalData.read() with setCustomType() for nested types
- Added compactionMapLocation field (ID 521) to ManifestFile schema

### Phase 2: Map Generation (Completed)

**Files:**
- `core/src/main/java/org/apache/iceberg/CompactionMapBuilder.java` - Builder with run merging

**Features:**
- Fluent API for building maps during compaction
- Automatic merging of consecutive runs
- Validation of inputs (non-negative positions, positive lengths)
- Lookup support via getFileMapping()

**Run Merging Example:**
```java
builder.addFileMapping("file1", "file2")
    .addRun(0, 0, 100)    // Rows 0-99
    .addRun(100, 100, 50) // Rows 100-149
    .addRun(150, 150, 25); // Rows 150-174

// Automatically merged into single run: (0, 0, 175)
```

### Phase 3: Storage Infrastructure (Completed)

**Files:**
- `core/src/main/java/org/apache/iceberg/TableProperties.java` - Configuration properties
- `core/src/main/java/org/apache/iceberg/CompactionMaps.java` - Location generation

**Features:**
- Table properties for enabling/configuring compaction maps
- Location generation following Iceberg patterns
- Uses TableOperations.metadataFileLocation() for proper placement
- File naming: `compaction-map-<snapshotId>-<uuid>.avro`

### Phase 4: Transaction Integration (Completed)

**Files:**
- `core/src/main/java/org/apache/iceberg/PositionDeleteRemapper.java` - Remapping utility
- `core/src/main/java/org/apache/iceberg/CompactionMapValidator.java` - Conflict detection

**Features:**
- PositionDeleteRemapper for remapping individual deletes
- CompactionMapValidator for detecting conflicts during commit
- Error handling for incomplete/corrupted maps
- Efficient lookup using file mapping index

**Validation Flow:**
1. Traverse snapshot history from current to starting snapshot
2. Collect all compaction maps from manifest files
3. Check if any position deletes reference compacted files
4. Throw CompactionConflictException with remediation guidance

### Phase 4.3: Commit Flow Integration (Completed)

**Files:**
- `core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java` - Automatic map generation
- `core/src/main/java/org/apache/iceberg/actions/RewriteFileGroup.java` - Position mapping support

**Features:**
- Automatic compaction map generation during commit
- `buildCompactionMap()` method generates maps from RewriteFileGroups
- `writeCompactionMap()` persists maps to metadata location
- Integration with BaseRewriteFiles.setCompactionMapLocation()
- FilePositionMapping class for tracking source-to-target transformations

**Map Generation Flow:**
1. RewriteDataFilesCommitManager.commitFileGroups() receives RewriteFileGroups
2. Check if compaction maps are enabled via table property
3. Build compaction map using position mappings from file groups
4. Write map to metadata location
5. Set location on BaseRewriteFiles via setCompactionMapLocation()
6. Commit proceeds with map location attached to manifest

### Phase 4.5: SERIALIZABLE Isolation Enhancements (Completed)

**Files:**
- `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java` - Compaction-aware validation
- `core/src/main/java/org/apache/iceberg/BaseRowDelta.java` - Integration with RowDelta

**Features:**
- `validateCompactionAwareConflicts()` method for SERIALIZABLE isolation
- Distinguishes between structural changes (with compaction maps) and data changes (without maps)
- Only validates REPLACE operations when validateNoConflictingDataFiles() is called
- Respects conflict detection filters for partition-aware validation
- Clear error messages indicating whether remapping is possible

**Isolation Semantics (read-conflict optimization only):**
- **REPLACE with compaction map**: No read conflict (structural change only, data unchanged)
- **REPLACE without compaction map**: Validation failure (potential data change)
- **SNAPSHOT isolation**: REPLACE operations not checked (existing behavior)
- **SERIALIZABLE isolation**: REPLACE operations checked with compaction awareness

**Note:** This is independent of write-conflict checking. Position deletes that reference
compacted files always trigger `CompactionConflictException` via `CompactionMapValidator`,
regardless of isolation level. See Phase 4 above.

### Phase 5: Compaction Integration - Core (Completed)

**Files:**
- `core/src/main/java/org/apache/iceberg/ManifestWriter.java` - Enhanced to store map location
- `core/src/main/java/org/apache/iceberg/BaseRewriteFiles.java` - API for associating maps

**Features:**
- ManifestWriter.setCompactionMapLocation() method
- BaseRewriteFiles.setCompactionMapLocation() for rewrite operations
- Backward compatible - defaults to null when not set
- Integration tests demonstrating the API surface

**Integration Points:**
```
RewriteDataFilesSparkAction
    ↓
SparkBinPackFileRewriteRunner
    ↓
RewriteFileGroup (with FilePositionMapping support)
    ↓
RewriteDataFilesCommitManager.commitFileGroups()
    ├─ buildCompactionMap() ──> CompactionMapBuilder
    ├─ writeCompactionMap()  ──> CompactionMaps.write()
    └─ setCompactionMapLocation() ──> BaseRewriteFiles
            ↓
MergingSnapshotProducer.newRollingManifestWriter()
    └─ passes to ManifestWriter.setCompactionMapLocation()
            ↓
ManifestWriter.toManifestFile()
    └─ includes compactionMapLocation in manifest metadata
```

### Phase 6: Deletion Vector (DV) Remapping Infrastructure (Completed)

**Status:** ✅ Completed January 10, 2026

**Files:**
- `core/src/main/java/org/apache/iceberg/deletes/DVPositionWriter.java` - Utility for writing DVs from position collections
- `core/src/main/java/org/apache/iceberg/deletes/RemappedDVWriter.java` - Helper for N:M remapping scenarios
- `core/src/main/java/org/apache/iceberg/deletes/DVPositionReader.java` - Reader for extracting positions from DV files

**Features:**
- **DVPositionWriter**: Convenience wrapper for writing deletion vectors from pre-computed position collections
  - Takes `Collection<Long>` of positions to mark as deleted
  - Returns null for empty position collections (no DV file needed)
  - Internally uses `BaseDVFileWriter` with no previous deletes to load
  - Handles resource cleanup automatically
- **RemappedDVWriter**: Helper for writing multiple DVs after N:M compaction (one source to many targets)
  - Takes `Map<String, Set<Long>>` from `PositionDeleteRemapper.remapDV()`
  - Creates `OutputFileFactory` internally for proper file naming
  - Skips empty position sets automatically
  - Returns `List<DeleteFile>` ready for commit
- **PositionDeleteRemapper Enhancement**: Extended to support deletion vector remapping
  - `remapDV(DeleteFile dv, FileIO io)` method for DV remapping
  - Returns `Map<String, Set<Long>>` of target files to remapped positions
  - Handles passthrough for non-compacted files
  - Properly drops positions in gaps (already deleted during compaction)

**Key Design Decisions:**
- DVPositionWriter returns null for empty collections to avoid creating unnecessary DV files
- RemappedDVWriter creates one DV per target file for N:M scenarios
- Reuses existing CompactionMap infrastructure for position transformation logic
- Clean separation: DVPositionWriter for single-file writes, RemappedDVWriter for multi-file scenarios

### Phase 7: DV Remapping Integration Testing (Completed)

**Status:** ✅ Completed January 10, 2026

**Files:**
- `core/src/test/java/org/apache/iceberg/TestDVRemappingEndToEnd.java` - Comprehensive end-to-end integration tests
- `core/src/test/java/org/apache/iceberg/deletes/TestDVPositionWriter.java` - Unit tests for DVPositionWriter
- `core/src/test/java/org/apache/iceberg/deletes/TestRemappedDVWriter.java` - Unit tests for RemappedDVWriter
- `core/src/test/java/org/apache/iceberg/TestCompactionConflictDetectionDV.java` - Conflict detection with DVs

**Test Coverage:**

**TestDVRemappingEndToEnd** (6 integration tests):
1. **testSimpleDVRemapping()** - Basic 1:1 source to target remapping with full verification
2. **testDVRemappingWithGaps()** - Positions in gaps are correctly dropped from output
3. **testDVRemappingMultipleSourcesOneTarget()** - N:1 compaction with correct offset handling
4. **testDVRemappingAllPositionsDeleted()** - All positions in gaps returns empty result
5. **testDVRemappingNonCompactedFile()** - Passthrough for non-compacted files
6. **testDVRemappingLargeNumberOfPositions()** - Stress test with 1000 positions

**TestDVPositionWriter** (5 unit tests):
- Writing single position
- Writing multiple positions
- Handling empty position collections (returns null)
- Writing large position sets
- Reading back written DVs for verification

**TestRemappedDVWriter** (5 unit tests):
- Writing single target file
- Writing multiple target files
- Skipping empty position sets
- Handling all-empty positions
- Verifying written DV correctness

**TestCompactionConflictDetectionDV** (1 test, disabled):
- Conflict detection with deletion vectors
- Currently disabled due to manifest timing issue (see compaction_maps_errata.md)

**Key Test Scenarios:**
- Simple 1:1 remapping validates basic functionality
- Gap handling ensures positions deleted during compaction are properly dropped
- N:1 compaction tests validate offset calculations when multiple sources map to one target
- Non-compacted file passthrough ensures DVs for non-compacted files are preserved
- Large-scale stress test validates performance with thousands of positions

**Validation Methodology:**
- Write DVs with known positions
- Remap using CompactionMap
- Verify remapped positions match expected offsets
- Read back written DVs to confirm correctness
- Test edge cases (empty positions, gaps, passthrough)

### Phase 8: Remapping Algorithm Optimization (Completed)

**Status:** ✅ Completed January 13, 2026

**Overview:**

The initial implementation used naive linear search (O(m) per position lookup) in `GenericCompactionMap.GenericFileMapping.runForPosition()`. For workloads with n position deletes and m runs, this resulted in O(n*m) total cost. A five-phase optimization implemented multiple strategies with automatic selection, achieving 1.1-32x speedup depending on workload characteristics (validated via JMH benchmarks January 2026).

**Implementation Summary:**

| Phase | Strategy | Complexity | Best For | Status |
|-------|----------|------------|----------|--------|
| 8.1-8.2 | Binary Search | O(log m) per position | 10 ≤ m < 100 | ✅ Complete |
| 8.3 | Interval Tree | O(log m) with tree | m ≥ 100 | ✅ Complete |
| 8.4 | Stream Join | O(n + m) bulk | Sorted, m ≈ n | ✅ Complete |
| 8.5 | Range Query | O(m log n) bulk | High fan-in (n >> m) | ✅ Complete |
| 8.6 | Bulk API Integration | - | PositionDeleteRemapper | ✅ Complete |
| 8.7 | Smart Selector | - | Automatic strategy choice | ✅ Complete |

**Files:**

**Core Strategy Implementation:**
- `core/src/main/java/org/apache/iceberg/RemappingStrategy.java` - Interface with factory and bulk API
- `core/src/main/java/org/apache/iceberg/LinearSearchStrategy.java` - Baseline O(m) algorithm
- `core/src/main/java/org/apache/iceberg/BinarySearchStrategy.java` - O(log m) optimized search
- `core/src/main/java/org/apache/iceberg/IntervalTreeStrategy.java` - O(log m) balanced tree
- `core/src/main/java/org/apache/iceberg/StreamJoinStrategy.java` - O(n + m) merge-join for sorted
- `core/src/main/java/org/apache/iceberg/RangeQueryStrategy.java` - O(m log n) inverted query

**Smart Selection:**
- `core/src/main/java/org/apache/iceberg/RemappingAlgorithmSelector.java` - Automatic optimal strategy selection
- `core/src/main/java/org/apache/iceberg/PositionDeleteRemapper.java` - Updated to use smart selector

**Testing:**
- `core/src/test/java/org/apache/iceberg/TestRemappingStrategies.java` - 44 comprehensive tests
- `core/src/test/java/org/apache/iceberg/TestRemappingStrategiesIntegration.java` - 4 integration tests
- `core/src/test/java/org/apache/iceberg/TestRemappingAlgorithmSelector.java` - 11 selector tests

**Benchmarking:**
- `core/src/jmh/java/org/apache/iceberg/RemappingAlgorithmBenchmark.java` - JMH benchmark suite
- `core/src/jmh/java/org/apache/iceberg/RemappingBenchmarkUtils.java` - Benchmark utilities
- `REMAPPING_BENCHMARKS.md` - Comprehensive benchmark documentation

**Measured Performance (January 22, 2026 benchmarks):**

Speedup vs LinearSearch baseline (sorted=true, gap=0.0):

| Scale | Best Strategy | Speedup | Time (µs) |
|-------|---------------|---------|-----------|
| n=1000, m=10 | StreamJoin | 1.4x | 28 |
| n=1000, m=100 | StreamJoin | 1.3x | 30 |
| n=1000, m=1000 | StreamJoin | 1.2x | 36 |
| n=10000, m=10 | RangeQuery | 6.5x | 48 |
| n=10000, m=100 | StreamJoin | 3.3x | 361 |
| n=10000, m=1000 | StreamJoin | 3.4x | 371 |
| n=100000, m=10 | RangeQuery | 6.7x | 529 |
| n=100000, m=100 | StreamJoin | 23.6x | 889 |
| n=100000, m=1000 | StreamJoin | 32.4x | 5380 |

*Note: Theoretical complexity analysis (ops counts) replaced with empirical benchmark data.*

**Smart Algorithm Selection:**

The `RemappingAlgorithmSelector` automatically chooses optimal strategy based on:

1. **Run count (m)**: Number of runs in compaction map
2. **Position count (n)**: Number of positions to remap
3. **Sortedness**: Whether positions are sorted (detected via sampling)
4. **Gap ratio**: Percentage of source range not covered by runs

**Selection Rules (updated January 22, 2026):**
- Unsorted: IntervalTree (wins 24/27 unsorted scenarios)
- Sorted + sparse (gapRatio > 0.3): RangeQuery (skip gaps efficiently)
- Sorted + dense + n >= 10000 + m >= 100: StreamJoin (bulk merge-join)
- Sorted + other: RangeQuery (default for sorted data)

Selection overhead: ~5% average vs optimal strategy.

**Primitive Array API (February 2026):**

All remapping strategies now support primitive `long[]` arrays to eliminate boxing overhead:

```java
// Primitive API (recommended for high performance)
long[] positions = new long[] {0, 10, 50, 100, 500};
RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
RemappingStrategy strategy = selector.selectOptimal(mapping, positions);
Map<Long, CompactionMap.Run> mappedRuns = strategy.runForPositions(positions);

// List<Long> API (deprecated but still supported)
List<Long> positionList = Arrays.asList(0L, 10L, 50L, 100L, 500L);
strategy.runForPositions(positionList);  // @Deprecated
```

**Performance benefit:** At 1M positions, primitive APIs eliminate ~16 bytes per position boxing overhead, reducing remap% from 29-33% to ~5-10%.

**Example Usage:**

```java
// Automatic selection in PositionDeleteRemapper (uses primitive API internally)
Map<String, long[]> result = remapper.remapPositionsBulkPrimitive(sourceFile, positions);

// Manual strategy selection for specific workload
if (n > m * 100) {
  // High fan-in: use range query
  strategy = new RangeQueryStrategy(runs);
} else if (sorted) {
  // Sorted: use stream join
  strategy = new StreamJoinStrategy(runs);
} else {
  // General case: use binary search or interval tree
  strategy = RemappingStrategy.Factory.create(runs);
}
```

**JMH Benchmarking:**

Comprehensive benchmark suite with 54 parameter combinations:
- **numRuns**: 10, 100, 1000 (m)
- **numPositions**: 1000, 10000, 100000 (n)
- **gapRatio**: 0.0 (dense), 0.3 (moderate), 0.5 (sparse)
- **sorted**: true, false

**Run benchmarks:**
```bash
# Run all scenarios (takes 2-3 hours)
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhOutputPath=benchmark/remapping-results.txt

# Run specific scenario
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhParams="numRuns=100,numPositions=10000,sorted=true"

# Test only smart selector
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark.smartSelector
```

See `REMAPPING_BENCHMARKS.md` for detailed documentation on running and interpreting benchmarks.

**Test Coverage:**

- **82 total tests** covering remapping optimization
  - TestRemappingStrategies: 44 comprehensive unit tests
  - TestRemappingStrategiesIntegration: 4 integration tests
  - TestRemappingAlgorithmSelector: 11 selector tests
  - Property-based tests comparing all strategies with 10,000+ lookups
  - Edge cases: empty, single run, gaps, boundaries, unsorted
  - Performance validation: parameterized with 10, 100, 1000 runs

**Key Design Decisions:**

**Why Multiple Strategies?**
- Different workloads have different optimal algorithms
- Stream join best when m ≈ n (linear scan of both)
- Range query best when n >> m (high fan-in scenarios)
- Crossover point: m ≈ n / log n

**Why Lazy Initialization?**
- Strategy construction has validation overhead
- Avoids cost until first lookup
- Only happens once per FileMapping

**Why Bulk API?**
- Per-position lookup: O(n log m) even with binary search
- Bulk sorted: O(n + m) with stream join
- Bulk high fan-in: O(m log n) with range query
- 5-10x speedup over repeated single-position calls

### Phase 8.8: Bulk Bitmap Construction and Int-Typed DV Path (Completed)

**Status:** ✅ Completed May 12, 2026

**Overview:**

Profiling of the DV remap phase identified `RoaringPositionBitmap.set(long)` called per position as the dominant cost in the bitmap-construction step. Every call recomputed the upper-32-bit key, located the appropriate sub-bitmap, and inserted into it — work that can be amortized across the input. Three changes collectively eliminate that per-call overhead and clean up the DV remap path.

**Bulk bitmap construction:**

- `RoaringPositionBitmap.setAll(long[] positions)` — single-bitmap fast path when all positions fit in `bitmaps[0]` (the common case for DVs and PD files with rows under 2^32). Multi-bitmap fall-through partitions positions by upper-32-bit key and dispatches `RoaringBitmap.addN` per sub-bitmap.
- `RoaringPositionBitmap.setAll(int[] positions)` — direct entry into `bitmaps[0]` for callers that already have positions as 32-bit ints.
- `PositionDeleteIndex.delete(long[] positions)` — new default interface method (loop by default) overridden by `BitmapPositionDeleteIndex` to forward to `bitmap.setAll(...)`.
- `Deletes.toPositionIndexes` and `toPositionIndex(CloseableIterable<Long>, ...)` — V2 PD read paths now accumulate per-data-file positions in a primitive `long[]` buffer with array doubling, then flush via the bulk interface call.

**Measured speedup (DVRemappingPhaseBenchmark, single fork, 5 iter × 3s):**

| numDeletes | `set(long)` per value | `setAll(long[])` | speedup |
|-----------:|----------------------:|-----------------:|--------:|
|      1,000 |     5.93 µs |     3.29 µs | 1.80× |
|     10,000 |    64.2 µs  |    41.8 µs  | 1.54× |
|    100,000 |     648 µs  |     446 µs  | 1.45× |
|  1,000,000 |    6447 µs  |    4624 µs  | 1.39× |

The win narrows at larger sizes because the per-call routing cost amortizes against the actual bitmap-container work, but the structural improvement (1.4–1.8×) propagates to every place a bitmap is constructed from a known list.

**Selector sorted-hint:**

`RemappingAlgorithmSelector.selectOptimal(mapping, positions)` samples up to 1000 elements to detect sortedness. When the caller has structural knowledge — e.g., positions iterated from a `RoaringBitmap` are sorted by construction — the new overload `selectOptimal(mapping, positions, Boolean.TRUE)` skips the sample. Negligible cycle savings on its own, but it documents an invariant that would otherwise be silently rechecked.

**Int-typed DV remap entry point:**

`PositionDeleteRemapper.remapPositionsBulkDV(String sourceFile, int[] positions)` exposes a DV-shaped API that takes `int[]` and returns `Map<String, int[]>`. The algorithm itself still operates on `long` internally (strategies are long-typed), but the new entry point eliminates the caller-side narrowing loop and halves the per-target output array width (`int[]` vs `long[]`). Benchmarked end-to-end against the long-typed path, this change is perf-neutral at the microbenchmark level — most conversion cost lives in the widening half, which still happens because the strategy implementations are long-typed. Producing a real measurable cycle win would require duplicating or refactoring the strategies to operate on `int[]` natively.

**Files:**

- `core/src/main/java/org/apache/iceberg/deletes/RoaringPositionBitmap.java` - `setAll(long[])`, `setAll(int[])`
- `core/src/main/java/org/apache/iceberg/deletes/PositionDeleteIndex.java` - `delete(long[])` default method
- `core/src/main/java/org/apache/iceberg/deletes/BitmapPositionDeleteIndex.java` - `delete(long[])` override
- `core/src/main/java/org/apache/iceberg/deletes/Deletes.java` - bulk accumulation in `toPositionIndexes` / `toPositionIndex`
- `core/src/main/java/org/apache/iceberg/RemappingAlgorithmSelector.java` - `selectOptimal(mapping, positions, sortedHint)`
- `core/src/main/java/org/apache/iceberg/PositionDeleteRemapper.java` - `remapPositionsBulkDV(String, int[])` and `remapWithParallelArraysInt`
- `core/src/jmh/java/org/apache/iceberg/deletes/DVRemappingPhaseBenchmark.java` - `bitmapConstructFromArrayBulk` and raw-bitmap construction benchmarks
- `core/src/jmh/java/org/apache/iceberg/DVRemapPathBenchmark.java` - end-to-end old vs new DV remap path comparison

**Tests:**

- `core/src/test/java/org/apache/iceberg/deletes/TestRoaringPositionBitmap.java` - tests for `setAll(long[])`, including single-key fast path and multi-key fall-through.

### Phase 9: Chained Compaction Map Support (Completed)

**Status:** ✅ Completed February 2, 2026

**Overview:**

When multiple compactions occur between a transaction's start and commit, compaction maps must be composed (chained) to correctly remap positions through the entire chain. For example, if F1 → F2 (via M1) and F2 → F3 (via M2), a transaction with deletes for F1 needs to remap F1 → F3.

**Files:**

- `core/src/main/java/org/apache/iceberg/CompactionMapChain.java` - Chain holder with lazy composition
- `core/src/main/java/org/apache/iceberg/CompactionMaps.java` - Added `compose(m1, m2)` method
- `api/src/main/java/org/apache/iceberg/exceptions/ChainedCompactionMapsException.java` - Exception for chain detection
- `core/src/main/java/org/apache/iceberg/PositionDeleteRemapper.java` - Updated to accept chains

**Composition Algorithm:**

For runs r1 in M1 (F1→F2) and r2 in M2 (F2→F3):

```
r1: sourcePos=100, targetPos=0, length=50   (F1[100,150) → F2[0,50))
r2: sourcePos=0, targetPos=200, length=100  (F2[0,100) → F3[200,300))

Overlap in F2: [0,50) ∩ [0,100) = [0,50)

Composed run:
  sourcePos = 100 (from r1)
  targetPos = 200 (r2.targetPos + offset where offset=0)
  length = 50
  → F1[100,150) → F3[200,250)
```

**Key Design Decisions:**

1. **Lazy composition**: Chains are composed on-demand when a mapping is requested, with results cached
2. **Per-file caching**: Each source file's composed mapping is cached separately
3. **Validation**: Chain continuity verified via `m1.targetSnapshotId == m2.sourceSnapshotId`
4. **Gap handling**: Positions in gaps (deleted during compaction) correctly return null

**Test Coverage:**

- `TestCompactionMapChain` - 9 tests for chain building and composition
- `TestCompactionMapComposition` - 8 tests for the composition algorithm
- `TestChainedCompactionMapsDetection` - 5 tests for chain detection
- `TestSerializableIsolationWithCompaction` - SERIALIZABLE tests with chained compactions

**Run tests:**
```bash
./gradlew :iceberg-core:test --tests "*Chain*" --tests "*Composition*"
```

### Overlapping Compactions

**Behavior:** When two compactions target overlapping file sets (e.g., Compaction A compacts {F1, F2} while Compaction B compacts {F2, F3}), the first to commit wins and the second fails.

**Detection Mechanism:**

Overlapping compactions are detected at commit time via `validateDataFilesExist()` in `MergingSnapshotProducer`:

```
Compaction A starts (files: [F1, F2])
    ↓
Compaction B starts (files: [F2, F3])  ← overlapping F2
    ↓
Compaction A commits → Snapshot S2
    - Deletes F1, F2
    - Adds A_output.parquet
    ↓
Compaction B attempts commit
    - validateDataFilesExist() scans S1→S2
    - Finds F2 was DELETED in REPLACE operation at S2
    - THROWS: ValidationException("Cannot commit, missing data files: F2")
```

**Validation Logic** (`MergingSnapshotProducer.java:869-918`):

The validation scans manifest history for DELETED entries between the starting snapshot and current snapshot:

```java
// Simplified logic
entry.status() != ManifestEntry.Status.ADDED     // Not additions
&& newSnapshots.contains(entry.snapshotId())      // In relevant snapshot range
&& requiredDataFiles.contains(entry.file().location())  // Our source files
```

**Key Distinctions:**

| Scenario | Error Type | Can Retry? |
|----------|-----------|------------|
| Overlapping compactions | `ValidationException` | Must re-plan with new files |
| Position deletes on compacted files | `CompactionConflictException` | Yes, with remapping |
| Chained compactions (sequential) | Works correctly | N/A |

**Why No Automatic Rebasing:**

Unlike chained compactions (which compose maps sequentially), overlapping compactions cannot be automatically rebased because:

1. **Source files are gone:** The overlapping file (F2) no longer exists, so there's nothing to compact
2. **No semantic merge:** Compaction maps only track position transformations, not file content merging
3. **Output collision:** Both compactions would claim to produce "the" compacted version of F2's data

**Recovery:**

When a compaction fails due to overlap:

1. The partial commit is rejected (no data corruption)
2. Output files from the failed compaction should be cleaned up
3. Re-plan the compaction with the current table state (which now includes A's output)

```java
// Example recovery flow
try {
    compactionB.commit();
} catch (ValidationException e) {
    if (e.getMessage().contains("missing data files")) {
        // Clean up orphan files from failed compaction
        cleanupOrphanFiles(compactionBOutputFiles);

        // Re-plan with current table state
        table.refresh();
        Set<DataFile> newFilesToCompact = planCompaction(table);
        // newFilesToCompact now includes A_output.parquet instead of F1, F2
        executeCompaction(newFilesToCompact);
    }
}
```

**Retry Mechanism:**

Iceberg includes built-in retry logic (`SnapshotProducer.java:424-467`):

```java
Tasks.foreach(ops)
    .retry(COMMIT_NUM_RETRIES)
    .exponentialBackoff(...)
    .onlyRetryOn(CommitFailedException.class)
    .run(taskOps -> {
        Snapshot newSnapshot = apply();  // Re-validates against fresh metadata
        taskOps.commit(base, updated);
    });
```

However, retrying an overlapping compaction will fail repeatedly because `refresh()` gets the latest metadata but doesn't re-plan the compaction. The source files remain in the operation's delete set even though they no longer exist.

**Test Coverage:**

```bash
# Run overlapping compaction tests
./gradlew :iceberg-core:test --tests "TestCompactionConflictDetection.testOverlappingCompactionsSecondFails"
./gradlew :iceberg-core:test --tests "TestCompactionConflictDetection.testNonOverlappingCompactionsBothSucceed"
```

## Testing

### Unit Tests

**TestCompactionMapSerialization** - Avro serialization/deserialization
- Round-trip serialization
- Multiple file mappings
- Position mapping correctness
- Copy functionality

**TestCompactionMapBuilder** - Builder and run merging
- Simple single file mappings
- Consecutive run merging
- Non-consecutive runs (gaps)
- Multiple file mappings
- Validation (null checks, negative values)
- Complex merging patterns

**TestCompactionMapsStorage** - Storage and configuration
- Table property defaults
- Location generation with default and custom paths
- Unique file naming with UUIDs

**TestPositionDeleteRemapper** - Remapping logic
- Basic remapping with offsets
- Multiple runs and gaps
- Handling non-compacted files
- Querying compacted files
- Checking remapping requirements

**TestCompactionMapIntegration** - Integration points
- ManifestWriter stores/retrieves map locations
- BaseRewriteFiles API for setting/getting locations
- Backward compatibility with null locations

**TestCompactionMapCommitFlow** - Commit flow integration
- RewriteDataFilesCommitManager automatic map generation
- Compaction map writing to metadata location
- ManifestFile includes compactionMapLocation
- Integration with BaseRewriteFiles.setCompactionMapLocation()
- Backward compatibility when property disabled

**TestCompactionConflictDetection** - Conflict detection
- Detection of position deletes referencing compacted files
- CompactionConflictException with actionable error messages
- Compaction map locations provided in exception
- Multiple compaction scenarios
- V2 and V3 table format support

**TestCompactionConflictResolution** - Conflict resolution
- Loading compaction maps from manifest history
- Remapping position deletes using PositionDeleteRemapper
- Retrying transactions with remapped deletes
- Verification of correct delete application
- End-to-end resolution workflow

**TestSerializableIsolationWithCompaction** - SERIALIZABLE isolation
- DELETE operations succeed when REPLACE has compaction maps
- DELETE operations fail when REPLACE lacks compaction maps
- SNAPSHOT isolation ignores REPLACE operations
- Filtered conflict detection respects partition boundaries
- Validation of isolation semantics

**TestRemappingStrategies** - Remapping algorithm optimization
- 44 comprehensive tests covering all strategies
- Property-based tests comparing strategies with 10,000+ lookups
- Edge cases: empty, single run, gaps, boundaries
- Validation: unsorted runs, overlapping runs
- Performance tests: parameterized with 10, 100, 1000 runs
- All strategies produce identical results

**TestRemappingStrategiesIntegration** - Bulk remapping integration
- StreamJoin optimal for sorted positions
- RangeQuery optimal for high fan-in scenarios
- Realistic workload distributions
- Linear scaling validation

**TestRemappingAlgorithmSelector** - Smart algorithm selection
- RangeQuery selection for few runs
- StreamJoin selection for sorted positions
- RangeQuery selection for high fan-in with gaps
- BinarySearch selection for medium runs
- IntervalTree selection for large runs
- Gap ratio estimation
- Sortedness detection
- Boundary condition testing

### Spark Integration Tests

**TestSparkCompactionConflictResolution** - End-to-end Spark integration
- **Test 8: Conflict Detection** (2 parameterized test cases)
  - Triggers `CompactionConflictException` when files are compacted
  - Verifies exception provides compaction map locations
  - Tests across v2 Parquet and v2 ORC formats

- **Test 9: Manual Conflict Resolution Workflow** (4 parameterized test cases)
  - Verifies compaction maps contain **real target file paths** (not "target-pending" placeholders)
  - **Critical bug fix validation:** Ensures buffer-and-record pattern correctly resolves file paths
  - Validates target file paths have proper format (contain '/', end with .parquet or .orc)
  - Confirms `PositionDeleteRemapper` can be created successfully
  - Verifies basic remapping operation succeeds
  - Tests across v2 Parquet, v2 ORC, v3 Parquet, v3 ORC

- **Test 10: Multiple Compaction Rounds** (4 parameterized test cases)
  - Verifies compaction map provided after snapshot advancement
  - Tests conflict detection with previously compacted files
  - Validates compaction map has correct source→target mappings with real file paths
  - Confirms resolution workflow can proceed with provided maps
  - Tests across all format combinations (v2/v3, Parquet/ORC)

**TestBinPackWithPositionTracking** - Position tracking integration
- Position tracking during bin-pack rewrites
- Compaction map generation with position deletes
- Verification of correct run structures with gaps

**TestSparkBinPackWithPositionDeletes** - Comprehensive position tracking (Spark 3.5)
- **Test 1: Single File Compaction** - Multiple sources to single target with deletes (4 test cases: v2/v3 × Parquet/ORC)
- **Test 2: Multiple Target Files** - N:M compaction with offset validation (4 test cases)
- **Test 3: High Delete Ratio** - 80% deletion with large gap handling (4 test cases)
- **Test 4: Sparse Deletes** - Many small gaps throughout files (4 test cases)
- **Test 5: Partitioned Tables** - Position tracking on partitioned tables (4 test cases)
  - **Enabled in commit 8b811d951:** Fixed schema mismatch bug in PartitionedDataWriter
  - Verifies compaction maps generated correctly for partitioned tables
  - Tests partition boundary handling and gap correctness per partition
  - Validates data correctness across partitions

### Test Coverage

- **250+ test cases passing** across core compaction maps, DV support, remapping optimization, chained maps, and Spark integration
  - Core compaction map tests: ~60 tests (serialization, builder, storage, conflict detection/resolution)
  - DV remapping tests: 16 tests (10 unit + 6 integration)
  - **Chained compaction map tests: 22 tests**
    - TestCompactionMapChain: 9 tests
    - TestCompactionMapComposition: 8 tests
    - TestChainedCompactionMapsDetection: 5 tests
  - **Remapping optimization tests: 78 tests**
    - TestRemappingStrategies: 37 comprehensive unit tests
    - TestRemappingStrategiesIntegration: 4 integration tests
    - TestRemappingAlgorithmSelector: 15 selector tests
    - TestRemappingBenchmarkUtils: 22 tests
  - **SERIALIZABLE isolation tests: 12 tests** (including chained compaction scenarios)
  - **Spark integration tests: 36 parameterized tests** (across format versions and file formats)
    - TestSparkCompactionConflictResolution: 18 tests per Spark version (Spark 3.5 and 4.0)
  - **JMH benchmarks: 8 benchmarks × 54 scenarios = 324+ benchmark configurations**
  - 1 test disabled (TestCompactionConflictDetectionDV - manifest timing issue)
- **All enabled tests passing**
- Coverage includes:
  - Happy paths and edge cases
  - Error conditions and validation
  - Backward compatibility
  - Integration between components
  - Isolation level semantics
  - Conflict detection and resolution workflows
  - Deletion vector remapping scenarios
  - N:M compaction (multiple sources to multiple targets)
  - Gap handling (positions deleted during compaction)
  - **Remapping algorithm optimization** (1.1-32x speedup validated via JMH)
  - **Smart algorithm selection** (automatic optimal strategy choice)
  - **Bulk API integration** (5-10x faster than per-position remapping)
  - **End-to-end Spark workflows** (bin-pack with position deletes, conflict resolution)
  - **Partitioned table support** (v2/v3 × Parquet/ORC)
  - **Target-pending bug fix verification** (critical fix ensuring real file paths in compaction maps)
  - Large-scale stress testing (1000+ positions, 10,000+ lookups)

### Running Tests

**Core Tests:**
```bash
# Run all compaction map tests
./gradlew :iceberg-core:test --tests "*CompactionMap*"

# Run all DV remapping tests
./gradlew :iceberg-core:test --tests "*DV*"

# Run all remapping strategy tests
./gradlew :iceberg-core:test --tests "*RemappingStrategies*"
./gradlew :iceberg-core:test --tests "*RemappingAlgorithmSelector*"

# Run specific test class
./gradlew :iceberg-core:test --tests "org.apache.iceberg.TestCompactionMapBuilder"
./gradlew :iceberg-core:test --tests "org.apache.iceberg.TestDVRemappingEndToEnd"
./gradlew :iceberg-core:test --tests "org.apache.iceberg.deletes.TestDVPositionWriter"
./gradlew :iceberg-core:test --tests "org.apache.iceberg.TestRemappingStrategies"
./gradlew :iceberg-core:test --tests "org.apache.iceberg.TestRemappingAlgorithmSelector"

# Run all compaction, DV, and remapping tests together
./gradlew :iceberg-core:test --tests "*CompactionMap*" --tests "*DV*" --tests "*Remapping*"

# Run with verbose output
./gradlew :iceberg-core:test --tests "*CompactionMap*" --info
```

**Spark Integration Tests (Spark 3.5):**
```bash
# Run all conflict resolution tests (Tests 8, 9, 10)
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests TestSparkCompactionConflictResolution

# Run specific test
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests TestSparkCompactionConflictResolution.testConflictDetectionWithSparkAction
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests TestSparkCompactionConflictResolution.testManualConflictResolutionWorkflow
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests TestSparkCompactionConflictResolution.testMultipleCompactionRounds

# Run position tracking integration tests
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests TestBinPackWithPositionTracking

# Run comprehensive position delete tests (Tests 1-5) - includes partitioned tables
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests TestSparkBinPackWithPositionDeletes

# Run all Spark compaction tests
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "*Compaction*"
```

**JMH Benchmarks:**
```bash
# Run all remapping algorithm benchmarks (takes 2-3 hours)
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhOutputPath=benchmark/remapping-results.txt

# Run specific scenario
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhParams="numRuns=100,numPositions=10000,sorted=true"

# Compare StreamJoin vs RangeQuery for sorted positions
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex="RemappingAlgorithmBenchmark.streamJoin|RemappingAlgorithmBenchmark.rangeQuery" \
    -PjmhParams="sorted=true"

# Test only smart selector
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark.smartSelector
```

See `REMAPPING_BENCHMARKS.md` for comprehensive benchmark documentation.

**Expected Output:**
- Core compaction map tests: ~20 passing
- DV remapping tests: 16 passing
- Remapping optimization tests: 59 passing
- Spark integration tests: 30 parameterized test cases passing
  - TestSparkCompactionConflictResolution: 10 tests (Tests 8-10)
  - TestSparkBinPackWithPositionDeletes: 20 tests (Tests 1-5)
- Total: 140+ tests passing
- JMH benchmarks: 324 benchmark configurations (6 benchmarks × 54 scenarios)

## Spark Implementation Details

### Position Tracking Architecture (Spark 3.5)

**Read Side:**
- Normal scans with metadata columns (`_file`, `_pos`)
- File path filtering to select specific files from rewrite group
- Position deletes applied during scan (standard Iceberg behavior)

**Write Side:**
- PositionTrackingDataWriter wraps delegate writer
- Extracts `_file` and `_pos` from each row
- Records position mappings to PositionMappingCoordinator
- Filters metadata columns before writing to data files
- **Schema Filtering (Fixed in 8b811d951):** SparkWrite.WriterFactory filters metadata columns for both UnpartitionedDataWriter and PartitionedDataWriter to prevent schema mismatch errors

**Coordination:**
- PositionMappingCoordinator aggregates mappings from distributed executors
- Identifies runs by detecting gaps in position sequences
- Returns FilePositionMapping objects for commit flow

**Integration:**
- SparkBinPackFileRewriteRunner enables tracking based on table property
- BaseFileRewriteCoordinator fetches aggregated mappings
- RewriteDataFilesCommitManager builds and writes compaction maps

**Code Locations:**
```
spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/
├── PositionMappingCoordinator.java (coordination layer)
├── source/
│   ├── PositionTrackingDataWriter.java (write-side tracking)
│   └── SparkWriteBuilder.java (schema filtering)
└── actions/
    ├── SparkBinPackFileRewriteRunner.java (read/write integration)
    └── BaseFileRewriteCoordinator.java (mapping fetch)
```

### Scan Type Selection

**Normal Operations (Position Tracking Disabled):**
- Uses **staged scans** (`SparkStagedScanBuilder` → `SparkStagedScan`)
- Pre-computed `FileScanTasks` from rewrite planning
- No manifest re-scanning required
- Optimal performance (~10-20% faster)

**Position Tracking Enabled:**
- Uses **normal scans** (`SparkScanBuilder` → `SparkBatchQueryScan`)
- Filters to specific file paths using `_file = 'path'` predicates
- Explicitly selects `_file` and `_pos` metadata columns
- Slight performance overhead due to manifest re-scanning

**Why Normal Scans:**

Staged scans don't properly expose metadata columns to Spark's physical planner. When metadata columns are included in a staged scan's schema, Spark's optimizer prunes them away during the `V2ScanRelationPushDown` optimization phase, causing errors.

Normal scans fully support metadata columns through Spark's `SupportsMetadataColumns` interface and preserve them through the entire query planning pipeline.

### Performance Considerations

**Position Tracking Overhead:**
- Manifest re-scanning: ~5-10% overhead
- Filter evaluation on file paths: ~2-5% overhead
- Total: ~10-20% slower than staged scans

**Why Acceptable:**
- Compaction map generation is an opt-in feature (disabled by default)
- Used primarily for high-concurrency workloads where conflict resolution matters more than raw throughput
- Overhead only applies when `write.compaction-map.enabled=true`
- Most production workloads can absorb this cost for the safety guarantees

## Design Considerations

### 1. Row-Level Position Granularity

Compaction maps track positions at row granularity, inherited from position delete semantics:

```java
// Position deletes reference specific row positions
PositionDelete: (file_path: String, pos: Long)

// Compaction maps must transform at same granularity
Run: (sourcePosition: Long, targetPosition: Long, length: Long)
```

**Why Row-Level:**
- Position deletes are row-level, so remapping must be row-level
- Coarser granularity (blocks, pages) would lose precision needed for accurate remapping
- Enables exact position transformation: `source[42] → target[137]`

**Size Management:**
- Run-length encoding automatically merges consecutive positions
- Example: 1000 consecutive positions → single run `(0, 0, 1000)`
- Testing shows maps stay reasonable size for typical workloads
- Most compactions (bin-pack) produce highly compressed maps

### 2. One Source File Per FileMapping

- Each source file has its own FileMapping
- One source can map to one or more targets (via per-run target files)
- Multiple sources can map to same target (bin-pack)
- Per-run target files support source files spanning multiple targets
- This design supports efficient lookup and simple position transformation

### 3. Map Storage Per Snapshot

- Each compaction creates a new compaction map file
- Maps are immutable once written
- Old maps can be garbage collected when snapshots expire
- **Benefit**: Simple lifecycle management aligned with snapshots

### 4. No Map Size Enforcement

- `write.compaction-map.target-size-bytes` property exists but not enforced
- Currently used for documentation/monitoring only
- Future: Could warn or split maps when threshold exceeded
- **Current approach**: Run-length encoding keeps maps small enough

## Soundness Analysis

This section analyzes the correctness guarantees of compaction map generation. **If a compaction map is produced, it is sound** — meaning position remapping using the map will produce correct results.

### Invariants

The following invariants are maintained throughout the compaction map generation pipeline:

#### 1. Completeness: Every Written Row is Tracked

**Guarantee:** If a row is written to a target file, its position mapping is recorded.

**Implementation:**
- `PositionTrackingDataWriter.write()` is called for every row
- Each call buffers a `(sourceFile, sourcePos, targetPos)` tuple
- After commit, all buffered mappings are recorded to `PositionMappingCoordinator`
- The recording happens atomically with the writer commit

**Code path:**
```
write(row) → buffer mapping → commit() → recordBufferedMappingsWithActualPaths()
```

#### 2. Source Position Correctness

**Guarantee:** Source positions in the map match actual row positions in source files.

**Implementation:**
- Source positions come from Iceberg's `_pos` metadata column
- `_pos` is populated by the scan and represents the row's actual position
- Position deletes are applied during scan, so deleted rows never reach the writer

**Key insight:** The `_pos` column is authoritative — it comes from Iceberg's internal tracking, not user data.

#### 3. Target Position Correctness

**Guarantee:** Target positions in the map match actual row positions in target files.

**Implementation:**
- `outputPosition` counter starts at 0 for each target file
- Counter increments by 1 for each `write()` call
- This matches exactly how rows are written to Parquet/ORC files (0-indexed, sequential)

**Invariant:** `outputPosition` after N writes equals N, matching the target file's row count.

#### 4. Target File Path Correctness

**Guarantee:** Target file paths in the map are actual file paths, not placeholders.

**Implementation:**
- Target paths are extracted from `WriterCommitMessage` after `commit()` completes
- The commit message contains `DataFile` objects with actual paths
- Buffered mappings are recorded only after paths are known

**Historical note:** An early bug used placeholder strings; this was fixed by the buffer-and-record pattern (see `PositionTrackingDataWriter.recordBufferedMappingsWithActualPaths()`).

#### 5. Gap Correctness (Deleted Rows)

**Guarantee:** Gaps in runs represent rows that were deleted during compaction.

**Implementation:**
- Iceberg applies position deletes during the scan phase
- Deleted rows are never passed to `PositionTrackingDataWriter`
- Gaps appear naturally in source positions (e.g., positions 0, 1, 3, 4 with 2 deleted)
- `PositionMappingCoordinator.aggregateMappings()` detects gaps by checking consecutive positions

**Correctness argument:** A source position is in a run if and only if its row was written. Therefore:
- Positions in runs: rows exist in target (correctly remapped)
- Positions in gaps: rows were deleted (remapping correctly returns null)

#### 6. Multi-Target Correctness

**Guarantee:** When a source file spans multiple target files, each run has the correct target.

**Implementation:**
- Each `recordMapping()` call includes the actual target file path
- `PositionMappingCoordinator.aggregateMappings()` creates new runs when target files change
- `CompactionMapBuilder.canMerge()` only merges runs with matching target files

**Invariant:** Runs with different target files are never merged.

#### 7. Run Merging Correctness

**Guarantee:** Merged runs are mathematically equivalent to their constituent mappings.

**Implementation:** `CompactionMapBuilder.RunBuilder.canMerge()` requires:
1. Same target file
2. `nextSourcePosition == currentSourceEnd` (consecutive in source)
3. `nextTargetPosition == currentTargetEnd` (consecutive in target)

**Mathematical property:** If runs R1=(s1, t1, len1) and R2=(s2, t2, len2) satisfy:
- s2 = s1 + len1
- t2 = t1 + len1
- same target file

Then merged run (s1, t1, len1+len2) produces identical mappings for all positions.

#### 8. Serialization Correctness

**Guarantee:** Avro serialization preserves all mapping data.

**Implementation:**
- `GenericCompactionMap` implements `StructLike`, `IndexedRecord`, `SchemaConstructable`
- Schema field IDs are stable (defined in `CompactionMap.java`)
- Round-trip tested in `TestCompactionMapSerialization`
- Target path interning preserves string equality (uses `String.equals()`)

### Failure Modes and Mitigations

| Failure Mode | Detection | Mitigation |
|--------------|-----------|------------|
| Writer task failure | Spark task retry | Buffered mappings discarded on abort |
| Coordinator data loss | Map has fewer mappings | Commit manager logs warning; incomplete maps not written |
| Avro write failure | IOException during write | Compaction map location not set; rewrite proceeds without map |
| Schema mismatch | Avro deserialization error | Maps are immutable; schema evolution via optional fields |

### Verification

The soundness of compaction maps is verified by:

1. **Unit tests:** `TestCompactionMapBuilder`, `TestCompactionMapSerialization`
2. **Integration tests:** `TestDVRemappingEndToEnd`, `TestSparkBinPackWithPositionDeletes`
3. **Property-based tests:** `TestRemappingStrategies` (10,000+ lookups comparing all strategies)
4. **End-to-end Spark tests:** `TestSparkCompactionConflictResolution`

### Summary

**Compaction maps are sound by construction:**

1. **Row tracking is complete:** Every `write()` records a mapping
2. **Source positions are authoritative:** `_pos` comes from Iceberg's scan
3. **Target positions are sequential:** Counter matches file layout
4. **Target paths are actual:** Extracted from commit message
5. **Gaps represent deletes:** Rows not written have no mapping
6. **Multi-target is handled:** Per-run target files prevent cross-target errors
7. **Merging is conservative:** Only mathematically equivalent runs merge

If the compaction completes successfully and a compaction map is written, the map correctly represents the position transformations that occurred.

## Contributing

When working with compaction maps:

1. **Follow Iceberg Patterns**
   - Use Avro for serialization (not Java Serializable)
   - Follow naming conventions (kebab-case for properties)
   - Use TableOperations.metadataFileLocation() for file placement

2. **Maintain Backward Compatibility**
   - compactionMapLocation field is optional
   - Defaults to null for tables not using the feature
   - Existing code continues to work without changes

3. **Add Tests**
   - Unit tests for new functionality
   - Integration tests for cross-component interactions
   - Test backward compatibility scenarios

4. **Update Documentation**
   - Keep documentation updated with changes
   - Add examples for new APIs
   - Document limitations and workarounds
