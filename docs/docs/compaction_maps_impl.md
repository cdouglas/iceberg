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
  string target_file;
  array<Run> runs;
}

record Run {
  long source_position;
  long target_position;
  long length;
}
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
        "s3://bucket/data/compacted.parquet"   // Target file
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

// Remap DV to new target files
Map<String, Set<Long>> remappedPositions = remapper.remapDV(sourceDV, fileIO);

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

When position deletes reference compacted files:

```java
try {
    rowDelta.addDeletes(deleteFile);
    rowDelta.commit();
} catch (CompactionConflictException e) {
    // Get compacted files and map locations from exception
    Set<String> compactedFiles = e.compactedFiles();
    Map<String, String> mapLocations = e.compactionMapLocations();

    // Load compaction maps
    List<CompactionMap> maps = mapLocations.values().stream()
        .distinct()
        .map(loc -> CompactionMaps.read(fileIO.newInputFile(loc)))
        .collect(Collectors.toList());

    // Remap position deletes
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(maps.get(0));
    DeleteFile remappedDelete = remapDeleteFile(deleteFile, remapper);

    // Retry with remapped deletes
    RowDelta retry = table.newRowDelta();
    retry.addDeletes(remappedDelete);
    retry.commit();  // Should succeed
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

**Isolation Semantics:**
- **REPLACE with compaction map**: No read conflict (structural change only, data unchanged)
- **REPLACE without compaction map**: Validation failure (potential data change)
- **SNAPSHOT isolation**: REPLACE operations not checked (existing behavior)
- **SERIALIZABLE isolation**: REPLACE operations checked with compaction awareness

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

### Test Coverage

- **36+ test cases passing** across core compaction map and DV support
  - Core compaction map tests: ~20 tests
  - DV remapping tests: 16 tests (10 unit + 6 integration)
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
  - Large-scale stress testing (1000+ positions)

### Running Tests

```bash
# Run all compaction map tests
./gradlew :iceberg-core:test --tests "*CompactionMap*"

# Run all DV remapping tests
./gradlew :iceberg-core:test --tests "*DV*"

# Run specific test class
./gradlew :iceberg-core:test --tests "org.apache.iceberg.TestCompactionMapBuilder"
./gradlew :iceberg-core:test --tests "org.apache.iceberg.TestDVRemappingEndToEnd"
./gradlew :iceberg-core:test --tests "org.apache.iceberg.deletes.TestDVPositionWriter"

# Run all compaction and DV tests together
./gradlew :iceberg-core:test --tests "*CompactionMap*" --tests "*DV*"

# Run with verbose output
./gradlew :iceberg-core:test --tests "*CompactionMap*" --info
```

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

See [`docs/staged_scan_investigation.md`](../../docs/staged_scan_investigation.md) for detailed investigation and [`compaction_maps_errata.md`](compaction_maps_errata.md) for tradeoffs.

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

- Each source file has its own FileMapping to a target file
- One source can map to one target
- Multiple sources can map to same target (bin-pack)
- One source split across multiple targets requires multiple FileMappings
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
