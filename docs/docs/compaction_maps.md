---
title: "Compaction Maps"
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

# Compaction Maps

## Overview

Compaction maps enable concurrent transactions writing position deletes to coexist with compaction operations. When data files are compacted, existing position deletes that reference the old files become invalid. Compaction maps track the position transformations from source to target files, allowing transactions to automatically remap their position deletes when conflicts are detected.

## Motivation

In Apache Iceberg, **position deletes** identify deleted rows using `(file_path, row_position)` tuples. When a data file is compacted (rewritten with other files), the original file is removed and positions change in the new files. This creates a conflict:

**Without Compaction Maps:**
1. Transaction A reads snapshot S1 and writes position deletes referencing file F1
2. Transaction B compacts F1 into F2, creating snapshot S2
3. Transaction A tries to commit on top of S2
4. **Result:** Transaction A fails because F1 no longer exists

**With Compaction Maps:**
1. Transaction A reads snapshot S1 and writes position deletes referencing file F1
2. Transaction B compacts F1 into F2, creating snapshot S2 with a compaction map
3. Transaction A tries to commit on top of S2
4. Iceberg detects the conflict and automatically remaps position deletes from F1 → F2
5. **Result:** Transaction A commits successfully with remapped deletes

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

### SERIALIZABLE Isolation with Compaction Awareness

Compaction maps enable SERIALIZABLE isolation to distinguish between structural and logical data changes:

```java
// Enable compaction maps and SERIALIZABLE isolation
table.updateProperties()
    .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
    .set(TableProperties.DELETE_ISOLATION_LEVEL, "serializable")
    .commit();

// Start a DELETE transaction with SERIALIZABLE isolation
RowDelta rowDelta = table.newRowDelta()
    .validateFromSnapshot(startingSnapshotId)
    .conflictDetectionFilter(Expressions.equal("region", "us-west"))
    .validateNoConflictingDataFiles();  // Enable SERIALIZABLE

// Add position deletes
rowDelta.addDeletes(deleteFile);

// Meanwhile, another transaction compacts the data
// WITH compaction map:
//   - rowDelta.commit() succeeds (structural change only)
// WITHOUT compaction map:
//   - rowDelta.commit() throws ValidationException (potential data change)

try {
    rowDelta.commit();
} catch (ValidationException e) {
    // REPLACE without compaction map detected
    // Cannot safely proceed - data may have changed
    System.err.println("Concurrent compaction without map: " + e.getMessage());
}
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

## Configuration

### Table Properties

**`write.compaction-map.enabled`** (default: `false`)
- Controls whether compaction maps are generated during compaction operations
- Set to `true` to enable compaction map generation
- Can be enabled per-table or globally

**`write.compaction-map.target-size-bytes`** (default: `8388608` / 8 MB)
- Target size for compaction map files
- Used for validation/monitoring (not currently enforced)
- Future: May be used to split large maps across multiple files

**`write.delete.isolation-level`** (default: `"serializable"`)
- Controls isolation level for DELETE/UPDATE/MERGE operations
- `"serializable"`: Validate concurrent data changes including compaction-aware REPLACE checks
- `"snapshot"`: No validation of concurrent operations (weaker isolation)

**Example Configuration:**
```java
// Enable compaction maps for a table
table.updateProperties()
    .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
    .commit();

// Configure SERIALIZABLE isolation (default)
table.updateProperties()
    .set(TableProperties.DELETE_ISOLATION_LEVEL, "serializable")
    .commit();

// Adjust target size
table.updateProperties()
    .set(TableProperties.COMPACTION_MAP_TARGET_SIZE_BYTES, "16777216")  // 16 MB
    .commit();
```

### End-to-End Setup

To enable compaction maps with SERIALIZABLE isolation:

```java
// 1. Enable compaction maps
table.updateProperties()
    .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
    .set(TableProperties.DELETE_ISOLATION_LEVEL, "serializable")
    .commit();

// 2. Run compaction (maps will be generated automatically)
RewriteFiles rewrite = table.newRewrite();
sourceFiles.forEach(rewrite::deleteFile);
targetFiles.forEach(rewrite::addFile);
rewrite.commit();
// RewriteDataFilesCommitManager automatically builds and attaches compaction map

// 3. Concurrent DELETE operations will benefit from compaction awareness
RowDelta rowDelta = table.newRowDelta()
    .validateFromSnapshot(startingSnapshotId)
    .conflictDetectionFilter(filter)
    .validateNoConflictingDataFiles();  // SERIALIZABLE isolation

rowDelta.addDeletes(deleteFile);
rowDelta.commit();
// If compaction occurred: commit succeeds (structural change only)
// If no compaction map: ValidationException (potential data change)
```

### Metadata Location

Compaction maps respect the `write.metadata.path` table property:

```java
// Store compaction maps in custom location
table.updateProperties()
    .set(TableProperties.WRITE_METADATA_LOCATION, "s3://custom-bucket/metadata")
    .commit();

// Maps will be written to: s3://custom-bucket/metadata/compaction-map-*.avro
```

## Implementation Details

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
- Fallback logic for bin-pack scenarios without explicit position tracking

**Map Generation Flow:**
1. RewriteDataFilesCommitManager.commitFileGroups() receives RewriteFileGroups
2. Check if compaction maps are enabled via table property
3. Build compaction map using position mappings from file groups
4. Write map to metadata location
5. Set location on BaseRewriteFiles via setCompactionMapLocation()
6. Commit proceeds with map location attached to manifest

**Supported Scenarios:**
- **Bin-pack with position tracking**: Uses explicit FilePositionMapping data
- **Simple bin-pack (fallback)**: Single target file, sequential offset mapping
- **Multiple targets without tracking**: Logs warning, skips map generation

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

**Validation Flow:**
1. Transaction calls validateNoConflictingDataFiles() for SERIALIZABLE isolation
2. BaseRowDelta.validate() calls validateCompactionAwareConflicts()
3. Method iterates through REPLACE operations between starting and current snapshot
4. For each REPLACE, checks if compaction maps exist in manifest files
5. If maps exist: continue (no conflict, structural change only)
6. If no maps: check for conflicting files matching conflict detection filter
7. If conflicts found: throw ValidationException with clear message

### Phase 5: Compaction Integration (Completed)

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

**Validation Flow:**
```
RowDelta.commit() with SERIALIZABLE isolation
    ↓
BaseRowDelta.validate()
    ├─ validateNoCompactionConflicts() ──> CompactionMapValidator
    │   └─ Checks position deletes referencing compacted files
    │       Throws CompactionConflictException if conflicts found
    │
    └─ validateCompactionAwareConflicts() ──> MergingSnapshotProducer
        └─ Checks REPLACE operations for compaction maps
            ├─ With map: continue (structural change only)
            └─ Without map: throw ValidationException (data change)
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

### Test Coverage

- **50+ test cases** total across all test files
- **All tests passing**
- Coverage includes:
  - Happy paths and edge cases
  - Error conditions and validation
  - Backward compatibility
  - Integration between components
  - Isolation level semantics
  - Conflict detection and resolution workflows

### Running Tests

```bash
# Run all compaction map tests
./gradlew :iceberg-core:test --tests "*CompactionMap*"

# Run specific test class
./gradlew :iceberg-core:test --tests "org.apache.iceberg.TestCompactionMapBuilder"

# Run with verbose output
./gradlew :iceberg-core:test --tests "*CompactionMap*" --info
```

## Current State

### What's Complete

✅ **Core Infrastructure (Phases 1-5)**
- CompactionMap data structures with Avro serialization
- CompactionMapBuilder with automatic run merging
- Storage utilities and configuration properties
- ManifestFile schema extension with compactionMapLocation field
- PositionDeleteRemapper for remapping deletes
- CompactionMapValidator for conflict detection
- BaseRewriteFiles API for attaching maps to rewrites

✅ **Commit Flow Integration (Phase 4.3)**
- RewriteDataFilesCommitManager automatic map generation
- FilePositionMapping support in RewriteFileGroup
- Fallback logic for simple bin-pack scenarios
- Map writing and location threading to manifests

✅ **Isolation Enhancements (Phase 4.5)**
- SERIALIZABLE isolation with compaction awareness
- Distinction between structural and data changes
- Validation that respects conflict detection filters
- Clear error messages for different conflict types

✅ **Comprehensive Test Coverage**
- 50+ test cases covering all components
- Unit tests, integration tests, and isolation tests
- Conflict detection and resolution workflows
- Backward compatibility verification

### What Remains

The core infrastructure is complete and functional. The primary remaining work is:

## Future Work

### Spark-Level Position Tracking

While compaction maps can be generated with fallback logic for simple bin-pack operations, full position tracking during Spark rewrites would enable accurate maps for all scenarios:

1. **Explicit Position Tracking in Spark Writers**
   - Track source file + row position during read
   - Track target file + row position during write
   - Build FilePositionMapping data during rewrite
   - Pass mappings to RewriteFileGroup

**Benefits:**
- Accurate maps for multi-target rewrites (not just bin-pack)
- Support for filtered/sorted rewrites with position changes
- Elimination of fallback assumptions

**Current Workaround:**
- Fallback logic works for simple bin-pack (multiple sources → single target)
- Multi-target scenarios without position tracking skip map generation with warning

### Automatic Conflict Resolution

Currently, conflicts are detected and reported but not automatically resolved:

**Current Behavior:**
1. CompactionConflictException thrown with remediation guidance
2. Application must catch exception
3. Application must load compaction maps
4. Application must remap position deletes
5. Application must retry commit

**Future Enhancement:**
1. Detect conflict in BaseRowDelta.validate()
2. Automatically load compaction maps
3. Automatically remap position deletes
4. Transparently retry commit
5. Success without application intervention

**Implementation Considerations:**
- Requires careful handling of validation ordering
- Must distinguish between remappable conflicts (compaction) and non-remappable (actual data changes)
- Should be configurable (auto-remap vs explicit control)

### Potential Enhancements

1. **Deletion Vector Support**
   - Extend to support deletion vector compaction
   - Track content offset transformations

2. **Incremental Compaction**
   - Support partial file rewrites
   - Track which row ranges were rewritten

3. **Map Compression**
   - Use columnar Avro for better compression
   - Split large maps across multiple files

4. **Performance Optimizations**
   - Cache compaction maps in memory
   - Lazy loading of maps only when needed
   - Bulk remapping of delete files

5. **Observability**
   - Metrics for map generation/usage
   - Logging for conflict detection/remapping
   - Validation of map completeness

## Limitations

### Current Limitations

1. **Spark Position Tracking Not Implemented**

   The compaction map data structure fully supports complex scenarios (gaps, interleaving, multiple runs per file), but automatic generation during Spark rewrites is limited to simple bin-pack operations.

   **What the Data Structure Supports:**
   ```java
   // Complex merge scenario with deletes and updates
   builder.addFileMapping("base.parquet", "merged.parquet")
       .addRun(0, 0, 1000)           // First 1000 rows
       .addRun(1100, 1000, 28900)    // Gap: rows 1000-1099 deleted
       .addRun(30001, 28901, 970000); // Gap: row 30000 updated

   builder.addFileMapping("updates.parquet", "merged.parquet")
       .addRun(0, 28900, 1);          // Updated row interleaved
   ```

   **What the Fallback Logic Generates:**
   ```java
   // Simple sequential concatenation only
   for (DataFile sourceFile : sourceFiles) {
     builder.addFileMapping(source, target)
         .addRun(0, targetOffset, sourceFile.recordCount());
     targetOffset += sourceFile.recordCount();
   }
   // Assumes: no filtering, no sorting, no gaps, sequential order
   ```

   **Scenarios That Work:**
   - ✅ **Bin-pack**: Multiple small files → single larger file (sequential concatenation)
   - ✅ **Order-preserving**: No sorting or filtering applied during rewrite

   **Scenarios That Don't Work:**
   - ❌ **Sorted rewrites**: Row order changes (e.g., `SORT BY column`)
   - ❌ **Filtered rewrites**: Some rows excluded (e.g., applying deletes during merge)
   - ❌ **Merge compactions**: Combining base table + deletes + updates
   - ❌ **Split rewrites**: One source → multiple targets (logs warning, skips map)

   **What's Needed:**
   Position tracking during Spark read/write operations:
   - Tag rows with (source_file, source_position) during read
   - Track (target_file, target_position) during write
   - Build accurate mappings that reflect actual position transformations
   - Handle filtering (row not written), reordering (position changes), interleaving (multiple sources)

   **Workaround:** Simple bin-pack operations (most common case) work correctly with fallback logic.

2. **No Automatic Conflict Resolution**

   Position delete conflicts are detected but not automatically resolved. Applications must manually remap deletes and retry.

   **Two Types of Conflicts:**

   a. **Read Conflicts** (SERIALIZABLE isolation - Phase 4.5):
   ```java
   // Transaction reads data, concurrent REPLACE occurs
   rowDelta.validateNoConflictingDataFiles(); // SERIALIZABLE mode
   rowDelta.commit();

   // WITH compaction map: ✅ Succeeds (structural change only)
   // WITHOUT compaction map: ❌ ValidationException (data may have changed)
   ```
   This is **handled automatically** - SERIALIZABLE isolation distinguishes structural vs data changes.

   b. **Position Delete Conflicts** (still manual):
   ```java
   // Position deletes reference files that were compacted
   DeleteFile posDelete = createPositionDelete("old_file.parquet", pos=42);
   rowDelta.addDeletes(posDelete);
   rowDelta.commit();

   // ❌ Throws CompactionConflictException
   // Must manually remap deletes even with compaction maps
   ```

   **Manual Resolution Workflow:**
   ```java
   try {
     rowDelta.addDeletes(deleteFile);
     rowDelta.commit();
   } catch (CompactionConflictException e) {
     // 1. Get compaction map locations from exception
     Map<String, String> mapLocations = e.compactionMapLocations();

     // 2. Load maps and create remapper
     CompactionMap map = CompactionMaps.read(fileIO.newInputFile(mapLocation));
     PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

     // 3. Remap position deletes
     DeleteFile remappedDelete = remapDeleteFile(deleteFile, remapper);

     // 4. Retry with remapped deletes
     RowDelta retry = table.newRowDelta();
     retry.addDeletes(remappedDelete);
     retry.commit(); // ✅ Succeeds
   }
   ```

   **Why Not Automatic:**
   - Safer to make remapping explicit initially
   - Allows users to audit what's being remapped
   - Simpler implementation (no retry logic)
   - Can be added later as opt-in enhancement

   **Workaround:** Manual remapping workflow is well-documented and tested. Most DELETE operations in practice don't hit this case because they create new delete files rather than referencing specific old files.

### Design Considerations

1. **Row-Level Position Granularity**

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

2. **One Source File Per FileMapping**
   - Each source file has its own FileMapping to a target file
   - One source can map to one target
   - Multiple sources can map to same target (bin-pack)
   - One source split across multiple targets requires multiple FileMappings
   - This design supports efficient lookup and simple position transformation

3. **Map Storage Per Snapshot**
   - Each compaction creates a new compaction map file
   - Maps are immutable once written
   - Old maps can be garbage collected when snapshots expire
   - **Benefit**: Simple lifecycle management aligned with snapshots

4. **No Map Size Enforcement**
   - `write.compaction-map.target-size-bytes` property exists but not enforced
   - Currently used for documentation/monitoring only
   - Future: Could warn or split maps when threshold exceeded
   - **Current approach**: Run-length encoding keeps maps small enough

## References

- [Iceberg Position Deletes Specification](https://iceberg.apache.org/spec/#position-delete-files)
- [Iceberg Manifest Format](https://iceberg.apache.org/spec/#manifests)
- [Compaction Maps Design Document](../compaction_maps.md)

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
   - Keep this document updated with changes
   - Add examples for new APIs
   - Document limitations and workarounds
