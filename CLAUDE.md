# Claude Code Session Notes: Compaction Maps Implementation

## Overview

This document captures the implementation of **Compaction Maps** for Apache Iceberg, a feature that enables concurrent transactions writing position deletes to coexist with compaction operations. The work spans ~14k lines of code added across multiple phases, with comprehensive test coverage and documentation.

**Key Achievement**: Full infrastructure for compaction-aware transactions, including automatic map generation, conflict detection, and SERIALIZABLE isolation enhancements.

## Feature Architecture

### Core Problem Solved

Position deletes in Iceberg reference rows via `(file_path, row_position)` tuples. When files are compacted, position deletes become invalid because:
1. Referenced files no longer exist
2. Row positions change in the new files

**Solution**: Compaction maps track position transformations from source to target files, enabling:
- Automatic remapping of position deletes
- Detection of compaction conflicts
- SERIALIZABLE isolation that distinguishes structural vs data changes

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────┐
│ Compaction Operation                                        │
├─────────────────────────────────────────────────────────────┤
│ 1. RewriteDataFilesCommitManager.commitFileGroups()        │
│ 2. buildCompactionMap() from RewriteFileGroups             │
│ 3. CompactionMaps.write() to metadata location             │
│ 4. BaseRewriteFiles.setCompactionMapLocation()             │
│ 5. ManifestWriter attaches location to ManifestFile        │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Transaction with Position Deletes                           │
├─────────────────────────────────────────────────────────────┤
│ 1. BaseRowDelta.validate() checks for conflicts            │
│ 2. CompactionMapValidator detects compacted references     │
│ 3. CompactionConflictException with remediation guidance   │
│ 4. Application remaps deletes using PositionDeleteRemapper │
│ 5. Retry with remapped deletes                             │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ SERIALIZABLE Isolation Enhancement                          │
├─────────────────────────────────────────────────────────────┤
│ 1. MergingSnapshotProducer.validateCompactionAwareConflicts()│
│ 2. Check REPLACE operations for compaction maps            │
│ 3. WITH maps → no conflict (structural change only)        │
│ 4. WITHOUT maps → ValidationException (data change)        │
└─────────────────────────────────────────────────────────────┘
```

## Code Organization

### Key Files by Layer

#### API Layer (`api/src/main/java/org/apache/iceberg/`)
- `CompactionMap.java` - Core interface for position transformations
- `exceptions/CompactionConflictException.java` - Typed exception for compaction conflicts

#### Core Implementation (`core/src/main/java/org/apache/iceberg/`)

**Data Structures:**
- `GenericCompactionMap.java` - Avro-based implementation
- `CompactionMapBuilder.java` - Builder with automatic run merging
- `CompactionMaps.java` - Storage utilities (read/write/location generation)

**Transaction Integration:**
- `PositionDeleteRemapper.java` - Remaps position deletes using maps
- `CompactionMapValidator.java` - Detects conflicts during commit
- `BaseRowDelta.java` - Integrates validation hooks
- `MergingSnapshotProducer.java` - SERIALIZABLE isolation logic

**Schema/Metadata:**
- `ManifestFile.java` - Extended with `compactionMapLocation` field (ID 521)
- `ManifestWriter.java` - Attaches map locations to manifests
- `TableProperties.java` - Configuration properties

**Actions Integration:**
- `actions/RewriteDataFilesCommitManager.java` - Automatic map generation
- `actions/RewriteFileGroup.java` - Position mapping support (FilePositionMapping class)

#### Test Suite (`core/src/test/java/org/apache/iceberg/`)
- `TestCompactionMapSerialization.java` - Avro round-trip
- `TestCompactionMapBuilder.java` - Builder and run merging
- `TestCompactionMapsStorage.java` - Storage and configuration
- `TestPositionDeleteRemapper.java` - Remapping logic
- `TestCompactionMapIntegration.java` - Cross-component integration
- `TestCompactionMapCommitFlow.java` - Commit flow integration
- `TestCompactionConflictDetection.java` - Conflict detection
- `TestCompactionConflictResolution.java` - Conflict resolution workflows
- `TestSerializableIsolationWithCompaction.java` - Isolation semantics

### Critical Implementation Patterns

#### 1. Avro Serialization (Not Java Serializable)

Compaction maps follow Iceberg's Avro patterns:

```java
public class GenericCompactionMap implements CompactionMap,
    StructLike, IndexedRecord, SchemaConstructable {

  // Key patterns:
  // - Implement StructLike for Iceberg integration
  // - Implement IndexedRecord for Avro compatibility
  // - Use InternalData.read() with setCustomType() for nested types
  // - Immutable after construction
}
```

**Why this matters**: Ensures compatibility with Iceberg's metadata system and enables efficient serialization.

#### 2. Run-Length Encoding for Efficiency

Position mappings use automatic run merging:

```java
// Without merging: 1000 individual position mappings
// With merging: Single run (0, 0, 1000)

CompactionMapBuilder builder = new CompactionMapBuilder(sourceSnap, targetSnap);
builder.addFileMapping("file1.parquet", "file2.parquet")
    .addRun(0, 0, 100)
    .addRun(100, 100, 50)   // Consecutive with previous run
    .addRun(150, 150, 25);  // Consecutive with previous run

// Result: Single merged run (0, 0, 175)
```

**Token Saver**: When debugging map generation issues, check `CompactionMapBuilder.java:89-118` for the merging logic.

#### 3. Two Types of Conflicts

The implementation handles two distinct conflict scenarios:

**A. Position Delete Conflicts** (Phase 4)
- Position deletes reference files that were compacted
- Detected by `CompactionMapValidator`
- Throws `CompactionConflictException`
- Remapping is possible and documented

**B. Read Conflicts** (Phase 4.5)
- SERIALIZABLE transactions read data that was replaced
- Detected by `MergingSnapshotProducer.validateCompactionAwareConflicts()`
- WITH compaction map → no conflict (structural change)
- WITHOUT compaction map → ValidationException (data change)

**Token Saver**: If confused about which conflict type is being discussed, refer to the test files - TestCompactionConflict* covers position deletes, TestSerializableIsolation* covers read conflicts.

## Implementation Phases (Completed)

### Phase 1-5: Core Infrastructure
- Data structures with Avro serialization ✅
- ManifestFile schema extension (field ID 521) ✅
- Storage utilities and configuration ✅
- PositionDeleteRemapper and CompactionMapValidator ✅
- BaseRewriteFiles API for map attachment ✅

### Phase 4.3: Commit Flow Integration
**Files Modified:**
- `actions/RewriteDataFilesCommitManager.java:100-260`
- `actions/RewriteFileGroup.java:45-189` (added FilePositionMapping)

**Key Methods:**
- `buildCompactionMap()` - Builds maps from RewriteFileGroups
- `writeCompactionMap()` - Persists to metadata location
- `shouldGenerateCompactionMap()` - Checks table property

**Fallback Logic**: For bin-pack operations without explicit position tracking, assumes sequential offset mapping (works for simple bin-pack scenarios).

### Phase 4.5: SERIALIZABLE Isolation
**Files Modified:**
- `MergingSnapshotProducer.java:417-508` (added validateCompactionAwareConflicts)
- `BaseRowDelta.java:160-168` (integrated validation)

**Key Insight**: Only runs when `validateNoConflictingDataFiles()` is called (SERIALIZABLE isolation). SNAPSHOT isolation doesn't check REPLACE operations at all.

**Token Saver**: The validation logic is in `MergingSnapshotProducer.java:430-508`. If you need to understand isolation semantics, read the test file `TestSerializableIsolationWithCompaction.java` first (it's well-commented).

## Configuration

### Table Properties

```java
// Required for compaction map generation
TableProperties.COMPACTION_MAP_ENABLED = "write.compaction-map.enabled"
TableProperties.COMPACTION_MAP_ENABLED_DEFAULT = false

// Target size (not enforced, documentation only)
TableProperties.COMPACTION_MAP_TARGET_SIZE_BYTES = "write.compaction-map.target-size-bytes"
TableProperties.COMPACTION_MAP_TARGET_SIZE_BYTES_DEFAULT = 8388608 // 8 MB

// Isolation level (affects validation behavior)
TableProperties.DELETE_ISOLATION_LEVEL = "write.delete.isolation-level"
TableProperties.DELETE_ISOLATION_LEVEL_DEFAULT = "serializable"
```

### File Naming Convention

```
<metadata-dir>/compaction-map-<snapshotId>-<uuid>.avro
```

Generated via: `CompactionMaps.newCompactionMapFile(table, snapshotId)`

## Testing Strategy

### Test Organization

**Unit Tests** (fast, isolated):
- `TestCompactionMapSerialization` - Avro round-trip
- `TestCompactionMapBuilder` - Builder logic and run merging
- `TestCompactionMapsStorage` - File location generation
- `TestPositionDeleteRemapper` - Remapping algorithms

**Integration Tests** (cross-component):
- `TestCompactionMapIntegration` - ManifestWriter ↔ BaseRewriteFiles
- `TestCompactionMapCommitFlow` - RewriteDataFilesCommitManager end-to-end

**Scenario Tests** (behavioral):
- `TestCompactionConflictDetection` - Concurrent transaction conflicts
- `TestCompactionConflictResolution` - Remapping and retry workflows
- `TestSerializableIsolationWithCompaction` - Isolation semantics

### Running Tests

```bash
# All compaction map tests
./gradlew :iceberg-core:test --tests "*CompactionMap*"

# Specific test class
./gradlew :iceberg-core:test --tests "TestSerializableIsolationWithCompaction"

# Isolation tests only
./gradlew :iceberg-core:test --tests "*Isolation*"

# With verbose output for debugging
./gradlew :iceberg-core:test --tests "*CompactionMap*" --info
```

### Test Data Patterns

When writing tests, follow these patterns:

**1. Use Mock Data Files**
```java
// Don't write actual Parquet/ORC files unless testing serialization
DataFile dataFile = DataFiles.builder(spec)
    .withPath("/path/to/file.parquet")
    .withFileSizeInBytes(1024)
    .withRecordCount(100)
    .build();
```

**2. Test Both Formats**
```java
@ParameterizedTest
@EnumSource(FileFormat.class, names = {"PARQUET", "ORC"})
public void testSomething(FileFormat format) {
  // Test implementation
}
```

**3. Use InMemoryCatalog**
```java
// Fast, no I/O
InMemoryCatalog catalog = new InMemoryCatalog();
catalog.initialize("test", Collections.emptyMap());
Table table = catalog.createTable(tableIdent, schema, spec);
```

## Token-Saving Strategies

### 1. Use Grep for Code Navigation

```bash
# Find where compaction maps are generated
grep -r "buildCompactionMap" core/src/main/java/

# Find validation logic
grep -r "validateCompactionAwareConflicts" core/src/main/java/

# Find exception throwing sites
grep -r "CompactionConflictException" core/src/main/java/
```

**In Claude Code**: Use the Grep tool instead of Read for finding code patterns.

### 2. Read Test Files First

Test files are typically well-commented and show usage patterns:
- `TestSerializableIsolationWithCompaction.java` - Best overview of isolation semantics
- `TestCompactionMapCommitFlow.java` - Shows end-to-end map generation
- `TestCompactionConflictResolution.java` - Shows remapping workflow

**Token Saver**: Reading a test file (200-400 lines) is cheaper than reading multiple implementation files (1000+ lines each).

### 3. Key Line References

Rather than reading entire files, jump to these key sections:

**Map Generation:**
- `RewriteDataFilesCommitManager.java:174-260` - buildCompactionMap() and writeCompactionMap()

**Conflict Detection:**
- `CompactionMapValidator.java:80-110` - validateNoCompactedReferences()
- `BaseRowDelta.java:219-238` - validateNoCompactionConflicts()

**Isolation Logic:**
- `MergingSnapshotProducer.java:430-508` - validateCompactionAwareConflicts()
- `BaseRowDelta.java:160-168` - Integration point

**Run Merging:**
- `CompactionMapBuilder.java:89-118` - Automatic run merging logic

### 4. Documentation References

**Primary Documentation:**
- `docs/docs/compaction_maps.md` - Comprehensive user-facing documentation (updated)
- Contains API examples, configuration, and architecture diagrams

**Implementation Plan (Historical):**
- `COMPACTION_MAPS_IMPLEMENTATION_PLAN_REVISED.md` - Detailed phase-by-phase plan
- Useful for understanding design decisions and future work

### 5. Useful Gradle Commands

```bash
# Compile only core module
./gradlew :iceberg-core:compileJava

# Run specific test without full rebuild
./gradlew :iceberg-core:test --tests "TestName" --rerun-tasks

# Apply code formatting
./gradlew spotlessApply
```

## Common Issues and Solutions

### Issue 1: Compilation Errors After Schema Changes

**Problem**: Adding fields to ManifestFile or other Avro schemas causes serialization errors.

**Solution**:
1. Ensure field IDs are unique and sequential
2. Use `StructLike` interface consistently
3. Implement `IndexedRecord` for Avro compatibility
4. Check `ManifestFile.java:521` for the compactionMapLocation field as reference

**Token Saver**: Don't read the entire Avro serialization code. Just check `GenericCompactionMap.java:50-120` for the pattern.

### Issue 2: Test Failures in Isolation Tests

**Problem**: Tests fail because validation runs at wrong time or isolation level isn't set correctly.

**Solution**:
- SERIALIZABLE validation only runs when `validateNoConflictingDataFiles()` is called
- Check if test is setting up isolation correctly:
  ```java
  .validateNoConflictingDataFiles()  // Required for SERIALIZABLE
  ```
- Read `TestSerializableIsolationWithCompaction.java:105-112` for correct pattern

### Issue 3: Compaction Maps Not Generated

**Problem**: Rewrite operation completes but no compaction map is written.

**Solution**:
1. Check if `write.compaction-map.enabled` is set to `true`
2. Verify `shouldGenerateCompactionMap()` returns true
3. Check if FilePositionMappings are present in RewriteFileGroup
4. Look at `RewriteDataFilesCommitManager.java:101-106` for the check

**Debugging**: Add logging in `buildCompactionMap()` to see what's passed in.

### Issue 4: Position Delete Remapping Incorrect

**Problem**: Remapped position deletes point to wrong positions.

**Solution**:
1. Verify run merging didn't create incorrect ranges
2. Check if source positions are 0-based (they should be)
3. Ensure target offsets account for previous source files
4. Review `CompactionMapBuilder.java:89-118` for merging logic

**Test Reference**: `TestPositionDeleteRemapper.java` has extensive test cases.

## Future Work

### 1. Spark-Level Position Tracking

**Current State**: Fallback logic in `RewriteDataFilesCommitManager.buildCompactionMap()` works for simple bin-pack (multiple sources → single target).

**Future Enhancement**: Track positions explicitly during Spark read/write:
- Instrument Spark readers to track (source_file, source_position)
- Instrument Spark writers to track (target_file, target_position)
- Build FilePositionMapping during rewrite
- Pass to RewriteFileGroup

**Why**: Enables accurate maps for multi-target rewrites and sorted rewrites.

**Token Saver**: If working on this, focus on `spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java`. The infrastructure is already in place in RewriteFileGroup.

### 2. Automatic Conflict Resolution

**Current State**: CompactionConflictException provides remediation guidance, but application must manually remap.

**Future Enhancement**: Automatic remapping in BaseRowDelta.validate():
1. Catch compaction conflicts
2. Load compaction maps from exception
3. Remap position deletes automatically
4. Transparently retry commit

**Implementation Note**: Requires careful ordering to avoid infinite retry loops. Should be configurable.

### 3. Deletion Vector Support

**Current State**: Only position deletes are supported.

**Future Enhancement**: Extend to deletion vectors:
- Track content offset transformations
- Similar API but different semantics (deletes are stored differently)

## Key Design Decisions

### Why Avro Instead of JSON?

**Decision**: Use Avro serialization for compaction maps.

**Rationale**:
- Follows Iceberg patterns (ManifestFile, DataFile all use Avro)
- Better performance and smaller file sizes
- Native support in Iceberg's metadata system

**Trade-off**: More complex implementation (must implement StructLike, IndexedRecord, SchemaConstructable).

### Why Two Separate Validations?

**Decision**: Separate `validateNoCompactionConflicts()` (position delete conflicts) from `validateCompactionAwareConflicts()` (read conflicts).

**Rationale**:
- Different concerns: position delete validity vs data consistency
- Different error types: CompactionConflictException (remappable) vs ValidationException (not remappable)
- Different phases: position delete check runs first, read conflict check runs with other SERIALIZABLE validations

**Token Saver**: Understanding this distinction early saves debugging time.

### Why Not Automatic Remapping Yet?

**Decision**: Require manual remapping for now (throw CompactionConflictException).

**Rationale**:
- Safer to make behavior explicit initially
- Allows users to audit remapping before it happens
- Simpler implementation (no retry logic)
- Can add automatic remapping later without breaking changes

**Future**: Phase 6 would add automatic remapping as opt-in behavior.

### Why Fallback Logic for Bin-Pack?

**Decision**: Generate maps automatically for simple bin-pack even without explicit position tracking.

**Rationale**:
- Bin-pack is most common compaction pattern
- Sequential offset mapping is correct for bin-pack
- Enables immediate value without Spark instrumentation
- Multi-target scenarios safely skip map generation (with warning)

**Implementation**: See `RewriteDataFilesCommitManager.java:234-255`.

**Limitation Clarification**: The data structure supports complex scenarios (gaps, interleaving, filtered rows, merged files), but automatic generation is limited to simple sequential bin-pack. For complex merge compactions (base table + deletes + updates), the map would need to be built manually or via Spark position tracking.

### Why Row-Level Granularity?

**Decision**: Track positions at row granularity, not coarser granularity (blocks, pages).

**Rationale**:
- Position deletes are row-level: `(file_path: String, pos: Long)`
- Compaction maps must transform at same granularity for accurate remapping
- Coarser granularity would lose precision needed for exact position mapping
- Run-length encoding provides effective compression for consecutive positions

**Not a Limitation**: This is inherited from position delete semantics. It's a design requirement, not a shortcoming. Testing shows maps stay reasonable size for typical workloads.

## Related Iceberg Concepts

### Position Deletes

Position deletes are Iceberg's way of marking rows as deleted without rewriting data files:
- Schema: `(file_path: String, pos: Long, optional row data)`
- Positions are 0-based ordinal indices within the file
- More efficient than rewriting files for small deletes

**Token Saver**: Full spec at https://iceberg.apache.org/spec/#position-delete-files

### Isolation Levels

Iceberg supports two isolation levels for DELETE/UPDATE/MERGE:

**SERIALIZABLE** (default):
- Validates no concurrent data changes
- Checks APPEND and OVERWRITE operations
- With compaction maps: also checks REPLACE operations
- Strongest consistency

**SNAPSHOT**:
- No validation of concurrent operations
- Weaker consistency but better concurrency
- Useful for idempotent operations

**Configuration**: `write.delete.isolation-level` property

### Manifest Files

Manifests are Avro files that list data/delete files in a snapshot:
- Store file metadata (path, metrics, partition info)
- Referenced by snapshot metadata
- Compaction map location attached as optional field (ID 521)

**Token Saver**: If you need to understand manifests, read `ManifestFile.java:30-100` (interface definition), not the entire implementation.

## Cost Analysis

**Session Stats**:
- Total cost: $55.75
- Duration: ~12 hours wall time, ~4.4 hours API time
- Code changes: 14,045 lines added, 1,172 removed
- Primary model: Claude Sonnet 4.5

**Token Usage Breakdown**:
- Largest costs: Reading implementation files (1000+ lines each)
- Efficient: Reading test files first (200-400 lines, well-commented)
- Efficient: Using Grep to locate code before reading full files
- Expensive: Re-reading files after modifications (use line number references)

**Recommendations for Future Work**:
1. Use Grep extensively before Read
2. Read test files to understand APIs
3. Use line number references (file:line) instead of re-reading
4. Read documentation (`compaction_maps.md`) before diving into code
5. Use Haiku for simple queries, Sonnet for implementation

## Session Workflow That Worked Well

1. **Phase-by-phase implementation** - Breaking work into clear phases (4.3, 4.5) with distinct goals
2. **Test-driven development** - Writing tests first or alongside implementation
3. **Comprehensive documentation updates** - Updating docs immediately after implementation
4. **Clear commit messages** - Detailed commits with "why" not just "what"
5. **Incremental validation** - Compiling and testing after each phase

## Quick Reference Commands

```bash
# Development
./gradlew :iceberg-core:compileJava              # Compile core
./gradlew :iceberg-core:test --tests "TestName"  # Run specific test
./gradlew spotlessApply                          # Format code

# Code Search
grep -r "CompactionMap" core/src/main/java/      # Find all usages
grep -r "validateCompactionAware" core/          # Find validation

# Git
git log --oneline --graph                        # View commit history
git diff HEAD~1 -- path/to/file                  # Compare with previous

# Documentation
ls docs/docs/compaction_maps.md                  # Main documentation
cat COMPACTION_MAPS_IMPLEMENTATION_PLAN_REVISED.md  # Implementation plan
```

## Contact Context

This implementation was done on the `cmpmap` branch (NOT `vldb` - that's a separate prototype). All work is committed and the documentation is up to date as of the last commit.

**Branch**: `cmpmap`
**Last Commit**: Documentation update (66054e15a)
**Test Status**: 50+ tests, all passing ✅
**Documentation Status**: Comprehensive and current ✅

---

*Generated during Claude Code session, 2026-01-07*
*Model: Claude Sonnet 4.5*
*Total Implementation: ~14k lines across core, tests, and documentation*
