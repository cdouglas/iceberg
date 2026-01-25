# Claude Code Session Notes: Compaction Maps Implementation

## Overview

**Compaction Maps** enable concurrent transactions writing position deletes to coexist with compaction operations in Apache Iceberg. When files are compacted, position deletes become invalid because referenced files no longer exist and row positions change. Compaction maps track these transformations, enabling automatic remapping and conflict detection.

**Key Achievements**:
- Full infrastructure for compaction-aware transactions (~18k lines)
- Advanced remapping optimization (1.1-32x speedup via smart algorithm selection)
- Comprehensive test coverage (150+ tests) and empirical validation (324 JMH benchmarks)
- SERIALIZABLE isolation enhancements
- **Compaction conflict resolution** - Automatic remapping of concurrent position deletes during compaction
- **Staged scan optimization** - Position tracking now uses efficient staged scans with explicit metadata column selection

**Implementation Status**: Complete on `cmpmap` branch
**Last Updated**: January 22, 2026

## Feature Architecture

### Core Problem

Position deletes reference rows via `(file_path, row_position)` tuples. When files are compacted:
1. Referenced files no longer exist
2. Row positions change in the new files

### Solution

Compaction maps track position transformations from source to target files:
- **Automatic remapping** of position deletes using efficient algorithms
- **Conflict detection** when transactions reference compacted files
- **SERIALIZABLE isolation** that distinguishes structural vs data changes

### High-Level Flow

```
Compaction Operation
└─> RewriteDataFilesCommitManager.commitFileGroups()
    └─> buildCompactionMap() from RewriteFileGroups
        └─> CompactionMaps.write() to metadata location
            └─> ManifestWriter attaches location to ManifestFile

Transaction with Position Deletes
└─> BaseRowDelta.validate() checks for conflicts
    └─> CompactionMapValidator detects compacted references
        └─> CompactionConflictException with remediation guidance
            └─> Application remaps deletes using PositionDeleteRemapper
                └─> Retry with remapped deletes

SERIALIZABLE Isolation
└─> MergingSnapshotProducer.validateCompactionAwareConflicts()
    └─> Check REPLACE operations for compaction maps
        └─> WITH maps → no conflict (structural change only)
        └─> WITHOUT maps → ValidationException (data change)

Compaction Conflict Resolution (Spark 3.5 and 4.0)
└─> SparkRewriteDataFilesCommitManager.commitFileGroups()
    └─> detectAndResolveConflicts()
        └─> CompactionConflictDetector.detectConflicts()
            └─> Scans manifests for conflicting delete files
        └─> SparkCompactionConflictResolver.resolve()
            └─> Separate DVs from position delete files
            └─> DVs: Remap using PositionDeleteRemapper.remapDVBulk()
            └─> Position deletes: Read via Spark, remap, write new files
            └─> Include remapped deletes in commit
```

## Code Organization

### Key Files by Layer

**API Layer** (`api/src/main/java/org/apache/iceberg/`):
- `CompactionMap.java` - Core interface for position transformations
- `exceptions/CompactionConflictException.java` - Typed exception for conflicts

**Core Implementation** (`core/src/main/java/org/apache/iceberg/`):

*Data Structures:*
- `GenericCompactionMap.java` - Avro-based implementation
- `CompactionMapBuilder.java` - Builder with automatic run merging
- `CompactionMaps.java` - Storage utilities (read/write/location generation)

*Transaction Integration:*
- `PositionDeleteRemapper.java` - Remaps position deletes (includes smart selector)
- `CompactionMapValidator.java` - Detects conflicts during commit
- `BaseRowDelta.java` - Validation hooks
- `MergingSnapshotProducer.java` - SERIALIZABLE isolation logic

*Remapping Optimization:*
- `RemappingStrategy.java` - Interface with Factory pattern and bulk API
- `LinearSearchStrategy.java` - O(m) baseline
- `BinarySearchStrategy.java` - O(log m) optimized
- `IntervalTreeStrategy.java` - O(log m) balanced tree
- `StreamJoinStrategy.java` - O(n + m) merge-join for sorted
- `RangeQueryStrategy.java` - O(m log n) inverted query for high fan-in
- `RemappingAlgorithmSelector.java` - Smart automatic strategy selection

*Schema/Metadata:*
- `ManifestFile.java` - Extended with `compactionMapLocation` field (ID 521)
- `ManifestWriter.java` - Attaches map locations
- `TableProperties.java` - Configuration properties

*Actions Integration:*
- `actions/RewriteDataFilesCommitManager.java` - Automatic map generation
- `actions/RewriteFileGroup.java` - Position mapping support

*Conflict Detection:*
- `CompactionConflictDetector.java` - Scans manifests to find conflicting delete files
- `DeleteConflictInfo.java` - Metadata about detected conflicts
- `PositionDeletesScanTasks.java` - Helper to create scan tasks for delete files

**Spark Layer** (`spark/v3.5/` and `spark/v4.0/`):

*Conflict Resolution (both Spark 3.5 and 4.0):*
- `SparkRewriteDataFilesCommitManager.java` - Extends core commit manager with conflict resolution
- `SparkCompactionConflictResolver.java` - Reads, remaps, and writes position deletes and DVs using Spark
- `RewriteDataFilesSparkAction.java` - Uses SparkRewriteDataFilesCommitManager

*Note*: Spark 4.0 requires cast to `org.apache.spark.sql.classic.SparkSession` for `cloneSession()`

**Test Suite** (`core/src/test/java/org/apache/iceberg/`):
- Core: TestCompactionMap{Serialization,Builder,Storage,Integration,CommitFlow}
- Conflicts: TestCompactionConflict{Detection,Detector,Resolution}
- Isolation: TestSerializableIsolationWithCompaction
- Remapping: TestRemapping{Strategies,StrategiesIntegration,AlgorithmSelector}
- Benchmarks: RemappingAlgorithmBenchmark (324 configurations)

**Spark Test Suite** (both `spark/v3.5/` and `spark/v4.0/`):
- TestSparkCompactionConflictResolution (12 tests each: 6 V2 + 6 V3) - End-to-end conflict resolution tests

## Critical Implementation Patterns

### 1. Avro Serialization (Not Java Serializable)

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

**Location**: `GenericCompactionMap.java:50-120`

### 2. Run-Length Encoding for Efficiency

Position mappings use automatic run merging:

```java
CompactionMapBuilder builder = new CompactionMapBuilder(sourceSnap, targetSnap);
builder.addFileMapping("file1.parquet", "file2.parquet")
    .addRun(0, 0, 100)
    .addRun(100, 100, 50)   // Consecutive → merged
    .addRun(150, 150, 25);  // Consecutive → merged
// Result: Single merged run (0, 0, 175)
```

**Why**: 1000 individual positions → 1 run. Efficient storage and faster lookups.
**Location**: `CompactionMapBuilder.java:89-118`

### 3. Design Scope: Order-Preserving Compactions

Compaction maps support **order-preserving** operations only:

- ✅ **Bin-pack rewrites**: Multiple files → larger files (simple concatenation)
- ✅ **Merge compactions**: Bin-pack with position deletes applied during scan
- ❌ **Sorted compactions**: Out of scope by design
- ❌ **Z-ordered compactions**: Out of scope by design

**Why order-changing operations are inappropriate** (not just unsupported):

1. **Degenerate mappings**: Reordering produces runs of length 1 (every row maps individually), defeating run-length encoding and creating maps as large as the data itself.

2. **Semantic mismatch**: Position deletes identify rows by `(file_path, position)`. After reordering, position N refers to a different logical row. Remapping would delete wrong rows.

3. **Better alternatives exist**: For sorted/Z-ordered compactions, use equality deletes or accept that position deletes are invalidated.

**Note**: Tables with sorted base data and unsorted changes are fine—unsorted changes can be compacted with position tracking, then merged into sorted runs (merge applies deletes during scan, no tracking needed).

### 3.5 One-to-One Source-Target Assumption

Compaction maps assume **each source file maps to exactly one target file**. Multi-target mappings (source file spanning multiple targets) are not supported.

**Where enforced**:
- `CompactionMapBuilder.addFileMapping()` - Uses source file as unique key
- `PositionMappingCoordinator.aggregateMappings()` - Uses first target only

**When this could fail**: Large source file + small target file size = source spans target boundary. Rare in practice because bin-pack targets small files and planner distributes evenly.

**Details**: See `docs/docs/compaction_maps_errata.md` Section 2.

### 4. Two Types of Conflicts

**A. Position Delete Conflicts** (detected by CompactionMapValidator):
- Position deletes reference files that were compacted
- Throws `CompactionConflictException` with compaction map for remapping
- Application can remap and retry

**B. Read Conflicts** (detected by MergingSnapshotProducer):
- SERIALIZABLE transactions read data that was replaced
- WITH compaction map → no conflict (structural change)
- WITHOUT compaction map → ValidationException (data change)

**Location**: `BaseRowDelta.java:219-238`, `MergingSnapshotProducer.java:430-508`

## Remapping Algorithm Selection

The smart selector (`RemappingAlgorithmSelector`) chooses optimal strategies based on empirical JMH benchmark data (324 configurations tested January 2026), not theoretical complexity analysis.

**Key Empirical Findings:**
- **UNSORTED data**: IntervalTree wins 25/27 scenarios regardless of m, n, or gaps
- **SORTED data**: RangeQuery or StreamJoin win; IntervalTree **never** wins
- **BinarySearch**: Rarely optimal (2 edge cases only, not worth selecting)

**Current Selection Logic** (Empirically-derived, January 2026):
```java
if (!sorted) {
    return IntervalTree;        // Wins 25/27 unsorted scenarios
}

// Sorted data below - IntervalTree never wins for sorted
if (gapRatio > 0.3) {
    return RangeQuery;          // Sparse: skip gaps efficiently
}

if (n >= 10000 && m >= 100) {
    return StreamJoin;          // Dense sorted bulk with many runs
}

return RangeQuery;              // Default for sorted (including small m with large n)
```

**Performance** (from Jan 22, 2026 benchmarks):
- Small scale (n=1000): 1.1-1.5x speedup vs linear
- Medium scale (n=10000): 4.9-5.6x speedup vs linear
- Large scale (n=100000): 21-31x speedup vs linear
- Smart selector overhead: 6% average

**Details**: See `REMAPPING_BENCHMARKS.md` for benchmark methodology. Implementation history available via `git log --grep="remapping" cmpmap`.

## Configuration

### Table Properties

```java
// Enable compaction map generation
"write.compaction-map.enabled" = "true" (default: false)

// Target size (documentation only, not enforced)
"write.compaction-map.target-size-bytes" = "8388608" (8 MB default)

// Isolation level (affects validation behavior)
"write.delete.isolation-level" = "serializable" (default)

// Enable automatic conflict resolution during compaction (Spark 3.5 and 4.0)
"write.compaction.resolve-delete-conflicts" = "true" (default: false)

// Maximum conflicting delete files to resolve automatically
"write.compaction.resolve-delete-conflicts.max-files" = "100" (default)
```

### File Naming Convention

```
<metadata-dir>/compaction-map-<snapshotId>-<uuid>.avro
```

Generated via: `CompactionMaps.newCompactionMapFile(table, snapshotId)`

## Common Issues and Solutions

### Issue 1: Compaction Maps Not Generated

**Symptoms**: Rewrite operation completes but no map written.

**Causes**:
1. `write.compaction-map.enabled` not set to `true`
2. `shouldGenerateCompactionMap()` returns false
3. No FilePositionMappings in RewriteFileGroup

**Solution**: Check table property and verify FilePositionMappings exist.
**Location**: `RewriteDataFilesCommitManager.java:101-106`

### Issue 2: Position Delete Remapping Incorrect

**Symptoms**: Remapped position deletes point to wrong positions.

**Causes**:
1. Run merging created incorrect ranges
2. Source positions not 0-based
3. Target offsets don't account for previous source files

**Solution**: Verify run merging logic and position calculations.
**Location**: `CompactionMapBuilder.java:89-118`
**Tests**: `TestPositionDeleteRemapper.java`

### Issue 3: Conflict Detection Not Working

**Symptoms**: Expected conflicts not detected.

**Causes**:
1. Validation not called (`validateNoConflictingDataFiles()` missing)
2. Isolation level not SERIALIZABLE
3. CompactionMapValidator not integrated in BaseRowDelta

**Solution**: Ensure SERIALIZABLE isolation and validation is called.
**Location**: `BaseRowDelta.java:219-238`
**Tests**: `TestCompactionConflictDetection.java`

### Issue 4: Smart Selector Choosing Wrong Strategy (FIXED)

**Status**: Complete rewrite based on empirical benchmark data (January 2026)

The selector was completely rewritten after multiple benchmark runs revealed fundamental problems with the original heuristic-based approach. The logic is derived entirely from JMH benchmark results across 324 configurations:

**Key empirical findings (Jan 22, 2026 benchmarks):**
- **UNSORTED data**: IntervalTree wins 24/27 scenarios regardless of m, n, or gaps
- **SORTED data**: RangeQuery (17 scenarios) or StreamJoin (10 scenarios) win; IntervalTree never wins
- **BinarySearch**: Wins 3 edge cases (unsorted, m=1000, n=1000) but not worth special-casing

**Current selection logic** (simple 4-branch decision):
```java
if (!sorted) return IntervalTree;           // Empirically optimal for unsorted
if (gapRatio > 0.3) return RangeQuery;      // Skip gaps efficiently
if (n >= 10000 && m >= 100) return StreamJoin;  // Dense sorted bulk with many runs
return RangeQuery;                          // Default for sorted
```

**Measured overhead**: 4.68% average compared to optimal strategy (Jan 22, 2026 benchmarks).

**Location**: `RemappingAlgorithmSelector.java:79-121`

### Issue 5: Test Failures in Isolation Tests

**Symptoms**: Serializable validation tests fail.

**Causes**:
- SERIALIZABLE validation only runs when `validateNoConflictingDataFiles()` is called
- Test not setting up isolation correctly

**Solution**: Ensure test calls validation:
```java
.validateNoConflictingDataFiles()  // Required for SERIALIZABLE
```

**Location**: `TestSerializableIsolationWithCompaction.java:105-112`

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

### 2. Read Test Files First

Test files are well-commented and show usage patterns:
- `TestSerializableIsolationWithCompaction.java` - Isolation semantics overview
- `TestCompactionMapCommitFlow.java` - End-to-end map generation
- `TestCompactionConflictResolution.java` - Remapping workflow

**Token Saver**: Test file (200-400 lines) is cheaper than reading multiple implementation files (1000+ lines each).

### 3. Key Line References

Jump directly to these sections instead of reading entire files:

**Map Generation**:
- `RewriteDataFilesCommitManager.java:174-260` - buildCompactionMap() and writeCompactionMap()

**Conflict Detection**:
- `CompactionMapValidator.java:80-110` - validateNoCompactedReferences()
- `BaseRowDelta.java:219-238` - validateNoCompactionConflicts()
- `CompactionConflictDetector.java` - Scans manifests for conflicting delete files

**Compaction Conflict Resolution (Spark 3.5 and 4.0)**:
- `SparkRewriteDataFilesCommitManager.java:185-234` - detectAndResolveConflicts()
- `SparkCompactionConflictResolver.java` - Read, remap, write position deletes and DVs

**Isolation Logic**:
- `MergingSnapshotProducer.java:430-508` - validateCompactionAwareConflicts()
- `BaseRowDelta.java:160-168` - Integration point

**Run Merging**:
- `CompactionMapBuilder.java:89-118` - Automatic run merging logic

**Remapping Algorithms**:
- Linear: `LinearSearchStrategy.java:48-59` (9 lines)
- Binary: `BinarySearchStrategy.java:62-91` (30 lines)
- Interval Tree: `IntervalTreeStrategy.java:78-103` (26 lines)
- Stream Join: `StreamJoinStrategy.java:119-157` (39 lines)
- Range Query: `RangeQueryStrategy.java:167-192` (26 lines)

### 4. Use Benchmark Analysis Tools

```bash
cd benchmark/remapping-optimization

# Analyze JMH results
python3 analyze_results.py results_20260116_162342.txt

# Generate charts (requires matplotlib)
python3 visualize_results.py results_20260116_162342.csv
```

**Output**: Strategy comparison, selector overhead, speedup vs baseline.

## Quick Reference Commands

### Development

```bash
# Compile core module
./gradlew :iceberg-core:compileJava

# Run specific test
./gradlew :iceberg-core:test --tests "TestSerializableIsolationWithCompaction"

# Run all compaction map tests
./gradlew :iceberg-core:test --tests "*CompactionMap*"

# Run all remapping tests
./gradlew :iceberg-core:test --tests "*Remapping*"

# Format code
./gradlew spotlessApply
```

### Benchmarks

```bash
# Run all remapping benchmarks (2-3 hours)
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhOutputPath=benchmark/remapping-optimization/results.txt

# Run specific scenario
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
    -PjmhParams="numRuns=100,numPositions=10000,sorted=true"

# Test smart selector only
./gradlew :iceberg-core:jmh \
    -PjmhIncludeRegex=RemappingAlgorithmBenchmark.smartSelector
```

### Code Search

```bash
# Find all usages
grep -r "CompactionMap" core/src/main/java/

# Find validation logic
grep -r "validateCompactionAware" core/

# Find strategy implementations
grep -l "implements RemappingStrategy" core/src/main/java/org/apache/iceberg/*.java
```

### Git

```bash
# View commit history
git log --oneline --graph

# Compare with previous commit
git diff HEAD~1 -- path/to/file

# Show commits for compaction maps
git log --oneline --grep="compaction\|remapping" cmpmap
```

## Documentation References

**Primary Documentation**:
- `docs/docs/compaction_maps.md` - User-facing documentation
- `docs/docs/compaction_maps_errata.md` - Known limitations and design scope
- `docs/docs/compaction_maps_impl.md` - Implementation details
- `REMAPPING_BENCHMARKS.md` - JMH benchmark documentation and methodology
- `benchmark/remapping-optimization/` - Benchmark execution and analysis tools

**Implementation Context**:
- `CLAUDE.md` - This file (practical guidance for working with compaction maps)

## Branch and Status

**Branch**: `cmpmap` (NOT `vldb` - that's separate prototype)
**Status**: Implementation complete, full conflict resolution for Spark 3.5 and 4.0
**Test Status**: 150+ tests passing ✅
**Documentation Status**: Current ✅

**Recent Additions**:
- Smart selector tuning: require m >= 100 for StreamJoin (Jan 22, 2026) - 4.68% avg overhead
- Spark 4.0 conflict resolution parity (commit 539432b51)
- V3 Deletion Vector conflict resolution support (commit 368ae59e9)
- Staged scan optimization for position tracking (commit 0fef1aee2)
- Compaction conflict resolution (SparkRewriteDataFilesCommitManager, SparkCompactionConflictResolver)
- TestSparkCompactionConflictResolution (12 tests: V2 + V3 for both Spark versions)
- Configuration properties for opt-in conflict resolution

**Pending Work**:
1. Application transaction conflict resolution (BaseRowDelta auto-remapping)

---

*Generated during Claude Code sessions, 2026-01-07 to 2026-01-22*
*Models: Claude Sonnet 4.5, Claude Opus 4.5*
*Total Implementation: ~19k lines across core, tests, and documentation*
