# Compaction Conflict Recovery via Delete Remapping - Implementation Plan

## Overview

**Feature**: Enable compactions to preserve their work when conflicting with concurrent position delete transactions by using compaction maps to remap the deletes onto compacted files.

**Scenario**:
```
S1 (base snapshot)
├─> Transaction T: writes position deletes on file A
│   └─> Commits → S2
└─> Compaction C: compacts A → B, creates map
    └─> Detects conflict with T (A has deletes at S2)
    └─> Uses map to remap T's deletes from A to B
    └─> Commits → S3 (preserves both T's deletes and C's compaction)
```

**Benefits**:
- Compactions don't need to discard work or partially commit
- Better concurrency between compactions and delete operations
- Idempotent handling of filtered rows (safely dropped)

## Architecture

### Key Insight: Reuse Existing Infrastructure

The remapping operation is **the same** whether:
1. A **transaction** remaps its deletes after discovering files were compacted
2. A **compaction** remaps concurrent deletes to complete its commit

Both use the existing `PositionDeleteRemapper` with compaction maps. The only new functionality needed is:
1. **Conflict detection** (in `core/`) - identify which delete files reference compacted sources
2. **Resolution orchestration** (in Spark layer) - read deletes, remap, write new delete files

### Component Layers

**Core Layer** (`core/`):
- `CompactionConflictDetector` - Detects conflicts by scanning manifests (metadata only)
- `DeleteConflictInfo` - Metadata about detected conflicts
- `PositionDeleteRemapper` - Existing remapping logic (reused, not duplicated)

**Spark Layer** (`spark/`):
- `SparkCompactionConflictResolver` - Orchestrates reading/remapping/writing using Spark infrastructure
- Uses existing `PositionDeletesTable` for reading position deletes
- Uses existing `ClusteredPositionDeleteWriter` for writing

### Why This Split?

1. **Conflict detection** only needs manifest metadata - no file content reading
2. **Resolution** requires reading position delete file contents - this is format-specific (Parquet/Avro)
3. Spark already has infrastructure for reading/writing position deletes
4. `PositionDeleteRemapper` already has optimized remapping with smart strategy selection

---

## Implementation Phases

## Phase 1: Conflict Detection (COMPLETED)

**Objective**: Detect when compaction conflicts with position delete transactions.

**Location**: `core/` (manifest-level only)

### Components Created

1. **CompactionConflictDetector** (`core/src/main/java/org/apache/iceberg/CompactionConflictDetector.java`)
   - Scans snapshots between compaction start and current
   - Finds delete files that reference files being compacted
   - Uses existing `ManifestFiles.readDeleteManifest()` infrastructure
   - Does NOT read delete file contents

2. **DeleteConflictInfo** (`core/src/main/java/org/apache/iceberg/DeleteConflictInfo.java`)
   - Metadata about detected conflicts
   - List of conflicting delete files
   - Affected data file paths
   - Snapshot information

### Tests

- `TestCompactionConflictDetector.java` - 10 tests, all passing
  - Test detecting simple conflict (1 file, 1 delete manifest)
  - Test multiple files with deletes
  - Test multiple snapshots with deletes
  - Test no conflicts (clean compaction)
  - Test partial conflicts
  - Test deletes on non-compacted files (no conflict)

**Status**: ✅ COMPLETED

---

## Phase 2: Spark-Layer Resolution (IN PROGRESS)

**Objective**: Implement conflict resolution in the Spark layer using existing infrastructure.

**Location**: `spark/` module

### Components to Create

1. **SparkCompactionConflictResolver** (`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkCompactionConflictResolver.java`)
   - Input: Table, CompactionMap, DeleteConflictInfo
   - Uses `PositionDeletesTable` to read conflicting delete files
   - Uses `PositionDeleteRemapper` to remap positions
   - Uses `ClusteredPositionDeleteWriter` to write new delete files
   - Output: List of new DeleteFile objects

2. **Integration with RewriteDataFilesSparkAction**
   - After building compaction map
   - Before committing
   - If conflicts detected → resolve → include remapped deletes in commit

### Flow

```
RewriteDataFilesSparkAction.execute()
├─> Rewrite files (existing)
├─> Build compaction map (existing)
├─> commitFileGroups()
│   ├─> Detect conflicts (using CompactionConflictDetector)
│   ├─> If conflicts and resolution enabled:
│   │   └─> SparkCompactionConflictResolver.resolve()
│   │       ├─> Read position deletes via PositionDeletesTable scan
│   │       ├─> Filter to deletes referencing compacted files
│   │       ├─> Remap using PositionDeleteRemapper
│   │       └─> Write using ClusteredPositionDeleteWriter
│   └─> Commit with remapped deletes
```

### Configuration

Table properties (to be added):
- `write.compaction.resolve-delete-conflicts` (boolean, default: false)
- `write.compaction.resolve-delete-conflicts.max-files` (int, default: 100)

### Tests

- `TestSparkCompactionConflictResolver.java`
  - Test end-to-end resolution (detect → remap → write)
  - Test multiple file groups
  - Test partitioned deletes
  - Test with row data preservation
  - Test disabled by default

- `TestRewriteDataFilesWithConflictResolution.java`
  - Integration test: full compaction with delete conflict
  - Verify deletes are remapped correctly
  - Verify table reads return correct data after resolution

**Status**: 🔄 IN PROGRESS

---

## Phase 3: Configuration and Opt-In

**Objective**: Add configuration properties and make feature opt-in.

### Tasks

1. **Add table properties**
   - `write.compaction.resolve-delete-conflicts` (boolean, default: false)
   - `write.compaction.resolve-delete-conflicts.max-files` (int, default: 100)

2. **Modify RewriteDataFilesSparkAction**
   - Check property before resolving conflicts
   - If disabled: existing behavior (fail on conflict)
   - If enabled: attempt resolution
   - Enforce max-files limit (safety valve)

3. **Logging and metrics**
   - Log when resolution is attempted
   - Log when resolution succeeds
   - Log metrics (deletes remapped, files created)

**Status**: ⏳ PENDING

---

## Phase 4: Edge Cases and Optimization

**Objective**: Handle complex scenarios and optimize performance.

### Edge Cases

1. **Deletes on filtered rows**
   - Rows not in compaction map → silently dropped (idempotent)

2. **Multiple source files → one target**
   - Deletes from multiple sources merged in target

3. **Large delete sets**
   - Stream processing for memory efficiency
   - Batch writing

4. **Concurrent compactions**
   - Independent file groups can resolve independently

### Optimizations

1. **Parallel reading** of multiple delete files
2. **Bulk remapping** using existing optimized strategies
3. **Efficient writing** with proper batching

**Status**: ⏳ PENDING

---

## Phase 5: Documentation

**Objective**: Document feature for users and developers.

### Tasks

1. Update `docs/docs/compaction_maps.md` with conflict resolution section
2. Update `CLAUDE.md` with architecture details
3. Add Javadoc to all public APIs
4. Create example configuration

**Status**: ⏳ PENDING

---

## Removed/Superseded Components

The initial implementation (Phases 1-7 in session-state.md) created redundant infrastructure:

**Removed from `core/`**:
- `DeleteManifestRemapper` - duplicated `PositionDeleteRemapper`
- `PositionDeleteRecord` - duplicated `PositionDelete<?>`
- `DeleteManifestReader` - unnecessary, Spark has readers
- `RemappedDeleteWriter` - unnecessary, use existing writers
- `RemappingResult` - metrics tracking (can add to existing classes if needed)
- `ParallelDeleteReader` - unnecessary
- `DeleteManifestChanges` - unnecessary
- `CompactionConflictResolver` - moved to Spark layer

**Removed from `PositionDeleteRemapper`**:
- `findCompactedReferences()` - vestigial, never used
- `readPositionDeletes()` - unimplemented TODO
- `remapDeleteManifests()` - delegated to removed class

**Removed table properties**:
- `write.compaction.remap-conflicting-deletes`
- `write.compaction.remap-conflicting-deletes.max-manifests`

---

## Success Criteria

### Functional
- ✅ Compaction conflict detection works (Phase 1 complete)
- ⏳ Compactions can commit when conflicting with deletes
- ⏳ Position deletes correctly remapped to compacted files
- ⏳ Filtered rows (gaps) handled correctly
- ⏳ Feature is opt-in and configurable

### Performance
- ⏳ Remapping adds <10% overhead to compaction commit
- ⏳ Uses existing optimized remapping strategies
- ⏳ Memory-efficient for large delete sets

### Quality
- ✅ Conflict detection tests pass (10 tests)
- ⏳ Resolution tests pass
- ⏳ Integration tests pass
- ⏳ All existing tests pass

---

## Git History

**Commits on `cmpmap` branch**:
- `41f5a36f` - docs: Add implementation plan for compaction delete recovery
- `6d86a637f` - feat(compaction): Add delete manifest reading infrastructure (Phase 1) - SUPERSEDED
- `da7414735` - feat(compaction): Implement delete remapping core logic (Phase 2) - SUPERSEDED
- `8d9de4fd0` - feat(compaction): Add remapped delete manifest writer (Phase 3) - SUPERSEDED
- `97057c79e` - feat(compaction): Add conflict detection (Phase 4) - KEPT
- `a8686fe83` - feat(compaction): Add conflict resolution integration (Phase 5) - SUPERSEDED
- `fcf9ecd02` - feat(compaction): Add configuration and opt-in (Phase 6) - SUPERSEDED
- `0a0569099` - feat(compaction): Add edge case handling (Phase 7) - SUPERSEDED
- `PENDING` - refactor: Remove redundant infrastructure, keep conflict detection

---

*Plan revised: January 19, 2026*
*Feature: Compaction Conflict Recovery via Delete Remapping*
*Architecture: Detection in core/, Resolution in Spark layer*
