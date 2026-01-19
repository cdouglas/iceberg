# Session State: Compaction Delete Recovery Implementation

## Current Work

Implementing **Compaction Delete Recovery** feature - a system that allows compaction operations (C) to remap position deletes from concurrent delete transactions (T) when both start from the same snapshot.

## Progress Summary

### Phases 1-7: Initial Implementation (SUPERSEDED)

The initial implementation created redundant infrastructure in `core/` that duplicated existing compaction map machinery:
- `DeleteManifestRemapper` duplicated `PositionDeleteRemapper`
- `PositionDeleteRecord` duplicated `PositionDelete<?>`
- `DeleteManifestReader`, `RemappedDeleteWriter` duplicated existing readers/writers
- `CompactionConflictResolver` created core-level resolution when this should happen at the engine (Spark) layer

### Refactoring: Remove Redundant Infrastructure (COMPLETED)

**Decision**: The reading, remapping, and writing of position deletes should use existing infrastructure and happen at the Spark layer, not create new classes in `core/`.

**Removed Files**:
- `core/src/main/java/org/apache/iceberg/DeleteManifestRemapper.java`
- `core/src/main/java/org/apache/iceberg/PositionDeleteRecord.java`
- `core/src/main/java/org/apache/iceberg/io/DeleteManifestReader.java`
- `core/src/main/java/org/apache/iceberg/io/RemappedDeleteWriter.java`
- `core/src/main/java/org/apache/iceberg/RemappingResult.java`
- `core/src/main/java/org/apache/iceberg/ParallelDeleteReader.java`
- `core/src/main/java/org/apache/iceberg/DeleteManifestChanges.java`
- `core/src/main/java/org/apache/iceberg/CompactionConflictResolver.java`
- `core/src/test/java/org/apache/iceberg/TestDeleteManifestRemapper.java`
- `core/src/test/java/org/apache/iceberg/TestRemappingEdgeCases.java`
- `core/src/test/java/org/apache/iceberg/io/TestDeleteManifestReader.java`
- `core/src/test/java/org/apache/iceberg/io/TestRemappedDeleteWriter.java`
- `core/src/test/java/org/apache/iceberg/TestCompactionConflictResolver.java`
- `core/src/test/java/org/apache/iceberg/TestCompactionConflictResolutionConfig.java`

**Modified Files**:
- `PositionDeleteRemapper.java` - Removed vestigial `findCompactedReferences()` and `readPositionDeletes()` methods, and `remapDeleteManifests()` static method
- `RewriteDataFilesCommitManager.java` - Removed conflict resolution integration
- `TableProperties.java` - Removed `COMPACTION_REMAP_CONFLICTING_DELETES` and `COMPACTION_REMAP_MAX_MANIFESTS` properties
- `ManifestFiles.java` - Removed utility methods that used deleted classes

**Kept Files (legitimate new functionality)**:
- `core/src/main/java/org/apache/iceberg/CompactionConflictDetector.java` - Detects conflicts by reading manifests (not file contents)
- `core/src/main/java/org/apache/iceberg/DeleteConflictInfo.java` - Metadata about detected conflicts
- `core/src/test/java/org/apache/iceberg/TestCompactionConflictDetector.java` - 10 tests, all passing

---

## Architecture Clarification

### Two Directions, Same Remapping Operation

1. **Transaction Recovery** (existing): Transaction T has position deletes referencing files that got compacted. T uses the compaction map via `PositionDeleteRemapper` to remap its deletes.

2. **Compaction Rebasing** (new): Compaction C completed, but concurrent transaction T added deletes referencing C's source files. C needs to remap T's deletes to complete.

Both use the **same remapping logic** via `PositionDeleteRemapper`. The only new functionality needed is:
1. **Conflict detection** (`CompactionConflictDetector`) - identify which delete files reference compacted sources
2. **Soundness checks** - ensure it's safe to remap
3. **Orchestration** - integrate into the compaction commit flow

### Where Resolution Should Happen

The actual reading, remapping, and writing of position deletes should happen at the **engine layer** (Spark) where:
- Position delete reading infrastructure already exists
- `PositionDeleteRemapper` can be used for remapping
- Existing writers can write new delete files

**Not** in `core/` with new redundant classes.

---

## What Remains in Core

### CompactionConflictDetector (Keep)

Detects conflicts by scanning manifests (metadata only, not file contents):
- Scans snapshots between compaction start and current
- Finds delete files that reference files being compacted
- Uses existing `ManifestFiles.readDeleteManifest()` infrastructure
- Returns `DeleteConflictInfo` with conflict metadata

### DeleteConflictInfo (Keep)

Simple metadata class containing:
- List of conflicting delete files
- Affected data file paths
- Snapshot information

---

## Next Steps

### Implement Conflict Resolution at Spark Layer

The actual conflict resolution (reading position deletes, remapping via `PositionDeleteRemapper`, writing new delete files) should be implemented in the Spark action layer, potentially in:
- `RewriteDataFilesSparkAction` or
- A new Spark-level conflict resolver that uses existing infrastructure

This would:
1. Use `CompactionConflictDetector` to detect conflicts
2. Read position deletes using Spark's existing infrastructure
3. Remap using `PositionDeleteRemapper`
4. Write new delete files using existing writers

---

## Git Status

**Current Branch**: `cmpmap`

**Recent Commits**:
- Pending: Refactoring to remove redundant infrastructure

**Test Status**: All remaining tests passing

---

*Last Updated*: 2026-01-19
*Session*: Refactoring session
*Status*: Redundant infrastructure removed, ready for Spark-layer implementation
