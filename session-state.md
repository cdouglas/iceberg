# Session State: Compaction Delete Recovery Implementation

## Current Work

Implementing **Compaction Delete Recovery** feature - a system that allows compaction operations (C) to remap position deletes from concurrent delete transactions (T) when both start from the same snapshot.

## Progress Summary

### Phase 1: Conflict Detection (COMPLETED)

Created conflict detection infrastructure in `core/`:
- `CompactionConflictDetector.java` - Detects conflicts by scanning manifests
- `DeleteConflictInfo.java` - Metadata about detected conflicts
- `TestCompactionConflictDetector.java` - 10 tests, all passing

### Phase 2: Spark-Layer Resolution (COMPLETED)

Created conflict resolution infrastructure in `spark/`:

**New Files**:
- `SparkCompactionConflictResolver.java` - Reads, remaps, and writes position deletes using Spark
- `SparkRewriteDataFilesCommitManager.java` - Extends core commit manager with conflict resolution
- `PositionDeletesScanTasks.java` (core) - Helper to create scan tasks for delete files

**Modified Files**:
- `RewriteDataFilesSparkAction.java` - Uses `SparkRewriteDataFilesCommitManager`
- `RewriteDataFilesCommitManager.java` - Made `CommitService` constructor protected
- `TableProperties.java` - Added conflict resolution properties

**Table Properties Added**:
- `write.compaction.resolve-delete-conflicts` (boolean, default: false)
- `write.compaction.resolve-delete-conflicts.max-files` (int, default: 100)

### Architecture

**Detection** (core layer):
1. `CompactionConflictDetector` scans snapshots between compaction start and current
2. Finds delete files that reference files being compacted
3. Returns `DeleteConflictInfo` with conflict metadata

**Resolution** (Spark layer):
1. `SparkRewriteDataFilesCommitManager.detectAndResolveConflicts()` detects conflicts
2. `SparkCompactionConflictResolver.resolve()` reads position deletes via Spark
3. Uses `PositionDeleteRemapper` to remap positions
4. Writes new delete files using existing Spark infrastructure
5. Includes remapped delete files in the commit

**Flow**:
```
RewriteDataFilesSparkAction.execute()
├─> Rewrite files (existing)
├─> commitManager.commitFileGroups()
│   ├─> Build compaction map
│   ├─> If resolution enabled:
│   │   └─> detectAndResolveConflicts()
│   │       ├─> CompactionConflictDetector.detectConflicts()
│   │       └─> SparkCompactionConflictResolver.resolve()
│   │           ├─> Read position deletes via Spark
│   │           ├─> Remap using PositionDeleteRemapper
│   │           └─> Write new delete files
│   └─> Commit with remapped deletes
```

---

## Next Steps

### Phase 3: Testing and Validation

1. Create integration tests for `SparkCompactionConflictResolver`
2. Create end-to-end tests for conflict resolution during rewrite
3. Test edge cases (filtered rows, multiple source files, etc.)

### Phase 4: Documentation

1. Update `docs/docs/compaction_maps.md` with conflict resolution section
2. Update `CLAUDE.md` with new components
3. Add example configuration

---

## Git Status

**Current Branch**: `cmpmap`

**Files Added**:
- `core/src/main/java/org/apache/iceberg/PositionDeletesScanTasks.java`
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkCompactionConflictResolver.java`
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkRewriteDataFilesCommitManager.java`

**Files Modified**:
- `core/src/main/java/org/apache/iceberg/TableProperties.java`
- `core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java`
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java`
- `COMPACTION_DELETE_RECOVERY_PLAN.md`

**Test Status**: All existing tests passing

---

*Last Updated*: 2026-01-19
*Session*: Spark-layer implementation
*Status*: Phase 2 complete, ready for testing
