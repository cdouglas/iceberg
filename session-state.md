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

### Phase 3: Testing and Validation (COMPLETED)

Created integration tests for conflict resolution:

**New Test File**:
- `TestSparkCompactionConflictResolution.java` - 6 tests for conflict resolution

**Tests**:
- `testConflictResolutionDisabledByDefault` - Verifies feature is off by default
- `testConflictResolutionEnabled` - Tests end-to-end resolution with single delete
- `testConflictResolutionWithMultipleDeletes` - Tests resolution with multiple deletes
- `testConflictResolutionMaxFilesExceeded` - Tests max-files limit configuration
- `testSparkCompactionConflictResolverDirectly` - Tests conflict detection directly
- `testNoConflictsWhenNoOverlap` - Tests non-overlapping delete scenario

**Notes**:
- Tests are restricted to V2 format version (position delete files)
- V3+ uses Deletion Vectors which have different semantics
- All 6 tests passing

### Phase 4: Documentation (COMPLETED)

Updated documentation with conflict resolution feature:

**Files Updated**:
- `docs/docs/compaction_maps.md` - Added conflict resolution section, configuration properties, examples
- `docs/docs/compaction_maps_errata.md` - Updated automatic conflict resolution status
- `CLAUDE.md` - Added new components, configuration, line references

**Documentation Changes**:
- Added `write.compaction.resolve-delete-conflicts` property documentation
- Added `write.compaction.resolve-delete-conflicts.max-files` property documentation
- Added "Compaction with Automatic Conflict Resolution" example
- Updated "Conflict Resolution Options" section
- Updated Future Work to reflect partial completion
- Updated errata to reflect compaction-level resolution is complete

### Phase 5: Soundness Review (COMPLETED)

Reviewed implementation for soundness issues and addressed critical gaps:

**Issues Identified and Fixed**:
1. **Multi-file position deletes** - Position delete files created with `DeleteGranularity.PARTITION` span multiple data files and couldn't be statically analyzed
   - Added `multiFilePositionDeletes()` to `DeleteConflictInfo` to track these separately
   - Updated `CompactionConflictDetector` to detect and flag these
   - Updated `SparkCompactionConflictResolver` to include them in scan tasks (content-based resolution)

2. **Equality delete handling** - Documented that equality deletes are intentionally ignored (not file-scoped)
   - Added explicit comments in `CompactionConflictDetector`
   - Added Javadoc explaining the behavior

**New Tests Added**:
- `testMultiFilePositionDeletesDetected` - Verifies multi-file position deletes are detected
- `testEqualityDeletesNotDetectedAsConflicts` - Verifies equality deletes are properly ignored
- `testMixedDeleteTypes` - Verifies both types are handled correctly together

**New API Methods**:
- `DeleteConflictInfo.multiFilePositionDeletes()` - Returns multi-file position deletes
- `DeleteConflictInfo.hasMultiFilePositionDeletes()` - Checks if any exist
- `DeleteConflictInfo.hasOnlyFileScopedConflicts()` - Checks for simple conflict case

### Phase 6: Transaction API Improvements (COMPLETED)

Improved the transaction-side API for resolving compaction conflicts:

**Issues Identified and Fixed**:
1. **Inconsistent error handling for unmapped positions**
   - `remapDelete()` threw exception, `remapDV*()` silently skipped
   - Added `remapDeleteOrNull()` for lenient handling (recommended for merge compactions)
   - Updated Javadoc to explain behavior differences

2. **Missing helper for loading compaction maps from exception**
   - Added `PositionDeleteRemapper.fromConflict(exception, io)` static helper
   - Handles multiple compaction maps, reuses remappers for same map location

3. **Multi-file position deletes not handled by needsRemapping()**
   - Added `mayNeedRemapping()` for conservative checking
   - Updated `needsRemapping()` Javadoc to explain limitations

4. **Documentation incomplete**
   - Updated `compaction_maps.md` with complete conflict resolution workflow
   - Added key API methods summary
   - Documented handling of unmapped positions

**New API Methods in PositionDeleteRemapper**:
- `fromConflict(exception, io)` - Loads remappers from CompactionConflictException
- `remapDeleteOrNull(delete)` - Lenient remapping (returns null if position not found)
- `mayNeedRemapping(deleteFile)` - Conservative check for uncertain cases

---

## Implementation Complete

All phases of Compaction Delete Recovery are now complete:
- **Phase 1**: Conflict Detection ✅
- **Phase 2**: Spark-Layer Resolution ✅
- **Phase 3**: Testing and Validation ✅
- **Phase 4**: Documentation ✅
- **Phase 5**: Soundness Review ✅
- **Phase 6**: Transaction API Improvements ✅

---

## Git Status

**Current Branch**: `cmpmap`

**Files Added**:
- `core/src/main/java/org/apache/iceberg/PositionDeletesScanTasks.java`
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkCompactionConflictResolver.java`
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkRewriteDataFilesCommitManager.java`
- `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/actions/TestSparkCompactionConflictResolution.java`

**Files Modified**:
- `core/src/main/java/org/apache/iceberg/TableProperties.java`
- `core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java`
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java`
- `COMPACTION_DELETE_RECOVERY_PLAN.md`

**Test Status**: All tests passing (153+ tests)

---

*Last Updated*: 2026-01-19
*Session*: Transaction API improvements complete
*Status*: All phases complete (1-6)
