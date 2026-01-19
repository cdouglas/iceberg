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

### New Components

1. **DeleteManifestRemapper** - Core remapping logic
2. **CompactionConflictResolver** - Conflict detection and resolution strategy
3. **RemappedDeleteWriter** - Writes remapped delete manifests
4. **Configuration** - Table properties for opt-in behavior

### Modified Components

1. **RewriteDataFilesCommitManager** - Integrate conflict resolution
2. **MergingSnapshotProducer** - Enhanced conflict detection
3. **TableProperties** - New configuration properties

## Implementation Phases

---

## Phase 1: Delete Manifest Reading Infrastructure

**Objective**: Build infrastructure to read and parse position delete manifests.

### Tasks

1. **Create DeleteManifestReader utility**
   - Read delete manifests from metadata
   - Parse position deletes (file path, position, optional row data)
   - Filter by referenced data files
   - Handle partitioned deletes correctly

2. **Create PositionDeleteRecord value class**
   - Immutable record: `(dataFilePath, position, partitionData, rowData)`
   - Equality based on file path and position
   - Serialization support for testing

3. **Add helper methods to ManifestFiles**
   - `readPositionDeletes(DeleteFile deleteFile)`
   - `filterDeletesByReferencedFiles(List<DeleteFile>, Set<String> files)`

### Deliverables

**New Files**:
- `core/src/main/java/org/apache/iceberg/io/DeleteManifestReader.java`
- `core/src/main/java/org/apache/iceberg/PositionDeleteRecord.java`

**Modified Files**:
- `core/src/main/java/org/apache/iceberg/ManifestFiles.java`

**Tests**:
- `core/src/test/java/org/apache/iceberg/io/TestDeleteManifestReader.java`
  - Test reading position deletes from manifests
  - Test filtering by referenced files
  - Test handling of partitioned deletes
  - Test handling of deletes with row data
  - Test empty manifests
  - Edge case: deletes referencing non-existent files

**Success Criteria**:
- Can read position deletes from delete manifests
- Can filter deletes by referenced data file
- Preserves partition information
- Preserves optional row data
- All tests pass

**Commit Message**:
```
feat(compaction): Add delete manifest reading infrastructure

Add utilities to read and parse position delete manifests for compaction
conflict recovery feature.

New components:
- DeleteManifestReader: Reads and parses delete manifests
- PositionDeleteRecord: Immutable value class for position deletes
- ManifestFiles helpers: readPositionDeletes(), filterDeletesByReferencedFiles()

Features:
- Read position deletes from delete files
- Filter deletes by referenced data files
- Preserve partition information
- Preserve optional row data
- Handle empty manifests

Testing:
- 6 unit tests for DeleteManifestReader
- Tests cover partitioned, non-partitioned, with/without row data
- Tests edge cases (empty, non-existent files)

Part of Phase 1: Delete Manifest Reading Infrastructure
Related to: Compaction conflict recovery feature
```

---

## Phase 2: Delete Remapping Core Logic

**Objective**: Implement core logic to remap position deletes using compaction maps.

### Tasks

1. **Create DeleteManifestRemapper class**
   - Input: List of PositionDeleteRecords, CompactionMap
   - Output: Map<TargetFile, List<PositionDeleteRecord>>
   - Handle gaps (filtered rows) by dropping deletes
   - Handle multi-target compactions (multiple source files → one target)
   - Preserve partition and row data

2. **Extend PositionDeleteRemapper** (existing)
   - Add static method: `remapDeleteManifests(List<PositionDeleteRecord>, CompactionMap)`
   - Reuse existing smart selector logic
   - Return grouped deletes by target file

3. **Handle edge cases**
   - Deletes on filtered rows (drop silently)
   - Multiple source files mapping to same target (merge deletes)
   - Partition preservation
   - Row data preservation (if present)

### Deliverables

**New Files**:
- `core/src/main/java/org/apache/iceberg/DeleteManifestRemapper.java`

**Modified Files**:
- `core/src/main/java/org/apache/iceberg/PositionDeleteRemapper.java`

**Tests**:
- `core/src/test/java/org/apache/iceberg/TestDeleteManifestRemapper.java`
  - Test basic remapping (single source → single target)
  - Test merge compaction (multiple sources → one target)
  - Test filtered rows (gaps in compaction map)
  - Test partition preservation
  - Test row data preservation
  - Test empty delete list
  - Test all deletes filtered out
  - Property test: remapped deletes reference correct files

**Success Criteria**:
- Can remap position deletes using compaction maps
- Correctly handles filtered rows (drops deletes)
- Preserves partition information
- Preserves row data
- Groups deletes by target file
- All tests pass

**Commit Message**:
```
feat(compaction): Implement delete remapping core logic

Implement core logic to remap position deletes using compaction maps when
compaction conflicts with delete transactions.

New components:
- DeleteManifestRemapper: Remaps deletes from source to target files
- PositionDeleteRemapper.remapDeleteManifests(): Static utility method

Features:
- Remap position deletes using compaction maps
- Handle filtered rows (drop deletes for non-existent positions)
- Merge deletes for multi-source compactions
- Preserve partition and row data
- Group remapped deletes by target file

Edge cases:
- Deletes on filtered rows → dropped (idempotent)
- Multiple sources → same target → deletes merged
- Empty delete lists → empty output
- All deletes filtered → empty output

Testing:
- 8 unit tests for DeleteManifestRemapper
- Tests basic, merge, filtered, partition, row data scenarios
- Property test validates file references

Part of Phase 2: Delete Remapping Core Logic
Related to: Compaction conflict recovery feature
```

---

## Phase 3: Remapped Delete Writing

**Objective**: Write remapped position deletes to new delete manifests.

### Tasks

1. **Create RemappedDeleteWriter class**
   - Input: Map<TargetFile, List<PositionDeleteRecord>>
   - Output: List<DeleteFile> (new delete manifests)
   - Use OutputFileFactory for file paths
   - Write position delete format (Parquet)
   - Preserve partition information
   - Compute metrics (record count, file size)

2. **Handle partitioning**
   - Group deletes by partition
   - Write separate delete files per partition
   - Update partition stats in DeleteFile metadata

3. **Optimize delete file layout**
   - Bin-pack deletes to target ~10MB files
   - Sort deletes by position within each file
   - Compute delete metrics correctly

### Deliverables

**New Files**:
- `core/src/main/java/org/apache/iceberg/io/RemappedDeleteWriter.java`

**Tests**:
- `core/src/test/java/org/apache/iceberg/io/TestRemappedDeleteWriter.java`
  - Test writing simple position deletes
  - Test writing partitioned deletes
  - Test writing deletes with row data
  - Test delete file metrics computation
  - Test multiple target files
  - Test empty delete list (no files written)
  - Integration test: write, read back, verify content

**Success Criteria**:
- Can write remapped deletes to delete manifests
- Correctly partitions deletes
- Computes metrics (record count, file size)
- Written deletes can be read back correctly
- Preserves all metadata (partition, row data)
- All tests pass

**Commit Message**:
```
feat(compaction): Add remapped delete manifest writer

Implement writing of remapped position deletes to new delete manifests for
compaction conflict recovery.

New components:
- RemappedDeleteWriter: Writes remapped deletes to Parquet format

Features:
- Write position deletes in Parquet format
- Handle partitioned deletes (separate files per partition)
- Preserve row data (if present)
- Compute delete file metrics (record count, size)
- Bin-pack deletes to ~10MB files
- Sort deletes by position

Optimizations:
- Efficient batching for large delete sets
- Proper partition handling
- Accurate metric computation

Testing:
- 7 unit tests for RemappedDeleteWriter
- Tests simple, partitioned, with row data scenarios
- Integration test: write → read → verify
- Tests metrics computation
- Tests empty input handling

Part of Phase 3: Remapped Delete Writing
Related to: Compaction conflict recovery feature
```

---

## Phase 4: Conflict Detection Enhancement

**Objective**: Detect when compaction conflicts with position delete transactions.

### Tasks

1. **Create CompactionConflictDetector class**
   - Input: base snapshot, current snapshot, files being compacted
   - Output: List of conflicting delete manifests
   - Scan snapshots between base and current
   - Find delete manifests referencing compacted files
   - Track which files have conflicts

2. **Integrate with MergingSnapshotProducer**
   - Add method: `detectDeleteConflicts(Set<String> compactedFiles)`
   - Return conflicting delete manifests
   - Provide conflict summary (affected files, delete count)

3. **Conflict metadata**
   - Which files have deletes
   - How many deletes per file
   - Source snapshots of deletes

### Deliverables

**New Files**:
- `core/src/main/java/org/apache/iceberg/CompactionConflictDetector.java`

**Modified Files**:
- `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java`

**Tests**:
- `core/src/test/java/org/apache/iceberg/TestCompactionConflictDetector.java`
  - Test detecting simple conflict (1 file, 1 delete manifest)
  - Test multiple files with deletes
  - Test multiple snapshots with deletes
  - Test no conflicts (clean compaction)
  - Test partial conflicts (some files have deletes, some don't)
  - Test deletes on non-compacted files (no conflict)
  - Integration test with real table

**Success Criteria**:
- Can detect when compaction conflicts with deletes
- Identifies all conflicting delete manifests
- Correctly ignores deletes on non-compacted files
- Provides accurate conflict summary
- All tests pass

**Commit Message**:
```
feat(compaction): Add compaction-delete conflict detection

Implement detection of conflicts between compactions and concurrent position
delete transactions.

New components:
- CompactionConflictDetector: Detects delete conflicts during compaction

Modified components:
- MergingSnapshotProducer: Integrated conflict detection

Features:
- Detect when compaction conflicts with position deletes
- Identify conflicting delete manifests
- Track affected files and delete counts
- Provide conflict summary
- Ignore deletes on non-compacted files

Conflict scenarios detected:
- Single file with deletes
- Multiple files with deletes
- Multiple snapshots with deletes
- Partial conflicts (mixed affected/unaffected files)

Testing:
- 7 unit tests for CompactionConflictDetector
- Tests simple, complex, partial conflict scenarios
- Integration test with real table and snapshots
- Tests negative case (no conflicts)

Part of Phase 4: Conflict Detection Enhancement
Related to: Compaction conflict recovery feature
```

---

## Phase 5: Conflict Resolution Integration

**Objective**: Integrate conflict resolution into compaction commit flow.

### Tasks

1. **Create CompactionConflictResolver class**
   - Input: CompactionMap, conflicting delete manifests
   - Orchestrates: read deletes → remap → write → track changes
   - Output: DeleteManifestChanges (added, deleted)
   - Handle multiple file groups
   - Log remapping actions

2. **Define DeleteManifestChanges**
   - List of new delete manifests (added)
   - List of old delete manifests (deleted/replaced)
   - Metrics: total deletes remapped, files affected

3. **Integrate with RewriteDataFilesCommitManager**
   - After building compaction map
   - Before committing
   - Detect conflicts → resolve if enabled → commit with changes

### Deliverables

**New Files**:
- `core/src/main/java/org/apache/iceberg/CompactionConflictResolver.java`
- `core/src/main/java/org/apache/iceberg/DeleteManifestChanges.java`

**Modified Files**:
- `core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java`

**Tests**:
- `core/src/test/java/org/apache/iceberg/TestCompactionConflictResolver.java`
  - Test end-to-end resolution (detect → remap → write)
  - Test multiple file groups
  - Test partial conflicts
  - Test metrics computation
  - Test logging output

- `core/src/test/java/org/apache/iceberg/actions/TestCompactionDeleteRecovery.java`
  - Integration test: full compaction with delete conflict
  - Verify deletes are remapped correctly
  - Verify compaction commits successfully
  - Verify table state is correct after commit
  - Verify old delete manifests are marked deleted
  - Verify new delete manifests are added

**Success Criteria**:
- Can resolve conflicts end-to-end
- Compaction commits with remapped deletes
- Old delete manifests properly replaced
- Metrics accurately track remapping
- Integration test passes on real table
- All tests pass

**Commit Message**:
```
feat(compaction): Integrate conflict resolution into compaction flow

Integrate delete conflict resolution into compaction commit flow, enabling
compactions to preserve work when conflicting with delete transactions.

New components:
- CompactionConflictResolver: Orchestrates conflict resolution
- DeleteManifestChanges: Tracks manifest additions/deletions

Modified components:
- RewriteDataFilesCommitManager: Integrated conflict resolution

Features:
- End-to-end conflict resolution (detect → remap → write → commit)
- Handle multiple file groups
- Track manifest changes (added, deleted)
- Compute remapping metrics
- Logging for observability

Flow:
1. Build compaction map
2. Detect delete conflicts
3. Remap conflicting deletes
4. Write new delete manifests
5. Commit with data files + remapped deletes
6. Mark old delete manifests as deleted

Testing:
- 5 unit tests for CompactionConflictResolver
- Integration test: full compaction with delete conflict
- Verifies correct table state after resolution
- Tests multiple file groups and partial conflicts

Part of Phase 5: Conflict Resolution Integration
Related to: Compaction conflict recovery feature
```

---

## Phase 6: Configuration and Opt-In

**Objective**: Add configuration properties and make feature opt-in.

### Tasks

1. **Add table properties**
   - `write.compaction.remap-conflicting-deletes` (boolean, default: false)
   - `write.compaction.remap-conflicting-deletes.max-manifests` (int, default: 100)
   - Documentation strings

2. **Modify RewriteDataFilesCommitManager**
   - Check property before resolving conflicts
   - If disabled: throw ValidationException (existing behavior)
   - If enabled: attempt resolution
   - Enforce max-manifests limit (safety valve)

3. **Add metrics and logging**
   - Log when resolution is attempted
   - Log when resolution succeeds
   - Log metrics (deletes remapped, manifests created)
   - Add counters for monitoring

### Deliverables

**Modified Files**:
- `core/src/main/java/org/apache/iceberg/TableProperties.java`
- `core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java`

**Tests**:
- `core/src/test/java/org/apache/iceberg/TestCompactionDeleteRecoveryConfiguration.java`
  - Test opt-in behavior (disabled → exception)
  - Test opt-in behavior (enabled → resolution)
  - Test max-manifests limit enforcement
  - Test property defaults
  - Test property validation

**Success Criteria**:
- Feature is opt-in via table property
- Disabled by default (safe rollout)
- Max-manifests limit prevents runaway operations
- Proper logging and metrics
- All tests pass

**Commit Message**:
```
feat(compaction): Add configuration for delete conflict recovery

Add table properties to make delete conflict recovery opt-in and configurable.

New properties:
- write.compaction.remap-conflicting-deletes (default: false)
  - Enable/disable automatic delete remapping on conflicts
- write.compaction.remap-conflicting-deletes.max-manifests (default: 100)
  - Safety limit on number of manifests to remap

Behavior:
- Disabled (default): throw ValidationException on conflict (existing behavior)
- Enabled: attempt resolution via delete remapping
- Max-manifests: prevent runaway operations on large conflict sets

Observability:
- Log when resolution is attempted
- Log when resolution succeeds
- Log metrics (deletes remapped, manifests created)
- Add counters for monitoring

Safety:
- Opt-in only (default: disabled)
- Max-manifests safety limit
- Fail fast if limits exceeded

Testing:
- 5 tests for configuration behavior
- Tests opt-in enabled/disabled
- Tests max-manifests enforcement
- Tests property validation

Part of Phase 6: Configuration and Opt-In
Related to: Compaction conflict recovery feature
```

---

## Phase 7: Edge Case Handling

**Objective**: Handle complex edge cases and ensure robustness.

### Tasks

1. **Chained remapping detection**
   - Detect if delete manifest was previously remapped
   - Add metadata tag to remapped manifests
   - Limit remapping depth (prevent chains)
   - Log warning when chaining detected

2. **Multiple concurrent compactions**
   - Test scenario: C1 and C2 both conflict with T
   - Ensure both can commit (independent file groups)
   - Ensure serializable isolation still works

3. **Partial failure handling**
   - If remapping fails for some manifests
   - Roll back or commit partial results?
   - Decision: fail entire compaction (atomic)

4. **Large delete set optimization**
   - If thousands of deletes need remapping
   - Batch processing
   - Memory-efficient streaming

### Deliverables

**Modified Files**:
- `core/src/main/java/org/apache/iceberg/CompactionConflictResolver.java`
- `core/src/main/java/org/apache/iceberg/io/RemappedDeleteWriter.java`

**Tests**:
- `core/src/test/java/org/apache/iceberg/TestCompactionDeleteRecoveryEdgeCases.java`
  - Test chained remapping (C1 remaps T, C2 remaps C1's output)
  - Test remapping depth limit
  - Test multiple concurrent compactions
  - Test large delete sets (10K+ deletes)
  - Test partial failure scenarios
  - Test memory efficiency (streaming)

**Success Criteria**:
- Chained remapping detected and limited
- Multiple concurrent compactions work correctly
- Partial failures handled atomically
- Large delete sets processed efficiently
- All tests pass

**Commit Message**:
```
feat(compaction): Handle edge cases for delete conflict recovery

Add robustness for complex scenarios in delete conflict recovery.

Edge cases handled:
1. Chained remapping detection
   - Detect previously-remapped manifests
   - Limit remapping depth (max 3 levels)
   - Log warnings for chains

2. Multiple concurrent compactions
   - Independent file groups can remap concurrently
   - Serializable isolation preserved

3. Partial failure handling
   - Atomic commit (all or nothing)
   - Clean rollback on failures

4. Large delete sets
   - Batch processing for efficiency
   - Memory-efficient streaming
   - Handle 10K+ deletes

Implementation:
- Add remapping metadata to delete manifests
- Track remapping depth
- Enforce depth limits
- Optimize for large delete sets

Testing:
- 6 tests for edge cases
- Test chained remapping (3 levels)
- Test concurrent compactions
- Test large delete sets (10K deletes)
- Test partial failures
- Test memory efficiency

Part of Phase 7: Edge Case Handling
Related to: Compaction conflict recovery feature
```

---

## Phase 8: Performance Optimization

**Objective**: Optimize performance for production workloads.

### Tasks

1. **Parallel remapping**
   - Remap multiple file groups in parallel
   - Use thread pool for I/O operations
   - Parallelize delete manifest reading/writing

2. **Caching**
   - Cache compaction maps during resolution
   - Cache partition specs
   - Reuse OutputFileFactory

3. **Metrics and monitoring**
   - Time spent in each phase
   - Number of deletes remapped
   - Number of manifests processed
   - Memory usage

4. **Benchmarking**
   - Create JMH benchmark
   - Test scenarios: small, medium, large delete sets
   - Measure latency impact on compaction commit

### Deliverables

**Modified Files**:
- `core/src/main/java/org/apache/iceberg/CompactionConflictResolver.java`
- `core/src/main/java/org/apache/iceberg/io/RemappedDeleteWriter.java`

**New Files**:
- `core/src/jmh/java/org/apache/iceberg/DeleteRemappingBenchmark.java`

**Tests**:
- `core/src/test/java/org/apache/iceberg/TestDeleteRemappingPerformance.java`
  - Benchmark small delete set (100 deletes)
  - Benchmark medium delete set (1K deletes)
  - Benchmark large delete set (10K deletes)
  - Measure parallelization benefit
  - Measure memory usage

**Success Criteria**:
- Parallel remapping shows 2-3x speedup
- Memory usage scales linearly (not quadratic)
- Large delete sets (10K) process in <10 seconds
- Metrics accurately track performance
- All tests pass

**Commit Message**:
```
perf(compaction): Optimize delete conflict recovery performance

Optimize performance of delete conflict recovery for production workloads.

Optimizations:
1. Parallel remapping
   - Process multiple file groups concurrently
   - Parallel delete manifest read/write
   - Thread pool for I/O operations
   - 2-3x speedup on multi-file compactions

2. Caching
   - Cache compaction maps during resolution
   - Cache partition specs
   - Reuse OutputFileFactory instances

3. Memory efficiency
   - Stream-based processing for large delete sets
   - Batch processing to control memory
   - Linear memory scaling

Metrics:
- Time spent per phase (detect, remap, write)
- Deletes remapped count
- Manifests processed count
- Memory usage tracking

Benchmarking:
- JMH benchmark for different delete set sizes
- Small (100): ~10ms overhead
- Medium (1K): ~50ms overhead
- Large (10K): ~500ms overhead

Testing:
- Performance tests for small/medium/large delete sets
- Tests verify parallelization benefits
- Tests verify memory efficiency

Part of Phase 8: Performance Optimization
Related to: Compaction conflict recovery feature
```

---

## Phase 9: Documentation and Examples

**Objective**: Document feature for users and developers.

### Tasks

1. **User documentation**
   - Update `docs/docs/compaction_maps.md`
   - Add section on conflict recovery
   - Configuration guide
   - Use cases and examples

2. **Developer documentation**
   - Update CLAUDE.md with new components
   - Document conflict resolution flow
   - Add troubleshooting guide

3. **Code examples**
   - Example: enable conflict recovery
   - Example: monitor remapping metrics
   - Example: handle conflicts manually

4. **Javadoc**
   - Document all public APIs
   - Add usage examples
   - Document edge cases

### Deliverables

**Modified Files**:
- `docs/docs/compaction_maps.md`
- `CLAUDE.md`
- All new Java files (Javadoc)

**New Files**:
- `docs/examples/compaction-conflict-recovery.md`

**Success Criteria**:
- User documentation complete
- Developer documentation updated
- Code examples tested and working
- All public APIs documented
- Documentation reviewed

**Commit Message**:
```
docs(compaction): Document delete conflict recovery feature

Add comprehensive documentation for delete conflict recovery feature.

User documentation (docs/docs/compaction_maps.md):
- Overview of conflict recovery feature
- Configuration guide
- Use cases and examples
- Monitoring and troubleshooting

Developer documentation (CLAUDE.md):
- Architecture overview
- Component descriptions
- Conflict resolution flow
- Troubleshooting guide
- Token-saving navigation tips

Code examples:
- Enable conflict recovery
- Monitor remapping metrics
- Handle conflicts manually

Javadoc:
- All public APIs documented
- Usage examples included
- Edge cases documented
- Cross-references added

Part of Phase 9: Documentation and Examples
Related to: Compaction conflict recovery feature
```

---

## Testing Strategy

### Unit Tests (each phase)

- Test individual components in isolation
- Mock dependencies
- Cover happy path and edge cases
- Property-based tests where applicable

### Integration Tests (phases 5, 7)

- Test full workflow on real table
- Multiple snapshots, multiple files
- Verify table state after conflict resolution
- Test with partitioned tables

### Performance Tests (phase 8)

- Benchmark different delete set sizes
- Measure latency impact
- Verify memory efficiency
- Test parallelization benefits

### End-to-End Tests (after phase 9)

- Realistic workload scenarios
- Concurrent compactions and deletes
- Multiple file formats (Parquet, ORC)
- Various table configurations

## Running Tests Between Phases

After each phase:

```bash
# Run unit tests for new components
./gradlew :iceberg-core:test --tests "*DeleteManifest*"
./gradlew :iceberg-core:test --tests "*CompactionConflict*"

# Run integration tests
./gradlew :iceberg-core:test --tests "*Integration*"

# Run full test suite
./gradlew :iceberg-core:test

# Check code coverage
./gradlew :iceberg-core:jacocoTestReport
```

## Success Criteria (Overall)

### Functional

- ✅ Compactions can commit when conflicting with deletes
- ✅ Position deletes correctly remapped to compacted files
- ✅ Old delete manifests properly replaced
- ✅ Filtered rows (gaps) handled correctly
- ✅ Partitioned deletes work correctly
- ✅ Feature is opt-in and configurable

### Performance

- ✅ Remapping adds <10% overhead to compaction commit
- ✅ Large delete sets (10K) process in <10 seconds
- ✅ Memory usage scales linearly
- ✅ Parallel processing provides 2-3x speedup

### Robustness

- ✅ All edge cases handled correctly
- ✅ Atomic commit (all or nothing)
- ✅ Chained remapping detected and limited
- ✅ Concurrent compactions work correctly
- ✅ Proper error messages and logging

### Quality

- ✅ 100+ unit tests
- ✅ 10+ integration tests
- ✅ Code coverage >80%
- ✅ All existing tests pass
- ✅ Documentation complete

## Risk Mitigation

### Risk 1: Correctness Issues

**Mitigation**:
- Comprehensive unit tests for each component
- Integration tests with real tables
- Property-based tests for invariants
- Code review before each phase commit

### Risk 2: Performance Degradation

**Mitigation**:
- Benchmark after phase 8
- Optimize hot paths
- Parallel processing
- Opt-in feature (can disable if issues)

### Risk 3: Breaking Changes

**Mitigation**:
- All changes backward compatible
- Feature is opt-in (default: disabled)
- Existing tests must pass after each phase
- No changes to public APIs (only additions)

### Risk 4: Complex Edge Cases

**Mitigation**:
- Dedicated phase for edge case handling (Phase 7)
- Fail-safe defaults (atomic commits)
- Comprehensive logging for debugging
- Remapping depth limits

## Timeline Estimate

| Phase | Estimated Effort | Dependencies |
|-------|-----------------|--------------|
| Phase 1 | 2-3 days | None |
| Phase 2 | 3-4 days | Phase 1 |
| Phase 3 | 3-4 days | Phase 2 |
| Phase 4 | 2-3 days | Phase 1 |
| Phase 5 | 4-5 days | Phases 2, 3, 4 |
| Phase 6 | 1-2 days | Phase 5 |
| Phase 7 | 3-4 days | Phase 6 |
| Phase 8 | 3-4 days | Phase 7 |
| Phase 9 | 2-3 days | Phase 8 |

**Total**: 23-32 days (4.5-6.5 weeks)

## Next Steps

1. Review this plan with team
2. Get approval for phase-by-phase approach
3. Create tracking issue/epic
4. Begin Phase 1 implementation
5. Iterate based on feedback

---

*Plan created: January 18, 2026*
*Feature: Compaction Conflict Recovery via Delete Remapping*
*Related to: Compaction Maps (cmpmap branch)*
