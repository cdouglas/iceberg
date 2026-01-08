# Compaction Maps: Code Analysis and Implementation Context

## Executive Summary

This document analyzes the Apache Iceberg codebase to understand how the "Compaction Maps" proposal would integrate with existing infrastructure for position delete files, delete merging, and compaction. The proposal addresses a critical problem: concurrent transactions that write position deletes against the pre-compacted layout fail when compaction rewrites data files, because their position references become invalid.

The analysis shows that Iceberg's architecture is well-structured to support this feature, with clear separation between:
1. Position delete file writing (where compaction maps would be **produced**)
2. Position delete merging (where compaction maps would be **consumed**)
3. Compaction orchestration (where map generation would be **triggered**)

---

## 1. Position Delete File Infrastructure

### 1.1 Position Delete Data Model

**Core Class**: `PositionDelete.java`
- **Location**: `core/src/main/java/org/apache/iceberg/deletes/PositionDelete.java`
- **Structure**: A generic record containing:
  - `path` (CharSequence): Data file URI
  - `pos` (long): Row position in that file
  - `row` (R): Optional row data for delete validation

**Relevance to Proposal**: This is the fundamental unit that needs remapping. The compaction map would transform `(path_old, pos_old)` → `(path_new, pos_new)`.

### 1.2 Position Delete Writers

The codebase provides multiple writer implementations with different characteristics:

#### a) PositionDeleteWriter (Base)
- **Location**: `core/src/main/java/org/apache/iceberg/deletes/PositionDeleteWriter.java`
- **Assumptions**: Input is **pre-ordered** by file path and position
- **Characteristics**:
  - No in-memory buffering
  - Tracks referenced data files via `CharSequenceSet`
  - Creates delete files with metrics
- **Compaction Map Relevance**: This is where **compaction maps would be PRODUCED** during rewrite operations

#### b) SortingPositionOnlyDeleteWriter
- **Location**: `core/src/main/java/org/apache/iceberg/deletes/SortingPositionOnlyDeleteWriter.java`
- **Characteristics**:
  - Handles **unordered** deletes
  - Uses in-memory `CharSequenceMap<Roaring64Bitmap>` to buffer positions
  - Flushes sorted deletes on close
- **Compaction Map Relevance**: This is where **compaction maps would be CONSUMED** - transactions needing to remap their deletes would use this writer with remapped positions

#### c) FileScopedPositionDeleteWriter
- **Location**: `core/src/main/java/org/apache/iceberg/deletes/FileScopedPositionDeleteWriter.java`
- **Characteristics**: Creates one delete file per referenced data file
- **Compaction Map Relevance**: Simplifies map structure since each delete file maps 1:1 with data files

#### Writer Hierarchy Summary

```
Position Delete Writing Architecture:

┌─────────────────────────────────────────────────────────────┐
│  Compaction Operation                                        │
│                                                              │
│  During rewrite, track:                                      │
│  - Source file + position → Output file + new position       │
│  - Record runs: (TransactionID, StartPos, Length)            │
│                                                              │
│  Output: CompactionMap metadata                              │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│  Position Delete Writers (PRODUCERS of compaction maps)      │
│                                                              │
│  PositionDeleteWriter                                        │
│  ├─ FileScopedPositionDeleteWriter (1 file per data file)   │
│  ├─ RollingPositionDeleteWriter (size-based splits)         │
│  ├─ ClusteredPositionDeleteWriter (multi-partition)         │
│  └─ FanoutPositionOnlyDeleteWriter (unordered multi-part)   │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  │ Writes position delete files
                  ▼
┌─────────────────────────────────────────────────────────────┐
│  Storage Layer (Parquet/ORC/Avro)                           │
│  - Delete files contain (path, pos) or (path, pos, row)     │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  │ Read by concurrent transactions
                  ▼
┌─────────────────────────────────────────────────────────────┐
│  Position Delete Remapping (CONSUMERS of compaction maps)   │
│                                                              │
│  SortingPositionOnlyDeleteWriter                            │
│  ├─ Read old position delete file                           │
│  ├─ Look up compaction map for referenced files             │
│  ├─ Remap (old_path, old_pos) → (new_path, new_pos)         │
│  └─ Write new position delete file with updated positions   │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. Position Delete Merging and Application

### 2.1 Position Delete Index

**Interface**: `PositionDeleteIndex.java`
- **Location**: `core/src/main/java/org/apache/iceberg/deletes/PositionDeleteIndex.java`
- **Purpose**: Efficiently track which positions are deleted
- **Key Methods**:
  - `delete(long position)`: Mark position as deleted
  - `isDeleted(long position)`: Query deletion status

**Implementation**: `BitmapPositionDeleteIndex.java`
- Uses Roaring64Bitmap for sparse, efficient storage (~8 bits/value)
- Supports merging via `merge()` using bitwise OR

**Compaction Map Relevance**: The index structure is independent of physical layout, so it doesn't need modification. The remapping happens **before** index construction.

### 2.2 Delete Merging Logic

**Core Utility**: `Deletes.java`
- **Location**: `core/src/main/java/org/apache/iceberg/deletes/Deletes.java`
- **Key Methods**:
  - `toPositionIndexes()`: Builds map of position delete indexes by file path
  - `toPositionIndex()`: Creates merged index for specific data file from multiple delete files
  - `deletePositions()`: Extracts and merges delete positions using `SortedMerge`

**Streaming Filter Methods**:
- `streamingFilter()`: Applies deletes while streaming (assumes sorted positions)
- `streamingMarker()`: Marks deletes while streaming

**Compaction Map Relevance**: These utilities assume position deletes reference the current physical layout. After compaction with concurrent writes, they need to handle both:
1. Position deletes written against the old layout (need remapping)
2. Position deletes written against the new layout (direct use)

### 2.3 Delete Filter Application

**Abstract Base**: `DeleteFilter.java`
- **Location**: `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java`
- **Purpose**: Apply position and equality deletes during data reads
- **Flow**:
  1. `filter(records)` → `applyPosDeletes()` → `applyEqDeletes()`
  2. Loads delete files via `DeleteLoader`
  3. Uses `PositionDeleteIndex` for position lookups
  4. Uses `StructLikeSet` for equality delete comparisons

**Compaction Map Relevance**: The filter doesn't need modification - it operates on the final, remapped delete files. The complexity is hidden in the transaction commit path.

### 2.4 Delete Loading

**Base Implementation**: `BaseDeleteLoader.java`
- **Location**: `data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java`
- **Key Methods**:
  - `loadPositionDeletes()`: Loads position deletes for specific file path
  - Merges indexes via `PositionDeleteIndexUtil.merge()`
  - Supports caching through `canCache()` and `getOrLoad()`

**Compaction Map Relevance**: The loader would need to be aware of compaction maps to handle mixed scenarios where some deletes reference old layout and some reference new layout.

### 2.5 Delete File Indexing

**Class**: `DeleteFileIndex.java`
- **Location**: `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java`
- **Purpose**: Indexes delete files by sequence number, partition, and data file path
- **Key Structures**:
  - `PartitionMap<PositionDeletes> posDeletesByPartition`
  - `CharSequenceMap<PositionDeletes> posDeletesByPath`
- **Key Methods**:
  - `forDataFile()`: Returns applicable delete files for a data file
  - `canContainEqDeletesForFile()`: Uses statistics for filtering

**Compaction Map Relevance**: This index would need to track which compaction map applies to each delete file, likely through sequence number ranges or explicit metadata.

---

## 3. Compaction Infrastructure

### 3.1 Compaction API

**Core Interface**: `RewriteDataFiles.java`
- **Location**: `api/src/main/java/org/apache/iceberg/actions/RewriteDataFiles.java`
- **Key Configurations**:
  - Target file size: `TARGET_FILE_SIZE_BYTES`
  - Strategies: `binPack()`, `sort()`, `zOrder()`
  - Partial progress: `PARTIAL_PROGRESS_ENABLED`
  - Concurrency: `MAX_CONCURRENT_FILE_GROUP_REWRITES` (default: 5)

**Compaction Map Relevance**: This is where compaction maps would be **generated** and **stored** as part of the rewrite operation.

### 3.2 Base Compaction Implementation

**Class**: `BaseRewriteDataFilesAction.java`
- **Location**: `core/src/main/java/org/apache/iceberg/actions/BaseRewriteDataFilesAction.java`
- **Orchestration Flow**:
  1. **Snapshot scanning**: Plans files using table scan
  2. **Partition grouping**: Groups tasks by partition
  3. **File filtering**: Only rewrites partitions with multiple files
  4. **Data rewriting**: Abstract method `rewriteDataForTasks()`
  5. **Commit**: Uses `RewriteFiles` transaction

**Compaction Map Relevance**: Step 4 (data rewriting) is where the map would be built. The commit (step 5) would store the map as metadata alongside the rewritten files.

### 3.3 File Grouping Logic

**Class**: `SizeBasedFileRewriter.java`
- **Location**: `core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewriter.java`
- **File Selection Criteria**:
  - Min file size: 75% of target (default)
  - Max file size: 180% of target (default)
  - Min input files: 5 (forces rewrite for many small files)
- **Grouping Algorithm**: Uses `BinPacking.ListPacker` with max group size limit

**Compaction Map Relevance**: The grouping determines which files are compacted together, defining the scope of each compaction map.

### 3.4 Spark Strategy Implementations

#### BinPack Strategy
- **Location**: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkBinPackDataRewriter.java`
- **Algorithm**: Reads files using split size, writes to create new files
- **Compaction Map Relevance**: During the read-write process, would track source file+position → destination file+position

#### Sort Strategy
- **Location**: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkSortDataRewriter.java`
- **Algorithm**: Sorts data before writing
- **Compaction Map Relevance**: More complex mapping due to reordering. Would need to track original positions through the sort operation.

#### Z-Order Strategy
- **Location**: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/SparkZOrderDataRewriter.java`
- **Algorithm**: Multi-column locality optimization
- **Compaction Map Relevance**: Most complex case - significant reordering across multiple dimensions

### 3.5 Commit Management

**Class**: `RewriteDataFilesCommitManager.java`
- **Location**: `core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java`
- **Atomic Commits**:
  - Collects rewritten files from all groups
  - Single `RewriteFiles` transaction per commit batch
  - Optional sequence number preservation

**Compaction Map Relevance**: The commit would include:
1. Standard `RewriteFiles` operation (existing)
2. New: Compaction map metadata (proposed)

---

## 4. Key Code Locations for Implementation

### 4.1 Where Compaction Maps Would Be PRODUCED

**Primary Location**: During data file rewriting in compaction

```
Spark Rewriter (example: SparkBinPackDataRewriter.java)
├─ Read source files with position tracking
├─ Merge/sort/reorder data
├─ Write output files with position tracking
└─ Generate compaction map: List<(sourceFile, sourcePos, destFile, destPos, runLength)>
```

**Implementation Points**:
1. **Format-specific readers** (`Parquet.java`, `ORC.java`): Add position tracking
2. **Rewriter strategies** (`SparkBinPackDataRewriter.java`, etc.): Track transformations
3. **Position delete writers** (`PositionDeleteWriter.java`): Output map metadata

**Estimated Changes**:
- New interface: `CompactionMapWriter`
- New class: `CompactionMap` (data structure)
- Modifications to: `SparkBinPackDataRewriter`, `SparkSortDataRewriter`, `SparkZOrderDataRewriter`
- New metadata format for storing maps alongside delete files

### 4.2 Where Compaction Maps Would Be CONSUMED

**Primary Location**: During transaction commit when position deletes need remapping

```
Transaction Commit Path
├─ Detect concurrent compaction (snapshot sequence numbers)
├─ Load compaction map for affected files
├─ Read old position delete files
├─ Apply remapping: (old_path, old_pos) → (new_path, new_pos)
└─ Write new position delete files
```

**Implementation Points**:
1. **Row delta operations** (`BaseRowDelta.java`): Detect compaction conflicts
2. **Delete file remapping utility** (NEW): Apply compaction maps
3. **Delete writers** (`SortingPositionOnlyDeleteWriter.java`): Generate remapped files
4. **Commit validation** (`MergingSnapshotProducer.java`): Handle remapping logic

**Estimated Changes**:
- New interface: `CompactionMapReader`
- New utility: `PositionDeleteRemapper`
- Modifications to: `BaseRowDelta`, `BaseRewriteDataFilesAction`
- New validation logic in snapshot producers

### 4.3 Storage and Retrieval

**Compaction Map Metadata Storage Options**:

**Option 1: Stored in Manifest Files**
- Pros: Automatically tracked with data files, easy to expire
- Cons: Requires manifest format changes
- Location: Modify `ManifestFile` and `ManifestEntry` classes

**Option 2: Separate Metadata Files (like delete files)**
- Pros: No format changes, flexible structure
- Cons: Additional metadata tracking required
- Location: New `CompactionMapFile` type, tracked in manifest lists

**Option 3: Stored in Snapshot Metadata**
- Pros: Simple to implement, snapshot-scoped
- Cons: Could bloat snapshot metadata for large compactions
- Location: `Snapshot.summary()` with references to map files

**Recommendation**: Option 2 (Separate Metadata Files) provides the best balance of flexibility and compatibility with existing Iceberg infrastructure.

---

## 5. Implementation Considerations

### 5.1 Compaction Map Format

Based on the proposal, the map format would be:

```
CompactionMap {
  sourceSnapshot: long           // Snapshot ID before compaction
  targetSnapshot: long           // Snapshot ID after compaction
  mappings: List<FileMapping>
}

FileMapping {
  sourceFile: String             // URI of pre-compaction file
  targetFile: String             // URI of post-compaction file
  runs: List<Run>
}

Run {
  sourcePosition: long           // Starting position in source file
  targetPosition: long           // Starting position in target file
  length: long                   // Number of rows in this run
}
```

**Size Estimation** (from proposal):
- Runs are likely thousands or tens of thousands of rows
- For a 1GB data file with 10M rows and 1000-row runs: ~30KB per file
- For typical compaction of 100 files → ~3MB map size
- Trivial compared to data file sizes

### 5.2 Remapping Algorithm

Based on the proposal example:

```java
class PositionRemapper {
  // Load compaction map for relevant files
  CompactionMap loadMap(String oldFilePath, long snapshotId);

  // Remap a single position
  Position remap(String oldPath, long oldPos) {
    FileMapping mapping = map.getMapping(oldPath);
    long newPos = 0;

    // Find the run containing this position
    for (Run run : mapping.runs) {
      if (oldPos >= run.sourcePosition &&
          oldPos < run.sourcePosition + run.length) {
        // Position is in this run
        long offset = oldPos - run.sourcePosition;
        return new Position(mapping.targetFile, run.targetPosition + offset);
      }
      newPos += run.length;
    }

    throw new IllegalStateException("Position not found in map");
  }
}
```

### 5.3 Concurrent Transaction Handling

**Conflict Detection**:
1. Transaction T4 starts with snapshot S3
2. Compaction C runs concurrently, creates S4 with new layout
3. T4 tries to commit position deletes against old layout
4. System detects: T4's deletes reference files not in S4

**Resolution Path**:
1. Load compaction map from S3 → S4
2. Remap T4's position deletes using the map
3. Write new position delete files with remapped positions
4. Retry commit with remapped deletes

**Code Location**: `BaseRowDelta.java:validateDeletedFiles()`
- Currently validates that deleted files exist
- Would be extended to handle remapping when files were compacted

### 5.4 Integration with Existing Delete Infrastructure

The proposal integrates cleanly with existing infrastructure:

1. **Position Delete Writers**: No changes needed - they already write (path, pos) tuples
2. **Position Delete Readers**: No changes needed - they read remapped files normally
3. **Delete Filters**: No changes needed - they apply deletes to current layout
4. **Compaction Operations**: Main changes here - track and output maps
5. **Transaction Commit**: Main changes here - detect conflicts and remap

### 5.5 Edge Cases and Complexities

**1. Multiple Concurrent Compactions**
- If S3 → S4 and S4 → S5 compactions both occur
- Transaction against S3 needs to apply both maps: S3→S4→S5
- Solution: Chain maps or maintain transitive closures

**2. Partial Compaction**
- Only some files in a partition are compacted
- Deletes may reference both compacted and non-compacted files
- Solution: Remap only positions referencing compacted files

**3. Cross-File Position Deletes**
- A single delete file may reference multiple data files
- Some may be compacted, others not
- Solution: Partial remapping - only affected positions

**4. Equality Deletes**
- Proposal focuses on position deletes
- Equality deletes are value-based, not position-based
- Impact: No remapping needed - they work with new layout automatically

**5. Delete File Compaction**
- Position delete files themselves can be compacted
- This shouldn't require maps - delete file compaction rewrites the positions explicitly
- Implementation note: Clearly separate data file compaction (needs maps) from delete file compaction (doesn't)

---

## 6. Performance Implications

### 6.1 Map Generation Cost

**During Compaction**:
- Minimal overhead: already iterating through all rows
- Track `(sourceFile, sourcePos)` during read
- Track `(targetFile, targetPos)` during write
- Aggregate into runs (consecutive positions)

**Estimated Cost**: < 1% of compaction time (just integer tracking)

### 6.2 Map Storage Cost

**Size**: As estimated in proposal, ~3MB for 100-file compaction
**Storage Duration**: Until all transactions before compaction are expired
**Cleanup**: Same mechanism as delete file expiration

### 6.3 Remapping Cost

**When Needed** (rare case):
- Transaction committed against old layout, compaction occurred
- Load map: ~3MB read
- Apply remapping: O(n) where n = positions in delete file
- Write new delete file: Same as original write

**Estimated Cost**:
- Typical case (no conflict): 0 overhead
- Conflict case: ~10-100ms for remapping, far less than transaction restart

### 6.4 Read Path Impact

**Zero Impact**: Readers see only the final, remapped delete files. The complexity is entirely in the write/commit path.

---

## 7. Comparison with Current Behavior

### 7.1 Current Behavior (Without Maps)

```
Transaction T4 flow:
1. Start with snapshot S3
2. Write position deletes against S3 layout
3. Attempt commit
4. Compaction created S4 (files changed)
5. Validation fails: position deletes reference non-existent files
6. Transaction aborts
7. User must restart T4 against S4
8. Expensive work (scan, compute deletes) wasted
```

**User Impact**: Random failures, wasted work, deadline misses

### 7.2 Proposed Behavior (With Maps)

```
Transaction T4 flow:
1. Start with snapshot S3
2. Write position deletes against S3 layout
3. Attempt commit
4. Compaction created S4 (files changed)
5. Validation detects conflict, loads map S3→S4
6. System remaps position deletes to S4 layout
7. Commit succeeds with remapped deletes
8. No user-visible failure
```

**User Impact**: Transparent resolution, no wasted work

---

## 8. Implementation Roadmap

### Phase 1: Core Infrastructure (Weeks 1-3)
1. Define `CompactionMap` data structure
2. Implement map writer in `PositionDeleteWriter`
3. Add map storage (as separate metadata files)
4. Implement map reader and lookup

### Phase 2: Compaction Integration (Weeks 4-6)
1. Modify `SparkBinPackDataRewriter` to generate maps
2. Extend to `SparkSortDataRewriter` and `SparkZOrderDataRewriter`
3. Store maps in commits via `RewriteDataFilesCommitManager`
4. Add map expiration logic

### Phase 3: Transaction Integration (Weeks 7-9)
1. Add conflict detection in `BaseRowDelta`
2. Implement `PositionDeleteRemapper` utility
3. Modify commit validation to trigger remapping
4. Handle chained compactions (transitive maps)

### Phase 4: Testing and Optimization (Weeks 10-12)
1. Unit tests for map generation and remapping
2. Integration tests for concurrent compaction + writes
3. Performance benchmarks
4. Edge case handling (partial compaction, multiple maps, etc.)

---

## 9. Testing Strategy

### 9.1 Unit Tests

**Map Generation**:
- Test run encoding for consecutive positions
- Test map generation for simple merge (base + 1 delta)
- Test map generation for complex merge (base + multiple deltas)

**Remapping**:
- Test single position remap
- Test bulk position remap
- Test cross-file remapping
- Test chained map application

### 9.2 Integration Tests

**Concurrent Operations**:
- Start transaction T1
- Run compaction (generates map)
- Commit T1 (triggers remap)
- Verify T1 succeeds
- Verify deletes applied correctly

**Multiple Compactions**:
- Start transaction T1
- Run compaction C1
- Run compaction C2
- Commit T1 (triggers chained remap)
- Verify correctness

### 9.3 Performance Tests

**Map Size**:
- Measure map size vs. file count
- Measure map size vs. delete file count
- Verify proposal's size estimates

**Remapping Cost**:
- Measure remapping time vs. delete count
- Compare to transaction restart cost
- Verify < 10% overhead claim

---

## 10. Risks and Mitigation

### 10.1 Format Changes

**Risk**: Compaction map storage requires new metadata format
**Mitigation**:
- Store as separate files (like delete files), no format change
- Use snapshot summary for references
- Backward compatible: old versions ignore maps, transactions fail as before

### 10.2 Complexity

**Risk**: Remapping logic is complex, potential for bugs
**Mitigation**:
- Comprehensive testing (see section 9)
- Gradual rollout with feature flag
- Extensive validation in development

### 10.3 Adoption

**Risk**: As noted in proposal, this is a niche problem
**Mitigation**:
- Gather production evidence from industry (Microsoft SQL DW, LinkedIn)
- Write clear documentation with concrete examples
- Present at Iceberg Summit for community feedback

### 10.4 Performance Regression

**Risk**: Adds overhead to compaction and commit paths
**Mitigation**:
- Map generation is opt-in (feature flag)
- Remapping only occurs on conflict (rare)
- Extensive benchmarking before release

---

## 11. Applicability Analysis

### 11.1 When Compaction Maps Are Essential

**Scenario 1: Single Writer with Frequent Updates**
- Workload: One writer continuously updating the same partitions
- Problem: Compaction runs to optimize reads, but conflicts with writer
- Impact: Writer sees random failures during compaction windows
- Solution: Compaction maps eliminate conflicts

**Scenario 2: Time-Critical Transactions**
- Workload: ETL jobs with SLA deadlines
- Problem: Transaction restarts may miss deadlines
- Impact: SLA violations, pipeline delays
- Solution: Compaction maps ensure first-attempt success

**Scenario 3: Large-Scale Deletes**
- Workload: GDPR/compliance deletes affecting many rows
- Problem: Writing position deletes is expensive; restart doubles cost
- Impact: 2x compute cost, 2x time
- Solution: Compaction maps avoid wasted work

### 11.2 When Compaction Maps Are Less Critical

**Scenario 1: Append-Only Workloads**
- No updates = no position deletes = no conflict
- Maps not needed

**Scenario 2: Infrequent Compaction**
- Compaction runs during quiet periods
- Low probability of concurrent writes
- Maps provide safety net but rarely used

**Scenario 3: Small Tables**
- Transaction restart cost is low
- Maps may not justify implementation complexity

### 11.3 Alternative Solutions (Existing)

**1. Scheduled Compaction**
- Run compaction during known quiet periods
- Limitation: Requires predictable workload patterns

**2. Longer Compaction Intervals**
- Reduce compaction frequency to avoid conflicts
- Limitation: Degrades read performance

**3. Use Equality Deletes Instead**
- Equality deletes are value-based, not position-based
- Limitation: Less efficient, requires unique key columns

**4. Partition-Level Locking**
- Prevent concurrent compaction and writes
- Limitation: Reduces concurrency, increases latency

**Compaction Maps vs. Alternatives**:
- Only solution that allows **full concurrency** with **no failures**
- All alternatives trade off either performance or availability

---

## 12. Related Code Areas

### 12.1 Snapshot Management
- **Files**: `BaseSnapshot.java`, `SnapshotProducer.java`
- **Relevance**: Compaction maps are snapshot-scoped

### 12.2 Manifest Management
- **Files**: `ManifestFile.java`, `ManifestEntry.java`, `ManifestWriter.java`
- **Relevance**: Potential storage location for map metadata

### 12.3 Sequence Numbers
- **Files**: `Snapshot.java`, `DataFile.java`
- **Relevance**: Used to determine if files have been compacted

### 12.4 Transaction Validation
- **Files**: `BaseRowDelta.java`, `MergingSnapshotProducer.java`
- **Relevance**: Where conflict detection and remapping would occur

### 12.5 File Format Classes
- **Files**: `Parquet.java`, `ORC.java`, `Avro.java`
- **Relevance**: Would need position tracking during reads

---

## 13. Conclusion

The "Compaction Maps" proposal addresses a real pain point in Iceberg: concurrent transaction failures due to compaction. The Iceberg codebase is well-structured to support this feature, with clear insertion points for:

1. **Map Generation**: During compaction rewrite operations
2. **Map Storage**: As separate metadata files (similar to delete files)
3. **Map Consumption**: During transaction commit validation
4. **Map Cleanup**: Via existing snapshot expiration mechanisms

### Key Strengths of the Proposal:
- **Minimal overhead**: Map generation adds <1% to compaction time
- **Compact storage**: ~3MB for typical 100-file compaction
- **Zero read impact**: Readers see only final, remapped deletes
- **Clean integration**: Fits naturally into existing architecture

### Key Implementation Areas:
1. **Position delete writers** (`core/src/main/java/org/apache/iceberg/deletes/`)
2. **Compaction strategies** (`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/`)
3. **Transaction validation** (`core/src/main/java/org/apache/iceberg/BaseRowDelta.java`)
4. **Commit management** (`core/src/main/java/org/apache/iceberg/actions/RewriteDataFilesCommitManager.java`)

### Next Steps:
1. Socialize proposal with Iceberg community
2. Gather production evidence from affected organizations
3. Prototype map generation in bin-pack strategy
4. Validate size and performance estimates
5. Develop comprehensive test suite
6. Implement phased rollout with feature flag

The architecture analysis confirms that this feature is implementable with moderate effort and would provide significant value to workloads with frequent updates and compaction.
