# Position Delete Remapping: Pseudocode

This document provides pseudocode for the position delete remapping algorithms used in compaction map conflict resolution.

## Data Structures

```
struct Run {
    sourceStart: int64      // Starting position in source file
    targetStart: int64      // Starting position in target file
    length: int64           // Number of consecutive rows
    targetFile: string      // Target file path (for multi-target mappings)
}

struct FileMapping {
    sourceFile: string      // Path to source (compacted) file
    targetFile: string      // Default target file path
    runs: List<Run>         // Sorted by sourceStart
}

struct CompactionMap {
    sourceSnapshotId: int64
    targetSnapshotId: int64
    fileMappings: Map<string, FileMapping>  // Indexed by source file path
}
```

## Algorithm 1: Single Position Lookup

```
function REMAP-POSITION(mapping: FileMapping, pos: int64) -> (string, int64)?
    // Binary search for the run containing pos
    run ← BINARY-SEARCH(mapping.runs, pos)

    if run = NULL or pos < run.sourceStart or pos >= run.sourceStart + run.length then
        return NULL  // Position not in any run (was deleted during compaction)

    offset ← pos - run.sourceStart
    targetPos ← run.targetStart + offset
    targetFile ← run.targetFile ≠ NULL ? run.targetFile : mapping.targetFile

    return (targetFile, targetPos)
```

## Algorithm 2: Bulk Remapping with Strategy Selection

```
function REMAP-BULK(mapping: FileMapping, positions: List<int64>) -> Map<int64, (string, int64)>
    n ← |positions|
    m ← |mapping.runs|
    sorted ← IS-SORTED(positions)
    gapRatio ← ESTIMATE-GAP-RATIO(mapping.runs)

    // Select optimal strategy based on workload characteristics
    // (Derived from empirical JMH benchmarks, Jan-Feb 2026)

    if not sorted then
        // Very high m with small n: StreamJoin's O(n+m) beats IntervalTree
        if m >= 5000 and n <= 2000 then
            return STREAM-JOIN-REMAP(mapping.runs, SORT(positions))
        return INTERVAL-TREE-REMAP(mapping.runs, positions)

    // Sorted data below
    if m >= 5000 and n <= 2000 then
        return STREAM-JOIN-REMAP(mapping.runs, positions)
    else if gapRatio > 0.3 then
        return RANGE-QUERY-REMAP(mapping.runs, positions)
    else if n >= 10000 and m >= 100 then
        return STREAM-JOIN-REMAP(mapping.runs, positions)
    else
        return RANGE-QUERY-REMAP(mapping.runs, positions)
```

## Algorithm 3: Stream Join (Sorted Bulk Remapping)

```
function STREAM-JOIN-REMAP(runs: List<Run>, positions: List<int64>) -> Map<int64, (string, int64)>
    // Precondition: positions is sorted, runs is sorted by sourceStart
    result ← empty map
    runIdx ← 0

    for each pos in positions do
        // Advance run pointer until we find a run that might contain pos
        while runIdx < |runs| and runs[runIdx].sourceStart + runs[runIdx].length <= pos do
            runIdx ← runIdx + 1

        if runIdx >= |runs| then
            break  // No more runs, remaining positions unmapped

        run ← runs[runIdx]
        if pos >= run.sourceStart and pos < run.sourceStart + run.length then
            offset ← pos - run.sourceStart
            targetFile ← run.targetFile ≠ NULL ? run.targetFile : defaultTarget
            result[pos] ← (targetFile, run.targetStart + offset)
        // else: pos falls in gap, not added to result

    return result
```

## Algorithm 4: Interval Tree (Unsorted Remapping)

```
function INTERVAL-TREE-REMAP(runs: List<Run>, positions: List<int64>) -> Map<int64, (string, int64)>
    // Build interval tree from runs (one-time cost: O(m log m))
    tree ← BUILD-INTERVAL-TREE(runs)
    result ← empty map

    for each pos in positions do
        run ← tree.QUERY(pos)  // O(log m)
        if run ≠ NULL then
            offset ← pos - run.sourceStart
            targetFile ← run.targetFile ≠ NULL ? run.targetFile : defaultTarget
            result[pos] ← (targetFile, run.targetStart + offset)

    return result
```

## Algorithm 5: Compaction Map Composition (Chaining)

```
function COMPOSE-MAPS(m1: CompactionMap, m2: CompactionMap) -> CompactionMap
    // Precondition: m1.targetSnapshotId = m2.sourceSnapshotId
    result ← new CompactionMap(m1.sourceSnapshotId, m2.targetSnapshotId)

    // Index m2 by source file for efficient lookup
    m2Index ← INDEX-BY-SOURCE(m2.fileMappings)

    for each (sourceFile, mapping1) in m1.fileMappings do
        for each (intermediateFile, runs1) in GROUP-BY-TARGET(mapping1) do
            mapping2 ← m2Index[intermediateFile]

            if mapping2 = NULL then
                // Intermediate file not compacted further - preserve mapping
                result.ADD-RUNS(sourceFile, intermediateFile, runs1)
            else
                // Compose through m2
                composedRuns ← COMPOSE-FILE-MAPPINGS(runs1, mapping2)
                for each (finalTarget, runs) in composedRuns do
                    result.ADD-RUNS(sourceFile, finalTarget, runs)

    return result

function COMPOSE-FILE-MAPPINGS(runs1: List<Run>, mapping2: FileMapping) -> List<(string, List<Run>)>
    result ← empty list

    for each r1 in runs1 do
        r1TargetStart ← r1.targetStart
        r1TargetEnd ← r1.targetStart + r1.length
        anyOverlap ← false

        for each r2 in mapping2.runs do
            r2SourceStart ← r2.sourceStart
            r2SourceEnd ← r2.sourceStart + r2.length

            // Compute overlap between r1's target range and r2's source range
            overlapStart ← max(r1TargetStart, r2SourceStart)
            overlapEnd ← min(r1TargetEnd, r2SourceEnd)

            if overlapStart < overlapEnd then
                anyOverlap ← true
                overlapLength ← overlapEnd - overlapStart

                // Map back to original source position
                composedSourcePos ← r1.sourceStart + (overlapStart - r1TargetStart)

                // Map forward to final target position
                composedTargetPos ← r2.targetStart + (overlapStart - r2SourceStart)

                finalTarget ← r2.targetFile ≠ NULL ? r2.targetFile : mapping2.targetFile
                result.ADD(finalTarget, Run(composedSourcePos, composedTargetPos, overlapLength))

        if not anyOverlap then
            // r1's target range not touched by m2 - preserve with intermediate as target
            result.ADD(r1.targetFile, r1)

    return result
```

## Algorithm 6: Conflict Resolution Workflow

```
function RESOLVE-COMPACTION-CONFLICT(
    deleteFile: DeleteFile,
    compactionMaps: List<CompactionMap>,
    fileIO: FileIO
) -> List<DeleteFile>

    // Build chain if multiple maps
    if |compactionMaps| > 1 then
        chain ← BUILD-CHAIN(compactionMaps)  // Validates snapshot continuity
        composedMap ← chain.COMPOSE()
    else
        composedMap ← compactionMaps[0]

    // Read original positions
    positions ← READ-POSITIONS(deleteFile, fileIO)

    // Group positions by source file
    positionsByFile ← GROUP-BY-FILE(positions)

    // Remap each file's positions
    remappedByTarget ← empty map<string, List<int64>>

    for each (sourceFile, filePositions) in positionsByFile do
        mapping ← composedMap.fileMappings[sourceFile]

        if mapping = NULL then
            // File not compacted - preserve original positions
            remappedByTarget[sourceFile].ADD-ALL(filePositions)
        else
            // Remap positions through compaction map
            remapped ← REMAP-BULK(mapping, filePositions)
            for each (pos, (targetFile, targetPos)) in remapped do
                remappedByTarget[targetFile].ADD(targetPos)
            // Positions not in remapped were deleted during compaction - skip

    // Write new delete files
    newDeleteFiles ← empty list
    for each (targetFile, targetPositions) in remappedByTarget do
        deleteFile ← WRITE-POSITION-DELETES(targetFile, targetPositions, fileIO)
        newDeleteFiles.ADD(deleteFile)

    return newDeleteFiles
```

## Complexity Analysis

| Algorithm | Time Complexity | Space Complexity | Best For |
|-----------|-----------------|------------------|----------|
| Single Lookup | O(log m) | O(1) | Individual queries |
| Stream Join | O(n + m) | O(1) | Sorted bulk, dense |
| Interval Tree | O(n log m) | O(m) | Unsorted data |
| Range Query | O(m log n) | O(n) | Sorted, sparse/small m |
| Composition | O(r₁ × r₂) | O(r₁ + r₂) | Chain of 2 maps |

Where:
- n = number of positions to remap
- m = number of runs in compaction map
- r₁, r₂ = number of runs in maps being composed

## Run-Length Encoding Efficiency

Compaction maps use run-length encoding for efficient storage:

```
Example: Bin-pack compaction of 3 files (1000 rows each) into 1 file

Without RLE: 3000 individual position mappings
With RLE:    3 runs: [(0,0,1000), (0,1000,1000), (0,2000,1000)]

Compression ratio: 1000:1 for simple concatenation
```

Gaps in runs represent positions deleted during merge compaction:

```
Source file: positions 0-999
Position deletes: rows 100-199 deleted
After compaction: 900 rows remain

Compaction map runs:
  Run 1: (0, 0, 100)      // Source [0,100) → Target [0,100)
  Run 2: (200, 100, 800)  // Source [200,1000) → Target [100,900)

Gap [100,200) represents deleted rows - remapping returns NULL
```
