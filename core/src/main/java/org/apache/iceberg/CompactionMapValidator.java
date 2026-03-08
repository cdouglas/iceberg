/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.ChainedCompactionMapsException;
import org.apache.iceberg.exceptions.CompactionConflictException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * Validator for detecting conflicts between position deletes and compacted data files.
 *
 * <p>When a transaction adds position deletes (either traditional position delete files or deletion
 * vectors) that reference data files, and those data files have been compacted by another
 * concurrent transaction, a conflict exists. This validator detects such conflicts by:
 *
 * <ol>
 *   <li>Identifying manifests with compaction maps in the snapshot history
 *   <li>Loading the compaction maps to determine which files were compacted
 *   <li>Checking if any position deletes in the transaction reference those compacted files
 *   <li>Detecting chains when multiple sequential compactions affect the same files
 * </ol>
 *
 * <p>This validator is intended to be used in the transaction validation phase, typically in a
 * {@link SnapshotProducer#validate} override.
 *
 * <p>Both position delete files and deletion vectors (DVs) are supported. DVs always have a {@code
 * referencedDataFile} that points to the data file they apply to, making conflict detection
 * straightforward.
 *
 * <p><b>Chained Compaction Maps:</b> When multiple compactions occur sequentially (e.g., F1→F2 then
 * F2→F3), deletes referencing F1 need to be remapped through the chain: F1→F2→F3. This validator
 * detects such chains and can build a {@link CompactionMapChain} to handle composition.
 */
class CompactionMapValidator {
  private final FileIO io;
  private final TableMetadata base;
  private final long startingSnapshotId;
  private final Snapshot currentSnapshot;

  // Cached loaded compaction maps (map location -> CompactionMap)
  private Map<String, CompactionMap> loadedMaps;
  // Cached list of maps in snapshot order (oldest to newest)
  private List<CompactionMap> orderedMaps;

  /**
   * Creates a validator for checking compaction conflicts.
   *
   * @param io the file IO for reading compaction maps
   * @param base the table metadata at the start of validation
   * @param startingSnapshotId the snapshot ID when the transaction started
   * @param currentSnapshot the current snapshot to validate against
   */
  CompactionMapValidator(
      FileIO io, TableMetadata base, long startingSnapshotId, Snapshot currentSnapshot) {
    this.io = io;
    this.base = base;
    this.startingSnapshotId = startingSnapshotId;
    this.currentSnapshot = currentSnapshot;
  }

  /**
   * Checks if any of the given delete files reference data files that were compacted.
   *
   * <p>This method also detects chained compaction maps. If files were compacted through multiple
   * sequential compactions (e.g., F1→F2→F3), a {@link ChainedCompactionMapsException} is thrown
   * with the chain information needed to compose the maps.
   *
   * @param deleteFiles the delete files to check
   * @throws CompactionConflictException if any delete files reference compacted data files (single
   *     map)
   * @throws ChainedCompactionMapsException if files were compacted through multiple sequential
   *     compactions
   */
  void validateNoCompactedReferences(List<DeleteFile> deleteFiles) {
    if (deleteFiles.isEmpty()) {
      return;
    }

    // Load all compaction maps in the snapshot history
    loadCompactionMaps();

    if (loadedMaps.isEmpty()) {
      return; // No compactions occurred
    }

    // Build index from source file to compaction map
    Map<String, String> sourceFileToMapLocation = buildSourceFileIndex();

    // Check if any delete files reference compacted files
    Set<String> conflicts = findConflicts(deleteFiles, sourceFileToMapLocation.keySet());

    if (conflicts.isEmpty()) {
      return; // No conflicts
    }

    // Check for chains that affect conflicting files
    ChainInfo chainInfo = detectChains(conflicts);

    if (chainInfo.hasChains()) {
      throw new ChainedCompactionMapsException(
          chainInfo.chainedFiles, chainInfo.chainSnapshotIds, chainInfo.chainMaps);
    }

    // Single-level conflicts - throw regular exception
    Map<String, String> conflictLocations = Maps.newHashMap();
    for (String conflictFile : conflicts) {
      conflictLocations.put(conflictFile, sourceFileToMapLocation.get(conflictFile));
    }

    throw new CompactionConflictException(
        String.format(
            "Cannot commit deletes: referenced data files were compacted: %s. "
                + "Use compaction maps to remap deletes before retrying.",
            conflicts),
        conflicts,
        conflictLocations);
  }

  /**
   * Finds all compaction maps in the snapshot history and extracts compacted file mappings.
   *
   * <p>This method traverses the snapshot history from the current snapshot back to the starting
   * snapshot, collecting all compaction maps. It returns a map from source file paths (that were
   * compacted) to the locations of their compaction maps.
   *
   * @return map from compacted file path to compaction map location
   */
  Map<String, String> findCompactionMaps() {
    loadCompactionMaps();
    return buildSourceFileIndex();
  }

  /**
   * Builds a compaction map chain for resolving conflicts with chained compactions.
   *
   * <p>This method loads all compaction maps and builds a chain that can compose mappings for files
   * that were compacted through multiple sequential operations.
   *
   * @return a CompactionMapChain, or null if no compaction maps exist
   */
  CompactionMapChain findCompactionMapChain() {
    loadCompactionMaps();

    if (orderedMaps.isEmpty()) {
      return null;
    }

    return CompactionMapChain.build(orderedMaps);
  }

  /**
   * Returns the list of loaded compaction maps in snapshot order (oldest to newest).
   *
   * @return list of compaction maps
   */
  List<CompactionMap> getOrderedMaps() {
    loadCompactionMaps();
    return Lists.newArrayList(orderedMaps);
  }

  /**
   * Loads all compaction maps from the snapshot history.
   *
   * <p>Maps are cached after first load.
   */
  private void loadCompactionMaps() {
    if (loadedMaps != null) {
      return; // Already loaded
    }

    loadedMaps = Maps.newHashMap();
    List<CompactionMap> mapsInSnapshotOrder = Lists.newArrayList();

    // Traverse snapshot history from current back to starting snapshot
    // Collect in reverse order (newest first), then reverse
    Snapshot snapshot = currentSnapshot;
    while (snapshot != null && snapshot.snapshotId() != startingSnapshotId) {
      // Check each manifest for compaction maps
      for (ManifestFile manifest : snapshot.dataManifests(io)) {
        String mapLocation = manifest.compactionMapLocation();
        if (mapLocation != null && !loadedMaps.containsKey(mapLocation)) {
          CompactionMap map = CompactionMaps.read(io.newInputFile(mapLocation));
          loadedMaps.put(mapLocation, map);
          mapsInSnapshotOrder.add(map);
        }
      }

      // Move to parent snapshot
      Long parentId = snapshot.parentId();
      snapshot = parentId != null ? base.snapshot(parentId) : null;
    }

    // Reverse to get oldest-to-newest order
    this.orderedMaps = Lists.reverse(mapsInSnapshotOrder);
  }

  /**
   * Builds an index from source file paths to compaction map locations.
   *
   * @return map from source file path to compaction map location
   */
  private Map<String, String> buildSourceFileIndex() {
    Map<String, String> index = Maps.newHashMap();

    for (Map.Entry<String, CompactionMap> entry : loadedMaps.entrySet()) {
      String mapLocation = entry.getKey();
      CompactionMap map = entry.getValue();

      for (CompactionMap.FileMapping mapping : map.fileMappings()) {
        index.put(mapping.sourceFile(), mapLocation);
      }
    }

    return index;
  }

  /**
   * Detects chains in the compaction maps that affect the given conflict files.
   *
   * <p>A chain exists when a file is compacted in one map (e.g., F1→F2) and the target file is then
   * compacted in a subsequent map (e.g., F2→F3).
   *
   * @param conflictFiles the files that are in conflict
   * @return chain information
   */
  private ChainInfo detectChains(Set<String> conflictFiles) {
    // Build index: target file -> map that produces it
    Map<String, CompactionMap> targetToMap = Maps.newHashMap();
    for (CompactionMap map : orderedMaps) {
      for (CompactionMap.FileMapping mapping : map.fileMappings()) {
        // Handle per-run targets for multi-target mappings
        for (CompactionMap.Run run : mapping.runs()) {
          String target = run.targetFile() != null ? run.targetFile() : mapping.targetFile();
          targetToMap.put(target, map);
        }
        // Also index the mapping-level target (for backward compat)
        targetToMap.put(mapping.targetFile(), map);
      }
    }

    // Build index: source file -> map that consumes it
    Map<String, CompactionMap> sourceToMap = Maps.newHashMap();
    for (CompactionMap map : orderedMaps) {
      for (CompactionMap.FileMapping mapping : map.fileMappings()) {
        sourceToMap.put(mapping.sourceFile(), map);
      }
    }

    // Find files that require chain composition
    Set<String> chainedFiles = Sets.newHashSet();
    Set<CompactionMap> mapsInChain = Sets.newLinkedHashSet();
    Set<Long> snapshotsInChain = Sets.newLinkedHashSet();

    for (String conflictFile : conflictFiles) {
      // Find the map that compacts this file
      CompactionMap firstMap = sourceToMap.get(conflictFile);
      if (firstMap == null) {
        continue;
      }

      // Get target file(s) from this map
      CompactionMap.FileMapping mapping = firstMap.mappingForFile(conflictFile);
      if (mapping == null) {
        continue;
      }

      // Check if any target is a source in a subsequent map
      Set<String> targets = getTargetFiles(mapping);
      for (String target : targets) {
        CompactionMap nextMap = sourceToMap.get(target);
        if (nextMap != null && nextMap != firstMap) {
          // Found a chain!
          chainedFiles.add(conflictFile);

          // Collect all maps in this chain
          collectChainMaps(conflictFile, mapsInChain, snapshotsInChain, sourceToMap);
        }
      }
    }

    if (chainedFiles.isEmpty()) {
      return ChainInfo.noChains();
    }

    List<CompactionMap> orderedChainMaps = Lists.newArrayList();
    for (CompactionMap map : orderedMaps) {
      if (mapsInChain.contains(map)) {
        orderedChainMaps.add(map);
      }
    }

    List<Long> orderedSnapshots = Lists.newArrayList(snapshotsInChain);

    return new ChainInfo(chainedFiles, orderedSnapshots, orderedChainMaps);
  }

  /**
   * Collects all maps in a chain starting from the given source file.
   *
   * @param sourceFile the starting source file
   * @param mapsInChain set to collect maps into
   * @param snapshotsInChain set to collect snapshot IDs into
   * @param sourceToMap index from source file to map
   */
  private void collectChainMaps(
      String sourceFile,
      Set<CompactionMap> mapsInChain,
      Set<Long> snapshotsInChain,
      Map<String, CompactionMap> sourceToMap) {

    String currentFile = sourceFile;
    while (currentFile != null) {
      CompactionMap map = sourceToMap.get(currentFile);
      if (map == null || mapsInChain.contains(map)) {
        break;
      }

      mapsInChain.add(map);
      snapshotsInChain.add(map.sourceSnapshotId());
      snapshotsInChain.add(map.targetSnapshotId());

      // Get targets and follow the chain
      CompactionMap.FileMapping mapping = map.mappingForFile(currentFile);
      if (mapping != null) {
        Set<String> targets = getTargetFiles(mapping);
        // Follow the first target that continues the chain
        currentFile = null;
        for (String target : targets) {
          if (sourceToMap.containsKey(target)) {
            currentFile = target;
            break;
          }
        }
      } else {
        currentFile = null;
      }
    }
  }

  /**
   * Gets all target files from a file mapping, including per-run targets.
   *
   * @param mapping the file mapping
   * @return set of target file paths
   */
  private Set<String> getTargetFiles(CompactionMap.FileMapping mapping) {
    Set<String> targets = Sets.newHashSet();
    targets.add(mapping.targetFile());

    for (CompactionMap.Run run : mapping.runs()) {
      if (run.targetFile() != null) {
        targets.add(run.targetFile());
      }
    }

    return targets;
  }

  /**
   * Finds conflicts between delete files and compacted files.
   *
   * <p>This method handles two cases:
   *
   * <ul>
   *   <li><b>File-scoped position deletes</b> (referencedDataFile set): checked directly against
   *       compacted files.
   *   <li><b>Deletion vectors (DVs)</b>: always have referencedDataFile set, same code path as
   *       file-scoped.
   * </ul>
   *
   * <p><b>Known gap — multi-file position deletes silently skipped:</b> V2 position deletes with
   * {@code referencedDataFile == null} (i.e., multi-file / partition-scoped) are not checked. If
   * such a delete references rows in a file that was concurrently compacted, this method will not
   * detect the conflict. The delete commits successfully but becomes stale: at read time, entries
   * referencing the now-absent source files are silently ignored, causing <b>missed deletions</b>
   * (rows that should have been deleted remain visible).
   *
   * <p>Detecting the conflict would require reading the delete file's content to enumerate which
   * data files it references — too expensive for commit-time validation. A blanket conservative
   * approach (treat all compacted files as conflicts whenever any multi-file delete exists) was
   * tried and reverted because it breaks SERIALIZABLE isolation: the validation pipeline calls
   * this method (via {@code validateNoCompactionConflicts}) <em>before</em> the SERIALIZABLE
   * check in {@code validateCompactionAwareConflicts}, which distinguishes structural changes
   * (compaction with map — allowed) from data changes (compaction without map — rejected). The
   * conservative approach throws {@code CompactionConflictException} before that distinction can
   * be made, rejecting valid commits where a multi-file delete targets <em>non-compacted</em>
   * files in a table that also had a structural-only compaction.
   *
   * <p>Note: {@link CompactionConflictDetector} (used by {@code
   * SparkRewriteDataFilesCommitManager}) does handle multi-file deletes, but from the
   * <em>compaction's</em> perspective ("were deletes added for files I'm replacing?"), not from
   * the RowDelta's perspective ("were files my deletes reference compacted?"). It does not
   * protect against the scenario described above.
   *
   * @param deleteFiles the delete files to check
   * @param compactedFiles the set of compacted file paths
   * @return set of file paths that are both referenced in deletes and were compacted
   */
  private Set<String> findConflicts(List<DeleteFile> deleteFiles, Set<String> compactedFiles) {
    Set<String> conflicts = Sets.newHashSet();

    for (DeleteFile deleteFile : deleteFiles) {
      if (deleteFile.referencedDataFile() != null) {
        // File-scoped position delete or DV — check directly
        String referencedFile = deleteFile.referencedDataFile();
        if (compactedFiles.contains(referencedFile)) {
          conflicts.add(referencedFile);
        }
      }
      // Multi-file position deletes (referencedDataFile == null) are not checked here.
      // See Javadoc above for the full rationale and consequences.
    }

    return conflicts;
  }

  /**
   * Identifies which delete files conflict with compacted data files.
   *
   * <p>This method is useful for programmatically determining which delete files need to be
   * remapped when resolving a compaction conflict.
   *
   * @param deleteFiles the delete files to check
   * @return list of delete files that reference compacted data files
   */
  List<DeleteFile> findConflictingDeletes(List<DeleteFile> deleteFiles) {
    Map<String, String> compactionMaps = findCompactionMaps();
    if (compactionMaps.isEmpty()) {
      return java.util.Collections.emptyList();
    }

    List<DeleteFile> conflicting = Lists.newArrayList();
    for (DeleteFile deleteFile : deleteFiles) {
      if (deleteFile.referencedDataFile() != null
          && compactionMaps.containsKey(deleteFile.referencedDataFile())) {
        conflicting.add(deleteFile);
      }
    }

    return java.util.Collections.unmodifiableList(conflicting);
  }

  /** Internal class to hold chain detection results. */
  private static class ChainInfo {
    final Set<String> chainedFiles;
    final List<Long> chainSnapshotIds;
    final List<CompactionMap> chainMaps;

    ChainInfo(
        Set<String> chainedFiles, List<Long> chainSnapshotIds, List<CompactionMap> chainMaps) {
      this.chainedFiles = chainedFiles;
      this.chainSnapshotIds = chainSnapshotIds;
      this.chainMaps = chainMaps;
    }

    static ChainInfo noChains() {
      return new ChainInfo(Sets.newHashSet(), Lists.newArrayList(), Lists.newArrayList());
    }

    boolean hasChains() {
      return !chainedFiles.isEmpty();
    }
  }
}
