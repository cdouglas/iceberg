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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.io.DeleteManifestReader;
import org.apache.iceberg.io.RemappedDeleteWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves conflicts between compaction operations and concurrent position delete transactions.
 *
 * <p>When a compaction operation conflicts with position deletes that reference the files being
 * compacted, this resolver can remap those deletes to reference the new compacted files instead.
 * This allows the compaction to complete without losing the delete information.
 *
 * <p>The resolution flow is:
 *
 * <ol>
 *   <li>Read position deletes from conflicting delete files
 *   <li>Filter deletes to only those referencing compacted files
 *   <li>Remap delete positions using the compaction map
 *   <li>Write new delete files with remapped positions
 *   <li>Return changes for the commit
 * </ol>
 *
 * <p>Example usage:
 *
 * <pre>
 * // After detecting conflicts
 * DeleteConflictInfo conflicts = detector.detectConflicts(filesToCompact);
 *
 * if (conflicts.hasConflicts()) {
 *   CompactionConflictResolver resolver = new CompactionConflictResolver(table);
 *   DeleteManifestChanges changes = resolver.resolve(compactionMap, conflicts);
 *
 *   // Add changes to commit
 *   for (DeleteFile deleteFile : changes.addedDeleteFiles()) {
 *     rowDelta.addDeletes(deleteFile);
 *   }
 * }
 * </pre>
 */
public class CompactionConflictResolver {
  private static final Logger LOG = LoggerFactory.getLogger(CompactionConflictResolver.class);

  private final Table table;
  private final DeleteManifestReader deleteReader;

  /**
   * Creates a resolver for compaction conflicts.
   *
   * @param table the table being compacted
   */
  public CompactionConflictResolver(Table table) {
    Preconditions.checkNotNull(table, "table is null");
    this.table = table;
    this.deleteReader = new DeleteManifestReader(table.io());
  }

  /**
   * Resolves conflicts by remapping position deletes to reference compacted files.
   *
   * @param compactionMap the compaction map describing file transformations
   * @param conflicts the detected conflicts to resolve
   * @return changes to apply during commit (new delete files, metrics)
   * @throws IOException if reading or writing delete files fails
   */
  public DeleteManifestChanges resolve(CompactionMap compactionMap, DeleteConflictInfo conflicts)
      throws IOException {
    Preconditions.checkNotNull(compactionMap, "compactionMap is null");
    Preconditions.checkNotNull(conflicts, "conflicts is null");

    if (!conflicts.hasConflicts()) {
      LOG.debug("No conflicts to resolve");
      return DeleteManifestChanges.empty();
    }

    LOG.info(
        "Resolving {} delete file conflicts affecting {} data files",
        conflicts.deleteFileCount(),
        conflicts.affectedDataFileCount());

    // Step 1: Collect source files from compaction map
    Set<String> compactedSourceFiles = getCompactedSourceFiles(compactionMap);

    // Step 2: Read position deletes from conflicting delete files
    List<PositionDeleteRecord> allDeletes = readConflictingDeletes(conflicts, compactedSourceFiles);

    if (allDeletes.isEmpty()) {
      LOG.info("No position deletes found referencing compacted files after filtering");
      return DeleteManifestChanges.empty();
    }

    LOG.info("Read {} position deletes referencing compacted files", allDeletes.size());

    // Step 3: Remap deletes using compaction map (with metrics)
    DeleteManifestRemapper remapper = new DeleteManifestRemapper(compactionMap);
    RemappingResult remappingResult = remapper.remapDeletesWithMetrics(allDeletes);
    Map<String, List<PositionDeleteRecord>> remappedDeletes = remappingResult.remappedDeletes();

    int totalRemapped = remappingResult.totalRemapped();
    LOG.info("Remapped {} deletes to {} target files", totalRemapped, remappedDeletes.size());

    // Log skipped deletes if any
    if (remappingResult.hasSkippedDeletes()) {
      LOG.info(
          "Skipped deletes during remapping: notCompacted={}, filteredRows={}, invalidPositions={}, duplicates={}",
          remappingResult.skippedNotCompacted(),
          remappingResult.skippedFilteredRows(),
          remappingResult.skippedInvalidPositions(),
          remappingResult.duplicatesRemoved());
    }

    if (remappedDeletes.isEmpty()) {
      LOG.info("All deletes were filtered out during remapping (rows were filtered in compaction)");
      return DeleteManifestChanges.builder()
          .addRemappedDeleteFiles(conflicts.conflictingDeleteFiles())
          .totalDeletesRemapped(0)
          .affectedDataFiles(conflicts.affectedDataFileCount())
          .build();
    }

    // Step 4: Write new delete files
    List<DeleteFile> newDeleteFiles;
    try (RemappedDeleteWriter writer = new RemappedDeleteWriter(table)) {
      newDeleteFiles = writer.writeDeletes(remappedDeletes);
    }

    LOG.info("Wrote {} new delete files with remapped positions", newDeleteFiles.size());

    // Step 5: Build and return changes
    return DeleteManifestChanges.builder()
        .addNewDeleteFiles(newDeleteFiles)
        .addRemappedDeleteFiles(conflicts.conflictingDeleteFiles())
        .totalDeletesRemapped(totalRemapped)
        .affectedDataFiles(conflicts.affectedDataFileCount())
        .build();
  }

  /**
   * Extracts source file paths from the compaction map.
   *
   * @param compactionMap the compaction map
   * @return set of source file paths that were compacted
   */
  private Set<String> getCompactedSourceFiles(CompactionMap compactionMap) {
    Set<String> sourceFiles = Sets.newHashSet();
    for (CompactionMap.FileMapping mapping : compactionMap.fileMappings()) {
      sourceFiles.add(mapping.sourceFile());
    }
    return sourceFiles;
  }

  /**
   * Reads position deletes from conflicting delete files.
   *
   * @param conflicts the detected conflicts
   * @param compactedSourceFiles the source files that were compacted
   * @return list of position delete records referencing compacted files
   */
  private List<PositionDeleteRecord> readConflictingDeletes(
      DeleteConflictInfo conflicts, Set<String> compactedSourceFiles) {
    List<PositionDeleteRecord> allDeletes = Lists.newArrayList();

    for (DeleteFile deleteFile : conflicts.conflictingDeleteFiles()) {
      try {
        // Read all position deletes from this file
        List<PositionDeleteRecord> deletes = deleteReader.readPositionDeletes(deleteFile);

        // Filter to only deletes referencing compacted files
        List<PositionDeleteRecord> filtered =
            DeleteManifestReader.filterByReferencedFiles(deletes, compactedSourceFiles);

        allDeletes.addAll(filtered);
      } catch (Exception e) {
        LOG.warn("Failed to read delete file {}", deleteFile.location(), e);
        throw new RuntimeException("Failed to read delete file: " + deleteFile.location(), e);
      }
    }

    return allDeletes;
  }

  /**
   * Resolves conflicts for a specific set of files being compacted.
   *
   * <p>This is a convenience method that combines conflict detection and resolution.
   *
   * @param compactionMap the compaction map describing file transformations
   * @param metadata the table metadata
   * @param startingSnapshotId the snapshot when compaction started
   * @param currentSnapshot the current snapshot to check against
   * @return changes to apply during commit
   * @throws IOException if reading or writing fails
   */
  public DeleteManifestChanges resolveForCompaction(
      CompactionMap compactionMap,
      TableMetadata metadata,
      long startingSnapshotId,
      Snapshot currentSnapshot)
      throws IOException {
    Preconditions.checkNotNull(compactionMap, "compactionMap is null");
    Preconditions.checkNotNull(metadata, "metadata is null");
    Preconditions.checkNotNull(currentSnapshot, "currentSnapshot is null");

    // Get files being compacted
    Set<String> filesToCompact = getCompactedSourceFiles(compactionMap);

    // Detect conflicts
    CompactionConflictDetector detector =
        new CompactionConflictDetector(table.io(), metadata, startingSnapshotId, currentSnapshot);
    DeleteConflictInfo conflicts = detector.detectConflicts(filesToCompact);

    // Resolve conflicts
    return resolve(compactionMap, conflicts);
  }
}
