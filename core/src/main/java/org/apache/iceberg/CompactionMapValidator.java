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
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * Validator for detecting conflicts between position deletes and compacted data files.
 *
 * <p>When a transaction adds position deletes that reference data files, and those data files have
 * been compacted by another concurrent transaction, a conflict exists. This validator detects such
 * conflicts by:
 *
 * <ol>
 *   <li>Identifying manifests with compaction maps in the snapshot history
 *   <li>Loading the compaction maps to determine which files were compacted
 *   <li>Checking if any position deletes in the transaction reference those compacted files
 * </ol>
 *
 * <p>This validator is intended to be used in the transaction validation phase, typically in a
 * {@link SnapshotProducer#validate} override.
 */
class CompactionMapValidator {
  private final FileIO io;
  private final TableMetadata base;
  private final long startingSnapshotId;
  private final Snapshot currentSnapshot;

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
   * @param deleteFiles the delete files to check
   * @throws ValidationException if any delete files reference compacted data files
   */
  void validateNoCompactedReferences(List<DeleteFile> deleteFiles) {
    if (deleteFiles.isEmpty()) {
      return;
    }

    // Find all compaction maps in the snapshot history since the transaction started
    Set<String> compactedFiles = findCompactedFiles();

    if (compactedFiles.isEmpty()) {
      return; // No compactions occurred
    }

    // Check if any delete files reference compacted files
    Set<String> conflicts = findConflicts(deleteFiles, compactedFiles);

    ValidationException.check(
        conflicts.isEmpty(),
        "Cannot commit position deletes: referenced data files were compacted: %s. "
            + "Use compaction maps to remap position deletes before retrying.",
        conflicts);
  }

  /**
   * Finds all data files that were compacted in snapshots since the transaction started.
   *
   * @return set of source file paths that were compacted
   */
  private Set<String> findCompactedFiles() {
    Set<String> compacted = Sets.newHashSet();

    // Traverse snapshot history from current back to starting snapshot
    Snapshot snapshot = currentSnapshot;
    while (snapshot != null && snapshot.snapshotId() != startingSnapshotId) {
      // Check each manifest for compaction maps
      for (ManifestFile manifest : snapshot.dataManifests(io)) {
        if (manifest.compactionMapLocation() != null) {
          // Load compaction map and extract source file paths
          CompactionMap map =
              CompactionMaps.read(io.newInputFile(manifest.compactionMapLocation()));
          for (CompactionMap.FileMapping mapping : map.fileMappings()) {
            compacted.add(mapping.sourceFile());
          }
        }
      }

      // Move to parent snapshot
      Long parentId = snapshot.parentId();
      snapshot = parentId != null ? base.snapshot(parentId) : null;
    }

    return compacted;
  }

  /**
   * Finds conflicts between delete files and compacted files.
   *
   * @param deleteFiles the delete files to check
   * @param compactedFiles the set of compacted file paths
   * @return set of file paths that are both referenced in deletes and were compacted
   */
  private Set<String> findConflicts(List<DeleteFile> deleteFiles, Set<String> compactedFiles) {
    Set<String> conflicts = Sets.newHashSet();

    for (DeleteFile deleteFile : deleteFiles) {
      // Check if this delete file has a single referenced data file
      if (deleteFile.referencedDataFile() != null) {
        String referencedFile = deleteFile.referencedDataFile();
        if (compactedFiles.contains(referencedFile)) {
          conflicts.add(referencedFile);
        }
      }
      // For delete files that may reference multiple files,
      // we would need to read the file content to check.
      // This is a trade-off: we detect obvious conflicts efficiently,
      // but may miss some conflicts that require reading delete file content.
    }

    return conflicts;
  }
}
