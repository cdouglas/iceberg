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
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.SnapshotUtil;

/**
 * Detects conflicts between compaction operations and concurrent position delete transactions.
 *
 * <p>When a compaction operation is about to commit, it may discover that other transactions have
 * added position deletes referencing the files being compacted. This detector identifies such
 * conflicts by:
 *
 * <ol>
 *   <li>Scanning snapshots between the compaction's starting snapshot and the current snapshot
 *   <li>Finding delete files that reference the files being compacted
 *   <li>Collecting metadata about the conflicts for potential resolution
 * </ol>
 *
 * <p>This is the reverse of {@link CompactionMapValidator} - while that class validates from the
 * delete transaction's perspective (checking if deletes reference compacted files), this class
 * detects conflicts from the compaction's perspective (finding deletes that reference files being
 * compacted).
 *
 * <p>Example usage:
 *
 * <pre>
 * Set&lt;String&gt; filesToCompact = getFilesBeingCompacted();
 * CompactionConflictDetector detector = new CompactionConflictDetector(
 *     table.io(), base, startingSnapshotId, currentSnapshot);
 *
 * DeleteConflictInfo conflicts = detector.detectConflicts(filesToCompact);
 * if (conflicts.hasConflicts()) {
 *   // Either abort or attempt to resolve via delete remapping
 *   List&lt;DeleteFile&gt; deleteFilesToRemap = conflicts.conflictingDeleteFiles();
 * }
 * </pre>
 */
public class CompactionConflictDetector {
  private final FileIO io;
  private final TableMetadata base;
  private final long startingSnapshotId;
  private final Snapshot currentSnapshot;

  /**
   * Creates a detector for finding delete conflicts with compaction.
   *
   * @param io the file IO for reading manifests
   * @param base the table metadata
   * @param startingSnapshotId the snapshot ID when the compaction started
   * @param currentSnapshot the current snapshot to check for conflicts against
   */
  public CompactionConflictDetector(
      FileIO io, TableMetadata base, long startingSnapshotId, Snapshot currentSnapshot) {
    Preconditions.checkNotNull(io, "io is null");
    Preconditions.checkNotNull(base, "base is null");
    Preconditions.checkNotNull(currentSnapshot, "currentSnapshot is null");
    this.io = io;
    this.base = base;
    this.startingSnapshotId = startingSnapshotId;
    this.currentSnapshot = currentSnapshot;
  }

  /**
   * Detects delete files that reference the files being compacted.
   *
   * <p>This method scans all snapshots between the starting snapshot and the current snapshot,
   * looking for delete files that reference any of the files being compacted.
   *
   * <p>The result includes:
   *
   * <ul>
   *   <li>File-scoped position deletes that definitely conflict (reference compacted files)
   *   <li>Multi-file position deletes that may conflict (need content-based resolution)
   * </ul>
   *
   * @param filesToCompact the set of data file paths being compacted
   * @return conflict information including all conflicting delete files and affected data files
   */
  public DeleteConflictInfo detectConflicts(Set<String> filesToCompact) {
    Preconditions.checkNotNull(filesToCompact, "filesToCompact is null");

    if (filesToCompact.isEmpty()) {
      return DeleteConflictInfo.empty();
    }

    // Check if the starting snapshot is the same as current (no intervening snapshots)
    if (currentSnapshot.snapshotId() == startingSnapshotId) {
      return DeleteConflictInfo.empty();
    }

    DeleteConflictInfo.Builder builder = new DeleteConflictInfo.Builder();

    // Iterate through snapshots between starting and current
    Iterable<Snapshot> snapshots =
        SnapshotUtil.ancestorsBetween(
            currentSnapshot.snapshotId(), startingSnapshotId, base::snapshot);

    for (Snapshot snapshot : snapshots) {
      SnapshotConflicts snapshotConflicts =
          findConflictingDeletesInSnapshot(snapshot, filesToCompact);

      // Process file-scoped conflicts
      if (!snapshotConflicts.fileScopedConflicts.isEmpty()) {
        List<DeleteFile> deleteFiles = Lists.newArrayList();
        for (DeleteFileWithReference conflict : snapshotConflicts.fileScopedConflicts) {
          builder.addConflict(
              conflict.deleteFile, snapshot.snapshotId(), conflict.referencedDataFile);
          deleteFiles.add(conflict.deleteFile);
        }
        builder.addSnapshotDeletes(snapshot.snapshotId(), deleteFiles);
      }

      // Process multi-file position deletes (need content-based resolution)
      for (DeleteFile multiFileDelete : snapshotConflicts.multiFilePositionDeletes) {
        builder.addMultiFilePositionDelete(multiFileDelete);
      }
    }

    return builder.build();
  }

  /** Internal class to track delete file with its referenced data file. */
  private static class DeleteFileWithReference {
    final DeleteFile deleteFile;
    final String referencedDataFile;

    DeleteFileWithReference(DeleteFile deleteFile, String referencedDataFile) {
      this.deleteFile = deleteFile;
      this.referencedDataFile = referencedDataFile;
    }
  }

  /** Internal class to track snapshot-level conflicts. */
  private static class SnapshotConflicts {
    final List<DeleteFileWithReference> fileScopedConflicts;
    final List<DeleteFile> multiFilePositionDeletes;

    SnapshotConflicts(
        List<DeleteFileWithReference> fileScopedConflicts,
        List<DeleteFile> multiFilePositionDeletes) {
      this.fileScopedConflicts = fileScopedConflicts;
      this.multiFilePositionDeletes = multiFilePositionDeletes;
    }
  }

  /**
   * Finds delete files in a snapshot that reference any of the files being compacted.
   *
   * @param snapshot the snapshot to check
   * @param filesToCompact the set of data file paths being compacted
   * @return conflicts found in the snapshot (both file-scoped and multi-file)
   */
  private SnapshotConflicts findConflictingDeletesInSnapshot(
      Snapshot snapshot, Set<String> filesToCompact) {
    List<DeleteFileWithReference> fileScopedConflicts = Lists.newArrayList();
    List<DeleteFile> multiFileDeletes = Lists.newArrayList();

    // Get delete manifests added in this snapshot
    List<ManifestFile> deleteManifests = snapshot.deleteManifests(io);

    for (ManifestFile manifestFile : deleteManifests) {
      // Only check manifests added by this snapshot
      if (manifestFile.snapshotId() != null && manifestFile.snapshotId() == snapshot.snapshotId()) {
        ManifestConflicts manifestConflicts = findConflictsInManifest(manifestFile, filesToCompact);
        fileScopedConflicts.addAll(manifestConflicts.fileScopedConflicts);
        multiFileDeletes.addAll(manifestConflicts.multiFilePositionDeletes);
      }
    }

    return new SnapshotConflicts(fileScopedConflicts, multiFileDeletes);
  }

  /** Internal class to track delete file with multi-file flag. */
  private static class ManifestConflicts {
    final List<DeleteFileWithReference> fileScopedConflicts;
    final List<DeleteFile> multiFilePositionDeletes;

    ManifestConflicts(
        List<DeleteFileWithReference> fileScopedConflicts,
        List<DeleteFile> multiFilePositionDeletes) {
      this.fileScopedConflicts = fileScopedConflicts;
      this.multiFilePositionDeletes = multiFilePositionDeletes;
    }
  }

  /**
   * Finds delete files in a manifest that reference any of the files being compacted.
   *
   * <p>This method identifies two types of potential conflicts:
   *
   * <ul>
   *   <li>File-scoped position deletes that definitely reference compacted files
   *   <li>Multi-file position deletes that may reference compacted files (need content-based
   *       resolution)
   * </ul>
   *
   * @param manifestFile the manifest to check
   * @param filesToCompact the set of data file paths being compacted
   * @return conflicts found in the manifest (both file-scoped and multi-file)
   */
  private ManifestConflicts findConflictsInManifest(
      ManifestFile manifestFile, Set<String> filesToCompact) {
    List<DeleteFileWithReference> fileScopedConflicts = Lists.newArrayList();
    List<DeleteFile> multiFileDeletes = Lists.newArrayList();

    try (ManifestReader<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(manifestFile, io, null)) {

      for (ManifestEntry<DeleteFile> entry : reader.entries()) {
        // Only check ADDED entries (not EXISTING or DELETED)
        if (entry.status() == ManifestEntry.Status.ADDED) {
          // ManifestReader.entries() reuses the same ManifestEntry instance (and therefore the
          // same DeleteFile instance) across iterations to avoid per-row allocations. Copy
          // before retaining so the elements we add to fileScopedConflicts / multiFileDeletes
          // don't all collapse to the last-read file.
          DeleteFile deleteFile = entry.file().copy(false);
          String referencedFile = getReferencedDataFile(deleteFile);

          if (referencedFile != null) {
            // File-scoped position delete - check if it references a compacted file
            if (filesToCompact.contains(referencedFile)) {
              fileScopedConflicts.add(new DeleteFileWithReference(deleteFile, referencedFile));
            }
          } else if (isMultiFilePositionDelete(deleteFile)) {
            // Multi-file position delete - may reference compacted files
            // Must be resolved by reading content
            multiFileDeletes.add(deleteFile);
          }
          // Equality deletes (content == EQUALITY_DELETES) are intentionally ignored
          // They are not file-scoped and don't conflict with compaction in the same way
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to read delete manifest: " + manifestFile.path(), e);
    }

    return new ManifestConflicts(fileScopedConflicts, multiFileDeletes);
  }

  /**
   * Gets the data file path referenced by a delete file.
   *
   * <p>This handles:
   *
   * <ul>
   *   <li>Position delete files with explicit referencedDataFile set
   *   <li>Deletion vectors (DVs), which always have referencedDataFile set
   *   <li>Position delete files with bounds on the file_path column (single file)
   * </ul>
   *
   * <p>Returns null for:
   *
   * <ul>
   *   <li>Equality delete files (not file-scoped by definition)
   *   <li>Multi-file position deletes (bounds differ, cannot determine statically)
   * </ul>
   *
   * <p>Callers should use {@link #isMultiFilePositionDelete(DeleteFile)} to distinguish between
   * these two null cases.
   *
   * @param deleteFile the delete file to check
   * @return the referenced data file path, or null if not file-scoped
   */
  private String getReferencedDataFile(DeleteFile deleteFile) {
    // Use ContentFileUtil for robust referenced file extraction
    return ContentFileUtil.referencedDataFileLocation(deleteFile);
  }

  /**
   * Checks if a delete file is a multi-file position delete.
   *
   * <p>Multi-file position deletes are position delete files created with {@link
   * org.apache.iceberg.deletes.DeleteGranularity#PARTITION} that contain deletes for multiple data
   * files. These cannot be statically analyzed to determine which data files they reference.
   *
   * @param deleteFile the delete file to check
   * @return true if this is a multi-file position delete
   */
  private boolean isMultiFilePositionDelete(DeleteFile deleteFile) {
    // Multi-file position delete: content is POSITION_DELETES but no single referenced file
    return deleteFile.content() == FileContent.POSITION_DELETES
        && ContentFileUtil.referencedDataFileLocation(deleteFile) == null;
  }

  /**
   * Checks if any deletes exist that reference the files being compacted.
   *
   * <p>This is a quick check that returns true as soon as any conflict is found, without collecting
   * all the conflict details.
   *
   * <p>This includes both:
   *
   * <ul>
   *   <li>File-scoped position deletes that definitely reference compacted files
   *   <li>Multi-file position deletes that may reference compacted files
   * </ul>
   *
   * @param filesToCompact the set of data file paths being compacted
   * @return true if any conflicts exist or may exist
   */
  public boolean hasConflicts(Set<String> filesToCompact) {
    Preconditions.checkNotNull(filesToCompact, "filesToCompact is null");

    if (filesToCompact.isEmpty()) {
      return false;
    }

    if (currentSnapshot.snapshotId() == startingSnapshotId) {
      return false;
    }

    // Iterate through snapshots between starting and current
    Iterable<Snapshot> snapshots =
        SnapshotUtil.ancestorsBetween(
            currentSnapshot.snapshotId(), startingSnapshotId, base::snapshot);

    for (Snapshot snapshot : snapshots) {
      if (hasConflictsInSnapshot(snapshot, filesToCompact)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Quick check for conflicts in a single snapshot.
   *
   * @param snapshot the snapshot to check
   * @param filesToCompact the set of data file paths being compacted
   * @return true if any conflicts found
   */
  private boolean hasConflictsInSnapshot(Snapshot snapshot, Set<String> filesToCompact) {
    List<ManifestFile> deleteManifests = snapshot.deleteManifests(io);

    for (ManifestFile manifestFile : deleteManifests) {
      if (manifestFile.snapshotId() != null && manifestFile.snapshotId() == snapshot.snapshotId()) {
        if (hasConflictsInManifest(manifestFile, filesToCompact)) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Quick check for conflicts in a single manifest.
   *
   * <p>Returns true if the manifest contains:
   *
   * <ul>
   *   <li>File-scoped position deletes that reference compacted files, OR
   *   <li>Multi-file position deletes (may reference compacted files, need content check)
   * </ul>
   *
   * @param manifestFile the manifest to check
   * @param filesToCompact the set of data file paths being compacted
   * @return true if any conflicts found or potential conflicts exist
   */
  private boolean hasConflictsInManifest(ManifestFile manifestFile, Set<String> filesToCompact) {
    try (ManifestReader<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(manifestFile, io, null)) {

      for (ManifestEntry<DeleteFile> entry : reader.entries()) {
        if (entry.status() == ManifestEntry.Status.ADDED) {
          DeleteFile deleteFile = entry.file();
          String referencedFile = getReferencedDataFile(deleteFile);

          if (referencedFile != null) {
            // File-scoped position delete - check if it references a compacted file
            if (filesToCompact.contains(referencedFile)) {
              return true;
            }
          } else if (isMultiFilePositionDelete(deleteFile)) {
            // Multi-file position delete - may reference compacted files
            return true;
          }
          // Equality deletes are intentionally ignored
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to read delete manifest: " + manifestFile.path(), e);
    }

    return false;
  }

  /**
   * Gets the set of data files that have conflicting deletes.
   *
   * <p>This is a convenience method that returns just the affected data file paths without full
   * conflict details.
   *
   * @param filesToCompact the set of data file paths being compacted
   * @return set of data file paths that have conflicting deletes
   */
  public Set<String> getAffectedFiles(Set<String> filesToCompact) {
    DeleteConflictInfo conflicts = detectConflicts(filesToCompact);
    return conflicts.affectedDataFiles();
  }
}
