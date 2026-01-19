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
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/**
 * Information about delete conflicts detected during compaction.
 *
 * <p>When compaction conflicts with position delete transactions, this class captures the details:
 *
 * <ul>
 *   <li>Which data files have conflicting deletes
 *   <li>Which delete files contain those deletes
 *   <li>Which snapshots added those delete files
 *   <li>Total count of delete files and affected data files
 * </ul>
 *
 * <p>This information can be used to:
 *
 * <ul>
 *   <li>Decide whether to attempt delete remapping vs aborting
 *   <li>Load the correct delete files for remapping
 *   <li>Track metrics about conflict resolution
 * </ul>
 */
public class DeleteConflictInfo {
  private final Set<String> affectedDataFiles;
  private final List<DeleteFile> conflictingDeleteFiles;
  private final Map<Long, List<DeleteFile>> deleteFilesBySnapshot;
  private final Map<String, List<DeleteFile>> deleteFilesByDataFile;

  private DeleteConflictInfo(
      Set<String> affectedDataFiles,
      List<DeleteFile> conflictingDeleteFiles,
      Map<Long, List<DeleteFile>> deleteFilesBySnapshot,
      Map<String, List<DeleteFile>> deleteFilesByDataFile) {
    this.affectedDataFiles = affectedDataFiles;
    this.conflictingDeleteFiles = conflictingDeleteFiles;
    this.deleteFilesBySnapshot = deleteFilesBySnapshot;
    this.deleteFilesByDataFile = deleteFilesByDataFile;
  }

  /** Returns the set of data files that have conflicting deletes. */
  public Set<String> affectedDataFiles() {
    return affectedDataFiles;
  }

  /** Returns all delete files that conflict with the compaction. */
  public List<DeleteFile> conflictingDeleteFiles() {
    return conflictingDeleteFiles;
  }

  /** Returns delete files grouped by the snapshot that added them. */
  public Map<Long, List<DeleteFile>> deleteFilesBySnapshot() {
    return deleteFilesBySnapshot;
  }

  /** Returns delete files grouped by the data file they reference. */
  public Map<String, List<DeleteFile>> deleteFilesByDataFile() {
    return deleteFilesByDataFile;
  }

  /** Returns the number of conflicting delete files. */
  public int deleteFileCount() {
    return conflictingDeleteFiles.size();
  }

  /** Returns the number of affected data files. */
  public int affectedDataFileCount() {
    return affectedDataFiles.size();
  }

  /** Returns true if there are any conflicts. */
  public boolean hasConflicts() {
    return !conflictingDeleteFiles.isEmpty();
  }

  /** Returns an empty conflict info (no conflicts). */
  public static DeleteConflictInfo empty() {
    return new DeleteConflictInfo(
        ImmutableSet.of(), ImmutableList.of(), ImmutableMap.of(), ImmutableMap.of());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("affectedDataFiles", affectedDataFiles.size())
        .add("conflictingDeleteFiles", conflictingDeleteFiles.size())
        .add("snapshotsWithDeletes", deleteFilesBySnapshot.size())
        .toString();
  }

  /** Builder for creating DeleteConflictInfo instances. */
  public static class Builder {
    private final ImmutableSet.Builder<String> affectedDataFiles = ImmutableSet.builder();
    private final ImmutableList.Builder<DeleteFile> conflictingDeleteFiles =
        ImmutableList.builder();
    private final ImmutableMap.Builder<Long, List<DeleteFile>> deleteFilesBySnapshot =
        ImmutableMap.builder();
    private final java.util.Map<String, List<DeleteFile>> deleteFilesByDataFile =
        org.apache.iceberg.relocated.com.google.common.collect.Maps.newHashMap();

    /**
     * Adds a conflicting delete file.
     *
     * @param deleteFile the conflicting delete file
     * @param snapshotId the snapshot that added this delete file
     * @param referencedDataFile the data file this delete references
     */
    public Builder addConflict(DeleteFile deleteFile, long snapshotId, String referencedDataFile) {
      affectedDataFiles.add(referencedDataFile);
      conflictingDeleteFiles.add(deleteFile);
      deleteFilesByDataFile
          .computeIfAbsent(
              referencedDataFile,
              k -> org.apache.iceberg.relocated.com.google.common.collect.Lists.newArrayList())
          .add(deleteFile);
      return this;
    }

    /**
     * Adds all delete files from a snapshot.
     *
     * @param snapshotId the snapshot ID
     * @param deleteFiles the delete files from that snapshot
     */
    public Builder addSnapshotDeletes(long snapshotId, List<DeleteFile> deleteFiles) {
      if (!deleteFiles.isEmpty()) {
        deleteFilesBySnapshot.put(snapshotId, ImmutableList.copyOf(deleteFiles));
      }
      return this;
    }

    /** Builds the DeleteConflictInfo. */
    public DeleteConflictInfo build() {
      ImmutableMap.Builder<String, List<DeleteFile>> byDataFile = ImmutableMap.builder();
      for (java.util.Map.Entry<String, List<DeleteFile>> entry : deleteFilesByDataFile.entrySet()) {
        byDataFile.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
      }

      return new DeleteConflictInfo(
          affectedDataFiles.build(),
          conflictingDeleteFiles.build(),
          deleteFilesBySnapshot.build(),
          byDataFile.build());
    }
  }
}
