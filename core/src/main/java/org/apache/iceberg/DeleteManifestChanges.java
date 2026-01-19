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
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * Tracks changes to delete manifests during compaction conflict resolution.
 *
 * <p>When compaction resolves conflicts with position delete transactions, it must:
 *
 * <ul>
 *   <li>Add new delete files with remapped positions
 *   <li>Track original delete files that were remapped (for potential cleanup)
 *   <li>Record metrics about the resolution
 * </ul>
 *
 * <p>This class captures all those changes for use during the commit process.
 */
public class DeleteManifestChanges {
  private final List<DeleteFile> addedDeleteFiles;
  private final List<DeleteFile> remappedDeleteFiles;
  private final int totalDeletesRemapped;
  private final int affectedDataFiles;

  private DeleteManifestChanges(
      List<DeleteFile> addedDeleteFiles,
      List<DeleteFile> remappedDeleteFiles,
      int totalDeletesRemapped,
      int affectedDataFiles) {
    this.addedDeleteFiles = addedDeleteFiles;
    this.remappedDeleteFiles = remappedDeleteFiles;
    this.totalDeletesRemapped = totalDeletesRemapped;
    this.affectedDataFiles = affectedDataFiles;
  }

  /**
   * Returns the new delete files created with remapped positions.
   *
   * <p>These files should be added to the commit.
   */
  public List<DeleteFile> addedDeleteFiles() {
    return addedDeleteFiles;
  }

  /**
   * Returns the original delete files that were remapped.
   *
   * <p>These files contain positions that reference the old (compacted) data files. They are not
   * automatically deleted, but can be used for tracking or cleanup.
   */
  public List<DeleteFile> remappedDeleteFiles() {
    return remappedDeleteFiles;
  }

  /** Returns the total number of delete records that were remapped. */
  public int totalDeletesRemapped() {
    return totalDeletesRemapped;
  }

  /** Returns the number of data files that had their deletes remapped. */
  public int affectedDataFiles() {
    return affectedDataFiles;
  }

  /** Returns true if any changes were made (i.e., some deletes were remapped). */
  public boolean hasChanges() {
    return !addedDeleteFiles.isEmpty();
  }

  /** Returns an empty changes instance (no remapping needed). */
  public static DeleteManifestChanges empty() {
    return new DeleteManifestChanges(ImmutableList.of(), ImmutableList.of(), 0, 0);
  }

  /** Creates a new builder for DeleteManifestChanges. */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("addedDeleteFiles", addedDeleteFiles.size())
        .add("remappedDeleteFiles", remappedDeleteFiles.size())
        .add("totalDeletesRemapped", totalDeletesRemapped)
        .add("affectedDataFiles", affectedDataFiles)
        .toString();
  }

  /** Builder for creating DeleteManifestChanges instances. */
  public static class Builder {
    private final ImmutableList.Builder<DeleteFile> addedDeleteFiles = ImmutableList.builder();
    private final ImmutableList.Builder<DeleteFile> remappedDeleteFiles = ImmutableList.builder();
    private int totalDeletesRemapped = 0;
    private int affectedDataFiles = 0;

    private Builder() {}

    /** Adds a new delete file that was created with remapped positions. */
    public Builder addNewDeleteFile(DeleteFile deleteFile) {
      addedDeleteFiles.add(deleteFile);
      return this;
    }

    /** Adds all new delete files that were created with remapped positions. */
    public Builder addNewDeleteFiles(Iterable<DeleteFile> deleteFiles) {
      addedDeleteFiles.addAll(deleteFiles);
      return this;
    }

    /** Adds an original delete file that was remapped (for tracking). */
    public Builder addRemappedDeleteFile(DeleteFile deleteFile) {
      remappedDeleteFiles.add(deleteFile);
      return this;
    }

    /** Adds all original delete files that were remapped (for tracking). */
    public Builder addRemappedDeleteFiles(Iterable<DeleteFile> deleteFiles) {
      remappedDeleteFiles.addAll(deleteFiles);
      return this;
    }

    /** Sets the total number of delete records that were remapped. */
    public Builder totalDeletesRemapped(int count) {
      this.totalDeletesRemapped = count;
      return this;
    }

    /** Adds to the total number of delete records that were remapped. */
    public Builder addDeletesRemapped(int count) {
      this.totalDeletesRemapped += count;
      return this;
    }

    /** Sets the number of data files that had their deletes remapped. */
    public Builder affectedDataFiles(int count) {
      this.affectedDataFiles = count;
      return this;
    }

    /** Builds the DeleteManifestChanges instance. */
    public DeleteManifestChanges build() {
      return new DeleteManifestChanges(
          addedDeleteFiles.build(),
          remappedDeleteFiles.build(),
          totalDeletesRemapped,
          affectedDataFiles);
    }
  }
}
