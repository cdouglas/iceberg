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

import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.util.DataFileSet;

public class BaseRewriteFiles extends MergingSnapshotProducer<RewriteFiles>
    implements RewriteFiles {
  private final DataFileSet replacedDataFiles = DataFileSet.create();
  private final DataFileSet addedDataFiles = DataFileSet.create();
  private Long startingSnapshotId = null;
  private String compactionMapLocation = null;

  BaseRewriteFiles(String tableName, TableOperations ops) {
    super(tableName, ops);

    // replace files must fail if any of the deleted paths is missing and cannot be deleted
    failMissingDeletePaths();
  }

  @Override
  protected RewriteFiles self() {
    return this;
  }

  @Override
  protected String operation() {
    return DataOperations.REPLACE;
  }

  @Override
  public RewriteFiles deleteFile(DataFile dataFile) {
    replacedDataFiles.add(dataFile);
    delete(dataFile);
    return self();
  }

  @Override
  public RewriteFiles deleteFile(DeleteFile deleteFile) {
    delete(deleteFile);
    return self();
  }

  @Override
  public RewriteFiles addFile(DataFile dataFile) {
    addedDataFiles.add(dataFile);
    add(dataFile);
    return self();
  }

  @Override
  public RewriteFiles addFile(DeleteFile deleteFile) {
    add(deleteFile);
    return self();
  }

  @Override
  public RewriteFiles addFile(DeleteFile deleteFile, long dataSequenceNumber) {
    add(deleteFile, dataSequenceNumber);
    return self();
  }

  @Override
  public RewriteFiles dataSequenceNumber(long sequenceNumber) {
    setNewDataFilesDataSequenceNumber(sequenceNumber);
    return self();
  }

  @Override
  public RewriteFiles rewriteFiles(
      Set<DataFile> filesToDelete, Set<DataFile> filesToAdd, long sequenceNumber) {
    setNewDataFilesDataSequenceNumber(sequenceNumber);
    return rewriteFiles(filesToDelete, ImmutableSet.of(), filesToAdd, ImmutableSet.of());
  }

  @Override
  public RewriteFiles rewriteFiles(
      Set<DataFile> dataFilesToReplace,
      Set<DeleteFile> deleteFilesToReplace,
      Set<DataFile> dataFilesToAdd,
      Set<DeleteFile> deleteFilesToAdd) {

    Preconditions.checkNotNull(dataFilesToReplace, "Replaced data files can't be null");
    Preconditions.checkNotNull(deleteFilesToReplace, "Replaced delete files can't be null");
    Preconditions.checkNotNull(dataFilesToAdd, "Added data files can't be null");
    Preconditions.checkNotNull(deleteFilesToAdd, "Added delete files can't be null");

    for (DataFile dataFile : dataFilesToReplace) {
      deleteFile(dataFile);
    }

    for (DeleteFile deleteFile : deleteFilesToReplace) {
      deleteFile(deleteFile);
    }

    for (DataFile dataFile : dataFilesToAdd) {
      addFile(dataFile);
    }

    for (DeleteFile deleteFile : deleteFilesToAdd) {
      addFile(deleteFile);
    }

    return this;
  }

  @Override
  public RewriteFiles validateFromSnapshot(long snapshotId) {
    this.startingSnapshotId = snapshotId;
    return this;
  }

  @Override
  public BaseRewriteFiles toBranch(String branch) {
    targetBranch(branch);
    return this;
  }

  /**
   * Sets the location of the compaction map for this rewrite operation.
   *
   * <p>When data files are rewritten during compaction, a compaction map tracks the position
   * transformations from source to target files. This method allows compaction operations to
   * associate the map location with the rewrite, enabling position delete remapping.
   *
   * <p>The compaction map location will be attached to the manifest file(s) containing the added
   * data files, allowing transactions with position deletes to detect compactions and remap their
   * deletes appropriately.
   *
   * @param location the location of the compaction map file, or null to clear
   * @return this for method chaining
   */
  public BaseRewriteFiles setCompactionMapLocation(String location) {
    this.compactionMapLocation = location;
    return this;
  }

  /**
   * Returns the compaction map location for this rewrite operation, or null if not set.
   *
   * @return the compaction map location
   */
  public String compactionMapLocation() {
    return compactionMapLocation;
  }

  @Override
  protected ManifestWriter<DataFile> newManifestWriter(PartitionSpec spec) {
    ManifestWriter<DataFile> writer = super.newManifestWriter(spec);

    // Set compaction map location on each manifest writer if available
    if (compactionMapLocation != null) {
      writer.setCompactionMapLocation(compactionMapLocation);
    }

    return writer;
  }

  @Override
  public java.util.List<ManifestFile> apply(TableMetadata base, Snapshot snapshot) {
    // Generate compaction map if enabled and not already set
    if (compactionMapLocation == null && shouldGenerateCompactionMap(base)) {
      generateAndWriteCompactionMap(base, snapshot);
    }

    return super.apply(base, snapshot);
  }

  private boolean shouldGenerateCompactionMap(TableMetadata base) {
    return base.properties()
        .getOrDefault(
            org.apache.iceberg.TableProperties.COMPACTION_MAP_ENABLED,
            String.valueOf(org.apache.iceberg.TableProperties.COMPACTION_MAP_ENABLED_DEFAULT))
        .equalsIgnoreCase("true");
  }

  private void generateAndWriteCompactionMap(TableMetadata base, Snapshot snapshot) {
    // Build compaction map from replaced and added files
    if (replacedDataFiles.isEmpty() || addedDataFiles.isEmpty()) {
      return; // Nothing to map
    }

    long sourceSnapshotId;
    if (startingSnapshotId != null) {
      sourceSnapshotId = startingSnapshotId;
    } else if (snapshot != null) {
      sourceSnapshotId = snapshot.snapshotId();
    } else if (base.currentSnapshot() != null) {
      sourceSnapshotId = base.currentSnapshot().snapshotId();
    } else {
      // No snapshots exist yet — use -1 as sentinel
      sourceSnapshotId = -1L;
    }
    long targetSnapshotId = snapshotId();

    CompactionMapBuilder builder = new CompactionMapBuilder(sourceSnapshotId, targetSnapshotId);

    // Simple bin-pack mapping: assume all source files map to target files sequentially
    // This is valid for bin-pack rewrites where row order is preserved
    if (addedDataFiles.size() == 1) {
      // Single target file case (most common)
      DataFile targetFile = addedDataFiles.iterator().next();
      long targetOffset = 0;

      for (DataFile sourceFile : replacedDataFiles) {
        builder
            .addFileMapping(sourceFile.path().toString(), targetFile.path().toString())
            .addRun(0L, targetOffset, sourceFile.recordCount());
        targetOffset += sourceFile.recordCount();
      }
    } else {
      // Multiple target files - more complex mapping would require position tracking
      // For now, log and skip (consistent with RewriteDataFilesCommitManager approach)
      org.slf4j.LoggerFactory.getLogger(BaseRewriteFiles.class)
          .warn(
              "Skipping compaction map for rewrite with multiple target files ({}). "
                  + "Multi-target compaction maps require position tracking during rewrite.",
              addedDataFiles.size());
      return;
    }

    CompactionMap map = builder.build();

    // Don't write empty maps
    if (map.fileMappings().isEmpty()) {
      return;
    }

    try {
      // Generate compaction map file location
      String fileName =
          String.format(
              java.util.Locale.ROOT,
              "compaction-map-%d-%s%s",
              targetSnapshotId,
              java.util.UUID.randomUUID(),
              org.apache.iceberg.FileFormat.AVRO.addExtension(""));

      org.apache.iceberg.io.OutputFile mapFile =
          ops().io().newOutputFile(ops().metadataFileLocation(fileName));

      CompactionMaps.write(map, mapFile);
      this.compactionMapLocation = mapFile.location();

      org.slf4j.LoggerFactory.getLogger(BaseRewriteFiles.class)
          .info(
              "Wrote compaction map with {} file mappings to {}",
              map.fileMappings().size(),
              mapFile.location());
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException("Failed to write compaction map", e);
    }
  }

  @Override
  protected void validate(TableMetadata base, Snapshot parent) {
    validateReplacedAndAddedFiles();
    if (!replacedDataFiles.isEmpty()) {
      // if there are replaced data files, there cannot be any new row-level deletes for those data
      // files
      validateNoNewDeletesForDataFiles(base, startingSnapshotId, replacedDataFiles, parent);
    }
  }

  private void validateReplacedAndAddedFiles() {
    Preconditions.checkArgument(
        deletesDataFiles() || deletesDeleteFiles(), "Files to delete cannot be empty");

    Preconditions.checkArgument(
        deletesDataFiles() || !addsDataFiles(),
        "Data files to add must be empty because there's no data file to be rewritten");

    Preconditions.checkArgument(
        deletesDeleteFiles() || !addsDeleteFiles(),
        "Delete files to add must be empty because there's no delete file to be rewritten");
  }
}
