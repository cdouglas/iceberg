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
package org.apache.iceberg.actions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseRewriteFiles;
import org.apache.iceberg.CompactionConflictDetector;
import org.apache.iceberg.CompactionConflictResolver;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMapBuilder;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteConflictInfo;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteManifestChanges;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Functionality used by RewriteDataFile Actions from different platforms to handle commits. */
public class RewriteDataFilesCommitManager {
  private static final Logger LOG = LoggerFactory.getLogger(RewriteDataFilesCommitManager.class);

  private final Table table;
  private final long startingSnapshotId;
  private final boolean useStartingSequenceNumber;
  private final Map<String, String> snapshotProperties;

  // constructor used for testing
  public RewriteDataFilesCommitManager(Table table) {
    this(table, table.currentSnapshot().snapshotId());
  }

  public RewriteDataFilesCommitManager(Table table, long startingSnapshotId) {
    this(table, startingSnapshotId, RewriteDataFiles.USE_STARTING_SEQUENCE_NUMBER_DEFAULT);
  }

  public RewriteDataFilesCommitManager(
      Table table, long startingSnapshotId, boolean useStartingSequenceNumber) {
    this(table, startingSnapshotId, useStartingSequenceNumber, ImmutableMap.of());
  }

  public RewriteDataFilesCommitManager(
      Table table,
      long startingSnapshotId,
      boolean useStartingSequenceNumber,
      Map<String, String> snapshotProperties) {
    this.table = table;
    this.startingSnapshotId = startingSnapshotId;
    this.useStartingSequenceNumber = useStartingSequenceNumber;
    this.snapshotProperties = snapshotProperties;
  }

  /**
   * Perform a commit operation on the table adding and removing files as required for this set of
   * file groups
   *
   * @param fileGroups fileSets to commit
   */
  public void commitFileGroups(Set<RewriteFileGroup> fileGroups) {
    DataFileSet rewrittenDataFiles = DataFileSet.create();
    DataFileSet addedDataFiles = DataFileSet.create();
    DeleteFileSet danglingDVs = DeleteFileSet.create();
    for (RewriteFileGroup group : fileGroups) {
      rewrittenDataFiles.addAll(group.rewrittenFiles());
      addedDataFiles.addAll(group.addedFiles());
      danglingDVs.addAll(group.danglingDVs());
    }

    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshotId);
    if (useStartingSequenceNumber) {
      long sequenceNumber = table.snapshot(startingSnapshotId).sequenceNumber();
      rewrite.dataSequenceNumber(sequenceNumber);
    }

    // Generate and write compaction map if enabled
    CompactionMap compactionMap = null;
    if (shouldGenerateCompactionMap()) {
      compactionMap = buildCompactionMap(fileGroups);
      String compactionMapLocation = writeCompactionMap(compactionMap);
      if (compactionMapLocation != null && rewrite instanceof BaseRewriteFiles) {
        ((BaseRewriteFiles) rewrite).setCompactionMapLocation(compactionMapLocation);
      }
    }

    // Resolve conflicting deletes if enabled
    DeleteManifestChanges conflictResolution = null;
    if (shouldResolveConflictingDeletes() && compactionMap != null) {
      conflictResolution = resolveConflictingDeletes(compactionMap, rewrittenDataFiles);
      if (conflictResolution.hasChanges()) {
        LOG.info(
            "Resolved {} conflicting position deletes affecting {} data files",
            conflictResolution.totalDeletesRemapped(),
            conflictResolution.affectedDataFiles());
        // Add remapped delete files to the rewrite operation
        for (DeleteFile deleteFile : conflictResolution.addedDeleteFiles()) {
          rewrite.addFile(deleteFile);
        }
      }
    }

    rewrittenDataFiles.forEach(rewrite::deleteFile);
    addedDataFiles.forEach(rewrite::addFile);
    danglingDVs.forEach(rewrite::deleteFile);

    snapshotProperties.forEach(rewrite::set);

    rewrite.commit();
  }

  /**
   * Clean up a specified file set by removing any files created for that operation, should not
   * throw any exceptions
   *
   * @param fileGroup group of files which has already been rewritten
   */
  public void abortFileGroup(RewriteFileGroup fileGroup) {
    Preconditions.checkState(
        fileGroup.addedFiles() != null, "Cannot abort a fileGroup that was not rewritten");

    Tasks.foreach(fileGroup.addedFiles())
        .noRetry()
        .suppressFailureWhenFinished()
        .onFailure((dataFile, exc) -> LOG.warn("Failed to delete: {}", dataFile.location(), exc))
        .run(dataFile -> table.io().deleteFile(dataFile.location()));
  }

  public void commitOrClean(Set<RewriteFileGroup> rewriteGroups) {
    try {
      commitFileGroups(rewriteGroups);
    } catch (CommitStateUnknownException e) {
      LOG.error(
          "Commit state unknown for {}, cannot clean up files because they may have been committed successfully.",
          rewriteGroups,
          e);
      throw e;
    } catch (Exception e) {
      if (e instanceof CleanableFailure) {
        LOG.error(
            "Cannot commit groups {}, attempting to clean up written files", rewriteGroups, e);
        rewriteGroups.forEach(this::abortFileGroup);
      }

      throw e;
    }
  }

  /**
   * Check if compaction maps should be generated based on table properties.
   *
   * @return true if compaction maps are enabled
   */
  private boolean shouldGenerateCompactionMap() {
    return table
        .properties()
        .getOrDefault(
            TableProperties.COMPACTION_MAP_ENABLED,
            String.valueOf(TableProperties.COMPACTION_MAP_ENABLED_DEFAULT))
        .equalsIgnoreCase("true");
  }

  /**
   * Check if conflicting deletes should be automatically remapped.
   *
   * @return true if conflict resolution is enabled
   */
  private boolean shouldResolveConflictingDeletes() {
    return table
        .properties()
        .getOrDefault(
            TableProperties.COMPACTION_REMAP_CONFLICTING_DELETES,
            String.valueOf(TableProperties.COMPACTION_REMAP_CONFLICTING_DELETES_DEFAULT))
        .equalsIgnoreCase("true");
  }

  /**
   * Get the maximum number of delete manifests to process during conflict resolution.
   *
   * @return the configured limit
   */
  private int maxRemapManifests() {
    return Integer.parseInt(
        table
            .properties()
            .getOrDefault(
                TableProperties.COMPACTION_REMAP_MAX_MANIFESTS,
                String.valueOf(TableProperties.COMPACTION_REMAP_MAX_MANIFESTS_DEFAULT)));
  }

  /**
   * Resolve conflicting position deletes that were added concurrently with this compaction.
   *
   * <p>This method detects position deletes that reference files being compacted and remaps them to
   * the new compacted files using the compaction map.
   *
   * @param compactionMap the compaction map describing file transformations
   * @param rewrittenDataFiles the data files being rewritten
   * @return changes containing remapped delete files, or empty if no conflicts
   */
  private DeleteManifestChanges resolveConflictingDeletes(
      CompactionMap compactionMap, DataFileSet rewrittenDataFiles) {
    Snapshot currentSnapshot = table.currentSnapshot();
    if (currentSnapshot == null || currentSnapshot.snapshotId() == startingSnapshotId) {
      // No changes since compaction started, no conflicts possible
      return DeleteManifestChanges.empty();
    }

    // Extract source file paths from the files being rewritten
    Set<String> filesToCompact = Sets.newHashSet();
    for (DataFile dataFile : rewrittenDataFiles) {
      filesToCompact.add(dataFile.path().toString());
    }

    // Detect conflicts
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(table.io(), metadata, startingSnapshotId, currentSnapshot);
    DeleteConflictInfo conflicts = detector.detectConflicts(filesToCompact);

    if (!conflicts.hasConflicts()) {
      LOG.debug("No conflicting deletes detected for compaction");
      return DeleteManifestChanges.empty();
    }

    // Check max manifests limit
    int deleteManifestCount = conflicts.deleteFileCount();
    int maxManifests = maxRemapManifests();
    if (deleteManifestCount > maxManifests) {
      throw new ValidationException(
          "Compaction conflict resolution exceeded maximum manifest limit. "
              + "Found %d conflicting delete files, limit is %d. "
              + "Either increase %s or disable %s to fail on conflicts.",
          deleteManifestCount,
          maxManifests,
          TableProperties.COMPACTION_REMAP_MAX_MANIFESTS,
          TableProperties.COMPACTION_REMAP_CONFLICTING_DELETES);
    }

    LOG.info(
        "Detected {} conflicting delete files affecting {} data files, attempting resolution",
        conflicts.deleteFileCount(),
        conflicts.affectedDataFileCount());

    // Resolve conflicts
    try {
      CompactionConflictResolver resolver = new CompactionConflictResolver(table);
      return resolver.resolve(compactionMap, conflicts);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to resolve conflicting deletes", e);
    }
  }

  /**
   * Write a compaction map to the metadata location.
   *
   * @param map the compaction map to write
   * @return the location of the written compaction map, or null if the map is empty
   */
  private String writeCompactionMap(CompactionMap map) {
    // Don't write empty maps
    if (map.fileMappings().isEmpty()) {
      return null;
    }

    try {
      // Generate unique snapshot ID for target (will be assigned during commit)
      long targetSnapshotId = startingSnapshotId + 1;
      OutputFile mapFile = CompactionMaps.newCompactionMapFile(table, targetSnapshotId);

      CompactionMaps.write(map, mapFile);

      LOG.info(
          "Wrote compaction map with {} file mappings to {}",
          map.fileMappings().size(),
          mapFile.location());

      return mapFile.location();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write compaction map", e);
    }
  }

  /**
   * Build a compaction map from file groups by tracking position mappings from source to target
   * files.
   *
   * <p>This method checks if file groups have explicit position mappings (from Spark-level
   * tracking). If available, those mappings are used to build accurate compaction maps. Otherwise,
   * it falls back to simple offset-based mapping for bin-pack scenarios.
   *
   * @param fileGroups the file groups being rewritten
   * @return the compaction map with file-level position mappings
   */
  private CompactionMap buildCompactionMap(Set<RewriteFileGroup> fileGroups) {
    CompactionMapBuilder builder =
        new CompactionMapBuilder(startingSnapshotId, startingSnapshotId + 1);

    for (RewriteFileGroup group : fileGroups) {
      // Get source and target files
      Set<DataFile> sourceFiles = group.rewrittenFiles();
      Set<DataFile> targetFiles = group.addedFiles();

      if (sourceFiles.isEmpty() || targetFiles.isEmpty()) {
        continue;
      }

      // Check if we have explicit position mappings from Spark-level tracking
      Map<String, RewriteFileGroup.FilePositionMapping> positionMappings = group.positionMappings();

      if (positionMappings != null && !positionMappings.isEmpty()) {
        // Use explicit position mappings from rewrite operation
        for (RewriteFileGroup.FilePositionMapping mapping : positionMappings.values()) {
          CompactionMapBuilder.FileMappingBuilder fileMappingBuilder =
              builder.addFileMapping(mapping.sourceFile(), mapping.targetFile());

          // Add all runs (supports both single-run and multi-run mappings)
          for (RewriteFileGroup.FilePositionMapping.Run run : mapping.runs()) {
            fileMappingBuilder.addRun(run.sourceOffset(), run.targetOffset(), run.length());
          }
        }
      } else {
        // Fallback: Simple bin-pack mapping for backward compatibility
        // Assumes row order is preserved and all sources map to a single target
        if (targetFiles.size() == 1) {
          DataFile targetFile = targetFiles.iterator().next();
          long targetOffset = 0;

          // Map each source file to the target file with sequential offsets
          for (DataFile sourceFile : sourceFiles) {
            builder
                .addFileMapping(sourceFile.path().toString(), targetFile.path().toString())
                .addRun(0L, targetOffset, sourceFile.recordCount());

            targetOffset += sourceFile.recordCount();
          }
        } else {
          // Multiple target files without position tracking - cannot build accurate map
          LOG.warn(
              "Skipping compaction map for group with multiple target files ({}). "
                  + "Multi-target compaction maps require position tracking during rewrite.",
              targetFiles.size());
        }
      }
    }

    return builder.build();
  }

  /**
   * An async service which allows for committing multiple file groups as their rewrites complete.
   * The service also allows for partial-progress since commits can fail. Once the service has been
   * closed no new file groups should not be offered.
   *
   * @param rewritesPerCommit number of file groups to include in a commit
   * @return the service for handling commits
   */
  public CommitService service(int rewritesPerCommit) {
    return new CommitService(rewritesPerCommit);
  }

  public class CommitService extends BaseCommitService<RewriteFileGroup> {

    CommitService(int rewritesPerCommit) {
      super(table, rewritesPerCommit);
    }

    @Override
    protected void commitOrClean(Set<RewriteFileGroup> batch) {
      RewriteDataFilesCommitManager.this.commitOrClean(batch);
    }

    @Override
    protected void abortFileGroup(RewriteFileGroup group) {
      RewriteDataFilesCommitManager.this.abortFileGroup(group);
    }
  }
}
