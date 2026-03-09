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
package org.apache.iceberg.spark.actions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseRewriteFiles;
import org.apache.iceberg.CompactionConflictDetector;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMapBuilder;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteConflictInfo;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFilesCommitManager;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark-specific extension of {@link RewriteDataFilesCommitManager} that adds conflict resolution
 * capabilities.
 *
 * <p>When enabled via table properties, this manager can detect and resolve conflicts with
 * concurrent position delete transactions by remapping the deletes to reference the compacted
 * files.
 */
public class SparkRewriteDataFilesCommitManager extends RewriteDataFilesCommitManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(SparkRewriteDataFilesCommitManager.class);

  private final SparkSession spark;
  private final Table table;
  private final long startingSnapshotId;
  private final boolean useStartingSequenceNumber;
  private final Map<String, String> snapshotProperties;

  public SparkRewriteDataFilesCommitManager(
      SparkSession spark, Table table, long startingSnapshotId) {
    this(spark, table, startingSnapshotId, RewriteDataFiles.USE_STARTING_SEQUENCE_NUMBER_DEFAULT);
  }

  public SparkRewriteDataFilesCommitManager(
      SparkSession spark, Table table, long startingSnapshotId, boolean useStartingSequenceNumber) {
    this(spark, table, startingSnapshotId, useStartingSequenceNumber, ImmutableMap.of());
  }

  public SparkRewriteDataFilesCommitManager(
      SparkSession spark,
      Table table,
      long startingSnapshotId,
      boolean useStartingSequenceNumber,
      Map<String, String> snapshotProperties) {
    super(table, startingSnapshotId, useStartingSequenceNumber, snapshotProperties);
    this.spark = spark;
    this.table = table;
    this.startingSnapshotId = startingSnapshotId;
    this.useStartingSequenceNumber = useStartingSequenceNumber;
    this.snapshotProperties = snapshotProperties;
  }

  /**
   * Perform a commit operation on the table with conflict resolution support.
   *
   * <p>If conflict resolution is enabled and conflicts are detected with concurrent position delete
   * transactions, this method will attempt to resolve them by remapping the deletes.
   *
   * @param fileGroups fileSets to commit
   */
  @Override
  public void commitFileGroups(Set<RewriteFileGroup> fileGroups) {
    DataFileSet rewrittenDataFiles = DataFileSet.create();
    DataFileSet addedDataFiles = DataFileSet.create();
    DeleteFileSet danglingDVs = DeleteFileSet.create();
    for (RewriteFileGroup group : fileGroups) {
      rewrittenDataFiles.addAll(group.rewrittenFiles());
      addedDataFiles.addAll(group.addedFiles());
      danglingDVs.addAll(group.danglingDVs());
    }

    // Build compaction map if enabled
    CompactionMap compactionMap = null;
    String compactionMapLocation = null;

    if (shouldGenerateCompactionMap()) {
      compactionMap = buildCompactionMap(fileGroups);
      compactionMapLocation = writeCompactionMap(compactionMap);
    }

    // Check for conflicts and potentially resolve them
    List<DeleteFile> remappedDeleteFiles = null;
    if (shouldResolveDeleteConflicts() && compactionMap != null) {
      remappedDeleteFiles = detectAndResolveConflicts(compactionMap, rewrittenDataFiles);
    }

    // Perform the commit
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshotId);
    if (useStartingSequenceNumber) {
      long sequenceNumber = table.snapshot(startingSnapshotId).sequenceNumber();
      rewrite.dataSequenceNumber(sequenceNumber);
    }

    // Attach compaction map location if available
    if (compactionMapLocation != null && rewrite instanceof BaseRewriteFiles) {
      ((BaseRewriteFiles) rewrite).setCompactionMapLocation(compactionMapLocation);
    }

    rewrittenDataFiles.forEach(rewrite::deleteFile);
    addedDataFiles.forEach(rewrite::addFile);
    danglingDVs.forEach(rewrite::deleteFile);

    // Add remapped delete files if any
    if (remappedDeleteFiles != null && !remappedDeleteFiles.isEmpty()) {
      remappedDeleteFiles.forEach(rewrite::addFile);
      LOG.info("Including {} remapped delete files in commit", remappedDeleteFiles.size());
    }

    snapshotProperties.forEach(rewrite::set);

    rewrite.commit();
  }

  @Override
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
   * Detects conflicts with concurrent position delete transactions and resolves them.
   *
   * @param compactionMap the compaction map describing file transformations
   * @param rewrittenDataFiles the data files being rewritten
   * @return list of new delete files with remapped positions, or null if no conflicts
   */
  private List<DeleteFile> detectAndResolveConflicts(
      CompactionMap compactionMap, DataFileSet rewrittenDataFiles) {
    // Refresh table to get latest snapshot
    table.refresh();
    Snapshot currentSnapshot = table.currentSnapshot();

    if (currentSnapshot == null || currentSnapshot.snapshotId() == startingSnapshotId) {
      LOG.debug("No intervening snapshots, skipping conflict detection");
      return null;
    }

    // Extract source file paths
    Set<String> sourceFilePaths = Sets.newHashSet();
    for (DataFile file : rewrittenDataFiles) {
      sourceFilePaths.add(file.path().toString());
    }

    // Detect conflicts
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(),
            ((HasTableOperations) table).operations().current(),
            startingSnapshotId,
            currentSnapshot);

    DeleteConflictInfo conflicts = detector.detectConflicts(sourceFilePaths);

    if (!conflicts.hasConflicts()) {
      LOG.debug("No delete conflicts detected");
      return null;
    }

    // Check if we should resolve based on limits
    int maxFilesToResolve = maxDeleteFilesToResolve();
    if (conflicts.deleteFileCount() > maxFilesToResolve) {
      throw new ValidationException(
          "Too many conflicting delete files to resolve (%d > %d). "
              + "Either increase %s or abort this compaction.",
          conflicts.deleteFileCount(),
          maxFilesToResolve,
          TableProperties.COMPACTION_RESOLVE_DELETE_CONFLICTS_MAX_FILES);
    }

    LOG.info(
        "Detected {} conflicting delete files, attempting resolution", conflicts.deleteFileCount());

    // Resolve conflicts using the Spark resolver
    SparkCompactionConflictResolver resolver = new SparkCompactionConflictResolver(spark, table);
    return resolver.resolve(compactionMap, conflicts);
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
   * Check if delete conflict resolution is enabled.
   *
   * @return true if conflict resolution is enabled
   */
  private boolean shouldResolveDeleteConflicts() {
    return table
        .properties()
        .getOrDefault(
            TableProperties.COMPACTION_RESOLVE_DELETE_CONFLICTS,
            String.valueOf(TableProperties.COMPACTION_RESOLVE_DELETE_CONFLICTS_DEFAULT))
        .equalsIgnoreCase("true");
  }

  /**
   * Get the maximum number of delete files to resolve.
   *
   * @return the maximum number of delete files
   */
  private int maxDeleteFilesToResolve() {
    return Integer.parseInt(
        table
            .properties()
            .getOrDefault(
                TableProperties.COMPACTION_RESOLVE_DELETE_CONFLICTS_MAX_FILES,
                String.valueOf(
                    TableProperties.COMPACTION_RESOLVE_DELETE_CONFLICTS_MAX_FILES_DEFAULT)));
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
   * Build a compaction map from file groups.
   *
   * @param fileGroups the file groups being rewritten
   * @return the compaction map
   */
  private CompactionMap buildCompactionMap(Set<RewriteFileGroup> fileGroups) {
    CompactionMapBuilder builder =
        new CompactionMapBuilder(startingSnapshotId, startingSnapshotId + 1);

    for (RewriteFileGroup group : fileGroups) {
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

          for (RewriteFileGroup.FilePositionMapping.Run run : mapping.runs()) {
            fileMappingBuilder.addRun(run.sourceOffset(), run.targetOffset(), run.length());
          }
        }
      } else {
        // No explicit position mappings available. Skip this group rather than generating
        // a potentially unsound fallback map. Without position tracking, we cannot guarantee
        // the map correctly reflects actual row-to-position assignments.
        LOG.warn(
            "Skipping compaction map for group without position tracking ({} source → {} target files). "
                + "Enable position tracking for correct compaction map generation.",
            sourceFiles.size(),
            targetFiles.size());
      }
    }

    return builder.build();
  }

  /** Spark-specific commit service that uses this manager. */
  @Override
  public CommitService service(int rewritesPerCommit) {
    return new SparkCommitService(rewritesPerCommit);
  }

  private class SparkCommitService extends CommitService {

    SparkCommitService(int rewritesPerCommit) {
      super(rewritesPerCommit);
    }

    @Override
    protected void commitOrClean(Set<RewriteFileGroup> batch) {
      SparkRewriteDataFilesCommitManager.this.commitOrClean(batch);
    }

    @Override
    protected void abortFileGroup(RewriteFileGroup group) {
      SparkRewriteDataFilesCommitManager.this.abortFileGroup(group);
    }
  }
}
