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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseRewriteFiles;
import org.apache.iceberg.CompactionConflictDetector;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMapBuilder;
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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.SnapshotUtil;
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
  private final String branch;

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
    this(spark, table, startingSnapshotId, useStartingSequenceNumber, snapshotProperties, null);
  }

  public SparkRewriteDataFilesCommitManager(
      SparkSession spark,
      Table table,
      long startingSnapshotId,
      boolean useStartingSequenceNumber,
      Map<String, String> snapshotProperties,
      String branch) {
    super(table, startingSnapshotId, useStartingSequenceNumber, snapshotProperties, branch);
    this.spark = spark;
    this.table = table;
    this.startingSnapshotId = startingSnapshotId;
    this.useStartingSequenceNumber = useStartingSequenceNumber;
    this.snapshotProperties = snapshotProperties;
    this.branch = branch;
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

    // Build compaction map if enabled; the rewrite writes it when it commits
    CompactionMap compactionMap = null;
    if (shouldGenerateCompactionMap()) {
      compactionMap = buildCompactionMap(fileGroups);
    }

    // Check for conflicts and potentially resolve them
    Resolution resolution = null;
    if (shouldResolveDeleteConflicts() && compactionMap != null) {
      resolution = detectAndResolveConflicts(compactionMap, rewrittenDataFiles);
    }

    // A resolution accounts for every delete committed up to the snapshot it read, so the rewrite
    // only validates the deletes committed after that snapshot
    long validateFromSnapshotId =
        resolution != null ? resolution.resolvedSnapshotId : startingSnapshotId;
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(validateFromSnapshotId);
    if (useStartingSequenceNumber) {
      long sequenceNumber = table.snapshot(startingSnapshotId).sequenceNumber();
      rewrite.dataSequenceNumber(sequenceNumber);
    }

    if (compactionMap != null && rewrite instanceof BaseRewriteFiles) {
      ((BaseRewriteFiles) rewrite).setCompactionMap(compactionMap);
    }

    rewrittenDataFiles.forEach(rewrite::deleteFile);
    addedDataFiles.forEach(rewrite::addFile);
    for (DeleteFile dv : danglingDVs) {
      // a concurrent commit may already have replaced a rewritten file's DV
      if (resolution == null || !resolution.removedDeleteFiles.contains(dv)) {
        rewrite.deleteFile(dv);
      }
    }

    // Each remapped delete file replaces the conflicting deletes it came from
    if (resolution != null) {
      resolution.replacedDeleteFiles.forEach(rewrite::deleteFile);
      resolution.remappedDeleteFiles.forEach(rewrite::addFile);
      LOG.info(
          "Replacing {} conflicting delete files with {} remapped delete files",
          resolution.replacedDeleteFiles.size(),
          resolution.remappedDeleteFiles.size());
    }

    snapshotProperties.forEach(rewrite::set);

    if (branch != null) {
      rewrite.toBranch(branch);
    }

    try {
      rewrite.commit();
    } catch (RuntimeException e) {
      if (resolution != null && e instanceof CleanableFailure) {
        resolution.remappedDeleteFiles.forEach(file -> table.io().deleteFile(file.location()));
      }

      throw e;
    }
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
   * Detects deletes committed to the rewritten data files since the rewrite started, and remaps
   * them to the files the rewrite writes.
   *
   * <p>A deletion vector or position delete file that references only rewritten data files is
   * replaced by its remapped copy. A position delete file that references rewritten data files and
   * others cannot be replaced by a rewrite, so the rewrite fails.
   *
   * @param compactionMap the compaction map describing file transformations
   * @param rewrittenDataFiles the data files being rewritten
   * @return the resolution, or null if no snapshot was committed since the rewrite started
   */
  private Resolution detectAndResolveConflicts(
      CompactionMap compactionMap, DataFileSet rewrittenDataFiles) {
    // Refresh table to get the latest snapshot of the branch the rewrite commits to
    table.refresh();
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);

    if (currentSnapshot == null || currentSnapshot.snapshotId() == startingSnapshotId) {
      LOG.debug("No intervening snapshots, skipping conflict detection");
      return null;
    }

    // Without the starting sequence number, the rewritten files would be newer than equality
    // deletes committed since the rewrite started, which would stop applying to them. Only the
    // rewrite's own validation from the starting snapshot catches that.
    if (!useStartingSequenceNumber) {
      LOG.info("Not resolving delete conflicts: the rewrite does not use the starting sequence");
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
      return new Resolution(
          currentSnapshot.snapshotId(),
          ImmutableList.of(),
          ImmutableList.of(),
          conflicts.removedDeleteFiles());
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

    SparkCompactionConflictResolver resolver = new SparkCompactionConflictResolver(spark, table);
    List<DeleteFile> replaced = Lists.newArrayList(conflicts.conflictingDeleteFiles());
    if (conflicts.hasMultiFilePositionDeletes()) {
      Map<String, Set<String>> referenced =
          resolver.referencedDataFiles(conflicts.multiFilePositionDeletes());
      for (DeleteFile deleteFile : conflicts.multiFilePositionDeletes()) {
        Set<String> dataFiles = referenced.getOrDefault(deleteFile.location(), ImmutableSet.of());
        Set<String> rewritten = Sets.intersection(dataFiles, sourceFilePaths);
        if (rewritten.isEmpty()) {
          continue;
        }

        ValidationException.check(
            rewritten.size() == dataFiles.size(),
            "Cannot resolve position delete file %s: it references %s rewritten data files and %s "
                + "others, and a rewrite can only replace deletes of the files it rewrites",
            deleteFile.location(),
            rewritten.size(),
            dataFiles.size() - rewritten.size());
        replaced.add(deleteFile);
      }
    }

    // Resolve conflicts using the Spark resolver
    List<DeleteFile> remapped =
        replaced.isEmpty() ? ImmutableList.of() : resolver.resolve(compactionMap, replaced);
    return new Resolution(
        currentSnapshot.snapshotId(), replaced, remapped, conflicts.removedDeleteFiles());
  }

  /** The deletes a rewrite replaces, and the snapshot through which it accounted for them. */
  private static class Resolution {
    private final long resolvedSnapshotId;
    private final List<DeleteFile> replacedDeleteFiles;
    private final List<DeleteFile> remappedDeleteFiles;
    private final Set<DeleteFile> removedDeleteFiles;

    private Resolution(
        long resolvedSnapshotId,
        List<DeleteFile> replacedDeleteFiles,
        List<DeleteFile> remappedDeleteFiles,
        Set<DeleteFile> removedDeleteFiles) {
      this.resolvedSnapshotId = resolvedSnapshotId;
      this.replacedDeleteFiles = replacedDeleteFiles;
      this.remappedDeleteFiles = remappedDeleteFiles;
      this.removedDeleteFiles = removedDeleteFiles;
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
   * Build a compaction map from file groups.
   *
   * @param fileGroups the file groups being rewritten
   * @return the compaction map
   */
  private CompactionMap buildCompactionMap(Set<RewriteFileGroup> fileGroups) {
    // The rewrite records its own snapshot as the target when it writes the map
    CompactionMapBuilder builder = new CompactionMapBuilder(startingSnapshotId, -1L);

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
            // Propagate the per-run target file so multi-target compactions (source rows
            // spanning multiple output files due to target-size rolls) remap to the correct
            // target file. Without this, every run silently uses the FileMapping's default
            // target and the remapped positions point at rows in the wrong file.
            fileMappingBuilder.addRun(
                run.sourceOffset(), run.targetOffset(), run.length(), run.targetFile());
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
