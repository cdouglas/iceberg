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
package org.apache.iceberg.spark;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collects the position mappings of Spark rewrites on the driver.
 *
 * <p>Each task of a rewrite that tracks source positions sends back, with its commit message, the
 * runs of source positions it wrote to each target file. The rewrite's commit records the runs of
 * its committed tasks here, under its file set id, and the action fetches them once the rewrite
 * completes. A source file that the rewrite split between tasks gets runs from each of them. Gaps
 * in source positions indicate rows filtered out by position deletes.
 *
 * <p>Thread-safe for rewrites of several file groups at once.
 */
public class PositionMappingCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(PositionMappingCoordinator.class);
  private static final PositionMappingCoordinator INSTANCE = new PositionMappingCoordinator();

  // Key: (tableUuid, fileSetId)
  // Value: Map of (sourceFile -> List<PositionRun>)
  private final Map<Pair<String, String>, Map<String, List<PositionRun>>> mappings =
      Maps.newConcurrentMap();

  private PositionMappingCoordinator() {}

  public static PositionMappingCoordinator get() {
    return INSTANCE;
  }

  /**
   * Records a run of rows that a rewrite wrote from a source file to a target file.
   *
   * <p>Called on the driver when the rewrite commits, for each run of each committed task.
   *
   * @param table the table being rewritten
   * @param fileSetId the unique identifier for this rewrite group
   * @param sourceFile the source file path
   * @param sourcePos the position of the run's first row in the source file
   * @param targetFile the target file path
   * @param targetPos the position of the run's first row in the target file
   * @param length the number of rows in the run
   */
  public synchronized void recordRun(
      Table table,
      String fileSetId,
      String sourceFile,
      long sourcePos,
      String targetFile,
      long targetPos,
      long length) {
    Pair<String, String> id = toId(table, fileSetId);

    Map<String, List<PositionRun>> fileMappings =
        mappings.computeIfAbsent(id, k -> Maps.newHashMap());

    List<PositionRun> runs = fileMappings.computeIfAbsent(sourceFile, k -> Lists.newArrayList());

    runs.add(new PositionRun(sourcePos, targetFile, targetPos, length));
  }

  /**
   * Fetches and aggregates all position mappings for a rewrite group.
   *
   * <p>Called from the driver after the rewrite commits. Groups runs by source file, sorts them by
   * position, and joins runs that continue one another. Gaps in source positions indicate rows that
   * were filtered out by position deletes.
   *
   * @param table the table being rewritten
   * @param fileSetId the unique identifier for this rewrite group
   * @return map of source file path to FilePositionMapping with runs
   */
  public synchronized Map<String, RewriteFileGroup.FilePositionMapping> fetchMappings(
      Table table, String fileSetId) {
    Pair<String, String> id = toId(table, fileSetId);
    Map<String, List<PositionRun>> recorded = mappings.get(id);

    if (recorded == null || recorded.isEmpty()) {
      LOG.debug("No position mappings found for {} - fileset {}", table.name(), fileSetId);
      return Maps.newHashMap();
    }

    LOG.debug(
        "Aggregating position mappings for {} - fileset {} with {} source files",
        table.name(),
        fileSetId,
        recorded.size());

    return aggregateMappings(recorded);
  }

  /**
   * Clears all position mappings for a rewrite group.
   *
   * <p>Called after commit completes to free memory.
   *
   * @param table the table being rewritten
   * @param fileSetId the unique identifier for this rewrite group
   */
  public synchronized void clearRewrite(Table table, String fileSetId) {
    LOG.debug("Removing position mappings for {} - id {}", table.name(), fileSetId);
    Pair<String, String> id = toId(table, fileSetId);
    mappings.remove(id);
  }

  /**
   * Aggregates recorded runs into FilePositionMapping objects.
   *
   * <p>Key insight: gaps in source positions indicate deleted rows. A run joins the one before it
   * only if both its source and target positions continue that run's, in the same target file;
   * otherwise it stays a run of its own.
   *
   * <p>Overlapping runs are impossible by construction: each task reads a source file's rows in
   * order, tasks read disjoint ranges of it, and each task numbers the rows it writes to a target
   * file sequentially.
   *
   * <p>Supports multi-target mappings where a source file's rows span multiple target files (e.g.,
   * due to target file size limits). Each run stores its target file path.
   *
   * @param recorded map of source file to the runs recorded for it
   * @return map of source file to FilePositionMapping with runs
   */
  private Map<String, RewriteFileGroup.FilePositionMapping> aggregateMappings(
      Map<String, List<PositionRun>> recorded) {

    Map<String, RewriteFileGroup.FilePositionMapping> result = Maps.newHashMap();

    for (Map.Entry<String, List<PositionRun>> entry : recorded.entrySet()) {
      String sourceFile = entry.getKey();
      List<PositionRun> positions = entry.getValue();

      if (positions.isEmpty()) {
        continue;
      }

      // Sort by source position
      positions.sort(Comparator.comparing(p -> p.sourcePos));

      // Check if this is a multi-target mapping
      Set<String> targetFiles =
          positions.stream().map(p -> p.targetFile).collect(Collectors.toSet());
      boolean isMultiTarget = targetFiles.size() > 1;

      if (isMultiTarget) {
        LOG.debug(
            "Source file {} maps to {} target files: {}",
            sourceFile,
            targetFiles.size(),
            targetFiles);
      }

      // Join runs unless a gap in source positions, target positions, or a target file change
      // separates them
      List<RewriteFileGroup.FilePositionMapping.Run> runs = new ArrayList<>();
      PositionRun first = positions.get(0);
      long runStartSource = first.sourcePos;
      long runStartTarget = first.targetPos;
      String runTargetFile = first.targetFile;
      long runLength = first.length;

      for (int i = 1; i < positions.size(); i++) {
        PositionRun curr = positions.get(i);

        // Gap detection: a run continues the current one if it starts right after it
        boolean sourceConsecutive = curr.sourcePos == runStartSource + runLength;
        boolean targetConsecutive = curr.targetPos == runStartTarget + runLength;
        boolean sameTarget = curr.targetFile.equals(runTargetFile);

        if (sourceConsecutive && targetConsecutive && sameTarget) {
          // Continue current run
          runLength += curr.length;
        } else {
          // Gap detected or target file changed - end current run, start new one
          // Store target file in run for multi-target support
          runs.add(
              new RewriteFileGroup.FilePositionMapping.Run(
                  runStartSource, runStartTarget, runLength, runTargetFile));

          if (!sameTarget) {
            LOG.debug(
                "Target file changed at source position {} in {}: {} -> {}",
                curr.sourcePos,
                sourceFile,
                runTargetFile,
                curr.targetFile);
          } else {
            LOG.debug(
                "Detected gap at source position {} -> {} in {}",
                runStartSource + runLength - 1,
                curr.sourcePos,
                sourceFile);
          }

          runStartSource = curr.sourcePos;
          runStartTarget = curr.targetPos;
          runTargetFile = curr.targetFile;
          runLength = curr.length;
        }
      }

      // Add final run with its target file
      runs.add(
          new RewriteFileGroup.FilePositionMapping.Run(
              runStartSource, runStartTarget, runLength, runTargetFile));

      LOG.debug(
          "Aggregated {} recorded runs into {} runs for source file {} (targets: {})",
          positions.size(),
          runs.size(),
          sourceFile,
          targetFiles);

      // Use first position's target as default (for backward compat), but runs have their own
      String defaultTarget = positions.get(0).targetFile;
      result.put(
          sourceFile, new RewriteFileGroup.FilePositionMapping(sourceFile, defaultTarget, runs));
    }

    return result;
  }

  private Pair<String, String> toId(Table table, String setId) {
    return Pair.of(table.uuid().toString(), setId);
  }

  /** Represents a run of rows mapped from a source file to a target file. */
  static class PositionRun {
    final long sourcePos;
    final String targetFile;
    final long targetPos;
    final long length;

    PositionRun(long sourcePos, String targetFile, long targetPos, long length) {
      this.sourcePos = sourcePos;
      this.targetFile = targetFile;
      this.targetPos = targetPos;
      this.length = length;
    }
  }
}
