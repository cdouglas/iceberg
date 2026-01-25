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
 * Singleton coordinator to aggregate position mappings from distributed Spark executors.
 *
 * <p>During Spark rewrite operations with position tracking enabled, this coordinator collects raw
 * position mappings from executor tasks and aggregates them into run-based FilePositionMapping
 * objects. Gaps in source positions indicate rows filtered out by position deletes.
 *
 * <p>Thread-safe for concurrent access from multiple Spark executors and driver.
 */
public class PositionMappingCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(PositionMappingCoordinator.class);
  private static final PositionMappingCoordinator INSTANCE = new PositionMappingCoordinator();

  // Key: (tableUuid, fileSetId)
  // Value: Map of (sourceFile -> List<PositionMapping>)
  private final Map<Pair<String, String>, Map<String, List<PositionMapping>>> mappings =
      Maps.newConcurrentMap();

  private PositionMappingCoordinator() {}

  public static PositionMappingCoordinator get() {
    return INSTANCE;
  }

  /**
   * Records a single position mapping from a source file to a target file.
   *
   * <p>Called from Spark executors during write operations. Each row written generates one mapping.
   *
   * @param table the table being rewritten
   * @param fileSetId the unique identifier for this rewrite group
   * @param sourceFile the source file path
   * @param sourcePos the position in the source file (after deletes applied)
   * @param targetFile the target file path
   * @param targetPos the position in the target file
   */
  public synchronized void recordMapping(
      Table table,
      String fileSetId,
      String sourceFile,
      long sourcePos,
      String targetFile,
      long targetPos) {
    Pair<String, String> id = toId(table, fileSetId);

    Map<String, List<PositionMapping>> fileMappings =
        mappings.computeIfAbsent(id, k -> Maps.newHashMap());

    List<PositionMapping> positions =
        fileMappings.computeIfAbsent(sourceFile, k -> Lists.newArrayList());

    positions.add(new PositionMapping(sourceFile, sourcePos, targetFile, targetPos));
  }

  /**
   * Fetches and aggregates all position mappings for a rewrite group.
   *
   * <p>Called from the driver after write completes. Groups raw position mappings by source file,
   * sorts by position, and identifies consecutive runs. Gaps in source positions indicate rows that
   * were filtered out by position deletes.
   *
   * @param table the table being rewritten
   * @param fileSetId the unique identifier for this rewrite group
   * @return map of source file path to FilePositionMapping with runs
   */
  public synchronized Map<String, RewriteFileGroup.FilePositionMapping> fetchMappings(
      Table table, String fileSetId) {
    Pair<String, String> id = toId(table, fileSetId);
    Map<String, List<PositionMapping>> rawMappings = mappings.get(id);

    if (rawMappings == null || rawMappings.isEmpty()) {
      LOG.debug("No position mappings found for {} - fileset {}", table.name(), fileSetId);
      return Maps.newHashMap();
    }

    LOG.debug(
        "Aggregating position mappings for {} - fileset {} with {} source files",
        table.name(),
        fileSetId,
        rawMappings.size());

    return aggregateMappings(rawMappings);
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
   * Aggregates raw position mappings into FilePositionMapping objects with runs.
   *
   * <p>Key insight: gaps in source positions indicate deleted rows. We detect gaps by checking if
   * positions are consecutive. When a gap is detected, we end the current run and start a new one.
   *
   * <p>Overlapping intervals are impossible by construction: each source file is processed
   * independently, positions are strictly increasing, and target positions are assigned
   * sequentially.
   *
   * <p>Supports multi-target mappings where a source file's rows span multiple target files (e.g.,
   * due to target file size limits). Each run stores its target file path.
   *
   * @param rawMappings map of source file to list of position mappings
   * @return map of source file to FilePositionMapping with runs
   */
  private Map<String, RewriteFileGroup.FilePositionMapping> aggregateMappings(
      Map<String, List<PositionMapping>> rawMappings) {

    Map<String, RewriteFileGroup.FilePositionMapping> result = Maps.newHashMap();

    for (Map.Entry<String, List<PositionMapping>> entry : rawMappings.entrySet()) {
      String sourceFile = entry.getKey();
      List<PositionMapping> positions = entry.getValue();

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

      // Identify runs by detecting gaps in source positions, target positions, or target file
      // changes
      List<RewriteFileGroup.FilePositionMapping.Run> runs = new ArrayList<>();
      PositionMapping first = positions.get(0);
      long runStartSource = first.sourcePos;
      long runStartTarget = first.targetPos;
      String runTargetFile = first.targetFile;
      long runLength = 1;

      for (int i = 1; i < positions.size(); i++) {
        PositionMapping curr = positions.get(i);
        PositionMapping prev = positions.get(i - 1);

        // Gap detection: positions are consecutive if they differ by exactly 1
        boolean sourceConsecutive = (curr.sourcePos == prev.sourcePos + 1);
        boolean targetConsecutive = (curr.targetPos == prev.targetPos + 1);
        boolean sameTarget = curr.targetFile.equals(prev.targetFile);

        if (sourceConsecutive && targetConsecutive && sameTarget) {
          // Continue current run
          runLength++;
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
                prev.targetFile,
                curr.targetFile);
          } else {
            LOG.debug(
                "Detected gap at source position {} -> {} in {}",
                prev.sourcePos,
                curr.sourcePos,
                sourceFile);
          }

          runStartSource = curr.sourcePos;
          runStartTarget = curr.targetPos;
          runTargetFile = curr.targetFile;
          runLength = 1;
        }
      }

      // Add final run with its target file
      runs.add(
          new RewriteFileGroup.FilePositionMapping.Run(
              runStartSource, runStartTarget, runLength, runTargetFile));

      LOG.debug(
          "Aggregated {} position mappings into {} runs for source file {} (targets: {})",
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

  /** Represents a single position mapping from source to target. */
  static class PositionMapping {
    final String sourceFile;
    final long sourcePos;
    final String targetFile;
    final long targetPos;

    PositionMapping(String sourceFile, long sourcePos, String targetFile, long targetPos) {
      this.sourceFile = sourceFile;
      this.sourcePos = sourcePos;
      this.targetFile = targetFile;
      this.targetPos = targetPos;
    }
  }
}
