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
package org.apache.iceberg.benchmark.cloud;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMapBuilder;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.benchmark.cloud.config.BenchmarkConfig;
import org.apache.iceberg.benchmark.cloud.metrics.MetricsCollector;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simulates compaction operations with compaction map generation.
 *
 * <p>This simulator executes compaction operations against a simulated table, generating compaction
 * maps that track position transformations from source to target files.
 */
public class CompactionSimulator {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionSimulator.class);

  private final SimulatedTable table;
  private final MetricsCollector metrics;
  private final BenchmarkConfig config;
  private final Random random;

  // Track files currently being compacted for concurrent conflict testing
  private volatile Set<String> compactingFiles = new HashSet<>();

  public CompactionSimulator(
      SimulatedTable table, MetricsCollector metrics, BenchmarkConfig config, long seed) {
    this.table = table;
    this.metrics = metrics;
    this.config = config;
    this.random = new Random(seed);
  }

  /**
   * Execute a simulated compaction operation.
   *
   * @param operation the compaction operation parameters
   * @return true if successful, false if failed
   */
  public boolean executeCompaction(CompactionOperation operation) {
    long startTime = System.nanoTime();

    try {
      // 1. Select files to compact
      List<DataFile> sourceFiles = selectFilesToCompact(operation.fileCount());
      if (sourceFiles.isEmpty()) {
        LOG.info("No files available for compaction");
        return false;
      }

      // Mark files as being compacted (for concurrent conflict testing)
      compactingFiles = sourceFiles.stream()
          .map(DataFile::location)
          .collect(Collectors.toSet());

      // 2. Calculate and simulate compaction time
      long totalRows = sumRows(sourceFiles);
      simulateCompactionTime(totalRows);

      // 3. Create target file
      DataFile targetFile = SimulatedDataFile.create(
          table.spec(),
          totalRows,
          String.format("data/compacted-%s.parquet", UUID.randomUUID()));

      // 4. Build compaction map (if enabled)
      CompactionMap compactionMap = null;
      long mapBuildStart = System.nanoTime();

      if (config.compactionMapsEnabled()) {
        compactionMap = buildCompactionMap(sourceFiles, targetFile);
        metrics.recordMapBuild(
            sourceFiles.size(),
            totalRows,
            compactionMap,
            System.nanoTime() - mapBuildStart);
      }

      // 5. Commit rewrite
      RewriteFiles rewrite = table.newRewrite();
      for (DataFile source : sourceFiles) {
        rewrite.deleteFile(source);
      }
      rewrite.addFile(targetFile);

      // Attach compaction map location (simulated)
      if (compactionMap != null) {
        String mapLocation = String.format(
            "%s/metadata/compaction-map-%d-%s.avro",
            config.tableLocation(),
            table.currentSnapshot().snapshotId(),
            UUID.randomUUID());
        rewrite.set("compaction-map-location", mapLocation);
      }

      rewrite.commit();

      // Update tracking
      for (DataFile source : sourceFiles) {
        table.untrackFile(source.location());
      }
      table.trackFile(targetFile.location(), totalRows);

      compactingFiles = new HashSet<>();

      long duration = System.nanoTime() - startTime;
      metrics.recordSuccessfulCompaction(
          sourceFiles.size(), totalRows, duration, compactionMap != null);

      LOG.info(
          "Compaction complete: {} files -> 1 file, {} rows, map: {}",
          sourceFiles.size(),
          totalRows,
          compactionMap != null);

      return true;

    } catch (CommitFailedException e) {
      compactingFiles = new HashSet<>();
      LOG.warn("Compaction commit failed: {}", e.getMessage());
      metrics.recordFailedCompaction(System.nanoTime() - startTime);
      return false;
    } finally {
      compactingFiles = new HashSet<>();
    }
  }

  /**
   * Get the set of files currently being compacted.
   *
   * @return set of file paths currently being compacted
   */
  public Set<String> getCompactingFiles() {
    return new HashSet<>(compactingFiles);
  }

  private List<DataFile> selectFilesToCompact(int targetCount) {
    List<DataFile> candidates = new ArrayList<>();

    Snapshot snapshot = table.currentSnapshot();
    if (snapshot == null) {
      return candidates;
    }

    try (CloseableIterable<DataFile> files =
        table.table().newScan().planFiles().iterator()
            ? CloseableIterable.withNoopClose(
                table.table().newScan().planFiles())
            : null) {
      // Just get some files based on what we track
      // In a real implementation, this would scan the manifest
    } catch (Exception e) {
      // Ignore - we'll use tracked files
    }

    // For simulation, we create pseudo-files based on tracked counts
    int fileCount = table.getFileCount();
    int toSelect = Math.min(targetCount, fileCount);

    for (int i = 0; i < toSelect; i++) {
      // Create a simulated file representing tracked data
      long rowCount = config.avgRowsPerFile();
      DataFile file = SimulatedDataFile.create(table.spec(), rowCount);
      candidates.add(file);
    }

    return candidates;
  }

  private void simulateCompactionTime(long totalRows) {
    long baseTimeMs = totalRows / config.rowsPerMs();
    long compactionTimeMs = (long) (baseTimeMs / config.compactionSpeedup());

    if (compactionTimeMs > 0) {
      try {
        Thread.sleep(Math.min(compactionTimeMs, 100)); // Cap at 100ms for benchmarking
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private CompactionMap buildCompactionMap(List<DataFile> sourceFiles, DataFile targetFile) {
    Snapshot currentSnapshot = table.currentSnapshot();
    long sourceSnapshotId = currentSnapshot != null ? currentSnapshot.snapshotId() : 0;
    long targetSnapshotId = sourceSnapshotId + 1;

    CompactionMapBuilder builder =
        new CompactionMapBuilder(sourceSnapshotId, targetSnapshotId);

    // Build position mappings: each source file's positions map to target offsets
    long targetOffset = 0;
    for (DataFile source : sourceFiles) {
      long sourceRows = source.recordCount();

      builder
          .addFileMapping(source.location(), targetFile.location())
          .addRun(0, targetOffset, sourceRows);

      targetOffset += sourceRows;
    }

    return builder.build();
  }

  private long sumRows(List<DataFile> files) {
    return files.stream().mapToLong(DataFile::recordCount).sum();
  }

  /** Parameters for a compaction operation. */
  public static class CompactionOperation {
    private final int fileCount;
    private final boolean orderPreserving;

    public CompactionOperation(int fileCount) {
      this(fileCount, true);
    }

    public CompactionOperation(int fileCount, boolean orderPreserving) {
      this.fileCount = fileCount;
      this.orderPreserving = orderPreserving;
    }

    public int fileCount() {
      return fileCount;
    }

    public boolean isOrderPreserving() {
      return orderPreserving;
    }
  }
}
