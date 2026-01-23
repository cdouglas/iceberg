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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.benchmark.cloud.config.BenchmarkConfig;
import org.apache.iceberg.benchmark.cloud.metrics.MetricsCollector;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CompactionConflictException;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simulates transaction execution with timing and conflict handling.
 *
 * <p>This simulator executes delete transactions against a simulated table, measuring scan times
 * proportionally to row counts and handling compaction conflicts through remapping.
 */
public class TransactionSimulator {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionSimulator.class);

  private final SimulatedTable table;
  private final MetricsCollector metrics;
  private final BenchmarkConfig config;
  private final Random random;

  public TransactionSimulator(
      SimulatedTable table, MetricsCollector metrics, BenchmarkConfig config, long seed) {
    this.table = table;
    this.metrics = metrics;
    this.config = config;
    this.random = new Random(seed);
  }

  /**
   * Execute a simulated delete operation.
   *
   * @param operation the delete operation parameters
   * @return true if successful (possibly after retries), false if failed
   */
  public boolean executeDelete(DeleteOperation operation) {
    long startTime = System.nanoTime();
    int retryCount = 0;
    boolean success = false;

    while (!success && retryCount < 3) {
      try {
        // 1. Simulate scan time proportional to data size
        long rowsScanned = calculateRowsToScan(operation);
        simulateScanTime(rowsScanned, operation.selectivity());

        // 2. Create position deletes
        List<DeleteFile> deleteFiles = createSimulatedDeletes(operation);

        // 3. Commit
        RowDelta rowDelta = table.newRowDelta();
        for (DeleteFile deleteFile : deleteFiles) {
          rowDelta.addDeletes(deleteFile);
        }

        if (config.compactionMapsEnabled()) {
          rowDelta.validateNoConflictingDataFiles();
        }

        rowDelta.commit();

        success = true;
        metrics.recordSuccessfulDelete(operation, System.nanoTime() - startTime);

      } catch (CompactionConflictException e) {
        retryCount++;
        metrics.recordConflict(e, operation);

        // Attempt remapping
        long remapStart = System.nanoTime();
        boolean remapped = handleConflict(e, operation);
        metrics.recordRemapAttempt(remapped, System.nanoTime() - remapStart);

        if (!remapped) {
          LOG.warn("Failed to remap deletes after compaction conflict", e);
          break;
        }
        // Loop continues with retry
      } catch (CommitFailedException e) {
        retryCount++;
        LOG.info("Commit failed, retrying ({}/3): {}", retryCount, e.getMessage());
        // Refresh and retry
        table.refresh();
      }
    }

    if (!success) {
      metrics.recordFailedDelete(operation, System.nanoTime() - startTime, retryCount);
    }

    return success;
  }

  /**
   * Execute initial load of data files.
   *
   * @param numFiles number of files to create
   * @param avgRowsPerFile average rows per file
   */
  public void executeInitialLoad(int numFiles, long avgRowsPerFile) {
    long startTime = System.nanoTime();

    DataFile[] files =
        SimulatedDataFile.createBatch(
            table.spec(), numFiles, avgRowsPerFile, config.rowCountVariance(), random);

    table.addFiles(files);

    metrics.recordInitialLoad(numFiles, sumRows(files), System.nanoTime() - startTime);
    LOG.info(
        "Initial load complete: {} files, {} total rows",
        numFiles,
        sumRows(files));
  }

  /**
   * Execute adding new files.
   *
   * @param numFiles number of files to add
   * @param avgRowsPerFile average rows per file
   */
  public void executeAddFiles(int numFiles, long avgRowsPerFile) {
    DataFile[] files =
        SimulatedDataFile.createBatch(
            table.spec(), numFiles, avgRowsPerFile, config.rowCountVariance(), random);

    table.addFiles(files);
    metrics.recordAddFiles(numFiles, sumRows(files));
  }

  private long calculateRowsToScan(DeleteOperation operation) {
    // Scan rows proportional to selectivity and table size
    return (long) (table.getTotalRowCount() * operation.selectivity() * 10);
  }

  private void simulateScanTime(long rowsScanned, double selectivity) {
    long baseTimeMs = rowsScanned / config.rowsPerMs();

    // Add merge-on-read penalty if deletes exist
    if (hasPendingDeletes()) {
      baseTimeMs = (long) (baseTimeMs * (1 + config.mergeOnReadPenalty()));
    }

    if (baseTimeMs > 0) {
      try {
        Thread.sleep(Math.min(baseTimeMs, 100)); // Cap at 100ms for benchmarking
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private boolean hasPendingDeletes() {
    Snapshot snapshot = table.currentSnapshot();
    if (snapshot == null) {
      return false;
    }
    String deleteCount =
        snapshot.summary().getOrDefault("total-delete-files", "0");
    return Long.parseLong(deleteCount) > 0;
  }

  private List<DeleteFile> createSimulatedDeletes(DeleteOperation operation) {
    List<DeleteFile> deletes = new ArrayList<>();
    long totalDeletes = (long) (table.getTotalRowCount() * operation.selectivity());
    int numFiles = Math.max(1, (int) (totalDeletes / 10000));

    // Distribute deletes across multiple delete files
    long remainingDeletes = totalDeletes;
    for (int i = 0; i < numFiles && remainingDeletes > 0; i++) {
      long deletesInFile = Math.min(remainingDeletes, 10000 + random.nextInt(5000));
      deletes.add(
          SimulatedDeleteFile.createPositionDeletes(table.spec(), deletesInFile));
      remainingDeletes -= deletesInFile;
    }

    return deletes;
  }

  private boolean handleConflict(CompactionConflictException e, DeleteOperation operation) {
    // In a real implementation, we would use PositionDeleteRemapper here
    // For simulation, we simply simulate the remapping time

    // Get compaction map info from exception
    if (e.compactionMap() == null) {
      return false;
    }

    // Simulate remapping (actual remapping would use the compaction map)
    // The time is proportional to the number of deletes and map complexity
    long remapTimeMs = 1 + random.nextInt(5);

    try {
      Thread.sleep(remapTimeMs);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return false;
    }

    return true;
  }

  private long sumRows(DataFile[] files) {
    long total = 0;
    for (DataFile file : files) {
      total += file.recordCount();
    }
    return total;
  }

  /** Parameters for a delete operation. */
  public static class DeleteOperation {
    private final double selectivity;
    private final WorkloadGenerator.DeletePattern pattern;
    private final Set<String> targetFiles;

    public DeleteOperation(double selectivity, WorkloadGenerator.DeletePattern pattern) {
      this(selectivity, pattern, null);
    }

    public DeleteOperation(
        double selectivity, WorkloadGenerator.DeletePattern pattern, Set<String> targetFiles) {
      this.selectivity = selectivity;
      this.pattern = pattern;
      this.targetFiles = targetFiles;
    }

    public double selectivity() {
      return selectivity;
    }

    public WorkloadGenerator.DeletePattern pattern() {
      return pattern;
    }

    public Set<String> targetFiles() {
      return targetFiles;
    }

    public boolean targetsSpecificFiles() {
      return targetFiles != null && !targetFiles.isEmpty();
    }
  }
}
