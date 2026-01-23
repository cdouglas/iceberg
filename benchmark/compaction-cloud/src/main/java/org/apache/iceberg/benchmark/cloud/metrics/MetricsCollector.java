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
package org.apache.iceberg.benchmark.cloud.metrics;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.benchmark.cloud.ConflictStatistics;
import org.apache.iceberg.benchmark.cloud.TransactionSimulator.DeleteOperation;
import org.apache.iceberg.exceptions.CompactionConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real-time metrics collector for benchmark execution.
 *
 * <p>Provides a simplified interface for recording various benchmark events and delegates to the
 * underlying ConflictStatistics for aggregation.
 */
public class MetricsCollector {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsCollector.class);

  private final ConflictStatistics stats;
  private final boolean detailedLogging;
  private final Instant startTime;
  private final AtomicLong eventCount;

  public MetricsCollector(ConflictStatistics stats, boolean detailedLogging) {
    this.stats = stats;
    this.detailedLogging = detailedLogging;
    this.startTime = Instant.now();
    this.eventCount = new AtomicLong();
  }

  /** Get the underlying statistics. */
  public ConflictStatistics getStats() {
    return stats;
  }

  /** Get the total event count. */
  public long getEventCount() {
    return eventCount.get();
  }

  /** Get benchmark elapsed time in milliseconds. */
  public long getElapsedTimeMs() {
    return Instant.now().toEpochMilli() - startTime.toEpochMilli();
  }

  // Delete operations
  public void recordSuccessfulDelete(DeleteOperation operation, long durationNs) {
    stats.recordSuccessfulDelete(durationNs);
    eventCount.incrementAndGet();

    if (detailedLogging) {
      LOG.debug(
          "Delete success: selectivity={}, pattern={}, duration={}ms",
          operation.selectivity(),
          operation.pattern(),
          durationNs / 1_000_000.0);
    }
  }

  public void recordFailedDelete(DeleteOperation operation, long durationNs, int retryCount) {
    stats.recordFailedDelete(durationNs);
    eventCount.incrementAndGet();

    LOG.warn(
        "Delete failed after {} retries: selectivity={}, duration={}ms",
        retryCount,
        operation.selectivity(),
        durationNs / 1_000_000.0);
  }

  public void recordConflict(CompactionConflictException exception, DeleteOperation operation) {
    stats.recordConflict();
    eventCount.incrementAndGet();

    if (detailedLogging) {
      LOG.debug(
          "Conflict detected: {} compacted files, map available: {}",
          exception.compactedFileCount(),
          exception.compactionMap() != null);
    }
  }

  public void recordRemapAttempt(boolean success, long durationNs) {
    stats.recordRemapAttempt(success, durationNs);

    if (detailedLogging) {
      LOG.debug(
          "Remap {}: duration={}ms",
          success ? "success" : "failed",
          durationNs / 1_000_000.0);
    }
  }

  // Compaction operations
  public void recordSuccessfulCompaction(
      int numFiles, long numRows, long durationNs, boolean hasMap) {
    stats.recordSuccessfulCompaction(numFiles, numRows, durationNs, hasMap);
    eventCount.incrementAndGet();

    if (detailedLogging) {
      LOG.debug(
          "Compaction success: {} files, {} rows, map={}, duration={}ms",
          numFiles,
          numRows,
          hasMap,
          durationNs / 1_000_000.0);
    }
  }

  public void recordFailedCompaction(long durationNs) {
    stats.recordFailedCompaction(durationNs);
    eventCount.incrementAndGet();

    LOG.warn("Compaction failed: duration={}ms", durationNs / 1_000_000.0);
  }

  public void recordMapBuild(
      int numFiles, long numRows, CompactionMap map, long durationNs) {
    // Estimate map size (simplified - actual size would require serialization)
    int runCount = estimateRunCount(numFiles, numRows);
    long estimatedSize = estimateMapSize(numFiles, runCount);

    stats.recordMapBuild(runCount, estimatedSize, durationNs);

    if (detailedLogging) {
      LOG.debug(
          "Map build: {} files, ~{} runs, ~{}KB, duration={}ms",
          numFiles,
          runCount,
          estimatedSize / 1024,
          durationNs / 1_000_000.0);
    }
  }

  // Initial load
  public void recordInitialLoad(int numFiles, long numRows, long durationNs) {
    stats.recordInitialLoad(numRows);
    eventCount.incrementAndGet();

    LOG.info(
        "Initial load: {} files, {} rows, duration={}ms",
        numFiles,
        numRows,
        durationNs / 1_000_000.0);
  }

  // Add files
  public void recordAddFiles(int numFiles, long numRows) {
    eventCount.incrementAndGet();

    if (detailedLogging) {
      LOG.debug("Added {} files, {} rows", numFiles, numRows);
    }
  }

  // Strategy tracking
  public void recordStrategyUsage(String strategy, long durationNs) {
    stats.recordStrategyUsage(strategy, durationNs);
  }

  // Progress reporting
  public void logProgress() {
    long events = eventCount.get();
    long elapsed = getElapsedTimeMs();
    double rate = elapsed > 0 ? events * 1000.0 / elapsed : 0;

    LOG.info(
        "Progress: {} events in {}ms ({:.1f} events/sec), conflicts: {}, remaps: {}",
        events,
        elapsed,
        rate,
        stats.getConflictedDeletes(),
        stats.getSuccessfulRemaps());
  }

  private int estimateRunCount(int numFiles, long numRows) {
    // With run merging, we typically get 1 run per source file
    // unless there are gaps in the position sequences
    return numFiles;
  }

  private long estimateMapSize(int numFiles, int runCount) {
    // Estimate based on Avro encoding:
    // - Header: ~50 bytes
    // - Per file mapping: ~100 bytes (paths encoded)
    // - Per run: ~24 bytes (3 longs)
    return 50 + (numFiles * 100L) + (runCount * 24L);
  }
}
