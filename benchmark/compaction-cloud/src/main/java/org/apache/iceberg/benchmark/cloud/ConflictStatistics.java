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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects and aggregates statistics about conflicts during benchmark execution.
 *
 * <p>Thread-safe for concurrent updates from multiple transaction executors.
 */
public class ConflictStatistics {

  // Transaction counts
  @JsonProperty("total-deletes")
  private final AtomicLong totalDeletes = new AtomicLong();

  @JsonProperty("successful-deletes")
  private final AtomicLong successfulDeletes = new AtomicLong();

  @JsonProperty("failed-deletes")
  private final AtomicLong failedDeletes = new AtomicLong();

  @JsonProperty("conflicted-deletes")
  private final AtomicLong conflictedDeletes = new AtomicLong();

  @JsonProperty("successful-remaps")
  private final AtomicLong successfulRemaps = new AtomicLong();

  @JsonProperty("failed-remaps")
  private final AtomicLong failedRemaps = new AtomicLong();

  // Compaction counts
  @JsonProperty("total-compactions")
  private final AtomicLong totalCompactions = new AtomicLong();

  @JsonProperty("successful-compactions")
  private final AtomicLong successfulCompactions = new AtomicLong();

  @JsonProperty("failed-compactions")
  private final AtomicLong failedCompactions = new AtomicLong();

  @JsonProperty("compactions-with-maps")
  private final AtomicLong compactionsWithMaps = new AtomicLong();

  // Timing (nanoseconds)
  @JsonProperty("total-delete-time-ns")
  private final AtomicLong totalDeleteTimeNs = new AtomicLong();

  @JsonProperty("total-compaction-time-ns")
  private final AtomicLong totalCompactionTimeNs = new AtomicLong();

  @JsonProperty("total-remap-time-ns")
  private final AtomicLong totalRemapTimeNs = new AtomicLong();

  @JsonProperty("total-map-build-time-ns")
  private final AtomicLong totalMapBuildTimeNs = new AtomicLong();

  // Latency distributions (stored as nanoseconds)
  private final CopyOnWriteArrayList<Long> deleteLatencies = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<Long> remapLatencies = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<Long> compactionLatencies = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<Long> mapBuildLatencies = new CopyOnWriteArrayList<>();

  // Map efficiency
  @JsonProperty("total-map-size-bytes")
  private final AtomicLong totalMapSizeBytes = new AtomicLong();

  private final CopyOnWriteArrayList<Long> compactionMapSizes = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<Integer> runCounts = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<Integer> filesPerCompaction = new CopyOnWriteArrayList<>();

  // Row counts
  @JsonProperty("total-rows-compacted")
  private final AtomicLong totalRowsCompacted = new AtomicLong();

  @JsonProperty("total-rows-loaded")
  private final AtomicLong totalRowsLoaded = new AtomicLong();

  // Per-strategy metrics (if detailed stats enabled)
  private final ConcurrentHashMap<String, StrategyMetrics> strategyMetrics =
      new ConcurrentHashMap<>();

  // Record delete operations
  public void recordSuccessfulDelete(long durationNs) {
    totalDeletes.incrementAndGet();
    successfulDeletes.incrementAndGet();
    totalDeleteTimeNs.addAndGet(durationNs);
    deleteLatencies.add(durationNs);
  }

  public void recordFailedDelete(long durationNs) {
    totalDeletes.incrementAndGet();
    failedDeletes.incrementAndGet();
    totalDeleteTimeNs.addAndGet(durationNs);
    deleteLatencies.add(durationNs);
  }

  public void recordConflict() {
    conflictedDeletes.incrementAndGet();
  }

  public void recordRemapAttempt(boolean success, long durationNs) {
    if (success) {
      successfulRemaps.incrementAndGet();
    } else {
      failedRemaps.incrementAndGet();
    }
    totalRemapTimeNs.addAndGet(durationNs);
    remapLatencies.add(durationNs);
  }

  // Record compaction operations
  public void recordSuccessfulCompaction(int numFiles, long numRows, long durationNs, boolean hasMap) {
    totalCompactions.incrementAndGet();
    successfulCompactions.incrementAndGet();
    totalCompactionTimeNs.addAndGet(durationNs);
    totalRowsCompacted.addAndGet(numRows);
    compactionLatencies.add(durationNs);
    filesPerCompaction.add(numFiles);

    if (hasMap) {
      compactionsWithMaps.incrementAndGet();
    }
  }

  public void recordFailedCompaction(long durationNs) {
    totalCompactions.incrementAndGet();
    failedCompactions.incrementAndGet();
    totalCompactionTimeNs.addAndGet(durationNs);
    compactionLatencies.add(durationNs);
  }

  public void recordMapBuild(int numRuns, long mapSizeBytes, long durationNs) {
    totalMapBuildTimeNs.addAndGet(durationNs);
    totalMapSizeBytes.addAndGet(mapSizeBytes);
    mapBuildLatencies.add(durationNs);
    runCounts.add(numRuns);
    compactionMapSizes.add(mapSizeBytes);
  }

  public void recordInitialLoad(long numRows) {
    totalRowsLoaded.addAndGet(numRows);
  }

  public void recordStrategyUsage(String strategy, long durationNs) {
    strategyMetrics
        .computeIfAbsent(strategy, k -> new StrategyMetrics())
        .record(durationNs);
  }

  // Getters for statistics
  public long getTotalDeletes() {
    return totalDeletes.get();
  }

  public long getSuccessfulDeletes() {
    return successfulDeletes.get();
  }

  public long getFailedDeletes() {
    return failedDeletes.get();
  }

  public long getConflictedDeletes() {
    return conflictedDeletes.get();
  }

  public long getSuccessfulRemaps() {
    return successfulRemaps.get();
  }

  public long getFailedRemaps() {
    return failedRemaps.get();
  }

  public long getTotalCompactions() {
    return totalCompactions.get();
  }

  public long getSuccessfulCompactions() {
    return successfulCompactions.get();
  }

  public long getCompactionsWithMaps() {
    return compactionsWithMaps.get();
  }

  public long getTotalRowsCompacted() {
    return totalRowsCompacted.get();
  }

  public long getTotalRowsLoaded() {
    return totalRowsLoaded.get();
  }

  // Computed statistics
  public double getConflictRate() {
    long total = totalDeletes.get();
    return total > 0 ? (double) conflictedDeletes.get() / total : 0.0;
  }

  public double getRemapSuccessRate() {
    long conflicts = conflictedDeletes.get();
    return conflicts > 0 ? (double) successfulRemaps.get() / conflicts : 0.0;
  }

  public double getAvgDeleteLatencyMs() {
    long total = totalDeletes.get();
    return total > 0 ? totalDeleteTimeNs.get() / total / 1_000_000.0 : 0.0;
  }

  public double getAvgRemapLatencyMs() {
    long remaps = successfulRemaps.get() + failedRemaps.get();
    return remaps > 0 ? totalRemapTimeNs.get() / remaps / 1_000_000.0 : 0.0;
  }

  public double getAvgCompactionLatencyMs() {
    long total = totalCompactions.get();
    return total > 0 ? totalCompactionTimeNs.get() / total / 1_000_000.0 : 0.0;
  }

  public double getAvgMapBuildLatencyMs() {
    long maps = compactionsWithMaps.get();
    return maps > 0 ? totalMapBuildTimeNs.get() / maps / 1_000_000.0 : 0.0;
  }

  public double getAvgMapSizeKB() {
    long maps = compactionsWithMaps.get();
    return maps > 0 ? totalMapSizeBytes.get() / maps / 1024.0 : 0.0;
  }

  public double getAvgRunCount() {
    return runCounts.isEmpty() ? 0.0 : runCounts.stream().mapToInt(i -> i).average().orElse(0.0);
  }

  public double getAvgFilesPerCompaction() {
    return filesPerCompaction.isEmpty()
        ? 0.0
        : filesPerCompaction.stream().mapToInt(i -> i).average().orElse(0.0);
  }

  // Percentile calculations
  public double getDeleteLatencyPercentile(int percentile) {
    return calculatePercentile(new ArrayList<>(deleteLatencies), percentile) / 1_000_000.0;
  }

  public double getRemapLatencyPercentile(int percentile) {
    return calculatePercentile(new ArrayList<>(remapLatencies), percentile) / 1_000_000.0;
  }

  private double calculatePercentile(List<Long> values, int percentile) {
    if (values.isEmpty()) {
      return 0.0;
    }
    Collections.sort(values);
    int index = (int) Math.ceil(percentile / 100.0 * values.size()) - 1;
    return values.get(Math.max(0, Math.min(index, values.size() - 1)));
  }

  public Map<String, StrategyMetrics> getStrategyMetrics() {
    return new ConcurrentHashMap<>(strategyMetrics);
  }

  /** Per-strategy metrics for remapping. */
  public static class StrategyMetrics {
    private final AtomicLong count = new AtomicLong();
    private final AtomicLong totalTimeNs = new AtomicLong();

    void record(long durationNs) {
      count.incrementAndGet();
      totalTimeNs.addAndGet(durationNs);
    }

    public long getCount() {
      return count.get();
    }

    public double getAvgLatencyMs() {
      long c = count.get();
      return c > 0 ? totalTimeNs.get() / c / 1_000_000.0 : 0.0;
    }
  }

  /** Reset all statistics. */
  public void reset() {
    totalDeletes.set(0);
    successfulDeletes.set(0);
    failedDeletes.set(0);
    conflictedDeletes.set(0);
    successfulRemaps.set(0);
    failedRemaps.set(0);
    totalCompactions.set(0);
    successfulCompactions.set(0);
    failedCompactions.set(0);
    compactionsWithMaps.set(0);
    totalDeleteTimeNs.set(0);
    totalCompactionTimeNs.set(0);
    totalRemapTimeNs.set(0);
    totalMapBuildTimeNs.set(0);
    totalMapSizeBytes.set(0);
    totalRowsCompacted.set(0);
    totalRowsLoaded.set(0);
    deleteLatencies.clear();
    remapLatencies.clear();
    compactionLatencies.clear();
    mapBuildLatencies.clear();
    compactionMapSizes.clear();
    runCounts.clear();
    filesPerCompaction.clear();
    strategyMetrics.clear();
  }
}
