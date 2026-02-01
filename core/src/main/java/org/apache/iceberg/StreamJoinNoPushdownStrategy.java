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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stream join strategy WITHOUT predicate pushdown optimization.
 *
 * <p><strong>WARNING: This strategy is for benchmarking purposes only.</strong> It deliberately
 * disables the predicate pushdown optimization to measure its impact. Do not use in production.
 *
 * <p>To use this strategy, you must explicitly enable it via {@link
 * #enableForBenchmarking(boolean)}. By default, this strategy will throw an exception if used
 * without enabling.
 *
 * <p>This strategy processes ALL runs regardless of whether they overlap with the position range,
 * which can be 2-10x slower than the optimized version for sparse position sets.
 */
class StreamJoinNoPushdownStrategy implements RemappingStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(StreamJoinNoPushdownStrategy.class);

  private static final AtomicBoolean BENCHMARK_MODE_ENABLED = new AtomicBoolean(false);
  private static final AtomicBoolean WARNING_LOGGED = new AtomicBoolean(false);

  private final List<Run> runs;
  private final BinarySearchStrategy binarySearchFallback;

  /**
   * Enables or disables benchmark mode for this strategy.
   *
   * <p><strong>WARNING:</strong> Only enable this for benchmarking purposes. This strategy is
   * intentionally slower than the production version.
   *
   * @param enabled true to enable benchmark mode, false to disable
   */
  public static void enableForBenchmarking(boolean enabled) {
    BENCHMARK_MODE_ENABLED.set(enabled);
    WARNING_LOGGED.set(false); // Reset warning so it logs again if re-enabled
    if (enabled) {
      LOG.warn(
          "StreamJoinNoPushdownStrategy ENABLED - this disables predicate pushdown optimization. "
              + "Only use for benchmarking, not production workloads.");
    }
  }

  /** Returns true if benchmark mode is enabled. */
  public static boolean isBenchmarkModeEnabled() {
    return BENCHMARK_MODE_ENABLED.get();
  }

  /**
   * Creates a stream join strategy without predicate pushdown.
   *
   * @param runs list of runs sorted by sourcePosition (ascending)
   * @throws IllegalStateException if benchmark mode is not enabled
   * @throws IllegalArgumentException if runs are not sorted
   */
  StreamJoinNoPushdownStrategy(List<Run> runs) {
    if (!BENCHMARK_MODE_ENABLED.get()) {
      throw new IllegalStateException(
          "StreamJoinNoPushdownStrategy is disabled. This strategy is for benchmarking only. "
              + "Call StreamJoinNoPushdownStrategy.enableForBenchmarking(true) to enable.");
    }

    if (!WARNING_LOGGED.getAndSet(true)) {
      LOG.warn(
          "Using StreamJoinNoPushdownStrategy - predicate pushdown is DISABLED. "
              + "Performance will be degraded compared to StreamJoinStrategy.");
    }

    this.runs = runs;
    this.binarySearchFallback = new BinarySearchStrategy(runs);
  }

  @Override
  public Run runForPosition(long sourcePosition) {
    return binarySearchFallback.runForPosition(sourcePosition);
  }

  @Override
  public Map<Long, Run> runForPositions(List<Long> sourcePositions) {
    if (sourcePositions == null || sourcePositions.isEmpty()) {
      return Maps.newHashMap();
    }

    if (isSorted(sourcePositions)) {
      return streamJoinNoPushdown(sourcePositions);
    } else {
      return RemappingStrategy.super.runForPositions(sourcePositions);
    }
  }

  @Override
  public String name() {
    return "stream-join-no-pushdown";
  }

  /**
   * Performs stream join WITHOUT predicate pushdown.
   *
   * <p>Unlike the optimized version, this processes ALL runs regardless of whether they overlap
   * with the position range. This is intentionally slower to measure the impact of predicate
   * pushdown.
   *
   * @param sortedPositions positions in ascending order
   * @return map from position to containing run
   */
  private Map<Long, Run> streamJoinNoPushdown(List<Long> sortedPositions) {
    Map<Long, Run> results = Maps.newHashMapWithExpectedSize(sortedPositions.size());

    if (runs == null || runs.isEmpty()) {
      return results;
    }

    // NO PREDICATE PUSHDOWN: Process all runs, not just relevant ones
    int runIndex = 0;
    Run currentRun = runs.get(0);
    long currentRunEnd = currentRun.sourcePosition() + currentRun.length();

    for (Long position : sortedPositions) {
      while (runIndex < runs.size() && position >= currentRunEnd) {
        runIndex++;
        if (runIndex < runs.size()) {
          currentRun = runs.get(runIndex);
          currentRunEnd = currentRun.sourcePosition() + currentRun.length();
        }
      }

      if (runIndex >= runs.size()) {
        break;
      }

      long runStart = currentRun.sourcePosition();
      if (position >= runStart && position < currentRunEnd) {
        results.put(position, currentRun);
      }
    }

    return results;
  }

  private boolean isSorted(List<Long> positions) {
    if (positions.size() <= 1) {
      return true;
    }

    long prev = positions.get(0);
    for (int i = 1; i < positions.size(); i++) {
      long current = positions.get(i);
      if (current < prev) {
        return false;
      }
      prev = current;
    }
    return true;
  }
}
