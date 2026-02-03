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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Range query strategy WITHOUT predicate pushdown optimization.
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
class RangeQueryNoPushdownStrategy implements RemappingStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(RangeQueryNoPushdownStrategy.class);

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
          "RangeQueryNoPushdownStrategy ENABLED - this disables predicate pushdown optimization. "
              + "Only use for benchmarking, not production workloads.");
    }
  }

  /** Returns true if benchmark mode is enabled. */
  public static boolean isBenchmarkModeEnabled() {
    return BENCHMARK_MODE_ENABLED.get();
  }

  /**
   * Creates a range query strategy without predicate pushdown.
   *
   * @param runs list of runs sorted by sourcePosition (ascending)
   * @throws IllegalStateException if benchmark mode is not enabled
   * @throws IllegalArgumentException if runs are not sorted
   */
  RangeQueryNoPushdownStrategy(List<Run> runs) {
    if (!BENCHMARK_MODE_ENABLED.get()) {
      throw new IllegalStateException(
          "RangeQueryNoPushdownStrategy is disabled. This strategy is for benchmarking only. "
              + "Call RangeQueryNoPushdownStrategy.enableForBenchmarking(true) to enable.");
    }

    if (!WARNING_LOGGED.getAndSet(true)) {
      LOG.warn(
          "Using RangeQueryNoPushdownStrategy - predicate pushdown is DISABLED. "
              + "Performance will be degraded compared to RangeQueryStrategy.");
    }

    this.runs = runs;
    this.binarySearchFallback = new BinarySearchStrategy(runs);
  }

  @Override
  public Run runForPosition(long sourcePosition) {
    return binarySearchFallback.runForPosition(sourcePosition);
  }

  @SuppressWarnings("deprecation")
  @Override
  public Map<Long, Run> runForPositions(List<Long> sourcePositions) {
    if (sourcePositions == null || sourcePositions.isEmpty()) {
      return Maps.newHashMap();
    }

    if (runs == null || runs.isEmpty()) {
      return Maps.newHashMap();
    }

    List<Long> sortedPositions = getSortedPositions(sourcePositions);
    return rangeQueryNoPushdown(sortedPositions);
  }

  @Override
  public String name() {
    return "range-query-no-pushdown";
  }

  private List<Long> getSortedPositions(List<Long> positions) {
    if (isSorted(positions)) {
      return positions;
    }

    List<Long> sorted = new ArrayList<>(positions);
    Collections.sort(sorted);
    return sorted;
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

  /**
   * Performs range query WITHOUT predicate pushdown.
   *
   * <p>Unlike the optimized version, this processes ALL runs regardless of whether they overlap
   * with the position range. This is intentionally slower to measure the impact of predicate
   * pushdown.
   *
   * @param sortedPositions positions in ascending order
   * @return map from position to containing run
   */
  private Map<Long, Run> rangeQueryNoPushdown(List<Long> sortedPositions) {
    Map<Long, Run> results = Maps.newHashMapWithExpectedSize(sortedPositions.size());

    // NO PREDICATE PUSHDOWN: Process all runs, not just relevant ones
    for (Run run : runs) {
      long runStart = run.sourcePosition();
      long runEnd = runStart + run.length();

      int startIndex = binarySearchLowerBound(sortedPositions, runStart);
      if (startIndex >= sortedPositions.size()) {
        continue;
      }

      int endIndex = binarySearchLowerBound(sortedPositions, runEnd);

      for (int i = startIndex; i < endIndex; i++) {
        results.put(sortedPositions.get(i), run);
      }
    }

    return results;
  }

  private int binarySearchLowerBound(List<Long> sortedList, long target) {
    int left = 0;
    int right = sortedList.size();

    while (left < right) {
      int mid = left + (right - left) / 2;
      long midValue = sortedList.get(mid);

      if (midValue < target) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }

    return left;
  }
}
