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
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Strategy for finding which run contains a given source position.
 *
 * <p>Different strategies have different complexity characteristics and are optimal for different
 * scenarios:
 *
 * <ul>
 *   <li>{@link LinearSearchStrategy}: O(m) lookup, no setup cost. Best for m &lt; 10.
 *   <li>{@link BinarySearchStrategy}: O(log m) lookup, no setup cost. Best for 10 &lt;= m &lt;
 *       100.
 *   <li>{@link IntervalTreeStrategy}: O(log m) lookup, O(m) setup. Best for m &gt;= 100.
 *   <li>{@link StreamJoinStrategy}: O(n + m) bulk lookup for sorted positions. Best for bulk
 *       remapping when m ≈ n.
 *   <li>{@link RangeQueryStrategy}: O(m log n) bulk lookup for sorted positions. Best when n
 *       &gt;&gt; m (high fan-in).
 * </ul>
 *
 * <p>where m = number of runs, n = number of positions to look up.
 */
interface RemappingStrategy {

  /**
   * Finds the run containing the given source position.
   *
   * @param sourcePosition the position to look up
   * @return the run containing this position, or null if position is in a gap
   */
  Run runForPosition(long sourcePosition);

  /**
   * Finds runs for multiple source positions (bulk lookup).
   *
   * <p>Default implementation calls {@link #runForPosition(long)} for each position. Strategies
   * can override this for better performance.
   *
   * <p><strong>Performance:</strong>
   *
   * <ul>
   *   <li>Default: O(n * complexity of single lookup)
   *   <li>Optimized: O(n + m) if positions are sorted (stream join)
   * </ul>
   *
   * @param sourcePositions positions to look up (sorted for best performance)
   * @return map from position to containing run (missing entries = gaps)
   */
  default Map<Long, Run> runForPositions(List<Long> sourcePositions) {
    Map<Long, Run> results = Maps.newHashMapWithExpectedSize(sourcePositions.size());
    for (Long position : sourcePositions) {
      Run run = runForPosition(position);
      if (run != null) {
        results.put(position, run);
      }
    }
    return results;
  }

  /**
   * Returns the name of this strategy for diagnostic purposes.
   *
   * @return strategy name (e.g., "linear-search", "binary-search", "stream-join")
   */
  String name();

  /**
   * Factory for creating appropriate strategy based on run characteristics.
   */
  class Factory {
    // Thresholds for algorithm selection
    private static final int BINARY_SEARCH_THRESHOLD = 10;
    private static final int INTERVAL_TREE_THRESHOLD = 100;

    /**
     * Creates the optimal strategy for the given runs.
     *
     * <p>Algorithm selection:
     *
     * <ul>
     *   <li>m &lt; 10: Linear search (simple, no overhead)
     *   <li>10 &lt;= m &lt; 100: Binary search (fast, minimal overhead)
     *   <li>m &gt;= 100: Interval tree (optimal for large m, better cache locality)
     * </ul>
     *
     * @param runs list of runs to search (must be sorted by sourcePosition)
     * @return optimal remapping strategy
     */
    public static RemappingStrategy create(List<Run> runs) {
      if (runs == null || runs.isEmpty()) {
        return new LinearSearchStrategy(runs);
      }

      if (runs.size() < BINARY_SEARCH_THRESHOLD) {
        return new LinearSearchStrategy(runs);
      }

      if (runs.size() < INTERVAL_TREE_THRESHOLD) {
        return new BinarySearchStrategy(runs);
      }

      return new IntervalTreeStrategy(runs);
    }
  }
}
