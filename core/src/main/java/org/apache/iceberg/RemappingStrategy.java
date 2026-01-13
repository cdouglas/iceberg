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
import org.apache.iceberg.CompactionMap.Run;

/**
 * Strategy for finding which run contains a given source position.
 *
 * <p>Different strategies have different complexity characteristics and are optimal for different
 * scenarios:
 *
 * <ul>
 *   <li>{@link LinearSearchStrategy}: O(m) lookup, no setup cost. Best for m &lt; 10.
 *   <li>{@link BinarySearchStrategy}: O(log m) lookup, no setup cost. Best for m &lt; 100.
 *   <li>IntervalTreeStrategy: O(log m) lookup, O(m log m) setup. Best for m &gt; 100.
 * </ul>
 *
 * <p>where m = number of runs in the compaction map.
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
   * Returns the name of this strategy for diagnostic purposes.
   *
   * @return strategy name (e.g., "linear-search", "binary-search")
   */
  String name();

  /**
   * Factory for creating appropriate strategy based on run characteristics.
   */
  class Factory {
    // Threshold for switching from linear to binary search
    private static final int BINARY_SEARCH_THRESHOLD = 10;

    /**
     * Creates the optimal strategy for the given runs.
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

      // For now, use binary search for larger run counts
      // Future: add interval tree for m > 100
      return new BinarySearchStrategy(runs);
    }
  }
}
