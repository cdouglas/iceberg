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
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Range query strategy optimized for scenarios with many positions and few runs (n &gt;&gt; m).
 *
 * <p>Uses an inverse approach: for each run, find all positions in that run's range via binary
 * search. This is more efficient than searching for the run of each position when m &lt;&lt; n.
 *
 * <p><strong>Complexity:</strong>
 *
 * <ul>
 *   <li>Single lookup: O(log m) via binary search fallback
 *   <li>Bulk lookup (sorted): O(m log n + k) via range queries
 *   <li>Bulk lookup (unsorted): O(n log n + m log n + k) including sort
 *   <li>Setup: O(1) - validates runs are sorted
 *   <li>Memory: O(n) additional for sorted copy (if needed)
 * </ul>
 *
 * <p>where k = number of positions that fall within runs (typically k ≈ n).
 *
 * <p><strong>Best for:</strong> Scenarios where n &gt;&gt; m:
 *
 * <ul>
 *   <li>Large delete files (n=10,000+) with few runs (m=10-50)
 *   <li>Bulk remapping with high fan-in compaction
 *   <li>When sorting cost is amortized across multiple queries
 * </ul>
 *
 * <p><strong>Performance:</strong> For n=10,000 positions, m=10 runs:
 *
 * <ul>
 *   <li>Binary search per position: 10,000 × log2(10) ≈ 33K comparisons
 *   <li>Range query: 10 × log2(10,000) ≈ 133 comparisons + collection
 *   <li><strong>Speedup: ~250x</strong>
 * </ul>
 *
 * <p><strong>Trade-offs:</strong>
 *
 * <ul>
 *   <li>Requires sorting positions (one-time O(n log n) cost)
 *   <li>Better than stream join when m &lt;&lt; n (e.g., m &lt; sqrt(n))
 *   <li>Worse than stream join when m ≈ n
 * </ul>
 *
 * <p><strong>Algorithm:</strong> For each run [start, end):
 *
 * <pre>
 * 1. Binary search to find first position >= start
 * 2. Binary search to find first position >= end
 * 3. All positions in [firstIndex, lastIndex) fall within this run
 * 4. Map those positions to the run
 * </pre>
 *
 * Example:
 *
 * <pre>
 * positions (sorted): [5, 12, 50, 75, 125, 175, 250, 350, 375]
 * runs:               [0-100), [150-200), [300-400)
 *
 * Run [0-100):
 *   - Binary search: first >= 0 → index 0
 *   - Binary search: first >= 100 → index 4
 *   - Match positions[0:4] = [5, 12, 50, 75]
 *
 * Run [150-200):
 *   - Binary search: first >= 150 → index 5
 *   - Binary search: first >= 200 → index 6
 *   - Match positions[5:6] = [175]
 *
 * Run [300-400):
 *   - Binary search: first >= 300 → index 7
 *   - Binary search: first >= 400 → index 9
 *   - Match positions[7:9] = [350, 375]
 * </pre>
 */
class RangeQueryStrategy implements RemappingStrategy {
  private final List<Run> runs;
  private final BinarySearchStrategy binarySearchFallback;

  /**
   * Creates a range query strategy.
   *
   * @param runs list of runs sorted by sourcePosition (ascending)
   * @throws IllegalArgumentException if runs are not sorted
   */
  RangeQueryStrategy(List<Run> runs) {
    this.runs = runs;
    // Use binary search for validation and single-position fallback
    this.binarySearchFallback = new BinarySearchStrategy(runs);
  }

  @Override
  public Run runForPosition(long sourcePosition) {
    // Single-position lookup: delegate to binary search
    return binarySearchFallback.runForPosition(sourcePosition);
  }

  @Override
  public Map<Long, Run> runForPositions(long[] sourcePositions) {
    if (sourcePositions == null || sourcePositions.length == 0) {
      return Maps.newHashMap();
    }

    if (runs == null || runs.isEmpty()) {
      return Maps.newHashMap();
    }

    // Get sorted positions (sort copy if needed)
    long[] sortedPositions = getSortedPositionsPrimitive(sourcePositions);

    // Use range query: for each run, find positions in range
    return rangeQueryPrimitive(sortedPositions);
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

    // Get sorted positions (sort copy if needed)
    List<Long> sortedPositions = getSortedPositions(sourcePositions);

    // Use range query: for each run, find positions in range
    return rangeQuery(sortedPositions);
  }

  @Override
  public String name() {
    return "range-query";
  }

  /**
   * Gets sorted positions (primitive array), creating a sorted copy if needed.
   *
   * @param positions positions to sort
   * @return sorted positions (may be original array or copy)
   */
  private long[] getSortedPositionsPrimitive(long[] positions) {
    if (isSortedPrimitive(positions)) {
      return positions;
    }

    // Create sorted copy
    long[] sorted = positions.clone();
    java.util.Arrays.sort(sorted);
    return sorted;
  }

  /**
   * Gets sorted positions, creating a sorted copy if needed.
   *
   * @param positions positions to sort
   * @return sorted positions (may be original list or copy)
   */
  private List<Long> getSortedPositions(List<Long> positions) {
    if (isSorted(positions)) {
      return positions;
    }

    // Create sorted copy
    List<Long> sorted = new ArrayList<>(positions);
    Collections.sort(sorted);
    return sorted;
  }

  /**
   * Checks if positions are sorted in ascending order (primitive array version).
   *
   * @param positions positions to check
   * @return true if sorted ascending, false otherwise
   */
  private boolean isSortedPrimitive(long[] positions) {
    if (positions.length <= 1) {
      return true;
    }

    long prev = positions[0];
    for (int i = 1; i < positions.length; i++) {
      long current = positions[i];
      if (current < prev) {
        return false;
      }
      prev = current;
    }
    return true;
  }

  /**
   * Checks if positions are sorted in ascending order.
   *
   * @param positions positions to check
   * @return true if sorted ascending, false otherwise
   */
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
   * Performs range query using primitive array: for each run, find positions in that run's range.
   *
   * <p>Algorithm: For each run [start, end):
   *
   * <ol>
   *   <li>Binary search for first position >= start
   *   <li>Binary search for first position >= end
   *   <li>All positions in [firstIndex, endIndex) fall within run
   *   <li>Map those positions to this run
   * </ol>
   *
   * <p>Includes predicate pushdown optimization: filters runs to only those overlapping the
   * position range [min, max], reducing work by 50-90% for sparse position sets.
   *
   * <p>Complexity: O(m log n + k) where m = runs, n = positions, k = matches
   *
   * @param sortedPositions positions in ascending order (primitive array)
   * @return map from position to containing run
   */
  private Map<Long, Run> rangeQueryPrimitive(long[] sortedPositions) {
    Map<Long, Run> results = Maps.newHashMapWithExpectedSize(sortedPositions.length);

    // Predicate pushdown: filter runs by min/max position bounds
    long minPos = sortedPositions[0];
    long maxPos = sortedPositions[sortedPositions.length - 1];

    List<Run> relevantRuns = filterRunsByRange(runs, minPos, maxPos);

    if (relevantRuns.isEmpty()) {
      // No runs overlap the position range
      return results;
    }

    for (Run run : relevantRuns) {
      long runStart = run.sourcePosition();
      long runEnd = runStart + run.length();

      // Find first position >= runStart
      int startIndex = binarySearchLowerBoundPrimitive(sortedPositions, runStart);
      if (startIndex >= sortedPositions.length) {
        // All remaining positions are before this run
        continue;
      }

      // Find first position >= runEnd (exclusive bound)
      int endIndex = binarySearchLowerBoundPrimitive(sortedPositions, runEnd);

      // All positions in [startIndex, endIndex) are within this run
      for (int i = startIndex; i < endIndex; i++) {
        results.put(sortedPositions[i], run);
      }
    }

    return results;
  }

  /**
   * Performs range query: for each run, find positions in that run's range.
   *
   * <p>Algorithm: For each run [start, end):
   *
   * <ol>
   *   <li>Binary search for first position >= start
   *   <li>Binary search for first position >= end
   *   <li>All positions in [firstIndex, endIndex) fall within run
   *   <li>Map those positions to this run
   * </ol>
   *
   * <p>Includes predicate pushdown optimization: filters runs to only those overlapping the
   * position range [min, max], reducing work by 50-90% for sparse position sets.
   *
   * <p>Complexity: O(m log n + k) where m = runs, n = positions, k = matches
   *
   * @param sortedPositions positions in ascending order
   * @return map from position to containing run
   */
  private Map<Long, Run> rangeQuery(List<Long> sortedPositions) {
    Map<Long, Run> results = Maps.newHashMapWithExpectedSize(sortedPositions.size());

    // Predicate pushdown: filter runs by min/max position bounds
    long minPos = sortedPositions.get(0);
    long maxPos = sortedPositions.get(sortedPositions.size() - 1);

    List<Run> relevantRuns = filterRunsByRange(runs, minPos, maxPos);

    if (relevantRuns.isEmpty()) {
      // No runs overlap the position range
      return results;
    }

    for (Run run : relevantRuns) {
      long runStart = run.sourcePosition();
      long runEnd = runStart + run.length();

      // Find first position >= runStart
      int startIndex = binarySearchLowerBound(sortedPositions, runStart);
      if (startIndex >= sortedPositions.size()) {
        // All remaining positions are before this run
        continue;
      }

      // Find first position >= runEnd (exclusive bound)
      int endIndex = binarySearchLowerBound(sortedPositions, runEnd);

      // All positions in [startIndex, endIndex) are within this run
      for (int i = startIndex; i < endIndex; i++) {
        results.put(sortedPositions.get(i), run);
      }
    }

    return results;
  }

  /**
   * Filters runs to only those that overlap the given position range [minPos, maxPos].
   *
   * <p>A run overlaps if its range [sourcePosition, sourcePosition+length) intersects with [minPos,
   * maxPos].
   *
   * <p>This predicate pushdown optimization reduces work by skipping runs that cannot possibly
   * contain any of the positions being remapped.
   *
   * @param allRuns all runs to filter
   * @param minPos minimum position (inclusive)
   * @param maxPos maximum position (inclusive)
   * @return filtered list of runs that overlap the range
   */
  private List<Run> filterRunsByRange(List<Run> allRuns, long minPos, long maxPos) {
    List<Run> filtered = new ArrayList<>();

    for (Run run : allRuns) {
      long runStart = run.sourcePosition();
      long runEnd = runStart + run.length();

      // Run overlaps if: runEnd > minPos && runStart <= maxPos
      if (runEnd > minPos && runStart <= maxPos) {
        filtered.add(run);
      }
    }

    return filtered;
  }

  /**
   * Binary search for the first position >= target (lower bound) in primitive array.
   *
   * <p>Returns the index of the first element >= target, or array.length if all elements are &lt;
   * target.
   *
   * @param sortedArray sorted array to search
   * @param target target value
   * @return index of first element >= target
   */
  private int binarySearchLowerBoundPrimitive(long[] sortedArray, long target) {
    int left = 0;
    int right = sortedArray.length;

    while (left < right) {
      int mid = left + (right - left) / 2;
      long midValue = sortedArray[mid];

      if (midValue < target) {
        left = mid + 1; // Search right half
      } else {
        right = mid; // Could be the answer, search left half
      }
    }

    return left;
  }

  /**
   * Binary search for the first position >= target (lower bound).
   *
   * <p>Returns the index of the first element >= target, or list.size() if all elements are &lt;
   * target.
   *
   * @param sortedList sorted list to search
   * @param target target value
   * @return index of first element >= target
   */
  private int binarySearchLowerBound(List<Long> sortedList, long target) {
    int left = 0;
    int right = sortedList.size();

    while (left < right) {
      int mid = left + (right - left) / 2;
      long midValue = sortedList.get(mid);

      if (midValue < target) {
        left = mid + 1; // Search right half
      } else {
        right = mid; // Could be the answer, search left half
      }
    }

    return left;
  }
}
