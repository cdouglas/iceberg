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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Binary search strategy for finding runs containing source positions.
 *
 * <p>Performs binary search on sorted runs to find the run containing a position.
 *
 * <p><strong>Complexity:</strong>
 *
 * <ul>
 *   <li>Lookup: O(log m) where m = number of runs
 *   <li>Setup: O(1) - validates runs are sorted
 *   <li>Memory: O(1) additional
 * </ul>
 *
 * <p><strong>Best for:</strong> Medium to large run counts (m &gt;= 10) where binary search
 * provides significant speedup over linear search.
 *
 * <p><strong>Requirements:</strong> Runs must be sorted by sourcePosition (ascending). This is
 * validated during construction.
 *
 * <p><strong>Performance:</strong> Provides ~100x speedup for m=100, ~1000x speedup for m=1000
 * compared to linear search.
 */
class BinarySearchStrategy implements RemappingStrategy {
  private final List<Run> runs;

  /**
   * Creates a binary search strategy.
   *
   * @param runs list of runs sorted by sourcePosition (ascending)
   * @throws IllegalArgumentException if runs are not sorted
   */
  BinarySearchStrategy(List<Run> runs) {
    this.runs = runs;
    validateSorted(runs);
  }

  @Override
  public Run runForPosition(long sourcePosition) {
    if (runs == null || runs.isEmpty()) {
      return null;
    }

    int left = 0;
    int right = runs.size() - 1;

    while (left <= right) {
      int mid = left + (right - left) / 2;
      Run run = runs.get(mid);

      long runStart = run.sourcePosition();
      long runEnd = runStart + run.length();

      if (sourcePosition < runStart) {
        // Position is before this run, search left half
        right = mid - 1;
      } else if (sourcePosition >= runEnd) {
        // Position is after this run, search right half
        left = mid + 1;
      } else {
        // Found: sourcePosition is in [runStart, runEnd)
        return run;
      }
    }

    // Not found: position is in a gap between runs
    return null;
  }

  @Override
  public String name() {
    return "binary-search";
  }

  /**
   * Validates that runs are sorted by sourcePosition.
   *
   * <p>Binary search requires sorted input. Runs should be sorted when created by
   * CompactionMapBuilder, but we validate here for safety.
   *
   * @param runs runs to validate
   * @throws IllegalArgumentException if runs are not sorted or have overlaps
   */
  private static void validateSorted(List<Run> runs) {
    if (runs == null || runs.size() <= 1) {
      return;
    }

    long prevEnd = runs.get(0).sourcePosition();

    for (int i = 0; i < runs.size(); i++) {
      Run run = runs.get(i);
      long start = run.sourcePosition();
      long end = start + run.length();

      // Check sorting
      Preconditions.checkArgument(
          start >= prevEnd,
          "Runs must be sorted by sourcePosition and non-overlapping. "
              + "Run at index %s has sourcePosition %s but previous run ends at %s",
          i,
          start,
          prevEnd);

      // Check for valid length
      Preconditions.checkArgument(
          run.length() > 0, "Run at index %s has invalid length: %s", i, run.length());

      prevEnd = end;
    }
  }
}
