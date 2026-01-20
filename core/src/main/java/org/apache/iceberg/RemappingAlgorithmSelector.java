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
import org.apache.iceberg.CompactionMap.FileMapping;
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Selects optimal remapping strategy based on runtime data characteristics.
 *
 * <p>Decision factors:
 *
 * <ul>
 *   <li>m = number of runs in mapping
 *   <li>n = number of positions to remap
 *   <li>sorted = whether positions are sorted (detected via sampling)
 *   <li>gapRatio = percentage of source range not covered by runs
 * </ul>
 *
 * <p>Selection rules:
 *
 * <ul>
 *   <li>m < 10, sorted: Use RangeQuery (optimal for few runs)
 *   <li>m < 10, unsorted: Use BinarySearch (no sorting overhead)
 *   <li>n/m > 100 with gaps: Use RangeQuery (high fan-in with sparsity)
 *   <li>m < 100, sorted && n > m: Use StreamJoin (optimal for sorted bulk)
 *   <li>m < 100, otherwise: Use BinarySearch (simple and fast)
 *   <li>m >= 100: Use IntervalTree (optimal for many runs)
 * </ul>
 */
public class RemappingAlgorithmSelector {

  private static final int FEW_RUNS_THRESHOLD = 10;
  private static final int BINARY_SEARCH_THRESHOLD = 100;
  private static final int HIGH_FAN_IN_THRESHOLD = 100; // n/m ratio
  private static final double SIGNIFICANT_GAPS_THRESHOLD = 0.3;
  private static final int SORTEDNESS_SAMPLE_SIZE = 1000;

  /**
   * Selects optimal remapping strategy for bulk remapping.
   *
   * @param mapping the file mapping to use
   * @param positions the positions to remap
   * @return optimal remapping strategy
   */
  public RemappingStrategy selectOptimal(FileMapping mapping, List<Long> positions) {
    Preconditions.checkNotNull(mapping, "mapping is null");
    Preconditions.checkNotNull(positions, "positions is null");

    List<Run> runs = mapping.runs();
    int m = runs.size();
    int n = positions.size();

    if (n == 0 || m == 0) {
      return new LinearSearchStrategy(runs);
    }

    // Very few runs: choice depends on sortedness
    // RangeQuery is optimal for sorted data (O(m log n) with no sorting overhead)
    // For unsorted data, RangeQuery requires O(n log n) sorting, making BinarySearch better
    if (m < FEW_RUNS_THRESHOLD) {
      boolean sorted = isSorted(positions);
      if (sorted) {
        return new RangeQueryStrategy(runs);
      } else {
        // BinarySearch is O(n log m) and works efficiently on unsorted data
        return new BinarySearchStrategy(runs);
      }
    }

    // High fan-in (many positions per run)
    if (n / m > HIGH_FAN_IN_THRESHOLD) {
      double gapRatio = estimateGapRatio(mapping);

      if (gapRatio > SIGNIFICANT_GAPS_THRESHOLD) {
        // Sparse runs with high fan-in: RangeQuery optimal
        return new RangeQueryStrategy(runs);
      }

      // Dense runs: compare costs
      long indexCost = (long) n * log2(n);
      long lookupCost = (long) n * log2(m);

      if (indexCost + n < lookupCost) {
        return new RangeQueryStrategy(runs);
      }
    }

    // Medium number of runs: choose based on sortedness
    if (m < BINARY_SEARCH_THRESHOLD) {
      boolean sorted = isSorted(positions);

      if (sorted && n > m) {
        // Sorted bulk remapping: StreamJoin optimal for m < 100
        return new StreamJoinStrategy(runs);
      }

      // Unsorted or small n: BinarySearch sufficient
      return new BinarySearchStrategy(runs);
    }

    // Large number of runs (m >= 100): IntervalTree always optimal
    // IntervalTree outperforms StreamJoin at high m due to better cache locality
    return new IntervalTreeStrategy(runs);
  }

  /**
   * Estimates what percentage of the source range is NOT covered by runs.
   *
   * @param mapping the file mapping
   * @return gap ratio (0.0 = no gaps, 1.0 = all gaps)
   */
  double estimateGapRatio(FileMapping mapping) {
    List<Run> runs = mapping.runs();

    if (runs.isEmpty()) {
      return 1.0; // 100% gaps
    }

    long minPos = Long.MAX_VALUE;
    long maxPos = Long.MIN_VALUE;
    long coveredLength = 0;

    for (Run run : runs) {
      long runStart = run.sourcePosition();
      long runEnd = runStart + run.length();

      minPos = Math.min(minPos, runStart);
      maxPos = Math.max(maxPos, runEnd);
      coveredLength += run.length();
    }

    long totalRange = maxPos - minPos;

    if (totalRange == 0) {
      return 0.0; // No gaps (single point)
    }

    return 1.0 - ((double) coveredLength / totalRange);
  }

  /**
   * Checks if positions are sorted by sampling.
   *
   * @param positions the positions to check
   * @return true if positions appear to be sorted
   */
  boolean isSorted(List<Long> positions) {
    if (positions.size() <= 1) {
      return true;
    }

    // Sample first N positions to check sortedness
    int sampleSize = Math.min(SORTEDNESS_SAMPLE_SIZE, positions.size());
    long prev = positions.get(0);

    for (int i = 1; i < sampleSize; i++) {
      long current = positions.get(i);
      if (current < prev) {
        return false; // Found out-of-order element
      }
      prev = current;
    }

    return true;
  }

  private static int log2(long n) {
    if (n <= 1) {
      return 1;
    }
    return 64 - Long.numberOfLeadingZeros(n - 1);
  }
}
