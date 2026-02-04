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
 * Selects optimal remapping strategy based on empirical benchmark data.
 *
 * <p>Selection is based on JMH benchmarks run January-February 2026 across 324+ configurations
 * testing all combinations of:
 *
 * <ul>
 *   <li>m (runs): 10, 100, 1000, 10000
 *   <li>n (positions): 1000, 10000, 100000
 *   <li>gapRatio: 0.0, 0.3, 0.5
 *   <li>sorted: true, false
 * </ul>
 *
 * <p>Key empirical findings:
 *
 * <ul>
 *   <li>UNSORTED data: IntervalTree wins 29/36 scenarios, but StreamJoin wins when m >> n
 *   <li>SORTED data: RangeQuery or StreamJoin win; IntervalTree never wins
 *   <li>SORTED + sparse (gap > 0.3): RangeQuery optimal (can skip gaps)
 *   <li>SORTED + dense (gap <= 0.3) + n >= 10000 + m >= 100: StreamJoin optimal
 *   <li>Very high m (>=5000) with small n (<=2000): StreamJoin optimal regardless of sorted/gaps
 *   <li>BinarySearch rarely wins (edge cases only, not worth selecting)
 * </ul>
 */
public class RemappingAlgorithmSelector {

  private static final double SPARSE_GAP_THRESHOLD = 0.3;
  private static final int BULK_POSITION_THRESHOLD = 10000;
  private static final int MANY_RUNS_THRESHOLD = 100;
  private static final int SORTEDNESS_SAMPLE_SIZE = 1000;

  // Thresholds for very high m scenarios (Feb 2026 hyperparallel benchmarks)
  private static final int VERY_HIGH_RUNS_THRESHOLD = 5000;
  private static final int SMALL_POSITIONS_THRESHOLD = 2000;

  /**
   * Selects optimal remapping strategy based on data characteristics (primitive array version).
   *
   * <p>The selection logic is derived from empirical benchmarks, not theoretical complexity:
   *
   * <pre>
   * if unsorted:
   *     if m >= 5000 AND n <= 2000:
   *         return StreamJoin      # Very high m: O(n+m) beats tree lookups
   *     return IntervalTree        # Wins 29/36 unsorted scenarios
   *
   * # Sorted data below
   * if m >= 5000 AND n <= 2000:
   *     return StreamJoin          # Very high m with small n
   *
   * if gapRatio > 0.3:
   *     return RangeQuery          # Sparse data: skip gaps efficiently
   *
   * if n >= 10000 AND m >= 100:
   *     return StreamJoin          # Dense sorted bulk: O(n+m) linear scan wins
   *
   * return RangeQuery              # Default for sorted: O(m log n)
   * </pre>
   *
   * @param mapping the file mapping containing runs
   * @param positions the positions to remap as primitive array
   * @return the optimal remapping strategy
   */
  public RemappingStrategy selectOptimal(FileMapping mapping, long[] positions) {
    Preconditions.checkNotNull(mapping, "mapping is null");
    Preconditions.checkNotNull(positions, "positions is null");

    List<Run> runs = mapping.runs();
    int m = runs.size();
    int n = positions.length;

    // Edge cases
    if (n == 0 || m == 0) {
      return new LinearSearchStrategy(runs);
    }

    // Check sortedness first - this is the primary decision factor
    boolean sorted = isSortedPrimitive(positions);

    // UNSORTED handling
    // Benchmark evidence (Feb 2026): IntervalTree wins 29/36 unsorted scenarios,
    // but StreamJoin wins when m is very high and n is small (m >> n)
    if (!sorted) {
      // When m >> n (very high runs, small positions), StreamJoin's O(n+m) beats
      // IntervalTree's O(n log m) because the linear scan through runs dominates
      // Benchmark: n=1000, m=10000, unsorted → StreamJoin 165us vs IntervalTree 465us
      if (m >= VERY_HIGH_RUNS_THRESHOLD && n <= SMALL_POSITIONS_THRESHOLD) {
        return new StreamJoinStrategy(runs);
      }
      return new IntervalTreeStrategy(runs);
    }

    // SORTED data below - IntervalTree never wins for sorted data

    // Very high m with small n: StreamJoin is safest choice
    // Benchmark (Feb 2026): n=1000, m=10000, sorted → StreamJoin within 37% of optimal
    // vs RangeQuery up to 63% overhead. StreamJoin's O(n+m) handles high m well.
    if (m >= VERY_HIGH_RUNS_THRESHOLD && n <= SMALL_POSITIONS_THRESHOLD) {
      return new StreamJoinStrategy(runs);
    }

    // Sparse data (gaps > 30%): RangeQuery can skip gaps efficiently
    // Benchmark evidence: RangeQuery wins all sparse sorted scenarios (except high m above)
    double gapRatio = estimateGapRatio(mapping);
    if (gapRatio > SPARSE_GAP_THRESHOLD) {
      return new RangeQueryStrategy(runs);
    }

    // Dense sorted data with many positions AND many runs: StreamJoin wins
    // Benchmark evidence: StreamJoin wins for (n>=10000, m>=100, gap<=0.3, sorted)
    // For small m (e.g., m=10), RangeQuery is still faster even with large n
    if (n >= BULK_POSITION_THRESHOLD && m >= MANY_RUNS_THRESHOLD) {
      return new StreamJoinStrategy(runs);
    }

    // Default for sorted data: RangeQuery
    // Optimal for: small m, small n, or moderate workloads
    // Benchmark evidence: wins majority of remaining sorted scenarios
    return new RangeQueryStrategy(runs);
  }

  /**
   * Estimates what percentage of the source range is NOT covered by runs.
   *
   * @param mapping the file mapping
   * @return gap ratio (0.0 = no gaps/dense, 1.0 = all gaps)
   */
  double estimateGapRatio(FileMapping mapping) {
    List<Run> runs = mapping.runs();

    if (runs.isEmpty()) {
      return 1.0;
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
      return 0.0;
    }

    return 1.0 - ((double) coveredLength / totalRange);
  }

  /**
   * Checks if positions are sorted by sampling the first N elements.
   *
   * @param positions the positions to check
   * @return true if positions appear to be sorted
   */
  boolean isSortedPrimitive(long[] positions) {
    if (positions.length <= 1) {
      return true;
    }

    int sampleSize = Math.min(SORTEDNESS_SAMPLE_SIZE, positions.length);
    long prev = positions[0];

    for (int i = 1; i < sampleSize; i++) {
      long current = positions[i];
      if (current < prev) {
        return false;
      }
      prev = current;
    }

    return true;
  }

  /**
   * Convenience method for selecting strategy with boxed positions.
   *
   * <p>This method converts the List to a primitive array and delegates to {@link
   * #selectOptimal(FileMapping, long[])}. It incurs boxing overhead and should only be used in
   * tests or other non-performance-critical code.
   *
   * @param mapping the file mapping containing runs
   * @param positions the positions to remap
   * @return the optimal remapping strategy
   */
  public RemappingStrategy selectOptimal(FileMapping mapping, List<Long> positions) {
    Preconditions.checkNotNull(mapping, "mapping is null");
    Preconditions.checkNotNull(positions, "positions is null");

    if (positions.isEmpty()) {
      return new LinearSearchStrategy(mapping.runs());
    }

    long[] primitivePositions = new long[positions.size()];
    for (int i = 0; i < positions.size(); i++) {
      primitivePositions[i] = positions.get(i);
    }

    return selectOptimal(mapping, primitivePositions);
  }

  /**
   * Convenience method for checking sortedness with boxed positions.
   *
   * <p>This method converts the List to a primitive array and delegates to {@link
   * #isSortedPrimitive(long[])}. It incurs boxing overhead and should only be used in tests.
   *
   * @param positions the positions to check
   * @return true if positions appear to be sorted
   */
  public boolean isSorted(List<Long> positions) {
    if (positions == null || positions.isEmpty()) {
      return true;
    }

    long[] primitivePositions = new long[positions.size()];
    for (int i = 0; i < positions.size(); i++) {
      primitivePositions[i] = positions.get(i);
    }

    return isSortedPrimitive(primitivePositions);
  }
}
