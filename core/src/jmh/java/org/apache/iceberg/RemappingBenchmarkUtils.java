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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.GenericCompactionMap.GenericRun;

/** Utility methods for creating test data in remapping benchmarks. */
public class RemappingBenchmarkUtils {

  private static final Random RANDOM = new Random(42); // Fixed seed for reproducibility
  static final long RUN_LENGTH = 1000; // Fixed run length for consistency

  /**
   * Creates runs with specified gap ratio.
   *
   * @param numRuns number of runs to create
   * @param gapRatio percentage of total range that is gaps (0.0 = no gaps, 0.5 = 50% gaps)
   * @return list of runs with gaps
   */
  public static List<Run> createRunsWithGaps(int numRuns, double gapRatio) {
    List<Run> runs = new ArrayList<>();
    long sourcePosition = 0;
    long targetPosition = 0;

    for (int i = 0; i < numRuns; i++) {
      runs.add(new GenericRun(sourcePosition, targetPosition, RUN_LENGTH));

      sourcePosition += RUN_LENGTH;
      targetPosition += RUN_LENGTH;

      // Add gap based on gapRatio
      if (gapRatio > 0 && i < numRuns - 1) {
        long gap = (long) (RUN_LENGTH * gapRatio / (1 - gapRatio));
        sourcePosition += gap;
      }
    }

    return runs;
  }

  /**
   * Creates sorted positions distributed across the run range.
   *
   * <p>Positions are generated to fall within run ranges for realistic scenarios.
   *
   * @param count number of positions to create
   * @param numRuns number of runs (used to determine range)
   * @return list of sorted positions
   * @throws IllegalArgumentException if count is too large for the range to produce meaningful
   *     sorted positions
   */
  public static List<Long> createSortedPositions(int count, int numRuns) {
    long maxPosition = numRuns * RUN_LENGTH * 2; // Account for potential gaps

    // Average increment is ~10.5 per position (random 1-20)
    // Need range > count * avgIncrement to avoid clamping most positions
    long minRangeNeeded = (long) (count * 10.5);
    if (maxPosition < minRangeNeeded) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "Cannot generate %d meaningful sorted positions in range of %d "
                  + "(need at least %d to avoid clamping). "
                  + "numRuns=%d. Reduce numPositions or increase numRuns.",
              count,
              maxPosition,
              minRangeNeeded,
              numRuns));
    }

    List<Long> positions = new ArrayList<>(count);

    long pos = 0;
    for (int i = 0; i < count; i++) {
      // Increment by 1-20 to create realistic sorted distribution
      pos += RANDOM.nextInt(20) + 1;
      if (pos >= maxPosition) {
        pos = maxPosition - 1;
      }
      positions.add(pos);
    }

    return positions;
  }

  /**
   * Creates random unsorted positions.
   *
   * @param count number of positions to create
   * @return list of random positions
   */
  public static List<Long> createRandomPositions(int count) {
    Set<Long> uniquePositions = new HashSet<>();
    long maxRange = (long) count * RUN_LENGTH * 2;

    while (uniquePositions.size() < count) {
      // Generate positive longs in reasonable range
      long pos = (RANDOM.nextLong() & Long.MAX_VALUE) % maxRange;
      uniquePositions.add(pos);
    }

    return new ArrayList<>(uniquePositions);
  }

  /**
   * Creates sorted positions covering only a fraction of the run range.
   *
   * <p>This is used to measure the impact of predicate pushdown optimization. When coverage is low
   * (e.g., 0.1), positions only span 10% of the runs, allowing strategies with predicate pushdown
   * to skip 90% of runs.
   *
   * @param count number of positions to create
   * @param numRuns total number of runs
   * @param coverage fraction of run range to cover (0.0-1.0)
   * @return list of sorted positions within the covered range
   * @throws IllegalArgumentException if count is too large for the covered range to produce
   *     meaningful sorted positions (would clamp most to the end)
   */
  public static List<Long> createSortedPositionsWithCoverage(
      int count, int numRuns, double coverage) {
    if (coverage <= 0.0 || coverage > 1.0) {
      throw new IllegalArgumentException("Coverage must be in (0.0, 1.0], got: " + coverage);
    }

    // Calculate the range that positions should cover
    long totalRange = numRuns * RUN_LENGTH * 2; // Account for potential gaps
    long coveredRange = (long) (totalRange * coverage);

    // Average increment is ~10.5 per position (random 1-20)
    // Need range > count * avgIncrement to avoid clamping most positions
    long minRangeNeeded = (long) (count * 10.5);
    if (coveredRange < minRangeNeeded) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "Cannot generate %d meaningful sorted positions in covered range of %d "
                  + "(need at least %d to avoid clamping). "
                  + "numRuns=%d, coverage=%.1f. Reduce numPositions or increase numRuns/coverage.",
              count,
              coveredRange,
              minRangeNeeded,
              numRuns,
              coverage));
    }

    List<Long> positions = new ArrayList<>(count);

    // Start positions at a random offset within the uncovered portion
    // This ensures positions don't always start at 0
    long maxOffset = totalRange - coveredRange;
    long startOffset = maxOffset > 0 ? (RANDOM.nextLong() & Long.MAX_VALUE) % maxOffset : 0;

    long pos = startOffset;
    long endPosition = startOffset + coveredRange;

    for (int i = 0; i < count; i++) {
      // Increment by 1-20 to create realistic sorted distribution
      pos += RANDOM.nextInt(20) + 1;
      if (pos >= endPosition) {
        pos = endPosition - 1;
      }
      positions.add(pos);
    }

    return positions;
  }

  /**
   * Creates random unsorted positions covering only a fraction of the run range.
   *
   * @param count number of positions to create
   * @param numRuns total number of runs
   * @param coverage fraction of run range to cover (0.0-1.0)
   * @return list of random positions within the covered range
   * @throws IllegalArgumentException if count exceeds the covered range (impossible to generate
   *     that many unique positions)
   */
  public static List<Long> createRandomPositionsWithCoverage(
      int count, int numRuns, double coverage) {
    if (coverage <= 0.0 || coverage > 1.0) {
      throw new IllegalArgumentException("Coverage must be in (0.0, 1.0], got: " + coverage);
    }

    long totalRange = numRuns * RUN_LENGTH * 2;
    long coveredRange = (long) (totalRange * coverage);

    // Fail fast if impossible: can't generate more unique positions than the range allows
    if (count > coveredRange) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "Cannot generate %d unique random positions in covered range of %d "
                  + "(numRuns=%d, coverage=%.1f, totalRange=%d). "
                  + "Reduce numPositions or increase numRuns/coverage.",
              count,
              coveredRange,
              numRuns,
              coverage,
              totalRange));
    }

    // Warn if this will be slow (> 50% fill rate means many collisions)
    if (count > coveredRange / 2) {
      System.err.printf(
          Locale.ROOT,
          "WARNING: High fill rate (%d/%d = %.0f%%) will cause slow position generation%n",
          count,
          coveredRange,
          100.0 * count / coveredRange);
    }

    Set<Long> uniquePositions = new HashSet<>();

    long maxOffset = totalRange - coveredRange;
    long startOffset = maxOffset > 0 ? (RANDOM.nextLong() & Long.MAX_VALUE) % maxOffset : 0;

    while (uniquePositions.size() < count) {
      long pos = startOffset + ((RANDOM.nextLong() & Long.MAX_VALUE) % coveredRange);
      uniquePositions.add(pos);
    }

    return new ArrayList<>(uniquePositions);
  }

  /**
   * Calculates the total range covered by runs.
   *
   * @param numRuns number of runs
   * @param gapRatio gap ratio used when creating runs
   * @return total range from first run start to last run end
   */
  public static long calculateTotalRunRange(int numRuns, double gapRatio) {
    if (numRuns == 0) {
      return 0;
    }

    long range = 0;
    for (int i = 0; i < numRuns; i++) {
      range += RUN_LENGTH;
      if (gapRatio > 0 && i < numRuns - 1) {
        range += (long) (RUN_LENGTH * gapRatio / (1 - gapRatio));
      }
    }
    return range;
  }
}
