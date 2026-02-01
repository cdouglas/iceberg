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
import java.util.Random;
import java.util.Set;
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.GenericCompactionMap.GenericRun;

/**
 * Utility class for generating test data for remapping benchmarks and tests.
 *
 * <p>This class provides methods to generate runs with configurable gap ratios and positions with
 * configurable coverage. It is used by both JMH benchmarks and unit tests.
 */
public class RemappingTestDataGenerator {

  /** Default run length used for consistency across tests and benchmarks. */
  public static final long RUN_LENGTH = 1000;

  private final Random random;

  /** Creates a generator with a fixed seed for reproducible results. */
  public RemappingTestDataGenerator() {
    this(42); // Fixed seed for reproducibility
  }

  /**
   * Creates a generator with a custom seed.
   *
   * @param seed the random seed
   */
  public RemappingTestDataGenerator(long seed) {
    this.random = new Random(seed);
  }

  /**
   * Creates runs with specified gap ratio.
   *
   * @param numRuns number of runs to create
   * @param gapRatio percentage of total range that is gaps (0.0 = no gaps, 0.5 = 50% gaps)
   * @return list of runs with gaps
   */
  public List<Run> createRunsWithGaps(int numRuns, double gapRatio) {
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
   */
  public List<Long> createSortedPositions(int count, int numRuns) {
    List<Long> positions = new ArrayList<>(count);
    long maxPosition = numRuns * RUN_LENGTH * 2; // Account for potential gaps

    long pos = 0;
    for (int i = 0; i < count; i++) {
      // Increment by 1-20 to create realistic sorted distribution
      pos += random.nextInt(20) + 1;
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
  public List<Long> createRandomPositions(int count) {
    Set<Long> uniquePositions = new HashSet<>();
    long maxRange = (long) count * RUN_LENGTH * 2;

    while (uniquePositions.size() < count) {
      // Generate positive longs in reasonable range
      long pos = (random.nextLong() & Long.MAX_VALUE) % maxRange;
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
   */
  public List<Long> createSortedPositionsWithCoverage(int count, int numRuns, double coverage) {
    if (coverage <= 0.0 || coverage > 1.0) {
      throw new IllegalArgumentException("Coverage must be in (0.0, 1.0], got: " + coverage);
    }

    List<Long> positions = new ArrayList<>(count);

    // Calculate the range that positions should cover
    long totalRange = numRuns * RUN_LENGTH * 2; // Account for potential gaps
    long coveredRange = (long) (totalRange * coverage);

    // Start positions at a random offset within the uncovered portion
    // This ensures positions don't always start at 0
    long maxOffset = totalRange - coveredRange;
    long startOffset = maxOffset > 0 ? (random.nextLong() & Long.MAX_VALUE) % maxOffset : 0;

    long pos = startOffset;
    long endPosition = startOffset + coveredRange;

    for (int i = 0; i < count; i++) {
      // Increment by 1-20 to create realistic sorted distribution
      pos += random.nextInt(20) + 1;
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
   */
  public List<Long> createRandomPositionsWithCoverage(int count, int numRuns, double coverage) {
    if (coverage <= 0.0 || coverage > 1.0) {
      throw new IllegalArgumentException("Coverage must be in (0.0, 1.0], got: " + coverage);
    }

    Set<Long> uniquePositions = new HashSet<>();

    long totalRange = numRuns * RUN_LENGTH * 2;
    long coveredRange = (long) (totalRange * coverage);
    long maxOffset = totalRange - coveredRange;
    long startOffset = maxOffset > 0 ? (random.nextLong() & Long.MAX_VALUE) % maxOffset : 0;

    while (uniquePositions.size() < count) {
      long pos = startOffset + ((random.nextLong() & Long.MAX_VALUE) % coveredRange);
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
