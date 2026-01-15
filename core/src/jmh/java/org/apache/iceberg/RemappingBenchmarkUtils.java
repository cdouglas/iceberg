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

/** Utility methods for creating test data in remapping benchmarks. */
public class RemappingBenchmarkUtils {

  private static final Random RANDOM = new Random(42); // Fixed seed for reproducibility
  private static final long RUN_LENGTH = 1000; // Fixed run length for consistency

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
   */
  public static List<Long> createSortedPositions(int count, int numRuns) {
    List<Long> positions = new ArrayList<>(count);
    long maxPosition = numRuns * RUN_LENGTH * 2; // Account for potential gaps

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
}
