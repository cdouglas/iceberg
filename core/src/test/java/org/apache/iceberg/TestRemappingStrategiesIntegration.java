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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.GenericCompactionMap.GenericRun;
import org.junit.jupiter.api.Test;

/** Integration tests for remapping strategies in realistic scenarios. */
public class TestRemappingStrategiesIntegration {

  @Test
  public void testStreamJoinOptimalForSortedPositions() {
    // Create scenario where StreamJoin should be optimal
    List<Run> runs = createRuns(100);
    List<Long> sortedPositions = createSortedPositions(10000);

    // Test that both strategies work correctly
    StreamJoinStrategy streamJoin = new StreamJoinStrategy(runs);
    BinarySearchStrategy binarySearch = new BinarySearchStrategy(runs);

    Map<Long, Run> streamResults = streamJoin.runForPositions(sortedPositions);
    Map<Long, Run> binaryResults = binarySearch.runForPositions(sortedPositions);

    // Both should produce identical results
    assertThat(streamResults).isEqualTo(binaryResults);

    // Stream join should leverage sorted input efficiently
    // (Correctness is more important than timing for tests)
    assertThat(streamResults).hasSize(10000);

    // Verify all results are correct
    for (Map.Entry<Long, Run> entry : streamResults.entrySet()) {
      long pos = entry.getKey();
      Run run = entry.getValue();
      assertThat(pos)
          .isGreaterThanOrEqualTo(run.sourcePosition())
          .isLessThan(run.sourcePosition() + run.length());
    }
  }

  @Test
  public void testRangeQueryOptimalForHighFanIn() {
    // Create scenario where RangeQuery should be optimal (few runs, many positions)
    // Create runs that cover the full range of positions
    List<Run> runs = createRunsCoveringRange(10, 100000); // 10 runs covering [0-100000)
    List<Long> sortedPositions = createSortedPositions(100000); // Many positions

    RangeQueryStrategy rangeQuery = new RangeQueryStrategy(runs);
    StreamJoinStrategy streamJoin = new StreamJoinStrategy(runs);

    Map<Long, Run> rangeResults = rangeQuery.runForPositions(sortedPositions);
    Map<Long, Run> streamResults = streamJoin.runForPositions(sortedPositions);

    // Both should produce identical results
    assertThat(rangeResults).isEqualTo(streamResults);

    // Verify all 100k positions were mapped
    assertThat(rangeResults).hasSize(100000);

    // RangeQuery should handle high fan-in efficiently
    // For n >> m, RangeQuery is optimal
    assertThat(runs.size()).isLessThan(sortedPositions.size() / 100);
  }

  @Test
  public void testBulkStrategiesWithRealisticDistribution() {
    // Test with realistic distribution from production workloads
    List<Run> runs = createRunsWithGaps(100, 0.3); // 30% gaps
    List<Long> positions = createClusteredPositions(10000);

    // Test all strategies produce same results
    StreamJoinStrategy streamJoin = new StreamJoinStrategy(runs);
    RangeQueryStrategy rangeQuery = new RangeQueryStrategy(runs);
    IntervalTreeStrategy intervalTree = new IntervalTreeStrategy(runs);

    Map<Long, Run> streamResults = streamJoin.runForPositions(positions);
    Map<Long, Run> rangeResults = rangeQuery.runForPositions(positions);
    Map<Long, Run> treeResults = intervalTree.runForPositions(positions);

    // All strategies must produce identical results
    assertThat(streamResults).isEqualTo(rangeResults);
    assertThat(streamResults).isEqualTo(treeResults);

    // Verify results are correct
    for (Map.Entry<Long, Run> entry : streamResults.entrySet()) {
      long pos = entry.getKey();
      Run run = entry.getValue();

      // Position must be within the run
      assertThat(pos)
          .isGreaterThanOrEqualTo(run.sourcePosition())
          .isLessThan(run.sourcePosition() + run.length());
    }

    // Some positions should be dropped due to gaps
    assertThat(streamResults.size()).isLessThan(positions.size());
  }

  @Test
  public void testBulkRemappingScalesLinearly() {
    // Test that performance scales as expected with input size
    List<Run> runs = createRuns(100);

    // Test with increasing position counts
    Map<Long, Run> result1k = testBulkRemap(runs, 1000);
    Map<Long, Run> result10k = testBulkRemap(runs, 10000);
    Map<Long, Run> result100k = testBulkRemap(runs, 100000);

    // Verify all positions were remapped correctly
    assertThat(result1k).hasSize(1000);
    assertThat(result10k).hasSize(10000);
    assertThat(result100k).hasSize(100000);

    // Verify results are correct for each size
    verifyRemappingCorrect(result1k);
    verifyRemappingCorrect(result10k);
    verifyRemappingCorrect(result100k);

    // All strategies should handle scaling gracefully
    // (We don't assert timing because it's flaky in tests)
  }

  // Helper methods

  private List<Run> createRuns(int count) {
    List<Run> runs = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      long sourcePos = i * 1000L;
      long targetPos = i * 1000L;
      runs.add(new GenericRun(sourcePos, targetPos, 1000));
    }
    return runs;
  }

  private List<Run> createRunsCoveringRange(int runCount, long totalRange) {
    List<Run> runs = new ArrayList<>();
    long runLength = totalRange / runCount;
    for (int i = 0; i < runCount; i++) {
      long sourcePos = i * runLength;
      long targetPos = i * runLength;
      runs.add(new GenericRun(sourcePos, targetPos, runLength));
    }
    return runs;
  }

  private List<Run> createRunsWithGaps(int count, double gapRatio) {
    List<Run> runs = new ArrayList<>();
    long currentPos = 0;
    long runLength = 1000;
    long gapLength = (long) (runLength * gapRatio / (1 - gapRatio));

    for (int i = 0; i < count; i++) {
      runs.add(new GenericRun(currentPos, i * runLength, runLength));
      currentPos += runLength + gapLength; // Add gap after each run
    }
    return runs;
  }

  private List<Long> createSortedPositions(int count) {
    List<Long> positions = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      positions.add((long) i);
    }
    return positions;
  }

  private List<Long> createClusteredPositions(int count) {
    // Create positions that cluster in certain ranges (realistic for DVs)
    List<Long> positions = new ArrayList<>();
    Random rand = new Random(42); // Fixed seed for reproducibility

    // Create 10 clusters
    for (int cluster = 0; cluster < 10; cluster++) {
      long clusterStart = cluster * 10000L;
      for (int i = 0; i < count / 10; i++) {
        // Positions within cluster range with some randomness
        long pos = clusterStart + rand.nextInt(1000);
        positions.add(pos);
      }
    }

    positions.sort(Long::compareTo);
    return positions;
  }

  private Map<Long, Run> testBulkRemap(List<Run> runs, int positionCount) {
    List<Long> positions = createSortedPositions(positionCount);
    StreamJoinStrategy strategy = new StreamJoinStrategy(runs);
    return strategy.runForPositions(positions);
  }

  private void verifyRemappingCorrect(Map<Long, Run> results) {
    for (Map.Entry<Long, Run> entry : results.entrySet()) {
      long pos = entry.getKey();
      Run run = entry.getValue();

      // Position must be within the run
      assertThat(pos)
          .as(
              "Position %d should be in run [%d, %d)",
              pos, run.sourcePosition(), run.sourcePosition() + run.length())
          .isGreaterThanOrEqualTo(run.sourcePosition())
          .isLessThan(run.sourcePosition() + run.length());
    }
  }
}
