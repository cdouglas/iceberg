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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.GenericCompactionMap.GenericRun;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestRemappingStrategies {

  /** Test basic functionality with a simple run set. */
  @Test
  public void testLinearSearchBasic() {
    List<Run> runs =
        Arrays.asList(
            new GenericRun(0, 0, 100), // [0, 100)
            new GenericRun(150, 100, 50), // [150, 200) - gap at [100, 150)
            new GenericRun(300, 150, 100)); // [300, 400) - gap at [200, 300)

    RemappingStrategy strategy = new LinearSearchStrategy(runs);

    // Test positions in runs
    assertThat(strategy.runForPosition(0)).isEqualTo(runs.get(0));
    assertThat(strategy.runForPosition(50)).isEqualTo(runs.get(0));
    assertThat(strategy.runForPosition(99)).isEqualTo(runs.get(0));
    assertThat(strategy.runForPosition(150)).isEqualTo(runs.get(1));
    assertThat(strategy.runForPosition(175)).isEqualTo(runs.get(1));
    assertThat(strategy.runForPosition(199)).isEqualTo(runs.get(1));
    assertThat(strategy.runForPosition(300)).isEqualTo(runs.get(2));
    assertThat(strategy.runForPosition(350)).isEqualTo(runs.get(2));
    assertThat(strategy.runForPosition(399)).isEqualTo(runs.get(2));

    // Test positions in gaps
    assertThat(strategy.runForPosition(100)).isNull();
    assertThat(strategy.runForPosition(125)).isNull();
    assertThat(strategy.runForPosition(149)).isNull();
    assertThat(strategy.runForPosition(200)).isNull();
    assertThat(strategy.runForPosition(250)).isNull();
    assertThat(strategy.runForPosition(299)).isNull();

    // Test out of bounds
    assertThat(strategy.runForPosition(-1)).isNull();
    assertThat(strategy.runForPosition(400)).isNull();
    assertThat(strategy.runForPosition(1000)).isNull();
  }

  /** Test interval tree with the same data as linear search. */
  @Test
  public void testIntervalTreeBasic() {
    List<Run> runs =
        Arrays.asList(
            new GenericRun(0, 0, 100),
            new GenericRun(150, 100, 50),
            new GenericRun(300, 150, 100));

    RemappingStrategy strategy = new IntervalTreeStrategy(runs);

    // Test positions in runs
    assertThat(strategy.runForPosition(0)).isEqualTo(runs.get(0));
    assertThat(strategy.runForPosition(50)).isEqualTo(runs.get(0));
    assertThat(strategy.runForPosition(99)).isEqualTo(runs.get(0));
    assertThat(strategy.runForPosition(150)).isEqualTo(runs.get(1));
    assertThat(strategy.runForPosition(175)).isEqualTo(runs.get(1));
    assertThat(strategy.runForPosition(199)).isEqualTo(runs.get(1));
    assertThat(strategy.runForPosition(300)).isEqualTo(runs.get(2));
    assertThat(strategy.runForPosition(350)).isEqualTo(runs.get(2));
    assertThat(strategy.runForPosition(399)).isEqualTo(runs.get(2));

    // Test positions in gaps (should match linear/binary search)
    assertThat(strategy.runForPosition(100)).isNull();
    assertThat(strategy.runForPosition(125)).isNull();
    assertThat(strategy.runForPosition(149)).isNull();
    assertThat(strategy.runForPosition(200)).isNull();
    assertThat(strategy.runForPosition(250)).isNull();
    assertThat(strategy.runForPosition(299)).isNull();

    // Test out of bounds (should match linear/binary search)
    assertThat(strategy.runForPosition(-1)).isNull();
    assertThat(strategy.runForPosition(400)).isNull();
    assertThat(strategy.runForPosition(1000)).isNull();
  }

  /** Test binary search with the same data as linear search. */
  @Test
  public void testBinarySearchBasic() {
    List<Run> runs =
        Arrays.asList(
            new GenericRun(0, 0, 100),
            new GenericRun(150, 100, 50),
            new GenericRun(300, 150, 100));

    RemappingStrategy strategy = new BinarySearchStrategy(runs);

    // Test positions in runs
    assertThat(strategy.runForPosition(0)).isEqualTo(runs.get(0));
    assertThat(strategy.runForPosition(50)).isEqualTo(runs.get(0));
    assertThat(strategy.runForPosition(99)).isEqualTo(runs.get(0));
    assertThat(strategy.runForPosition(150)).isEqualTo(runs.get(1));
    assertThat(strategy.runForPosition(175)).isEqualTo(runs.get(1));
    assertThat(strategy.runForPosition(199)).isEqualTo(runs.get(1));
    assertThat(strategy.runForPosition(300)).isEqualTo(runs.get(2));
    assertThat(strategy.runForPosition(350)).isEqualTo(runs.get(2));
    assertThat(strategy.runForPosition(399)).isEqualTo(runs.get(2));

    // Test positions in gaps (should match linear search)
    assertThat(strategy.runForPosition(100)).isNull();
    assertThat(strategy.runForPosition(125)).isNull();
    assertThat(strategy.runForPosition(149)).isNull();
    assertThat(strategy.runForPosition(200)).isNull();
    assertThat(strategy.runForPosition(250)).isNull();
    assertThat(strategy.runForPosition(299)).isNull();

    // Test out of bounds (should match linear search)
    assertThat(strategy.runForPosition(-1)).isNull();
    assertThat(strategy.runForPosition(400)).isNull();
    assertThat(strategy.runForPosition(1000)).isNull();
  }

  /** Test empty run lists. */
  @Test
  public void testEmptyRuns() {
    List<Run> empty = new ArrayList<>();

    RemappingStrategy linear = new LinearSearchStrategy(empty);
    RemappingStrategy binary = new BinarySearchStrategy(empty);
    RemappingStrategy intervalTree = new IntervalTreeStrategy(empty);

    assertThat(linear.runForPosition(0)).isNull();
    assertThat(linear.runForPosition(100)).isNull();

    assertThat(binary.runForPosition(0)).isNull();
    assertThat(binary.runForPosition(100)).isNull();

    assertThat(intervalTree.runForPosition(0)).isNull();
    assertThat(intervalTree.runForPosition(100)).isNull();
  }

  /** Test single run. */
  @Test
  public void testSingleRun() {
    List<Run> runs = Arrays.asList(new GenericRun(0, 0, 1000));

    RemappingStrategy linear = new LinearSearchStrategy(runs);
    RemappingStrategy binary = new BinarySearchStrategy(runs);
    RemappingStrategy intervalTree = new IntervalTreeStrategy(runs);

    // Inside run
    assertThat(linear.runForPosition(0)).isEqualTo(runs.get(0));
    assertThat(linear.runForPosition(500)).isEqualTo(runs.get(0));
    assertThat(linear.runForPosition(999)).isEqualTo(runs.get(0));

    assertThat(binary.runForPosition(0)).isEqualTo(runs.get(0));
    assertThat(binary.runForPosition(500)).isEqualTo(runs.get(0));
    assertThat(binary.runForPosition(999)).isEqualTo(runs.get(0));

    assertThat(intervalTree.runForPosition(0)).isEqualTo(runs.get(0));
    assertThat(intervalTree.runForPosition(500)).isEqualTo(runs.get(0));
    assertThat(intervalTree.runForPosition(999)).isEqualTo(runs.get(0));

    // Outside run
    assertThat(linear.runForPosition(-1)).isNull();
    assertThat(linear.runForPosition(1000)).isNull();

    assertThat(binary.runForPosition(-1)).isNull();
    assertThat(binary.runForPosition(1000)).isNull();

    assertThat(intervalTree.runForPosition(-1)).isNull();
    assertThat(intervalTree.runForPosition(1000)).isNull();
  }

  /** Test that binary search rejects unsorted runs. */
  @Test
  public void testBinarySearchRequiresSorted() {
    List<Run> unsorted =
        Arrays.asList(
            new GenericRun(150, 100, 50), // Wrong order
            new GenericRun(0, 0, 100));

    assertThatThrownBy(() -> new BinarySearchStrategy(unsorted))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be sorted");
  }

  /** Test that binary search rejects overlapping runs. */
  @Test
  public void testBinarySearchRejectsOverlaps() {
    List<Run> overlapping =
        Arrays.asList(
            new GenericRun(0, 0, 100), // [0, 100)
            new GenericRun(50, 100, 50)); // [50, 100) - overlaps!

    assertThatThrownBy(() -> new BinarySearchStrategy(overlapping))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be sorted");
  }

  /** Test that interval tree rejects unsorted runs. */
  @Test
  public void testIntervalTreeRequiresSorted() {
    List<Run> unsorted =
        Arrays.asList(
            new GenericRun(150, 100, 50), // Wrong order
            new GenericRun(0, 0, 100));

    assertThatThrownBy(() -> new IntervalTreeStrategy(unsorted))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be sorted");
  }

  /** Test that interval tree rejects overlapping runs. */
  @Test
  public void testIntervalTreeRejectsOverlaps() {
    List<Run> overlapping =
        Arrays.asList(
            new GenericRun(0, 0, 100), // [0, 100)
            new GenericRun(50, 100, 50)); // [50, 100) - overlaps!

    assertThatThrownBy(() -> new IntervalTreeStrategy(overlapping))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be sorted");
  }

  /** Test with large number of runs to verify all strategies match. */
  @ParameterizedTest
  @ValueSource(ints = {10, 100, 1000})
  public void testLargeRunCount(int numRuns) {
    List<Run> runs = generateSortedRuns(numRuns);

    RemappingStrategy linear = new LinearSearchStrategy(runs);
    RemappingStrategy binary = new BinarySearchStrategy(runs);
    RemappingStrategy intervalTree = new IntervalTreeStrategy(runs);

    // Test random positions
    Random rand = new Random(42);
    for (int i = 0; i < 1000; i++) {
      long pos = nextLong(rand, numRuns * 1000L);

      Run linearResult = linear.runForPosition(pos);
      Run binaryResult = binary.runForPosition(pos);
      Run intervalTreeResult = intervalTree.runForPosition(pos);

      // Results should match
      assertThat(binaryResult)
          .as("Binary search should match linear search for position %s", pos)
          .isEqualTo(linearResult);
      assertThat(intervalTreeResult)
          .as("Interval tree should match linear search for position %s", pos)
          .isEqualTo(linearResult);
    }
  }

  /**
   * Property-based test: all strategies should produce identical results.
   *
   * <p>This test generates many random run configurations and verifies that binary search and
   * interval tree produce identical results to linear search for all positions.
   */
  @Test
  public void testAllStrategiesMatch() {
    Random rand = new Random(123);

    // Test 100 different run configurations
    for (int config = 0; config < 100; config++) {
      int numRuns = 1 + rand.nextInt(100); // 1-100 runs
      List<Run> runs = generateSortedRunsWithGaps(numRuns, rand);

      RemappingStrategy linear = new LinearSearchStrategy(runs);
      RemappingStrategy binary = new BinarySearchStrategy(runs);
      RemappingStrategy intervalTree = new IntervalTreeStrategy(runs);

      // Test 100 random positions for each configuration
      long maxPos = runs.get(runs.size() - 1).sourcePosition() + runs.get(runs.size() - 1).length();

      for (int i = 0; i < 100; i++) {
        long pos = nextLong(rand, maxPos + 1000); // Include out-of-bounds

        Run linearResult = linear.runForPosition(pos);
        Run binaryResult = binary.runForPosition(pos);
        Run intervalTreeResult = intervalTree.runForPosition(pos);

        assertThat(binaryResult)
            .as(
                "Binary search should match linear search for config %s, position %s",
                config, pos)
            .isEqualTo(linearResult);
        assertThat(intervalTreeResult)
            .as(
                "Interval tree should match linear search for config %s, position %s",
                config, pos)
            .isEqualTo(linearResult);
      }
    }
  }

  /** Test edge cases at run boundaries. */
  @Test
  public void testRunBoundaries() {
    List<Run> runs =
        Arrays.asList(
            new GenericRun(0, 0, 100), // [0, 100)
            new GenericRun(100, 100, 100)); // [100, 200) - no gap

    RemappingStrategy linear = new LinearSearchStrategy(runs);
    RemappingStrategy binary = new BinarySearchStrategy(runs);
    RemappingStrategy intervalTree = new IntervalTreeStrategy(runs);

    // Test boundary between runs
    assertThat(linear.runForPosition(99)).isEqualTo(runs.get(0));
    assertThat(linear.runForPosition(100)).isEqualTo(runs.get(1));
    assertThat(linear.runForPosition(101)).isEqualTo(runs.get(1));

    assertThat(binary.runForPosition(99)).isEqualTo(runs.get(0));
    assertThat(binary.runForPosition(100)).isEqualTo(runs.get(1));
    assertThat(binary.runForPosition(101)).isEqualTo(runs.get(1));

    assertThat(intervalTree.runForPosition(99)).isEqualTo(runs.get(0));
    assertThat(intervalTree.runForPosition(100)).isEqualTo(runs.get(1));
    assertThat(intervalTree.runForPosition(101)).isEqualTo(runs.get(1));
  }

  /** Test strategy factory selection. */
  @Test
  public void testStrategyFactorySelection() {
    // Few runs (m < 10): should use linear search
    List<Run> fewRuns = generateSortedRuns(5);
    RemappingStrategy strategy1 = RemappingStrategy.Factory.create(fewRuns);
    assertThat(strategy1.name()).isEqualTo("linear-search");

    // Medium runs (10 <= m < 100): should use binary search
    List<Run> mediumRuns = generateSortedRuns(50);
    RemappingStrategy strategy2 = RemappingStrategy.Factory.create(mediumRuns);
    assertThat(strategy2.name()).isEqualTo("binary-search");

    // Many runs (m >= 100): should use interval tree
    List<Run> manyRuns = generateSortedRuns(150);
    RemappingStrategy strategy3 = RemappingStrategy.Factory.create(manyRuns);
    assertThat(strategy3.name()).isEqualTo("interval-tree");
  }

  /** Test stream join bulk lookup with sorted positions. */
  @Test
  public void testStreamJoinBulkSorted() {
    List<Run> runs =
        Arrays.asList(
            new GenericRun(0, 0, 100), // [0, 100)
            new GenericRun(150, 100, 50), // [150, 200) - gap at [100, 150)
            new GenericRun(300, 150, 100)); // [300, 400) - gap at [200, 300)

    StreamJoinStrategy strategy = new StreamJoinStrategy(runs);

    // Sorted positions including positions in runs, gaps, and out of bounds
    List<Long> positions = Arrays.asList(5L, 50L, 99L, 125L, 150L, 175L, 250L, 300L, 350L, 500L);

    Map<Long, Run> results = strategy.runForPositions(positions);

    // Positions in runs should be mapped
    assertThat(results.get(5L)).isEqualTo(runs.get(0));
    assertThat(results.get(50L)).isEqualTo(runs.get(0));
    assertThat(results.get(99L)).isEqualTo(runs.get(0));
    assertThat(results.get(150L)).isEqualTo(runs.get(1));
    assertThat(results.get(175L)).isEqualTo(runs.get(1));
    assertThat(results.get(300L)).isEqualTo(runs.get(2));
    assertThat(results.get(350L)).isEqualTo(runs.get(2));

    // Positions in gaps or out of bounds should not be in results
    assertThat(results.containsKey(125L)).isFalse();
    assertThat(results.containsKey(250L)).isFalse();
    assertThat(results.containsKey(500L)).isFalse();
  }

  /** Test stream join bulk lookup with unsorted positions (should fall back to binary search). */
  @Test
  public void testStreamJoinBulkUnsorted() {
    List<Run> runs =
        Arrays.asList(
            new GenericRun(0, 0, 100),
            new GenericRun(150, 100, 50),
            new GenericRun(300, 150, 100));

    StreamJoinStrategy strategy = new StreamJoinStrategy(runs);

    // Unsorted positions
    List<Long> positions = Arrays.asList(350L, 50L, 175L, 125L, 5L);

    Map<Long, Run> results = strategy.runForPositions(positions);

    // All positions in runs should still be mapped (via fallback)
    assertThat(results.get(5L)).isEqualTo(runs.get(0));
    assertThat(results.get(50L)).isEqualTo(runs.get(0));
    assertThat(results.get(175L)).isEqualTo(runs.get(1));
    assertThat(results.get(350L)).isEqualTo(runs.get(2));

    // Position in gap should not be in results
    assertThat(results.containsKey(125L)).isFalse();
  }

  /** Test stream join single-position lookup (compatibility). */
  @Test
  public void testStreamJoinSinglePosition() {
    List<Run> runs =
        Arrays.asList(
            new GenericRun(0, 0, 100),
            new GenericRun(150, 100, 50),
            new GenericRun(300, 150, 100));

    StreamJoinStrategy strategy = new StreamJoinStrategy(runs);

    // Single-position lookup should work
    assertThat(strategy.runForPosition(50)).isEqualTo(runs.get(0));
    assertThat(strategy.runForPosition(175)).isEqualTo(runs.get(1));
    assertThat(strategy.runForPosition(350)).isEqualTo(runs.get(2));

    // Gaps and out of bounds
    assertThat(strategy.runForPosition(125)).isNull();
    assertThat(strategy.runForPosition(500)).isNull();
  }

  /** Test stream join with empty positions list. */
  @Test
  public void testStreamJoinEmptyPositions() {
    List<Run> runs = Arrays.asList(new GenericRun(0, 0, 100));
    StreamJoinStrategy strategy = new StreamJoinStrategy(runs);

    List<Long> empty = new ArrayList<>();
    Map<Long, Run> results = strategy.runForPositions(empty);

    assertThat(results).isEmpty();
  }

  /** Test stream join with empty runs list. */
  @Test
  public void testStreamJoinEmptyRuns() {
    List<Run> empty = new ArrayList<>();
    StreamJoinStrategy strategy = new StreamJoinStrategy(empty);

    List<Long> positions = Arrays.asList(0L, 50L, 100L);
    Map<Long, Run> results = strategy.runForPositions(positions);

    assertThat(results).isEmpty();
  }

  /** Test stream join with positions all in gaps. */
  @Test
  public void testStreamJoinAllGaps() {
    List<Run> runs =
        Arrays.asList(
            new GenericRun(0, 0, 100), // [0, 100)
            new GenericRun(200, 100, 100)); // [200, 300) - gap at [100, 200)

    StreamJoinStrategy strategy = new StreamJoinStrategy(runs);

    // All positions in the gap
    List<Long> positions = Arrays.asList(100L, 125L, 150L, 175L, 199L);
    Map<Long, Run> results = strategy.runForPositions(positions);

    assertThat(results).isEmpty();
  }

  /** Test stream join with positions all before first run. */
  @Test
  public void testStreamJoinAllBefore() {
    List<Run> runs = Arrays.asList(new GenericRun(100, 0, 100)); // [100, 200)

    StreamJoinStrategy strategy = new StreamJoinStrategy(runs);

    // All positions before first run
    List<Long> positions = Arrays.asList(0L, 25L, 50L, 75L, 99L);
    Map<Long, Run> results = strategy.runForPositions(positions);

    assertThat(results).isEmpty();
  }

  /** Test stream join with positions all after last run. */
  @Test
  public void testStreamJoinAllAfter() {
    List<Run> runs = Arrays.asList(new GenericRun(0, 0, 100)); // [0, 100)

    StreamJoinStrategy strategy = new StreamJoinStrategy(runs);

    // All positions after last run
    List<Long> positions = Arrays.asList(100L, 200L, 300L, 400L, 500L);
    Map<Long, Run> results = strategy.runForPositions(positions);

    assertThat(results).isEmpty();
  }

  /** Test stream join matches other strategies for random data. */
  @Test
  public void testStreamJoinMatchesOtherStrategies() {
    Random rand = new Random(456);

    // Test 50 different configurations
    for (int config = 0; config < 50; config++) {
      int numRuns = 1 + rand.nextInt(50); // 1-50 runs
      List<Run> runs = generateSortedRunsWithGaps(numRuns, rand);

      // Create all strategies
      RemappingStrategy linear = new LinearSearchStrategy(runs);
      RemappingStrategy binary = new BinarySearchStrategy(runs);
      RemappingStrategy intervalTree = new IntervalTreeStrategy(runs);
      RemappingStrategy streamJoin = new StreamJoinStrategy(runs);

      // Generate sorted positions
      long maxPos =
          runs.get(runs.size() - 1).sourcePosition() + runs.get(runs.size() - 1).length();
      List<Long> sortedPositions = new ArrayList<>();
      for (int i = 0; i < 50; i++) {
        sortedPositions.add(nextLong(rand, maxPos + 1000));
      }
      sortedPositions.sort(Long::compareTo);

      // Test bulk lookup
      Map<Long, Run> linearResults = linear.runForPositions(sortedPositions);
      Map<Long, Run> binaryResults = binary.runForPositions(sortedPositions);
      Map<Long, Run> intervalTreeResults = intervalTree.runForPositions(sortedPositions);
      Map<Long, Run> streamJoinResults = streamJoin.runForPositions(sortedPositions);

      // All strategies should match
      assertThat(binaryResults).isEqualTo(linearResults);
      assertThat(intervalTreeResults).isEqualTo(linearResults);
      assertThat(streamJoinResults)
          .as("Stream join should match linear search for config %s", config)
          .isEqualTo(linearResults);

      // Also test single-position lookups
      for (Long pos : sortedPositions) {
        Run linearResult = linear.runForPosition(pos);
        Run streamJoinResult = streamJoin.runForPosition(pos);
        assertThat(streamJoinResult)
            .as("Stream join single lookup should match for config %s, position %s", config, pos)
            .isEqualTo(linearResult);
      }
    }
  }

  /** Test that stream join rejects unsorted runs. */
  @Test
  public void testStreamJoinRequiresSorted() {
    List<Run> unsorted =
        Arrays.asList(
            new GenericRun(150, 100, 50), // Wrong order
            new GenericRun(0, 0, 100));

    assertThatThrownBy(() -> new StreamJoinStrategy(unsorted))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be sorted");
  }

  // Helper methods

  /**
   * Generates sorted runs without gaps.
   *
   * @param numRuns number of runs to generate
   * @return list of sorted, contiguous runs
   */
  private List<Run> generateSortedRuns(int numRuns) {
    List<Run> runs = new ArrayList<>(numRuns);
    long pos = 0;
    long targetPos = 0;

    for (int i = 0; i < numRuns; i++) {
      long length = 1000; // Fixed length for simplicity
      runs.add(new GenericRun(pos, targetPos, length));
      pos += length;
      targetPos += length;
    }

    return runs;
  }

  /**
   * Generates sorted runs with random gaps.
   *
   * @param numRuns number of runs to generate
   * @param rand random number generator
   * @return list of sorted runs with gaps
   */
  private List<Run> generateSortedRunsWithGaps(int numRuns, Random rand) {
    List<Run> runs = new ArrayList<>(numRuns);
    long pos = rand.nextInt(1000); // Random starting position
    long targetPos = 0;

    for (int i = 0; i < numRuns; i++) {
      long length = 100 + rand.nextInt(900); // Random length 100-999
      runs.add(new GenericRun(pos, targetPos, length));

      pos += length;
      targetPos += length;

      // Add random gap (30% chance)
      if (rand.nextDouble() < 0.3) {
        pos += rand.nextInt(500);
      }
    }

    return runs;
  }

  /**
   * Helper to generate random long within bounds (Java 11 compatible).
   *
   * @param rand random number generator
   * @param bound upper bound (exclusive)
   * @return random long in range [0, bound)
   */
  private long nextLong(Random rand, long bound) {
    // Generate random long and map to range [0, bound)
    return Math.abs(rand.nextLong()) % bound;
  }
}
