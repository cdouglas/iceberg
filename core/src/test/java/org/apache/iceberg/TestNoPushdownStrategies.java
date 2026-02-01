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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.GenericCompactionMap.GenericRun;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for NoPushdown strategy variants.
 *
 * <p>These strategies are for benchmarking only and deliberately disable predicate pushdown
 * optimization to measure its impact.
 */
public class TestNoPushdownStrategies {

  @BeforeEach
  public void setUp() {
    // Enable benchmark mode for tests
    StreamJoinNoPushdownStrategy.enableForBenchmarking(true);
    RangeQueryNoPushdownStrategy.enableForBenchmarking(true);
  }

  @AfterEach
  public void tearDown() {
    // Disable benchmark mode after tests
    StreamJoinNoPushdownStrategy.enableForBenchmarking(false);
    RangeQueryNoPushdownStrategy.enableForBenchmarking(false);
  }

  // =========================================================================
  // StreamJoinNoPushdownStrategy Tests
  // =========================================================================

  @Test
  public void testStreamJoinNoPushdownDisabledByDefault() {
    // Disable benchmark mode
    StreamJoinNoPushdownStrategy.enableForBenchmarking(false);

    List<Run> runs = createSimpleRuns();

    assertThatThrownBy(() -> new StreamJoinNoPushdownStrategy(runs))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("disabled")
        .hasMessageContaining("benchmarking only");
  }

  @Test
  public void testStreamJoinNoPushdownCanBeEnabled() {
    List<Run> runs = createSimpleRuns();

    // Should not throw when enabled
    StreamJoinNoPushdownStrategy strategy = new StreamJoinNoPushdownStrategy(runs);
    assertThat(strategy.name()).isEqualTo("stream-join-no-pushdown");
  }

  @Test
  public void testStreamJoinNoPushdownBenchmarkModeFlag() {
    assertThat(StreamJoinNoPushdownStrategy.isBenchmarkModeEnabled()).isTrue();

    StreamJoinNoPushdownStrategy.enableForBenchmarking(false);
    assertThat(StreamJoinNoPushdownStrategy.isBenchmarkModeEnabled()).isFalse();

    StreamJoinNoPushdownStrategy.enableForBenchmarking(true);
    assertThat(StreamJoinNoPushdownStrategy.isBenchmarkModeEnabled()).isTrue();
  }

  @Test
  public void testStreamJoinNoPushdownProducesSameResultsAsOptimized() {
    List<Run> runs = createSimpleRuns();
    List<Long> positions = Arrays.asList(5L, 50L, 150L, 250L, 350L);

    StreamJoinStrategy optimized = new StreamJoinStrategy(runs);
    StreamJoinNoPushdownStrategy noPushdown = new StreamJoinNoPushdownStrategy(runs);

    Map<Long, Run> optimizedResult = optimized.runForPositions(positions);
    Map<Long, Run> noPushdownResult = noPushdown.runForPositions(positions);

    assertThat(noPushdownResult).isEqualTo(optimizedResult);
  }

  @Test
  public void testStreamJoinNoPushdownWithPartialCoverage() {
    // Create runs spanning 0-10000
    List<Run> runs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      runs.add(new GenericRun(i * 1000L, i * 1000L, 1000));
    }

    // Positions only in first 10% of range (0-1000)
    List<Long> positions = Arrays.asList(100L, 200L, 300L, 500L, 800L);

    StreamJoinStrategy optimized = new StreamJoinStrategy(runs);
    StreamJoinNoPushdownStrategy noPushdown = new StreamJoinNoPushdownStrategy(runs);

    Map<Long, Run> optimizedResult = optimized.runForPositions(positions);
    Map<Long, Run> noPushdownResult = noPushdown.runForPositions(positions);

    // Results should be identical
    assertThat(noPushdownResult).isEqualTo(optimizedResult);
    assertThat(noPushdownResult).hasSize(5);

    // All positions should map to the first run [0-1000)
    for (Long pos : positions) {
      assertThat(noPushdownResult.get(pos).sourcePosition()).isEqualTo(0L);
    }
  }

  @Test
  public void testStreamJoinNoPushdownSinglePosition() {
    List<Run> runs = createSimpleRuns();

    StreamJoinNoPushdownStrategy strategy = new StreamJoinNoPushdownStrategy(runs);

    // Single position lookup should work
    Run run = strategy.runForPosition(50L);
    assertThat(run).isNotNull();
    assertThat(run.sourcePosition()).isEqualTo(0L);
  }

  @Test
  public void testStreamJoinNoPushdownEmptyInputs() {
    List<Run> runs = createSimpleRuns();
    StreamJoinNoPushdownStrategy strategy = new StreamJoinNoPushdownStrategy(runs);

    // Empty positions
    assertThat(strategy.runForPositions(Collections.emptyList())).isEmpty();
    assertThat(strategy.runForPositions(null)).isEmpty();
  }

  @Test
  public void testStreamJoinNoPushdownUnsortedPositions() {
    List<Run> runs = createSimpleRuns();
    List<Long> unsortedPositions = Arrays.asList(350L, 50L, 250L, 150L);

    StreamJoinStrategy optimized = new StreamJoinStrategy(runs);
    StreamJoinNoPushdownStrategy noPushdown = new StreamJoinNoPushdownStrategy(runs);

    Map<Long, Run> optimizedResult = optimized.runForPositions(unsortedPositions);
    Map<Long, Run> noPushdownResult = noPushdown.runForPositions(unsortedPositions);

    // Results should be identical (both fall back to binary search for unsorted)
    assertThat(noPushdownResult).isEqualTo(optimizedResult);
  }

  // =========================================================================
  // RangeQueryNoPushdownStrategy Tests
  // =========================================================================

  @Test
  public void testRangeQueryNoPushdownDisabledByDefault() {
    // Disable benchmark mode
    RangeQueryNoPushdownStrategy.enableForBenchmarking(false);

    List<Run> runs = createSimpleRuns();

    assertThatThrownBy(() -> new RangeQueryNoPushdownStrategy(runs))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("disabled")
        .hasMessageContaining("benchmarking only");
  }

  @Test
  public void testRangeQueryNoPushdownCanBeEnabled() {
    List<Run> runs = createSimpleRuns();

    // Should not throw when enabled
    RangeQueryNoPushdownStrategy strategy = new RangeQueryNoPushdownStrategy(runs);
    assertThat(strategy.name()).isEqualTo("range-query-no-pushdown");
  }

  @Test
  public void testRangeQueryNoPushdownBenchmarkModeFlag() {
    assertThat(RangeQueryNoPushdownStrategy.isBenchmarkModeEnabled()).isTrue();

    RangeQueryNoPushdownStrategy.enableForBenchmarking(false);
    assertThat(RangeQueryNoPushdownStrategy.isBenchmarkModeEnabled()).isFalse();

    RangeQueryNoPushdownStrategy.enableForBenchmarking(true);
    assertThat(RangeQueryNoPushdownStrategy.isBenchmarkModeEnabled()).isTrue();
  }

  @Test
  public void testRangeQueryNoPushdownProducesSameResultsAsOptimized() {
    List<Run> runs = createSimpleRuns();
    List<Long> positions = Arrays.asList(5L, 50L, 150L, 250L, 350L);

    RangeQueryStrategy optimized = new RangeQueryStrategy(runs);
    RangeQueryNoPushdownStrategy noPushdown = new RangeQueryNoPushdownStrategy(runs);

    Map<Long, Run> optimizedResult = optimized.runForPositions(positions);
    Map<Long, Run> noPushdownResult = noPushdown.runForPositions(positions);

    assertThat(noPushdownResult).isEqualTo(optimizedResult);
  }

  @Test
  public void testRangeQueryNoPushdownWithPartialCoverage() {
    // Create runs spanning 0-10000
    List<Run> runs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      runs.add(new GenericRun(i * 1000L, i * 1000L, 1000));
    }

    // Positions only in last 10% of range (9000-10000)
    List<Long> positions = Arrays.asList(9100L, 9200L, 9500L, 9800L, 9900L);

    RangeQueryStrategy optimized = new RangeQueryStrategy(runs);
    RangeQueryNoPushdownStrategy noPushdown = new RangeQueryNoPushdownStrategy(runs);

    Map<Long, Run> optimizedResult = optimized.runForPositions(positions);
    Map<Long, Run> noPushdownResult = noPushdown.runForPositions(positions);

    // Results should be identical
    assertThat(noPushdownResult).isEqualTo(optimizedResult);
    assertThat(noPushdownResult).hasSize(5);

    // All positions should map to the last run [9000-10000)
    for (Long pos : positions) {
      assertThat(noPushdownResult.get(pos).sourcePosition()).isEqualTo(9000L);
    }
  }

  @Test
  public void testRangeQueryNoPushdownSinglePosition() {
    List<Run> runs = createSimpleRuns();

    RangeQueryNoPushdownStrategy strategy = new RangeQueryNoPushdownStrategy(runs);

    // Single position lookup should work
    Run run = strategy.runForPosition(250L);
    assertThat(run).isNotNull();
    assertThat(run.sourcePosition()).isEqualTo(200L);
  }

  @Test
  public void testRangeQueryNoPushdownEmptyInputs() {
    List<Run> runs = createSimpleRuns();
    RangeQueryNoPushdownStrategy strategy = new RangeQueryNoPushdownStrategy(runs);

    // Empty positions
    assertThat(strategy.runForPositions(Collections.emptyList())).isEmpty();
    assertThat(strategy.runForPositions(null)).isEmpty();
  }

  @Test
  public void testRangeQueryNoPushdownUnsortedPositions() {
    List<Run> runs = createSimpleRuns();
    List<Long> unsortedPositions = Arrays.asList(350L, 50L, 250L, 150L);

    RangeQueryStrategy optimized = new RangeQueryStrategy(runs);
    RangeQueryNoPushdownStrategy noPushdown = new RangeQueryNoPushdownStrategy(runs);

    Map<Long, Run> optimizedResult = optimized.runForPositions(unsortedPositions);
    Map<Long, Run> noPushdownResult = noPushdown.runForPositions(unsortedPositions);

    // Results should be identical
    assertThat(noPushdownResult).isEqualTo(optimizedResult);
  }

  // =========================================================================
  // Cross-Strategy Consistency Tests
  // =========================================================================

  @Test
  public void testAllStrategiesProduceSameResults() {
    List<Run> runs = createSimpleRuns();
    List<Long> positions = Arrays.asList(5L, 50L, 99L, 150L, 199L, 250L, 350L, 399L);

    // All strategies should produce identical results
    LinearSearchStrategy linear = new LinearSearchStrategy(runs);
    BinarySearchStrategy binary = new BinarySearchStrategy(runs);
    IntervalTreeStrategy intervalTree = new IntervalTreeStrategy(runs);
    StreamJoinStrategy streamJoin = new StreamJoinStrategy(runs);
    RangeQueryStrategy rangeQuery = new RangeQueryStrategy(runs);
    StreamJoinNoPushdownStrategy streamJoinNoPushdown = new StreamJoinNoPushdownStrategy(runs);
    RangeQueryNoPushdownStrategy rangeQueryNoPushdown = new RangeQueryNoPushdownStrategy(runs);

    Map<Long, Run> expected = linear.runForPositions(positions);

    assertThat(binary.runForPositions(positions)).isEqualTo(expected);
    assertThat(intervalTree.runForPositions(positions)).isEqualTo(expected);
    assertThat(streamJoin.runForPositions(positions)).isEqualTo(expected);
    assertThat(rangeQuery.runForPositions(positions)).isEqualTo(expected);
    assertThat(streamJoinNoPushdown.runForPositions(positions)).isEqualTo(expected);
    assertThat(rangeQueryNoPushdown.runForPositions(positions)).isEqualTo(expected);
  }

  @Test
  public void testAllStrategiesHandleGaps() {
    List<Run> runs = createSimpleRuns();
    // Position 125 is in a gap between runs [0-100) and [200-300)
    List<Long> positions = Arrays.asList(50L, 125L, 250L);

    LinearSearchStrategy linear = new LinearSearchStrategy(runs);
    StreamJoinNoPushdownStrategy streamJoinNoPushdown = new StreamJoinNoPushdownStrategy(runs);
    RangeQueryNoPushdownStrategy rangeQueryNoPushdown = new RangeQueryNoPushdownStrategy(runs);

    Map<Long, Run> expected = linear.runForPositions(positions);

    // Gap position should not be in results
    assertThat(expected).hasSize(2);
    assertThat(expected).containsKey(50L);
    assertThat(expected).containsKey(250L);
    assertThat(expected).doesNotContainKey(125L);

    assertThat(streamJoinNoPushdown.runForPositions(positions)).isEqualTo(expected);
    assertThat(rangeQueryNoPushdown.runForPositions(positions)).isEqualTo(expected);
  }

  @Test
  public void testNoPushdownWithManyRuns() {
    // Create 1000 runs to simulate high-run-count scenario
    List<Run> runs = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      runs.add(new GenericRun(i * 100L, i * 100L, 100));
    }

    // Positions only in first 10 runs (1% of range)
    List<Long> positions = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      positions.add((long) (i * 10));
    }

    StreamJoinStrategy streamJoin = new StreamJoinStrategy(runs);
    StreamJoinNoPushdownStrategy streamJoinNoPushdown = new StreamJoinNoPushdownStrategy(runs);
    RangeQueryStrategy rangeQuery = new RangeQueryStrategy(runs);
    RangeQueryNoPushdownStrategy rangeQueryNoPushdown = new RangeQueryNoPushdownStrategy(runs);

    Map<Long, Run> streamJoinResult = streamJoin.runForPositions(positions);
    Map<Long, Run> streamJoinNoPushdownResult = streamJoinNoPushdown.runForPositions(positions);
    Map<Long, Run> rangeQueryResult = rangeQuery.runForPositions(positions);
    Map<Long, Run> rangeQueryNoPushdownResult = rangeQueryNoPushdown.runForPositions(positions);

    // All should produce same results
    assertThat(streamJoinNoPushdownResult).isEqualTo(streamJoinResult);
    assertThat(rangeQueryNoPushdownResult).isEqualTo(rangeQueryResult);
    assertThat(streamJoinResult).isEqualTo(rangeQueryResult);
  }

  // =========================================================================
  // Helper Methods
  // =========================================================================

  /**
   * Creates simple test runs:
   *
   * <pre>
   * Run 0: [0, 100) -> [0, 100)
   * Run 1: [200, 300) -> [100, 200)  (gap at 100-200)
   * Run 2: [300, 400) -> [200, 300)
   * </pre>
   */
  private List<Run> createSimpleRuns() {
    List<Run> runs = new ArrayList<>();
    runs.add(new GenericRun(0, 0, 100));
    runs.add(new GenericRun(200, 100, 100)); // Gap at 100-200
    runs.add(new GenericRun(300, 200, 100));
    return runs;
  }
}
