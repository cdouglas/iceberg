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
import static org.assertj.core.api.Assertions.within;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.iceberg.CompactionMap.FileMapping;
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.GenericCompactionMap.GenericFileMapping;
import org.apache.iceberg.GenericCompactionMap.GenericRun;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for RemappingAlgorithmSelector.
 *
 * <p>The selector logic is based on empirical JMH benchmark data (January-February 2026), not
 * theoretical complexity analysis. Key findings:
 *
 * <ul>
 *   <li>UNSORTED: IntervalTree wins in most scenarios, but StreamJoin wins when m >> n
 *   <li>Very high m (>= 5000) with small n (<= 2000): StreamJoin wins regardless of sorted/gaps
 *   <li>SORTED + sparse (gap > 0.3): RangeQuery wins (can skip gaps)
 *   <li>SORTED + dense + bulk (m >= 100, n >= 10000): StreamJoin wins
 *   <li>SORTED + small m (m < 100): RangeQuery wins even for large n
 *   <li>SORTED + other: RangeQuery wins
 * </ul>
 */
public class TestRemappingAlgorithmSelector {

  @Test
  public void testSelectsRangeQueryForFewRunsSorted() {
    // m = 5 (few runs), sorted positions -> RangeQuery
    FileMapping mapping = createMapping(5);
    List<Long> sortedPositions = createSortedPositions(1000);

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, sortedPositions);

    assertThat(strategy).isInstanceOf(RangeQueryStrategy.class);
  }

  @Test
  public void testSelectsIntervalTreeForUnsortedData() {
    // Unsorted data with typical m values uses IntervalTree (wins 29/36 unsorted scenarios)
    // Exception: very high m (>= 5000) with small n (<= 2000) uses StreamJoin
    FileMapping mapping = createMapping(5);
    List<Long> unsortedPositions = createUnsortedPositions(1000);

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, unsortedPositions);

    assertThat(strategy).isInstanceOf(IntervalTreeStrategy.class);
  }

  @Test
  public void testSelectsStreamJoinForBulkSortedDense() {
    // m >= 100, n >= 10000, dense, sorted -> StreamJoin
    FileMapping mapping = createDenseMapping(100);
    List<Long> sortedPositions = createSortedPositions(10000);

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, sortedPositions);

    assertThat(strategy).isInstanceOf(StreamJoinStrategy.class);
  }

  @Test
  public void testSelectsRangeQueryForSortedWithGaps() {
    // Sparse (gap > 0.3), sorted -> RangeQuery (can skip gaps efficiently)
    FileMapping mapping = createMappingWithGaps(50, 0.4);
    List<Long> sortedPositions = createSortedPositions(10000);

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, sortedPositions);

    assertThat(strategy).isInstanceOf(RangeQueryStrategy.class);
  }

  @Test
  public void testSelectsIntervalTreeForUnsortedWithGaps() {
    // Unsorted always uses IntervalTree, regardless of gaps
    FileMapping mapping = createMappingWithGaps(50, 0.4);
    List<Long> unsortedPositions = createUnsortedPositions(10000);

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, unsortedPositions);

    assertThat(strategy).isInstanceOf(IntervalTreeStrategy.class);
  }

  @Test
  public void testSelectsIntervalTreeForMediumRunsUnsorted() {
    // m = 50, unsorted -> IntervalTree
    FileMapping mapping = createMapping(50);
    List<Long> unsortedPositions = createUnsortedPositions(1000);

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, unsortedPositions);

    assertThat(strategy).isInstanceOf(IntervalTreeStrategy.class);
  }

  @Test
  public void testSelectsIntervalTreeForLargeRunsUnsorted() {
    // m = 500, unsorted -> IntervalTree
    FileMapping mapping = createMapping(500);
    List<Long> positions = createUnsortedPositions(1000);

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, positions);

    assertThat(strategy).isInstanceOf(IntervalTreeStrategy.class);
  }

  @Test
  public void testSelectsRangeQueryForMediumRunsSorted() {
    // m = 50 (< 100), n = 1000 (< 10000), sorted, dense -> RangeQuery
    FileMapping mapping = createDenseMapping(50);
    List<Long> sortedPositions = createSortedPositions(1000);

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, sortedPositions);

    assertThat(strategy).isInstanceOf(RangeQueryStrategy.class);
  }

  @Test
  public void testGapRatioEstimation() {
    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();

    // Dense runs (no gaps)
    FileMapping dense = createDenseMapping(100);
    double denseGapRatio = selector.estimateGapRatio(dense);
    assertThat(denseGapRatio).isCloseTo(0.0, within(0.01));

    // Sparse runs (50% gaps)
    FileMapping sparse = createMappingWithGaps(100, 0.5);
    double sparseGapRatio = selector.estimateGapRatio(sparse);
    assertThat(sparseGapRatio).isCloseTo(0.5, within(0.05));
  }

  @Test
  public void testSortednessDetection() {
    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();

    // Sorted positions
    List<Long> sorted = createSortedPositions(1000);
    assertThat(selector.isSorted(sorted)).isTrue();

    // Unsorted positions
    List<Long> unsorted = createUnsortedPositions(1000);
    assertThat(selector.isSorted(unsorted)).isFalse();

    // Empty list
    assertThat(selector.isSorted(new ArrayList<>())).isTrue();

    // Single element
    assertThat(selector.isSorted(ImmutableList.of(42L))).isTrue();
  }

  @Test
  public void testEdgeCaseEmptyPositions() {
    FileMapping mapping = createMapping(10);
    List<Long> emptyPositions = new ArrayList<>();

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, emptyPositions);

    // Should return LinearSearch for empty positions
    assertThat(strategy).isInstanceOf(LinearSearchStrategy.class);
  }

  @Test
  public void testEdgeCaseNoRuns() {
    FileMapping mapping = createMapping(0);
    List<Long> positions = createSortedPositions(100);

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, positions);

    // Should return LinearSearch for no runs
    assertThat(strategy).isInstanceOf(LinearSearchStrategy.class);
  }

  @Test
  public void testStreamJoinThreshold() {
    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    List<Long> sortedPositions = createSortedPositions(10000);

    // m = 99, n = 10000, sorted, dense -> RangeQuery (below m threshold)
    // Benchmark evidence: For small m, RangeQuery wins even with large n
    FileMapping at99 = createDenseMapping(99);
    assertThat(selector.selectOptimal(at99, sortedPositions))
        .isInstanceOf(RangeQueryStrategy.class);

    // m = 100, n = 10000, sorted, dense -> StreamJoin (both thresholds met)
    FileMapping at100 = createDenseMapping(100);
    assertThat(selector.selectOptimal(at100, sortedPositions))
        .isInstanceOf(StreamJoinStrategy.class);

    // m = 100, n = 9999, sorted, dense -> RangeQuery (below n threshold)
    List<Long> positions9999 = createSortedPositions(9999);
    assertThat(selector.selectOptimal(at100, positions9999)).isInstanceOf(RangeQueryStrategy.class);

    // m = 10, n = 100000, sorted, dense -> RangeQuery (below m threshold)
    FileMapping at10 = createDenseMapping(10);
    List<Long> positions100k = createSortedPositions(100000);
    assertThat(selector.selectOptimal(at10, positions100k)).isInstanceOf(RangeQueryStrategy.class);
  }

  @Test
  public void testUnsortedUsesIntervalTreeForTypicalScenarios() {
    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    List<Long> unsortedPositions = createUnsortedPositions(10000);

    // Typical unsorted scenarios (m < 5000) should use IntervalTree
    assertThat(selector.selectOptimal(createMapping(5), unsortedPositions))
        .isInstanceOf(IntervalTreeStrategy.class);
    assertThat(selector.selectOptimal(createMapping(50), unsortedPositions))
        .isInstanceOf(IntervalTreeStrategy.class);
    assertThat(selector.selectOptimal(createMapping(500), unsortedPositions))
        .isInstanceOf(IntervalTreeStrategy.class);
    assertThat(selector.selectOptimal(createMappingWithGaps(50, 0.5), unsortedPositions))
        .isInstanceOf(IntervalTreeStrategy.class);
  }

  @Test
  public void testVeryHighRunsWithSmallPositionsUsesStreamJoin() {
    // Feb 2026 benchmark finding: when m >= 5000 and n <= 2000,
    // StreamJoin's O(n+m) beats IntervalTree because the scan through runs dominates
    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();

    // m = 5000, n = 1000, unsorted -> StreamJoin (very high m, small n)
    FileMapping highM = createDenseMapping(5000);
    List<Long> smallUnsorted = createUnsortedPositions(1000);
    assertThat(selector.selectOptimal(highM, smallUnsorted)).isInstanceOf(StreamJoinStrategy.class);

    // m = 5000, n = 1000, sorted -> StreamJoin (very high m, small n)
    List<Long> smallSorted = createSortedPositions(1000);
    assertThat(selector.selectOptimal(highM, smallSorted)).isInstanceOf(StreamJoinStrategy.class);

    // m = 10000, n = 2000, unsorted -> StreamJoin
    FileMapping veryHighM = createDenseMapping(10000);
    List<Long> mediumUnsorted = createUnsortedPositions(2000);
    assertThat(selector.selectOptimal(veryHighM, mediumUnsorted))
        .isInstanceOf(StreamJoinStrategy.class);

    // m = 10000, n = 2000, sorted -> StreamJoin
    List<Long> mediumSorted = createSortedPositions(2000);
    assertThat(selector.selectOptimal(veryHighM, mediumSorted)).isInstanceOf(StreamJoinStrategy.class);
  }

  @Test
  public void testVeryHighRunsWithLargePositionsUsesIntervalTree() {
    // When n is large (> 2000), even with very high m, IntervalTree is better for unsorted
    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();

    // m = 5000, n = 3000, unsorted -> IntervalTree (n > 2000, so normal rules apply)
    FileMapping highM = createDenseMapping(5000);
    List<Long> largeUnsorted = createUnsortedPositions(3000);
    assertThat(selector.selectOptimal(highM, largeUnsorted)).isInstanceOf(IntervalTreeStrategy.class);

    // m = 5000, n = 10000, unsorted -> IntervalTree
    List<Long> veryLargeUnsorted = createUnsortedPositions(10000);
    assertThat(selector.selectOptimal(highM, veryLargeUnsorted))
        .isInstanceOf(IntervalTreeStrategy.class);
  }

  @Test
  public void testVeryHighRunsThresholdBoundary() {
    // Test the exact boundary at m=5000, n=2000
    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();

    // m = 4999, n = 2000, unsorted -> IntervalTree (below m threshold)
    FileMapping belowThreshold = createDenseMapping(4999);
    List<Long> positions2000 = createUnsortedPositions(2000);
    assertThat(selector.selectOptimal(belowThreshold, positions2000))
        .isInstanceOf(IntervalTreeStrategy.class);

    // m = 5000, n = 2000, unsorted -> StreamJoin (at threshold)
    FileMapping atThreshold = createDenseMapping(5000);
    assertThat(selector.selectOptimal(atThreshold, positions2000))
        .isInstanceOf(StreamJoinStrategy.class);

    // m = 5000, n = 2001, unsorted -> IntervalTree (above n threshold)
    List<Long> positions2001 = createUnsortedPositions(2001);
    assertThat(selector.selectOptimal(atThreshold, positions2001))
        .isInstanceOf(IntervalTreeStrategy.class);
  }

  @Test
  public void testSortedNeverUsesIntervalTree() {
    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    List<Long> sortedPositions = createSortedPositions(10000);

    // All sorted scenarios should NOT use IntervalTree
    assertThat(selector.selectOptimal(createMapping(5), sortedPositions))
        .isNotInstanceOf(IntervalTreeStrategy.class);
    assertThat(selector.selectOptimal(createMapping(50), sortedPositions))
        .isNotInstanceOf(IntervalTreeStrategy.class);
    assertThat(selector.selectOptimal(createDenseMapping(100), sortedPositions))
        .isNotInstanceOf(IntervalTreeStrategy.class);
    assertThat(selector.selectOptimal(createMappingWithGaps(50, 0.5), sortedPositions))
        .isNotInstanceOf(IntervalTreeStrategy.class);
  }

  // Helper methods

  private FileMapping createMapping(int runCount) {
    List<Run> runs = new ArrayList<>();
    for (int i = 0; i < runCount; i++) {
      long sourcePos = i * 1000L;
      long targetPos = i * 1000L;
      runs.add(new GenericRun(sourcePos, targetPos, 1000));
    }
    return new GenericFileMapping("s3://bucket/source.parquet", "s3://bucket/target.parquet", runs);
  }

  private FileMapping createDenseMapping(int runCount) {
    // Create runs with no gaps (consecutive)
    List<Run> runs = new ArrayList<>();
    for (int i = 0; i < runCount; i++) {
      long sourcePos = i * 1000L;
      long targetPos = i * 1000L;
      runs.add(new GenericRun(sourcePos, targetPos, 1000));
    }
    return new GenericFileMapping("s3://bucket/source.parquet", "s3://bucket/target.parquet", runs);
  }

  private FileMapping createMappingWithGaps(int runCount, double gapRatio) {
    // Create runs with specified gap ratio
    List<Run> runs = new ArrayList<>();
    long runLength = 1000;
    long gapLength = (long) (runLength * gapRatio / (1 - gapRatio));
    long currentPos = 0;

    for (int i = 0; i < runCount; i++) {
      runs.add(new GenericRun(currentPos, i * runLength, runLength));
      currentPos += runLength + gapLength; // Add gap after each run
    }
    return new GenericFileMapping("s3://bucket/source.parquet", "s3://bucket/target.parquet", runs);
  }

  private List<Long> createSortedPositions(int count) {
    List<Long> positions = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      positions.add((long) i);
    }
    return positions;
  }

  private List<Long> createUnsortedPositions(int count) {
    List<Long> positions = createSortedPositions(count);
    Collections.shuffle(positions, new Random(42)); // Fixed seed for reproducibility
    return positions;
  }
}
