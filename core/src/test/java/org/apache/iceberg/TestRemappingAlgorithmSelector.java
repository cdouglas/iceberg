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
 * <p>The selector logic is based on empirical JMH benchmark data (January 2026), not theoretical
 * complexity analysis. Key findings:
 *
 * <ul>
 *   <li>UNSORTED: IntervalTree wins regardless of m, n, or gaps
 *   <li>SORTED + sparse: RangeQuery wins (can skip gaps)
 *   <li>SORTED + dense + bulk (m>=100, n>=10000): StreamJoin wins
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
    // Unsorted data always uses IntervalTree (benchmark evidence: wins 46/54 unsorted scenarios)
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

    // m = 99, n = 10000, sorted, dense -> StreamJoin (n threshold met, m irrelevant)
    // Benchmark evidence: StreamJoin wins for dense sorted data regardless of m
    FileMapping at99 = createDenseMapping(99);
    assertThat(selector.selectOptimal(at99, sortedPositions))
        .isInstanceOf(StreamJoinStrategy.class);

    // m = 100, n = 10000, sorted, dense -> StreamJoin (n threshold met)
    FileMapping at100 = createDenseMapping(100);
    assertThat(selector.selectOptimal(at100, sortedPositions))
        .isInstanceOf(StreamJoinStrategy.class);

    // m = 100, n = 9999, sorted, dense -> RangeQuery (below n threshold)
    List<Long> positions9999 = createSortedPositions(9999);
    assertThat(selector.selectOptimal(at100, positions9999)).isInstanceOf(RangeQueryStrategy.class);
  }

  @Test
  public void testUnsortedAlwaysUsesIntervalTree() {
    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    List<Long> unsortedPositions = createUnsortedPositions(10000);

    // All unsorted scenarios should use IntervalTree
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
