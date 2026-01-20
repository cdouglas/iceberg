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

/** Unit tests for RemappingAlgorithmSelector. */
public class TestRemappingAlgorithmSelector {

  @Test
  public void testSelectsRangeQueryForFewRunsSorted() {
    // m = 5 (< 10 threshold), sorted positions
    FileMapping mapping = createMapping(5);
    List<Long> sortedPositions = createSortedPositions(1000);

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, sortedPositions);

    assertThat(strategy).isInstanceOf(RangeQueryStrategy.class);
  }

  @Test
  public void testSelectsBinarySearchForFewRunsUnsorted() {
    // m = 5 (< 10 threshold), unsorted positions
    // RangeQuery would require O(n log n) sorting, so BinarySearch is better
    FileMapping mapping = createMapping(5);
    List<Long> unsortedPositions = createUnsortedPositions(1000);

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, unsortedPositions);

    assertThat(strategy).isInstanceOf(BinarySearchStrategy.class);
  }

  @Test
  public void testSelectsStreamJoinForSortedPositions() {
    // m = 50, n = 10000, sorted (m < 100, so StreamJoin is selected)
    FileMapping mapping = createMapping(50);
    List<Long> sortedPositions = createSortedPositions(10000);

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, sortedPositions);

    assertThat(strategy).isInstanceOf(StreamJoinStrategy.class);
  }

  @Test
  public void testSelectsRangeQueryForHighFanInWithGaps() {
    // m = 50, n = 10000 (n/m = 200 > 100), 40% gaps
    FileMapping mapping = createMappingWithGaps(50, 0.4);
    List<Long> positions = createPositions(10000);

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, positions);

    assertThat(strategy).isInstanceOf(RangeQueryStrategy.class);
  }

  @Test
  public void testSelectsBinarySearchForMediumRuns() {
    // m = 50 (< 100 threshold), unsorted
    FileMapping mapping = createMapping(50);
    List<Long> unsortedPositions = createUnsortedPositions(1000);

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, unsortedPositions);

    assertThat(strategy).isInstanceOf(BinarySearchStrategy.class);
  }

  @Test
  public void testSelectsIntervalTreeForLargeRuns() {
    // m = 500 (> 100 threshold)
    FileMapping mapping = createMapping(500);
    List<Long> positions = createPositions(1000);

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, positions);

    assertThat(strategy).isInstanceOf(IntervalTreeStrategy.class);
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
    List<Long> positions = createPositions(100);

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, positions);

    // Should return LinearSearch for no runs
    assertThat(strategy).isInstanceOf(LinearSearchStrategy.class);
  }

  @Test
  public void testHighFanInDenseScenario() {
    // High fan-in (n/m > 100) but dense (low gap ratio)
    // Should compare costs and potentially use StreamJoin if sorted
    FileMapping mapping = createDenseMapping(50); // m = 50, dense
    List<Long> sortedPositions = createSortedPositions(10000); // n = 10000, n/m = 200

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, sortedPositions);

    // Should select StreamJoin (sorted + high n/m + dense)
    assertThat(strategy).isInstanceOf(StreamJoinStrategy.class);
  }

  @Test
  public void testBoundaryConditions() {
    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();

    // Exactly at FEW_RUNS_THRESHOLD (10) - uses unsorted positions
    FileMapping at10 = createMapping(10);
    List<Long> unsortedPositions = createUnsortedPositions(1000);

    RemappingStrategy strategyAt10 = selector.selectOptimal(at10, unsortedPositions);
    // At threshold, should not use RangeQuery (threshold is < 10)
    assertThat(strategyAt10).isNotInstanceOf(RangeQueryStrategy.class);

    // Just below FEW_RUNS_THRESHOLD with sorted positions -> RangeQuery
    FileMapping at9 = createMapping(9);
    List<Long> sortedPositions = createSortedPositions(1000);
    RemappingStrategy strategyAt9Sorted = selector.selectOptimal(at9, sortedPositions);
    assertThat(strategyAt9Sorted).isInstanceOf(RangeQueryStrategy.class);

    // Just below FEW_RUNS_THRESHOLD with unsorted positions -> BinarySearch
    RemappingStrategy strategyAt9Unsorted = selector.selectOptimal(at9, unsortedPositions);
    assertThat(strategyAt9Unsorted).isInstanceOf(BinarySearchStrategy.class);

    // Exactly at BINARY_SEARCH_THRESHOLD (100)
    FileMapping at100 = createMapping(100);
    RemappingStrategy strategyAt100 = selector.selectOptimal(at100, unsortedPositions);
    // At threshold, should not use BinarySearch (threshold is < 100)
    assertThat(strategyAt100).isNotInstanceOf(BinarySearchStrategy.class);

    // Just below BINARY_SEARCH_THRESHOLD
    FileMapping at99 = createMapping(99);
    RemappingStrategy strategyAt99 = selector.selectOptimal(at99, unsortedPositions);
    assertThat(strategyAt99).isInstanceOf(BinarySearchStrategy.class);
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

  private List<Long> createPositions(int count) {
    // Create random positions (not necessarily sorted)
    List<Long> positions = new ArrayList<>();
    Random rand = new Random(42); // Fixed seed for reproducibility
    for (int i = 0; i < count; i++) {
      positions.add((long) rand.nextInt(100000));
    }
    return positions;
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
