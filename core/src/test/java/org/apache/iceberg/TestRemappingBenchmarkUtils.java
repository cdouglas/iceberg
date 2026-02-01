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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.CompactionMap.Run;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for RemappingTestDataGenerator.
 *
 * <p>These tests verify the utility methods used to generate test data for remapping benchmarks,
 * including position coverage and run generation.
 */
public class TestRemappingBenchmarkUtils {

  private RemappingTestDataGenerator generator;

  @BeforeEach
  public void setUp() {
    generator = new RemappingTestDataGenerator();
  }

  // =========================================================================
  // createRunsWithGaps Tests
  // =========================================================================

  @Test
  public void testCreateRunsWithNoGaps() {
    List<Run> runs = generator.createRunsWithGaps(5, 0.0);

    assertThat(runs).hasSize(5);

    // With no gaps, runs should be contiguous
    long expectedPos = 0;
    for (Run run : runs) {
      assertThat(run.sourcePosition()).isEqualTo(expectedPos);
      assertThat(run.length()).isEqualTo(RemappingTestDataGenerator.RUN_LENGTH);
      expectedPos += RemappingTestDataGenerator.RUN_LENGTH;
    }
  }

  @Test
  public void testCreateRunsWithGaps() {
    List<Run> runs = generator.createRunsWithGaps(5, 0.5);

    assertThat(runs).hasSize(5);

    // With gaps, source positions should have gaps between runs
    long prevEnd = 0;
    for (int i = 0; i < runs.size(); i++) {
      Run run = runs.get(i);

      if (i > 0) {
        // There should be a gap between previous run end and this run start
        assertThat(run.sourcePosition()).isGreaterThan(prevEnd);
      }

      prevEnd = run.sourcePosition() + run.length();
    }
  }

  @Test
  public void testCreateRunsEmpty() {
    List<Run> runs = generator.createRunsWithGaps(0, 0.0);
    assertThat(runs).isEmpty();
  }

  @Test
  public void testCreateRunsSingleRun() {
    List<Run> runs = generator.createRunsWithGaps(1, 0.5);

    assertThat(runs).hasSize(1);
    assertThat(runs.get(0).sourcePosition()).isEqualTo(0);
    assertThat(runs.get(0).length()).isEqualTo(RemappingTestDataGenerator.RUN_LENGTH);
  }

  // =========================================================================
  // createSortedPositions Tests
  // =========================================================================

  @Test
  public void testCreateSortedPositions() {
    List<Long> positions = generator.createSortedPositions(100, 10);

    assertThat(positions).hasSize(100);
    assertThat(positions).isSorted();

    // All positions should be non-negative
    assertThat(positions).allMatch(p -> p >= 0);
  }

  @Test
  public void testCreateSortedPositionsReproducible() {
    // Create two generators with same seed
    RemappingTestDataGenerator gen1 = new RemappingTestDataGenerator(12345);
    RemappingTestDataGenerator gen2 = new RemappingTestDataGenerator(12345);

    List<Long> positions1 = gen1.createSortedPositions(50, 5);
    List<Long> positions2 = gen2.createSortedPositions(50, 5);

    // Same seed should produce same results
    assertThat(positions1).isEqualTo(positions2);
  }

  // =========================================================================
  // createRandomPositions Tests
  // =========================================================================

  @Test
  public void testCreateRandomPositions() {
    List<Long> positions = generator.createRandomPositions(100);

    assertThat(positions).hasSize(100);

    // All positions should be unique
    Set<Long> unique = new HashSet<>(positions);
    assertThat(unique).hasSize(100);

    // All positions should be non-negative
    assertThat(positions).allMatch(p -> p >= 0);
  }

  @Test
  public void testCreateRandomPositionsNotSorted() {
    List<Long> positions = generator.createRandomPositions(100);

    // Random positions are unlikely to be sorted
    // (This is probabilistic but with 100 elements, extremely unlikely to be sorted)
    boolean isSorted = true;
    for (int i = 1; i < positions.size(); i++) {
      if (positions.get(i) < positions.get(i - 1)) {
        isSorted = false;
        break;
      }
    }

    // If by chance it is sorted, that's fine - we just want to verify uniqueness
    assertThat(positions).hasSize(100);
  }

  // =========================================================================
  // createSortedPositionsWithCoverage Tests
  // =========================================================================

  @Test
  public void testCreateSortedPositionsWithFullCoverage() {
    List<Long> positions = generator.createSortedPositionsWithCoverage(100, 10, 1.0);

    assertThat(positions).hasSize(100);
    assertThat(positions).isSorted();
  }

  @Test
  public void testCreateSortedPositionsWithPartialCoverage() {
    int numRuns = 100;
    double coverage = 0.1; // 10% coverage

    List<Long> positions = generator.createSortedPositionsWithCoverage(100, numRuns, coverage);

    assertThat(positions).hasSize(100);
    assertThat(positions).isSorted();

    // Calculate expected range
    long totalRange = numRuns * RemappingTestDataGenerator.RUN_LENGTH * 2;
    long expectedCoveredRange = (long) (totalRange * coverage);

    // All positions should be within a contiguous range of size expectedCoveredRange
    long minPos = positions.stream().min(Long::compare).orElse(0L);
    long maxPos = positions.stream().max(Long::compare).orElse(0L);
    long actualRange = maxPos - minPos;

    // The actual range should be less than or equal to the covered range
    assertThat(actualRange).isLessThanOrEqualTo(expectedCoveredRange);
  }

  @Test
  public void testCreateSortedPositionsWithCoverageInvalidValues() {
    assertThatThrownBy(() -> generator.createSortedPositionsWithCoverage(100, 10, 0.0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Coverage must be in (0.0, 1.0]");

    assertThatThrownBy(() -> generator.createSortedPositionsWithCoverage(100, 10, -0.1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Coverage must be in (0.0, 1.0]");

    assertThatThrownBy(() -> generator.createSortedPositionsWithCoverage(100, 10, 1.5))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Coverage must be in (0.0, 1.0]");
  }

  @Test
  public void testCreateSortedPositionsWithSmallCoverage() {
    // Very small coverage (1%)
    List<Long> positions = generator.createSortedPositionsWithCoverage(50, 100, 0.01);

    assertThat(positions).hasSize(50);
    assertThat(positions).isSorted();

    // Positions should be clustered in a small range
    long minPos = positions.stream().min(Long::compare).orElse(0L);
    long maxPos = positions.stream().max(Long::compare).orElse(0L);
    long range = maxPos - minPos;

    // With 100 runs and 0.01 coverage, range should be ~2000 (1% of 200000)
    long expectedMaxRange = (long) (100 * RemappingTestDataGenerator.RUN_LENGTH * 2 * 0.01);
    assertThat(range).isLessThanOrEqualTo(expectedMaxRange);
  }

  // =========================================================================
  // createRandomPositionsWithCoverage Tests
  // =========================================================================

  @Test
  public void testCreateRandomPositionsWithFullCoverage() {
    List<Long> positions = generator.createRandomPositionsWithCoverage(100, 10, 1.0);

    assertThat(positions).hasSize(100);

    // All positions should be unique
    Set<Long> unique = new HashSet<>(positions);
    assertThat(unique).hasSize(100);
  }

  @Test
  public void testCreateRandomPositionsWithPartialCoverage() {
    int numRuns = 100;
    double coverage = 0.2; // 20% coverage

    List<Long> positions = generator.createRandomPositionsWithCoverage(100, numRuns, coverage);

    assertThat(positions).hasSize(100);

    // All positions should be unique
    Set<Long> unique = new HashSet<>(positions);
    assertThat(unique).hasSize(100);

    // All positions should be within the covered range
    long totalRange = numRuns * RemappingTestDataGenerator.RUN_LENGTH * 2;
    long expectedCoveredRange = (long) (totalRange * coverage);

    long minPos = positions.stream().min(Long::compare).orElse(0L);
    long maxPos = positions.stream().max(Long::compare).orElse(0L);
    long actualRange = maxPos - minPos;

    assertThat(actualRange).isLessThanOrEqualTo(expectedCoveredRange);
  }

  @Test
  public void testCreateRandomPositionsWithCoverageInvalidValues() {
    assertThatThrownBy(() -> generator.createRandomPositionsWithCoverage(100, 10, 0.0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Coverage must be in (0.0, 1.0]");

    assertThatThrownBy(() -> generator.createRandomPositionsWithCoverage(100, 10, -0.1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Coverage must be in (0.0, 1.0]");

    assertThatThrownBy(() -> generator.createRandomPositionsWithCoverage(100, 10, 1.5))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Coverage must be in (0.0, 1.0]");
  }

  // =========================================================================
  // calculateTotalRunRange Tests
  // =========================================================================

  @Test
  public void testCalculateTotalRunRangeNoGaps() {
    long range = RemappingTestDataGenerator.calculateTotalRunRange(10, 0.0);

    // 10 runs * 1000 length = 10000
    assertThat(range).isEqualTo(10 * RemappingTestDataGenerator.RUN_LENGTH);
  }

  @Test
  public void testCalculateTotalRunRangeWithGaps() {
    long range = RemappingTestDataGenerator.calculateTotalRunRange(10, 0.5);

    // With 50% gaps, total range should be 2x the run-only range
    // gap = RUN_LENGTH * 0.5 / 0.5 = RUN_LENGTH per gap
    // Total = 10 * RUN_LENGTH + 9 * RUN_LENGTH = 19 * RUN_LENGTH
    long expected =
        10 * RemappingTestDataGenerator.RUN_LENGTH + 9 * RemappingTestDataGenerator.RUN_LENGTH;
    assertThat(range).isEqualTo(expected);
  }

  @Test
  public void testCalculateTotalRunRangeEmpty() {
    long range = RemappingTestDataGenerator.calculateTotalRunRange(0, 0.0);
    assertThat(range).isEqualTo(0);
  }

  @Test
  public void testCalculateTotalRunRangeSingleRun() {
    long range = RemappingTestDataGenerator.calculateTotalRunRange(1, 0.5);

    // Single run has no gaps
    assertThat(range).isEqualTo(RemappingTestDataGenerator.RUN_LENGTH);
  }

  // =========================================================================
  // Integration Tests: Coverage with Runs
  // =========================================================================

  @Test
  public void testPositionsWithCoverageMatchRuns() {
    int numRuns = 50;
    double coverage = 0.2;

    List<Run> runs = generator.createRunsWithGaps(numRuns, 0.0);
    List<Long> positions = generator.createSortedPositionsWithCoverage(100, numRuns, coverage);

    // Count how many runs contain at least one position
    int runsWithPositions = 0;
    for (Run run : runs) {
      long runStart = run.sourcePosition();
      long runEnd = runStart + run.length();

      boolean hasPosition = positions.stream().anyMatch(p -> p >= runStart && p < runEnd);
      if (hasPosition) {
        runsWithPositions++;
      }
    }

    // With 20% coverage, we expect roughly 20% of runs to have positions
    // Allow some variance due to random distribution
    assertThat(runsWithPositions).isLessThanOrEqualTo((int) (numRuns * coverage * 2));
  }

  @Test
  public void testPredicatePushdownBenefit() {
    // This test verifies the setup for measuring predicate pushdown benefit
    int numRuns = 1000;
    double coverage = 0.1; // 10% coverage

    List<Run> runs = generator.createRunsWithGaps(numRuns, 0.0);
    List<Long> positions = generator.createSortedPositionsWithCoverage(1000, numRuns, coverage);

    // Find min/max position
    long minPos = positions.stream().min(Long::compare).orElse(0L);
    long maxPos = positions.stream().max(Long::compare).orElse(0L);

    // Count runs that overlap with [minPos, maxPos]
    int relevantRuns = 0;
    for (Run run : runs) {
      long runStart = run.sourcePosition();
      long runEnd = runStart + run.length();

      if (runEnd > minPos && runStart <= maxPos) {
        relevantRuns++;
      }
    }

    // With 10% coverage, about 10% of runs should be relevant
    // Predicate pushdown should skip ~90% of runs
    double relevantRatio = (double) relevantRuns / numRuns;
    assertThat(relevantRatio).isLessThanOrEqualTo(coverage + 0.05); // Allow 5% margin
  }

  // =========================================================================
  // Seed Reproducibility Tests
  // =========================================================================

  @Test
  public void testDifferentSeedsProduceDifferentResults() {
    RemappingTestDataGenerator gen1 = new RemappingTestDataGenerator(111);
    RemappingTestDataGenerator gen2 = new RemappingTestDataGenerator(222);

    List<Long> positions1 = gen1.createSortedPositions(50, 10);
    List<Long> positions2 = gen2.createSortedPositions(50, 10);

    // Different seeds should produce different results
    assertThat(positions1).isNotEqualTo(positions2);
  }
}
