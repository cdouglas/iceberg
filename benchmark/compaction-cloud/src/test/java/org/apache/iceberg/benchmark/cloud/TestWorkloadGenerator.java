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
package org.apache.iceberg.benchmark.cloud;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.iceberg.benchmark.cloud.config.BenchmarkConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Thorough tests for WorkloadGenerator, particularly deterministic replay from seeds. */
public class TestWorkloadGenerator {

  // ========== Deterministic Replay Tests ==========

  @Test
  void testDeterministicReplayFromSameSeed() {
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withRandomSeed(12345L)
            .withNumIterations(20)
            .withNumFiles(50)
            .withCompactionFrequency(0.5)
            .withConflictProbability(0.3);

    // Generate events twice with same seed
    List<WorkloadEvent> firstRun = collectAllEvents(config, 12345L);
    List<WorkloadEvent> secondRun = collectAllEvents(config, 12345L);

    // Must be identical
    assertThat(firstRun).hasSize(secondRun.size());
    for (int i = 0; i < firstRun.size(); i++) {
      assertThat(firstRun.get(i))
          .as("Event at index %d should be identical", i)
          .isEqualTo(secondRun.get(i));
    }
  }

  @Test
  void testDifferentSeedsProduceDifferentWorkloads() {
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withNumIterations(20)
            .withNumFiles(50)
            .withCompactionFrequency(0.5)
            .withConflictProbability(0.5);

    List<WorkloadEvent> seed42 = collectAllEvents(config, 42L);
    List<WorkloadEvent> seed123 = collectAllEvents(config, 123L);

    // Different seeds should produce different results (with high probability)
    // Check at least event count or patterns differ
    boolean anyDifference = seed42.size() != seed123.size();
    if (!anyDifference) {
      for (int i = 0; i < seed42.size(); i++) {
        if (!seed42.get(i).equals(seed123.get(i))) {
          anyDifference = true;
          break;
        }
      }
    }

    assertThat(anyDifference).as("Different seeds should produce different workloads").isTrue();
  }

  @ParameterizedTest
  @ValueSource(longs = {0L, 1L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 42L, 12345678901234L})
  void testDeterministicWithVariousSeeds(long seed) {
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withNumIterations(10)
            .withCompactionFrequency(0.5)
            .withConflictProbability(0.5);

    List<WorkloadEvent> run1 = collectAllEvents(config, seed);
    List<WorkloadEvent> run2 = collectAllEvents(config, seed);

    assertThat(run1).as("Same seed %d should produce identical workloads", seed).isEqualTo(run2);
  }

  @Test
  void testDeterministicPatternSelection() {
    // Verify that pattern selection is deterministic
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withRandomSeed(999L)
            .withNumIterations(100) // Many iterations to get variety
            .withCompactionFrequency(0.0); // No compaction to simplify

    List<WorkloadEvent> run1 = collectAllEvents(config, 999L);
    List<WorkloadEvent> run2 = collectAllEvents(config, 999L);

    // Check that patterns match exactly
    List<WorkloadGenerator.DeletePattern> patterns1 = new ArrayList<>();
    List<WorkloadGenerator.DeletePattern> patterns2 = new ArrayList<>();

    for (WorkloadEvent e : run1) {
      if (e.type() == WorkloadGenerator.EventType.DELETE_ROWS) {
        patterns1.add(e.pattern());
      }
    }
    for (WorkloadEvent e : run2) {
      if (e.type() == WorkloadGenerator.EventType.DELETE_ROWS) {
        patterns2.add(e.pattern());
      }
    }

    assertThat(patterns1).as("Delete patterns should be deterministic").isEqualTo(patterns2);
  }

  // ========== Event Order Tests ==========

  @Test
  void testEventsEmittedInTimestampOrder() {
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withRandomSeed(42L)
            .withNumIterations(50)
            .withCompactionFrequency(0.8)
            .withConflictProbability(0.5);

    WorkloadGenerator generator = WorkloadGenerator.createRandom(config, 42L);

    long lastTimestamp = Long.MIN_VALUE;
    int eventCount = 0;

    while (generator.hasNext()) {
      WorkloadEvent event = generator.next();
      assertThat(event.timestamp())
          .as("Event %d timestamp should be >= previous", eventCount)
          .isGreaterThanOrEqualTo(lastTimestamp);
      lastTimestamp = event.timestamp();
      eventCount++;
    }

    assertThat(eventCount).isGreaterThan(0);
  }

  @Test
  void testConcurrentEventsHaveSameTimestamp() {
    // When compaction and concurrent delete happen together, they should have same timestamp
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withRandomSeed(42L)
            .withNumIterations(100)
            .withCompactionFrequency(1.0) // Always compact
            .withConflictProbability(1.0); // Always add concurrent delete

    List<WorkloadEvent> events = collectAllEvents(config, 42L);

    // Find compaction events and verify concurrent deletes have same timestamp
    for (int i = 0; i < events.size(); i++) {
      WorkloadEvent event = events.get(i);
      if (event.type() == WorkloadGenerator.EventType.COMPACTION) {
        // Check if there's a concurrent delete at the same timestamp
        boolean hasConcurrentDelete = false;
        for (WorkloadEvent other : events) {
          if (other.type() == WorkloadGenerator.EventType.CONCURRENT_DELETE
              && other.timestamp() == event.timestamp()) {
            hasConcurrentDelete = true;
            break;
          }
        }
        // With conflictProbability=1.0, we should always have concurrent deletes
        assertThat(hasConcurrentDelete)
            .as("Compaction at timestamp %d should have concurrent delete", event.timestamp())
            .isTrue();
      }
    }
  }

  // ========== Lazy Generation Tests ==========

  @Test
  void testLazyGenerationDoesNotPrecomputeAll() {
    // Create a generator and verify it doesn't consume all events on construction
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withRandomSeed(42L)
            .withNumIterations(1000000) // Very large to detect eager evaluation
            .withCompactionFrequency(0.5);

    long startTime = System.nanoTime();
    WorkloadGenerator generator = WorkloadGenerator.createRandom(config, 42L);
    long constructionTime = System.nanoTime() - startTime;

    // Construction should be fast (< 10ms) since we're not precomputing
    assertThat(constructionTime)
        .as("Generator construction should be fast (lazy)")
        .isLessThan(10_000_000L); // 10ms in nanoseconds

    // Verify we can get the first event quickly
    assertThat(generator.hasNext()).isTrue();
    WorkloadEvent first = generator.next();
    assertThat(first.type()).isEqualTo(WorkloadGenerator.EventType.INITIAL_LOAD);
  }

  @Test
  void testPartialConsumptionDeterminstic() {
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withRandomSeed(42L)
            .withNumIterations(100)
            .withCompactionFrequency(0.5);

    // Consume first 10 events from two generators with same seed
    WorkloadGenerator gen1 = WorkloadGenerator.createRandom(config, 42L);
    WorkloadGenerator gen2 = WorkloadGenerator.createRandom(config, 42L);

    for (int i = 0; i < 10; i++) {
      assertThat(gen1.hasNext()).isTrue();
      assertThat(gen2.hasNext()).isTrue();

      WorkloadEvent e1 = gen1.next();
      WorkloadEvent e2 = gen2.next();

      assertThat(e1).as("Event %d should match", i).isEqualTo(e2);
    }
  }

  // ========== Iterator Contract Tests ==========

  @Test
  void testIteratorContractHasNextIdempotent() {
    BenchmarkConfig config = BenchmarkConfig.defaults().withRandomSeed(42L).withNumIterations(5);

    WorkloadGenerator generator = WorkloadGenerator.createRandom(config, 42L);

    // Multiple hasNext() calls should not change state
    assertThat(generator.hasNext()).isTrue();
    assertThat(generator.hasNext()).isTrue();
    assertThat(generator.hasNext()).isTrue();

    WorkloadEvent event = generator.next();
    assertThat(event.type()).isEqualTo(WorkloadGenerator.EventType.INITIAL_LOAD);
  }

  @Test
  void testIteratorContractNoSuchElement() {
    BenchmarkConfig config = BenchmarkConfig.defaults().withRandomSeed(42L).withNumIterations(1);

    WorkloadGenerator generator = WorkloadGenerator.createRandom(config, 42L);

    // Consume all events
    while (generator.hasNext()) {
      generator.next();
    }

    // Further calls should throw NoSuchElementException
    assertThat(generator.hasNext()).isFalse();
    assertThatThrownBy(generator::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void testIteratorContractEmptyAfterExhaustion() {
    BenchmarkConfig config = BenchmarkConfig.defaults().withRandomSeed(42L).withNumIterations(2);

    WorkloadGenerator generator = WorkloadGenerator.createRandom(config, 42L);

    List<WorkloadEvent> events = collectAll(generator);

    // After exhaustion, hasNext should consistently return false
    for (int i = 0; i < 5; i++) {
      assertThat(generator.hasNext()).isFalse();
    }

    assertThat(events).isNotEmpty();
  }

  // ========== Event Content Tests ==========

  @Test
  void testInitialLoadEventIsFirst() {
    BenchmarkConfig config =
        BenchmarkConfig.defaults().withRandomSeed(42L).withNumIterations(10).withNumFiles(75);

    WorkloadGenerator generator = WorkloadGenerator.createRandom(config, 42L);

    assertThat(generator.hasNext()).isTrue();
    WorkloadEvent first = generator.next();

    assertThat(first.type()).isEqualTo(WorkloadGenerator.EventType.INITIAL_LOAD);
    assertThat(first.timestamp()).isEqualTo(0);
    assertThat(first.fileCount()).isEqualTo(75);
  }

  @Test
  void testDeleteEventsHaveCorrectSelectivity() {
    double targetSelectivity = 0.005;
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withRandomSeed(42L)
            .withNumIterations(10)
            .withDeleteSelectivity(targetSelectivity)
            .withCompactionFrequency(0.0); // No compaction

    List<WorkloadEvent> events = collectAllEvents(config, 42L);

    for (WorkloadEvent event : events) {
      if (event.type() == WorkloadGenerator.EventType.DELETE_ROWS) {
        assertThat(event.selectivity()).isEqualTo(targetSelectivity);
      }
    }
  }

  @Test
  void testCompactionEventsHaveCorrectFileCount() {
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withRandomSeed(42L)
            .withNumIterations(50)
            .withNumFiles(100)
            .withCompactionFrequency(1.0) // Always compact
            .withConflictProbability(0.0);

    List<WorkloadEvent> events = collectAllEvents(config, 42L);

    int expectedFileCount = Math.max(2, 100 / 10); // numFiles / 10

    for (WorkloadEvent event : events) {
      if (event.type() == WorkloadGenerator.EventType.COMPACTION) {
        assertThat(event.fileCount()).isEqualTo(expectedFileCount);
      }
    }
  }

  @Test
  void testConcurrentDeleteHasHalfSelectivity() {
    double baseSelectivity = 0.01;
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withRandomSeed(42L)
            .withNumIterations(50)
            .withDeleteSelectivity(baseSelectivity)
            .withCompactionFrequency(1.0)
            .withConflictProbability(1.0);

    List<WorkloadEvent> events = collectAllEvents(config, 42L);

    for (WorkloadEvent event : events) {
      if (event.type() == WorkloadGenerator.EventType.CONCURRENT_DELETE) {
        assertThat(event.selectivity()).isEqualTo(baseSelectivity / 2);
        assertThat(event.target()).isEqualTo(WorkloadGenerator.DeleteTarget.COMPACTING);
      }
    }
  }

  // ========== Configuration Variation Tests ==========

  @Test
  void testZeroIterationsProducesOnlyInitialLoad() {
    BenchmarkConfig config = BenchmarkConfig.defaults().withRandomSeed(42L).withNumIterations(0);

    List<WorkloadEvent> events = collectAllEvents(config, 42L);

    assertThat(events).hasSize(1);
    assertThat(events.get(0).type()).isEqualTo(WorkloadGenerator.EventType.INITIAL_LOAD);
  }

  @Test
  void testZeroCompactionFrequency() {
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withRandomSeed(42L)
            .withNumIterations(100)
            .withCompactionFrequency(0.0);

    List<WorkloadEvent> events = collectAllEvents(config, 42L);

    // Should have no compaction events
    long compactionCount =
        events.stream().filter(e -> e.type() == WorkloadGenerator.EventType.COMPACTION).count();

    assertThat(compactionCount).isZero();
  }

  @Test
  void testZeroConflictProbability() {
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withRandomSeed(42L)
            .withNumIterations(100)
            .withCompactionFrequency(1.0) // Always compact
            .withConflictProbability(0.0); // Never conflict

    List<WorkloadEvent> events = collectAllEvents(config, 42L);

    // Should have no concurrent delete events
    long concurrentDeleteCount =
        events.stream()
            .filter(e -> e.type() == WorkloadGenerator.EventType.CONCURRENT_DELETE)
            .count();

    assertThat(concurrentDeleteCount).isZero();
  }

  @Test
  void testHighIterationCount() {
    int iterations = 10000;
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withRandomSeed(42L)
            .withNumIterations(iterations)
            .withCompactionFrequency(0.5)
            .withConflictProbability(0.3);

    List<WorkloadEvent> events = collectAllEvents(config, 42L);

    // Should have at least iterations + 1 events (initial load + iterations)
    assertThat(events.size()).isGreaterThanOrEqualTo(iterations + 1);

    // Count event types for sanity check
    long deleteCount =
        events.stream().filter(e -> e.type() == WorkloadGenerator.EventType.DELETE_ROWS).count();

    // Should have exactly 'iterations' delete events
    assertThat(deleteCount).isEqualTo(iterations);
  }

  // ========== Pattern Distribution Tests ==========

  @Test
  void testAllDeletePatternsAppear() {
    // With enough iterations and the right seed, all patterns should appear
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withRandomSeed(42L)
            .withNumIterations(1000)
            .withCompactionFrequency(0.0);

    List<WorkloadEvent> events = collectAllEvents(config, 42L);

    Set<WorkloadGenerator.DeletePattern> patterns = new HashSet<>();
    for (WorkloadEvent event : events) {
      if (event.type() == WorkloadGenerator.EventType.DELETE_ROWS) {
        patterns.add(event.pattern());
      }
    }

    // All three patterns should appear with high probability
    assertThat(patterns)
        .as("All delete patterns should appear with sufficient iterations")
        .containsExactlyInAnyOrder(
            WorkloadGenerator.DeletePattern.RANDOM,
            WorkloadGenerator.DeletePattern.SEQUENTIAL,
            WorkloadGenerator.DeletePattern.CLUSTERED);
  }

  // ========== Timestamp Sequence Tests ==========

  @Test
  void testTimestampsAreMonotonicallyIncreasing() {
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withRandomSeed(42L)
            .withNumIterations(100)
            .withCompactionFrequency(0.5)
            .withConflictProbability(0.5);

    List<WorkloadEvent> events = collectAllEvents(config, 42L);

    for (int i = 1; i < events.size(); i++) {
      assertThat(events.get(i).timestamp())
          .as("Timestamp at index %d should be >= previous", i)
          .isGreaterThanOrEqualTo(events.get(i - 1).timestamp());
    }
  }

  @Test
  void testTimestampGapsForSequentialEvents() {
    // When there's no compaction, timestamps should increment by 1 for each iteration
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withRandomSeed(42L)
            .withNumIterations(10)
            .withCompactionFrequency(0.0);

    List<WorkloadEvent> events = collectAllEvents(config, 42L);

    // Initial load at 0, then deletes at 1, 2, 3, ...
    assertThat(events.get(0).timestamp()).isEqualTo(0); // INITIAL_LOAD
    for (int i = 1; i < events.size(); i++) {
      assertThat(events.get(i).timestamp()).isEqualTo(i);
    }
  }

  // ========== Helper Methods ==========

  private List<WorkloadEvent> collectAllEvents(BenchmarkConfig config, long seed) {
    WorkloadGenerator generator = WorkloadGenerator.createRandom(config, seed);
    return collectAll(generator);
  }

  private List<WorkloadEvent> collectAll(WorkloadGenerator generator) {
    List<WorkloadEvent> events = new ArrayList<>();
    while (generator.hasNext()) {
      events.add(generator.next());
    }
    return events;
  }
}
