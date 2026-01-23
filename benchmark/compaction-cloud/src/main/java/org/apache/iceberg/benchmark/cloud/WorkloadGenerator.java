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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import org.apache.iceberg.benchmark.cloud.config.BenchmarkConfig;
import org.apache.iceberg.benchmark.cloud.config.TraceParser;
import org.apache.iceberg.benchmark.cloud.config.WorkloadConfig;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Iterator-based interface for generating workload events for the compaction benchmark.
 *
 * <p>Events are generated on demand (lazily) to minimize memory usage. Implementations MUST emit
 * events in non-decreasing timestamp order - this is validated by consumers.
 *
 * <p>Supports two modes:
 *
 * <ul>
 *   <li>TRACE_REPLAY: Replay events from a YAML trace file (sorted at construction time)
 *   <li>RANDOM: Generate random events based on configuration parameters (generated on demand)
 * </ul>
 */
public interface WorkloadGenerator extends Iterator<WorkloadEvent> {

  /**
   * Create a workload generator based on configuration.
   *
   * @param config benchmark configuration
   * @return appropriate generator instance
   * @throws IOException if trace file cannot be read
   */
  static WorkloadGenerator create(BenchmarkConfig config) throws IOException {
    switch (config.workloadMode()) {
      case TRACE_REPLAY:
        WorkloadConfig workloadConfig = TraceParser.parse(config.traceFile());
        return new TraceReplayGenerator(workloadConfig);
      case RANDOM:
        return new RandomWorkloadGenerator(config);
      default:
        throw new IllegalArgumentException("Unknown workload mode: " + config.workloadMode());
    }
  }

  /**
   * Create a random workload generator with explicit seed.
   *
   * @param config benchmark configuration
   * @param seed random seed for reproducibility
   * @return generator instance
   */
  static WorkloadGenerator createRandom(BenchmarkConfig config, long seed) {
    return new RandomWorkloadGenerator(config, seed);
  }

  /** Event types for the workload. */
  enum EventType {
    /** Initial load of data files */
    INITIAL_LOAD,
    /** Delete rows from existing files */
    DELETE_ROWS,
    /** Compact files */
    COMPACTION,
    /** Delete concurrent with compaction (to create conflicts) */
    CONCURRENT_DELETE,
    /** Add new data files */
    ADD_FILES
  }

  /** Delete pattern for row deletions. */
  enum DeletePattern {
    /** Random row selection */
    RANDOM,
    /** Sequential row selection */
    SEQUENTIAL,
    /** Clustered rows (locality) */
    CLUSTERED
  }

  /** Target for concurrent deletes. */
  enum DeleteTarget {
    /** Any files */
    ANY,
    /** Files currently being compacted */
    COMPACTING
  }

  /**
   * Generator that replays events from a trace file.
   *
   * <p>Events are sorted by timestamp at construction time and iterated in order.
   */
  class TraceReplayGenerator implements WorkloadGenerator {
    private final List<WorkloadEvent> sortedEvents;
    private int currentIndex = 0;

    public TraceReplayGenerator(WorkloadConfig workloadConfig) {
      List<WorkloadEvent> events = new ArrayList<>();

      for (WorkloadConfig.WorkloadEvent traceEvent : workloadConfig.events()) {
        EventType type = convertEventType(traceEvent.type());
        DeletePattern pattern = convertPattern(traceEvent.pattern());
        DeleteTarget target = convertTarget(traceEvent.target());

        WorkloadEvent event =
            WorkloadEvent.builder(type)
                .timestamp(traceEvent.timestamp())
                .table(traceEvent.table())
                .selectivity(traceEvent.selectivity())
                .pattern(pattern)
                .fileCount(traceEvent.fileCount())
                .target(target)
                .build();

        events.add(event);
      }

      // Sort once at construction time
      events.sort(Comparator.comparingLong(WorkloadEvent::timestamp));
      this.sortedEvents = events;

      // Validate order
      validateOrder(sortedEvents);
    }

    private void validateOrder(List<WorkloadEvent> events) {
      for (int i = 1; i < events.size(); i++) {
        Preconditions.checkArgument(
            events.get(i).timestamp() >= events.get(i - 1).timestamp(),
            "Events must be in non-decreasing timestamp order at index %s: %s > %s",
            i,
            events.get(i - 1).timestamp(),
            events.get(i).timestamp());
      }
    }

    @Override
    public boolean hasNext() {
      return currentIndex < sortedEvents.size();
    }

    @Override
    public WorkloadEvent next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more events");
      }
      return sortedEvents.get(currentIndex++);
    }

    private EventType convertEventType(WorkloadConfig.EventType type) {
      switch (type) {
        case INITIAL_LOAD:
          return EventType.INITIAL_LOAD;
        case DELETE_ROWS:
          return EventType.DELETE_ROWS;
        case COMPACTION:
          return EventType.COMPACTION;
        case CONCURRENT_DELETE:
          return EventType.CONCURRENT_DELETE;
        case ADD_FILES:
          return EventType.ADD_FILES;
        default:
          throw new IllegalArgumentException("Unknown event type: " + type);
      }
    }

    private DeletePattern convertPattern(WorkloadConfig.DeletePattern pattern) {
      if (pattern == null) {
        return DeletePattern.RANDOM;
      }
      switch (pattern) {
        case RANDOM:
          return DeletePattern.RANDOM;
        case SEQUENTIAL:
          return DeletePattern.SEQUENTIAL;
        case CLUSTERED:
          return DeletePattern.CLUSTERED;
        default:
          return DeletePattern.RANDOM;
      }
    }

    private DeleteTarget convertTarget(WorkloadConfig.DeleteTarget target) {
      if (target == null) {
        return DeleteTarget.ANY;
      }
      switch (target) {
        case ANY:
          return DeleteTarget.ANY;
        case COMPACTING:
          return DeleteTarget.COMPACTING;
        default:
          return DeleteTarget.ANY;
      }
    }
  }

  /**
   * Generator that creates random workload events on demand.
   *
   * <p>Events are generated lazily using a state machine to ensure deterministic replay from the
   * same seed. Events are always emitted in non-decreasing timestamp order.
   *
   * <p>State machine phases:
   *
   * <ol>
   *   <li>INITIAL_LOAD - emit initial load event
   *   <li>ITERATION - for each iteration, emit DELETE_ROWS, possibly COMPACTION with optional
   *       CONCURRENT_DELETE
   *   <li>DONE - no more events
   * </ol>
   */
  class RandomWorkloadGenerator implements WorkloadGenerator {
    private final BenchmarkConfig config;
    private final Random random;

    // State machine
    private enum Phase {
      INITIAL_LOAD,
      ITERATION,
      DONE
    }

    private Phase phase = Phase.INITIAL_LOAD;
    private int currentIteration = 0;
    private long currentTimestamp = 0;

    // Buffer for events at the same timestamp (e.g., COMPACTION + CONCURRENT_DELETE)
    private final Deque<WorkloadEvent> pendingEvents = new ArrayDeque<>();

    public RandomWorkloadGenerator(BenchmarkConfig config) {
      this(config, config.randomSeed());
    }

    public RandomWorkloadGenerator(BenchmarkConfig config, long seed) {
      this.config = config;
      this.random = new Random(seed);
    }

    @Override
    public boolean hasNext() {
      if (!pendingEvents.isEmpty()) {
        return true;
      }
      generateNextBatch();
      return !pendingEvents.isEmpty();
    }

    @Override
    public WorkloadEvent next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more events");
      }
      return pendingEvents.pollFirst();
    }

    /**
     * Generate the next batch of events based on current state machine phase.
     *
     * <p>This method advances the state machine and populates pendingEvents with the next
     * event(s). Events at the same timestamp are batched together.
     */
    private void generateNextBatch() {
      switch (phase) {
        case INITIAL_LOAD:
          pendingEvents.add(
              WorkloadEvent.builder(EventType.INITIAL_LOAD)
                  .timestamp(currentTimestamp++)
                  .table("benchmark")
                  .fileCount(config.numFiles())
                  .build());
          phase = Phase.ITERATION;
          break;

        case ITERATION:
          if (currentIteration >= config.numIterations()) {
            phase = Phase.DONE;
            return;
          }

          // Always add a DELETE_ROWS event
          pendingEvents.add(
              WorkloadEvent.builder(EventType.DELETE_ROWS)
                  .timestamp(currentTimestamp++)
                  .table("benchmark")
                  .selectivity(config.deleteSelectivity())
                  .pattern(randomPattern())
                  .build());

          // Possibly add compaction (and concurrent delete)
          if (random.nextDouble() < config.compactionFrequency()) {
            long compactionTs = currentTimestamp++;

            // Concurrent delete has same timestamp as compaction
            if (random.nextDouble() < config.conflictProbability()) {
              pendingEvents.add(
                  WorkloadEvent.builder(EventType.CONCURRENT_DELETE)
                      .timestamp(compactionTs)
                      .table("benchmark")
                      .selectivity(config.deleteSelectivity() / 2)
                      .target(DeleteTarget.COMPACTING)
                      .build());
            }

            pendingEvents.add(
                WorkloadEvent.builder(EventType.COMPACTION)
                    .timestamp(compactionTs)
                    .table("benchmark")
                    .fileCount(Math.max(2, config.numFiles() / 10))
                    .build());
          }

          currentIteration++;
          break;

        case DONE:
          // No more events
          break;
      }
    }

    private DeletePattern randomPattern() {
      DeletePattern[] patterns = DeletePattern.values();
      return patterns[random.nextInt(patterns.length)];
    }
  }
}
