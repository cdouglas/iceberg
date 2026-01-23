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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.apache.iceberg.benchmark.cloud.config.BenchmarkConfig;
import org.apache.iceberg.benchmark.cloud.config.TraceParser;
import org.apache.iceberg.benchmark.cloud.config.WorkloadConfig;

/**
 * Interface for generating workload events for the compaction benchmark.
 *
 * <p>Supports two modes:
 *
 * <ul>
 *   <li>TRACE_REPLAY: Replay events from a YAML trace file
 *   <li>RANDOM: Generate random events based on configuration parameters
 * </ul>
 */
public interface WorkloadGenerator {

  /**
   * Generate a list of workload events.
   *
   * @return list of events sorted by timestamp
   */
  List<WorkloadEvent> generate();

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

  /** A workload event to be executed. */
  class WorkloadEvent {
    private final EventType type;
    private final long timestamp;
    private final String table;
    private final double selectivity;
    private final DeletePattern pattern;
    private final int fileCount;
    private final DeleteTarget target;

    public WorkloadEvent(
        EventType type,
        long timestamp,
        String table,
        double selectivity,
        DeletePattern pattern,
        int fileCount,
        DeleteTarget target) {
      this.type = type;
      this.timestamp = timestamp;
      this.table = table;
      this.selectivity = selectivity;
      this.pattern = pattern;
      this.fileCount = fileCount;
      this.target = target;
    }

    public static Builder builder(EventType type) {
      return new Builder(type);
    }

    public EventType type() {
      return type;
    }

    public long timestamp() {
      return timestamp;
    }

    public String table() {
      return table;
    }

    public double selectivity() {
      return selectivity;
    }

    public DeletePattern pattern() {
      return pattern;
    }

    public int fileCount() {
      return fileCount;
    }

    public DeleteTarget target() {
      return target;
    }

    public static class Builder {
      private final EventType type;
      private long timestamp = 0;
      private String table = "default";
      private double selectivity = 0.001;
      private DeletePattern pattern = DeletePattern.RANDOM;
      private int fileCount = 10;
      private DeleteTarget target = DeleteTarget.ANY;

      Builder(EventType type) {
        this.type = type;
      }

      public Builder timestamp(long ts) {
        this.timestamp = ts;
        return this;
      }

      public Builder table(String t) {
        this.table = t;
        return this;
      }

      public Builder selectivity(double s) {
        this.selectivity = s;
        return this;
      }

      public Builder pattern(DeletePattern p) {
        this.pattern = p;
        return this;
      }

      public Builder fileCount(int count) {
        this.fileCount = count;
        return this;
      }

      public Builder target(DeleteTarget t) {
        this.target = t;
        return this;
      }

      public WorkloadEvent build() {
        return new WorkloadEvent(type, timestamp, table, selectivity, pattern, fileCount, target);
      }
    }
  }

  /** Generator that replays events from a trace file. */
  class TraceReplayGenerator implements WorkloadGenerator {
    private final WorkloadConfig workloadConfig;

    public TraceReplayGenerator(WorkloadConfig workloadConfig) {
      this.workloadConfig = workloadConfig;
    }

    @Override
    public List<WorkloadEvent> generate() {
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

      events.sort(Comparator.comparingLong(WorkloadEvent::timestamp));
      return events;
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

  /** Generator that creates random workload events. */
  class RandomWorkloadGenerator implements WorkloadGenerator {
    private final BenchmarkConfig config;
    private final Random random;

    public RandomWorkloadGenerator(BenchmarkConfig config) {
      this.config = config;
      this.random = new Random(config.randomSeed());
    }

    @Override
    public List<WorkloadEvent> generate() {
      List<WorkloadEvent> events = new ArrayList<>();
      long timestamp = 0;

      // Initial load
      events.add(
          WorkloadEvent.builder(EventType.INITIAL_LOAD)
              .timestamp(timestamp++)
              .table("benchmark")
              .fileCount(config.numFiles())
              .build());

      // Generate iterations of operations
      for (int i = 0; i < config.numIterations(); i++) {
        // Add delete operations
        events.add(
            WorkloadEvent.builder(EventType.DELETE_ROWS)
                .timestamp(timestamp++)
                .table("benchmark")
                .selectivity(config.deleteSelectivity())
                .pattern(randomPattern())
                .build());

        // Possibly add compaction
        if (random.nextDouble() < config.compactionFrequency()) {
          long compactionTs = timestamp++;

          // Possibly add concurrent delete (for conflict testing)
          if (random.nextDouble() < config.conflictProbability()) {
            events.add(
                WorkloadEvent.builder(EventType.CONCURRENT_DELETE)
                    .timestamp(compactionTs) // Same timestamp as compaction
                    .table("benchmark")
                    .selectivity(config.deleteSelectivity() / 2)
                    .target(DeleteTarget.COMPACTING)
                    .build());
          }

          events.add(
              WorkloadEvent.builder(EventType.COMPACTION)
                  .timestamp(compactionTs)
                  .table("benchmark")
                  .fileCount(Math.max(2, config.numFiles() / 10))
                  .build());
        }
      }

      events.sort(Comparator.comparingLong(WorkloadEvent::timestamp));
      return events;
    }

    private DeletePattern randomPattern() {
      DeletePattern[] patterns = DeletePattern.values();
      return patterns[random.nextInt(patterns.length)];
    }
  }
}
