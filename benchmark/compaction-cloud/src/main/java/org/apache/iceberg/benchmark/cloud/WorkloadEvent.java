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

import java.util.Objects;

/**
 * A workload event to be executed in the benchmark.
 *
 * <p>Each event has a timestamp that determines its execution order. Events with the same timestamp
 * may be executed concurrently.
 */
public class WorkloadEvent {
  private final WorkloadGenerator.EventType type;
  private final long timestamp;
  private final String table;
  private final double selectivity;
  private final WorkloadGenerator.DeletePattern pattern;
  private final int fileCount;
  private final WorkloadGenerator.DeleteTarget target;

  WorkloadEvent(
      WorkloadGenerator.EventType type,
      long timestamp,
      String table,
      double selectivity,
      WorkloadGenerator.DeletePattern pattern,
      int fileCount,
      WorkloadGenerator.DeleteTarget target) {
    this.type = type;
    this.timestamp = timestamp;
    this.table = table;
    this.selectivity = selectivity;
    this.pattern = pattern;
    this.fileCount = fileCount;
    this.target = target;
  }

  public static Builder builder(WorkloadGenerator.EventType type) {
    return new Builder(type);
  }

  public WorkloadGenerator.EventType type() {
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

  public WorkloadGenerator.DeletePattern pattern() {
    return pattern;
  }

  public int fileCount() {
    return fileCount;
  }

  public WorkloadGenerator.DeleteTarget target() {
    return target;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkloadEvent that = (WorkloadEvent) o;
    return timestamp == that.timestamp
        && Double.compare(that.selectivity, selectivity) == 0
        && fileCount == that.fileCount
        && type == that.type
        && Objects.equals(table, that.table)
        && pattern == that.pattern
        && target == that.target;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, timestamp, table, selectivity, pattern, fileCount, target);
  }

  @Override
  public String toString() {
    return String.format(
        "WorkloadEvent{type=%s, timestamp=%d, table='%s', selectivity=%.4f, pattern=%s, fileCount=%d, target=%s}",
        type, timestamp, table, selectivity, pattern, fileCount, target);
  }

  /** Builder for WorkloadEvent instances. */
  public static class Builder {
    private final WorkloadGenerator.EventType type;
    private long timestamp = 0;
    private String table = "default";
    private double selectivity = 0.001;
    private WorkloadGenerator.DeletePattern pattern = WorkloadGenerator.DeletePattern.RANDOM;
    private int fileCount = 10;
    private WorkloadGenerator.DeleteTarget target = WorkloadGenerator.DeleteTarget.ANY;

    Builder(WorkloadGenerator.EventType type) {
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

    public Builder pattern(WorkloadGenerator.DeletePattern p) {
      this.pattern = p;
      return this;
    }

    public Builder fileCount(int count) {
      this.fileCount = count;
      return this;
    }

    public Builder target(WorkloadGenerator.DeleteTarget t) {
      this.target = t;
      return this;
    }

    public WorkloadEvent build() {
      return new WorkloadEvent(type, timestamp, table, selectivity, pattern, fileCount, target);
    }
  }
}
