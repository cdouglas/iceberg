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
package org.apache.iceberg.benchmark.cloud.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for workload events in trace-based benchmarks.
 *
 * <p>Supports parsing from YAML trace files with table definitions and event sequences.
 */
public class WorkloadConfig {

  @JsonProperty("name")
  private String name;

  @JsonProperty("description")
  private String description;

  @JsonProperty("parameters")
  private Parameters parameters = new Parameters();

  @JsonProperty("events")
  private List<WorkloadEvent> events = new ArrayList<>();

  public static class Parameters {
    @JsonProperty("base_tables")
    private List<TableConfig> baseTables = new ArrayList<>();

    public List<TableConfig> baseTables() {
      return baseTables;
    }
  }

  public static class TableConfig {
    @JsonProperty("name")
    private String name;

    @JsonProperty("rows")
    private long rows;

    @JsonProperty("files")
    private int files;

    public String name() {
      return name;
    }

    public long rows() {
      return rows;
    }

    public int files() {
      return files;
    }
  }

  public static class WorkloadEvent {
    @JsonProperty("type")
    private EventType type;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("table")
    private String table;

    @JsonProperty("tables")
    private List<String> tables;

    @JsonProperty("selectivity")
    private double selectivity;

    @JsonProperty("pattern")
    private DeletePattern pattern = DeletePattern.RANDOM;

    @JsonProperty("file_count")
    private int fileCount;

    @JsonProperty("target")
    private DeleteTarget target = DeleteTarget.ANY;

    public EventType type() {
      return type;
    }

    public long timestamp() {
      return timestamp;
    }

    public String table() {
      return table;
    }

    public List<String> tables() {
      return tables;
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
  }

  public enum EventType {
    INITIAL_LOAD,
    DELETE_ROWS,
    COMPACTION,
    CONCURRENT_DELETE,
    ADD_FILES
  }

  public enum DeletePattern {
    RANDOM,
    SEQUENTIAL,
    CLUSTERED
  }

  public enum DeleteTarget {
    ANY,
    COMPACTING
  }

  // Getters
  public String name() {
    return name;
  }

  public String description() {
    return description;
  }

  public Parameters parameters() {
    return parameters;
  }

  public List<WorkloadEvent> events() {
    return events;
  }
}
