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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Configuration for the compaction cloud benchmark.
 *
 * <p>Supports loading from YAML files and programmatic configuration.
 */
public class BenchmarkConfig {

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  // Table configuration
  @JsonProperty("table-location")
  private String tableLocation = "benchmark-tables";

  @JsonProperty("format-version")
  private int formatVersion = 2;

  @JsonProperty("compaction-maps-enabled")
  private boolean compactionMapsEnabled = true;

  // Workload configuration
  @JsonProperty("workload-mode")
  private WorkloadMode workloadMode = WorkloadMode.RANDOM;

  @JsonProperty("trace-file")
  private String traceFile;

  @JsonProperty("random-seed")
  private long randomSeed = 42L;

  // Simulation parameters
  @JsonProperty("base-row-count")
  private long baseRowCount = 1_000_000L;

  @JsonProperty("merge-on-read-penalty")
  private double mergeOnReadPenalty = 0.1;

  @JsonProperty("compaction-speedup")
  private double compactionSpeedup = 10.0;

  @JsonProperty("rows-per-ms")
  private long rowsPerMs = 10_000L;

  // Execution parameters
  @JsonProperty("num-iterations")
  private int numIterations = 10;

  @JsonProperty("concurrent-writers")
  private int concurrentWriters = 4;

  @JsonProperty("concurrent-compactors")
  private int concurrentCompactors = 1;

  // Random workload parameters
  @JsonProperty("num-files")
  private int numFiles = 100;

  @JsonProperty("avg-rows-per-file")
  private long avgRowsPerFile = 100_000L;

  @JsonProperty("row-count-variance")
  private double rowCountVariance = 0.2;

  @JsonProperty("delete-selectivity")
  private double deleteSelectivity = 0.001;

  @JsonProperty("compaction-frequency")
  private double compactionFrequency = 0.1;

  @JsonProperty("conflict-probability")
  private double conflictProbability = 0.3;

  // Statistics
  @JsonProperty("collect-detailed-stats")
  private boolean collectDetailedStats = true;

  @JsonProperty("output-dir")
  private String outputDir = "benchmark-results";

  @JsonProperty("comparison-run")
  private boolean comparisonRun = false;

  public enum WorkloadMode {
    TRACE_REPLAY,
    RANDOM
  }

  public static BenchmarkConfig load(String path) throws IOException {
    return YAML_MAPPER.readValue(new File(path), BenchmarkConfig.class);
  }

  public static BenchmarkConfig load(InputStream stream) throws IOException {
    return YAML_MAPPER.readValue(stream, BenchmarkConfig.class);
  }

  public static BenchmarkConfig defaults() {
    return new BenchmarkConfig();
  }

  // Getters
  public String tableLocation() {
    return tableLocation;
  }

  public int formatVersion() {
    return formatVersion;
  }

  public boolean compactionMapsEnabled() {
    return compactionMapsEnabled;
  }

  public WorkloadMode workloadMode() {
    return workloadMode;
  }

  public String traceFile() {
    return traceFile;
  }

  public long randomSeed() {
    return randomSeed;
  }

  public long baseRowCount() {
    return baseRowCount;
  }

  public double mergeOnReadPenalty() {
    return mergeOnReadPenalty;
  }

  public double compactionSpeedup() {
    return compactionSpeedup;
  }

  public long rowsPerMs() {
    return rowsPerMs;
  }

  public int numIterations() {
    return numIterations;
  }

  public int concurrentWriters() {
    return concurrentWriters;
  }

  public int concurrentCompactors() {
    return concurrentCompactors;
  }

  public int numFiles() {
    return numFiles;
  }

  public long avgRowsPerFile() {
    return avgRowsPerFile;
  }

  public double rowCountVariance() {
    return rowCountVariance;
  }

  public double deleteSelectivity() {
    return deleteSelectivity;
  }

  public double compactionFrequency() {
    return compactionFrequency;
  }

  public double conflictProbability() {
    return conflictProbability;
  }

  public boolean collectDetailedStats() {
    return collectDetailedStats;
  }

  public String outputDir() {
    return outputDir;
  }

  public boolean comparisonRun() {
    return comparisonRun;
  }

  // Builder-style setters for programmatic configuration
  public BenchmarkConfig withTableLocation(String location) {
    this.tableLocation = location;
    return this;
  }

  public BenchmarkConfig withFormatVersion(int version) {
    this.formatVersion = version;
    return this;
  }

  public BenchmarkConfig withCompactionMapsEnabled(boolean enabled) {
    this.compactionMapsEnabled = enabled;
    return this;
  }

  public BenchmarkConfig withWorkloadMode(WorkloadMode mode) {
    this.workloadMode = mode;
    return this;
  }

  public BenchmarkConfig withTraceFile(String file) {
    this.traceFile = file;
    return this;
  }

  public BenchmarkConfig withRandomSeed(long seed) {
    this.randomSeed = seed;
    return this;
  }

  public BenchmarkConfig withBaseRowCount(long count) {
    this.baseRowCount = count;
    return this;
  }

  public BenchmarkConfig withMergeOnReadPenalty(double penalty) {
    this.mergeOnReadPenalty = penalty;
    return this;
  }

  public BenchmarkConfig withCompactionSpeedup(double speedup) {
    this.compactionSpeedup = speedup;
    return this;
  }

  public BenchmarkConfig withRowsPerMs(long rows) {
    this.rowsPerMs = rows;
    return this;
  }

  public BenchmarkConfig withNumIterations(int iterations) {
    this.numIterations = iterations;
    return this;
  }

  public BenchmarkConfig withConcurrentWriters(int writers) {
    this.concurrentWriters = writers;
    return this;
  }

  public BenchmarkConfig withConcurrentCompactors(int compactors) {
    this.concurrentCompactors = compactors;
    return this;
  }

  public BenchmarkConfig withNumFiles(int files) {
    this.numFiles = files;
    return this;
  }

  public BenchmarkConfig withAvgRowsPerFile(long rows) {
    this.avgRowsPerFile = rows;
    return this;
  }

  public BenchmarkConfig withRowCountVariance(double variance) {
    this.rowCountVariance = variance;
    return this;
  }

  public BenchmarkConfig withDeleteSelectivity(double selectivity) {
    this.deleteSelectivity = selectivity;
    return this;
  }

  public BenchmarkConfig withCompactionFrequency(double frequency) {
    this.compactionFrequency = frequency;
    return this;
  }

  public BenchmarkConfig withConflictProbability(double probability) {
    this.conflictProbability = probability;
    return this;
  }

  public BenchmarkConfig withCollectDetailedStats(boolean collect) {
    this.collectDetailedStats = collect;
    return this;
  }

  public BenchmarkConfig withOutputDir(String dir) {
    this.outputDir = dir;
    return this;
  }

  public BenchmarkConfig withComparisonRun(boolean comparison) {
    this.comparisonRun = comparison;
    return this;
  }

  public void save(String path) throws IOException {
    YAML_MAPPER.writeValue(new File(path), this);
  }
}
