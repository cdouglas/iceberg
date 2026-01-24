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
package org.apache.iceberg.benchmark.remapping;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Configuration for the remapping microbenchmark.
 *
 * <p>This benchmark measures the cost of rebasing position deletes on top of a compaction commit,
 * which occurs in conflict scenarios where either:
 *
 * <ul>
 *   <li>A transaction with position deletes conflicts with a concurrent compaction
 *   <li>A compaction needs to rebase concurrent transaction deletes onto its compacted state
 * </ul>
 *
 * <p>These scenarios are symmetric - the remapping algorithm is the same.
 */
public class BenchmarkConfig {

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  // Storage configuration
  @JsonProperty("storage-uri")
  private String storageUri = "file:///tmp/remapping-benchmark";

  @JsonProperty("cloud-provider")
  private CloudProvider cloudProvider = CloudProvider.LOCAL;

  // Test data configuration
  @JsonProperty("delete-counts")
  private List<Integer> deleteCounts = Arrays.asList(1_000, 10_000, 100_000, 1_000_000);

  @JsonProperty("run-counts")
  private List<Integer> runCounts = Arrays.asList(10, 100, 1_000, 10_000);

  @JsonProperty("densities")
  private List<Density> densities = Arrays.asList(Density.SPARSE, Density.DENSE);

  @JsonProperty("formats")
  private List<DeleteFormat> formats =
      Arrays.asList(DeleteFormat.POSITION_DELETE_FILE, DeleteFormat.DELETION_VECTOR);

  @JsonProperty("strategies")
  private List<Strategy> strategies =
      Arrays.asList(
          Strategy.LINEAR,
          Strategy.BINARY_SEARCH,
          Strategy.INTERVAL_TREE,
          Strategy.STREAM_JOIN,
          Strategy.RANGE_QUERY,
          Strategy.SMART);

  // Execution configuration
  @JsonProperty("warmup-iterations")
  private int warmupIterations = 3;

  @JsonProperty("measurement-iterations")
  private int measurementIterations = 10;

  @JsonProperty("output-dir")
  private String outputDir = "benchmark-results";

  @JsonProperty("random-seed")
  private long randomSeed = 42L;

  // Scenario configuration
  @JsonProperty("fanout-factors")
  private List<Integer> fanoutFactors = Arrays.asList(2, 10, 100);

  @JsonProperty("split-factors")
  private List<Integer> splitFactors = Arrays.asList(1, 5, 10);

  public enum CloudProvider {
    LOCAL,
    AWS_S3,
    GCP_GCS,
    AZURE_BLOB
  }

  public enum Density {
    /** ~1% of rows deleted, scattered positions */
    SPARSE,
    /** ~50% of rows deleted, clustered positions */
    DENSE
  }

  public enum DeleteFormat {
    /** V2/V3 Parquet files with (file_path, position) tuples */
    POSITION_DELETE_FILE,
    /** V3 Puffin files with Roaring bitmaps */
    DELETION_VECTOR
  }

  public enum Strategy {
    LINEAR,
    BINARY_SEARCH,
    INTERVAL_TREE,
    STREAM_JOIN,
    RANGE_QUERY,
    SMART
  }

  public static BenchmarkConfig load(String path) throws IOException {
    return YAML_MAPPER.readValue(new File(path), BenchmarkConfig.class);
  }

  public static BenchmarkConfig defaults() {
    return new BenchmarkConfig();
  }

  public void save(String path) throws IOException {
    YAML_MAPPER.writeValue(new File(path), this);
  }

  // Getters
  public String storageUri() {
    return storageUri;
  }

  public CloudProvider cloudProvider() {
    return cloudProvider;
  }

  public List<Integer> deleteCounts() {
    return deleteCounts;
  }

  public List<Integer> runCounts() {
    return runCounts;
  }

  public List<Density> densities() {
    return densities;
  }

  public List<DeleteFormat> formats() {
    return formats;
  }

  public List<Strategy> strategies() {
    return strategies;
  }

  public int warmupIterations() {
    return warmupIterations;
  }

  public int measurementIterations() {
    return measurementIterations;
  }

  public String outputDir() {
    return outputDir;
  }

  public long randomSeed() {
    return randomSeed;
  }

  public List<Integer> fanoutFactors() {
    return fanoutFactors;
  }

  public List<Integer> splitFactors() {
    return splitFactors;
  }

  // Builder-style setters
  public BenchmarkConfig withStorageUri(String uri) {
    this.storageUri = uri;
    return this;
  }

  public BenchmarkConfig withCloudProvider(CloudProvider provider) {
    this.cloudProvider = provider;
    return this;
  }

  public BenchmarkConfig withDeleteCounts(List<Integer> counts) {
    this.deleteCounts = counts;
    return this;
  }

  public BenchmarkConfig withRunCounts(List<Integer> counts) {
    this.runCounts = counts;
    return this;
  }

  public BenchmarkConfig withDensities(List<Density> d) {
    this.densities = d;
    return this;
  }

  public BenchmarkConfig withFormats(List<DeleteFormat> f) {
    this.formats = f;
    return this;
  }

  public BenchmarkConfig withStrategies(List<Strategy> s) {
    this.strategies = s;
    return this;
  }

  public BenchmarkConfig withWarmupIterations(int n) {
    this.warmupIterations = n;
    return this;
  }

  public BenchmarkConfig withMeasurementIterations(int n) {
    this.measurementIterations = n;
    return this;
  }

  public BenchmarkConfig withOutputDir(String dir) {
    this.outputDir = dir;
    return this;
  }

  public BenchmarkConfig withRandomSeed(long seed) {
    this.randomSeed = seed;
    return this;
  }

  public BenchmarkConfig withFanoutFactors(List<Integer> factors) {
    this.fanoutFactors = factors;
    return this;
  }

  public BenchmarkConfig withSplitFactors(List<Integer> factors) {
    this.splitFactors = factors;
    return this;
  }
}
