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
package org.apache.iceberg.benchmark.remapping.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.DeleteFormat;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.Density;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.Strategy;

/**
 * Collects and reports benchmark metrics.
 *
 * <p>Metrics are organized by scenario parameters to enable analysis across dimensions:
 *
 * <ul>
 *   <li>Delete format (position delete file vs deletion vector)
 *   <li>Density (sparse vs dense)
 *   <li>Strategy (linear, binary search, interval tree, etc.)
 *   <li>Scale (number of deletes, number of runs)
 * </ul>
 */
public class BenchmarkMetrics {

  private static final ObjectMapper JSON_MAPPER =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  private final List<BenchmarkResult> results = new ArrayList<>();

  /** Record a single benchmark iteration. */
  public void record(BenchmarkResult result) {
    results.add(result);
  }

  public List<BenchmarkResult> getResults() {
    return Collections.unmodifiableList(results);
  }

  /** Generate summary statistics grouped by scenario. */
  public Map<String, ScenarioSummary> summarize() {
    Map<String, List<BenchmarkResult>> byScenario = new TreeMap<>();

    for (BenchmarkResult result : results) {
      String key = result.scenarioKey();
      byScenario.computeIfAbsent(key, k -> new ArrayList<>()).add(result);
    }

    Map<String, ScenarioSummary> summaries = new TreeMap<>();
    for (Map.Entry<String, List<BenchmarkResult>> entry : byScenario.entrySet()) {
      summaries.put(entry.getKey(), ScenarioSummary.compute(entry.getValue()));
    }

    return summaries;
  }

  /** Save results to JSON file. */
  public void saveResults(String path) throws IOException {
    JSON_MAPPER.writeValue(new File(path), results);
  }

  /** Save summary to JSON file. */
  public void saveSummary(String path) throws IOException {
    JSON_MAPPER.writeValue(new File(path), summarize());
  }

  /** Load results from JSON file. */
  public static BenchmarkMetrics loadResults(String path) throws IOException {
    BenchmarkMetrics metrics = new BenchmarkMetrics();
    BenchmarkResult[] loaded = JSON_MAPPER.readValue(new File(path), BenchmarkResult[].class);
    Collections.addAll(metrics.results, loaded);
    return metrics;
  }

  /** A single benchmark measurement. */
  public static class BenchmarkResult {
    @JsonProperty("format")
    private DeleteFormat format;

    @JsonProperty("density")
    private Density density;

    @JsonProperty("strategy")
    private Strategy strategy;

    @JsonProperty("num-deletes")
    private int numDeletes;

    @JsonProperty("num-runs")
    private int numRuns;

    @JsonProperty("num-source-files")
    private int numSourceFiles;

    @JsonProperty("num-target-files")
    private int numTargetFiles;

    @JsonProperty("read-latency-ns")
    private long readLatencyNs;

    @JsonProperty("remap-latency-ns")
    private long remapLatencyNs;

    @JsonProperty("write-latency-ns")
    private long writeLatencyNs;

    @JsonProperty("total-latency-ns")
    private long totalLatencyNs;

    @JsonProperty("input-size-bytes")
    private long inputSizeBytes;

    @JsonProperty("output-size-bytes")
    private long outputSizeBytes;

    @JsonProperty("map-size-bytes")
    private long mapSizeBytes;

    @JsonProperty("iteration")
    private int iteration;

    @JsonProperty("warmup")
    private boolean warmup;

    // Default constructor for Jackson
    public BenchmarkResult() {}

    public BenchmarkResult(Builder builder) {
      this.format = builder.format;
      this.density = builder.density;
      this.strategy = builder.strategy;
      this.numDeletes = builder.numDeletes;
      this.numRuns = builder.numRuns;
      this.numSourceFiles = builder.numSourceFiles;
      this.numTargetFiles = builder.numTargetFiles;
      this.readLatencyNs = builder.readLatencyNs;
      this.remapLatencyNs = builder.remapLatencyNs;
      this.writeLatencyNs = builder.writeLatencyNs;
      this.totalLatencyNs = builder.totalLatencyNs;
      this.inputSizeBytes = builder.inputSizeBytes;
      this.outputSizeBytes = builder.outputSizeBytes;
      this.mapSizeBytes = builder.mapSizeBytes;
      this.iteration = builder.iteration;
      this.warmup = builder.warmup;
    }

    public static Builder builder() {
      return new Builder();
    }

    /** Scenario key for grouping results. */
    public String scenarioKey() {
      return String.format(
          Locale.ROOT, "%s_%s_%s_d%d_r%d", format, density, strategy, numDeletes, numRuns);
    }

    // Getters
    public DeleteFormat format() {
      return format;
    }

    public Density density() {
      return density;
    }

    public Strategy strategy() {
      return strategy;
    }

    public int numDeletes() {
      return numDeletes;
    }

    public int numRuns() {
      return numRuns;
    }

    public int numSourceFiles() {
      return numSourceFiles;
    }

    public int numTargetFiles() {
      return numTargetFiles;
    }

    public long readLatencyNs() {
      return readLatencyNs;
    }

    public long remapLatencyNs() {
      return remapLatencyNs;
    }

    public long writeLatencyNs() {
      return writeLatencyNs;
    }

    public long totalLatencyNs() {
      return totalLatencyNs;
    }

    public long inputSizeBytes() {
      return inputSizeBytes;
    }

    public long outputSizeBytes() {
      return outputSizeBytes;
    }

    public long mapSizeBytes() {
      return mapSizeBytes;
    }

    public int iteration() {
      return iteration;
    }

    public boolean warmup() {
      return warmup;
    }

    /** Throughput in deletes per second. */
    public double deletesPerSecond() {
      return totalLatencyNs > 0 ? (double) numDeletes / totalLatencyNs * 1_000_000_000 : 0;
    }

    /** Read latency as percentage of total. */
    public double readPct() {
      return totalLatencyNs > 0 ? 100.0 * readLatencyNs / totalLatencyNs : 0;
    }

    /** Remap latency as percentage of total. */
    public double remapPct() {
      return totalLatencyNs > 0 ? 100.0 * remapLatencyNs / totalLatencyNs : 0;
    }

    /** Write latency as percentage of total. */
    public double writePct() {
      return totalLatencyNs > 0 ? 100.0 * writeLatencyNs / totalLatencyNs : 0;
    }

    public static class Builder {
      private Builder() {}

      private DeleteFormat format;
      private Density density;
      private Strategy strategy;
      private int numDeletes;
      private int numRuns;
      private int numSourceFiles;
      private int numTargetFiles;
      private long readLatencyNs;
      private long remapLatencyNs;
      private long writeLatencyNs;
      private long totalLatencyNs;
      private long inputSizeBytes;
      private long outputSizeBytes;
      private long mapSizeBytes;
      private int iteration;
      private boolean warmup;

      public Builder format(DeleteFormat f) {
        this.format = f;
        return this;
      }

      public Builder density(Density d) {
        this.density = d;
        return this;
      }

      public Builder strategy(Strategy s) {
        this.strategy = s;
        return this;
      }

      public Builder numDeletes(int n) {
        this.numDeletes = n;
        return this;
      }

      public Builder numRuns(int n) {
        this.numRuns = n;
        return this;
      }

      public Builder numSourceFiles(int n) {
        this.numSourceFiles = n;
        return this;
      }

      public Builder numTargetFiles(int n) {
        this.numTargetFiles = n;
        return this;
      }

      public Builder readLatencyNs(long ns) {
        this.readLatencyNs = ns;
        return this;
      }

      public Builder remapLatencyNs(long ns) {
        this.remapLatencyNs = ns;
        return this;
      }

      public Builder writeLatencyNs(long ns) {
        this.writeLatencyNs = ns;
        return this;
      }

      public Builder totalLatencyNs(long ns) {
        this.totalLatencyNs = ns;
        return this;
      }

      public Builder inputSizeBytes(long bytes) {
        this.inputSizeBytes = bytes;
        return this;
      }

      public Builder outputSizeBytes(long bytes) {
        this.outputSizeBytes = bytes;
        return this;
      }

      public Builder mapSizeBytes(long bytes) {
        this.mapSizeBytes = bytes;
        return this;
      }

      public Builder iteration(int i) {
        this.iteration = i;
        return this;
      }

      public Builder warmup(boolean w) {
        this.warmup = w;
        return this;
      }

      public BenchmarkResult build() {
        return new BenchmarkResult(this);
      }
    }
  }

  /** Summary statistics for a scenario. */
  public static class ScenarioSummary {
    @JsonProperty("count")
    private int count;

    @JsonProperty("format")
    private DeleteFormat format;

    @JsonProperty("density")
    private Density density;

    @JsonProperty("strategy")
    private Strategy strategy;

    @JsonProperty("num-deletes")
    private int numDeletes;

    @JsonProperty("num-runs")
    private int numRuns;

    @JsonProperty("avg-total-latency-ms")
    private double avgTotalLatencyMs;

    @JsonProperty("p50-total-latency-ms")
    private double p50TotalLatencyMs;

    @JsonProperty("p95-total-latency-ms")
    private double p95TotalLatencyMs;

    @JsonProperty("p99-total-latency-ms")
    private double p99TotalLatencyMs;

    @JsonProperty("avg-read-pct")
    private double avgReadPct;

    @JsonProperty("avg-remap-pct")
    private double avgRemapPct;

    @JsonProperty("avg-write-pct")
    private double avgWritePct;

    @JsonProperty("avg-throughput-deletes-per-sec")
    private double avgThroughput;

    // Default constructor for Jackson
    public ScenarioSummary() {}

    public static ScenarioSummary compute(List<BenchmarkResult> results) {
      ScenarioSummary summary = new ScenarioSummary();

      // Filter out warmup iterations
      List<BenchmarkResult> measured =
          results.stream().filter(r -> !r.warmup).collect(java.util.stream.Collectors.toList());

      if (measured.isEmpty()) {
        return summary;
      }

      BenchmarkResult first = measured.get(0);
      summary.format = first.format;
      summary.density = first.density;
      summary.strategy = first.strategy;
      summary.numDeletes = first.numDeletes;
      summary.numRuns = first.numRuns;
      summary.count = measured.size();

      // Compute latency statistics
      List<Long> latencies =
          measured.stream()
              .map(BenchmarkResult::totalLatencyNs)
              .sorted()
              .collect(java.util.stream.Collectors.toList());

      summary.avgTotalLatencyMs =
          latencies.stream().mapToLong(Long::longValue).average().orElse(0) / 1_000_000.0;
      summary.p50TotalLatencyMs = percentile(latencies, 50) / 1_000_000.0;
      summary.p95TotalLatencyMs = percentile(latencies, 95) / 1_000_000.0;
      summary.p99TotalLatencyMs = percentile(latencies, 99) / 1_000_000.0;

      // Compute phase breakdowns
      summary.avgReadPct = measured.stream().mapToDouble(BenchmarkResult::readPct).average().orElse(0);
      summary.avgRemapPct = measured.stream().mapToDouble(BenchmarkResult::remapPct).average().orElse(0);
      summary.avgWritePct = measured.stream().mapToDouble(BenchmarkResult::writePct).average().orElse(0);

      // Compute throughput
      summary.avgThroughput =
          measured.stream().mapToDouble(BenchmarkResult::deletesPerSecond).average().orElse(0);

      return summary;
    }

    private static long percentile(List<Long> sorted, int pct) {
      if (sorted.isEmpty()) {
        return 0;
      }
      int idx = (int) Math.ceil(pct / 100.0 * sorted.size()) - 1;
      return sorted.get(Math.max(0, Math.min(idx, sorted.size() - 1)));
    }

    // Getters
    public int count() {
      return count;
    }

    public DeleteFormat format() {
      return format;
    }

    public Density density() {
      return density;
    }

    public Strategy strategy() {
      return strategy;
    }

    public int numDeletes() {
      return numDeletes;
    }

    public int numRuns() {
      return numRuns;
    }

    public double avgTotalLatencyMs() {
      return avgTotalLatencyMs;
    }

    public double p50TotalLatencyMs() {
      return p50TotalLatencyMs;
    }

    public double p95TotalLatencyMs() {
      return p95TotalLatencyMs;
    }

    public double p99TotalLatencyMs() {
      return p99TotalLatencyMs;
    }

    public double avgReadPct() {
      return avgReadPct;
    }

    public double avgRemapPct() {
      return avgRemapPct;
    }

    public double avgWritePct() {
      return avgWritePct;
    }

    public double avgThroughput() {
      return avgThroughput;
    }

    @Override
    public String toString() {
      return String.format(
          Locale.ROOT,
          "ScenarioSummary{format=%s, density=%s, strategy=%s, deletes=%d, runs=%d, "
              + "avgLatency=%.2fms, p99=%.2fms, read=%.1f%%, remap=%.1f%%, write=%.1f%%, throughput=%.0f/s}",
          format,
          density,
          strategy,
          numDeletes,
          numRuns,
          avgTotalLatencyMs,
          p99TotalLatencyMs,
          avgReadPct,
          avgRemapPct,
          avgWritePct,
          avgThroughput);
    }
  }
}
