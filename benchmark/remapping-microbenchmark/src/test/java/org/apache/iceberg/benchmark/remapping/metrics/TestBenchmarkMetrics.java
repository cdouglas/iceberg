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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.DeleteFormat;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.Density;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.Strategy;
import org.apache.iceberg.benchmark.remapping.metrics.BenchmarkMetrics.BenchmarkResult;
import org.apache.iceberg.benchmark.remapping.metrics.BenchmarkMetrics.ScenarioSummary;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestBenchmarkMetrics {

  private Path tempDir;
  private BenchmarkMetrics metrics;

  @BeforeEach
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("benchmark-metrics-test");
    metrics = new BenchmarkMetrics();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (tempDir != null && Files.exists(tempDir)) {
      Files.walk(tempDir)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    }
  }

  @Test
  public void testRecordAndRetrieveResults() {
    BenchmarkResult result =
        BenchmarkResult.builder()
            .format(DeleteFormat.POSITION_DELETE_FILE)
            .density(Density.SPARSE)
            .strategy(Strategy.LINEAR)
            .numDeletes(1000)
            .numRuns(10)
            .numSourceFiles(5)
            .numTargetFiles(1)
            .readLatencyNs(1_000_000L)
            .remapLatencyNs(2_000_000L)
            .writeLatencyNs(1_500_000L)
            .totalLatencyNs(4_500_000L)
            .inputSizeBytes(10000)
            .outputSizeBytes(8000)
            .mapSizeBytes(5000)
            .iteration(1)
            .warmup(false)
            .build();

    metrics.record(result);

    assertThat(metrics.getResults()).hasSize(1);
    assertThat(metrics.getResults().get(0)).isEqualTo(result);
  }

  @Test
  public void testBenchmarkResultBuilder() {
    BenchmarkResult result =
        BenchmarkResult.builder()
            .format(DeleteFormat.DELETION_VECTOR)
            .density(Density.DENSE)
            .strategy(Strategy.INTERVAL_TREE)
            .numDeletes(5000)
            .numRuns(50)
            .numSourceFiles(10)
            .numTargetFiles(2)
            .readLatencyNs(2_000_000L)
            .remapLatencyNs(3_000_000L)
            .writeLatencyNs(1_000_000L)
            .totalLatencyNs(6_000_000L)
            .inputSizeBytes(20000)
            .outputSizeBytes(15000)
            .mapSizeBytes(8000)
            .iteration(3)
            .warmup(true)
            .build();

    assertThat(result.format()).isEqualTo(DeleteFormat.DELETION_VECTOR);
    assertThat(result.density()).isEqualTo(Density.DENSE);
    assertThat(result.strategy()).isEqualTo(Strategy.INTERVAL_TREE);
    assertThat(result.numDeletes()).isEqualTo(5000);
    assertThat(result.numRuns()).isEqualTo(50);
    assertThat(result.numSourceFiles()).isEqualTo(10);
    assertThat(result.numTargetFiles()).isEqualTo(2);
    assertThat(result.readLatencyNs()).isEqualTo(2_000_000L);
    assertThat(result.remapLatencyNs()).isEqualTo(3_000_000L);
    assertThat(result.writeLatencyNs()).isEqualTo(1_000_000L);
    assertThat(result.totalLatencyNs()).isEqualTo(6_000_000L);
    assertThat(result.inputSizeBytes()).isEqualTo(20000);
    assertThat(result.outputSizeBytes()).isEqualTo(15000);
    assertThat(result.mapSizeBytes()).isEqualTo(8000);
    assertThat(result.iteration()).isEqualTo(3);
    assertThat(result.warmup()).isTrue();
  }

  @Test
  public void testScenarioKey() {
    BenchmarkResult result =
        BenchmarkResult.builder()
            .format(DeleteFormat.POSITION_DELETE_FILE)
            .density(Density.SPARSE)
            .strategy(Strategy.BINARY_SEARCH)
            .numDeletes(1000)
            .numRuns(10)
            .build();

    assertThat(result.scenarioKey())
        .isEqualTo("POSITION_DELETE_FILE_SPARSE_BINARY_SEARCH_d1000_r10");
  }

  @Test
  public void testThroughputCalculation() {
    BenchmarkResult result =
        BenchmarkResult.builder()
            .numDeletes(1000)
            .totalLatencyNs(1_000_000_000L) // 1 second
            .build();

    assertThat(result.deletesPerSecond()).isCloseTo(1000.0, within(0.1));
  }

  @Test
  public void testLatencyPercentages() {
    BenchmarkResult result =
        BenchmarkResult.builder()
            .readLatencyNs(25_000_000L)
            .remapLatencyNs(50_000_000L)
            .writeLatencyNs(25_000_000L)
            .totalLatencyNs(100_000_000L)
            .build();

    assertThat(result.readPct()).isCloseTo(25.0, within(0.1));
    assertThat(result.remapPct()).isCloseTo(50.0, within(0.1));
    assertThat(result.writePct()).isCloseTo(25.0, within(0.1));
  }

  @Test
  public void testSummarizeByScenario() {
    // Add multiple results for the same scenario
    for (int i = 0; i < 5; i++) {
      metrics.record(
          BenchmarkResult.builder()
              .format(DeleteFormat.POSITION_DELETE_FILE)
              .density(Density.SPARSE)
              .strategy(Strategy.LINEAR)
              .numDeletes(1000)
              .numRuns(10)
              .totalLatencyNs((i + 1) * 1_000_000L)
              .warmup(false)
              .build());
    }

    Map<String, ScenarioSummary> summaries = metrics.summarize();

    assertThat(summaries).hasSize(1);
    ScenarioSummary summary = summaries.get("POSITION_DELETE_FILE_SPARSE_LINEAR_d1000_r10");
    assertThat(summary).isNotNull();
    assertThat(summary.count()).isEqualTo(5);
    assertThat(summary.format()).isEqualTo(DeleteFormat.POSITION_DELETE_FILE);
    assertThat(summary.density()).isEqualTo(Density.SPARSE);
    assertThat(summary.strategy()).isEqualTo(Strategy.LINEAR);
    assertThat(summary.numDeletes()).isEqualTo(1000);
    assertThat(summary.numRuns()).isEqualTo(10);
  }

  @Test
  public void testSummaryLatencyStatistics() {
    // Add results with known latencies: 1, 2, 3, 4, 5 ms
    for (int i = 1; i <= 5; i++) {
      metrics.record(
          BenchmarkResult.builder()
              .format(DeleteFormat.POSITION_DELETE_FILE)
              .density(Density.SPARSE)
              .strategy(Strategy.LINEAR)
              .numDeletes(1000)
              .numRuns(10)
              .totalLatencyNs(i * 1_000_000L)
              .warmup(false)
              .build());
    }

    Map<String, ScenarioSummary> summaries = metrics.summarize();
    ScenarioSummary summary = summaries.get("POSITION_DELETE_FILE_SPARSE_LINEAR_d1000_r10");

    // Average should be 3 ms
    assertThat(summary.avgTotalLatencyMs()).isCloseTo(3.0, within(0.01));
    // P50 should be around 3 ms
    assertThat(summary.p50TotalLatencyMs()).isCloseTo(3.0, within(0.01));
  }

  @Test
  public void testWarmupIterationsExcludedFromSummary() {
    // Add warmup iterations
    for (int i = 0; i < 3; i++) {
      metrics.record(
          BenchmarkResult.builder()
              .format(DeleteFormat.POSITION_DELETE_FILE)
              .density(Density.SPARSE)
              .strategy(Strategy.LINEAR)
              .numDeletes(1000)
              .numRuns(10)
              .totalLatencyNs(100_000_000L) // Very high warmup times
              .warmup(true)
              .build());
    }

    // Add measurement iterations
    for (int i = 0; i < 5; i++) {
      metrics.record(
          BenchmarkResult.builder()
              .format(DeleteFormat.POSITION_DELETE_FILE)
              .density(Density.SPARSE)
              .strategy(Strategy.LINEAR)
              .numDeletes(1000)
              .numRuns(10)
              .totalLatencyNs(1_000_000L)
              .warmup(false)
              .build());
    }

    Map<String, ScenarioSummary> summaries = metrics.summarize();
    ScenarioSummary summary = summaries.get("POSITION_DELETE_FILE_SPARSE_LINEAR_d1000_r10");

    // Only 5 measurement iterations should be counted
    assertThat(summary.count()).isEqualTo(5);
    // Average should be 1 ms (not affected by warmup)
    assertThat(summary.avgTotalLatencyMs()).isCloseTo(1.0, within(0.01));
  }

  @Test
  public void testSaveAndLoadResults() throws IOException {
    // Record some results
    metrics.record(
        BenchmarkResult.builder()
            .format(DeleteFormat.POSITION_DELETE_FILE)
            .density(Density.SPARSE)
            .strategy(Strategy.LINEAR)
            .numDeletes(1000)
            .numRuns(10)
            .totalLatencyNs(5_000_000L)
            .warmup(false)
            .build());

    metrics.record(
        BenchmarkResult.builder()
            .format(DeleteFormat.DELETION_VECTOR)
            .density(Density.DENSE)
            .strategy(Strategy.INTERVAL_TREE)
            .numDeletes(2000)
            .numRuns(20)
            .totalLatencyNs(10_000_000L)
            .warmup(false)
            .build());

    // Save results
    String resultsPath = tempDir.resolve("results.json").toString();
    metrics.saveResults(resultsPath);

    assertThat(new File(resultsPath)).exists();

    // Load results
    BenchmarkMetrics loadedMetrics = BenchmarkMetrics.loadResults(resultsPath);

    assertThat(loadedMetrics.getResults()).hasSize(2);
    assertThat(loadedMetrics.getResults().get(0).format())
        .isEqualTo(DeleteFormat.POSITION_DELETE_FILE);
    assertThat(loadedMetrics.getResults().get(1).format()).isEqualTo(DeleteFormat.DELETION_VECTOR);
  }

  @Test
  public void testSaveSummary() throws IOException {
    // Record results
    metrics.record(
        BenchmarkResult.builder()
            .format(DeleteFormat.POSITION_DELETE_FILE)
            .density(Density.SPARSE)
            .strategy(Strategy.LINEAR)
            .numDeletes(1000)
            .numRuns(10)
            .totalLatencyNs(5_000_000L)
            .warmup(false)
            .build());

    // Save summary
    String summaryPath = tempDir.resolve("summary.json").toString();
    metrics.saveSummary(summaryPath);

    assertThat(new File(summaryPath)).exists();
    String content = Files.readString(Path.of(summaryPath));
    assertThat(content).contains("POSITION_DELETE_FILE");
    assertThat(content).contains("avg-total-latency-ms");
  }

  @Test
  public void testMultipleScenariosInSummary() {
    // Add results for different scenarios
    metrics.record(
        BenchmarkResult.builder()
            .format(DeleteFormat.POSITION_DELETE_FILE)
            .density(Density.SPARSE)
            .strategy(Strategy.LINEAR)
            .numDeletes(1000)
            .numRuns(10)
            .totalLatencyNs(5_000_000L)
            .warmup(false)
            .build());

    metrics.record(
        BenchmarkResult.builder()
            .format(DeleteFormat.POSITION_DELETE_FILE)
            .density(Density.SPARSE)
            .strategy(Strategy.BINARY_SEARCH)
            .numDeletes(1000)
            .numRuns(10)
            .totalLatencyNs(3_000_000L)
            .warmup(false)
            .build());

    metrics.record(
        BenchmarkResult.builder()
            .format(DeleteFormat.DELETION_VECTOR)
            .density(Density.DENSE)
            .strategy(Strategy.INTERVAL_TREE)
            .numDeletes(2000)
            .numRuns(20)
            .totalLatencyNs(10_000_000L)
            .warmup(false)
            .build());

    Map<String, ScenarioSummary> summaries = metrics.summarize();

    assertThat(summaries).hasSize(3);
    assertThat(summaries).containsKey("POSITION_DELETE_FILE_SPARSE_LINEAR_d1000_r10");
    assertThat(summaries).containsKey("POSITION_DELETE_FILE_SPARSE_BINARY_SEARCH_d1000_r10");
    assertThat(summaries).containsKey("DELETION_VECTOR_DENSE_INTERVAL_TREE_d2000_r20");
  }

  @Test
  public void testScenarioSummaryToString() {
    metrics.record(
        BenchmarkResult.builder()
            .format(DeleteFormat.POSITION_DELETE_FILE)
            .density(Density.SPARSE)
            .strategy(Strategy.LINEAR)
            .numDeletes(1000)
            .numRuns(10)
            .readLatencyNs(1_000_000L)
            .remapLatencyNs(2_000_000L)
            .writeLatencyNs(1_000_000L)
            .totalLatencyNs(4_000_000L)
            .warmup(false)
            .build());

    Map<String, ScenarioSummary> summaries = metrics.summarize();
    ScenarioSummary summary = summaries.get("POSITION_DELETE_FILE_SPARSE_LINEAR_d1000_r10");

    String toString = summary.toString();
    assertThat(toString).contains("format=POSITION_DELETE_FILE");
    assertThat(toString).contains("density=SPARSE");
    assertThat(toString).contains("strategy=LINEAR");
    assertThat(toString).contains("deletes=1000");
    assertThat(toString).contains("runs=10");
    assertThat(toString).contains("avgLatency=");
    assertThat(toString).contains("throughput=");
  }

  @Test
  public void testEmptyMetrics() {
    Map<String, ScenarioSummary> summaries = metrics.summarize();
    assertThat(summaries).isEmpty();
    assertThat(metrics.getResults()).isEmpty();
  }

  @Test
  public void testOnlyWarmupIterations() {
    metrics.record(
        BenchmarkResult.builder()
            .format(DeleteFormat.POSITION_DELETE_FILE)
            .density(Density.SPARSE)
            .strategy(Strategy.LINEAR)
            .numDeletes(1000)
            .numRuns(10)
            .totalLatencyNs(5_000_000L)
            .warmup(true)
            .build());

    Map<String, ScenarioSummary> summaries = metrics.summarize();
    ScenarioSummary summary = summaries.get("POSITION_DELETE_FILE_SPARSE_LINEAR_d1000_r10");

    // Summary should exist but have count 0
    assertThat(summary.count()).isEqualTo(0);
  }
}
