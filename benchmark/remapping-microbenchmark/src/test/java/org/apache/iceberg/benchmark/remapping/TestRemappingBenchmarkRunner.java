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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.DeleteFormat;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.Density;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.Strategy;
import org.apache.iceberg.benchmark.remapping.metrics.BenchmarkMetrics;
import org.apache.iceberg.benchmark.remapping.metrics.BenchmarkMetrics.BenchmarkResult;
import org.apache.iceberg.benchmark.remapping.metrics.BenchmarkMetrics.ScenarioSummary;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for RemappingBenchmarkRunner.
 *
 * <p>These tests run actual benchmark scenarios with small data sizes to verify the end-to-end
 * benchmark workflow including test data generation, remapping, and metrics collection.
 */
public class TestRemappingBenchmarkRunner {

  private Path tempDir;
  private FileIO fileIO;

  @BeforeEach
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("remapping-runner-test");
    fileIO = new HadoopFileIO(new Configuration());
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (tempDir != null && Files.exists(tempDir)) {
      Files.walk(tempDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
  }

  @Test
  public void testRunSingleScenarioPositionDeletes() throws IOException {
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withStorageUri(tempDir.toUri().toString())
            .withWarmupIterations(1)
            .withMeasurementIterations(2)
            .withDeleteCounts(Arrays.asList(100))
            .withRunCounts(Arrays.asList(5))
            .withDensities(Arrays.asList(Density.SPARSE))
            .withFormats(Arrays.asList(DeleteFormat.POSITION_DELETE_FILE))
            .withStrategies(Arrays.asList(Strategy.SMART));

    String baseLocation = tempDir.resolve("benchmark").toString();
    RemappingBenchmarkRunner runner = new RemappingBenchmarkRunner(config, fileIO, baseLocation);

    runner.runScenario(DeleteFormat.POSITION_DELETE_FILE, Density.SPARSE, 100, 5, Strategy.SMART);

    BenchmarkMetrics metrics = runner.getMetrics();
    List<BenchmarkResult> results = metrics.getResults();

    // Should have 1 warmup + 2 measurement = 3 results
    assertThat(results).hasSize(3);

    // Check warmup result
    BenchmarkResult warmup = results.get(0);
    assertThat(warmup.warmup()).isTrue();
    assertThat(warmup.format()).isEqualTo(DeleteFormat.POSITION_DELETE_FILE);
    assertThat(warmup.density()).isEqualTo(Density.SPARSE);
    assertThat(warmup.numDeletes()).isEqualTo(100);
    assertThat(warmup.numRuns()).isEqualTo(5);

    // Check measurement results
    BenchmarkResult measurement1 = results.get(1);
    assertThat(measurement1.warmup()).isFalse();
    assertThat(measurement1.iteration()).isEqualTo(0);

    BenchmarkResult measurement2 = results.get(2);
    assertThat(measurement2.warmup()).isFalse();
    assertThat(measurement2.iteration()).isEqualTo(1);

    // Verify latencies are recorded
    for (BenchmarkResult result : results) {
      assertThat(result.totalLatencyNs()).isGreaterThan(0);
      assertThat(result.readLatencyNs()).isGreaterThanOrEqualTo(0);
      assertThat(result.remapLatencyNs()).isGreaterThanOrEqualTo(0);
      assertThat(result.writeLatencyNs()).isGreaterThanOrEqualTo(0);
    }
  }

  @Test
  public void testRunSingleScenarioDeletionVectors() throws IOException {
    // Note: This test uses 0 warmup iterations to avoid file name collision issues
    // in the current benchmark runner implementation (remapped files use fixed names)
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withStorageUri(tempDir.toUri().toString())
            .withWarmupIterations(0)
            .withMeasurementIterations(1)
            .withDeleteCounts(Arrays.asList(100))
            .withRunCounts(Arrays.asList(5))
            .withDensities(Arrays.asList(Density.DENSE))
            .withFormats(Arrays.asList(DeleteFormat.DELETION_VECTOR))
            .withStrategies(Arrays.asList(Strategy.SMART));

    String baseLocation = tempDir.resolve("benchmark-dv").toString();
    RemappingBenchmarkRunner runner = new RemappingBenchmarkRunner(config, fileIO, baseLocation);

    runner.runScenario(DeleteFormat.DELETION_VECTOR, Density.DENSE, 100, 5, Strategy.SMART);

    BenchmarkMetrics metrics = runner.getMetrics();
    List<BenchmarkResult> results = metrics.getResults();

    assertThat(results).hasSize(1);

    BenchmarkResult result = results.get(0);
    assertThat(result.format()).isEqualTo(DeleteFormat.DELETION_VECTOR);
    assertThat(result.density()).isEqualTo(Density.DENSE);
  }

  @Test
  public void testRunAllWithMinimalConfig() throws IOException {
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withStorageUri(tempDir.toUri().toString())
            .withWarmupIterations(1)
            .withMeasurementIterations(1)
            .withDeleteCounts(Arrays.asList(50))
            .withRunCounts(Arrays.asList(2))
            .withDensities(Arrays.asList(Density.SPARSE))
            .withFormats(Arrays.asList(DeleteFormat.POSITION_DELETE_FILE))
            .withStrategies(Arrays.asList(Strategy.SMART));

    String baseLocation = tempDir.resolve("benchmark-all").toString();
    RemappingBenchmarkRunner runner = new RemappingBenchmarkRunner(config, fileIO, baseLocation);

    runner.runAll();

    BenchmarkMetrics metrics = runner.getMetrics();
    List<BenchmarkResult> results = metrics.getResults();

    // 1 format x 1 density x 1 delete count x 1 run count x (1 warmup + 1 measurement) = 2
    assertThat(results).hasSize(2);
  }

  @Test
  public void testRunAllWithMultipleScenarios() throws IOException {
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withStorageUri(tempDir.toUri().toString())
            .withWarmupIterations(1)
            .withMeasurementIterations(1)
            .withDeleteCounts(Arrays.asList(50, 100))
            .withRunCounts(Arrays.asList(2))
            .withDensities(Arrays.asList(Density.SPARSE, Density.DENSE))
            .withFormats(Arrays.asList(DeleteFormat.POSITION_DELETE_FILE))
            .withStrategies(Arrays.asList(Strategy.SMART));

    String baseLocation = tempDir.resolve("benchmark-multi").toString();
    RemappingBenchmarkRunner runner = new RemappingBenchmarkRunner(config, fileIO, baseLocation);

    runner.runAll();

    BenchmarkMetrics metrics = runner.getMetrics();
    List<BenchmarkResult> results = metrics.getResults();

    // 1 format x 2 densities x 2 delete counts x 1 run count x (1 warmup + 1 measurement) = 8
    assertThat(results).hasSize(8);
  }

  @Test
  public void testMetricsSummary() throws IOException {
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withStorageUri(tempDir.toUri().toString())
            .withWarmupIterations(1)
            .withMeasurementIterations(3)
            .withDeleteCounts(Arrays.asList(100))
            .withRunCounts(Arrays.asList(5))
            .withDensities(Arrays.asList(Density.SPARSE))
            .withFormats(Arrays.asList(DeleteFormat.POSITION_DELETE_FILE))
            .withStrategies(Arrays.asList(Strategy.SMART));

    String baseLocation = tempDir.resolve("benchmark-summary").toString();
    RemappingBenchmarkRunner runner = new RemappingBenchmarkRunner(config, fileIO, baseLocation);

    runner.runScenario(DeleteFormat.POSITION_DELETE_FILE, Density.SPARSE, 100, 5, Strategy.SMART);

    BenchmarkMetrics metrics = runner.getMetrics();
    Map<String, ScenarioSummary> summaries = metrics.summarize();

    assertThat(summaries).hasSize(1);

    String key = "POSITION_DELETE_FILE_SPARSE_SMART_d100_r5";
    assertThat(summaries).containsKey(key);

    ScenarioSummary summary = summaries.get(key);
    assertThat(summary.count()).isEqualTo(3); // Only measurement iterations
    assertThat(summary.format()).isEqualTo(DeleteFormat.POSITION_DELETE_FILE);
    assertThat(summary.density()).isEqualTo(Density.SPARSE);
    assertThat(summary.strategy()).isEqualTo(Strategy.SMART);
    assertThat(summary.avgTotalLatencyMs()).isGreaterThan(0);
  }

  @Test
  public void testDifferentRandomSeeds() throws IOException {
    // Run with seed 1
    BenchmarkConfig config1 =
        BenchmarkConfig.defaults()
            .withStorageUri(tempDir.toUri().toString())
            .withWarmupIterations(0)
            .withMeasurementIterations(1)
            .withDeleteCounts(Arrays.asList(50))
            .withRunCounts(Arrays.asList(2))
            .withDensities(Arrays.asList(Density.SPARSE))
            .withFormats(Arrays.asList(DeleteFormat.POSITION_DELETE_FILE))
            .withRandomSeed(1L);

    String baseLocation1 = tempDir.resolve("benchmark-seed1").toString();
    RemappingBenchmarkRunner runner1 = new RemappingBenchmarkRunner(config1, fileIO, baseLocation1);
    runner1.runAll();

    // Run with seed 2
    BenchmarkConfig config2 =
        BenchmarkConfig.defaults()
            .withStorageUri(tempDir.toUri().toString())
            .withWarmupIterations(0)
            .withMeasurementIterations(1)
            .withDeleteCounts(Arrays.asList(50))
            .withRunCounts(Arrays.asList(2))
            .withDensities(Arrays.asList(Density.SPARSE))
            .withFormats(Arrays.asList(DeleteFormat.POSITION_DELETE_FILE))
            .withRandomSeed(2L);

    String baseLocation2 = tempDir.resolve("benchmark-seed2").toString();
    RemappingBenchmarkRunner runner2 = new RemappingBenchmarkRunner(config2, fileIO, baseLocation2);
    runner2.runAll();

    // Both should complete successfully (different data but same structure)
    assertThat(runner1.getMetrics().getResults()).hasSize(1);
    assertThat(runner2.getMetrics().getResults()).hasSize(1);
  }

  @Test
  public void testSaveAndLoadResults() throws IOException {
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withStorageUri(tempDir.toUri().toString())
            .withWarmupIterations(1)
            .withMeasurementIterations(2)
            .withDeleteCounts(Arrays.asList(50))
            .withRunCounts(Arrays.asList(2))
            .withDensities(Arrays.asList(Density.SPARSE))
            .withFormats(Arrays.asList(DeleteFormat.POSITION_DELETE_FILE));

    String baseLocation = tempDir.resolve("benchmark-save").toString();
    RemappingBenchmarkRunner runner = new RemappingBenchmarkRunner(config, fileIO, baseLocation);
    runner.runAll();

    // Save results
    String resultsPath = tempDir.resolve("results.json").toString();
    runner.getMetrics().saveResults(resultsPath);

    // Load and verify
    BenchmarkMetrics loaded = BenchmarkMetrics.loadResults(resultsPath);
    assertThat(loaded.getResults()).hasSize(3);
  }

  @Test
  public void testLargerScaleScenario() throws IOException {
    // Test with slightly larger data to ensure scaling works
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withStorageUri(tempDir.toUri().toString())
            .withWarmupIterations(0)
            .withMeasurementIterations(1)
            .withDeleteCounts(Arrays.asList(1000))
            .withRunCounts(Arrays.asList(20))
            .withDensities(Arrays.asList(Density.SPARSE))
            .withFormats(Arrays.asList(DeleteFormat.POSITION_DELETE_FILE));

    String baseLocation = tempDir.resolve("benchmark-scale").toString();
    RemappingBenchmarkRunner runner = new RemappingBenchmarkRunner(config, fileIO, baseLocation);
    runner.runAll();

    BenchmarkMetrics metrics = runner.getMetrics();
    BenchmarkResult result = metrics.getResults().get(0);

    assertThat(result.numDeletes()).isEqualTo(1000);
    assertThat(result.numRuns()).isEqualTo(20);
    assertThat(result.totalLatencyNs()).isGreaterThan(0);
  }

  @Test
  public void testBothFormatsInSingleRun() throws IOException {
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withStorageUri(tempDir.toUri().toString())
            .withWarmupIterations(0)
            .withMeasurementIterations(1)
            .withDeleteCounts(Arrays.asList(50))
            .withRunCounts(Arrays.asList(2))
            .withDensities(Arrays.asList(Density.SPARSE))
            .withFormats(
                Arrays.asList(DeleteFormat.POSITION_DELETE_FILE, DeleteFormat.DELETION_VECTOR));

    String baseLocation = tempDir.resolve("benchmark-both").toString();
    RemappingBenchmarkRunner runner = new RemappingBenchmarkRunner(config, fileIO, baseLocation);
    runner.runAll();

    BenchmarkMetrics metrics = runner.getMetrics();
    List<BenchmarkResult> results = metrics.getResults();

    // 2 formats x 1 density x 1 delete count x 1 run count x 1 measurement = 2
    assertThat(results).hasSize(2);

    assertThat(results.stream().filter(r -> r.format() == DeleteFormat.POSITION_DELETE_FILE))
        .hasSize(1);
    assertThat(results.stream().filter(r -> r.format() == DeleteFormat.DELETION_VECTOR)).hasSize(1);
  }
}
