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
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.CloudProvider;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.DeleteFormat;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.Density;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.Strategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestBenchmarkConfig {

  private Path tempDir;

  @BeforeEach
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("benchmark-config-test");
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
  public void testDefaults() {
    BenchmarkConfig config = BenchmarkConfig.defaults();

    assertThat(config.storageUri()).isEqualTo("file:///tmp/remapping-benchmark");
    assertThat(config.cloudProvider()).isEqualTo(CloudProvider.LOCAL);
    assertThat(config.warmupIterations()).isEqualTo(3);
    assertThat(config.measurementIterations()).isEqualTo(10);
    assertThat(config.outputDir()).isEqualTo("benchmark-results");
    assertThat(config.randomSeed()).isEqualTo(42L);
    assertThat(config.deleteCounts()).containsExactly(1_000, 10_000, 100_000, 1_000_000);
    assertThat(config.runCounts()).containsExactly(10, 100, 1_000, 10_000);
    assertThat(config.densities()).containsExactly(Density.SPARSE, Density.DENSE);
    assertThat(config.formats())
        .containsExactly(DeleteFormat.POSITION_DELETE_FILE, DeleteFormat.DELETION_VECTOR);
    assertThat(config.strategies())
        .containsExactly(
            Strategy.LINEAR,
            Strategy.BINARY_SEARCH,
            Strategy.INTERVAL_TREE,
            Strategy.STREAM_JOIN,
            Strategy.RANGE_QUERY,
            Strategy.SMART);
    assertThat(config.fanoutFactors()).containsExactly(2, 10, 100);
    assertThat(config.splitFactors()).containsExactly(1, 5, 10);
  }

  @Test
  public void testBuilderStyleSetters() {
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withStorageUri("s3://my-bucket/benchmark")
            .withCloudProvider(CloudProvider.AWS_S3)
            .withWarmupIterations(5)
            .withMeasurementIterations(20)
            .withOutputDir("/custom/output")
            .withRandomSeed(12345L)
            .withDeleteCounts(Arrays.asList(100, 200))
            .withRunCounts(Arrays.asList(5, 10))
            .withDensities(Arrays.asList(Density.SPARSE))
            .withFormats(Arrays.asList(DeleteFormat.DELETION_VECTOR))
            .withStrategies(Arrays.asList(Strategy.SMART))
            .withFanoutFactors(Arrays.asList(5))
            .withSplitFactors(Arrays.asList(2, 4));

    assertThat(config.storageUri()).isEqualTo("s3://my-bucket/benchmark");
    assertThat(config.cloudProvider()).isEqualTo(CloudProvider.AWS_S3);
    assertThat(config.warmupIterations()).isEqualTo(5);
    assertThat(config.measurementIterations()).isEqualTo(20);
    assertThat(config.outputDir()).isEqualTo("/custom/output");
    assertThat(config.randomSeed()).isEqualTo(12345L);
    assertThat(config.deleteCounts()).containsExactly(100, 200);
    assertThat(config.runCounts()).containsExactly(5, 10);
    assertThat(config.densities()).containsExactly(Density.SPARSE);
    assertThat(config.formats()).containsExactly(DeleteFormat.DELETION_VECTOR);
    assertThat(config.strategies()).containsExactly(Strategy.SMART);
    assertThat(config.fanoutFactors()).containsExactly(5);
    assertThat(config.splitFactors()).containsExactly(2, 4);
  }

  @Test
  public void testSaveAndLoad() throws IOException {
    BenchmarkConfig original =
        BenchmarkConfig.defaults()
            .withStorageUri("gs://test-bucket/data")
            .withCloudProvider(CloudProvider.GCP_GCS)
            .withWarmupIterations(2)
            .withMeasurementIterations(5)
            .withDeleteCounts(Arrays.asList(500, 1000))
            .withStrategies(Arrays.asList(Strategy.LINEAR, Strategy.INTERVAL_TREE));

    String configPath = tempDir.resolve("test-config.yaml").toString();
    original.save(configPath);

    assertThat(new File(configPath)).exists();

    BenchmarkConfig loaded = BenchmarkConfig.load(configPath);

    assertThat(loaded.storageUri()).isEqualTo("gs://test-bucket/data");
    assertThat(loaded.cloudProvider()).isEqualTo(CloudProvider.GCP_GCS);
    assertThat(loaded.warmupIterations()).isEqualTo(2);
    assertThat(loaded.measurementIterations()).isEqualTo(5);
    assertThat(loaded.deleteCounts()).containsExactly(500, 1000);
    assertThat(loaded.strategies()).containsExactly(Strategy.LINEAR, Strategy.INTERVAL_TREE);
  }

  @Test
  public void testLoadFromYaml() throws IOException {
    String yamlContent =
        "storage-uri: \"s3://custom-bucket/test\"\n"
            + "cloud-provider: AWS_S3\n"
            + "delete-counts:\n"
            + "  - 100\n"
            + "  - 200\n"
            + "  - 300\n"
            + "run-counts:\n"
            + "  - 1\n"
            + "  - 2\n"
            + "densities:\n"
            + "  - DENSE\n"
            + "formats:\n"
            + "  - DELETION_VECTOR\n"
            + "strategies:\n"
            + "  - SMART\n"
            + "  - RANGE_QUERY\n"
            + "warmup-iterations: 1\n"
            + "measurement-iterations: 3\n"
            + "output-dir: custom-results\n"
            + "random-seed: 99\n"
            + "fanout-factors:\n"
            + "  - 4\n"
            + "split-factors:\n"
            + "  - 3\n";

    Path configPath = tempDir.resolve("custom-config.yaml");
    Files.writeString(configPath, yamlContent);

    BenchmarkConfig config = BenchmarkConfig.load(configPath.toString());

    assertThat(config.storageUri()).isEqualTo("s3://custom-bucket/test");
    assertThat(config.cloudProvider()).isEqualTo(CloudProvider.AWS_S3);
    assertThat(config.deleteCounts()).containsExactly(100, 200, 300);
    assertThat(config.runCounts()).containsExactly(1, 2);
    assertThat(config.densities()).containsExactly(Density.DENSE);
    assertThat(config.formats()).containsExactly(DeleteFormat.DELETION_VECTOR);
    assertThat(config.strategies()).containsExactly(Strategy.SMART, Strategy.RANGE_QUERY);
    assertThat(config.warmupIterations()).isEqualTo(1);
    assertThat(config.measurementIterations()).isEqualTo(3);
    assertThat(config.outputDir()).isEqualTo("custom-results");
    assertThat(config.randomSeed()).isEqualTo(99);
    assertThat(config.fanoutFactors()).containsExactly(4);
    assertThat(config.splitFactors()).containsExactly(3);
  }

  @Test
  public void testPartialYamlUsesDefaults() throws IOException {
    // YAML with only some fields specified
    String yamlContent =
        "storage-uri: \"file:///custom/path\"\n" + "warmup-iterations: 1\n";

    Path configPath = tempDir.resolve("partial-config.yaml");
    Files.writeString(configPath, yamlContent);

    BenchmarkConfig config = BenchmarkConfig.load(configPath.toString());

    // Specified fields
    assertThat(config.storageUri()).isEqualTo("file:///custom/path");
    assertThat(config.warmupIterations()).isEqualTo(1);

    // Default fields
    assertThat(config.cloudProvider()).isEqualTo(CloudProvider.LOCAL);
    assertThat(config.measurementIterations()).isEqualTo(10);
    assertThat(config.randomSeed()).isEqualTo(42L);
  }

  @Test
  public void testAllCloudProviders() {
    for (CloudProvider provider : CloudProvider.values()) {
      BenchmarkConfig config = BenchmarkConfig.defaults().withCloudProvider(provider);
      assertThat(config.cloudProvider()).isEqualTo(provider);
    }
  }

  @Test
  public void testAllStrategies() {
    List<Strategy> allStrategies = Arrays.asList(Strategy.values());
    BenchmarkConfig config = BenchmarkConfig.defaults().withStrategies(allStrategies);

    assertThat(config.strategies()).containsExactlyElementsOf(allStrategies);
    assertThat(config.strategies()).hasSize(6);
  }

  @Test
  public void testAllDensities() {
    List<Density> allDensities = Arrays.asList(Density.values());
    BenchmarkConfig config = BenchmarkConfig.defaults().withDensities(allDensities);

    assertThat(config.densities()).containsExactlyElementsOf(allDensities);
    assertThat(config.densities()).hasSize(2);
  }

  @Test
  public void testAllFormats() {
    List<DeleteFormat> allFormats = Arrays.asList(DeleteFormat.values());
    BenchmarkConfig config = BenchmarkConfig.defaults().withFormats(allFormats);

    assertThat(config.formats()).containsExactlyElementsOf(allFormats);
    assertThat(config.formats()).hasSize(2);
  }

  @Test
  public void testChainedModifications() {
    BenchmarkConfig config =
        BenchmarkConfig.defaults()
            .withStorageUri("uri1")
            .withStorageUri("uri2")
            .withStorageUri("uri3");

    assertThat(config.storageUri()).isEqualTo("uri3");
  }

  @Test
  public void testSavedYamlFormat() throws IOException {
    BenchmarkConfig config =
        BenchmarkConfig.defaults().withStorageUri("s3://test").withWarmupIterations(5);

    String configPath = tempDir.resolve("output-config.yaml").toString();
    config.save(configPath);

    String content = Files.readString(Path.of(configPath));

    // Verify YAML format
    assertThat(content).contains("storage-uri:");
    assertThat(content).contains("s3://test");
    assertThat(content).contains("warmup-iterations:");
    assertThat(content).contains("5");
  }
}
