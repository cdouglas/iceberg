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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.benchmark.cloud.config.BenchmarkConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestCompactionCloudBenchmark {

  @TempDir File tempDir;

  private BenchmarkConfig config;

  @BeforeEach
  void setUp() {
    config = BenchmarkConfig.defaults()
        .withTableLocation(tempDir.getAbsolutePath())
        .withNumIterations(2)
        .withNumFiles(10)
        .withAvgRowsPerFile(1000)
        .withCollectDetailedStats(true);
  }

  @Test
  void testSimulatedDataFileCreation() {
    PartitionSpec spec = PartitionSpec.unpartitioned();

    DataFile file = SimulatedDataFile.create(spec, 1000);

    assertThat(file).isNotNull();
    assertThat(file.recordCount()).isEqualTo(1000);
    assertThat(file.location()).contains("simulated");
    assertThat(file.location()).endsWith(".parquet");
  }

  @Test
  void testSimulatedDataFileBatch() {
    PartitionSpec spec = PartitionSpec.unpartitioned();

    DataFile[] files = SimulatedDataFile.createBatch(
        spec, 5, 1000, 0.2, new java.util.Random(42));

    assertThat(files).hasSize(5);
    for (DataFile file : files) {
      assertThat(file.recordCount()).isGreaterThan(0);
    }
  }

  @Test
  void testSimulatedDeleteFileCreation() {
    PartitionSpec spec = PartitionSpec.unpartitioned();

    DeleteFile file = SimulatedDeleteFile.createPositionDeletes(spec, 500);

    assertThat(file).isNotNull();
    assertThat(file.recordCount()).isEqualTo(500);
    assertThat(file.location()).contains("delete");
  }

  @Test
  void testWorkloadGeneratorRandom() {
    WorkloadGenerator generator = new WorkloadGenerator.RandomWorkloadGenerator(config);

    List<WorkloadGenerator.WorkloadEvent> events = generator.generate();

    assertThat(events).isNotEmpty();
    // Should have at least initial load
    assertThat(events.stream()
        .anyMatch(e -> e.type() == WorkloadGenerator.EventType.INITIAL_LOAD))
        .isTrue();
  }

  @Test
  void testConflictStatistics() {
    ConflictStatistics stats = new ConflictStatistics();

    stats.recordSuccessfulDelete(1_000_000);
    stats.recordSuccessfulDelete(2_000_000);
    stats.recordConflict();
    stats.recordRemapAttempt(true, 500_000);
    stats.recordSuccessfulCompaction(5, 10000, 5_000_000, true);

    assertThat(stats.getTotalDeletes()).isEqualTo(2);
    assertThat(stats.getSuccessfulDeletes()).isEqualTo(2);
    assertThat(stats.getConflictedDeletes()).isEqualTo(1);
    assertThat(stats.getSuccessfulRemaps()).isEqualTo(1);
    assertThat(stats.getTotalCompactions()).isEqualTo(1);
    assertThat(stats.getAvgDeleteLatencyMs()).isGreaterThan(0);
  }

  @Test
  void testBenchmarkConfig() throws Exception {
    BenchmarkConfig loaded = BenchmarkConfig.defaults();

    assertThat(loaded.formatVersion()).isEqualTo(2);
    assertThat(loaded.compactionMapsEnabled()).isTrue();
    assertThat(loaded.numFiles()).isEqualTo(100);
  }

  @Test
  void testConfigSaveAndLoad() throws Exception {
    File configFile = new File(tempDir, "test-config.yaml");

    config.save(configFile.getAbsolutePath());
    assertThat(configFile).exists();

    BenchmarkConfig loaded = BenchmarkConfig.load(configFile.getAbsolutePath());

    assertThat(loaded.tableLocation()).isEqualTo(config.tableLocation());
    assertThat(loaded.numIterations()).isEqualTo(config.numIterations());
    assertThat(loaded.numFiles()).isEqualTo(config.numFiles());
  }
}
