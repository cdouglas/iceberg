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
package org.apache.iceberg.benchmark.remapping.generators;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.benchmark.remapping.generators.CompactionMapGenerator.CompactionScenario;
import org.apache.iceberg.benchmark.remapping.generators.CompactionMapGenerator.GeneratedCompactionMap;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestCompactionMapGenerator {

  private static final long SEED = 42L;
  private static final long ROWS_PER_FILE = 1_000_000L;
  private Path tempDir;
  private FileIO fileIO;
  private CompactionMapGenerator generator;

  @BeforeEach
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("compaction-map-test");
    fileIO = new HadoopFileIO(new Configuration());
    generator = new CompactionMapGenerator(SEED, ROWS_PER_FILE);
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (tempDir != null && Files.exists(tempDir)) {
      Files.walk(tempDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
  }

  @Test
  public void testGenerateFanoutScenario() throws IOException {
    String outputPath = tempDir.resolve("fanout-map.avro").toString();

    GeneratedCompactionMap result =
        generator.generateFanout(fileIO.newOutputFile(outputPath), 5, 1);

    assertThat(result.path()).isEqualTo(outputPath);
    assertThat(result.numSourceFiles()).isEqualTo(5);
    assertThat(result.numTargetFiles()).isEqualTo(1);
    assertThat(result.totalRuns()).isEqualTo(5); // 5 files x 1 run each
    assertThat(result.scenario()).isEqualTo(CompactionScenario.FANOUT);
    assertThat(result.totalSourceRows()).isEqualTo(5 * ROWS_PER_FILE);
    assertThat(result.fileSizeBytes()).isGreaterThan(0);
    assertThat(new File(outputPath)).exists();

    // Verify the compaction map can be read back
    CompactionMap map = CompactionMaps.read(fileIO.newInputFile(outputPath));
    assertThat(map.fileMappings()).hasSize(5);
  }

  @Test
  public void testGenerateFanoutWithMultipleRuns() throws IOException {
    String outputPath = tempDir.resolve("fanout-multi-run.avro").toString();

    GeneratedCompactionMap result =
        generator.generateFanout(fileIO.newOutputFile(outputPath), 3, 4);

    assertThat(result.numSourceFiles()).isEqualTo(3);
    assertThat(result.totalRuns()).isEqualTo(12); // 3 files x 4 runs each
    assertThat(result.avgRunsPerSource()).isEqualTo(4.0);
  }

  // Note: Split scenario tests are disabled because CompactionMapBuilder doesn't support
  // one-to-many mappings (same source file to multiple targets). The CompactionMap format
  // is designed for fanout (many-to-one) scenarios. Split scenarios would require a
  // different approach where each "split" creates a separate source file representation.

  @Test
  public void testGenerateMixedScenario() throws IOException {
    String outputPath = tempDir.resolve("mixed-map.avro").toString();

    GeneratedCompactionMap result =
        generator.generateMixed(fileIO.newOutputFile(outputPath), 4, 2, 2);

    assertThat(result.path()).isEqualTo(outputPath);
    assertThat(result.numSourceFiles()).isEqualTo(4);
    assertThat(result.numTargetFiles()).isEqualTo(2);
    assertThat(result.scenario()).isEqualTo(CompactionScenario.MIXED);
    assertThat(result.totalSourceRows()).isEqualTo(4 * ROWS_PER_FILE);
    assertThat(result.totalRuns()).isGreaterThan(0);
    assertThat(new File(outputPath)).exists();
  }

  @Test
  public void testSourceFilePaths() throws IOException {
    String outputPath = tempDir.resolve("source-paths.avro").toString();

    GeneratedCompactionMap result =
        generator.generateFanout(fileIO.newOutputFile(outputPath), 3, 1);

    assertThat(result.sourceFiles()).hasSize(3);
    assertThat(result.sourceFiles())
        .containsExactly(
            "s3://bucket/data/source-00000.parquet",
            "s3://bucket/data/source-00001.parquet",
            "s3://bucket/data/source-00002.parquet");
  }

  @Test
  public void testTargetFilePaths() throws IOException {
    // Test target file paths in a fanout scenario (all sources merge to one target)
    String outputPath = tempDir.resolve("target-paths.avro").toString();

    GeneratedCompactionMap result =
        generator.generateFanout(fileIO.newOutputFile(outputPath), 3, 1);

    // Fanout creates a single target file
    assertThat(result.targetFiles()).hasSize(1);
    assertThat(result.targetFiles().get(0)).contains("compacted");
  }

  @Test
  public void testLargeFanout() throws IOException {
    String outputPath = tempDir.resolve("large-fanout.avro").toString();

    GeneratedCompactionMap result =
        generator.generateFanout(fileIO.newOutputFile(outputPath), 100, 1);

    assertThat(result.numSourceFiles()).isEqualTo(100);
    assertThat(result.totalRuns()).isEqualTo(100);
    assertThat(result.fileSizeBytes()).isGreaterThan(0);

    // Verify readability
    CompactionMap map = CompactionMaps.read(fileIO.newInputFile(outputPath));
    assertThat(map.fileMappings()).hasSize(100);
  }

  @Test
  public void testSingleFileMapping() throws IOException {
    String outputPath = tempDir.resolve("single-file.avro").toString();

    GeneratedCompactionMap result =
        generator.generateFanout(fileIO.newOutputFile(outputPath), 1, 1);

    assertThat(result.numSourceFiles()).isEqualTo(1);
    assertThat(result.numTargetFiles()).isEqualTo(1);
    assertThat(result.totalRuns()).isEqualTo(1);
  }

  @Test
  public void testGeneratedMetadataToString() throws IOException {
    String outputPath = tempDir.resolve("metadata-test.avro").toString();

    GeneratedCompactionMap result =
        generator.generateFanout(fileIO.newOutputFile(outputPath), 3, 2);

    String toString = result.toString();
    assertThat(toString).contains("CompactionMap");
    assertThat(toString).contains("sources=3");
    assertThat(toString).contains("targets=1");
    assertThat(toString).contains("runs=6");
    assertThat(toString).contains("FANOUT");
  }

  @Test
  public void testAvgRunsPerSource() throws IOException {
    String outputPath = tempDir.resolve("avg-runs.avro").toString();

    GeneratedCompactionMap result =
        generator.generateFanout(fileIO.newOutputFile(outputPath), 4, 5);

    assertThat(result.avgRunsPerSource()).isEqualTo(5.0);
  }
}
