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
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.Density;
import org.apache.iceberg.benchmark.remapping.generators.PositionDeleteGenerator.GeneratedDeleteFile;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestPositionDeleteGenerator {

  private static final long SEED = 42L;
  private Path tempDir;
  private FileIO fileIO;
  private PositionDeleteGenerator generator;

  @BeforeEach
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("position-delete-test");
    fileIO = new HadoopFileIO(new Configuration());
    generator = new PositionDeleteGenerator(SEED, 1_000_000L);
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (tempDir != null && Files.exists(tempDir)) {
      Files.walk(tempDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
  }

  @Test
  public void testGenerateSparseDeletes() throws IOException {
    String outputPath = tempDir.resolve("sparse-deletes.parquet").toString();

    GeneratedDeleteFile result =
        generator.generate(fileIO.newOutputFile(outputPath), 1000, 2, Density.SPARSE, false);

    assertThat(result.path()).isEqualTo(outputPath);
    assertThat(result.numDeletes()).isEqualTo(1000);
    assertThat(result.numSourceFiles()).isEqualTo(2);
    assertThat(result.density()).isEqualTo(Density.SPARSE);
    assertThat(result.sorted()).isFalse();
    assertThat(result.fileSizeBytes()).isGreaterThan(0);
    assertThat(result.sourceFiles()).hasSize(2);
    assertThat(new File(outputPath)).exists();
  }

  @Test
  public void testGenerateDenseDeletes() throws IOException {
    String outputPath = tempDir.resolve("dense-deletes.parquet").toString();

    GeneratedDeleteFile result =
        generator.generate(fileIO.newOutputFile(outputPath), 500, 1, Density.DENSE, false);

    assertThat(result.path()).isEqualTo(outputPath);
    assertThat(result.numDeletes()).isEqualTo(500);
    assertThat(result.density()).isEqualTo(Density.DENSE);
    assertThat(result.fileSizeBytes()).isGreaterThan(0);
  }

  @Test
  public void testGenerateSortedDeletes() throws IOException {
    String outputPath = tempDir.resolve("sorted-deletes.parquet").toString();

    GeneratedDeleteFile result =
        generator.generate(fileIO.newOutputFile(outputPath), 100, 3, Density.SPARSE, true);

    assertThat(result.path()).isEqualTo(outputPath);
    assertThat(result.sorted()).isTrue();
    assertThat(result.numSourceFiles()).isEqualTo(3);
  }

  @Test
  public void testDeterministicGeneration() throws IOException {
    String outputPath1 = tempDir.resolve("deletes1.parquet").toString();
    String outputPath2 = tempDir.resolve("deletes2.parquet").toString();

    // Create two generators with same seed
    PositionDeleteGenerator gen1 = new PositionDeleteGenerator(SEED, 1_000_000L);
    PositionDeleteGenerator gen2 = new PositionDeleteGenerator(SEED, 1_000_000L);

    GeneratedDeleteFile result1 =
        gen1.generate(fileIO.newOutputFile(outputPath1), 100, 2, Density.SPARSE, true);
    GeneratedDeleteFile result2 =
        gen2.generate(fileIO.newOutputFile(outputPath2), 100, 2, Density.SPARSE, true);

    // Files should be the same size (deterministic positions)
    assertThat(result1.fileSizeBytes()).isEqualTo(result2.fileSizeBytes());
    assertThat(result1.numDeletes()).isEqualTo(result2.numDeletes());
  }

  @Test
  public void testMultipleSourceFiles() throws IOException {
    String outputPath = tempDir.resolve("multi-source-deletes.parquet").toString();

    GeneratedDeleteFile result =
        generator.generate(fileIO.newOutputFile(outputPath), 2500, 5, Density.SPARSE, false);

    assertThat(result.numDeletes()).isEqualTo(2500);
    assertThat(result.numSourceFiles()).isEqualTo(5);
    assertThat(result.sourceFiles()).hasSize(5);
    assertThat(result.sourceFiles().get(0)).contains("file-00000.parquet");
    assertThat(result.sourceFiles().get(4)).contains("file-00004.parquet");
  }

  @Test
  public void testSmallDeleteCount() throws IOException {
    String outputPath = tempDir.resolve("small-deletes.parquet").toString();

    GeneratedDeleteFile result =
        generator.generate(fileIO.newOutputFile(outputPath), 10, 1, Density.SPARSE, false);

    assertThat(result.numDeletes()).isEqualTo(10);
    assertThat(result.fileSizeBytes()).isGreaterThan(0);
  }

  @Test
  public void testLargeDeleteCount() throws IOException {
    String outputPath = tempDir.resolve("large-deletes.parquet").toString();

    GeneratedDeleteFile result =
        generator.generate(fileIO.newOutputFile(outputPath), 100_000, 1, Density.SPARSE, false);

    assertThat(result.numDeletes()).isEqualTo(100_000);
    assertThat(result.fileSizeBytes()).isGreaterThan(0);
  }

  @Test
  public void testGeneratedMetadataToString() throws IOException {
    String outputPath = tempDir.resolve("metadata-test.parquet").toString();

    GeneratedDeleteFile result =
        generator.generate(fileIO.newOutputFile(outputPath), 100, 2, Density.SPARSE, true);

    String toString = result.toString();
    assertThat(toString).contains("PositionDeleteFile");
    assertThat(toString).contains("deletes=100");
    assertThat(toString).contains("files=2");
    assertThat(toString).contains("SPARSE");
    assertThat(toString).contains("sorted=true");
  }

  @Test
  public void testDeleteSchema() {
    // Verify the schema has the expected structure
    assertThat(PositionDeleteGenerator.DELETE_SCHEMA.columns()).hasSize(2);
    assertThat(PositionDeleteGenerator.DELETE_SCHEMA.findField("file_path")).isNotNull();
    assertThat(PositionDeleteGenerator.DELETE_SCHEMA.findField("pos")).isNotNull();
  }

  @Test
  public void testDefaultMaxRowsPerFile() throws IOException {
    // Test using default constructor
    PositionDeleteGenerator defaultGenerator = new PositionDeleteGenerator(SEED);
    String outputPath = tempDir.resolve("default-config.parquet").toString();

    GeneratedDeleteFile result =
        defaultGenerator.generate(fileIO.newOutputFile(outputPath), 50, 1, Density.SPARSE, false);

    assertThat(result.numDeletes()).isEqualTo(50);
  }
}
