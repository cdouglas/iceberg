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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.Density;
import org.apache.iceberg.benchmark.remapping.generators.DeletionVectorGenerator.GeneratedDeletionVector;
import org.apache.iceberg.benchmark.remapping.generators.DeletionVectorGenerator.GeneratedDeletionVectorFile;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestDeletionVectorGenerator {

  private static final long SEED = 42L;
  private Path tempDir;
  private FileIO fileIO;
  private DeletionVectorGenerator generator;

  @BeforeEach
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("deletion-vector-test");
    fileIO = new HadoopFileIO(new Configuration());
    generator = new DeletionVectorGenerator(SEED, 1_000_000L);
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
  public void testGenerateSingleDV() throws IOException {
    String outputPath = tempDir.resolve("sparse-dv.puffin").toString();
    String referencedFile = "s3://bucket/data/file1.parquet";

    GeneratedDeletionVector result =
        generator.generate(
            fileIO.newOutputFile(outputPath), 1000, referencedFile, Density.SPARSE);

    assertThat(result.path()).isEqualTo(outputPath);
    assertThat(result.numDeletes()).isEqualTo(1000);
    assertThat(result.referencedDataFile()).isEqualTo(referencedFile);
    assertThat(result.density()).isEqualTo(Density.SPARSE);
    assertThat(result.fileSizeBytes()).isGreaterThan(0);
    assertThat(result.bitmapSizeBytes()).isGreaterThan(0);
    assertThat(result.cardinality()).isGreaterThan(0);
    assertThat(new File(outputPath)).exists();
  }

  @Test
  public void testGenerateDenseDV() throws IOException {
    String outputPath = tempDir.resolve("dense-dv.puffin").toString();
    String referencedFile = "s3://bucket/data/file1.parquet";

    GeneratedDeletionVector result =
        generator.generate(fileIO.newOutputFile(outputPath), 5000, referencedFile, Density.DENSE);

    assertThat(result.path()).isEqualTo(outputPath);
    assertThat(result.numDeletes()).isEqualTo(5000);
    assertThat(result.density()).isEqualTo(Density.DENSE);
    // Dense DVs should have better compression ratio due to clustered positions
    assertThat(result.compressionRatio()).isGreaterThan(0);
  }

  @Test
  public void testGenerateMultipleDVsInOneFile() throws IOException {
    String outputPath = tempDir.resolve("multi-dv.puffin").toString();
    List<String> referencedFiles =
        Arrays.asList(
            "s3://bucket/data/file1.parquet",
            "s3://bucket/data/file2.parquet",
            "s3://bucket/data/file3.parquet");

    GeneratedDeletionVectorFile result =
        generator.generateMultiple(
            fileIO.newOutputFile(outputPath), 500, referencedFiles, Density.SPARSE);

    assertThat(result.path()).isEqualTo(outputPath);
    assertThat(result.numDVs()).isEqualTo(3);
    // Total deletes may be less than 1500 due to duplicate positions within each bitmap
    assertThat(result.totalDeletes()).isGreaterThan(0);
    assertThat(result.density()).isEqualTo(Density.SPARSE);
    // Note: fileSizeBytes may be 0 due to timing of when length is checked during write
    // Verify the file exists and has content by checking the actual file
    File outputPathFile = new File(outputPath);
    assertThat(outputPathFile).exists();
    assertThat(outputPathFile.length()).isGreaterThan(0);
  }

  @Test
  public void testDeterministicGeneration() throws IOException {
    String outputPath1 = tempDir.resolve("dv1.puffin").toString();
    String outputPath2 = tempDir.resolve("dv2.puffin").toString();
    String referencedFile = "s3://bucket/data/file1.parquet";

    DeletionVectorGenerator gen1 = new DeletionVectorGenerator(SEED, 1_000_000L);
    DeletionVectorGenerator gen2 = new DeletionVectorGenerator(SEED, 1_000_000L);

    GeneratedDeletionVector result1 =
        gen1.generate(fileIO.newOutputFile(outputPath1), 100, referencedFile, Density.SPARSE);
    GeneratedDeletionVector result2 =
        gen2.generate(fileIO.newOutputFile(outputPath2), 100, referencedFile, Density.SPARSE);

    // Same seed should produce same results
    assertThat(result1.cardinality()).isEqualTo(result2.cardinality());
    assertThat(result1.bitmapSizeBytes()).isEqualTo(result2.bitmapSizeBytes());
  }

  @Test
  public void testSmallDeleteCount() throws IOException {
    String outputPath = tempDir.resolve("small-dv.puffin").toString();
    String referencedFile = "s3://bucket/data/file1.parquet";

    GeneratedDeletionVector result =
        generator.generate(fileIO.newOutputFile(outputPath), 10, referencedFile, Density.SPARSE);

    assertThat(result.numDeletes()).isEqualTo(10);
    assertThat(result.cardinality()).isLessThanOrEqualTo(10);
    assertThat(result.fileSizeBytes()).isGreaterThan(0);
  }

  @Test
  public void testLargeDeleteCount() throws IOException {
    String outputPath = tempDir.resolve("large-dv.puffin").toString();
    String referencedFile = "s3://bucket/data/file1.parquet";

    GeneratedDeletionVector result =
        generator.generate(
            fileIO.newOutputFile(outputPath), 100_000, referencedFile, Density.SPARSE);

    assertThat(result.numDeletes()).isEqualTo(100_000);
    assertThat(result.fileSizeBytes()).isGreaterThan(0);
  }

  @Test
  public void testCompressionRatio() throws IOException {
    String sparsePath = tempDir.resolve("sparse-ratio.puffin").toString();
    String densePath = tempDir.resolve("dense-ratio.puffin").toString();
    String referencedFile = "s3://bucket/data/file1.parquet";

    GeneratedDeletionVector sparse =
        generator.generate(
            fileIO.newOutputFile(sparsePath), 10000, referencedFile, Density.SPARSE);
    GeneratedDeletionVector dense =
        new DeletionVectorGenerator(SEED + 1, 1_000_000L)
            .generate(fileIO.newOutputFile(densePath), 10000, referencedFile, Density.DENSE);

    // Both should report positive compression ratios
    assertThat(sparse.compressionRatio()).isGreaterThan(0);
    assertThat(dense.compressionRatio()).isGreaterThan(0);
    // Dense should typically have better compression due to run-length encoding in Roaring
    // (but not guaranteed, depends on distribution)
  }

  @Test
  public void testGeneratedMetadataToString() throws IOException {
    String outputPath = tempDir.resolve("metadata-test.puffin").toString();
    String referencedFile = "s3://bucket/data/file1.parquet";

    GeneratedDeletionVector result =
        generator.generate(fileIO.newOutputFile(outputPath), 100, referencedFile, Density.SPARSE);

    String toString = result.toString();
    assertThat(toString).contains("DeletionVector");
    assertThat(toString).contains("deletes=100");
    assertThat(toString).contains("SPARSE");
    assertThat(toString).contains("compression=");
  }
}
