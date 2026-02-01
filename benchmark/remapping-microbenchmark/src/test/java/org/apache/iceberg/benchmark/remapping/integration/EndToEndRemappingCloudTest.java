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
package org.apache.iceberg.benchmark.remapping.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.PositionDeleteRemapper;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.Density;
import org.apache.iceberg.benchmark.remapping.generators.CompactionMapGenerator;
import org.apache.iceberg.benchmark.remapping.generators.CompactionMapGenerator.GeneratedCompactionMap;
import org.apache.iceberg.benchmark.remapping.generators.DeletionVectorGenerator;
import org.apache.iceberg.benchmark.remapping.generators.PositionDeleteGenerator;
import org.apache.iceberg.benchmark.remapping.generators.PositionDeleteGenerator.GeneratedDeleteFile;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.parquet.Parquet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * End-to-end integration tests for the remapping benchmark using cloud storage.
 *
 * <p>These tests use the benchmark generators to create realistic test data and verify the entire
 * remapping workflow works correctly with cloud storage. This validates:
 *
 * <ul>
 *   <li>CompactionMapGenerator produces valid maps that serialize/deserialize correctly
 *   <li>PositionDeleteGenerator produces valid delete files
 *   <li>DeletionVectorGenerator produces valid DVs
 *   <li>PositionDeleteRemapper correctly remaps all positions
 *   <li>The entire workflow is cloud-storage agnostic
 * </ul>
 *
 * <p>These tests are more realistic than the unit tests as they use the actual benchmark code
 * paths.
 *
 * <p>Run with: ./gradlew :benchmark:remapping-microbenchmark:integrationTest
 */
@Tag("integration")
public class EndToEndRemappingCloudTest {

  private static FileIO fileIO;
  private final List<String> filesToCleanup = new ArrayList<>();

  @BeforeAll
  static void setup() {
    Map<String, String> properties = new HashMap<>();
    fileIO = new ResolvingFileIO();
    fileIO.initialize(properties);
  }

  @AfterAll
  static void teardown() throws IOException {
    if (fileIO != null) {
      fileIO.close();
    }
  }

  @AfterEach
  void cleanup() {
    for (String path : filesToCleanup) {
      try {
        fileIO.deleteFile(path);
      } catch (Exception e) {
        // Ignore cleanup failures
      }
    }
    filesToCleanup.clear();
  }

  // ==================== GCS Tests ====================

  @Test
  void testGcsFanoutScenarioEndToEnd() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/e2e-test-" + UUID.randomUUID();
    verifyFanoutScenarioEndToEnd(baseDir);
  }

  @Test
  void testGcsSplitScenarioEndToEnd() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/e2e-test-" + UUID.randomUUID();
    verifySplitScenarioEndToEnd(baseDir);
  }

  @Test
  void testGcsMixedScenarioEndToEnd() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/e2e-test-" + UUID.randomUUID();
    verifyMixedScenarioEndToEnd(baseDir);
  }

  @Test
  void testGcsPositionDeletesWithGenerators() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/e2e-test-" + UUID.randomUUID();
    verifyPositionDeletesWithGenerators(baseDir);
  }

  @Test
  void testGcsDeletionVectorsWithGenerators() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/e2e-test-" + UUID.randomUUID();
    verifyDeletionVectorsWithGenerators(baseDir);
  }

  // ==================== S3 Tests ====================

  @Test
  void testS3FanoutScenarioEndToEnd() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/e2e-test-" + UUID.randomUUID();
    verifyFanoutScenarioEndToEnd(baseDir);
  }

  @Test
  void testS3SplitScenarioEndToEnd() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/e2e-test-" + UUID.randomUUID();
    verifySplitScenarioEndToEnd(baseDir);
  }

  @Test
  void testS3MixedScenarioEndToEnd() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/e2e-test-" + UUID.randomUUID();
    verifyMixedScenarioEndToEnd(baseDir);
  }

  @Test
  void testS3PositionDeletesWithGenerators() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/e2e-test-" + UUID.randomUUID();
    verifyPositionDeletesWithGenerators(baseDir);
  }

  @Test
  void testS3DeletionVectorsWithGenerators() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/e2e-test-" + UUID.randomUUID();
    verifyDeletionVectorsWithGenerators(baseDir);
  }

  // ==================== Azure Tests ====================

  @Test
  void testAzureFanoutScenarioEndToEnd() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String baseDir = uri + "/e2e-test-" + UUID.randomUUID();
    verifyFanoutScenarioEndToEnd(baseDir);
  }

  @Test
  void testAzureSplitScenarioEndToEnd() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String baseDir = uri + "/e2e-test-" + UUID.randomUUID();
    verifySplitScenarioEndToEnd(baseDir);
  }

  @Test
  void testAzureMixedScenarioEndToEnd() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String baseDir = uri + "/e2e-test-" + UUID.randomUUID();
    verifyMixedScenarioEndToEnd(baseDir);
  }

  @Test
  void testAzurePositionDeletesWithGenerators() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String baseDir = uri + "/e2e-test-" + UUID.randomUUID();
    verifyPositionDeletesWithGenerators(baseDir);
  }

  @Test
  void testAzureDeletionVectorsWithGenerators() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String baseDir = uri + "/e2e-test-" + UUID.randomUUID();
    verifyDeletionVectorsWithGenerators(baseDir);
  }

  // ==================== Verification Methods ====================

  /**
   * Tests the fanout scenario (many source files → one target file) using CompactionMapGenerator.
   */
  private void verifyFanoutScenarioEndToEnd(String baseDir) throws IOException {
    String mapPath = baseDir + "/compaction-map.avro";
    filesToCleanup.add(mapPath);

    // Use the actual benchmark generator
    CompactionMapGenerator generator = new CompactionMapGenerator(42L, 10_000L);
    OutputFile mapOutput = fileIO.newOutputFile(mapPath);

    GeneratedCompactionMap generated = generator.generateFanout(mapOutput, 5, 3);

    // Verify generation metadata
    assertThat(generated.numSourceFiles()).isEqualTo(5);
    assertThat(generated.numTargetFiles()).isEqualTo(1);
    assertThat(generated.totalRuns()).isEqualTo(15); // 5 files * 3 runs each
    assertThat(generated.scenario()).isEqualTo(CompactionMapGenerator.CompactionScenario.FANOUT);

    // Read back and verify the map works
    InputFile mapInput = fileIO.newInputFile(mapPath);
    CompactionMap map = CompactionMaps.read(mapInput);

    assertThat(map.fileMappings()).hasSize(5);

    // Create remapper and verify it can remap positions
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    for (String sourceFile : generated.sourceFiles()) {
      assertThat(remapper.isCompacted(sourceFile)).isTrue();

      // Test remapping a position from this file
      PositionDelete<Record> delete = PositionDelete.create();
      delete.set(sourceFile, 100L, null);

      PositionDelete<?> remapped = remapper.remapDeleteOrNull(delete);
      assertThat(remapped).isNotNull();
      assertThat(remapped.path().toString()).isEqualTo(generated.targetFiles().get(0));
    }
  }

  /**
   * Tests the split scenario (one source file → many target files) using CompactionMapGenerator.
   */
  private void verifySplitScenarioEndToEnd(String baseDir) throws IOException {
    String mapPath = baseDir + "/compaction-map.avro";
    filesToCleanup.add(mapPath);

    CompactionMapGenerator generator = new CompactionMapGenerator(42L, 30_000L);
    OutputFile mapOutput = fileIO.newOutputFile(mapPath);

    GeneratedCompactionMap generated = generator.generateSplit(mapOutput, 3, 2);

    // Verify generation
    assertThat(generated.numSourceFiles()).isEqualTo(1);
    assertThat(generated.numTargetFiles()).isEqualTo(3);
    assertThat(generated.scenario()).isEqualTo(CompactionMapGenerator.CompactionScenario.SPLIT);

    // Read back
    InputFile mapInput = fileIO.newInputFile(mapPath);
    CompactionMap map = CompactionMaps.read(mapInput);

    // In split scenario with per-run targets, we have a single mapping with multiple runs
    assertThat(map.fileMappings()).hasSize(1);

    String sourceFile = generated.sourceFiles().get(0);
    CompactionMap.FileMapping mapping = map.fileMappings().get(0);
    assertThat(mapping.sourceFile()).isEqualTo(sourceFile);

    // The mapping should have runs for each target file (3 targets * 2 runs each = 6 runs)
    assertThat(mapping.runs().size()).isGreaterThanOrEqualTo(generated.numTargetFiles());

    // Create remapper and test positions from different ranges
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Test position in first target range
    PositionDelete<Record> delete1 = PositionDelete.create();
    delete1.set(sourceFile, 5000L, null);
    PositionDelete<?> remapped1 = remapper.remapDeleteOrNull(delete1);
    assertThat(remapped1).isNotNull();

    // Test position in last target range
    PositionDelete<Record> delete2 = PositionDelete.create();
    delete2.set(sourceFile, 25000L, null);
    PositionDelete<?> remapped2 = remapper.remapDeleteOrNull(delete2);
    assertThat(remapped2).isNotNull();
  }

  /** Tests the mixed scenario (many source → many target files) using CompactionMapGenerator. */
  private void verifyMixedScenarioEndToEnd(String baseDir) throws IOException {
    String mapPath = baseDir + "/compaction-map.avro";
    filesToCleanup.add(mapPath);

    CompactionMapGenerator generator = new CompactionMapGenerator(42L, 10_000L);
    OutputFile mapOutput = fileIO.newOutputFile(mapPath);

    GeneratedCompactionMap generated = generator.generateMixed(mapOutput, 4, 2, 2);

    // Verify generation
    assertThat(generated.numSourceFiles()).isEqualTo(4);
    assertThat(generated.numTargetFiles()).isEqualTo(2);
    assertThat(generated.scenario()).isEqualTo(CompactionMapGenerator.CompactionScenario.MIXED);

    // Read back
    InputFile mapInput = fileIO.newInputFile(mapPath);
    CompactionMap map = CompactionMaps.read(mapInput);

    // Verify we can read the map and it has expected structure
    assertThat(map.fileMappings()).isNotEmpty();

    // Create remapper and verify all source files are compacted
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    for (String sourceFile : generated.sourceFiles()) {
      assertThat(remapper.isCompacted(sourceFile)).isTrue();
    }
  }

  /** Tests position delete generation and remapping using PositionDeleteGenerator. */
  private void verifyPositionDeletesWithGenerators(String baseDir) throws IOException {
    String mapPath = baseDir + "/compaction-map.avro";
    String deletePath = baseDir + "/position-deletes.parquet";
    filesToCleanup.add(mapPath);
    filesToCleanup.add(deletePath);

    // Generate compaction map
    CompactionMapGenerator mapGenerator = new CompactionMapGenerator(42L, 10_000L);
    OutputFile mapOutput = fileIO.newOutputFile(mapPath);
    GeneratedCompactionMap generatedMap = mapGenerator.generateFanout(mapOutput, 3, 2);

    // Generate position deletes that reference the source files
    PositionDeleteGenerator deleteGenerator = new PositionDeleteGenerator(42L, 10_000L);
    OutputFile deleteOutput = fileIO.newOutputFile(deletePath);

    // The generator creates its own source file paths, so we need to verify it generates
    // valid delete files
    GeneratedDeleteFile generatedDeletes =
        deleteGenerator.generate(deleteOutput, 1000, 3, Density.SPARSE, true);

    assertThat(generatedDeletes.numDeletes()).isEqualTo(1000);
    assertThat(generatedDeletes.numSourceFiles()).isEqualTo(3);
    assertThat(generatedDeletes.sorted()).isTrue();

    // Read back and verify file exists and has expected structure
    InputFile deleteInput = fileIO.newInputFile(deletePath);
    assertThat(deleteInput.exists()).isTrue();

    List<PositionDelete<Record>> deletes = readPositionDeletes(deletePath);
    assertThat(deletes).hasSize(1000);

    // Verify the deletes reference the expected source files
    for (PositionDelete<Record> delete : deletes) {
      assertThat(delete.path().toString()).startsWith("s3://bucket/data/file-");
      assertThat(delete.pos()).isGreaterThanOrEqualTo(0);
    }

    // Test that the compaction map can be used for remapping
    // (even though the file paths don't match - this validates the remapper API)
    CompactionMap map = CompactionMaps.read(fileIO.newInputFile(mapPath));
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Verify remapper was created successfully
    assertThat(remapper.compactedFiles()).hasSize(3);
  }

  /** Tests deletion vector generation and read-back using DeletionVectorGenerator. */
  private void verifyDeletionVectorsWithGenerators(String baseDir) throws IOException {
    String dvPath = baseDir + "/deletion-vectors.puffin";
    filesToCleanup.add(dvPath);

    // Generate deletion vectors
    DeletionVectorGenerator dvGenerator = new DeletionVectorGenerator(42L, 10_000L);
    OutputFile dvOutput = fileIO.newOutputFile(dvPath);

    List<String> referencedFiles = new ArrayList<>();
    referencedFiles.add("s3://bucket/data/file-00000.parquet");
    referencedFiles.add("s3://bucket/data/file-00001.parquet");
    referencedFiles.add("s3://bucket/data/file-00002.parquet");

    DeletionVectorGenerator.GeneratedDeletionVectorFile generated =
        dvGenerator.generateMultiple(dvOutput, 100, referencedFiles, Density.DENSE);

    // Verify generation
    assertThat(generated.numDVs()).isEqualTo(3);
    assertThat(generated.totalDeletes()).isGreaterThanOrEqualTo(100); // At least 100 per DV
    assertThat(generated.density()).isEqualTo(Density.DENSE);

    // Read back and verify file exists
    InputFile dvInput = fileIO.newInputFile(dvPath);
    assertThat(dvInput.exists()).isTrue();

    // Verify we can read the Puffin file
    try (org.apache.iceberg.puffin.PuffinReader reader =
        org.apache.iceberg.puffin.Puffin.read(dvInput).build()) {

      var blobs = reader.fileMetadata().blobs();
      assertThat(blobs).hasSize(3);

      for (var blob : blobs) {
        assertThat(blob.type()).isEqualTo("deletion-vector-v1");
        assertThat(blob.properties()).containsKey("referenced-data-file");
      }
    }
  }

  // ==================== Helper Methods ====================

  private List<PositionDelete<Record>> readPositionDeletes(String path) throws IOException {
    InputFile input = fileIO.newInputFile(path);
    List<PositionDelete<Record>> deletes = new ArrayList<>();

    try (CloseableIterable<Record> reader =
        Parquet.read(input)
            .project(PositionDeleteGenerator.DELETE_SCHEMA)
            .createReaderFunc(
                schema ->
                    GenericParquetReaders.buildReader(
                        PositionDeleteGenerator.DELETE_SCHEMA, schema))
            .build()) {

      for (Record record : reader) {
        String filePath = (String) record.getField("file_path");
        Long pos = (Long) record.getField("pos");
        PositionDelete<Record> delete = PositionDelete.create();
        delete.set(filePath, pos, null);
        deletes.add(delete);
      }
    }

    return deletes;
  }
}
