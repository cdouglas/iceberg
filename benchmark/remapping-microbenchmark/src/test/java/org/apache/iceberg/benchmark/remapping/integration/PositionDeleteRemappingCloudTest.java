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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMapBuilder;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.PositionDeleteRemapper;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for position delete remapping with cloud storage.
 *
 * <p>These tests verify that the PositionDeleteRemapper correctly transforms position deletes when
 * using cloud storage for compaction maps and delete files. This validates the full end-to-end flow
 * of:
 *
 * <ol>
 *   <li>Writing compaction maps to cloud storage
 *   <li>Reading position deletes from cloud storage
 *   <li>Remapping positions using the compaction map
 *   <li>Writing remapped position deletes back to cloud storage
 *   <li>Verifying the remapped positions are correct
 * </ol>
 *
 * <p>Run with: ./gradlew :benchmark:remapping-microbenchmark:integrationTest
 */
@Tag("integration")
public class PositionDeleteRemappingCloudTest {

  private static final Schema DELETE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "file_path", Types.StringType.get()),
          Types.NestedField.required(2, "pos", Types.LongType.get()));

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
  void testGcsSimpleRemapping() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/remapping-test-" + UUID.randomUUID();
    verifySimpleRemapping(baseDir);
  }

  @Test
  void testGcsFanoutRemapping() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/remapping-test-" + UUID.randomUUID();
    verifyFanoutRemapping(baseDir);
  }

  @Test
  void testGcsSplitRemapping() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/remapping-test-" + UUID.randomUUID();
    verifySplitRemapping(baseDir);
  }

  @Test
  void testGcsLargeScaleRemapping() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/remapping-test-" + UUID.randomUUID();
    verifyLargeScaleRemapping(baseDir);
  }

  @Test
  void testGcsMergeCompactionWithFilteredRows() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/remapping-test-" + UUID.randomUUID();
    verifyMergeCompactionWithFilteredRows(baseDir);
  }

  // ==================== S3 Tests ====================

  @Test
  void testS3SimpleRemapping() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/remapping-test-" + UUID.randomUUID();
    verifySimpleRemapping(baseDir);
  }

  @Test
  void testS3FanoutRemapping() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/remapping-test-" + UUID.randomUUID();
    verifyFanoutRemapping(baseDir);
  }

  @Test
  void testS3SplitRemapping() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/remapping-test-" + UUID.randomUUID();
    verifySplitRemapping(baseDir);
  }

  @Test
  void testS3LargeScaleRemapping() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/remapping-test-" + UUID.randomUUID();
    verifyLargeScaleRemapping(baseDir);
  }

  @Test
  void testS3MergeCompactionWithFilteredRows() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/remapping-test-" + UUID.randomUUID();
    verifyMergeCompactionWithFilteredRows(baseDir);
  }

  // ==================== Azure Tests ====================

  @Test
  void testAzureSimpleRemapping() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String baseDir = uri + "/remapping-test-" + UUID.randomUUID();
    verifySimpleRemapping(baseDir);
  }

  @Test
  void testAzureFanoutRemapping() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String baseDir = uri + "/remapping-test-" + UUID.randomUUID();
    verifyFanoutRemapping(baseDir);
  }

  @Test
  void testAzureSplitRemapping() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String baseDir = uri + "/remapping-test-" + UUID.randomUUID();
    verifySplitRemapping(baseDir);
  }

  @Test
  void testAzureLargeScaleRemapping() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String baseDir = uri + "/remapping-test-" + UUID.randomUUID();
    verifyLargeScaleRemapping(baseDir);
  }

  @Test
  void testAzureMergeCompactionWithFilteredRows() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String baseDir = uri + "/remapping-test-" + UUID.randomUUID();
    verifyMergeCompactionWithFilteredRows(baseDir);
  }

  // ==================== Verification Methods ====================

  /** Simple 1:1 remapping where positions shift. */
  private void verifySimpleRemapping(String baseDir) throws IOException {
    String mapPath = baseDir + "/compaction-map.avro";
    String deleteInputPath = baseDir + "/position-deletes-input.parquet";
    String deleteOutputPath = baseDir + "/position-deletes-output.parquet";
    filesToCleanup.add(mapPath);
    filesToCleanup.add(deleteInputPath);
    filesToCleanup.add(deleteOutputPath);

    // Create compaction map: source file positions shift by 1000
    String sourceFile = "s3://bucket/data/source-00000.parquet";
    String targetFile = "s3://bucket/data/target-00000.parquet";

    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder
        .addFileMapping(sourceFile, targetFile)
        .addRun(0, 1000, 5000); // positions 0-4999 map to 1000-5999

    // Write compaction map
    CompactionMaps.write(builder.build(), fileIO.newOutputFile(mapPath));

    // Create position deletes referencing source file
    List<PositionDelete<Record>> deletes = new ArrayList<>();
    long[] testPositions = {0, 100, 500, 1000, 2500, 4999};
    for (long pos : testPositions) {
      PositionDelete<Record> delete = PositionDelete.create();
      delete.set(sourceFile, pos, null);
      deletes.add(delete);
    }

    // Write position deletes
    writePositionDeletes(deleteInputPath, deletes);

    // Read compaction map and create remapper
    CompactionMap map = CompactionMaps.read(fileIO.newInputFile(mapPath));
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Verify file is marked as compacted
    assertThat(remapper.isCompacted(sourceFile)).isTrue();
    assertThat(remapper.isCompacted("other-file.parquet")).isFalse();

    // Read and remap position deletes
    List<PositionDelete<Record>> inputDeletes = readPositionDeletes(deleteInputPath);
    List<PositionDelete<Record>> remappedDeletes = new ArrayList<>();

    for (PositionDelete<Record> delete : inputDeletes) {
      @SuppressWarnings("unchecked")
      PositionDelete<Record> remapped = (PositionDelete<Record>) remapper.remapDeleteOrNull(delete);
      assertThat(remapped).isNotNull();
      remappedDeletes.add(remapped);
    }

    // Write remapped deletes
    writePositionDeletes(deleteOutputPath, remappedDeletes);

    // Read back and verify
    List<PositionDelete<Record>> outputDeletes = readPositionDeletes(deleteOutputPath);
    assertThat(outputDeletes).hasSize(testPositions.length);

    // Verify positions are correctly shifted by 1000
    for (int i = 0; i < testPositions.length; i++) {
      assertThat(outputDeletes.get(i).path().toString()).isEqualTo(targetFile);
      assertThat(outputDeletes.get(i).pos()).isEqualTo(testPositions[i] + 1000);
    }
  }

  /** Fanout scenario: multiple source files → single target file. */
  private void verifyFanoutRemapping(String baseDir) throws IOException {
    String mapPath = baseDir + "/compaction-map.avro";
    String deleteInputPath = baseDir + "/position-deletes-input.parquet";
    String deleteOutputPath = baseDir + "/position-deletes-output.parquet";
    filesToCleanup.add(mapPath);
    filesToCleanup.add(deleteInputPath);
    filesToCleanup.add(deleteOutputPath);

    // Create compaction map: 3 source files → 1 target file
    String targetFile = "s3://bucket/data/compacted.parquet";
    int numSourceFiles = 3;
    long rowsPerFile = 1000;

    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    long targetOffset = 0;

    for (int i = 0; i < numSourceFiles; i++) {
      String sourceFile = String.format(Locale.ROOT, "s3://bucket/data/source-%05d.parquet", i);
      builder.addFileMapping(sourceFile, targetFile).addRun(0, targetOffset, rowsPerFile);
      targetOffset += rowsPerFile;
    }

    // Write compaction map
    CompactionMaps.write(builder.build(), fileIO.newOutputFile(mapPath));

    // Create position deletes across all source files
    List<PositionDelete<Record>> deletes = new ArrayList<>();
    for (int fileIdx = 0; fileIdx < numSourceFiles; fileIdx++) {
      String sourceFile =
          String.format(Locale.ROOT, "s3://bucket/data/source-%05d.parquet", fileIdx);
      // Delete positions 0, 100, 500 from each file
      for (long pos : new long[] {0, 100, 500}) {
        PositionDelete<Record> delete = PositionDelete.create();
        delete.set(sourceFile, pos, null);
        deletes.add(delete);
      }
    }

    // Write position deletes
    writePositionDeletes(deleteInputPath, deletes);

    // Remap
    CompactionMap map = CompactionMaps.read(fileIO.newInputFile(mapPath));
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    List<PositionDelete<Record>> inputDeletes = readPositionDeletes(deleteInputPath);
    List<PositionDelete<Record>> remappedDeletes = new ArrayList<>();

    for (PositionDelete<Record> delete : inputDeletes) {
      @SuppressWarnings("unchecked")
      PositionDelete<Record> remapped = (PositionDelete<Record>) remapper.remapDeleteOrNull(delete);
      assertThat(remapped).isNotNull();
      remappedDeletes.add(remapped);
    }

    // Write and verify
    writePositionDeletes(deleteOutputPath, remappedDeletes);
    List<PositionDelete<Record>> outputDeletes = readPositionDeletes(deleteOutputPath);

    assertThat(outputDeletes).hasSize(9); // 3 files * 3 positions each

    // All remapped deletes should reference the target file
    for (PositionDelete<Record> delete : outputDeletes) {
      assertThat(delete.path().toString()).isEqualTo(targetFile);
    }

    // Verify expected positions:
    // File 0: 0, 100, 500 → 0, 100, 500
    // File 1: 0, 100, 500 → 1000, 1100, 1500
    // File 2: 0, 100, 500 → 2000, 2100, 2500
    Set<Long> expectedPositions = new HashSet<>();
    expectedPositions.add(0L);
    expectedPositions.add(100L);
    expectedPositions.add(500L);
    expectedPositions.add(1000L);
    expectedPositions.add(1100L);
    expectedPositions.add(1500L);
    expectedPositions.add(2000L);
    expectedPositions.add(2100L);
    expectedPositions.add(2500L);

    Set<Long> actualPositions = new HashSet<>();
    for (PositionDelete<Record> delete : outputDeletes) {
      actualPositions.add(delete.pos());
    }

    assertThat(actualPositions).isEqualTo(expectedPositions);
  }

  /** Split scenario: single source file → multiple target files. */
  private void verifySplitRemapping(String baseDir) throws IOException {
    String mapPath = baseDir + "/compaction-map.avro";
    String deleteInputPath = baseDir + "/position-deletes-input.parquet";
    filesToCleanup.add(mapPath);
    filesToCleanup.add(deleteInputPath);

    // Create compaction map: 1 source file → 3 target files
    // Use per-run target files for multi-target mapping
    String sourceFile = "s3://bucket/data/large-source.parquet";
    long rowsPerTarget = 1000;
    int numTargets = 3;

    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    // Create single file mapping with per-run targets
    CompactionMapBuilder.FileMappingBuilder fileMapping =
        builder.addFileMapping(sourceFile, "s3://bucket/data/target-00000.parquet");

    for (int i = 0; i < numTargets; i++) {
      String targetFile = String.format(Locale.ROOT, "s3://bucket/data/target-%05d.parquet", i);
      long sourceOffset = i * rowsPerTarget;
      fileMapping.addRun(sourceOffset, 0, rowsPerTarget, targetFile);
    }

    // Write compaction map
    CompactionMaps.write(builder.build(), fileIO.newOutputFile(mapPath));

    // Create position deletes spanning all target ranges
    List<PositionDelete<Record>> deletes = new ArrayList<>();
    // Positions in first target (0-999)
    deletes.add(createDelete(sourceFile, 0));
    deletes.add(createDelete(sourceFile, 500));
    deletes.add(createDelete(sourceFile, 999));
    // Positions in second target (1000-1999)
    deletes.add(createDelete(sourceFile, 1000));
    deletes.add(createDelete(sourceFile, 1500));
    deletes.add(createDelete(sourceFile, 1999));
    // Positions in third target (2000-2999)
    deletes.add(createDelete(sourceFile, 2000));
    deletes.add(createDelete(sourceFile, 2500));
    deletes.add(createDelete(sourceFile, 2999));

    writePositionDeletes(deleteInputPath, deletes);

    // Remap
    CompactionMap map = CompactionMaps.read(fileIO.newInputFile(mapPath));
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    List<PositionDelete<Record>> inputDeletes = readPositionDeletes(deleteInputPath);

    // Track remapped deletes by target file
    Map<String, List<Long>> remappedByTarget = new HashMap<>();

    for (PositionDelete<Record> delete : inputDeletes) {
      @SuppressWarnings("unchecked")
      PositionDelete<Record> remapped = (PositionDelete<Record>) remapper.remapDeleteOrNull(delete);
      assertThat(remapped).isNotNull();

      String targetPath = remapped.path().toString();
      remappedByTarget.computeIfAbsent(targetPath, k -> new ArrayList<>()).add(remapped.pos());
    }

    // Verify deletes are distributed across target files
    assertThat(remappedByTarget).hasSize(3);

    // Target 0 should have positions 0, 500, 999
    List<Long> target0Positions = remappedByTarget.get("s3://bucket/data/target-00000.parquet");
    assertThat(target0Positions).containsExactlyInAnyOrder(0L, 500L, 999L);

    // Target 1 should have positions 0, 500, 999 (remapped from 1000, 1500, 1999)
    List<Long> target1Positions = remappedByTarget.get("s3://bucket/data/target-00001.parquet");
    assertThat(target1Positions).containsExactlyInAnyOrder(0L, 500L, 999L);

    // Target 2 should have positions 0, 500, 999 (remapped from 2000, 2500, 2999)
    List<Long> target2Positions = remappedByTarget.get("s3://bucket/data/target-00002.parquet");
    assertThat(target2Positions).containsExactlyInAnyOrder(0L, 500L, 999L);
  }

  /** Large scale test with many deletes. */
  private void verifyLargeScaleRemapping(String baseDir) throws IOException {
    String mapPath = baseDir + "/compaction-map.avro";
    String deleteInputPath = baseDir + "/position-deletes-input.parquet";
    filesToCleanup.add(mapPath);
    filesToCleanup.add(deleteInputPath);

    // Create compaction map: 10 source files → 1 target file
    String targetFile = "s3://bucket/data/compacted.parquet";
    int numSourceFiles = 10;
    long rowsPerFile = 100_000;

    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    long targetOffset = 0;

    for (int i = 0; i < numSourceFiles; i++) {
      String sourceFile = String.format(Locale.ROOT, "s3://bucket/data/source-%05d.parquet", i);
      // Add multiple runs per file to test run lookup performance
      long sourceOffset = 0;
      long remaining = rowsPerFile;
      long runSize = 10_000;

      CompactionMapBuilder.FileMappingBuilder fileMapping =
          builder.addFileMapping(sourceFile, targetFile);

      while (remaining > 0) {
        long thisRun = Math.min(runSize, remaining);
        fileMapping.addRun(sourceOffset, targetOffset, thisRun);
        sourceOffset += thisRun;
        targetOffset += thisRun;
        remaining -= thisRun;
      }
    }

    CompactionMaps.write(builder.build(), fileIO.newOutputFile(mapPath));

    // Create many position deletes (100 per file)
    int deletesPerFile = 100;
    List<PositionDelete<Record>> deletes = new ArrayList<>();

    for (int fileIdx = 0; fileIdx < numSourceFiles; fileIdx++) {
      String sourceFile =
          String.format(Locale.ROOT, "s3://bucket/data/source-%05d.parquet", fileIdx);
      for (int i = 0; i < deletesPerFile; i++) {
        long pos = (long) i * (rowsPerFile / deletesPerFile);
        deletes.add(createDelete(sourceFile, pos));
      }
    }

    writePositionDeletes(deleteInputPath, deletes);

    // Remap all deletes
    CompactionMap map = CompactionMaps.read(fileIO.newInputFile(mapPath));
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    List<PositionDelete<Record>> inputDeletes = readPositionDeletes(deleteInputPath);
    assertThat(inputDeletes).hasSize(numSourceFiles * deletesPerFile);

    int remappedCount = 0;
    for (PositionDelete<Record> delete : inputDeletes) {
      PositionDelete<?> remapped = remapper.remapDeleteOrNull(delete);
      assertThat(remapped).isNotNull();
      assertThat(remapped.path().toString()).isEqualTo(targetFile);
      remappedCount++;
    }

    assertThat(remappedCount).isEqualTo(numSourceFiles * deletesPerFile);
  }

  /** Tests merge compaction scenario where some rows are filtered (positions return null). */
  private void verifyMergeCompactionWithFilteredRows(String baseDir) throws IOException {
    String mapPath = baseDir + "/compaction-map.avro";
    String deleteInputPath = baseDir + "/position-deletes-input.parquet";
    filesToCleanup.add(mapPath);
    filesToCleanup.add(deleteInputPath);

    // Create compaction map with gaps (simulating rows filtered during merge compaction)
    String sourceFile = "s3://bucket/data/source.parquet";
    String targetFile = "s3://bucket/data/target.parquet";

    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    // Rows 0-99 → 0-99
    // Rows 100-199 are filtered (no mapping)
    // Rows 200-299 → 100-199
    builder.addFileMapping(sourceFile, targetFile).addRun(0, 0, 100).addRun(200, 100, 100);

    CompactionMaps.write(builder.build(), fileIO.newOutputFile(mapPath));

    // Create position deletes including some in the filtered range
    List<PositionDelete<Record>> deletes = new ArrayList<>();
    deletes.add(createDelete(sourceFile, 50)); // Should map to 50
    deletes.add(createDelete(sourceFile, 150)); // Should return null (filtered)
    deletes.add(createDelete(sourceFile, 250)); // Should map to 150

    writePositionDeletes(deleteInputPath, deletes);

    // Remap
    CompactionMap map = CompactionMaps.read(fileIO.newInputFile(mapPath));
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    List<PositionDelete<Record>> inputDeletes = readPositionDeletes(deleteInputPath);
    List<PositionDelete<Record>> remappedDeletes = new ArrayList<>();
    int filteredCount = 0;

    for (PositionDelete<Record> delete : inputDeletes) {
      @SuppressWarnings("unchecked")
      PositionDelete<Record> remapped = (PositionDelete<Record>) remapper.remapDeleteOrNull(delete);
      if (remapped == null) {
        filteredCount++;
      } else {
        remappedDeletes.add(remapped);
      }
    }

    // Verify: 1 delete was filtered (position 150)
    assertThat(filteredCount).isEqualTo(1);
    assertThat(remappedDeletes).hasSize(2);

    // Verify remapped positions
    assertThat(remappedDeletes.get(0).path().toString()).isEqualTo(targetFile);
    assertThat(remappedDeletes.get(0).pos()).isEqualTo(50);

    assertThat(remappedDeletes.get(1).path().toString()).isEqualTo(targetFile);
    assertThat(remappedDeletes.get(1).pos()).isEqualTo(150);
  }

  // ==================== Helper Methods ====================

  private PositionDelete<Record> createDelete(String filePath, long position) {
    PositionDelete<Record> delete = PositionDelete.create();
    delete.set(filePath, position, null);
    return delete;
  }

  private void writePositionDeletes(String path, List<PositionDelete<Record>> deletes)
      throws IOException {
    OutputFile output = fileIO.newOutputFile(path);

    try (FileAppender<Record> appender =
        Parquet.write(output)
            .schema(DELETE_SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .build()) {

      for (PositionDelete<Record> delete : deletes) {
        GenericRecord record = GenericRecord.create(DELETE_SCHEMA);
        record.setField("file_path", delete.path().toString());
        record.setField("pos", delete.pos());
        appender.add(record);
      }
    }
  }

  private List<PositionDelete<Record>> readPositionDeletes(String path) throws IOException {
    InputFile input = fileIO.newInputFile(path);
    List<PositionDelete<Record>> deletes = new ArrayList<>();

    try (CloseableIterable<Record> reader =
        Parquet.read(input)
            .project(DELETE_SCHEMA)
            .createReaderFunc(schema -> GenericParquetReaders.buildReader(DELETE_SCHEMA, schema))
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
