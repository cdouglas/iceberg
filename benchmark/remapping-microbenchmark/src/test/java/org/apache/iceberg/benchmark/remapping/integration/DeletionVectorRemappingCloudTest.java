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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.PuffinWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

/**
 * Integration tests for deletion vector remapping with cloud storage.
 *
 * <p>These tests verify that the PositionDeleteRemapper correctly transforms deletion vectors (DVs)
 * when using cloud storage. DVs are stored in Puffin format with Roaring bitmaps.
 *
 * <p>Tests cover:
 *
 * <ul>
 *   <li>Simple DV remapping with position shifts
 *   <li>Fanout scenario: multiple DVs → single target file
 *   <li>Split scenario: single DV → multiple target files
 *   <li>Large DVs with many deleted positions
 *   <li>Sparse vs dense deletion patterns
 *   <li>Merge compaction with filtered rows
 * </ul>
 *
 * <p>Run with: ./gradlew :benchmark:remapping-microbenchmark:integrationTest
 */
@Tag("integration")
public class DeletionVectorRemappingCloudTest {

  private static final String DV_BLOB_TYPE = "deletion-vector-v1";

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
  void testGcsSimpleDvRemapping() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/dv-remapping-test-" + UUID.randomUUID();
    verifySimpleDvRemapping(baseDir);
  }

  @Test
  void testGcsFanoutDvRemapping() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/dv-remapping-test-" + UUID.randomUUID();
    verifyFanoutDvRemapping(baseDir);
  }

  @Test
  void testGcsSplitDvRemapping() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/dv-remapping-test-" + UUID.randomUUID();
    verifySplitDvRemapping(baseDir);
  }

  @Test
  void testGcsLargeDvRemapping() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/dv-remapping-test-" + UUID.randomUUID();
    verifyLargeDvRemapping(baseDir);
  }

  @Test
  void testGcsSparseDvRemapping() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/dv-remapping-test-" + UUID.randomUUID();
    verifySparseDvRemapping(baseDir);
  }

  @Test
  void testGcsDenseDvRemapping() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/dv-remapping-test-" + UUID.randomUUID();
    verifyDenseDvRemapping(baseDir);
  }

  // ==================== S3 Tests ====================

  @Test
  void testS3SimpleDvRemapping() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/dv-remapping-test-" + UUID.randomUUID();
    verifySimpleDvRemapping(baseDir);
  }

  @Test
  void testS3FanoutDvRemapping() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/dv-remapping-test-" + UUID.randomUUID();
    verifyFanoutDvRemapping(baseDir);
  }

  @Test
  void testS3SplitDvRemapping() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/dv-remapping-test-" + UUID.randomUUID();
    verifySplitDvRemapping(baseDir);
  }

  @Test
  void testS3LargeDvRemapping() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/dv-remapping-test-" + UUID.randomUUID();
    verifyLargeDvRemapping(baseDir);
  }

  @Test
  void testS3SparseDvRemapping() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/dv-remapping-test-" + UUID.randomUUID();
    verifySparseDvRemapping(baseDir);
  }

  @Test
  void testS3DenseDvRemapping() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String baseDir = bucket + "/dv-remapping-test-" + UUID.randomUUID();
    verifyDenseDvRemapping(baseDir);
  }

  // ==================== Azure Tests ====================

  @Test
  void testAzureSimpleDvRemapping() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String baseDir = uri + "/dv-remapping-test-" + UUID.randomUUID();
    verifySimpleDvRemapping(baseDir);
  }

  @Test
  void testAzureFanoutDvRemapping() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String baseDir = uri + "/dv-remapping-test-" + UUID.randomUUID();
    verifyFanoutDvRemapping(baseDir);
  }

  @Test
  void testAzureSplitDvRemapping() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String baseDir = uri + "/dv-remapping-test-" + UUID.randomUUID();
    verifySplitDvRemapping(baseDir);
  }

  @Test
  void testAzureLargeDvRemapping() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String baseDir = uri + "/dv-remapping-test-" + UUID.randomUUID();
    verifyLargeDvRemapping(baseDir);
  }

  @Test
  void testAzureSparseDvRemapping() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String baseDir = uri + "/dv-remapping-test-" + UUID.randomUUID();
    verifySparseDvRemapping(baseDir);
  }

  @Test
  void testAzureDenseDvRemapping() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String baseDir = uri + "/dv-remapping-test-" + UUID.randomUUID();
    verifyDenseDvRemapping(baseDir);
  }

  // ==================== Verification Methods ====================

  /** Simple DV remapping where positions shift. */
  private void verifySimpleDvRemapping(String baseDir) throws IOException {
    String mapPath = baseDir + "/compaction-map.avro";
    String dvInputPath = baseDir + "/dv-input.puffin";
    filesToCleanup.add(mapPath);
    filesToCleanup.add(dvInputPath);

    String sourceFile = "s3://bucket/data/source-00000.parquet";
    String targetFile = "s3://bucket/data/target-00000.parquet";

    // Create compaction map: positions shift by 1000
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping(sourceFile, targetFile).addRun(0, 1000, 5000);

    CompactionMaps.write(builder.build(), fileIO.newOutputFile(mapPath));

    // Create DV with some deleted positions
    RoaringBitmap bitmap = new RoaringBitmap();
    int[] testPositions = {0, 100, 500, 1000, 2500, 4999};
    for (int pos : testPositions) {
      bitmap.add(pos);
    }

    writeDeletionVector(dvInputPath, sourceFile, bitmap);

    // Read compaction map and create remapper
    CompactionMap map = CompactionMaps.read(fileIO.newInputFile(mapPath));
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Read DV and remap positions manually (simulating what SparkCompactionConflictResolver does)
    RoaringBitmap inputBitmap = readDeletionVector(dvInputPath);
    assertThat(inputBitmap.getCardinality()).isEqualTo(testPositions.length);

    // Remap each position
    Set<Long> remappedPositions = new HashSet<>();
    for (int pos : inputBitmap) {
      PositionDelete<Record> delete = PositionDelete.create();
      delete.set(sourceFile, pos, null);

      @SuppressWarnings("unchecked")
      PositionDelete<Record> remapped = (PositionDelete<Record>) remapper.remapDeleteOrNull(delete);
      assertThat(remapped).isNotNull();
      assertThat(remapped.path().toString()).isEqualTo(targetFile);
      remappedPositions.add(remapped.pos());
    }

    // Verify all positions are shifted by 1000
    for (int originalPos : testPositions) {
      assertThat(remappedPositions).contains((long) originalPos + 1000);
    }
  }

  /** Fanout: multiple source files' DVs merge into single target file. */
  private void verifyFanoutDvRemapping(String baseDir) throws IOException {
    String mapPath = baseDir + "/compaction-map.avro";
    filesToCleanup.add(mapPath);

    String targetFile = "s3://bucket/data/compacted.parquet";
    int numSourceFiles = 3;
    long rowsPerFile = 1000;

    // Create compaction map
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    long targetOffset = 0;

    for (int i = 0; i < numSourceFiles; i++) {
      String sourceFile = String.format(Locale.ROOT, "s3://bucket/data/source-%05d.parquet", i);
      builder.addFileMapping(sourceFile, targetFile).addRun(0, targetOffset, rowsPerFile);
      targetOffset += rowsPerFile;
    }

    CompactionMaps.write(builder.build(), fileIO.newOutputFile(mapPath));

    // Create DVs for each source file
    List<String> dvPaths = new ArrayList<>();
    for (int i = 0; i < numSourceFiles; i++) {
      String sourceFile = String.format(Locale.ROOT, "s3://bucket/data/source-%05d.parquet", i);
      String dvPath = baseDir + String.format(Locale.ROOT, "/dv-%05d.puffin", i);
      dvPaths.add(dvPath);
      filesToCleanup.add(dvPath);

      RoaringBitmap bitmap = new RoaringBitmap();
      // Delete positions 0, 100, 500 from each file
      bitmap.add(0, 100, 500);
      writeDeletionVector(dvPath, sourceFile, bitmap);
    }

    // Remap all DVs
    CompactionMap map = CompactionMaps.read(fileIO.newInputFile(mapPath));
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    Set<Long> allRemappedPositions = new HashSet<>();

    for (int i = 0; i < numSourceFiles; i++) {
      String sourceFile = String.format(Locale.ROOT, "s3://bucket/data/source-%05d.parquet", i);
      RoaringBitmap bitmap = readDeletionVector(dvPaths.get(i));

      for (int pos : bitmap) {
        PositionDelete<Record> delete = PositionDelete.create();
        delete.set(sourceFile, pos, null);

        @SuppressWarnings("unchecked")
        PositionDelete<Record> remapped =
            (PositionDelete<Record>) remapper.remapDeleteOrNull(delete);
        assertThat(remapped).isNotNull();
        assertThat(remapped.path().toString()).isEqualTo(targetFile);
        allRemappedPositions.add(remapped.pos());
      }
    }

    // Verify positions:
    // File 0: 0, 100, 500 → 0, 100, 500
    // File 1: 0, 100, 500 → 1000, 1100, 1500
    // File 2: 0, 100, 500 → 2000, 2100, 2500
    assertThat(allRemappedPositions)
        .containsExactlyInAnyOrder(0L, 100L, 500L, 1000L, 1100L, 1500L, 2000L, 2100L, 2500L);
  }

  /** Split: single source file's DV maps to multiple target files. */
  private void verifySplitDvRemapping(String baseDir) throws IOException {
    String mapPath = baseDir + "/compaction-map.avro";
    String dvInputPath = baseDir + "/dv-input.puffin";
    filesToCleanup.add(mapPath);
    filesToCleanup.add(dvInputPath);

    String sourceFile = "s3://bucket/data/large-source.parquet";
    long rowsPerTarget = 1000;
    int numTargets = 3;

    // Create compaction map: 1 source → 3 targets using per-run target files
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    CompactionMapBuilder.FileMappingBuilder fileMapping =
        builder.addFileMapping(sourceFile, "s3://bucket/data/target-00000.parquet");

    for (int i = 0; i < numTargets; i++) {
      String targetFile = String.format(Locale.ROOT, "s3://bucket/data/target-%05d.parquet", i);
      long sourceOffset = i * rowsPerTarget;
      fileMapping.addRun(sourceOffset, 0, rowsPerTarget, targetFile);
    }

    CompactionMaps.write(builder.build(), fileIO.newOutputFile(mapPath));

    // Create DV with positions spanning all target ranges
    RoaringBitmap bitmap = new RoaringBitmap();
    // Target 0 range (0-999)
    bitmap.add(0);
    bitmap.add(500);
    bitmap.add(999);
    // Target 1 range (1000-1999)
    bitmap.add(1000);
    bitmap.add(1500);
    bitmap.add(1999);
    // Target 2 range (2000-2999)
    bitmap.add(2000);
    bitmap.add(2500);
    bitmap.add(2999);

    writeDeletionVector(dvInputPath, sourceFile, bitmap);

    // Remap
    CompactionMap map = CompactionMaps.read(fileIO.newInputFile(mapPath));
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    RoaringBitmap inputBitmap = readDeletionVector(dvInputPath);

    // Group remapped positions by target file
    Map<String, Set<Long>> remappedByTarget = new HashMap<>();

    for (int pos : inputBitmap) {
      PositionDelete<Record> delete = PositionDelete.create();
      delete.set(sourceFile, pos, null);

      @SuppressWarnings("unchecked")
      PositionDelete<Record> remapped = (PositionDelete<Record>) remapper.remapDeleteOrNull(delete);
      assertThat(remapped).isNotNull();

      String targetPath = remapped.path().toString();
      remappedByTarget.computeIfAbsent(targetPath, k -> new HashSet<>()).add(remapped.pos());
    }

    // Verify distribution
    assertThat(remappedByTarget).hasSize(3);

    assertThat(remappedByTarget.get("s3://bucket/data/target-00000.parquet"))
        .containsExactlyInAnyOrder(0L, 500L, 999L);

    assertThat(remappedByTarget.get("s3://bucket/data/target-00001.parquet"))
        .containsExactlyInAnyOrder(0L, 500L, 999L);

    assertThat(remappedByTarget.get("s3://bucket/data/target-00002.parquet"))
        .containsExactlyInAnyOrder(0L, 500L, 999L);
  }

  /** Large DV with many positions (tests performance and correctness at scale). */
  private void verifyLargeDvRemapping(String baseDir) throws IOException {
    String mapPath = baseDir + "/compaction-map.avro";
    String dvInputPath = baseDir + "/dv-input.puffin";
    filesToCleanup.add(mapPath);
    filesToCleanup.add(dvInputPath);

    String sourceFile = "s3://bucket/data/source.parquet";
    String targetFile = "s3://bucket/data/target.parquet";

    // Create compaction map
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping(sourceFile, targetFile).addRun(0, 0, 1_000_000);

    CompactionMaps.write(builder.build(), fileIO.newOutputFile(mapPath));

    // Create large DV with 10,000 deleted positions
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < 10_000; i++) {
      bitmap.add(i * 100); // Every 100th row
    }

    writeDeletionVector(dvInputPath, sourceFile, bitmap);

    // Remap
    CompactionMap map = CompactionMaps.read(fileIO.newInputFile(mapPath));
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    RoaringBitmap inputBitmap = readDeletionVector(dvInputPath);
    assertThat(inputBitmap.getCardinality()).isEqualTo(10_000);

    int remappedCount = 0;
    for (int pos : inputBitmap) {
      PositionDelete<Record> delete = PositionDelete.create();
      delete.set(sourceFile, pos, null);

      PositionDelete<?> remapped = remapper.remapDeleteOrNull(delete);
      assertThat(remapped).isNotNull();
      assertThat(remapped.path().toString()).isEqualTo(targetFile);
      // With 1:1 mapping at offset 0, positions should be unchanged
      assertThat(remapped.pos()).isEqualTo(pos);
      remappedCount++;
    }

    assertThat(remappedCount).isEqualTo(10_000);
  }

  /** Sparse DV pattern (scattered deletions). */
  private void verifySparseDvRemapping(String baseDir) throws IOException {
    String mapPath = baseDir + "/compaction-map.avro";
    String dvInputPath = baseDir + "/dv-input.puffin";
    filesToCleanup.add(mapPath);
    filesToCleanup.add(dvInputPath);

    String sourceFile = "s3://bucket/data/source.parquet";
    String targetFile = "s3://bucket/data/target.parquet";

    // Create compaction map with offset
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping(sourceFile, targetFile).addRun(0, 500, 100_000); // Offset by 500

    CompactionMaps.write(builder.build(), fileIO.newOutputFile(mapPath));

    // Create sparse DV (random positions spread across the range)
    RoaringBitmap bitmap = new RoaringBitmap();
    java.util.Random rand = new java.util.Random(42);
    Set<Integer> positions = new HashSet<>();
    while (positions.size() < 100) {
      positions.add(rand.nextInt(100_000));
    }
    for (int pos : positions) {
      bitmap.add(pos);
    }

    writeDeletionVector(dvInputPath, sourceFile, bitmap);

    // Remap
    CompactionMap map = CompactionMaps.read(fileIO.newInputFile(mapPath));
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    RoaringBitmap inputBitmap = readDeletionVector(dvInputPath);
    assertThat(inputBitmap.getCardinality()).isEqualTo(100);

    for (int pos : inputBitmap) {
      PositionDelete<Record> delete = PositionDelete.create();
      delete.set(sourceFile, pos, null);

      @SuppressWarnings("unchecked")
      PositionDelete<Record> remapped = (PositionDelete<Record>) remapper.remapDeleteOrNull(delete);
      assertThat(remapped).isNotNull();
      assertThat(remapped.path().toString()).isEqualTo(targetFile);
      // Position should be shifted by 500
      assertThat(remapped.pos()).isEqualTo(pos + 500);
    }
  }

  /** Dense DV pattern (contiguous ranges of deletions). */
  private void verifyDenseDvRemapping(String baseDir) throws IOException {
    String mapPath = baseDir + "/compaction-map.avro";
    String dvInputPath = baseDir + "/dv-input.puffin";
    filesToCleanup.add(mapPath);
    filesToCleanup.add(dvInputPath);

    String sourceFile = "s3://bucket/data/source.parquet";
    String targetFile = "s3://bucket/data/target.parquet";

    // Create compaction map
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping(sourceFile, targetFile).addRun(0, 0, 100_000);

    CompactionMaps.write(builder.build(), fileIO.newOutputFile(mapPath));

    // Create dense DV (contiguous ranges)
    RoaringBitmap bitmap = new RoaringBitmap();
    // Add 10 contiguous ranges of 100 rows each
    for (int cluster = 0; cluster < 10; cluster++) {
      int start = cluster * 10_000;
      bitmap.add((long) start, (long) start + 100);
    }
    bitmap.runOptimize();

    writeDeletionVector(dvInputPath, sourceFile, bitmap);

    // Remap
    CompactionMap map = CompactionMaps.read(fileIO.newInputFile(mapPath));
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    RoaringBitmap inputBitmap = readDeletionVector(dvInputPath);
    assertThat(inputBitmap.getCardinality()).isEqualTo(1000); // 10 clusters * 100 rows

    Set<Long> remappedPositions = new HashSet<>();
    for (int pos : inputBitmap) {
      PositionDelete<Record> delete = PositionDelete.create();
      delete.set(sourceFile, pos, null);

      @SuppressWarnings("unchecked")
      PositionDelete<Record> remapped = (PositionDelete<Record>) remapper.remapDeleteOrNull(delete);
      assertThat(remapped).isNotNull();
      remappedPositions.add(remapped.pos());
    }

    // Verify all positions are remapped (1:1 mapping at offset 0)
    assertThat(remappedPositions).hasSize(1000);

    // Verify contiguous ranges are preserved
    for (int cluster = 0; cluster < 10; cluster++) {
      int start = cluster * 10_000;
      for (int offset = 0; offset < 100; offset++) {
        assertThat(remappedPositions).contains((long) start + offset);
      }
    }
  }

  // ==================== Helper Methods ====================

  private void writeDeletionVector(String path, String referencedDataFile, RoaringBitmap bitmap)
      throws IOException {
    OutputFile output = fileIO.newOutputFile(path);

    ByteBuffer buffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes());
    bitmap.serialize(buffer);
    buffer.flip();

    try (PuffinWriter writer = Puffin.write(output).build()) {
      Blob blob =
          new Blob(
              DV_BLOB_TYPE,
              Collections.singletonList(1),
              0,
              0,
              buffer,
              null,
              Collections.singletonMap("referenced-data-file", referencedDataFile));
      writer.add(blob);
      writer.finish();
    }
  }

  private RoaringBitmap readDeletionVector(String path) throws IOException {
    InputFile input = fileIO.newInputFile(path);

    try (PuffinReader reader = Puffin.read(input).build()) {
      List<BlobMetadata> blobMetadata = reader.fileMetadata().blobs();
      assertThat(blobMetadata).isNotEmpty();

      BlobMetadata metadata = blobMetadata.get(0);
      assertThat(metadata.type()).isEqualTo(DV_BLOB_TYPE);

      // Read blob data
      for (org.apache.iceberg.util.Pair<BlobMetadata, ByteBuffer> pair :
          reader.readAll(Collections.singletonList(metadata))) {
        ByteBuffer buffer = pair.second();
        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.deserialize(buffer);
        return bitmap;
      }
    }

    throw new IOException("Failed to read deletion vector from " + path);
  }
}
