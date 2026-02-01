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
import org.apache.iceberg.CompactionMapBuilder;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for compaction map serialization with cloud storage.
 *
 * <p>These tests verify that compaction maps can be correctly written to and read from GCS, S3, and
 * Azure storage. This validates the core Avro serialization and deserialization works correctly
 * with each cloud storage implementation.
 *
 * <p>Tests cover:
 *
 * <ul>
 *   <li>Simple compaction maps with single file mapping
 *   <li>Complex compaction maps with multiple file mappings
 *   <li>Large compaction maps with many runs
 *   <li>Run merging behavior
 *   <li>Multi-target mappings (source file split across multiple targets)
 * </ul>
 *
 * <p>Run with: ./gradlew :benchmark:remapping-microbenchmark:integrationTest
 */
@Tag("integration")
public class CompactionMapCloudStorageTest {

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
  void testGcsSimpleCompactionMapRoundTrip() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String path = bucket + "/compaction-map-test-" + UUID.randomUUID() + "/simple.avro";
    filesToCleanup.add(path);

    verifySimpleCompactionMapRoundTrip(path);
  }

  @Test
  void testGcsComplexCompactionMapRoundTrip() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String path = bucket + "/compaction-map-test-" + UUID.randomUUID() + "/complex.avro";
    filesToCleanup.add(path);

    verifyComplexCompactionMapRoundTrip(path);
  }

  @Test
  void testGcsLargeCompactionMapRoundTrip() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String path = bucket + "/compaction-map-test-" + UUID.randomUUID() + "/large.avro";
    filesToCleanup.add(path);

    verifyLargeCompactionMapRoundTrip(path);
  }

  @Test
  void testGcsRunMergingBehavior() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String path = bucket + "/compaction-map-test-" + UUID.randomUUID() + "/merged.avro";
    filesToCleanup.add(path);

    verifyRunMergingBehavior(path);
  }

  @Test
  void testGcsMultiTargetMapping() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set").isNotNull();

    String path = bucket + "/compaction-map-test-" + UUID.randomUUID() + "/multi-target.avro";
    filesToCleanup.add(path);

    verifyMultiTargetMapping(path);
  }

  // ==================== S3 Tests ====================

  @Test
  void testS3SimpleCompactionMapRoundTrip() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String path = bucket + "/compaction-map-test-" + UUID.randomUUID() + "/simple.avro";
    filesToCleanup.add(path);

    verifySimpleCompactionMapRoundTrip(path);
  }

  @Test
  void testS3ComplexCompactionMapRoundTrip() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String path = bucket + "/compaction-map-test-" + UUID.randomUUID() + "/complex.avro";
    filesToCleanup.add(path);

    verifyComplexCompactionMapRoundTrip(path);
  }

  @Test
  void testS3LargeCompactionMapRoundTrip() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String path = bucket + "/compaction-map-test-" + UUID.randomUUID() + "/large.avro";
    filesToCleanup.add(path);

    verifyLargeCompactionMapRoundTrip(path);
  }

  @Test
  void testS3RunMergingBehavior() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String path = bucket + "/compaction-map-test-" + UUID.randomUUID() + "/merged.avro";
    filesToCleanup.add(path);

    verifyRunMergingBehavior(path);
  }

  @Test
  void testS3MultiTargetMapping() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set").isNotNull();

    String path = bucket + "/compaction-map-test-" + UUID.randomUUID() + "/multi-target.avro";
    filesToCleanup.add(path);

    verifyMultiTargetMapping(path);
  }

  // ==================== Azure Tests ====================

  @Test
  void testAzureSimpleCompactionMapRoundTrip() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String path = uri + "/compaction-map-test-" + UUID.randomUUID() + "/simple.avro";
    filesToCleanup.add(path);

    verifySimpleCompactionMapRoundTrip(path);
  }

  @Test
  void testAzureComplexCompactionMapRoundTrip() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String path = uri + "/compaction-map-test-" + UUID.randomUUID() + "/complex.avro";
    filesToCleanup.add(path);

    verifyComplexCompactionMapRoundTrip(path);
  }

  @Test
  void testAzureLargeCompactionMapRoundTrip() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String path = uri + "/compaction-map-test-" + UUID.randomUUID() + "/large.avro";
    filesToCleanup.add(path);

    verifyLargeCompactionMapRoundTrip(path);
  }

  @Test
  void testAzureRunMergingBehavior() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String path = uri + "/compaction-map-test-" + UUID.randomUUID() + "/merged.avro";
    filesToCleanup.add(path);

    verifyRunMergingBehavior(path);
  }

  @Test
  void testAzureMultiTargetMapping() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set").isNotNull();

    String path = uri + "/compaction-map-test-" + UUID.randomUUID() + "/multi-target.avro";
    filesToCleanup.add(path);

    verifyMultiTargetMapping(path);
  }

  // ==================== Verification Methods ====================

  /** Verifies a simple compaction map with single source → single target mapping. */
  private void verifySimpleCompactionMapRoundTrip(String path) throws IOException {
    // Build a simple compaction map: one source file → one target file
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder
        .addFileMapping(
            "s3://bucket/data/source-00000.parquet", "s3://bucket/data/target-00000.parquet")
        .addRun(0, 0, 1000);

    CompactionMap original = builder.build();

    // Write to cloud storage
    OutputFile outputFile = fileIO.newOutputFile(path);
    CompactionMaps.write(original, outputFile);

    // Read back
    InputFile inputFile = fileIO.newInputFile(path);
    CompactionMap read = CompactionMaps.read(inputFile);

    // Verify
    assertThat(read.sourceSnapshotId()).isEqualTo(1L);
    assertThat(read.targetSnapshotId()).isEqualTo(2L);
    assertThat(read.fileMappings()).hasSize(1);

    CompactionMap.FileMapping mapping = read.fileMappings().get(0);
    assertThat(mapping.sourceFile()).isEqualTo("s3://bucket/data/source-00000.parquet");
    assertThat(mapping.targetFile()).isEqualTo("s3://bucket/data/target-00000.parquet");
    assertThat(mapping.runs()).hasSize(1);

    CompactionMap.Run run = mapping.runs().get(0);
    assertThat(run.sourcePosition()).isEqualTo(0);
    assertThat(run.targetPosition()).isEqualTo(0);
    assertThat(run.length()).isEqualTo(1000);
  }

  /** Verifies a complex compaction map with multiple file mappings and runs. */
  private void verifyComplexCompactionMapRoundTrip(String path) throws IOException {
    // Build: 3 source files → 1 target file (bin-pack scenario)
    // Uses non-consecutive source positions to prevent run merging
    CompactionMapBuilder builder = new CompactionMapBuilder(10L, 20L);

    long targetOffset = 0;
    for (int i = 0; i < 3; i++) {
      String sourceFile = String.format("s3://bucket/data/source-%05d.parquet", i);
      // Run 1: source 0-1000 → target targetOffset-targetOffset+1000
      // Run 2: source 2000-2500 → target targetOffset+1000-targetOffset+1500 (gap at source 1000)
      builder
          .addFileMapping(sourceFile, "s3://bucket/data/compacted.parquet")
          .addRun(0, targetOffset, 1000)
          .addRun(2000, targetOffset + 1000, 500); // Gap at source 1000-2000 prevents merging
      targetOffset += 1500;
    }

    CompactionMap original = builder.build();

    // Write and read
    OutputFile outputFile = fileIO.newOutputFile(path);
    CompactionMaps.write(original, outputFile);

    InputFile inputFile = fileIO.newInputFile(path);
    CompactionMap read = CompactionMaps.read(inputFile);

    // Verify
    assertThat(read.sourceSnapshotId()).isEqualTo(10L);
    assertThat(read.targetSnapshotId()).isEqualTo(20L);
    assertThat(read.fileMappings()).hasSize(3);

    long verifyOffset = 0;
    for (int i = 0; i < 3; i++) {
      CompactionMap.FileMapping mapping = read.fileMappings().get(i);
      assertThat(mapping.sourceFile())
          .isEqualTo(String.format("s3://bucket/data/source-%05d.parquet", i));
      assertThat(mapping.targetFile()).isEqualTo("s3://bucket/data/compacted.parquet");
      assertThat(mapping.runs()).hasSize(2);

      // Verify first run
      CompactionMap.Run run1 = mapping.runs().get(0);
      assertThat(run1.sourcePosition()).isEqualTo(0);
      assertThat(run1.targetPosition()).isEqualTo(verifyOffset);
      assertThat(run1.length()).isEqualTo(1000);

      // Verify second run (source starts at 2000 due to gap)
      CompactionMap.Run run2 = mapping.runs().get(1);
      assertThat(run2.sourcePosition()).isEqualTo(2000);
      assertThat(run2.targetPosition()).isEqualTo(verifyOffset + 1000);
      assertThat(run2.length()).isEqualTo(500);

      verifyOffset += 1500;
    }
  }

  /** Verifies a large compaction map with many files and runs. */
  private void verifyLargeCompactionMapRoundTrip(String path) throws IOException {
    // Build: 100 source files, each with 10 runs (non-consecutive to prevent merging)
    int numSourceFiles = 100;
    int runsPerFile = 10;
    long rowsPerRun = 10_000;
    long gapSize = 100; // Gap between runs to prevent merging

    CompactionMapBuilder builder = new CompactionMapBuilder(100L, 200L);

    long targetOffset = 0;
    for (int i = 0; i < numSourceFiles; i++) {
      String sourceFile = String.format("s3://bucket/data/source-%05d.parquet", i);
      CompactionMapBuilder.FileMappingBuilder fileMapping =
          builder.addFileMapping(sourceFile, "s3://bucket/data/compacted.parquet");

      long sourceOffset = 0;
      for (int r = 0; r < runsPerFile; r++) {
        fileMapping.addRun(sourceOffset, targetOffset, rowsPerRun);
        // Add a gap in source positions to prevent run merging
        sourceOffset += rowsPerRun + gapSize;
        targetOffset += rowsPerRun;
      }
    }

    CompactionMap original = builder.build();

    // Write and read
    OutputFile outputFile = fileIO.newOutputFile(path);
    CompactionMaps.write(original, outputFile);

    InputFile inputFile = fileIO.newInputFile(path);
    assertThat(inputFile.exists()).as("File should exist after write").isTrue();

    CompactionMap read = CompactionMaps.read(inputFile);

    // Verify structure
    assertThat(read.sourceSnapshotId()).isEqualTo(100L);
    assertThat(read.targetSnapshotId()).isEqualTo(200L);
    assertThat(read.fileMappings()).hasSize(numSourceFiles);

    // Verify a sample of mappings
    CompactionMap.FileMapping first = read.fileMappings().get(0);
    assertThat(first.sourceFile()).isEqualTo("s3://bucket/data/source-00000.parquet");
    assertThat(first.runs()).hasSize(runsPerFile);

    CompactionMap.FileMapping last = read.fileMappings().get(numSourceFiles - 1);
    assertThat(last.sourceFile())
        .isEqualTo(String.format("s3://bucket/data/source-%05d.parquet", numSourceFiles - 1));
    assertThat(last.runs()).hasSize(runsPerFile);

    // Verify position mapping works correctly
    // First file, first position
    CompactionMap.Run runForPos0 = first.runForPosition(0);
    assertThat(runForPos0).isNotNull();
    assertThat(runForPos0.mapPosition(0)).isEqualTo(0);

    // Last file, last position
    long lastFileFirstPos = 0;
    CompactionMap.Run runForLastFilePos = last.runForPosition(lastFileFirstPos);
    assertThat(runForLastFilePos).isNotNull();
  }

  /** Verifies that consecutive runs are properly merged. */
  private void verifyRunMergingBehavior(String path) throws IOException {
    // Add consecutive runs that should be merged
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    CompactionMapBuilder.FileMappingBuilder fileMapping =
        builder.addFileMapping(
            "s3://bucket/data/source.parquet", "s3://bucket/data/target.parquet");

    // These consecutive runs should be merged into one
    fileMapping.addRun(0, 0, 100);
    fileMapping.addRun(100, 100, 100);
    fileMapping.addRun(200, 200, 100);

    // This run has a gap and should NOT be merged
    fileMapping.addRun(400, 400, 100);

    CompactionMap original = builder.build();

    // Write and read
    OutputFile outputFile = fileIO.newOutputFile(path);
    CompactionMaps.write(original, outputFile);

    InputFile inputFile = fileIO.newInputFile(path);
    CompactionMap read = CompactionMaps.read(inputFile);

    // Verify merging
    assertThat(read.fileMappings()).hasSize(1);
    CompactionMap.FileMapping mapping = read.fileMappings().get(0);

    // Should have 2 runs: merged (0-299) and separate (400-499)
    assertThat(mapping.runs()).hasSize(2);

    CompactionMap.Run mergedRun = mapping.runs().get(0);
    assertThat(mergedRun.sourcePosition()).isEqualTo(0);
    assertThat(mergedRun.targetPosition()).isEqualTo(0);
    assertThat(mergedRun.length()).isEqualTo(300); // 100 + 100 + 100 merged

    CompactionMap.Run separateRun = mapping.runs().get(1);
    assertThat(separateRun.sourcePosition()).isEqualTo(400);
    assertThat(separateRun.targetPosition()).isEqualTo(400);
    assertThat(separateRun.length()).isEqualTo(100);

    // Verify position lookups work across merged runs
    assertThat(mapping.runForPosition(0)).isEqualTo(mergedRun);
    assertThat(mapping.runForPosition(150)).isEqualTo(mergedRun);
    assertThat(mapping.runForPosition(299)).isEqualTo(mergedRun);
    assertThat(mapping.runForPosition(300)).isNull(); // In the gap
    assertThat(mapping.runForPosition(400)).isEqualTo(separateRun);
    assertThat(mapping.runForPosition(499)).isEqualTo(separateRun);
    assertThat(mapping.runForPosition(500)).isNull(); // Past the end
  }

  /**
   * Verifies multi-target mapping where source file maps to multiple target files. This happens
   * when a large source file is split across multiple smaller targets. Uses per-run target files.
   */
  private void verifyMultiTargetMapping(String path) throws IOException {
    // Build: 1 source file → 3 target files (split scenario) using per-run targets
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    String sourceFile = "s3://bucket/data/large-source.parquet";

    // Single file mapping with per-run target files
    CompactionMapBuilder.FileMappingBuilder fileMapping =
        builder.addFileMapping(sourceFile, "s3://bucket/data/target-00000.parquet");

    // First third → target-0
    fileMapping.addRun(0, 0, 1000, "s3://bucket/data/target-00000.parquet");
    // Second third → target-1
    fileMapping.addRun(1000, 0, 1000, "s3://bucket/data/target-00001.parquet");
    // Third third → target-2
    fileMapping.addRun(2000, 0, 1000, "s3://bucket/data/target-00002.parquet");

    CompactionMap original = builder.build();

    // Write and read
    OutputFile outputFile = fileIO.newOutputFile(path);
    CompactionMaps.write(original, outputFile);

    InputFile inputFile = fileIO.newInputFile(path);
    CompactionMap read = CompactionMaps.read(inputFile);

    // Verify structure - single mapping with 3 runs
    assertThat(read.fileMappings()).hasSize(1);

    CompactionMap.FileMapping mapping = read.fileMappings().get(0);
    assertThat(mapping.sourceFile()).isEqualTo(sourceFile);
    assertThat(mapping.runs()).hasSize(3);

    // Verify each run has the correct per-run target file
    CompactionMap.Run run0 = mapping.runs().get(0);
    assertThat(run0.sourcePosition()).isEqualTo(0);
    assertThat(run0.targetPosition()).isEqualTo(0);
    assertThat(run0.targetFile()).isEqualTo("s3://bucket/data/target-00000.parquet");
    assertThat(mapping.runForPosition(500)).isEqualTo(run0);
    assertThat(run0.mapPosition(500)).isEqualTo(500);

    CompactionMap.Run run1 = mapping.runs().get(1);
    assertThat(run1.sourcePosition()).isEqualTo(1000);
    assertThat(run1.targetPosition()).isEqualTo(0);
    assertThat(run1.targetFile()).isEqualTo("s3://bucket/data/target-00001.parquet");
    assertThat(mapping.runForPosition(1500)).isEqualTo(run1);
    assertThat(run1.mapPosition(1500)).isEqualTo(500);

    CompactionMap.Run run2 = mapping.runs().get(2);
    assertThat(run2.sourcePosition()).isEqualTo(2000);
    assertThat(run2.targetPosition()).isEqualTo(0);
    assertThat(run2.targetFile()).isEqualTo("s3://bucket/data/target-00002.parquet");
    assertThat(mapping.runForPosition(2500)).isEqualTo(run2);
    assertThat(run2.mapPosition(2500)).isEqualTo(500);
  }
}
