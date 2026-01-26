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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.CompactionMap.FileMapping;
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.GenericCompactionMap.GenericFileMapping;
import org.apache.iceberg.GenericCompactionMap.GenericRun;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Test;

/**
 * One-off validation test to confirm that the additional per-run target_file field does not cause
 * significant overhead in practice.
 *
 * <p>Key findings from commit 3a2a7fed2e92f4865bc398284034f31b1fbe95e9:
 *
 * <ul>
 *   <li>Avro + compression handles repeated strings efficiently
 *   <li>Application-level interning deduplicates in-memory String instances
 *   <li>Per-run targets add minimal overhead due to run-length encoding
 * </ul>
 */
public class TestMultiTargetSerializationOverhead {

  private static final long SOURCE_SNAPSHOT_ID = 1000L;
  private static final long TARGET_SNAPSHOT_ID = 2000L;

  /**
   * Validates that repeated per-run target files do not significantly increase serialized size.
   *
   * <p>Creates two compaction maps:
   *
   * <ol>
   *   <li>Without per-run targets (null targetFile on runs)
   *   <li>With per-run targets (same target repeated on every run)
   * </ol>
   *
   * <p>The overhead should be minimal because:
   *
   * <ul>
   *   <li>Avro uses string references for repeated values
   *   <li>Compression (gzip/zstd) handles repeated patterns well
   *   <li>Run-length encoding means far fewer runs than individual positions
   * </ul>
   */
  @Test
  public void testRepeatedTargetFileOverhead() throws IOException {
    // Create a realistic scenario: 100 source files, each with 10 runs, all mapping to same target
    int numSourceFiles = 100;
    int runsPerFile = 10;
    String sharedTarget = "s3://warehouse/db/table/data/compacted-output-00000.parquet";

    // Version 1: Without per-run targets (null)
    List<FileMapping> mappingsWithoutRunTargets = new ArrayList<>();
    for (int f = 0; f < numSourceFiles; f++) {
      List<Run> runs = new ArrayList<>();
      for (int r = 0; r < runsPerFile; r++) {
        runs.add(new GenericRun(r * 100L, r * 100L, 100L)); // null targetFile
      }
      mappingsWithoutRunTargets.add(
          new GenericFileMapping(
              "s3://warehouse/db/table/data/file-" + f + ".parquet", sharedTarget, runs));
    }

    CompactionMap mapWithoutRunTargets =
        new GenericCompactionMap(SOURCE_SNAPSHOT_ID, TARGET_SNAPSHOT_ID, mappingsWithoutRunTargets);

    // Version 2: With per-run targets (same target repeated)
    List<FileMapping> mappingsWithRunTargets = new ArrayList<>();
    for (int f = 0; f < numSourceFiles; f++) {
      List<Run> runs = new ArrayList<>();
      for (int r = 0; r < runsPerFile; r++) {
        runs.add(new GenericRun(r * 100L, r * 100L, 100L, sharedTarget)); // explicit target
      }
      mappingsWithRunTargets.add(
          new GenericFileMapping(
              "s3://warehouse/db/table/data/file-" + f + ".parquet", sharedTarget, runs));
    }

    CompactionMap mapWithRunTargets =
        new GenericCompactionMap(SOURCE_SNAPSHOT_ID, TARGET_SNAPSHOT_ID, mappingsWithRunTargets);

    // Measure serialized sizes
    long sizeWithoutRunTargets = measureSerializedSize(mapWithoutRunTargets);
    long sizeWithRunTargets = measureSerializedSize(mapWithRunTargets);

    // Calculate overhead
    double overheadRatio = (double) sizeWithRunTargets / sizeWithoutRunTargets;
    long overheadBytes = sizeWithRunTargets - sizeWithoutRunTargets;

    System.out.println("=== Multi-Target Serialization Overhead Test ===");
    System.out.println(
        "Configuration: " + numSourceFiles + " source files, " + runsPerFile + " runs each");
    System.out.println("Total runs: " + (numSourceFiles * runsPerFile));
    System.out.println();
    System.out.println(
        "Serialized size without per-run targets: " + sizeWithoutRunTargets + " bytes");
    System.out.println("Serialized size with per-run targets: " + sizeWithRunTargets + " bytes");
    System.out.println(
        "Overhead: "
            + overheadBytes
            + " bytes ("
            + String.format("%.2f", (overheadRatio - 1) * 100)
            + "%)");
    System.out.println();

    // Assert overhead is reasonable (less than 50% increase)
    // In practice, Avro string deduplication and compression should keep this much lower
    assertThat(overheadRatio)
        .as("Per-run target field overhead should be less than 50%")
        .isLessThan(1.5);

    // For typical workloads, we expect much lower overhead (< 20%)
    // because repeated strings are deduplicated in Avro
    System.out.println(
        overheadRatio < 1.2
            ? "PASS: Overhead is minimal (< 20%) - Avro string deduplication working well"
            : "WARN: Overhead is "
                + String.format("%.1f", (overheadRatio - 1) * 100)
                + "% - still acceptable but higher than expected");
  }

  /**
   * Validates that multi-target mappings (different targets per run) work correctly and have
   * reasonable size.
   */
  @Test
  public void testMultiTargetMappingSerialization() throws IOException {
    // Create a scenario where source files span multiple targets
    int numSourceFiles = 10;
    int targetsPerSource = 3;
    int runsPerTarget = 5;

    List<FileMapping> mappings = new ArrayList<>();
    for (int f = 0; f < numSourceFiles; f++) {
      List<Run> runs = new ArrayList<>();
      long sourcePos = 0;
      for (int t = 0; t < targetsPerSource; t++) {
        String targetFile = "s3://warehouse/db/table/data/compacted-" + t + ".parquet";
        for (int r = 0; r < runsPerTarget; r++) {
          runs.add(new GenericRun(sourcePos, r * 100L, 100L, targetFile));
          sourcePos += 100;
        }
      }
      mappings.add(
          new GenericFileMapping(
              "s3://warehouse/db/table/data/large-file-" + f + ".parquet",
              "s3://warehouse/db/table/data/compacted-0.parquet", // default target
              runs));
    }

    CompactionMap map = new GenericCompactionMap(SOURCE_SNAPSHOT_ID, TARGET_SNAPSHOT_ID, mappings);

    // Write and read back to verify round-trip
    CompactionMap readMap = writeAndRead(map);

    // Verify all targets are preserved
    assertThat(readMap.fileMappings()).hasSize(numSourceFiles);

    for (FileMapping mapping : readMap.fileMappings()) {
      assertThat(mapping.runs()).hasSize(targetsPerSource * runsPerTarget);

      // Verify each run has correct target
      for (int i = 0; i < mapping.runs().size(); i++) {
        Run run = mapping.runs().get(i);
        int targetIndex = i / runsPerTarget;
        String expectedTarget =
            "s3://warehouse/db/table/data/compacted-" + targetIndex + ".parquet";
        assertThat(run.targetFile()).isEqualTo(expectedTarget);
      }
    }

    long serializedSize = measureSerializedSize(map);
    System.out.println("=== Multi-Target Mapping Test ===");
    System.out.println(
        "Configuration: "
            + numSourceFiles
            + " source files, "
            + targetsPerSource
            + " targets each");
    System.out.println("Total runs: " + (numSourceFiles * targetsPerSource * runsPerTarget));
    System.out.println("Serialized size: " + serializedSize + " bytes");
    System.out.println(
        "Bytes per run: "
            + String.format(
                "%.1f",
                (double) serializedSize / (numSourceFiles * targetsPerSource * runsPerTarget)));
  }

  /** Validates that target file interning reduces memory usage after read. */
  @Test
  public void testTargetFileInterningAfterRead() throws IOException {
    String sharedTarget = "s3://warehouse/db/table/data/compacted.parquet";
    int numRuns = 100;

    // Create runs with the same target string (different instances)
    List<Run> runs = new ArrayList<>();
    for (int i = 0; i < numRuns; i++) {
      // Create a new String instance each time to simulate what Avro deserialization does
      runs.add(new GenericRun(i * 100L, i * 100L, 100L, new String(sharedTarget)));
    }

    FileMapping mapping =
        new GenericFileMapping("s3://warehouse/source.parquet", new String(sharedTarget), runs);

    CompactionMap map =
        new GenericCompactionMap(SOURCE_SNAPSHOT_ID, TARGET_SNAPSHOT_ID, List.of(mapping));

    // Write and read back (read triggers interning)
    CompactionMap readMap = writeAndRead(map);
    FileMapping readMapping = readMap.fileMappings().get(0);

    // After interning, all target file strings should be the same instance
    String firstTarget = readMapping.runs().get(0).targetFile();
    for (int i = 1; i < readMapping.runs().size(); i++) {
      String target = readMapping.runs().get(i).targetFile();
      assertThat(target)
          .as("Target file at run " + i + " should be same instance due to interning")
          .isSameAs(firstTarget);
    }

    // Also verify mapping-level target is interned with runs
    assertThat(readMapping.targetFile()).isSameAs(firstTarget);

    System.out.println("=== Target File Interning Test ===");
    System.out.println(
        "PASS: All " + numRuns + " runs share the same String instance after interning");
  }

  private long measureSerializedSize(CompactionMap compactionMap) throws IOException {
    OutputFile outputFile = new InMemoryOutputFile();

    try (CompactionMaps.CompactionMapWriter writer = CompactionMaps.write(outputFile)) {
      writer.write(compactionMap);
    }

    return outputFile.toInputFile().getLength();
  }

  private CompactionMap writeAndRead(CompactionMap compactionMap) throws IOException {
    OutputFile outputFile = new InMemoryOutputFile();

    try (CompactionMaps.CompactionMapWriter writer = CompactionMaps.write(outputFile)) {
      writer.write(compactionMap);
    }

    InputFile inputFile = outputFile.toInputFile();
    return CompactionMaps.read(inputFile);
  }
}
