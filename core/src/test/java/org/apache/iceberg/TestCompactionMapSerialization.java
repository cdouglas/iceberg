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
import java.util.List;
import org.apache.iceberg.CompactionMap.FileMapping;
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.GenericCompactionMap.GenericFileMapping;
import org.apache.iceberg.GenericCompactionMap.GenericRun;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class TestCompactionMapSerialization {

  private static final long SOURCE_SNAPSHOT_ID = 1000L;
  private static final long TARGET_SNAPSHOT_ID = 2000L;

  @Test
  public void testBasicRoundTrip() throws IOException {
    // Create test data: one file mapping with two runs
    Run run1 = new GenericRun(0L, 0L, 100L);
    Run run2 = new GenericRun(100L, 200L, 50L);
    List<Run> runs = ImmutableList.of(run1, run2);

    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/table/data1.parquet", "s3://bucket/table/data2.parquet", runs);

    CompactionMap originalMap =
        new GenericCompactionMap(SOURCE_SNAPSHOT_ID, TARGET_SNAPSHOT_ID, ImmutableList.of(mapping));

    // Write and read back
    CompactionMap readMap = writeAndRead(originalMap);

    // Verify all fields
    assertThat(readMap.sourceSnapshotId()).isEqualTo(SOURCE_SNAPSHOT_ID);
    assertThat(readMap.targetSnapshotId()).isEqualTo(TARGET_SNAPSHOT_ID);
    assertThat(readMap.fileMappings()).hasSize(1);

    FileMapping readMapping = readMap.fileMappings().get(0);
    assertThat(readMapping.sourceFile()).isEqualTo("s3://bucket/table/data1.parquet");
    assertThat(readMapping.targetFile()).isEqualTo("s3://bucket/table/data2.parquet");
    assertThat(readMapping.runs()).hasSize(2);

    Run readRun1 = readMapping.runs().get(0);
    assertThat(readRun1.sourcePosition()).isEqualTo(0L);
    assertThat(readRun1.targetPosition()).isEqualTo(0L);
    assertThat(readRun1.length()).isEqualTo(100L);

    Run readRun2 = readMapping.runs().get(1);
    assertThat(readRun2.sourcePosition()).isEqualTo(100L);
    assertThat(readRun2.targetPosition()).isEqualTo(200L);
    assertThat(readRun2.length()).isEqualTo(50L);
  }

  @Test
  public void testMultipleFileMappings() throws IOException {
    // Create multiple file mappings
    FileMapping mapping1 =
        new GenericFileMapping(
            "s3://bucket/table/file1.parquet",
            "s3://bucket/table/compacted1.parquet",
            ImmutableList.of(new GenericRun(0L, 0L, 1000L)));

    FileMapping mapping2 =
        new GenericFileMapping(
            "s3://bucket/table/file2.parquet",
            "s3://bucket/table/compacted1.parquet",
            ImmutableList.of(new GenericRun(0L, 1000L, 500L)));

    CompactionMap originalMap =
        new GenericCompactionMap(
            SOURCE_SNAPSHOT_ID, TARGET_SNAPSHOT_ID, ImmutableList.of(mapping1, mapping2));

    CompactionMap readMap = writeAndRead(originalMap);

    assertThat(readMap.fileMappings()).hasSize(2);
    assertThat(readMap.fileMappings().get(0).sourceFile())
        .isEqualTo("s3://bucket/table/file1.parquet");
    assertThat(readMap.fileMappings().get(1).sourceFile())
        .isEqualTo("s3://bucket/table/file2.parquet");
  }

  @Test
  public void testMappingForFile() throws IOException {
    FileMapping mapping1 =
        new GenericFileMapping(
            "s3://bucket/table/file1.parquet",
            "s3://bucket/table/compacted.parquet",
            ImmutableList.of(new GenericRun(0L, 0L, 100L)));

    FileMapping mapping2 =
        new GenericFileMapping(
            "s3://bucket/table/file2.parquet",
            "s3://bucket/table/compacted.parquet",
            ImmutableList.of(new GenericRun(0L, 100L, 200L)));

    CompactionMap originalMap =
        new GenericCompactionMap(
            SOURCE_SNAPSHOT_ID, TARGET_SNAPSHOT_ID, ImmutableList.of(mapping1, mapping2));

    CompactionMap readMap = writeAndRead(originalMap);

    // Test lookup by file path
    FileMapping found1 = readMap.mappingForFile("s3://bucket/table/file1.parquet");
    assertThat(found1).isNotNull();
    assertThat(found1.sourceFile()).isEqualTo("s3://bucket/table/file1.parquet");

    FileMapping found2 = readMap.mappingForFile("s3://bucket/table/file2.parquet");
    assertThat(found2).isNotNull();
    assertThat(found2.sourceFile()).isEqualTo("s3://bucket/table/file2.parquet");

    FileMapping notFound = readMap.mappingForFile("s3://bucket/table/nonexistent.parquet");
    assertThat(notFound).isNull();
  }

  @Test
  public void testRunForPosition() throws IOException {
    // Create runs with gaps
    Run run1 = new GenericRun(0L, 100L, 50L); // positions 0-49 -> 100-149
    Run run2 = new GenericRun(100L, 200L, 30L); // positions 100-129 -> 200-229

    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/table/source.parquet",
            "s3://bucket/table/target.parquet",
            ImmutableList.of(run1, run2));

    CompactionMap originalMap =
        new GenericCompactionMap(SOURCE_SNAPSHOT_ID, TARGET_SNAPSHOT_ID, ImmutableList.of(mapping));

    CompactionMap readMap = writeAndRead(originalMap);
    FileMapping readMapping = readMap.fileMappings().get(0);

    // Test position lookups
    Run foundRun1 = readMapping.runForPosition(0L);
    assertThat(foundRun1).isNotNull();
    assertThat(foundRun1.sourcePosition()).isEqualTo(0L);

    Run foundRun2 = readMapping.runForPosition(100L);
    assertThat(foundRun2).isNotNull();
    assertThat(foundRun2.sourcePosition()).isEqualTo(100L);

    // Position in gap between runs
    Run notInRun = readMapping.runForPosition(50L);
    assertThat(notInRun).isNull();

    // Position beyond all runs
    Run beyondRuns = readMapping.runForPosition(200L);
    assertThat(beyondRuns).isNull();
  }

  @Test
  public void testMapPosition() {
    Run run = new GenericRun(100L, 500L, 200L); // source 100-299 -> target 500-699

    // Test mapping positions within the run
    assertThat(run.mapPosition(100L)).isEqualTo(500L);
    assertThat(run.mapPosition(150L)).isEqualTo(550L);
    assertThat(run.mapPosition(299L)).isEqualTo(699L);
  }

  @Test
  public void testTargetPathInterning() throws IOException {
    // Create multiple file mappings that all share the same target file
    // This simulates a bin-pack compaction where many small files are combined into one
    String sharedTargetPath = "s3://bucket/warehouse/db/table/data/compacted-output.parquet";

    FileMapping mapping1 =
        new GenericFileMapping(
            "s3://bucket/warehouse/db/table/data/file1.parquet",
            sharedTargetPath,
            ImmutableList.of(new GenericRun(0L, 0L, 1000L)));

    FileMapping mapping2 =
        new GenericFileMapping(
            "s3://bucket/warehouse/db/table/data/file2.parquet",
            sharedTargetPath,
            ImmutableList.of(new GenericRun(0L, 1000L, 500L)));

    FileMapping mapping3 =
        new GenericFileMapping(
            "s3://bucket/warehouse/db/table/data/file3.parquet",
            sharedTargetPath,
            ImmutableList.of(new GenericRun(0L, 1500L, 750L)));

    CompactionMap originalMap =
        new GenericCompactionMap(
            SOURCE_SNAPSHOT_ID, TARGET_SNAPSHOT_ID, ImmutableList.of(mapping1, mapping2, mapping3));

    // Write and read back - this should trigger interning
    CompactionMap readMap = writeAndRead(originalMap);

    assertThat(readMap.fileMappings()).hasSize(3);

    // After interning, all three mappings should share the same String instance for targetFile
    String target1 = readMap.fileMappings().get(0).targetFile();
    String target2 = readMap.fileMappings().get(1).targetFile();
    String target3 = readMap.fileMappings().get(2).targetFile();

    // Verify values are equal
    assertThat(target1).isEqualTo(sharedTargetPath);
    assertThat(target2).isEqualTo(sharedTargetPath);
    assertThat(target3).isEqualTo(sharedTargetPath);

    // Verify object identity (same instance due to interning)
    assertThat(target1).isSameAs(target2);
    assertThat(target2).isSameAs(target3);
  }

  @Test
  public void testCopy() throws IOException {
    Run run = new GenericRun(0L, 100L, 50L);
    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/table/source.parquet",
            "s3://bucket/table/target.parquet",
            ImmutableList.of(run));

    CompactionMap originalMap =
        new GenericCompactionMap(SOURCE_SNAPSHOT_ID, TARGET_SNAPSHOT_ID, ImmutableList.of(mapping));

    CompactionMap copy = originalMap.copy();

    // Verify copy has same data
    assertThat(copy.sourceSnapshotId()).isEqualTo(originalMap.sourceSnapshotId());
    assertThat(copy.targetSnapshotId()).isEqualTo(originalMap.targetSnapshotId());
    assertThat(copy.fileMappings()).hasSize(1);
    assertThat(copy.fileMappings().get(0).sourceFile())
        .isEqualTo(originalMap.fileMappings().get(0).sourceFile());
  }

  @Test
  public void testMultiTargetMappingRoundTrip() throws IOException {
    // Create a multi-target mapping where source rows span multiple target files
    Run run1 = new GenericRun(0L, 0L, 100L, "s3://bucket/table/target1.parquet");
    Run run2 = new GenericRun(100L, 0L, 100L, "s3://bucket/table/target2.parquet");
    Run run3 = new GenericRun(200L, 0L, 50L, "s3://bucket/table/target3.parquet");

    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/table/large-source.parquet",
            "s3://bucket/table/target1.parquet", // default target
            ImmutableList.of(run1, run2, run3));

    CompactionMap originalMap =
        new GenericCompactionMap(SOURCE_SNAPSHOT_ID, TARGET_SNAPSHOT_ID, ImmutableList.of(mapping));

    // Write and read back
    CompactionMap readMap = writeAndRead(originalMap);

    // Verify multi-target mapping preserved
    assertThat(readMap.fileMappings()).hasSize(1);
    FileMapping readMapping = readMap.fileMappings().get(0);
    assertThat(readMapping.sourceFile()).isEqualTo("s3://bucket/table/large-source.parquet");
    assertThat(readMapping.targetFile()).isEqualTo("s3://bucket/table/target1.parquet");
    assertThat(readMapping.runs()).hasSize(3);

    Run readRun1 = readMapping.runs().get(0);
    assertThat(readRun1.sourcePosition()).isEqualTo(0L);
    assertThat(readRun1.targetPosition()).isEqualTo(0L);
    assertThat(readRun1.length()).isEqualTo(100L);
    assertThat(readRun1.targetFile()).isEqualTo("s3://bucket/table/target1.parquet");

    Run readRun2 = readMapping.runs().get(1);
    assertThat(readRun2.sourcePosition()).isEqualTo(100L);
    assertThat(readRun2.targetPosition()).isEqualTo(0L);
    assertThat(readRun2.length()).isEqualTo(100L);
    assertThat(readRun2.targetFile()).isEqualTo("s3://bucket/table/target2.parquet");

    Run readRun3 = readMapping.runs().get(2);
    assertThat(readRun3.sourcePosition()).isEqualTo(200L);
    assertThat(readRun3.targetPosition()).isEqualTo(0L);
    assertThat(readRun3.length()).isEqualTo(50L);
    assertThat(readRun3.targetFile()).isEqualTo("s3://bucket/table/target3.parquet");
  }

  @Test
  public void testMultiTargetWithNullRunTargets() throws IOException {
    // Create mapping with runs that use the default target (null targetFile)
    Run run1 = new GenericRun(0L, 0L, 100L); // null target = use default
    Run run2 = new GenericRun(100L, 100L, 50L, null); // explicit null = use default
    Run run3 = new GenericRun(150L, 0L, 50L, "s3://bucket/table/different.parquet"); // explicit

    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/table/source.parquet",
            "s3://bucket/table/default-target.parquet",
            ImmutableList.of(run1, run2, run3));

    CompactionMap originalMap =
        new GenericCompactionMap(SOURCE_SNAPSHOT_ID, TARGET_SNAPSHOT_ID, ImmutableList.of(mapping));

    CompactionMap readMap = writeAndRead(originalMap);

    FileMapping readMapping = readMap.fileMappings().get(0);
    assertThat(readMapping.runs()).hasSize(3);

    // First two runs should have null targetFile (use default)
    assertThat(readMapping.runs().get(0).targetFile()).isNull();
    assertThat(readMapping.runs().get(1).targetFile()).isNull();

    // Third run has explicit target
    assertThat(readMapping.runs().get(2).targetFile())
        .isEqualTo("s3://bucket/table/different.parquet");
  }

  @Test
  public void testMultiTargetRunPathInterning() throws IOException {
    // Create multiple runs that share the same target file path
    String sharedTarget = "s3://bucket/warehouse/db/table/compacted.parquet";

    Run run1 = new GenericRun(0L, 0L, 100L, sharedTarget);
    Run run2 = new GenericRun(100L, 100L, 50L, sharedTarget);
    Run run3 = new GenericRun(150L, 0L, 50L, "s3://bucket/other.parquet");
    Run run4 = new GenericRun(200L, 150L, 25L, sharedTarget);

    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/source.parquet", "s3://bucket/default.parquet",
            ImmutableList.of(run1, run2, run3, run4));

    CompactionMap originalMap =
        new GenericCompactionMap(SOURCE_SNAPSHOT_ID, TARGET_SNAPSHOT_ID, ImmutableList.of(mapping));

    CompactionMap readMap = writeAndRead(originalMap);
    FileMapping readMapping = readMap.fileMappings().get(0);

    // After interning, runs with the same target path should share the same String instance
    String target1 = readMapping.runs().get(0).targetFile();
    String target2 = readMapping.runs().get(1).targetFile();
    String target4 = readMapping.runs().get(3).targetFile();

    assertThat(target1).isEqualTo(sharedTarget);
    assertThat(target2).isEqualTo(sharedTarget);
    assertThat(target4).isEqualTo(sharedTarget);

    // Verify object identity (same instance due to interning)
    assertThat(target1).isSameAs(target2);
    assertThat(target2).isSameAs(target4);
  }

  private CompactionMap writeAndRead(CompactionMap compactionMap) throws IOException {
    OutputFile outputFile = new InMemoryOutputFile();

    // Write the compaction map
    try (CompactionMaps.CompactionMapWriter writer = CompactionMaps.write(outputFile)) {
      writer.write(compactionMap);
    }

    // Read it back
    InputFile inputFile = outputFile.toInputFile();
    return CompactionMaps.read(inputFile);
  }
}
