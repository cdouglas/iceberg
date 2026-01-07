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
