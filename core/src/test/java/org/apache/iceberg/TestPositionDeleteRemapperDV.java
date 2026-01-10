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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CompactionMap.FileMapping;
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.GenericCompactionMap.GenericFileMapping;
import org.apache.iceberg.GenericCompactionMap.GenericRun;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestPositionDeleteRemapperDV {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir private File tempDir;

  private org.apache.iceberg.hadoop.HadoopTables tables;
  private Table table;
  private OutputFileFactory fileFactory;

  @BeforeEach
  public void setUp() {
    tables = new org.apache.iceberg.hadoop.HadoopTables(new org.apache.hadoop.conf.Configuration());
    String tableLocation = tempDir.toURI().toString();
    table =
        tables.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of("format-version", "3"),
            tableLocation);

    fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
  }

  @Test
  public void testNeedsRemappingTrue() throws IOException {
    // Create a compaction map for file1
    Run run = new GenericRun(0L, 0L, 100L);
    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/file1.parquet", "s3://bucket/file2.parquet", ImmutableList.of(run));
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Create a DV that references file1 (which is in the compaction map)
    DeleteFile dv = writeDV("s3://bucket/file1.parquet", 5L, 10L, 20L);

    // Should need remapping since it references file1
    assertThat(remapper.needsRemapping(dv)).isTrue();
  }

  @Test
  public void testNeedsRemappingFalse() throws IOException {
    // Create a compaction map for file1
    Run run = new GenericRun(0L, 0L, 100L);
    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/file1.parquet", "s3://bucket/file2.parquet", ImmutableList.of(run));
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Create a DV that references file3 (not in the compaction map)
    DeleteFile dv = writeDV("s3://bucket/file3.parquet", 5L, 10L, 20L);

    // Should not need remapping since file3 is not compacted
    assertThat(remapper.needsRemapping(dv)).isFalse();
  }

  @Test
  public void testNeedsRemappingThrowsForNonDV() {
    // Create a compaction map
    Run run = new GenericRun(0L, 0L, 100L);
    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/file1.parquet", "s3://bucket/file2.parquet", ImmutableList.of(run));
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Create a non-DV position delete file (Parquet format)
    DeleteFile posDeletes =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withFormat(FileFormat.PARQUET)
            .withPath("s3://bucket/pos-deletes.parquet")
            .withFileSizeInBytes(100L)
            .withRecordCount(10L)
            .withReferencedDataFile("s3://bucket/file1.parquet")
            .build();

    // needsRemapping should work for regular position deletes too
    assertThat(remapper.needsRemapping(posDeletes)).isTrue();
  }

  @Test
  public void testRemapDVSimple() throws IOException {
    // Create a compaction map: file1 rows 0-99 -> file2 rows 0-99 (sequential, no gaps)
    Run run = new GenericRun(0L, 0L, 100L);
    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/file1.parquet", "s3://bucket/file2.parquet", ImmutableList.of(run));
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Create a DV with positions 5, 10, 20
    DeleteFile dv = writeDV("s3://bucket/file1.parquet", 5L, 10L, 20L);

    // Remap the DV
    Map<String, Set<Long>> remapped = remapper.remapDV(dv, table.io());

    // Should have one target file
    assertThat(remapped).hasSize(1);
    assertThat(remapped).containsKey("s3://bucket/file2.parquet");

    // Positions should be unchanged (simple sequential mapping)
    Set<Long> positions = remapped.get("s3://bucket/file2.parquet");
    assertThat(positions).containsExactlyInAnyOrder(5L, 10L, 20L);
  }

  @Test
  public void testRemapDVWithGaps() throws IOException {
    // Create a compaction map with gaps:
    // file1: rows 0-49 -> file2 rows 0-49
    // file1: rows 100-149 -> file2 rows 50-99 (gap in source at 50-99)
    Run run1 = new GenericRun(0L, 0L, 50L);
    Run run2 = new GenericRun(100L, 50L, 50L);
    List<Run> runs = ImmutableList.of(run1, run2);

    FileMapping mapping =
        new GenericFileMapping("s3://bucket/file1.parquet", "s3://bucket/file2.parquet", runs);
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Create a DV with positions: 10 (in run1), 75 (in gap), 120 (in run2)
    DeleteFile dv = writeDV("s3://bucket/file1.parquet", 10L, 75L, 120L);

    // Remap the DV
    Map<String, Set<Long>> remapped = remapper.remapDV(dv, table.io());

    // Should have one target file
    assertThat(remapped).hasSize(1);
    assertThat(remapped).containsKey("s3://bucket/file2.parquet");

    // Position 10 -> 10, Position 75 dropped (in gap), Position 120 -> 70 (120-100+50)
    Set<Long> positions = remapped.get("s3://bucket/file2.parquet");
    assertThat(positions).containsExactlyInAnyOrder(10L, 70L);
  }

  @Test
  public void testRemapDVAllPositionsDeleted() throws IOException {
    // Create a compaction map with a single run: rows 0-49 -> rows 0-49
    // Gap at 50-99 (deleted in source)
    Run run = new GenericRun(0L, 0L, 50L);
    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/file1.parquet", "s3://bucket/file2.parquet", ImmutableList.of(run));
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Create a DV with positions all in the gap (50-99)
    DeleteFile dv = writeDV("s3://bucket/file1.parquet", 50L, 75L, 99L);

    // Remap the DV
    Map<String, Set<Long>> remapped = remapper.remapDV(dv, table.io());

    // Should return empty map (all positions were in gaps)
    assertThat(remapped).isEmpty();
  }

  @Test
  public void testRemapDVSomePositionsDeleted() throws IOException {
    // Create a compaction map with gaps
    // file1: rows 0-49 -> file2 rows 0-49
    // file1: rows 100-149 -> file2 rows 50-99
    Run run1 = new GenericRun(0L, 0L, 50L);
    Run run2 = new GenericRun(100L, 50L, 50L);
    List<Run> runs = ImmutableList.of(run1, run2);

    FileMapping mapping =
        new GenericFileMapping("s3://bucket/file1.parquet", "s3://bucket/file2.parquet", runs);
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Create a DV with mix of valid and gap positions
    DeleteFile dv =
        writeDV(
            "s3://bucket/file1.parquet",
            10L, // in run1 -> 10
            60L, // in gap, dropped
            80L, // in gap, dropped
            110L // in run2 -> 60
            );

    // Remap the DV
    Map<String, Set<Long>> remapped = remapper.remapDV(dv, table.io());

    // Should have one target file with 2 positions
    assertThat(remapped).hasSize(1);
    Set<Long> positions = remapped.get("s3://bucket/file2.parquet");
    assertThat(positions).containsExactlyInAnyOrder(10L, 60L);
  }

  @Test
  public void testRemapDVMultipleSourceFiles() throws IOException {
    // Create a compaction map with multiple source files compacted into same target
    // This tests that remapping works correctly when multiple files are involved
    // file1: rows 0-49 -> target rows 0-49
    // file2: rows 0-49 -> target rows 50-99
    Run run1 = new GenericRun(0L, 0L, 50L);
    Run run2 = new GenericRun(0L, 50L, 50L);

    FileMapping mapping1 =
        new GenericFileMapping(
            "s3://bucket/file1.parquet", "s3://bucket/target.parquet", ImmutableList.of(run1));
    FileMapping mapping2 =
        new GenericFileMapping(
            "s3://bucket/file2.parquet", "s3://bucket/target.parquet", ImmutableList.of(run2));

    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping1, mapping2));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Create a DV for file1
    DeleteFile dv = writeDV("s3://bucket/file1.parquet", 10L, 20L);

    // Remap the DV
    Map<String, Set<Long>> remapped = remapper.remapDV(dv, table.io());

    // Should map to target file with correct offsets
    assertThat(remapped).hasSize(1);
    assertThat(remapped).containsKey("s3://bucket/target.parquet");

    Set<Long> positions = remapped.get("s3://bucket/target.parquet");
    assertThat(positions).containsExactlyInAnyOrder(10L, 20L);
  }

  @Test
  public void testRemapDVLargePositions() throws IOException {
    // Create a compaction map with large positions (> Integer.MAX_VALUE)
    long largeSourceOffset = (long) Integer.MAX_VALUE + 1000L;
    long largeTargetOffset = (long) Integer.MAX_VALUE + 2000L;
    Run run = new GenericRun(largeSourceOffset, largeTargetOffset, 1000L);

    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/file1.parquet", "s3://bucket/file2.parquet", ImmutableList.of(run));
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Create a DV with large positions
    long pos1 = largeSourceOffset + 10L;
    long pos2 = largeSourceOffset + 500L;
    DeleteFile dv = writeDV("s3://bucket/file1.parquet", pos1, pos2);

    // Remap the DV
    Map<String, Set<Long>> remapped = remapper.remapDV(dv, table.io());

    // Should handle large positions correctly
    assertThat(remapped).hasSize(1);
    Set<Long> positions = remapped.get("s3://bucket/file2.parquet");

    long expectedPos1 = largeTargetOffset + 10L;
    long expectedPos2 = largeTargetOffset + 500L;
    assertThat(positions).containsExactlyInAnyOrder(expectedPos1, expectedPos2);
  }

  @Test
  public void testRemapDVNonCompactedFile() throws IOException {
    // Create a compaction map for file1
    Run run = new GenericRun(0L, 0L, 100L);
    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/file1.parquet", "s3://bucket/file2.parquet", ImmutableList.of(run));
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Create a DV that references file3 (not in compaction map)
    DeleteFile dv = writeDV("s3://bucket/file3.parquet", 5L, 10L, 20L);

    // Remap the DV
    Map<String, Set<Long>> remapped = remapper.remapDV(dv, table.io());

    // Should return original file with original positions (passthrough)
    assertThat(remapped).hasSize(1);
    assertThat(remapped).containsKey("s3://bucket/file3.parquet");

    Set<Long> positions = remapped.get("s3://bucket/file3.parquet");
    assertThat(positions).containsExactlyInAnyOrder(5L, 10L, 20L);
  }

  /**
   * Helper method to write a deletion vector file with the given positions.
   *
   * @param dataFilePath the data file that this DV references
   * @param positions the positions to mark as deleted
   * @return the written DeleteFile
   */
  private DeleteFile writeDV(String dataFilePath, Long... positions) throws IOException {
    // Function that returns empty index (no previous deletes)
    java.util.function.Function<String, PositionDeleteIndex> noPreviousDeletes =
        path -> PositionDeleteIndex.empty();

    DVFileWriter writer = new BaseDVFileWriter(fileFactory, noPreviousDeletes);

    for (Long pos : positions) {
      writer.delete(dataFilePath, pos, table.spec(), null);
    }

    writer.close();
    DeleteWriteResult result = writer.result();

    assertThat(result.deleteFiles())
        .as("DV writer should produce exactly one delete file")
        .hasSize(1);

    return result.deleteFiles().get(0);
  }
}
