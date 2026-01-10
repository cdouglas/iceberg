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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.DVPositionReader;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.RemappedDVWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end integration tests for deletion vector (DV) remapping with compaction maps.
 *
 * <p>These tests validate the complete DV remapping workflow: creating compaction maps, remapping
 * DVs, writing new DVs, and verifying correctness.
 */
public class TestDVRemappingEndToEnd {
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
  public void testSimpleDVRemapping() throws IOException {
    // Scenario: 1 source file compacted to 1 target file
    String sourceFile = "s3://bucket/source.parquet";
    String targetFile = "s3://bucket/target.parquet";

    // Build compaction map: source[0-99] -> target[0-99]
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping(sourceFile, targetFile).addRun(0L, 0L, 100L);
    CompactionMap map = builder.build();

    // Create DV with deleted positions: 10, 20, 30, 40, 50
    DeleteFile sourceDV = writeDV(sourceFile, 10L, 20L, 30L, 40L, 50L);

    // Remap DV using compaction map
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);
    Map<String, Set<Long>> remappedPositions = remapper.remapDV(sourceDV, table.io());

    // Verify remapped positions
    assertThat(remappedPositions).hasSize(1);
    assertThat(remappedPositions).containsKey(targetFile);

    Set<Long> positions = remappedPositions.get(targetFile);
    assertThat(positions).containsExactlyInAnyOrder(10L, 20L, 30L, 40L, 50L);

    // Write remapped DVs
    RemappedDVWriter writer = new RemappedDVWriter(table, table.spec(), null);
    List<DeleteFile> newDVs = writer.writeRemappedDVs(remappedPositions);

    // Verify new DV was created
    assertThat(newDVs).hasSize(1);
    DeleteFile newDV = newDVs.get(0);
    assertThat(newDV.referencedDataFile()).isEqualTo(targetFile);
    assertThat(newDV.recordCount()).isEqualTo(5L);

    // Read back and verify positions
    DVPositionReader reader = new DVPositionReader(table.io());
    Set<Long> readPositions = new HashSet<>();
    try (CloseableIterable<Long> iter = reader.readDeletedPositions(newDV)) {
      iter.forEach(readPositions::add);
    }
    assertThat(readPositions).containsExactlyInAnyOrder(10L, 20L, 30L, 40L, 50L);
  }

  @Test
  public void testDVRemappingWithGaps() throws IOException {
    // Scenario: source file compacted with gaps (some rows deleted)
    String sourceFile = "s3://bucket/source.parquet";
    String targetFile = "s3://bucket/target.parquet";

    // Build compaction map with gaps:
    // source[0-49] -> target[0-49]
    // source[100-149] -> target[50-99] (gap at source 50-99 - rows deleted)
    Run run1 = new GenericCompactionMap.GenericRun(0L, 0L, 50L);
    Run run2 = new GenericCompactionMap.GenericRun(100L, 50L, 50L);
    List<Run> runs = ImmutableList.of(run1, run2);

    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping(sourceFile, targetFile).addRun(0L, 0L, 50L).addRun(100L, 50L, 50L);
    CompactionMap map = builder.build();

    // Create DV with positions: 25 (in run1), 75 (in gap), 125 (in run2)
    DeleteFile sourceDV = writeDV(sourceFile, 25L, 75L, 125L);

    // Remap DV
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);
    Map<String, Set<Long>> remappedPositions = remapper.remapDV(sourceDV, table.io());

    // Verify remapped positions
    // Position 25 -> 25, Position 75 dropped (in gap), Position 125 -> 75 (125-100+50)
    assertThat(remappedPositions).hasSize(1);
    Set<Long> positions = remappedPositions.get(targetFile);
    assertThat(positions).containsExactlyInAnyOrder(25L, 75L);
  }

  @Test
  public void testDVRemappingMultipleSourcesOneTarget() throws IOException {
    // Scenario: 3 source files -> 1 target file (N:1 compaction)
    String source1 = "s3://bucket/source1.parquet";
    String source2 = "s3://bucket/source2.parquet";
    String source3 = "s3://bucket/source3.parquet";
    String target = "s3://bucket/target.parquet";

    // Build compaction map
    // source1[0-49] -> target[0-49]
    // source2[0-49] -> target[50-99]
    // source3[0-49] -> target[100-149]
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping(source1, target).addRun(0L, 0L, 50L);
    builder.addFileMapping(source2, target).addRun(0L, 50L, 50L);
    builder.addFileMapping(source3, target).addRun(0L, 100L, 50L);
    CompactionMap map = builder.build();

    // Create DVs for each source file
    DeleteFile dv1 = writeDV(source1, 10L, 20L);
    DeleteFile dv2 = writeDV(source2, 10L, 20L);
    DeleteFile dv3 = writeDV(source3, 10L, 20L);

    // Remap all DVs
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    Map<String, Set<Long>> remapped1 = remapper.remapDV(dv1, table.io());
    Map<String, Set<Long>> remapped2 = remapper.remapDV(dv2, table.io());
    Map<String, Set<Long>> remapped3 = remapper.remapDV(dv3, table.io());

    // Verify all remap to the same target with different offsets
    assertThat(remapped1.get(target)).containsExactlyInAnyOrder(10L, 20L); // source1 -> target
    assertThat(remapped2.get(target)).containsExactlyInAnyOrder(60L, 70L); // source2 -> target+50
    assertThat(remapped3.get(target))
        .containsExactlyInAnyOrder(110L, 120L); // source3 -> target+100

    // Merge all positions and write combined DV
    Set<Long> allPositions = new HashSet<>();
    allPositions.addAll(remapped1.get(target));
    allPositions.addAll(remapped2.get(target));
    allPositions.addAll(remapped3.get(target));

    Map<String, Set<Long>> mergedPositions = ImmutableMap.of(target, allPositions);

    RemappedDVWriter writer = new RemappedDVWriter(table, table.spec(), null);
    List<DeleteFile> newDVs = writer.writeRemappedDVs(mergedPositions);

    // Verify merged DV
    assertThat(newDVs).hasSize(1);
    assertThat(newDVs.get(0).recordCount()).isEqualTo(6L); // 2 from each source file
  }

  @Test
  public void testDVRemappingAllPositionsDeleted() throws IOException {
    // Scenario: All positions in DV are in gaps (already deleted in compaction)
    String sourceFile = "s3://bucket/source.parquet";
    String targetFile = "s3://bucket/target.parquet";

    // Build compaction map with large gap
    // source[0-49] -> target[0-49]
    // (gap at source 50-199 - all rows deleted)
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping(sourceFile, targetFile).addRun(0L, 0L, 50L);
    CompactionMap map = builder.build();

    // Create DV with positions all in the gap
    DeleteFile sourceDV = writeDV(sourceFile, 50L, 75L, 100L, 150L);

    // Remap DV
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);
    Map<String, Set<Long>> remappedPositions = remapper.remapDV(sourceDV, table.io());

    // Verify remapped positions are empty (all in gaps)
    assertThat(remappedPositions).isEmpty();

    // Write remapped DVs
    RemappedDVWriter writer = new RemappedDVWriter(table, table.spec(), null);
    List<DeleteFile> newDVs = writer.writeRemappedDVs(remappedPositions);

    // Verify no DVs were written
    assertThat(newDVs).isEmpty();
  }

  @Test
  public void testDVRemappingNonCompactedFile() throws IOException {
    // Scenario: DV references file not in compaction map (passthrough)
    String sourceFile = "s3://bucket/source.parquet";
    String otherFile = "s3://bucket/other.parquet";
    String targetFile = "s3://bucket/target.parquet";

    // Build compaction map for different file
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping(otherFile, targetFile).addRun(0L, 0L, 100L);
    CompactionMap map = builder.build();

    // Create DV for non-compacted file
    DeleteFile sourceDV = writeDV(sourceFile, 10L, 20L, 30L);

    // Remap DV
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);
    Map<String, Set<Long>> remappedPositions = remapper.remapDV(sourceDV, table.io());

    // Verify passthrough - same file, same positions
    assertThat(remappedPositions).hasSize(1);
    assertThat(remappedPositions).containsKey(sourceFile);
    assertThat(remappedPositions.get(sourceFile)).containsExactlyInAnyOrder(10L, 20L, 30L);
  }

  @Test
  public void testDVRemappingLargeNumberOfPositions() throws IOException {
    // Scenario: DV with many positions (stress test)
    String sourceFile = "s3://bucket/source.parquet";
    String targetFile = "s3://bucket/target.parquet";

    // Build compaction map: source[0-9999] -> target[0-9999]
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping(sourceFile, targetFile).addRun(0L, 0L, 10000L);
    CompactionMap map = builder.build();

    // Create DV with 1000 deleted positions
    Set<Long> originalPositions = new HashSet<>();
    for (long i = 0; i < 10000; i += 10) {
      originalPositions.add(i);
    }
    DeleteFile sourceDV = writeDV(sourceFile, originalPositions.toArray(new Long[0]));

    // Remap DV
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);
    Map<String, Set<Long>> remappedPositions = remapper.remapDV(sourceDV, table.io());

    // Verify all positions remapped correctly
    assertThat(remappedPositions.get(targetFile)).hasSize(1000);
    assertThat(remappedPositions.get(targetFile)).containsAll(originalPositions);

    // Write and verify
    RemappedDVWriter writer = new RemappedDVWriter(table, table.spec(), null);
    List<DeleteFile> newDVs = writer.writeRemappedDVs(remappedPositions);

    assertThat(newDVs).hasSize(1);
    assertThat(newDVs.get(0).recordCount()).isEqualTo(1000L);
  }

  /**
   * Helper method to write a DV file with specified positions.
   *
   * @param dataFilePath the data file this DV references
   * @param positions the positions to mark as deleted
   * @return the written DeleteFile
   */
  private DeleteFile writeDV(String dataFilePath, Long... positions) throws IOException {
    DVFileWriter writer = new BaseDVFileWriter(fileFactory, path -> PositionDeleteIndex.empty());

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
