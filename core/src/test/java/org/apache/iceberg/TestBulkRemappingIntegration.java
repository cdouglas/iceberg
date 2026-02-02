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

/** Integration tests for bulk remapping functionality. */
public class TestBulkRemappingIntegration {
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
  public void testBulkRemappingWithMultipleTargets() throws IOException {
    // Test N:1 compaction (multiple sources to single target)
    List<Run> runs =
        ImmutableList.of(
            new GenericRun(0L, 0L, 100L), // file1 [0-100) -> file4 [0-100)
            new GenericRun(100L, 100L, 100L)); // file1 [100-200) -> file4 [100-200)

    FileMapping mapping1 =
        new GenericFileMapping("s3://bucket/file1.parquet", "s3://bucket/file4.parquet", runs);

    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping1));

    // Create DV with positions spanning both runs
    DeleteFile dv = writeDV("s3://bucket/file1.parquet", 10L, 50L, 110L, 150L);

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Remap using bulk API
    Map<String, Set<Long>> remapped = remapper.remapDVBulk(dv, table.io());

    // All positions should map to file4
    assertThat(remapped).hasSize(1);
    assertThat(remapped).containsKey("s3://bucket/file4.parquet");

    Set<Long> mappedPositions = remapped.get("s3://bucket/file4.parquet");
    assertThat(mappedPositions).containsExactlyInAnyOrder(10L, 50L, 110L, 150L);
  }

  @Test
  public void testBulkRemappingWithGaps() throws IOException {
    // Test positions falling in gaps (should be dropped)
    List<Run> runs =
        ImmutableList.of(
            new GenericRun(0L, 0L, 50L), // [0-50)
            new GenericRun(100L, 50L, 50L)); // [100-150) - gap at [50-100)

    FileMapping mapping =
        new GenericFileMapping("s3://bucket/file1.parquet", "s3://bucket/file2.parquet", runs);

    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    // Create DV with positions in runs and gaps
    DeleteFile dv = writeDV("s3://bucket/file1.parquet", 10L, 40L, 60L, 80L, 110L, 140L);

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Remap using bulk API
    Map<String, Set<Long>> remapped = remapper.remapDVBulk(dv, table.io());

    assertThat(remapped).hasSize(1);
    Set<Long> mappedPositions = remapped.get("s3://bucket/file2.parquet");

    // Only positions in runs should be mapped (60L, 80L in gap should be dropped)
    assertThat(mappedPositions).containsExactlyInAnyOrder(10L, 40L, 60L, 90L);
  }

  @Test
  public void testBulkRemappingLargeScale() throws IOException {
    // Test with 100,000 positions to validate performance at scale
    List<Run> runs = new java.util.ArrayList<>();
    for (int i = 0; i < 100; i++) {
      long sourcePos = i * 1000L;
      long targetPos = i * 1000L;
      runs.add(new GenericRun(sourcePos, targetPos, 1000));
    }

    FileMapping mapping =
        new GenericFileMapping("s3://bucket/file1.parquet", "s3://bucket/file2.parquet", runs);
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    // Create DV with 100,000 positions
    Long[] positions = new Long[100000];
    for (int i = 0; i < 100000; i++) {
      positions[i] = (long) i; // Consecutive positions
    }
    DeleteFile dv = writeDV("s3://bucket/file1.parquet", positions);

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Should complete in reasonable time (<5 seconds)
    long start = System.nanoTime();
    Map<String, Set<Long>> remapped = remapper.remapDVBulk(dv, table.io());
    long elapsed = System.nanoTime() - start;

    // Verify correctness
    assertThat(remapped).hasSize(1);
    assertThat(remapped.get("s3://bucket/file2.parquet")).hasSize(100000);

    // Performance check: should complete in less than 5 seconds
    assertThat(elapsed).isLessThan(5_000_000_000L); // 5 seconds in nanoseconds
  }

  @Test
  public void testBulkRemappingCorrectness() throws IOException {
    // Comprehensive correctness test with complex scenario
    List<Run> runs =
        ImmutableList.of(
            new GenericRun(0L, 0L, 100L),
            new GenericRun(200L, 100L, 100L),
            new GenericRun(400L, 200L, 100L));

    FileMapping mapping =
        new GenericFileMapping("s3://bucket/file1.parquet", "s3://bucket/file2.parquet", runs);
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    // Create DV with positions across all runs
    DeleteFile dv = writeDV("s3://bucket/file1.parquet", 50L, 250L, 450L);

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Test both APIs produce same result
    Map<String, Set<Long>> bulkResults = remapper.remapDVBulk(dv, table.io());
    Map<String, Set<Long>> oldResults = remapper.remapDV(dv, table.io());

    assertThat(bulkResults).isEqualTo(oldResults);

    // Verify correct remapping
    Set<Long> positions = bulkResults.get("s3://bucket/file2.parquet");
    assertThat(positions).containsExactlyInAnyOrder(50L, 150L, 250L);
  }

  @Test
  public void testRemapPositionsBulkBasic() {
    // Test the new remapPositionsBulk(String, Iterable<Long>) API
    List<Run> runs =
        ImmutableList.of(new GenericRun(0L, 0L, 100L), new GenericRun(100L, 100L, 100L));

    FileMapping mapping =
        new GenericFileMapping("s3://bucket/file1.parquet", "s3://bucket/file2.parquet", runs);
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Remap using new API with raw positions
    List<Long> positions = ImmutableList.of(10L, 50L, 110L, 150L);
    Map<String, Set<Long>> result =
        remapper.remapPositionsBulk("s3://bucket/file1.parquet", positions);

    assertThat(result).hasSize(1);
    assertThat(result).containsKey("s3://bucket/file2.parquet");
    assertThat(result.get("s3://bucket/file2.parquet"))
        .containsExactlyInAnyOrder(10L, 50L, 110L, 150L);
  }

  @Test
  public void testRemapPositionsBulkNonCompacted() {
    // Test remapping positions for a file that wasn't compacted
    List<Run> runs = ImmutableList.of(new GenericRun(0L, 0L, 100L));
    FileMapping mapping =
        new GenericFileMapping("s3://bucket/file1.parquet", "s3://bucket/file2.parquet", runs);
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Remap positions for a file NOT in the compaction map
    List<Long> positions = ImmutableList.of(5L, 10L, 20L);
    Map<String, Set<Long>> result =
        remapper.remapPositionsBulk("s3://bucket/other-file.parquet", positions);

    // Should return original file with original positions
    assertThat(result).hasSize(1);
    assertThat(result).containsKey("s3://bucket/other-file.parquet");
    assertThat(result.get("s3://bucket/other-file.parquet"))
        .containsExactlyInAnyOrder(5L, 10L, 20L);
  }

  @Test
  public void testRemapPositionsBulkWithGaps() {
    // Test positions in gaps (from merge compaction) are dropped
    List<Run> runs =
        ImmutableList.of(
            new GenericRun(0L, 0L, 50L), // [0-50)
            new GenericRun(100L, 50L, 50L)); // [100-150) - gap at [50-100)

    FileMapping mapping =
        new GenericFileMapping("s3://bucket/file1.parquet", "s3://bucket/file2.parquet", runs);
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Positions 60 and 80 are in the gap and should be dropped
    List<Long> positions = ImmutableList.of(10L, 40L, 60L, 80L, 110L, 140L);
    Map<String, Set<Long>> result =
        remapper.remapPositionsBulk("s3://bucket/file1.parquet", positions);

    assertThat(result).hasSize(1);
    Set<Long> mappedPositions = result.get("s3://bucket/file2.parquet");
    // 10->10, 40->40, 60 dropped, 80 dropped, 110->60, 140->90
    assertThat(mappedPositions).containsExactlyInAnyOrder(10L, 40L, 60L, 90L);
  }

  @Test
  public void testRemapPositionsBulkEmpty() {
    List<Run> runs = ImmutableList.of(new GenericRun(0L, 0L, 100L));
    FileMapping mapping =
        new GenericFileMapping("s3://bucket/file1.parquet", "s3://bucket/file2.parquet", runs);
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Empty positions list
    Map<String, Set<Long>> result =
        remapper.remapPositionsBulk("s3://bucket/file1.parquet", ImmutableList.of());

    assertThat(result).isEmpty();
  }

  @Test
  public void testRemapPositionsBulkLargeScale() {
    // Test with 100,000 positions to validate performance
    List<Run> runs = new java.util.ArrayList<>();
    for (int i = 0; i < 100; i++) {
      long sourcePos = i * 1000L;
      long targetPos = i * 1000L;
      runs.add(new GenericRun(sourcePos, targetPos, 1000));
    }

    FileMapping mapping =
        new GenericFileMapping("s3://bucket/file1.parquet", "s3://bucket/file2.parquet", runs);
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    // Create 100,000 positions
    List<Long> positions = new java.util.ArrayList<>(100000);
    for (int i = 0; i < 100000; i++) {
      positions.add((long) i);
    }

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Should complete quickly
    long start = System.nanoTime();
    Map<String, Set<Long>> result =
        remapper.remapPositionsBulk("s3://bucket/file1.parquet", positions);
    long elapsed = System.nanoTime() - start;

    assertThat(result).hasSize(1);
    assertThat(result.get("s3://bucket/file2.parquet")).hasSize(100000);

    // Should complete in less than 1 second
    assertThat(elapsed).isLessThan(1_000_000_000L);
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
