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

import java.util.List;
import org.apache.iceberg.CompactionMap.FileMapping;
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.GenericCompactionMap.GenericFileMapping;
import org.apache.iceberg.GenericCompactionMap.GenericRun;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class TestPositionDeleteRemapper {

  @Test
  public void testRemapDeleteInRange() {
    // Create a compaction map: file1 rows 0-99 -> file2 rows 0-99
    Run run = new GenericRun(0L, 0L, 100L);
    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/file1.parquet", "s3://bucket/file2.parquet", ImmutableList.of(run));
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Create a position delete for row 50 in file1
    PositionDelete<?> delete = PositionDelete.create().set("s3://bucket/file1.parquet", 50L);

    // Remap the delete
    PositionDelete<?> remapped = remapper.remapDelete(delete);

    // Should be remapped to row 50 in file2
    assertThat(remapped.path().toString()).isEqualTo("s3://bucket/file2.parquet");
    assertThat(remapped.pos()).isEqualTo(50L);
  }

  @Test
  public void testRemapDeleteWithOffset() {
    // Create a compaction map: file1 rows 100-199 -> file2 rows 0-99
    Run run = new GenericRun(100L, 0L, 100L);
    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/file1.parquet", "s3://bucket/file2.parquet", ImmutableList.of(run));
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Create a position delete for row 150 in file1
    PositionDelete<?> delete = PositionDelete.create().set("s3://bucket/file1.parquet", 150L);

    // Remap the delete
    PositionDelete<?> remapped = remapper.remapDelete(delete);

    // Should be remapped to row 50 in file2 (150 - 100 = 50)
    assertThat(remapped.path().toString()).isEqualTo("s3://bucket/file2.parquet");
    assertThat(remapped.pos()).isEqualTo(50L);
  }

  @Test
  public void testRemapDeleteNotInCompactionMap() {
    // Create a compaction map for file1
    Run run = new GenericRun(0L, 0L, 100L);
    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/file1.parquet", "s3://bucket/file2.parquet", ImmutableList.of(run));
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Create a position delete for file3 (not in compaction map)
    PositionDelete<?> delete = PositionDelete.create().set("s3://bucket/file3.parquet", 50L);

    // Remap the delete
    PositionDelete<?> remapped = remapper.remapDelete(delete);

    // Should return the original delete unchanged
    assertThat(remapped.path().toString()).isEqualTo("s3://bucket/file3.parquet");
    assertThat(remapped.pos()).isEqualTo(50L);
  }

  @Test
  public void testRemapDeleteWithRow() {
    // Create a compaction map
    Run run = new GenericRun(0L, 0L, 100L);
    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/file1.parquet", "s3://bucket/file2.parquet", ImmutableList.of(run));
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Create a position delete with row data
    String rowData = "test-row-data";
    PositionDelete<String> delete =
        PositionDelete.<String>create().set("s3://bucket/file1.parquet", 50L, rowData);

    // Remap the delete
    PositionDelete<?> remapped = remapper.remapDelete(delete);

    // Should preserve the row data
    assertThat(remapped.path().toString()).isEqualTo("s3://bucket/file2.parquet");
    assertThat(remapped.pos()).isEqualTo(50L);
    assertThat(remapped.row()).isEqualTo(rowData);
  }

  @Test
  public void testRemapDeleteWithMultipleRuns() {
    // Create a compaction map with multiple runs
    // file1: rows 0-99 -> file2 rows 0-99
    // file1: rows 200-299 -> file2 rows 100-199 (gap in source)
    Run run1 = new GenericRun(0L, 0L, 100L);
    Run run2 = new GenericRun(200L, 100L, 100L);
    List<Run> runs = ImmutableList.of(run1, run2);

    FileMapping mapping =
        new GenericFileMapping("s3://bucket/file1.parquet", "s3://bucket/file2.parquet", runs);
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Test position in first run
    PositionDelete<?> delete1 = PositionDelete.create().set("s3://bucket/file1.parquet", 50L);
    PositionDelete<?> remapped1 = remapper.remapDelete(delete1);
    assertThat(remapped1.pos()).isEqualTo(50L);

    // Test position in second run
    PositionDelete<?> delete2 = PositionDelete.create().set("s3://bucket/file1.parquet", 250L);
    PositionDelete<?> remapped2 = remapper.remapDelete(delete2);
    assertThat(remapped2.pos()).isEqualTo(150L); // 250 - 200 + 100 = 150
  }

  @Test
  public void testIsCompacted() {
    // Create a compaction map
    Run run = new GenericRun(0L, 0L, 100L);
    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/file1.parquet", "s3://bucket/file2.parquet", ImmutableList.of(run));
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // file1 should be in the compaction map
    assertThat(remapper.isCompacted("s3://bucket/file1.parquet")).isTrue();

    // file3 should not be in the compaction map
    assertThat(remapper.isCompacted("s3://bucket/file3.parquet")).isFalse();
  }

  @Test
  public void testCompactedFiles() {
    // Create a compaction map with multiple file mappings
    Run run1 = new GenericRun(0L, 0L, 100L);
    Run run2 = new GenericRun(0L, 100L, 50L);

    FileMapping mapping1 =
        new GenericFileMapping(
            "s3://bucket/file1.parquet", "s3://bucket/compacted.parquet", ImmutableList.of(run1));
    FileMapping mapping2 =
        new GenericFileMapping(
            "s3://bucket/file2.parquet", "s3://bucket/compacted.parquet", ImmutableList.of(run2));

    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping1, mapping2));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Should return both compacted files
    assertThat(remapper.compactedFiles())
        .containsExactlyInAnyOrder("s3://bucket/file1.parquet", "s3://bucket/file2.parquet");
  }

  @Test
  public void testNeedsRemappingWithReferencedDataFile() {
    // Create a compaction map
    Run run = new GenericRun(0L, 0L, 100L);
    FileMapping mapping =
        new GenericFileMapping(
            "s3://bucket/file1.parquet", "s3://bucket/file2.parquet", ImmutableList.of(run));
    CompactionMap map = new GenericCompactionMap(1L, 2L, ImmutableList.of(mapping));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Create a delete file that references file1 (which is in the compaction map)
    DeleteFile deleteFileCompacted =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("s3://bucket/delete1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(10)
            .withReferencedDataFile("s3://bucket/file1.parquet")
            .build();

    // Should need remapping since it references file1
    assertThat(remapper.needsRemapping(deleteFileCompacted)).isTrue();

    // Create a delete file that references file3 (not in the compaction map)
    DeleteFile deleteFileNotCompacted =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("s3://bucket/delete2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(10)
            .withReferencedDataFile("s3://bucket/file3.parquet")
            .build();

    // Should not need remapping since file3 is not in the compaction map
    assertThat(remapper.needsRemapping(deleteFileNotCompacted)).isFalse();

    // Create a delete file without a referenced data file
    DeleteFile deleteFileNoRef =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("s3://bucket/delete3.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(10)
            .build();

    // Should return false since we can't determine without reading the file
    assertThat(remapper.needsRemapping(deleteFileNoRef)).isFalse();
  }
}
