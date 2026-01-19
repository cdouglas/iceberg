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
import java.util.Map;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestDeleteManifestRemapper {

  private static final Schema TEST_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  @Test
  public void testBasicRemapping() {
    // Single source file → single target file
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping("source.parquet", "target.parquet").addRun(0, 0, 100);
    CompactionMap map = builder.build();

    // Create deletes for source file
    List<PositionDeleteRecord> deletes =
        Lists.newArrayList(
            new PositionDeleteRecord("source.parquet", 10L),
            new PositionDeleteRecord("source.parquet", 50L),
            new PositionDeleteRecord("source.parquet", 99L));

    // Remap
    DeleteManifestRemapper remapper = new DeleteManifestRemapper(map);
    Map<String, List<PositionDeleteRecord>> remapped = remapper.remapDeletes(deletes);

    // Verify
    assertThat(remapped).hasSize(1);
    assertThat(remapped).containsKey("target.parquet");

    List<PositionDeleteRecord> targetDeletes = remapped.get("target.parquet");
    assertThat(targetDeletes).hasSize(3);
    assertThat(targetDeletes.get(0).dataFilePath()).isEqualTo("target.parquet");
    assertThat(targetDeletes.get(0).position()).isEqualTo(10L);
    assertThat(targetDeletes.get(1).position()).isEqualTo(50L);
    assertThat(targetDeletes.get(2).position()).isEqualTo(99L);
  }

  @Test
  public void testMergeCompaction() {
    // Multiple source files → one target file
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping("source1.parquet", "target.parquet").addRun(0, 0, 100);
    builder.addFileMapping("source2.parquet", "target.parquet").addRun(0, 100, 50);
    CompactionMap map = builder.build();

    // Create deletes for both source files
    List<PositionDeleteRecord> deletes =
        Lists.newArrayList(
            new PositionDeleteRecord("source1.parquet", 10L),
            new PositionDeleteRecord("source2.parquet", 5L),
            new PositionDeleteRecord("source1.parquet", 50L),
            new PositionDeleteRecord("source2.parquet", 20L));

    // Remap
    DeleteManifestRemapper remapper = new DeleteManifestRemapper(map);
    Map<String, List<PositionDeleteRecord>> remapped = remapper.remapDeletes(deletes);

    // Verify - all deletes merged to target
    assertThat(remapped).hasSize(1);
    assertThat(remapped).containsKey("target.parquet");

    List<PositionDeleteRecord> targetDeletes = remapped.get("target.parquet");
    assertThat(targetDeletes).hasSize(4);

    // Check remapped positions
    // source1[10] → target[10], source1[50] → target[50]
    // source2[5] → target[105], source2[20] → target[120]
    assertThat(targetDeletes)
        .extracting(PositionDeleteRecord::position)
        .containsExactlyInAnyOrder(10L, 50L, 105L, 120L);
  }

  @Test
  public void testFilteredRows() {
    // Compaction with gaps - some rows filtered out
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder
        .addFileMapping("source.parquet", "target.parquet")
        .addRun(0, 0, 50) // Rows 0-49 preserved
        .addRun(100, 50, 50); // Rows 100-149 preserved, gap at 50-99

    CompactionMap map = builder.build();

    // Create deletes including positions in the gap
    List<PositionDeleteRecord> deletes =
        Lists.newArrayList(
            new PositionDeleteRecord("source.parquet", 10L), // Preserved
            new PositionDeleteRecord("source.parquet", 75L), // Filtered (in gap)
            new PositionDeleteRecord("source.parquet", 110L)); // Preserved

    // Remap
    DeleteManifestRemapper remapper = new DeleteManifestRemapper(map);
    Map<String, List<PositionDeleteRecord>> remapped = remapper.remapDeletes(deletes);

    // Verify - only 2 deletes (gap delete dropped)
    assertThat(remapped).hasSize(1);
    List<PositionDeleteRecord> targetDeletes = remapped.get("target.parquet");
    assertThat(targetDeletes).hasSize(2);
    assertThat(targetDeletes.get(0).position()).isEqualTo(10L);
    assertThat(targetDeletes.get(1).position()).isEqualTo(60L); // 110 - 100 + 50
  }

  @Test
  public void testPartitionPreservation() {
    // Create partitioned deletes
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping("source.parquet", "target.parquet").addRun(0, 0, 100);
    CompactionMap map = builder.build();

    // Create partition data
    Schema partitionSchema =
        new Schema(Types.NestedField.required(1, "partition_col", Types.StringType.get()));
    Record partition1 = GenericRecord.create(partitionSchema);
    partition1.setField("partition_col", "p1");

    // Create deletes with partition data
    List<PositionDeleteRecord> deletes =
        Lists.newArrayList(
            new PositionDeleteRecord("source.parquet", 10L, partition1, null),
            new PositionDeleteRecord("source.parquet", 50L, partition1, null));

    // Remap
    DeleteManifestRemapper remapper = new DeleteManifestRemapper(map);
    Map<String, List<PositionDeleteRecord>> remapped = remapper.remapDeletes(deletes);

    // Verify partition data preserved
    List<PositionDeleteRecord> targetDeletes = remapped.get("target.parquet");
    assertThat(targetDeletes).hasSize(2);
    assertThat(targetDeletes.get(0).partitionData()).isEqualTo(partition1);
    assertThat(targetDeletes.get(1).partitionData()).isEqualTo(partition1);
  }

  @Test
  public void testRowDataPreservation() {
    // Create deletes with row data
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping("source.parquet", "target.parquet").addRun(0, 0, 100);
    CompactionMap map = builder.build();

    // Create row data
    Record row1 = GenericRecord.create(TEST_SCHEMA);
    row1 = row1.copy(ImmutableMap.of("id", 1, "data", "row1"));

    Record row2 = GenericRecord.create(TEST_SCHEMA);
    row2 = row2.copy(ImmutableMap.of("id", 2, "data", "row2"));

    // Create deletes with row data
    List<PositionDeleteRecord> deletes =
        Lists.newArrayList(
            new PositionDeleteRecord("source.parquet", 10L, null, row1),
            new PositionDeleteRecord("source.parquet", 50L, null, row2));

    // Remap
    DeleteManifestRemapper remapper = new DeleteManifestRemapper(map);
    Map<String, List<PositionDeleteRecord>> remapped = remapper.remapDeletes(deletes);

    // Verify row data preserved
    List<PositionDeleteRecord> targetDeletes = remapped.get("target.parquet");
    assertThat(targetDeletes).hasSize(2);
    assertThat(targetDeletes.get(0).rowData()).isEqualTo(row1);
    assertThat(targetDeletes.get(1).rowData()).isEqualTo(row2);
  }

  @Test
  public void testEmptyDeleteList() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping("source.parquet", "target.parquet").addRun(0, 0, 100);
    CompactionMap map = builder.build();

    DeleteManifestRemapper remapper = new DeleteManifestRemapper(map);
    Map<String, List<PositionDeleteRecord>> remapped = remapper.remapDeletes(ImmutableList.of());

    // Verify empty result
    assertThat(remapped).isEmpty();
  }

  @Test
  public void testAllDeletesFiltered() {
    // All deletes reference positions that were filtered out
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder
        .addFileMapping("source.parquet", "target.parquet")
        .addRun(0, 0, 50) // Only rows 0-49 preserved
        .addRun(100, 50, 50); // And rows 100-149

    CompactionMap map = builder.build();

    // All deletes in the gap (50-99)
    List<PositionDeleteRecord> deletes =
        Lists.newArrayList(
            new PositionDeleteRecord("source.parquet", 60L),
            new PositionDeleteRecord("source.parquet", 75L),
            new PositionDeleteRecord("source.parquet", 90L));

    // Remap
    DeleteManifestRemapper remapper = new DeleteManifestRemapper(map);
    Map<String, List<PositionDeleteRecord>> remapped = remapper.remapDeletes(deletes);

    // Verify all filtered out
    assertThat(remapped).isEmpty();
  }

  @Test
  public void testRemappedDeletesReferenceCorrectFiles() {
    // Property test: all remapped deletes reference target files, not source files
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping("source1.parquet", "target1.parquet").addRun(0, 0, 100);
    builder.addFileMapping("source2.parquet", "target2.parquet").addRun(0, 0, 50);
    CompactionMap map = builder.build();

    // Create deletes for both sources
    List<PositionDeleteRecord> deletes =
        Lists.newArrayList(
            new PositionDeleteRecord("source1.parquet", 10L),
            new PositionDeleteRecord("source1.parquet", 50L),
            new PositionDeleteRecord("source2.parquet", 20L),
            new PositionDeleteRecord("source2.parquet", 40L));

    // Remap
    DeleteManifestRemapper remapper = new DeleteManifestRemapper(map);
    Map<String, List<PositionDeleteRecord>> remapped = remapper.remapDeletes(deletes);

    // Verify: only target files referenced
    assertThat(remapped.keySet()).containsExactlyInAnyOrder("target1.parquet", "target2.parquet");

    // Verify: no source file references in any delete
    for (Map.Entry<String, List<PositionDeleteRecord>> entry : remapped.entrySet()) {
      for (PositionDeleteRecord delete : entry.getValue()) {
        assertThat(delete.dataFilePath()).isIn("target1.parquet", "target2.parquet");
        assertThat(delete.dataFilePath()).isNotIn("source1.parquet", "source2.parquet");
      }
    }
  }

  @Test
  public void testNonCompactedFilesIgnored() {
    // Deletes referencing files not in compaction map are silently ignored
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping("compacted.parquet", "target.parquet").addRun(0, 0, 100);
    CompactionMap map = builder.build();

    // Mix of compacted and non-compacted file references
    List<PositionDeleteRecord> deletes =
        Lists.newArrayList(
            new PositionDeleteRecord("compacted.parquet", 10L),
            new PositionDeleteRecord("not-compacted.parquet", 20L),
            new PositionDeleteRecord("compacted.parquet", 50L));

    // Remap
    DeleteManifestRemapper remapper = new DeleteManifestRemapper(map);
    Map<String, List<PositionDeleteRecord>> remapped = remapper.remapDeletes(deletes);

    // Verify: only compacted file deletes remapped
    assertThat(remapped).hasSize(1);
    assertThat(remapped).containsKey("target.parquet");
    List<PositionDeleteRecord> targetDeletes = remapped.get("target.parquet");
    assertThat(targetDeletes).hasSize(2);
    assertThat(targetDeletes).extracting(PositionDeleteRecord::position).containsExactly(10L, 50L);
  }

  @Test
  public void testIsCompacted() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping("source1.parquet", "target.parquet").addRun(0, 0, 100);
    builder.addFileMapping("source2.parquet", "target.parquet").addRun(0, 100, 50);
    CompactionMap map = builder.build();

    DeleteManifestRemapper remapper = new DeleteManifestRemapper(map);

    // Verify compacted file check
    assertThat(remapper.isCompacted("source1.parquet")).isTrue();
    assertThat(remapper.isCompacted("source2.parquet")).isTrue();
    assertThat(remapper.isCompacted("target.parquet")).isFalse();
    assertThat(remapper.isCompacted("other.parquet")).isFalse();
  }

  @Test
  public void testStaticUtilityMethod() {
    // Test PositionDeleteRemapper.remapDeleteManifests() static method
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping("source.parquet", "target.parquet").addRun(0, 0, 100);
    CompactionMap map = builder.build();

    List<PositionDeleteRecord> deletes =
        Lists.newArrayList(
            new PositionDeleteRecord("source.parquet", 10L),
            new PositionDeleteRecord("source.parquet", 50L));

    // Use static method
    Map<String, List<PositionDeleteRecord>> remapped =
        PositionDeleteRemapper.remapDeleteManifests(deletes, map);

    // Verify same behavior as instance method
    assertThat(remapped).hasSize(1);
    assertThat(remapped).containsKey("target.parquet");
    List<PositionDeleteRecord> targetDeletes = remapped.get("target.parquet");
    assertThat(targetDeletes).hasSize(2);
    assertThat(targetDeletes).extracting(PositionDeleteRecord::position).containsExactly(10L, 50L);
  }
}
