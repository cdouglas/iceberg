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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.iceberg.deletes.PositionDelete;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Integration tests for position delete remapping with CompactionMap infrastructure.
 *
 * <p>These tests validate the end-to-end remapping workflow: building compaction maps from file
 * rewrites and using PositionDeleteRemapper to remap position deletes to new file locations.
 */
public class TestPositionDeleteRemappingIntegration {

  @Test
  public void testBasicPositionDeleteRemapping() {
    // Scenario: 3 source files compacted into 1 target file
    String source1 = "s3://bucket/source1.parquet";
    String source2 = "s3://bucket/source2.parquet";
    String source3 = "s3://bucket/source3.parquet";
    String target = "s3://bucket/target.parquet";

    // Build compaction map
    // source1[0-99] -> target[0-99]
    // source2[0-99] -> target[100-199]
    // source3[0-99] -> target[200-299]
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping(source1, target).addRun(0L, 0L, 100L);
    builder.addFileMapping(source2, target).addRun(0L, 100L, 100L);
    builder.addFileMapping(source3, target).addRun(0L, 200L, 100L);

    CompactionMap map = builder.build();

    // Create position deletes on source files
    List<PositionDelete<?>> originalDeletes = new ArrayList<>();
    originalDeletes.add(PositionDelete.create().set(source1, 10L, null)); // -> target:10
    originalDeletes.add(PositionDelete.create().set(source1, 50L, null)); // -> target:50
    originalDeletes.add(PositionDelete.create().set(source2, 10L, null)); // -> target:110
    originalDeletes.add(PositionDelete.create().set(source2, 80L, null)); // -> target:180
    originalDeletes.add(PositionDelete.create().set(source3, 5L, null)); // -> target:205
    originalDeletes.add(PositionDelete.create().set(source3, 95L, null)); // -> target:295

    // Remap deletes
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);
    List<PositionDelete<?>> remappedDeletes = new ArrayList<>();
    for (PositionDelete<?> delete : originalDeletes) {
      remappedDeletes.add(remapper.remapDelete(delete));
    }

    // Verify remapping
    assertThat(remappedDeletes).hasSize(6);

    // Check each remapped delete
    assertThat(remappedDeletes.get(0).path().toString()).isEqualTo(target);
    assertThat(remappedDeletes.get(0).pos()).isEqualTo(10L);

    assertThat(remappedDeletes.get(1).path().toString()).isEqualTo(target);
    assertThat(remappedDeletes.get(1).pos()).isEqualTo(50L);

    assertThat(remappedDeletes.get(2).path().toString()).isEqualTo(target);
    assertThat(remappedDeletes.get(2).pos()).isEqualTo(110L);

    assertThat(remappedDeletes.get(3).path().toString()).isEqualTo(target);
    assertThat(remappedDeletes.get(3).pos()).isEqualTo(180L);

    assertThat(remappedDeletes.get(4).path().toString()).isEqualTo(target);
    assertThat(remappedDeletes.get(4).pos()).isEqualTo(205L);

    assertThat(remappedDeletes.get(5).path().toString()).isEqualTo(target);
    assertThat(remappedDeletes.get(5).pos()).isEqualTo(295L);
  }

  @Test
  public void testBinPackScenario() {
    // Scenario: 5 small files -> 2 target files
    String[] sources = {
        "s3://bucket/s1.parquet",
        "s3://bucket/s2.parquet",
        "s3://bucket/s3.parquet",
        "s3://bucket/s4.parquet",
        "s3://bucket/s5.parquet"
    };

    String target1 = "s3://bucket/t1.parquet";
    String target2 = "s3://bucket/t2.parquet";

    // Build compaction map
    // s1[0-49], s2[0-49], s3[0-49] -> t1[0-149]
    // s4[0-49], s5[0-49] -> t2[0-99]
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping(sources[0], target1).addRun(0L, 0L, 50L);
    builder.addFileMapping(sources[1], target1).addRun(0L, 50L, 50L);
    builder.addFileMapping(sources[2], target1).addRun(0L, 100L, 50L);
    builder.addFileMapping(sources[3], target2).addRun(0L, 0L, 50L);
    builder.addFileMapping(sources[4], target2).addRun(0L, 50L, 50L);

    CompactionMap map = builder.build();

    // Create random position deletes
    Random random = new Random(42);
    List<PositionDelete<?>> originalDeletes = new ArrayList<>();
    for (String source : sources) {
      for (int i = 0; i < 10; i++) {
        long pos = random.nextInt(50);
        originalDeletes.add(PositionDelete.create().set(source, pos, null));
      }
    }

    // Remap deletes
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);
    List<PositionDelete<?>> remappedDeletes = new ArrayList<>();
    for (PositionDelete<?> delete : originalDeletes) {
      remappedDeletes.add(remapper.remapDelete(delete));
    }

    // Verify all deletes remapped
    assertThat(remappedDeletes).hasSize(originalDeletes.size());

    // Verify all deletes point to one of the two target files
    for (PositionDelete<?> delete : remappedDeletes) {
      String path = delete.path().toString();
      assertThat(path).isIn(target1, target2);
    }
  }

  @Test
  public void testRemappingWithGaps() {
    // Scenario with position gaps (e.g., from filtering)
    String source = "s3://bucket/source.parquet";
    String target = "s3://bucket/target.parquet";

    // Build compaction map with gaps
    // source[0-49] -> target[0-49]
    // source[100-149] -> target[50-99] (gap at source 50-99)
    // source[200-249] -> target[100-149] (gap at source 150-199)
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    CompactionMapBuilder.FileMappingBuilder fileBuilder =
        builder.addFileMapping(source, target);
    fileBuilder.addRun(0L, 0L, 50L);
    fileBuilder.addRun(100L, 50L, 50L);
    fileBuilder.addRun(200L, 100L, 50L);

    CompactionMap map = builder.build();

    // Create position deletes in various ranges
    List<PositionDelete<?>> originalDeletes = new ArrayList<>();
    originalDeletes.add(PositionDelete.create().set(source, 10L, null)); // In first run -> 10
    originalDeletes.add(PositionDelete.create().set(source, 110L, null)); // In second run -> 60
    originalDeletes.add(PositionDelete.create().set(source, 220L, null)); // In third run -> 120

    // Remap deletes
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);
    List<PositionDelete<?>> remappedDeletes = new ArrayList<>();
    for (PositionDelete<?> delete : originalDeletes) {
      remappedDeletes.add(remapper.remapDelete(delete));
    }

    // Verify remapping
    assertThat(remappedDeletes).hasSize(3);
    assertThat(remappedDeletes.get(0).pos()).isEqualTo(10L);
    assertThat(remappedDeletes.get(1).pos()).isEqualTo(60L); // 110 - 100 + 50
    assertThat(remappedDeletes.get(2).pos()).isEqualTo(120L); // 220 - 200 + 100
  }

  @Test
  public void testRemappingPreservesRowData() {
    // Test that remapping preserves optional row data
    String source = "s3://bucket/source.parquet";
    String target = "s3://bucket/target.parquet";

    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping(source, target).addRun(0L, 0L, 100L);

    CompactionMap map = builder.build();

    // Create position delete with row data
    String rowData = "test-row-data";
    PositionDelete<String> deleteWithRow =
        PositionDelete.<String>create().set(source, 50L, rowData);

    // Remap delete
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);
    PositionDelete<?> remapped = remapper.remapDelete(deleteWithRow);

    // Verify row data preserved
    assertThat(remapped.path().toString()).isEqualTo(target);
    assertThat(remapped.pos()).isEqualTo(50L);
    assertThat(remapped.row()).isEqualTo(rowData);
  }

  @Test
  public void testNonCompactedFilesPassThrough() {
    // Test that deletes on non-compacted files pass through unchanged
    String compactedFile = "s3://bucket/compacted.parquet";
    String nonCompactedFile = "s3://bucket/other.parquet";
    String target = "s3://bucket/target.parquet";

    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping(compactedFile, target).addRun(0L, 0L, 100L);

    CompactionMap map = builder.build();

    // Create deletes on both compacted and non-compacted files
    PositionDelete<?> deleteCompacted =
        PositionDelete.create().set(compactedFile, 50L, null);
    PositionDelete<?> deleteNonCompacted =
        PositionDelete.create().set(nonCompactedFile, 50L, null);

    // Remap deletes
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);
    PositionDelete<?> remappedCompacted = remapper.remapDelete(deleteCompacted);
    PositionDelete<?> remappedNonCompacted = remapper.remapDelete(deleteNonCompacted);

    // Verify compacted file delete was remapped
    assertThat(remappedCompacted.path().toString()).isEqualTo(target);
    assertThat(remappedCompacted.pos()).isEqualTo(50L);

    // Verify non-compacted file delete passed through unchanged
    assertThat(remappedNonCompacted.path().toString()).isEqualTo(nonCompactedFile);
    assertThat(remappedNonCompacted.pos()).isEqualTo(50L);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 5, 10, 20})
  public void testScalability(int numFiles) {
    // Test remapping with various numbers of files
    String target = "s3://bucket/target.parquet";

    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    long offset = 0;
    long rowsPerFile = 100;

    // Create mappings for N source files to 1 target
    List<String> sources = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      String source = String.format("s3://bucket/source%d.parquet", i);
      sources.add(source);
      builder.addFileMapping(source, target).addRun(0L, offset, rowsPerFile);
      offset += rowsPerFile;
    }

    CompactionMap map = builder.build();

    // Create deletes across all source files
    Random random = new Random(numFiles);
    List<PositionDelete<?>> originalDeletes = new ArrayList<>();
    for (String source : sources) {
      for (int i = 0; i < 10; i++) {
        long pos = random.nextInt((int) rowsPerFile);
        originalDeletes.add(PositionDelete.create().set(source, pos, null));
      }
    }

    // Remap deletes
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);
    List<PositionDelete<?>> remappedDeletes = new ArrayList<>();
    for (PositionDelete<?> delete : originalDeletes) {
      remappedDeletes.add(remapper.remapDelete(delete));
    }

    // Verify all deletes remapped correctly
    assertThat(remappedDeletes).hasSize(numFiles * 10);
    for (PositionDelete<?> delete : remappedDeletes) {
      assertThat(delete.path().toString()).isEqualTo(target);
      assertThat(delete.pos()).isLessThan(numFiles * rowsPerFile);
    }
  }
}
