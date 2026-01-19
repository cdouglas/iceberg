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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

/**
 * Tests for edge cases in position delete remapping.
 *
 * <p>These tests verify handling of:
 *
 * <ul>
 *   <li>Invalid (negative) positions
 *   <li>Files not in compaction map
 *   <li>Filtered rows (gaps in runs)
 *   <li>Duplicate deletes
 *   <li>Empty inputs
 *   <li>Large position values
 * </ul>
 */
public class TestRemappingEdgeCases {

  @Test
  public void testRemapNegativePosition() {
    // Setup: Create compaction map
    CompactionMapBuilder mapBuilder = new CompactionMapBuilder(1L, 2L);
    mapBuilder
        .addFileMapping("/path/to/source.parquet", "/path/to/target.parquet")
        .addRun(0, 0, 100);
    CompactionMap compactionMap = mapBuilder.build();

    // Create deletes with negative position
    List<PositionDeleteRecord> deletes =
        ImmutableList.of(
            new PositionDeleteRecord("/path/to/source.parquet", -1L, null, null),
            new PositionDeleteRecord("/path/to/source.parquet", 10L, null, null),
            new PositionDeleteRecord("/path/to/source.parquet", -100L, null, null));

    // Remap
    DeleteManifestRemapper remapper = new DeleteManifestRemapper(compactionMap);
    RemappingResult result = remapper.remapDeletesWithMetrics(deletes);

    // Verify: only valid position was remapped
    assertThat(result.totalRemapped()).isEqualTo(1);
    assertThat(result.skippedInvalidPositions()).isEqualTo(2);
    assertThat(result.remappedDeletes()).containsKey("/path/to/target.parquet");
    assertThat(result.remappedDeletes().get("/path/to/target.parquet")).hasSize(1);
    assertThat(result.remappedDeletes().get("/path/to/target.parquet").get(0).position())
        .isEqualTo(10L);
  }

  @Test
  public void testRemapFileNotInCompactionMap() {
    // Setup: Create compaction map for one file
    CompactionMapBuilder mapBuilder = new CompactionMapBuilder(1L, 2L);
    mapBuilder
        .addFileMapping("/path/to/source.parquet", "/path/to/target.parquet")
        .addRun(0, 0, 100);
    CompactionMap compactionMap = mapBuilder.build();

    // Create deletes for a different file
    List<PositionDeleteRecord> deletes =
        ImmutableList.of(
            new PositionDeleteRecord("/path/to/other.parquet", 10L, null, null),
            new PositionDeleteRecord("/path/to/source.parquet", 20L, null, null));

    // Remap
    DeleteManifestRemapper remapper = new DeleteManifestRemapper(compactionMap);
    RemappingResult result = remapper.remapDeletesWithMetrics(deletes);

    // Verify: only the delete for the compacted file was remapped
    assertThat(result.totalRemapped()).isEqualTo(1);
    assertThat(result.skippedNotCompacted()).isEqualTo(1);
    assertThat(result.remappedDeletes().get("/path/to/target.parquet")).hasSize(1);
    assertThat(result.remappedDeletes().get("/path/to/target.parquet").get(0).position())
        .isEqualTo(20L);
  }

  @Test
  public void testRemapFilteredRows() {
    // Setup: Create compaction map with gaps (simulating filtered rows)
    // Source file had positions 0-99, but compaction only kept positions 0-49 and 75-99
    CompactionMapBuilder mapBuilder = new CompactionMapBuilder(1L, 2L);
    mapBuilder
        .addFileMapping("/path/to/source.parquet", "/path/to/target.parquet")
        .addRun(0, 0, 50) // positions 0-49 map to 0-49
        .addRun(75, 50, 25); // positions 75-99 map to 50-74
    CompactionMap compactionMap = mapBuilder.build();

    // Create deletes including positions that were filtered
    List<PositionDeleteRecord> deletes =
        ImmutableList.of(
            new PositionDeleteRecord("/path/to/source.parquet", 10L, null, null), // in first run
            new PositionDeleteRecord("/path/to/source.parquet", 55L, null, null), // filtered
            new PositionDeleteRecord("/path/to/source.parquet", 60L, null, null), // filtered
            new PositionDeleteRecord("/path/to/source.parquet", 80L, null, null)); // in second run

    // Remap
    DeleteManifestRemapper remapper = new DeleteManifestRemapper(compactionMap);
    RemappingResult result = remapper.remapDeletesWithMetrics(deletes);

    // Verify: only positions in runs were remapped
    assertThat(result.totalRemapped()).isEqualTo(2);
    assertThat(result.skippedFilteredRows()).isEqualTo(2);

    List<PositionDeleteRecord> remapped = result.remappedDeletes().get("/path/to/target.parquet");
    assertThat(remapped).hasSize(2);

    // Position 10 stays at 10 (first run: offset 0)
    assertThat(remapped.stream().anyMatch(d -> d.position() == 10L)).isTrue();

    // Position 80 maps to 55 (second run: 80 - 75 + 50 = 55)
    assertThat(remapped.stream().anyMatch(d -> d.position() == 55L)).isTrue();
  }

  @Test
  public void testRemapDuplicateDeletes() {
    // Setup: Create compaction map
    CompactionMapBuilder mapBuilder = new CompactionMapBuilder(1L, 2L);
    mapBuilder
        .addFileMapping("/path/to/source.parquet", "/path/to/target.parquet")
        .addRun(0, 0, 100);
    CompactionMap compactionMap = mapBuilder.build();

    // Create duplicate deletes (same file, same position)
    List<PositionDeleteRecord> deletes =
        ImmutableList.of(
            new PositionDeleteRecord("/path/to/source.parquet", 10L, null, null),
            new PositionDeleteRecord("/path/to/source.parquet", 10L, null, null),
            new PositionDeleteRecord("/path/to/source.parquet", 20L, null, null),
            new PositionDeleteRecord("/path/to/source.parquet", 10L, null, null));

    // Remap
    DeleteManifestRemapper remapper = new DeleteManifestRemapper(compactionMap);
    RemappingResult result = remapper.remapDeletesWithMetrics(deletes);

    // Verify: duplicates were removed
    assertThat(result.totalRemapped()).isEqualTo(2); // Only 2 unique positions
    assertThat(result.duplicatesRemoved()).isEqualTo(2); // 2 duplicates removed

    List<PositionDeleteRecord> remapped = result.remappedDeletes().get("/path/to/target.parquet");
    assertThat(remapped).hasSize(2);
  }

  @Test
  public void testRemapEmptyInput() {
    // Setup: Create compaction map
    CompactionMapBuilder mapBuilder = new CompactionMapBuilder(1L, 2L);
    mapBuilder
        .addFileMapping("/path/to/source.parquet", "/path/to/target.parquet")
        .addRun(0, 0, 100);
    CompactionMap compactionMap = mapBuilder.build();

    // Empty deletes list
    List<PositionDeleteRecord> deletes = ImmutableList.of();

    // Remap
    DeleteManifestRemapper remapper = new DeleteManifestRemapper(compactionMap);
    RemappingResult result = remapper.remapDeletesWithMetrics(deletes);

    // Verify: empty result
    assertThat(result.totalRemapped()).isEqualTo(0);
    assertThat(result.totalSkipped()).isEqualTo(0);
    assertThat(result.hasRemappedDeletes()).isFalse();
    assertThat(result.remappedDeletes()).isEmpty();
  }

  @Test
  public void testRemapLargePositions() {
    // Setup: Create compaction map with large values
    CompactionMapBuilder mapBuilder = new CompactionMapBuilder(1L, 2L);
    mapBuilder
        .addFileMapping("/path/to/source.parquet", "/path/to/target.parquet")
        .addRun(0, 0, Long.MAX_VALUE / 2);
    CompactionMap compactionMap = mapBuilder.build();

    // Create delete with large position
    long largePosition = Long.MAX_VALUE / 4;
    List<PositionDeleteRecord> deletes =
        ImmutableList.of(
            new PositionDeleteRecord("/path/to/source.parquet", largePosition, null, null));

    // Remap
    DeleteManifestRemapper remapper = new DeleteManifestRemapper(compactionMap);
    RemappingResult result = remapper.remapDeletesWithMetrics(deletes);

    // Verify: large position was remapped correctly
    assertThat(result.totalRemapped()).isEqualTo(1);
    assertThat(result.remappedDeletes().get("/path/to/target.parquet").get(0).position())
        .isEqualTo(largePosition);
  }

  @Test
  public void testRemapMultipleFilesToSameTarget() {
    // Setup: Multiple source files mapping to same target
    CompactionMapBuilder mapBuilder = new CompactionMapBuilder(1L, 2L);
    mapBuilder
        .addFileMapping("/path/to/source1.parquet", "/path/to/target.parquet")
        .addRun(0, 0, 100);
    mapBuilder
        .addFileMapping("/path/to/source2.parquet", "/path/to/target.parquet")
        .addRun(0, 100, 100);
    CompactionMap compactionMap = mapBuilder.build();

    // Create deletes from both source files
    List<PositionDeleteRecord> deletes =
        ImmutableList.of(
            new PositionDeleteRecord("/path/to/source1.parquet", 10L, null, null),
            new PositionDeleteRecord("/path/to/source2.parquet", 20L, null, null));

    // Remap
    DeleteManifestRemapper remapper = new DeleteManifestRemapper(compactionMap);
    RemappingResult result = remapper.remapDeletesWithMetrics(deletes);

    // Verify: both deletes mapped to same target
    assertThat(result.totalRemapped()).isEqualTo(2);
    assertThat(result.remappedDeletes()).hasSize(1); // Only one target file

    List<PositionDeleteRecord> remapped = result.remappedDeletes().get("/path/to/target.parquet");
    assertThat(remapped).hasSize(2);

    // source1 position 10 stays at 10
    assertThat(remapped.stream().anyMatch(d -> d.position() == 10L)).isTrue();

    // source2 position 20 maps to 120 (20 + 100 offset)
    assertThat(remapped.stream().anyMatch(d -> d.position() == 120L)).isTrue();
  }

  @Test
  public void testRemappingResultBuilder() {
    // Test the builder directly
    RemappingResult result =
        RemappingResult.builder()
            .remappedDeletes(
                Map.of(
                    "/path/to/target.parquet",
                    ImmutableList.of(
                        new PositionDeleteRecord("/path/to/target.parquet", 10L, null, null))))
            .skippedNotCompacted(5)
            .skippedFilteredRows(3)
            .skippedInvalidPositions(2)
            .duplicatesRemoved(1)
            .build();

    assertThat(result.totalRemapped()).isEqualTo(1);
    assertThat(result.totalSkipped()).isEqualTo(10); // 5 + 3 + 2
    assertThat(result.skippedNotCompacted()).isEqualTo(5);
    assertThat(result.skippedFilteredRows()).isEqualTo(3);
    assertThat(result.skippedInvalidPositions()).isEqualTo(2);
    assertThat(result.duplicatesRemoved()).isEqualTo(1);
    assertThat(result.hasRemappedDeletes()).isTrue();
    assertThat(result.hasSkippedDeletes()).isTrue();
    assertThat(result.toString()).contains("totalRemapped");
  }

  @Test
  public void testRemappingResultEmpty() {
    RemappingResult empty = RemappingResult.empty();

    assertThat(empty.totalRemapped()).isEqualTo(0);
    assertThat(empty.totalSkipped()).isEqualTo(0);
    assertThat(empty.hasRemappedDeletes()).isFalse();
    assertThat(empty.hasSkippedDeletes()).isFalse();
    assertThat(empty.remappedDeletes()).isEmpty();
  }

  @Test
  public void testRemapPositionAtBoundary() {
    // Test positions at exact run boundaries
    CompactionMapBuilder mapBuilder = new CompactionMapBuilder(1L, 2L);
    mapBuilder
        .addFileMapping("/path/to/source.parquet", "/path/to/target.parquet")
        .addRun(0, 0, 50)
        .addRun(50, 50, 50);
    CompactionMap compactionMap = mapBuilder.build();

    // Positions at exact boundaries
    List<PositionDeleteRecord> deletes =
        ImmutableList.of(
            new PositionDeleteRecord(
                "/path/to/source.parquet", 0L, null, null), // Start of first run
            new PositionDeleteRecord(
                "/path/to/source.parquet", 49L, null, null), // End of first run
            new PositionDeleteRecord(
                "/path/to/source.parquet", 50L, null, null), // Start of second run
            new PositionDeleteRecord(
                "/path/to/source.parquet", 99L, null, null)); // End of second run

    // Remap
    DeleteManifestRemapper remapper = new DeleteManifestRemapper(compactionMap);
    RemappingResult result = remapper.remapDeletesWithMetrics(deletes);

    // All positions should be remapped
    assertThat(result.totalRemapped()).isEqualTo(4);
    assertThat(result.totalSkipped()).isEqualTo(0);

    List<PositionDeleteRecord> remapped = result.remappedDeletes().get("/path/to/target.parquet");
    List<Long> positions =
        remapped.stream()
            .map(PositionDeleteRecord::position)
            .sorted()
            .collect(java.util.stream.Collectors.toList());
    assertThat(positions).containsExactly(0L, 49L, 50L, 99L);
  }

  @Test
  public void testRemapPreservesPartitionAndRowData() {
    CompactionMapBuilder mapBuilder = new CompactionMapBuilder(1L, 2L);
    mapBuilder
        .addFileMapping("/path/to/source.parquet", "/path/to/target.parquet")
        .addRun(0, 0, 100);
    CompactionMap compactionMap = mapBuilder.build();

    // Create delete with partition and row data
    StructLike partitionData = new TestPartitionData("value1");
    StructLike rowData = new TestRowData(42, "test");

    List<PositionDeleteRecord> deletes =
        ImmutableList.of(
            new PositionDeleteRecord("/path/to/source.parquet", 10L, partitionData, rowData));

    // Remap
    DeleteManifestRemapper remapper = new DeleteManifestRemapper(compactionMap);
    RemappingResult result = remapper.remapDeletesWithMetrics(deletes);

    // Verify partition and row data preserved
    assertThat(result.totalRemapped()).isEqualTo(1);
    PositionDeleteRecord remapped = result.remappedDeletes().get("/path/to/target.parquet").get(0);
    assertThat(remapped.partitionData()).isEqualTo(partitionData);
    assertThat(remapped.rowData()).isEqualTo(rowData);
  }

  // Test helpers for partition and row data
  private static class TestPartitionData implements StructLike {
    private final String value;

    TestPartitionData(String value) {
      this.value = value;
    }

    @Override
    public int size() {
      return 1;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int pos, Class<T> javaClass) {
      return (T) value;
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TestPartitionData)) return false;
      return value.equals(((TestPartitionData) o).value);
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }
  }

  private static class TestRowData implements StructLike {
    private final int id;
    private final String data;

    TestRowData(int id, String data) {
      this.id = id;
      this.data = data;
    }

    @Override
    public int size() {
      return 2;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int pos, Class<T> javaClass) {
      if (pos == 0) return (T) Integer.valueOf(id);
      return (T) data;
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TestRowData)) return false;
      TestRowData that = (TestRowData) o;
      return id == that.id && data.equals(that.data);
    }

    @Override
    public int hashCode() {
      return 31 * id + data.hashCode();
    }
  }
}
