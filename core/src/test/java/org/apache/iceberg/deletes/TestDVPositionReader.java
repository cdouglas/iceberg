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
package org.apache.iceberg.deletes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestDVPositionReader {
  private static final org.apache.iceberg.Schema SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir private File tempDir;

  private org.apache.iceberg.hadoop.HadoopTables tables;
  private org.apache.iceberg.Table table;
  private OutputFileFactory fileFactory;
  private DVPositionReader reader;

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
    reader = new DVPositionReader(table.io());
  }

  @Test
  public void testReadEmptyDV() throws IOException {
    // Note: BaseDVFileWriter optimizes empty DVs by not creating any files
    // So we test reading a DV with a single position and verify it's not empty
    String dataFilePath = "file:/data/file1.parquet";
    DeleteFile dv = writeDV(dataFilePath, 0L); // Single position at 0

    try (CloseableIterable<Long> positions = reader.readDeletedPositions(dv)) {
      List<Long> positionList = Lists.newArrayList(positions);
      assertThat(positionList).hasSize(1).containsExactly(0L);
    }
  }

  @Test
  public void testReadSinglePosition() throws IOException {
    String dataFilePath = "file:/data/file1.parquet";
    DeleteFile dv = writeDV(dataFilePath, 5L);

    try (CloseableIterable<Long> positions = reader.readDeletedPositions(dv)) {
      List<Long> positionList = Lists.newArrayList(positions);
      assertThat(positionList).containsExactly(5L);
    }
  }

  @Test
  public void testReadMultiplePositions() throws IOException {
    String dataFilePath = "file:/data/file1.parquet";
    DeleteFile dv = writeDV(dataFilePath, 0L, 1L, 2L, 5L, 10L);

    try (CloseableIterable<Long> positions = reader.readDeletedPositions(dv)) {
      List<Long> positionList = Lists.newArrayList(positions);
      assertThat(positionList).containsExactly(0L, 1L, 2L, 5L, 10L);
    }
  }

  @Test
  public void testReadSparsePositions() throws IOException {
    // Test positions with large gaps
    String dataFilePath = "file:/data/file1.parquet";
    DeleteFile dv = writeDV(dataFilePath, 0L, 5L, 100L, 1000L);

    try (CloseableIterable<Long> positions = reader.readDeletedPositions(dv)) {
      List<Long> positionList = Lists.newArrayList(positions);
      assertThat(positionList).containsExactly(0L, 5L, 100L, 1000L);
    }
  }

  @Test
  public void testReadDensePositions() throws IOException {
    // Test consecutive deleted positions (0-99)
    String dataFilePath = "file:/data/file1.parquet";
    List<Long> expectedPositions = Lists.newArrayList();
    for (long i = 0; i < 100; i++) {
      expectedPositions.add(i);
    }

    DeleteFile dv = writeDV(dataFilePath, expectedPositions.toArray(new Long[0]));

    try (CloseableIterable<Long> positions = reader.readDeletedPositions(dv)) {
      List<Long> positionList = Lists.newArrayList(positions);
      assertThat(positionList).isEqualTo(expectedPositions);
    }
  }

  @Test
  public void testReadLargePositions() throws IOException {
    // Test positions > Integer.MAX_VALUE (verify 64-bit support)
    String dataFilePath = "file:/data/file1.parquet";
    long largePos1 = (long) Integer.MAX_VALUE + 1;
    long largePos2 = (long) Integer.MAX_VALUE + 1000;

    DeleteFile dv = writeDV(dataFilePath, 0L, largePos1, largePos2);

    try (CloseableIterable<Long> positions = reader.readDeletedPositions(dv)) {
      List<Long> positionList = Lists.newArrayList(positions);
      assertThat(positionList).containsExactly(0L, largePos1, largePos2);
    }
  }

  @Test
  public void testReferencedDataFile() throws IOException {
    String dataFilePath = "file:/data/file1.parquet";
    DeleteFile dv = writeDV(dataFilePath, 0L, 1L, 2L);

    String referenced = reader.referencedDataFile(dv);
    assertThat(referenced).isEqualTo(dataFilePath);
  }

  @Test
  public void testInvalidDVThrows() {
    // Create a position delete file (not a DV) and verify it throws
    // For this test, we'll create a DeleteFile that is not PUFFIN format
    DeleteFile nonDV =
        org.apache.iceberg.FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withFormat(FileFormat.PARQUET) // Not PUFFIN
            .withPath("file:/data/pos-deletes.parquet")
            .withFileSizeInBytes(100L)
            .withRecordCount(10L)
            .build();

    assertThatThrownBy(() -> reader.readDeletedPositions(nonDV))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Not a deletion vector")
        .hasMessageContaining("PARQUET");
  }

  @Test
  public void testPositionsInAscendingOrder() throws IOException {
    // Write positions in random order, verify they come back sorted
    String dataFilePath = "file:/data/file1.parquet";
    DeleteFile dv = writeDV(dataFilePath, 100L, 5L, 50L, 1L, 200L, 10L);

    try (CloseableIterable<Long> positions = reader.readDeletedPositions(dv)) {
      List<Long> positionList = Lists.newArrayList(positions);

      // Verify sorted in ascending order
      assertThat(positionList).isSorted();
      assertThat(positionList).containsExactly(1L, 5L, 10L, 50L, 100L, 200L);
    }
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
