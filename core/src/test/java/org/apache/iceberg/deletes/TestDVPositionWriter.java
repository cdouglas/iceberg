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

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestDVPositionWriter {
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
  public void testWriteSinglePosition() throws IOException {
    DVPositionWriter writer =
        new DVPositionWriter(fileFactory, table.spec(), null, "s3://bucket/data-file.parquet");

    Set<Long> positions = ImmutableSet.of(42L);
    DeleteFile dv = writer.writePositions(positions);

    // Verify DV was written
    assertThat(dv).isNotNull();
    assertThat(dv.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(dv.referencedDataFile()).isEqualTo("s3://bucket/data-file.parquet");
    assertThat(dv.recordCount()).isEqualTo(1L);
  }

  @Test
  public void testWriteMultiplePositions() throws IOException {
    DVPositionWriter writer =
        new DVPositionWriter(fileFactory, table.spec(), null, "s3://bucket/data-file.parquet");

    Set<Long> positions = ImmutableSet.of(10L, 20L, 30L, 40L, 50L);
    DeleteFile dv = writer.writePositions(positions);

    // Verify DV was written with correct count
    assertThat(dv).isNotNull();
    assertThat(dv.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(dv.referencedDataFile()).isEqualTo("s3://bucket/data-file.parquet");
    assertThat(dv.recordCount()).isEqualTo(5L);
  }

  @Test
  public void testWriteEmptyPositions() throws IOException {
    DVPositionWriter writer =
        new DVPositionWriter(fileFactory, table.spec(), null, "s3://bucket/data-file.parquet");

    Set<Long> positions = ImmutableSet.of();
    DeleteFile dv = writer.writePositions(positions);

    // Empty positions should return null (no DV needed)
    assertThat(dv).isNull();
  }

  @Test
  public void testWriteLargePositions() throws IOException {
    DVPositionWriter writer =
        new DVPositionWriter(fileFactory, table.spec(), null, "s3://bucket/data-file.parquet");

    // Test positions larger than Integer.MAX_VALUE
    long largePos1 = (long) Integer.MAX_VALUE + 100L;
    long largePos2 = (long) Integer.MAX_VALUE + 1000L;
    Set<Long> positions = ImmutableSet.of(largePos1, largePos2);

    DeleteFile dv = writer.writePositions(positions);

    // Verify large positions are handled correctly
    assertThat(dv).isNotNull();
    assertThat(dv.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(dv.referencedDataFile()).isEqualTo("s3://bucket/data-file.parquet");
    assertThat(dv.recordCount()).isEqualTo(2L);
  }

  @Test
  public void testReadWrittenDV() throws IOException {
    DVPositionWriter writer =
        new DVPositionWriter(fileFactory, table.spec(), null, "s3://bucket/data-file.parquet");

    Set<Long> originalPositions = ImmutableSet.of(5L, 10L, 15L, 20L, 25L);
    DeleteFile dv = writer.writePositions(originalPositions);

    // Read back the DV and verify positions
    DVPositionReader reader = new DVPositionReader(table.io());
    Set<Long> readPositions = new HashSet<>();

    try (CloseableIterable<Long> positions = reader.readDeletedPositions(dv)) {
      positions.forEach(readPositions::add);
    }

    // Verify all positions were written and read correctly
    assertThat(readPositions).containsExactlyInAnyOrderElementsOf(originalPositions);
  }
}
