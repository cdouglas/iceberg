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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestRemappedDVWriter {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir private File tempDir;

  private org.apache.iceberg.hadoop.HadoopTables tables;
  private Table table;

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
  }

  @Test
  public void testWriteSingleTargetFile() throws IOException {
    RemappedDVWriter writer = new RemappedDVWriter(table, table.spec(), null);

    // Create remapped positions for a single target file
    Map<String, Set<Long>> remappedPositions = new HashMap<>();
    Set<Long> positions = new HashSet<>();
    positions.add(10L);
    positions.add(20L);
    positions.add(30L);
    remappedPositions.put("s3://bucket/target1.parquet", positions);

    List<DeleteFile> dvs = writer.writeRemappedDVs(remappedPositions);

    // Should write exactly one DV
    assertThat(dvs).hasSize(1);

    DeleteFile dv = dvs.get(0);
    assertThat(dv.referencedDataFile()).isEqualTo("s3://bucket/target1.parquet");
    assertThat(dv.recordCount()).isEqualTo(3L);
  }

  @Test
  public void testWriteMultipleTargetFiles() throws IOException {
    RemappedDVWriter writer = new RemappedDVWriter(table, table.spec(), null);

    // Create remapped positions for three target files
    Map<String, Set<Long>> remappedPositions = new HashMap<>();

    Set<Long> positions1 = new HashSet<>();
    positions1.add(5L);
    positions1.add(10L);
    remappedPositions.put("s3://bucket/target1.parquet", positions1);

    Set<Long> positions2 = new HashSet<>();
    positions2.add(15L);
    positions2.add(20L);
    positions2.add(25L);
    remappedPositions.put("s3://bucket/target2.parquet", positions2);

    Set<Long> positions3 = new HashSet<>();
    positions3.add(30L);
    remappedPositions.put("s3://bucket/target3.parquet", positions3);

    List<DeleteFile> dvs = writer.writeRemappedDVs(remappedPositions);

    // Should write three DVs
    assertThat(dvs).hasSize(3);

    // Verify each DV references the correct target file
    Set<String> referencedFiles = new HashSet<>();
    for (DeleteFile dv : dvs) {
      referencedFiles.add(dv.referencedDataFile());
    }

    assertThat(referencedFiles)
        .containsExactlyInAnyOrder(
            "s3://bucket/target1.parquet",
            "s3://bucket/target2.parquet",
            "s3://bucket/target3.parquet");
  }

  @Test
  public void testSkipEmptyPositions() throws IOException {
    RemappedDVWriter writer = new RemappedDVWriter(table, table.spec(), null);

    // Create remapped positions with one target having empty positions
    Map<String, Set<Long>> remappedPositions = new HashMap<>();

    Set<Long> positions1 = new HashSet<>();
    positions1.add(10L);
    remappedPositions.put("s3://bucket/target1.parquet", positions1);

    // Empty positions - should be skipped
    Set<Long> emptyPositions = new HashSet<>();
    remappedPositions.put("s3://bucket/target2.parquet", emptyPositions);

    Set<Long> positions3 = new HashSet<>();
    positions3.add(20L);
    remappedPositions.put("s3://bucket/target3.parquet", positions3);

    List<DeleteFile> dvs = writer.writeRemappedDVs(remappedPositions);

    // Should only write two DVs (skip the empty one)
    assertThat(dvs).hasSize(2);

    Set<String> referencedFiles = new HashSet<>();
    for (DeleteFile dv : dvs) {
      referencedFiles.add(dv.referencedDataFile());
    }

    assertThat(referencedFiles)
        .containsExactlyInAnyOrder("s3://bucket/target1.parquet", "s3://bucket/target3.parquet");
  }

  @Test
  public void testAllEmptyPositions() throws IOException {
    RemappedDVWriter writer = new RemappedDVWriter(table, table.spec(), null);

    // Create remapped positions with all empty position sets
    Map<String, Set<Long>> remappedPositions = new HashMap<>();
    remappedPositions.put("s3://bucket/target1.parquet", new HashSet<>());
    remappedPositions.put("s3://bucket/target2.parquet", new HashSet<>());
    remappedPositions.put("s3://bucket/target3.parquet", new HashSet<>());

    List<DeleteFile> dvs = writer.writeRemappedDVs(remappedPositions);

    // Should write no DVs (all empty)
    assertThat(dvs).isEmpty();
  }

  @Test
  public void testVerifyWrittenDVs() throws IOException {
    RemappedDVWriter writer = new RemappedDVWriter(table, table.spec(), null);

    // Create remapped positions for two target files
    Map<String, Set<Long>> remappedPositions = new HashMap<>();

    Set<Long> positions1 = new HashSet<>();
    positions1.add(5L);
    positions1.add(10L);
    positions1.add(15L);
    remappedPositions.put("s3://bucket/target1.parquet", positions1);

    Set<Long> positions2 = new HashSet<>();
    positions2.add(20L);
    positions2.add(25L);
    remappedPositions.put("s3://bucket/target2.parquet", positions2);

    List<DeleteFile> dvs = writer.writeRemappedDVs(remappedPositions);

    // Read back each DV and verify positions
    DVPositionReader reader = new DVPositionReader(table.io());

    for (DeleteFile dv : dvs) {
      String targetFile = dv.referencedDataFile();
      Set<Long> expectedPositions = remappedPositions.get(targetFile);

      Set<Long> readPositions = new HashSet<>();
      try (CloseableIterable<Long> positions = reader.readDeletedPositions(dv)) {
        positions.forEach(readPositions::add);
      }

      // Verify positions match
      assertThat(readPositions).containsExactlyInAnyOrderElementsOf(expectedPositions);
    }
  }
}
