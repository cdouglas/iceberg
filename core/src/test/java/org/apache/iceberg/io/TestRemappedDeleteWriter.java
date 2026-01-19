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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PositionDeleteRecord;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestRemappedDeleteWriter {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  @TempDir private File tableDir;

  private Table table;

  @BeforeEach
  public void setupTable() {
    table = TestTables.create(tableDir, "test", SCHEMA, PartitionSpec.unpartitioned(), 2);
  }

  @AfterEach
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @Test
  public void testWriteSimplePositionDeletes() throws IOException {
    Map<String, List<PositionDeleteRecord>> remappedDeletes = Maps.newHashMap();
    remappedDeletes.put(
        "target.parquet",
        ImmutableList.of(
            new PositionDeleteRecord("target.parquet", 10L),
            new PositionDeleteRecord("target.parquet", 20L),
            new PositionDeleteRecord("target.parquet", 30L)));

    List<DeleteFile> deleteFiles;
    try (RemappedDeleteWriter writer = new RemappedDeleteWriter(table)) {
      deleteFiles = writer.writeDeletes(remappedDeletes);
    }

    assertThat(deleteFiles).hasSize(1);
    DeleteFile deleteFile = deleteFiles.get(0);

    assertThat(deleteFile.content()).isEqualTo(FileContent.POSITION_DELETES);
    assertThat(deleteFile.format()).isEqualTo(FileFormat.AVRO);
    assertThat(deleteFile.recordCount()).isEqualTo(3);
    assertThat(deleteFile.fileSizeInBytes()).isGreaterThan(0);
  }

  @Test
  public void testWriteDeletesSortedByPosition() throws IOException {
    // Create deletes in unsorted order
    Map<String, List<PositionDeleteRecord>> remappedDeletes = Maps.newHashMap();
    remappedDeletes.put(
        "target.parquet",
        Lists.newArrayList(
            new PositionDeleteRecord("target.parquet", 30L),
            new PositionDeleteRecord("target.parquet", 10L),
            new PositionDeleteRecord("target.parquet", 20L)));

    List<DeleteFile> deleteFiles;
    try (RemappedDeleteWriter writer = new RemappedDeleteWriter(table)) {
      deleteFiles = writer.writeDeletes(remappedDeletes);
    }

    assertThat(deleteFiles).hasSize(1);

    // Read back and verify sorted order
    DeleteFile deleteFile = deleteFiles.get(0);
    List<Record> deletedRecords = readDeleteFile(deleteFile);

    assertThat(deletedRecords).hasSize(3);
    // Verify sorted by position
    assertThat(deletedRecords.get(0).get(1, Long.class)).isEqualTo(10L);
    assertThat(deletedRecords.get(1).get(1, Long.class)).isEqualTo(20L);
    assertThat(deletedRecords.get(2).get(1, Long.class)).isEqualTo(30L);
  }

  @Test
  public void testWriteMultipleTargetFiles() throws IOException {
    Map<String, List<PositionDeleteRecord>> remappedDeletes = Maps.newHashMap();
    remappedDeletes.put(
        "target1.parquet",
        ImmutableList.of(
            new PositionDeleteRecord("target1.parquet", 10L),
            new PositionDeleteRecord("target1.parquet", 20L)));
    remappedDeletes.put(
        "target2.parquet",
        ImmutableList.of(
            new PositionDeleteRecord("target2.parquet", 5L),
            new PositionDeleteRecord("target2.parquet", 15L),
            new PositionDeleteRecord("target2.parquet", 25L)));

    List<DeleteFile> deleteFiles;
    try (RemappedDeleteWriter writer = new RemappedDeleteWriter(table)) {
      deleteFiles = writer.writeDeletes(remappedDeletes);
    }

    // Both targets go to same partition (unpartitioned table) so expect 1 file
    assertThat(deleteFiles).hasSize(1);

    // Total record count should be 5
    long totalRecords = deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum();
    assertThat(totalRecords).isEqualTo(5);
  }

  @Test
  public void testWriteEmptyDeleteList() throws IOException {
    Map<String, List<PositionDeleteRecord>> remappedDeletes = Maps.newHashMap();

    List<DeleteFile> deleteFiles;
    try (RemappedDeleteWriter writer = new RemappedDeleteWriter(table)) {
      deleteFiles = writer.writeDeletes(remappedDeletes);
    }

    assertThat(deleteFiles).isEmpty();
  }

  @Test
  public void testWriteEmptyDeletesForTarget() throws IOException {
    Map<String, List<PositionDeleteRecord>> remappedDeletes = Maps.newHashMap();
    remappedDeletes.put("target.parquet", Lists.newArrayList());

    List<DeleteFile> deleteFiles;
    try (RemappedDeleteWriter writer = new RemappedDeleteWriter(table)) {
      deleteFiles = writer.writeDeletes(remappedDeletes);
    }

    // Empty list should result in no files
    assertThat(deleteFiles).isEmpty();
  }

  @Test
  public void testDeleteFileMetrics() throws IOException {
    Map<String, List<PositionDeleteRecord>> remappedDeletes = Maps.newHashMap();
    remappedDeletes.put(
        "target.parquet",
        ImmutableList.of(
            new PositionDeleteRecord("target.parquet", 100L),
            new PositionDeleteRecord("target.parquet", 200L)));

    List<DeleteFile> deleteFiles;
    try (RemappedDeleteWriter writer = new RemappedDeleteWriter(table)) {
      deleteFiles = writer.writeDeletes(remappedDeletes);
    }

    assertThat(deleteFiles).hasSize(1);
    DeleteFile deleteFile = deleteFiles.get(0);

    // Verify metrics
    assertThat(deleteFile.recordCount()).isEqualTo(2);
    assertThat(deleteFile.fileSizeInBytes()).isGreaterThan(0);
    assertThat(deleteFile.content()).isEqualTo(FileContent.POSITION_DELETES);
  }

  @Test
  public void testIntegrationWriteReadVerify() throws IOException {
    String targetPath = "s3://bucket/table/data/target-file.parquet";

    Map<String, List<PositionDeleteRecord>> remappedDeletes = Maps.newHashMap();
    remappedDeletes.put(
        targetPath,
        ImmutableList.of(
            new PositionDeleteRecord(targetPath, 5L),
            new PositionDeleteRecord(targetPath, 50L),
            new PositionDeleteRecord(targetPath, 500L)));

    List<DeleteFile> deleteFiles;
    try (RemappedDeleteWriter writer = new RemappedDeleteWriter(table)) {
      deleteFiles = writer.writeDeletes(remappedDeletes);
    }

    assertThat(deleteFiles).hasSize(1);
    DeleteFile deleteFile = deleteFiles.get(0);

    // Read back the delete file
    List<Record> deletedRecords = readDeleteFile(deleteFile);

    // Verify content
    assertThat(deletedRecords).hasSize(3);

    // Records should be sorted by position
    assertThat(deletedRecords.get(0).get(0, CharSequence.class).toString()).isEqualTo(targetPath);
    assertThat(deletedRecords.get(0).get(1, Long.class)).isEqualTo(5L);
    assertThat(deletedRecords.get(1).get(0, CharSequence.class).toString()).isEqualTo(targetPath);
    assertThat(deletedRecords.get(1).get(1, Long.class)).isEqualTo(50L);
    assertThat(deletedRecords.get(2).get(0, CharSequence.class).toString()).isEqualTo(targetPath);
    assertThat(deletedRecords.get(2).get(1, Long.class)).isEqualTo(500L);
  }

  @Test
  public void testWrittenDeleteFilesMethod() throws IOException {
    Map<String, List<PositionDeleteRecord>> remappedDeletes = Maps.newHashMap();
    remappedDeletes.put(
        "target.parquet", ImmutableList.of(new PositionDeleteRecord("target.parquet", 10L)));

    RemappedDeleteWriter writer = new RemappedDeleteWriter(table);

    // Before writing, list should be empty
    assertThat(writer.writtenDeleteFiles()).isEmpty();

    // Write deletes
    writer.writeDeletes(remappedDeletes);

    // After writing, list should have files
    assertThat(writer.writtenDeleteFiles()).hasSize(1);

    writer.close();
  }

  private List<Record> readDeleteFile(DeleteFile deleteFile) throws IOException {
    Schema deleteSchema =
        new Schema(
            Types.NestedField.required(
                org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH.fieldId(),
                "file_path",
                Types.StringType.get()),
            Types.NestedField.required(
                org.apache.iceberg.MetadataColumns.DELETE_FILE_POS.fieldId(),
                "pos",
                Types.LongType.get()));

    InputFile inputFile = table.io().newInputFile(deleteFile.location());

    try (AvroIterable<Record> reader =
        Avro.read(inputFile)
            .project(deleteSchema)
            .createResolvingReader(PlannedDataReader::create)
            .build()) {
      return Lists.newArrayList(reader);
    }
  }
}
