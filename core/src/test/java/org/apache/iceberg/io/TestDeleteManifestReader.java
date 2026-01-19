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

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PositionDeleteRecord;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestDeleteManifestReader {

  private static final Schema TEST_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  private DeleteManifestReader reader;
  private InMemoryFileIO fileIO;

  @TempDir Path temp;

  @BeforeEach
  public void setupReader() {
    fileIO = new InMemoryFileIO();
    reader = new DeleteManifestReader(fileIO);
  }

  @Test
  public void testReadSimplePositionDeletes() throws IOException {
    String dataFilePath = "data-file-1.parquet";

    // Write position deletes
    DeleteFile deleteFile =
        writePositionDeletes("test-deletes", dataFilePath, ImmutableList.of(10L, 20L, 30L));

    // Read position deletes
    List<PositionDeleteRecord> records = reader.readPositionDeletes(deleteFile);

    // Verify
    assertThat(records).hasSize(3);
    assertThat(records.get(0).dataFilePath()).isEqualTo(dataFilePath);
    assertThat(records.get(0).position()).isEqualTo(10L);
    assertThat(records.get(1).position()).isEqualTo(20L);
    assertThat(records.get(2).position()).isEqualTo(30L);
    assertThat(records).allMatch(r -> r.partitionData() == null);
    assertThat(records).allMatch(r -> r.rowData() == null);
  }

  @Test
  public void testFilterByReferencedFiles() throws IOException {
    String path1 = "data-file-1.parquet";
    String path2 = "data-file-2.parquet";
    String path3 = "data-file-3.parquet";

    // Write deletes for files 1 and 2
    DeleteFile deleteFile1 = writePositionDeletes("deletes-1", path1, ImmutableList.of(5L, 10L));
    DeleteFile deleteFile2 = writePositionDeletes("deletes-2", path2, ImmutableList.of(15L, 20L));

    // Read all deletes
    List<PositionDeleteRecord> allRecords = Lists.newArrayList();
    allRecords.addAll(reader.readPositionDeletes(deleteFile1));
    allRecords.addAll(reader.readPositionDeletes(deleteFile2));

    assertThat(allRecords).hasSize(4);

    // Filter by file 1 only
    Set<String> file1Set = ImmutableSet.of(path1);
    List<PositionDeleteRecord> filtered1 =
        DeleteManifestReader.filterByReferencedFiles(allRecords, file1Set);
    assertThat(filtered1).hasSize(2);
    assertThat(filtered1).allMatch(r -> r.dataFilePath().equals(path1));

    // Filter by file 2 only
    Set<String> file2Set = ImmutableSet.of(path2);
    List<PositionDeleteRecord> filtered2 =
        DeleteManifestReader.filterByReferencedFiles(allRecords, file2Set);
    assertThat(filtered2).hasSize(2);
    assertThat(filtered2).allMatch(r -> r.dataFilePath().equals(path2));

    // Filter by file 3 (no deletes)
    Set<String> file3Set = ImmutableSet.of(path3);
    List<PositionDeleteRecord> filtered3 =
        DeleteManifestReader.filterByReferencedFiles(allRecords, file3Set);
    assertThat(filtered3).isEmpty();

    // Filter by files 1 and 2
    Set<String> bothFiles = ImmutableSet.of(path1, path2);
    List<PositionDeleteRecord> filteredBoth =
        DeleteManifestReader.filterByReferencedFiles(allRecords, bothFiles);
    assertThat(filteredBoth).hasSize(4);
  }

  @Test
  public void testReadAndFilterPositionDeletes() throws IOException {
    String path1 = "data-file-1.parquet";
    String path2 = "data-file-2.parquet";

    // Write deletes
    DeleteFile deleteFile1 = writePositionDeletes("deletes-1", path1, ImmutableList.of(1L, 2L, 3L));
    DeleteFile deleteFile2 = writePositionDeletes("deletes-2", path2, ImmutableList.of(4L, 5L));

    // Read and filter for file 1
    List<DeleteFile> deleteFiles = Lists.newArrayList(deleteFile1, deleteFile2);
    Set<String> targetFiles = ImmutableSet.of(path1);

    List<PositionDeleteRecord> records =
        reader.readAndFilterPositionDeletes(deleteFiles, targetFiles);

    assertThat(records).hasSize(3);
    assertThat(records).allMatch(r -> r.dataFilePath().equals(path1));
    assertThat(records.get(0).position()).isEqualTo(1L);
    assertThat(records.get(1).position()).isEqualTo(2L);
    assertThat(records.get(2).position()).isEqualTo(3L);
  }

  @Test
  public void testEmptyDeleteFile() throws IOException {
    String dataFilePath = "data-file-1.parquet";

    // Write empty deletes (no positions)
    DeleteFile deleteFile = writePositionDeletes("empty-deletes", dataFilePath, ImmutableList.of());

    // Read position deletes
    List<PositionDeleteRecord> records = reader.readPositionDeletes(deleteFile);

    // Verify empty
    assertThat(records).isEmpty();
  }

  @Test
  public void testFilterWithEmptySet() throws IOException {
    String path = "data-file-1.parquet";

    DeleteFile deleteFile = writePositionDeletes("deletes", path, ImmutableList.of(1L, 2L));

    List<PositionDeleteRecord> records = reader.readPositionDeletes(deleteFile);
    assertThat(records).hasSize(2);

    // Filter with empty set
    Set<String> emptySet = ImmutableSet.of();
    List<PositionDeleteRecord> filtered =
        DeleteManifestReader.filterByReferencedFiles(records, emptySet);

    assertThat(filtered).isEmpty();
  }

  @Test
  public void testMultipleDeletesPerFile() throws IOException {
    String path = "data-file-1.parquet";

    // Write two separate delete files for the same data file
    DeleteFile deleteFile1 = writePositionDeletes("deletes-1", path, ImmutableList.of(1L, 2L));
    DeleteFile deleteFile2 = writePositionDeletes("deletes-2", path, ImmutableList.of(10L, 20L));

    // Read both delete files
    List<DeleteFile> deleteFiles = Lists.newArrayList(deleteFile1, deleteFile2);
    Set<String> targetFiles = ImmutableSet.of(path);

    List<PositionDeleteRecord> records =
        reader.readAndFilterPositionDeletes(deleteFiles, targetFiles);

    // Should have all 4 deletes
    assertThat(records).hasSize(4);
    assertThat(records)
        .extracting(PositionDeleteRecord::position)
        .containsExactlyInAnyOrder(1L, 2L, 10L, 20L);
  }

  @Test
  public void testReadPositionDeletesWithRowData() throws IOException {
    String dataFilePath = "data-file-1.parquet";

    // Write position deletes with row data
    DeleteFile deleteFile =
        writePositionDeletesWithRowData(
            "deletes-with-rows", dataFilePath, ImmutableList.of(10L, 20L));

    // Read position deletes
    List<PositionDeleteRecord> records = reader.readPositionDeletes(deleteFile);

    // Verify - Phase 1 limitation: row data is not preserved
    assertThat(records).hasSize(2);
    assertThat(records.get(0).dataFilePath()).isEqualTo(dataFilePath);
    assertThat(records.get(0).position()).isEqualTo(10L);
    assertThat(records.get(0).rowData()).isNull(); // Phase 1: row data not preserved
    assertThat(records.get(1).position()).isEqualTo(20L);
    assertThat(records.get(1).rowData()).isNull(); // Phase 1: row data not preserved
  }

  // Helper methods

  private DeleteFile writePositionDeletes(
      String filename, String dataFilePath, List<Long> positions) throws IOException {
    OutputFile out = fileIO.newOutputFile(filename);

    // Write position deletes without row data
    PositionDeleteWriter<Void> deleteWriter =
        Avro.writeDeletes(out)
            .createWriterFunc(DataWriter::create)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .buildPositionWriter();

    try (PositionDeleteWriter<Void> writer = deleteWriter) {
      PositionDelete<Void> delete = PositionDelete.create();
      for (Long position : positions) {
        writer.write(delete.set(dataFilePath, position));
      }
    }

    return deleteWriter.toDeleteFile();
  }

  private DeleteFile writePositionDeletesWithRowData(
      String filename, String dataFilePath, List<Long> positions) throws IOException {
    OutputFile out = fileIO.newOutputFile(filename);

    // Write position deletes with row data
    PositionDeleteWriter<Record> deleteWriter =
        Avro.writeDeletes(out)
            .createWriterFunc(DataWriter::create)
            .overwrite()
            .rowSchema(TEST_SCHEMA)
            .withSpec(PartitionSpec.unpartitioned())
            .buildPositionWriter();

    try (PositionDeleteWriter<Record> writer = deleteWriter) {
      PositionDelete<Record> delete = PositionDelete.create();
      for (int i = 0; i < positions.size(); i++) {
        Long position = positions.get(i);
        // Create sample row data
        GenericRecord rowData = GenericRecord.create(TEST_SCHEMA);
        Record row = rowData.copy(ImmutableMap.of("id", i + 1, "data", "row-" + i));
        writer.write(delete.set(dataFilePath, position, row));
      }
    }

    return deleteWriter.toDeleteFile();
  }
}
