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
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for CompactionConflictResolver.
 *
 * <p>These tests verify end-to-end conflict resolution: detecting conflicts, remapping deletes, and
 * writing new delete files.
 */
public class TestCompactionConflictResolver {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir public File temp;

  private InMemoryCatalog catalog;

  @BeforeEach
  public void setup() {
    catalog = new InMemoryCatalog();
    catalog.initialize("test", java.util.Collections.emptyMap());
    catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of("db"));
  }

  @Test
  public void testResolveNoConflicts() throws IOException {
    // Setup: Create table with data but no deletes
    TableIdentifier tableIdent = TableIdentifier.of("db", "no_conflicts");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add data file
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile).commit();

    // Create empty conflict info (no conflicts)
    DeleteConflictInfo conflicts = DeleteConflictInfo.empty();

    // Create compaction map
    CompactionMapBuilder mapBuilder = new CompactionMapBuilder(1L, 2L);
    mapBuilder
        .addFileMapping("/path/to/source.parquet", "/path/to/target.parquet")
        .addRun(0, 0, 100);
    CompactionMap compactionMap = mapBuilder.build();

    // Resolve
    CompactionConflictResolver resolver = new CompactionConflictResolver(table);
    DeleteManifestChanges changes = resolver.resolve(compactionMap, conflicts);

    // Verify no changes
    assertThat(changes.hasChanges()).isFalse();
    assertThat(changes.addedDeleteFiles()).isEmpty();
    assertThat(changes.totalDeletesRemapped()).isEqualTo(0);
  }

  @Test
  public void testResolveSimpleConflict() throws IOException {
    // Setup: Create table with data and deletes
    TableIdentifier tableIdent = TableIdentifier.of("db", "simple_conflict");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add data file
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Write actual position deletes to a file
    DeleteFile deleteFile = writePositionDeletes(table, "/path/to/source.parquet", 10L, 20L, 30L);
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Get conflict info
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());
    DeleteConflictInfo conflicts =
        detector.detectConflicts(
            org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet.of(
                "/path/to/source.parquet"));

    assertThat(conflicts.hasConflicts()).isTrue();

    // Create compaction map
    CompactionMapBuilder mapBuilder = new CompactionMapBuilder(1L, 2L);
    mapBuilder
        .addFileMapping("/path/to/source.parquet", "/path/to/target.parquet")
        .addRun(0, 0, 100);
    CompactionMap compactionMap = mapBuilder.build();

    // Resolve
    CompactionConflictResolver resolver = new CompactionConflictResolver(table);
    DeleteManifestChanges changes = resolver.resolve(compactionMap, conflicts);

    // Verify changes
    assertThat(changes.hasChanges()).isTrue();
    assertThat(changes.addedDeleteFiles()).hasSize(1);
    assertThat(changes.remappedDeleteFiles()).hasSize(1);
    assertThat(changes.totalDeletesRemapped()).isEqualTo(3);
    assertThat(changes.affectedDataFiles()).isEqualTo(1);
  }

  @Test
  public void testResolveMultipleDeleteFiles() throws IOException {
    // Setup: Create table with data and multiple delete files
    TableIdentifier tableIdent = TableIdentifier.of("db", "multiple_deletes");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add data files
    DataFile dataFile1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    DataFile dataFile2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Write position deletes for both files
    DeleteFile deleteFile1 = writePositionDeletes(table, "/path/to/source1.parquet", 5L, 15L);
    table.newRowDelta().addDeletes(deleteFile1).commit();

    DeleteFile deleteFile2 = writePositionDeletes(table, "/path/to/source2.parquet", 25L, 35L, 45L);
    table.newRowDelta().addDeletes(deleteFile2).commit();

    // Get conflict info
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());
    DeleteConflictInfo conflicts =
        detector.detectConflicts(
            org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet.of(
                "/path/to/source1.parquet", "/path/to/source2.parquet"));

    assertThat(conflicts.hasConflicts()).isTrue();
    assertThat(conflicts.deleteFileCount()).isEqualTo(2);

    // Create compaction map (merge two source files into one target)
    CompactionMapBuilder mapBuilder = new CompactionMapBuilder(1L, 2L);
    mapBuilder
        .addFileMapping("/path/to/source1.parquet", "/path/to/target.parquet")
        .addRun(0, 0, 100);
    mapBuilder
        .addFileMapping("/path/to/source2.parquet", "/path/to/target.parquet")
        .addRun(0, 100, 100);
    CompactionMap compactionMap = mapBuilder.build();

    // Resolve
    CompactionConflictResolver resolver = new CompactionConflictResolver(table);
    DeleteManifestChanges changes = resolver.resolve(compactionMap, conflicts);

    // Verify changes
    assertThat(changes.hasChanges()).isTrue();
    assertThat(changes.addedDeleteFiles()).isNotEmpty();
    assertThat(changes.remappedDeleteFiles()).hasSize(2);
    assertThat(changes.totalDeletesRemapped()).isEqualTo(5); // 2 + 3 deletes
    assertThat(changes.affectedDataFiles()).isEqualTo(2);
  }

  @Test
  public void testDeleteManifestChangesBuilder() {
    // Test the builder directly
    DeleteFile addedFile =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/new-deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(5)
            .build();

    DeleteFile remappedFile =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/old-deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(5)
            .build();

    DeleteManifestChanges changes =
        DeleteManifestChanges.builder()
            .addNewDeleteFile(addedFile)
            .addRemappedDeleteFile(remappedFile)
            .totalDeletesRemapped(10)
            .affectedDataFiles(2)
            .build();

    assertThat(changes.hasChanges()).isTrue();
    assertThat(changes.addedDeleteFiles()).containsExactly(addedFile);
    assertThat(changes.remappedDeleteFiles()).containsExactly(remappedFile);
    assertThat(changes.totalDeletesRemapped()).isEqualTo(10);
    assertThat(changes.affectedDataFiles()).isEqualTo(2);
    assertThat(changes.toString()).contains("addedDeleteFiles");
  }

  @Test
  public void testDeleteManifestChangesEmpty() {
    DeleteManifestChanges empty = DeleteManifestChanges.empty();

    assertThat(empty.hasChanges()).isFalse();
    assertThat(empty.addedDeleteFiles()).isEmpty();
    assertThat(empty.remappedDeleteFiles()).isEmpty();
    assertThat(empty.totalDeletesRemapped()).isEqualTo(0);
    assertThat(empty.affectedDataFiles()).isEqualTo(0);
  }

  @Test
  public void testDeleteManifestChangesAddMultiple() {
    List<DeleteFile> addedFiles =
        ImmutableList.of(
            FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofPositionDeletes()
                .withPath("/path/to/new1.parquet")
                .withFileSizeInBytes(100)
                .withRecordCount(5)
                .build(),
            FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofPositionDeletes()
                .withPath("/path/to/new2.parquet")
                .withFileSizeInBytes(100)
                .withRecordCount(5)
                .build());

    DeleteManifestChanges changes =
        DeleteManifestChanges.builder()
            .addNewDeleteFiles(addedFiles)
            .addDeletesRemapped(5)
            .addDeletesRemapped(3)
            .build();

    assertThat(changes.addedDeleteFiles()).hasSize(2);
    assertThat(changes.totalDeletesRemapped()).isEqualTo(8);
  }

  @Test
  public void testResolveForCompaction() throws IOException {
    // Setup: Create table with data and deletes
    TableIdentifier tableIdent = TableIdentifier.of("db", "resolve_for_compaction");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add data file
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Write position deletes
    DeleteFile deleteFile = writePositionDeletes(table, "/path/to/source.parquet", 10L, 20L);
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Create compaction map
    CompactionMapBuilder mapBuilder = new CompactionMapBuilder(1L, 2L);
    mapBuilder
        .addFileMapping("/path/to/source.parquet", "/path/to/target.parquet")
        .addRun(0, 0, 100);
    CompactionMap compactionMap = mapBuilder.build();

    // Use resolveForCompaction convenience method
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictResolver resolver = new CompactionConflictResolver(table);
    DeleteManifestChanges changes =
        resolver.resolveForCompaction(
            compactionMap, metadata, startingSnapshot, table.currentSnapshot());

    // Verify changes
    assertThat(changes.hasChanges()).isTrue();
    assertThat(changes.totalDeletesRemapped()).isEqualTo(2);
  }

  // Helper method to write position deletes to a file
  private DeleteFile writePositionDeletes(Table table, String dataFilePath, Long... positions)
      throws IOException {
    OutputFile outputFile =
        table
            .io()
            .newOutputFile(
                table.location()
                    + "/metadata/deletes-"
                    + System.nanoTime()
                    + "-"
                    + dataFilePath.hashCode()
                    + ".avro");

    PositionDeleteWriter<Void> writer =
        Avro.writeDeletes(outputFile)
            .createWriterFunc(DataWriter::create)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .buildPositionWriter();

    try {
      PositionDelete<Void> delete = PositionDelete.create();
      for (Long position : positions) {
        writer.write(delete.set(dataFilePath, position));
      }
    } finally {
      writer.close();
    }

    DeleteFile baseDeleteFile = writer.toDeleteFile();

    // Create a new DeleteFile with explicit referencedDataFile set
    // This is needed for conflict detection to work
    return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
        .ofPositionDeletes()
        .withPath(baseDeleteFile.location())
        .withFileSizeInBytes(baseDeleteFile.fileSizeInBytes())
        .withRecordCount(baseDeleteFile.recordCount())
        .withReferencedDataFile(dataFilePath)
        .build();
  }
}
