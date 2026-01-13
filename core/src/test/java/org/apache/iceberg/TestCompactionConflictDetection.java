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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.exceptions.CompactionConflictException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for compaction conflict detection.
 *
 * <p>These tests verify that concurrent transactions are correctly detected when position deletes
 * reference data files that have been compacted.
 */
public class TestCompactionConflictDetection {

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
  public void testCompactionConflictDetectedV2() throws IOException {
    // 1. Create table with compaction maps enabled (V2 supports position deletes)
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table_v2");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // 2. Write initial data files
    List<DataFile> sourceFiles = Lists.newArrayList();
    for (int i = 0; i < 3; i++) {
      DataFile dataFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath(String.format("/path/to/source%d.parquet", i))
              .withFileSizeInBytes(1024)
              .withRecordCount(100)
              .build();
      sourceFiles.add(dataFile);
    }

    AppendFiles append = table.newAppend();
    sourceFiles.forEach(append::appendFile);
    append.commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // 3. Start a RowDelta transaction (don't commit yet)
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(startingSnapshot);

    // Create position delete on first source file
    DataFile fileToDelete = sourceFiles.get(0);
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .withReferencedDataFile(fileToDelete.path().toString())
            .build();

    rowDelta.addDeletes(deleteFile);

    // 4. Meanwhile, another transaction compacts the data
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    sourceFiles.forEach(rewrite::deleteFile);

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(3072)
            .withRecordCount(300)
            .build();

    rewrite.addFile(targetFile);
    rewrite.commit(); // Compaction commits successfully

    // 5. Try to commit the RowDelta - should fail with CompactionConflictException
    CompactionConflictException exception =
        assertThrows(CompactionConflictException.class, () -> rowDelta.commit());

    assertThat(exception.getMessage()).contains("referenced data files were compacted");

    // Exception provides programmatic access to conflict details
    assertThat(exception.compactedFiles()).contains(fileToDelete.path().toString());

    assertThat(exception.compactionMapLocations())
        .isNotEmpty()
        .containsKey(fileToDelete.path().toString());
  }

  @Test
  public void testSuccessfulConcurrentAppendV2() throws IOException {
    // Test that non-conflicting operations succeed (V2)
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table_append_v2");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // Write initial data
    DataFile sourceFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(sourceFile).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Start a transaction that adds new data (not deletes)
    AppendFiles append = table.newAppend();
    DataFile newFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/new.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    append.appendFile(newFile);

    // Meanwhile, compact the existing data
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    rewrite.deleteFile(sourceFile);

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    rewrite.addFile(targetFile);
    rewrite.commit();

    // Append should succeed - no conflict because we're not adding deletes
    append.commit();

    assertThat(table.currentSnapshot()).isNotNull();
  }

  @Test
  public void testCompactionMapsDisabledV2() throws IOException {
    // Test that validation is skipped when compaction maps are disabled (V2)
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table_disabled_v2");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "false")
        .commit();

    // Write and compact data
    DataFile sourceFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(sourceFile).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Start RowDelta transaction
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(startingSnapshot);

    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .withReferencedDataFile(sourceFile.path().toString())
            .build();

    rowDelta.addDeletes(deleteFile);

    // Compact the data
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    rewrite.deleteFile(sourceFile);

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    rewrite.addFile(targetFile);
    rewrite.commit();

    // Since compaction maps are disabled, compaction conflict detection is skipped
    // In V2, without compaction map validation, the commit may succeed or fail based on
    // generic validation rules. The key point is that CompactionConflictException is NOT thrown
    // because the feature is disabled.
    try {
      rowDelta.commit();
      // If it succeeds, that's fine - compaction conflict detection was disabled
    } catch (CompactionConflictException e) {
      // Should NOT throw CompactionConflictException when feature is disabled
      throw new AssertionError(
          "CompactionConflictException should not be thrown when compaction maps are disabled", e);
    } catch (Exception e) {
      // Other exceptions are acceptable (generic validation failures)
    }
  }

  // ===== V3 Tests with Deletion Vectors =====

  @Test
  public void testCompactionConflictDetectedV3WithDVs() throws IOException {
    // 1. Create table with compaction maps enabled (V3 supports deletion vectors)
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table_v3_dv");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "3")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // 2. Write initial data files
    List<DataFile> sourceFiles = Lists.newArrayList();
    for (int i = 0; i < 3; i++) {
      DataFile dataFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath(String.format("/path/to/source%d.parquet", i))
              .withFileSizeInBytes(1024)
              .withRecordCount(100)
              .build();
      sourceFiles.add(dataFile);
    }

    AppendFiles append = table.newAppend();
    sourceFiles.forEach(append::appendFile);
    append.commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // 3. Start a RowDelta transaction (don't commit yet)
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(startingSnapshot);

    // Create deletion vector on first source file
    DataFile fileToDelete = sourceFiles.get(0);
    DeleteFile dv = writeDV(table, fileToDelete.path().toString(), 10L, 20L, 30L);

    rowDelta.addDeletes(dv);

    // 4. Meanwhile, another transaction compacts the data
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    sourceFiles.forEach(rewrite::deleteFile);

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(3072)
            .withRecordCount(300)
            .build();

    rewrite.addFile(targetFile);
    rewrite.commit(); // Compaction commits successfully

    // 5. Try to commit the RowDelta - should fail with CompactionConflictException
    CompactionConflictException exception =
        assertThrows(CompactionConflictException.class, () -> rowDelta.commit());

    assertThat(exception.getMessage()).contains("referenced data files were compacted");

    // Exception provides programmatic access to conflict details
    assertThat(exception.compactedFiles()).contains(fileToDelete.path().toString());

    assertThat(exception.compactionMapLocations())
        .isNotEmpty()
        .containsKey(fileToDelete.path().toString());
  }

  @Test
  public void testSuccessfulConcurrentAppendV3WithDVs() throws IOException {
    // Test that non-conflicting operations succeed (V3)
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table_append_v3_dv");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "3")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // Write initial data
    DataFile sourceFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(sourceFile).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Start a transaction that adds new data (not deletes)
    AppendFiles append = table.newAppend();
    DataFile newFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/new.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    append.appendFile(newFile);

    // Meanwhile, compact the existing data
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    rewrite.deleteFile(sourceFile);

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    rewrite.addFile(targetFile);
    rewrite.commit();

    // Append should succeed - no conflict because we're not adding deletes
    append.commit();

    assertThat(table.currentSnapshot()).isNotNull();
  }

  @Test
  public void testCompactionMapsDisabledV3WithDVs() throws IOException {
    // Test that validation is skipped when compaction maps are disabled (V3)
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table_disabled_v3_dv");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "3")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "false")
        .commit();

    // Write and compact data
    DataFile sourceFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(sourceFile).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Start RowDelta transaction
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(startingSnapshot);

    DeleteFile dv = writeDV(table, sourceFile.path().toString(), 10L, 20L, 30L);

    rowDelta.addDeletes(dv);

    // Compact the data
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    rewrite.deleteFile(sourceFile);

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    rewrite.addFile(targetFile);
    rewrite.commit();

    // Since compaction maps are disabled, compaction conflict detection is skipped
    // In V3, without compaction map validation, the commit may succeed or fail based on
    // generic validation rules. The key point is that CompactionConflictException is NOT thrown
    // because the feature is disabled.
    try {
      rowDelta.commit();
      // If it succeeds, that's fine - compaction conflict detection was disabled
    } catch (CompactionConflictException e) {
      // Should NOT throw CompactionConflictException when feature is disabled
      throw new AssertionError(
          "CompactionConflictException should not be thrown when compaction maps are disabled", e);
    } catch (Exception e) {
      // Other exceptions are acceptable (generic validation failures)
    }
  }

  /**
   * Helper method to write a deletion vector file.
   *
   * @param table the table to write the DV for
   * @param dataFilePath the path of the data file the DV references
   * @param positions the positions to mark as deleted
   * @return the written DeleteFile (DV)
   */
  private DeleteFile writeDV(Table table, String dataFilePath, Long... positions)
      throws IOException {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();

    java.util.function.Function<String, PositionDeleteIndex> noPreviousDeletes =
        path -> PositionDeleteIndex.empty();

    DVFileWriter writer = new BaseDVFileWriter(fileFactory, noPreviousDeletes);

    for (Long pos : positions) {
      writer.delete(dataFilePath, pos, table.spec(), null);
    }

    writer.close();
    DeleteWriteResult result = writer.result();

    if (result.deleteFiles().isEmpty()) {
      throw new IllegalStateException("DV writer produced no delete files");
    }

    return result.deleteFiles().get(0);
  }
}
