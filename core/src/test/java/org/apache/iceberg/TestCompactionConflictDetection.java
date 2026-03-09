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
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
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

  @Test
  public void testChainedCompactionMapsDetected() throws IOException {
    // Test that chained compaction maps are detected and throw ChainedCompactionMapsException
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_chain_detection");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // Write initial file F1
    DataFile f1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/f1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(f1).commit();
    long snapshotS1 = table.currentSnapshot().snapshotId();

    // Start transaction with deletes referencing F1 (don't commit yet)
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(snapshotS1);
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .withReferencedDataFile(f1.path().toString())
            .build();
    rowDelta.addDeletes(deleteFile);

    // First compaction: F1 -> F2 (S1 -> S2)
    DataFile f2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/f2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    RewriteFiles rewrite1 = table.newRewrite().validateFromSnapshot(snapshotS1);
    rewrite1.deleteFile(f1);
    rewrite1.addFile(f2);
    rewrite1.commit();

    // Second compaction: F2 -> F3 (S2 -> S3)
    long snapshotS2 = table.currentSnapshot().snapshotId();
    DataFile f3 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/f3.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    RewriteFiles rewrite2 = table.newRewrite().validateFromSnapshot(snapshotS2);
    rewrite2.deleteFile(f2);
    rewrite2.addFile(f3);
    rewrite2.commit();

    // Try to commit - should detect chain F1 -> F2 -> F3
    org.apache.iceberg.exceptions.ChainedCompactionMapsException chainedException =
        assertThrows(
            org.apache.iceberg.exceptions.ChainedCompactionMapsException.class,
            () -> rowDelta.commit());

    assertThat(chainedException.chainedFiles()).contains(f1.path().toString());
    assertThat(chainedException.chainSnapshotIds()).hasSize(3); // S1, S2, S3
    assertThat(chainedException.compactionMaps()).hasSize(2); // Two maps in the chain
  }

  @Test
  public void testValidatorFindCompactionMapsApi() throws IOException {
    // Test the findCompactionMaps() public API
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_find_maps_api");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // Write and compact file
    DataFile source =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(source).commit();
    long startingSnapshot = table.currentSnapshot().snapshotId();

    DataFile target =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    rewrite.deleteFile(source);
    rewrite.addFile(target);
    rewrite.commit();

    // Create validator and use findCompactionMaps()
    TableMetadata metadata = ((BaseTable) table).operations().current();
    CompactionMapValidator validator =
        new CompactionMapValidator(table.io(), metadata, startingSnapshot, table.currentSnapshot());

    java.util.Map<String, String> maps = validator.findCompactionMaps();
    assertThat(maps).containsKey(source.path().toString());
    assertThat(maps.get(source.path().toString())).isNotNull();
  }

  @Test
  public void testValidatorFindCompactionMapChainApi() throws IOException {
    // Test the findCompactionMapChain() public API
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_find_chain_api");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    DataFile f1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/f1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(f1).commit();
    long startingSnapshot = table.currentSnapshot().snapshotId();

    // First compaction
    DataFile f2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/f2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newRewrite().validateFromSnapshot(startingSnapshot).deleteFile(f1).addFile(f2).commit();

    // Second compaction
    long snap2 = table.currentSnapshot().snapshotId();
    DataFile f3 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/f3.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newRewrite().validateFromSnapshot(snap2).deleteFile(f2).addFile(f3).commit();

    // Create validator and use findCompactionMapChain()
    TableMetadata metadata = ((BaseTable) table).operations().current();
    CompactionMapValidator validator =
        new CompactionMapValidator(table.io(), metadata, startingSnapshot, table.currentSnapshot());

    CompactionMapChain chain = validator.findCompactionMapChain();
    assertThat(chain).isNotNull();
    assertThat(chain.size()).isEqualTo(2); // Two maps in the chain

    java.util.List<CompactionMap> orderedMaps = validator.getOrderedMaps();
    assertThat(orderedMaps).hasSize(2);
  }

  @Test
  public void testValidatorWithEmptyDeleteFiles() throws IOException {
    // Test edge case: empty delete files list
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_empty_deletes");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    DataFile source =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(source).commit();
    long startingSnapshot = table.currentSnapshot().snapshotId();

    DataFile target =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table
        .newRewrite()
        .validateFromSnapshot(startingSnapshot)
        .deleteFile(source)
        .addFile(target)
        .commit();

    // Create validator and call with empty list
    TableMetadata metadata = ((BaseTable) table).operations().current();
    CompactionMapValidator validator =
        new CompactionMapValidator(table.io(), metadata, startingSnapshot, table.currentSnapshot());

    // Should not throw - early return for empty input
    validator.validateNoCompactedReferences(java.util.Collections.emptyList());
  }

  @Test
  public void testOverlappingCompactionsSecondFails() throws IOException {
    // Test that when two compactions target overlapping file sets, the second one fails
    // at commit time with ValidationException (not CompactionConflictException)
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_overlapping_compactions");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // Create three data files: F1, F2, F3
    DataFile f1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/f1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    DataFile f2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/f2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    DataFile f3 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/f3.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(f1).appendFile(f2).appendFile(f3).commit();
    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Compaction A: compact F1 and F2 (don't commit yet)
    RewriteFiles compactionA = table.newRewrite().validateFromSnapshot(startingSnapshot);
    compactionA.deleteFile(f1);
    compactionA.deleteFile(f2);

    DataFile compactionAOutput =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/compaction_a_output.parquet")
            .withFileSizeInBytes(2048)
            .withRecordCount(200)
            .build();
    compactionA.addFile(compactionAOutput);

    // Compaction B: compact F2 and F3 (overlapping on F2)
    RewriteFiles compactionB = table.newRewrite().validateFromSnapshot(startingSnapshot);
    compactionB.deleteFile(f2);
    compactionB.deleteFile(f3);

    DataFile compactionBOutput =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/compaction_b_output.parquet")
            .withFileSizeInBytes(2048)
            .withRecordCount(200)
            .build();
    compactionB.addFile(compactionBOutput);

    // Compaction A commits successfully
    compactionA.commit();

    // Compaction B should fail because F2 was already deleted by Compaction A
    // The error is a generic ValidationException, not CompactionConflictException,
    // because this is a commit-time validation failure (missing data files)
    org.apache.iceberg.exceptions.ValidationException exception =
        assertThrows(
            org.apache.iceberg.exceptions.ValidationException.class, () -> compactionB.commit());

    // The exception message should indicate the missing file
    assertThat(exception.getMessage()).contains("f2.parquet");
  }

  @Test
  public void testNonOverlappingCompactionsBothSucceed() throws IOException {
    // Test that non-overlapping compactions can both succeed
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_non_overlapping_compactions");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // Create four data files: F1, F2, F3, F4
    DataFile f1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/f1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    DataFile f2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/f2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    DataFile f3 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/f3.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    DataFile f4 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/f4.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(f1).appendFile(f2).appendFile(f3).appendFile(f4).commit();
    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Compaction A: compact F1 and F2 (disjoint from B)
    RewriteFiles compactionA = table.newRewrite().validateFromSnapshot(startingSnapshot);
    compactionA.deleteFile(f1);
    compactionA.deleteFile(f2);

    DataFile compactionAOutput =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/compaction_a_output.parquet")
            .withFileSizeInBytes(2048)
            .withRecordCount(200)
            .build();
    compactionA.addFile(compactionAOutput);

    // Compaction B: compact F3 and F4 (disjoint from A)
    RewriteFiles compactionB = table.newRewrite().validateFromSnapshot(startingSnapshot);
    compactionB.deleteFile(f3);
    compactionB.deleteFile(f4);

    DataFile compactionBOutput =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/compaction_b_output.parquet")
            .withFileSizeInBytes(2048)
            .withRecordCount(200)
            .build();
    compactionB.addFile(compactionBOutput);

    // Both compactions should succeed (they target disjoint file sets)
    compactionA.commit();
    compactionB.commit();

    // Verify both outputs are in the table
    assertThat(table.currentSnapshot()).isNotNull();
  }

  @Test
  public void testValidatorNoCompactionMaps() throws IOException {
    // Test edge case: no compaction maps in history
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_no_maps");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "false") // Disabled
        .commit();

    DataFile source =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(source).commit();
    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Compact without maps (feature disabled)
    DataFile target =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table
        .newRewrite()
        .validateFromSnapshot(startingSnapshot)
        .deleteFile(source)
        .addFile(target)
        .commit();

    // Create validator
    TableMetadata metadata = ((BaseTable) table).operations().current();
    CompactionMapValidator validator =
        new CompactionMapValidator(table.io(), metadata, startingSnapshot, table.currentSnapshot());

    // findCompactionMapChain should return null when no maps
    CompactionMapChain chain = validator.findCompactionMapChain();
    assertThat(chain).isNull();

    // getOrderedMaps should return empty list
    java.util.List<CompactionMap> orderedMaps = validator.getOrderedMaps();
    assertThat(orderedMaps).isEmpty();
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

  /**
   * Tests that chain collection follows all branches in a fan-out rewrite.
   *
   * <p>When F1 maps to {T1, T2} (fan-out), and T1→T3 and T2→T4 are subsequent compactions, the
   * chain collector must find ALL downstream maps, not just the first branch encountered.
   *
   * <p>This is a regression test for a bug where {@code collectChainMaps} followed only one
   * downstream target due to single-path traversal instead of BFS.
   */
  @Test
  public void testFanOutChainCollectsAllBranches() throws IOException {
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_fanout_chain");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // Write initial file F1
    DataFile f1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/f1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(200)
            .build();

    table.newAppend().appendFile(f1).commit();
    long snap1 = table.currentSnapshot().snapshotId();

    // Start transaction with deletes referencing F1
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(snap1);
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .withReferencedDataFile(f1.path().toString())
            .build();
    rowDelta.addDeletes(deleteFile);

    // First compaction: F1 → {T1, T2} (fan-out, first 100 rows to T1, next 100 to T2)
    // This requires an explicit map since auto-generation only handles single-target rewrites.
    DataFile t1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/t1.parquet")
            .withFileSizeInBytes(512)
            .withRecordCount(100)
            .build();

    DataFile t2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/t2.parquet")
            .withFileSizeInBytes(512)
            .withRecordCount(100)
            .build();

    RewriteFiles rewrite1 = table.newRewrite().validateFromSnapshot(snap1);
    rewrite1.deleteFile(f1);
    rewrite1.addFile(t1);
    rewrite1.addFile(t2);

    // Build fan-out compaction map: F1 → T1 (rows 0-99) and F1 → T2 (rows 100-199)
    {
      CompactionMapBuilder cmb = new CompactionMapBuilder(snap1, snap1 + 1);
      CompactionMapBuilder.FileMappingBuilder fmb =
          cmb.addFileMapping(f1.path().toString(), t1.path().toString());
      fmb.addRun(0L, 0L, 100L);
      fmb.addRun(100L, 0L, 100L, t2.path().toString());
      CompactionMap cmap = cmb.build();
      OutputFile cmf = CompactionMaps.newCompactionMapFile(table, snap1 + 1);
      CompactionMaps.write(cmap, cmf);
      ((BaseRewriteFiles) rewrite1).setCompactionMapLocation(cmf.location());
    }

    rewrite1.commit();

    // Second compaction: T1 → T3 (auto-generated map via single-target fallback)
    long snap2 = table.currentSnapshot().snapshotId();

    DataFile t3 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/t3.parquet")
            .withFileSizeInBytes(512)
            .withRecordCount(100)
            .build();

    table.newRewrite().validateFromSnapshot(snap2).deleteFile(t1).addFile(t3).commit();

    // Third compaction: T2 → T4 (auto-generated map via single-target fallback)
    long snap3 = table.currentSnapshot().snapshotId();

    DataFile t4 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/t4.parquet")
            .withFileSizeInBytes(512)
            .withRecordCount(100)
            .build();

    table.newRewrite().validateFromSnapshot(snap3).deleteFile(t2).addFile(t4).commit();

    // Try to commit deletes referencing F1 — should detect chain with ALL 3 maps
    org.apache.iceberg.exceptions.ChainedCompactionMapsException chainedException =
        assertThrows(
            org.apache.iceberg.exceptions.ChainedCompactionMapsException.class,
            () -> rowDelta.commit());

    assertThat(chainedException.chainedFiles()).contains(f1.path().toString());
    // All 3 maps should be collected (fan-out map + both branch maps)
    assertThat(chainedException.compactionMaps())
        .as("Chain must include all 3 maps: fan-out F1→{T1,T2}, T1→T3, and T2→T4")
        .hasSize(3);
  }
}
