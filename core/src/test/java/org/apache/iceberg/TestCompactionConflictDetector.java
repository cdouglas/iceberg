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
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for CompactionConflictDetector.
 *
 * <p>These tests verify that the detector correctly identifies position delete files that reference
 * files being compacted.
 */
public class TestCompactionConflictDetector {

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
  public void testDetectSimpleConflict() throws IOException {
    // Setup: Create table with data files, then add deletes
    TableIdentifier tableIdent = TableIdentifier.of("db", "simple_conflict");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add data file
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Add position delete that references the data file
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .withReferencedDataFile(dataFile.path().toString())
            .build();
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Create detector and check for conflicts
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());

    Set<String> filesToCompact = ImmutableSet.of(dataFile.path().toString());
    DeleteConflictInfo conflicts = detector.detectConflicts(filesToCompact);

    // Verify conflict detected
    assertThat(conflicts.hasConflicts()).isTrue();
    assertThat(conflicts.affectedDataFileCount()).isEqualTo(1);
    assertThat(conflicts.deleteFileCount()).isEqualTo(1);
    assertThat(conflicts.affectedDataFiles()).contains(dataFile.path().toString());
    assertThat(conflicts.conflictingDeleteFiles()).hasSize(1);
  }

  @Test
  public void testNoConflictsWhenNoDeletes() throws IOException {
    // Setup: Create table with only data files (no deletes)
    TableIdentifier tableIdent = TableIdentifier.of("db", "no_deletes");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add data files
    DataFile dataFile1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    DataFile dataFile2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // No deletes added - detector should find no conflicts
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());

    Set<String> filesToCompact =
        ImmutableSet.of(dataFile1.path().toString(), dataFile2.path().toString());
    DeleteConflictInfo conflicts = detector.detectConflicts(filesToCompact);

    // Verify no conflicts
    assertThat(conflicts.hasConflicts()).isFalse();
    assertThat(conflicts.deleteFileCount()).isEqualTo(0);
    assertThat(conflicts.affectedDataFileCount()).isEqualTo(0);
  }

  @Test
  public void testMultipleDeletesOnSameFile() throws IOException {
    // Setup: Create table with multiple deletes on same data file
    TableIdentifier tableIdent = TableIdentifier.of("db", "multiple_deletes");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add data file
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Add first delete
    DeleteFile deleteFile1 =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/deletes1.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(5)
            .withReferencedDataFile(dataFile.path().toString())
            .build();
    table.newRowDelta().addDeletes(deleteFile1).commit();

    // Add second delete
    DeleteFile deleteFile2 =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/deletes2.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(5)
            .withReferencedDataFile(dataFile.path().toString())
            .build();
    table.newRowDelta().addDeletes(deleteFile2).commit();

    // Create detector
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());

    Set<String> filesToCompact = ImmutableSet.of(dataFile.path().toString());
    DeleteConflictInfo conflicts = detector.detectConflicts(filesToCompact);

    // Verify both deletes detected
    assertThat(conflicts.hasConflicts()).isTrue();
    assertThat(conflicts.deleteFileCount()).isEqualTo(2);
    assertThat(conflicts.affectedDataFileCount()).isEqualTo(1);
    assertThat(conflicts.deleteFilesByDataFile()).containsKey(dataFile.path().toString());
    assertThat(conflicts.deleteFilesByDataFile().get(dataFile.path().toString())).hasSize(2);
  }

  @Test
  public void testPartialConflicts() throws IOException {
    // Setup: Create table where only some files have deletes
    TableIdentifier tableIdent = TableIdentifier.of("db", "partial_conflicts");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add data files
    DataFile dataFile1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    DataFile dataFile2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    DataFile dataFile3 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data3.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).appendFile(dataFile3).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Add delete only on dataFile1
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .withReferencedDataFile(dataFile1.path().toString())
            .build();
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Create detector
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());

    // Compact all three files - only dataFile1 has conflict
    Set<String> filesToCompact =
        ImmutableSet.of(
            dataFile1.path().toString(), dataFile2.path().toString(), dataFile3.path().toString());
    DeleteConflictInfo conflicts = detector.detectConflicts(filesToCompact);

    // Verify partial conflict
    assertThat(conflicts.hasConflicts()).isTrue();
    assertThat(conflicts.deleteFileCount()).isEqualTo(1);
    assertThat(conflicts.affectedDataFileCount()).isEqualTo(1);
    assertThat(conflicts.affectedDataFiles()).contains(dataFile1.path().toString());
    assertThat(conflicts.affectedDataFiles()).doesNotContain(dataFile2.path().toString());
    assertThat(conflicts.affectedDataFiles()).doesNotContain(dataFile3.path().toString());
  }

  @Test
  public void testDeletesOnNonCompactedFiles() throws IOException {
    // Setup: Create table with deletes on files NOT being compacted
    TableIdentifier tableIdent = TableIdentifier.of("db", "non_compacted_deletes");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add data files
    DataFile dataFile1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    DataFile dataFile2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Add delete on dataFile1
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .withReferencedDataFile(dataFile1.path().toString())
            .build();
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Create detector
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());

    // Compact only dataFile2 (not the one with deletes)
    Set<String> filesToCompact = ImmutableSet.of(dataFile2.path().toString());
    DeleteConflictInfo conflicts = detector.detectConflicts(filesToCompact);

    // Verify no conflict (delete is on different file)
    assertThat(conflicts.hasConflicts()).isFalse();
    assertThat(conflicts.deleteFileCount()).isEqualTo(0);
  }

  @Test
  public void testEmptyFilesToCompact() throws IOException {
    // Setup: Create table with deletes
    TableIdentifier tableIdent = TableIdentifier.of("db", "empty_compact");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add data file
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Add delete
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .withReferencedDataFile(dataFile.path().toString())
            .build();
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Create detector
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());

    // Empty set of files to compact
    Set<String> filesToCompact = ImmutableSet.of();
    DeleteConflictInfo conflicts = detector.detectConflicts(filesToCompact);

    // Verify no conflict (nothing to compact)
    assertThat(conflicts.hasConflicts()).isFalse();
  }

  @Test
  public void testHasConflictsMethod() throws IOException {
    // Setup: Create table with deletes
    TableIdentifier tableIdent = TableIdentifier.of("db", "has_conflicts");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add data file
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Add delete
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .withReferencedDataFile(dataFile.path().toString())
            .build();
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Create detector
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());

    // Test hasConflicts() returns true for conflicting file
    assertThat(detector.hasConflicts(ImmutableSet.of(dataFile.path().toString()))).isTrue();

    // Test hasConflicts() returns false for non-conflicting file
    assertThat(detector.hasConflicts(ImmutableSet.of("/path/to/other.parquet"))).isFalse();

    // Test hasConflicts() returns false for empty set
    assertThat(detector.hasConflicts(ImmutableSet.of())).isFalse();
  }

  @Test
  public void testDeleteConflictInfoMethods() throws IOException {
    // Setup: Create table with deletes from multiple snapshots
    TableIdentifier tableIdent = TableIdentifier.of("db", "info_methods");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add data files
    DataFile dataFile1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    DataFile dataFile2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Add deletes in separate snapshots
    DeleteFile deleteFile1 =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/deletes1.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(5)
            .withReferencedDataFile(dataFile1.path().toString())
            .build();
    table.newRowDelta().addDeletes(deleteFile1).commit();

    DeleteFile deleteFile2 =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/deletes2.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(5)
            .withReferencedDataFile(dataFile2.path().toString())
            .build();
    table.newRowDelta().addDeletes(deleteFile2).commit();

    // Create detector
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());

    Set<String> filesToCompact =
        ImmutableSet.of(dataFile1.path().toString(), dataFile2.path().toString());
    DeleteConflictInfo conflicts = detector.detectConflicts(filesToCompact);

    // Verify DeleteConflictInfo methods
    assertThat(conflicts.hasConflicts()).isTrue();
    assertThat(conflicts.deleteFileCount()).isEqualTo(2);
    assertThat(conflicts.affectedDataFileCount()).isEqualTo(2);

    // Check deleteFilesBySnapshot has entries
    assertThat(conflicts.deleteFilesBySnapshot()).isNotEmpty();
    assertThat(conflicts.deleteFilesBySnapshot()).hasSize(2); // Two separate snapshots

    // Check deleteFilesByDataFile
    assertThat(conflicts.deleteFilesByDataFile()).hasSize(2);
    assertThat(conflicts.deleteFilesByDataFile().get(dataFile1.path().toString())).hasSize(1);
    assertThat(conflicts.deleteFilesByDataFile().get(dataFile2.path().toString())).hasSize(1);

    // Check toString doesn't throw
    assertThat(conflicts.toString()).contains("affectedDataFiles");
  }

  @Test
  public void testGetAffectedFiles() throws IOException {
    // Setup: Create table with deletes on multiple files
    TableIdentifier tableIdent = TableIdentifier.of("db", "affected_files");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add data files
    DataFile dataFile1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    DataFile dataFile2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Add delete only on dataFile1
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .withReferencedDataFile(dataFile1.path().toString())
            .build();
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Create detector
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());

    // Test getAffectedFiles
    Set<String> filesToCompact =
        ImmutableSet.of(dataFile1.path().toString(), dataFile2.path().toString());
    Set<String> affected = detector.getAffectedFiles(filesToCompact);

    assertThat(affected).hasSize(1);
    assertThat(affected).contains(dataFile1.path().toString());
    assertThat(affected).doesNotContain(dataFile2.path().toString());
  }

  @Test
  public void testMultiFilePositionDeletesDetected() throws IOException {
    // Setup: Create table with multi-file position deletes (partition-scoped)
    // Multi-file position deletes have different lower/upper bounds on file_path column
    TableIdentifier tableIdent = TableIdentifier.of("db", "multi_file_deletes");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add data files
    DataFile dataFile1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    DataFile dataFile2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Create multi-file position delete (different lower/upper bounds on file_path)
    // This simulates a delete file created with DeleteGranularity.PARTITION
    int pathFieldId = MetadataColumns.DELETE_FILE_PATH.fieldId();
    ByteBuffer lowerBound =
        Conversions.toByteBuffer(Types.StringType.get(), "/path/to/data1.parquet");
    ByteBuffer upperBound =
        Conversions.toByteBuffer(Types.StringType.get(), "/path/to/data2.parquet");

    Map<Integer, ByteBuffer> lowerBounds = ImmutableMap.of(pathFieldId, lowerBound);
    Map<Integer, ByteBuffer> upperBounds = ImmutableMap.of(pathFieldId, upperBound);

    // Create Metrics with different lower/upper bounds for file_path
    Metrics metrics = new Metrics(20L, null, null, null, null, lowerBounds, upperBounds);

    // Create position delete without referencedDataFile (multi-file)
    DeleteFile multiFileDelete =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/multi_deletes.parquet")
            .withFileSizeInBytes(100)
            .withMetrics(metrics)
            .build();
    table.newRowDelta().addDeletes(multiFileDelete).commit();

    // Create detector
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());

    Set<String> filesToCompact =
        ImmutableSet.of(dataFile1.path().toString(), dataFile2.path().toString());
    DeleteConflictInfo conflicts = detector.detectConflicts(filesToCompact);

    // Verify multi-file position delete is detected
    assertThat(conflicts.hasConflicts()).isTrue();
    assertThat(conflicts.hasMultiFilePositionDeletes()).isTrue();
    assertThat(conflicts.multiFilePositionDeletes()).hasSize(1);
    // File-scoped conflicts should be empty (no single-file position deletes)
    assertThat(conflicts.conflictingDeleteFiles()).isEmpty();
    assertThat(conflicts.deleteFileCount()).isEqualTo(0);
  }

  @Test
  public void testEqualityDeletesNotDetectedAsConflicts() throws IOException {
    // Setup: Create table with equality deletes
    // Equality deletes should NOT be detected as conflicts
    TableIdentifier tableIdent = TableIdentifier.of("db", "equality_deletes");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add data file
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Add equality delete (not file-scoped)
    DeleteFile equalityDelete =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofEqualityDeletes(1) // Equality delete on column 1 (id)
            .withPath("/path/to/eq_deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .build();
    table.newRowDelta().addDeletes(equalityDelete).commit();

    // Create detector
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());

    Set<String> filesToCompact = ImmutableSet.of(dataFile.path().toString());
    DeleteConflictInfo conflicts = detector.detectConflicts(filesToCompact);

    // Verify equality delete is NOT detected as conflict
    assertThat(conflicts.hasConflicts()).isFalse();
    assertThat(conflicts.deleteFileCount()).isEqualTo(0);
    assertThat(conflicts.multiFilePositionDeletes()).isEmpty();
  }

  @Test
  public void testMixedDeleteTypes() throws IOException {
    // Setup: Create table with both file-scoped and multi-file position deletes
    TableIdentifier tableIdent = TableIdentifier.of("db", "mixed_deletes");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add data files
    DataFile dataFile1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    DataFile dataFile2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Add file-scoped position delete
    DeleteFile fileScopedDelete =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/file_deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(5)
            .withReferencedDataFile(dataFile1.path().toString())
            .build();
    table.newRowDelta().addDeletes(fileScopedDelete).commit();

    // Add multi-file position delete
    int pathFieldId = MetadataColumns.DELETE_FILE_PATH.fieldId();
    ByteBuffer lowerBound =
        Conversions.toByteBuffer(Types.StringType.get(), "/path/to/data1.parquet");
    ByteBuffer upperBound =
        Conversions.toByteBuffer(Types.StringType.get(), "/path/to/data2.parquet");
    Map<Integer, ByteBuffer> lowerBounds = ImmutableMap.of(pathFieldId, lowerBound);
    Map<Integer, ByteBuffer> upperBounds = ImmutableMap.of(pathFieldId, upperBound);
    Metrics metrics = new Metrics(10L, null, null, null, null, lowerBounds, upperBounds);

    DeleteFile multiFileDelete =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/multi_deletes.parquet")
            .withFileSizeInBytes(100)
            .withMetrics(metrics)
            .build();
    table.newRowDelta().addDeletes(multiFileDelete).commit();

    // Create detector
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());

    Set<String> filesToCompact =
        ImmutableSet.of(dataFile1.path().toString(), dataFile2.path().toString());
    DeleteConflictInfo conflicts = detector.detectConflicts(filesToCompact);

    // Verify both types are detected
    assertThat(conflicts.hasConflicts()).isTrue();
    // File-scoped delete
    assertThat(conflicts.deleteFileCount()).isEqualTo(1);
    assertThat(conflicts.affectedDataFiles()).contains(dataFile1.path().toString());
    // Multi-file position delete
    assertThat(conflicts.hasMultiFilePositionDeletes()).isTrue();
    assertThat(conflicts.multiFilePositionDeletes()).hasSize(1);
    // Verify hasOnlyFileScopedConflicts returns false when multi-file deletes exist
    assertThat(conflicts.hasOnlyFileScopedConflicts()).isFalse();
  }
}
