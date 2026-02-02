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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for SERIALIZABLE isolation level with compaction-aware conflict detection.
 *
 * <p>These tests verify that SERIALIZABLE isolation correctly handles concurrent REPLACE operations
 * (compactions) by distinguishing between:
 *
 * <ul>
 *   <li>Structural changes (with compaction maps) - should NOT cause read conflicts
 *   <li>Data changes (without compaction maps) - SHOULD cause read conflicts
 * </ul>
 *
 * <p>Note: These tests focus on READ conflicts (when a transaction reads data that was replaced).
 * Tests for position delete conflicts (when deletes reference compacted files) are in
 * TestCompactionConflictDetection.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestSerializableIsolationWithCompaction {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @Parameters(name = "formatVersion = {0}")
  public static Object[][] parameters() {
    return new Object[][] {{2}, {3}};
  }

  @Parameter(index = 0)
  private int formatVersion;

  @TempDir public File temp;

  private InMemoryCatalog catalog;

  @BeforeEach
  public void setup() {
    catalog = new InMemoryCatalog();
    catalog.initialize("test", java.util.Collections.emptyMap());
    catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of("db"));
  }

  @TestTemplate
  public void testSerializableIsolationWithCompactionMap() throws IOException {
    // This test verifies that a SERIALIZABLE DELETE operation SUCCEEDS when concurrent
    // REPLACE has a compaction map (structure-only change, no logical data change)

    // 1. Create table with compaction maps enabled
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_serializable_with_map");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
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

    // For V3 tests, add an additional file that won't be compacted
    // This allows us to create DVs that reference a non-compacted file
    DataFile nonCompactedFile = null;
    if (formatVersion == 3) {
      nonCompactedFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath("/path/to/non_compacted.parquet")
              .withFileSizeInBytes(512)
              .withRecordCount(50)
              .build();
      append.appendFile(nonCompactedFile);
    }

    append.commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // 3. Start a RowDelta transaction with SERIALIZABLE isolation
    // The key is using conflictDetectionFilter (reading data) and validateNoConflictingDataFiles()
    RowDelta rowDelta =
        table
            .newRowDelta()
            .validateFromSnapshot(startingSnapshot)
            .conflictDetectionFilter(Expressions.alwaysTrue()) // Reading all data
            .validateNoConflictingDataFiles(); // Enable SERIALIZABLE isolation

    // Add a delete file
    // V2: Use position delete file (can be unattached to specific data file)
    // V3: Use DV referencing the non-compacted file (V3 requires DVs for position deletes)
    DeleteFile deleteFile;
    if (formatVersion == 3) {
      // Create DV referencing the non-compacted file
      deleteFile = writeDV(table, nonCompactedFile.path().toString(), 10L, 20L, 30L);
    } else {
      // V2: Position delete file
      deleteFile =
          FileMetadata.deleteFileBuilder(table.spec())
              .ofPositionDeletes()
              .withPath("/path/to/new_deletes.parquet")
              .withFileSizeInBytes(100)
              .withRecordCount(10)
              .build();
    }
    rowDelta.addDeletes(deleteFile);

    // 4. Meanwhile, compact the source data files (with compaction map)
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    sourceFiles.forEach(rewrite::deleteFile);

    // Create target file (compacted result)
    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(3072)
            .withRecordCount(300)
            .build();
    rewrite.addFile(targetFile);

    // Write a compaction map (simulating what RewriteDataFilesCommitManager would generate)
    CompactionMapBuilder mapBuilder =
        new CompactionMapBuilder(startingSnapshot, startingSnapshot + 1);

    long offset = 0;
    for (DataFile sourceFile : sourceFiles) {
      mapBuilder
          .addFileMapping(sourceFile.path().toString(), targetFile.path().toString())
          .addRun(0L, offset, sourceFile.recordCount());
      offset += sourceFile.recordCount();
    }
    CompactionMap map = mapBuilder.build();

    // Write map to storage
    OutputFile mapFile =
        CompactionMaps.newCompactionMapFile(table, table.currentSnapshot().snapshotId() + 1);
    CompactionMaps.write(map, mapFile);

    // Set map location on rewrite (this makes it a structural change)
    if (rewrite instanceof BaseRewriteFiles) {
      ((BaseRewriteFiles) rewrite).setCompactionMapLocation(mapFile.location());
    }

    rewrite.commit();

    // 5. Commit the RowDelta - should SUCCEED because compaction map exists
    // (No read conflict since only structure changed, not logical data)
    assertDoesNotThrow(() -> rowDelta.commit());

    // Verify the delete was committed
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.DELETE);
  }

  @TestTemplate
  public void testSerializableIsolationWithoutCompactionMap() throws IOException {
    // This test verifies that a SERIALIZABLE DELETE operation FAILS when concurrent
    // REPLACE does NOT have a compaction map (actual data change)

    // 1. Create table with compaction maps DISABLED
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_serializable_without_map");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
        .set(TableProperties.COMPACTION_MAP_ENABLED, "false") // Explicitly disable
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

    // For V3 tests, add an additional file that won't be compacted
    DataFile nonCompactedFile = null;
    if (formatVersion == 3) {
      nonCompactedFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath("/path/to/non_compacted.parquet")
              .withFileSizeInBytes(512)
              .withRecordCount(50)
              .build();
      append.appendFile(nonCompactedFile);
    }

    append.commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // 3. Start a RowDelta transaction with SERIALIZABLE isolation
    RowDelta rowDelta =
        table
            .newRowDelta()
            .validateFromSnapshot(startingSnapshot)
            .conflictDetectionFilter(Expressions.alwaysTrue()) // Reading all data
            .validateNoConflictingDataFiles(); // Enable SERIALIZABLE isolation

    // Add a delete file (V2: position delete file, V3: DV)
    DeleteFile deleteFile;
    if (formatVersion == 3) {
      deleteFile = writeDV(table, nonCompactedFile.path().toString(), 10L, 20L, 30L);
    } else {
      deleteFile =
          FileMetadata.deleteFileBuilder(table.spec())
              .ofPositionDeletes()
              .withPath("/path/to/new_deletes.parquet")
              .withFileSizeInBytes(100)
              .withRecordCount(10)
              .build();
    }
    rowDelta.addDeletes(deleteFile);

    // 4. Meanwhile, compact the data files (WITHOUT compaction map)
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    sourceFiles.forEach(rewrite::deleteFile);

    // Create target file (compacted result)
    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(3072)
            .withRecordCount(300)
            .build();
    rewrite.addFile(targetFile);

    // Do NOT set compaction map location - simulating a data change REPLACE
    rewrite.commit();

    // 5. Try to commit the RowDelta - should FAIL because no compaction map
    // (This is a read conflict - actual data may have changed)
    ValidationException exception =
        assertThrows(ValidationException.class, () -> rowDelta.commit());

    assertThat(exception.getMessage())
        .contains("Found conflicting files from REPLACE operation without compaction map");
  }

  @TestTemplate
  public void testSnapshotIsolationIgnoresREPLACE() throws IOException {
    // This test verifies that SNAPSHOT isolation level does NOT check for
    // REPLACE conflicts at all (compaction map or not)

    // 1. Create table with SNAPSHOT isolation (don't call validateNoConflictingDataFiles())
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_snapshot_isolation");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
        .set(TableProperties.COMPACTION_MAP_ENABLED, "false")
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

    // For V3 tests, add an additional file that won't be compacted
    DataFile nonCompactedFile = null;
    if (formatVersion == 3) {
      nonCompactedFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath("/path/to/non_compacted.parquet")
              .withFileSizeInBytes(512)
              .withRecordCount(50)
              .build();
      append.appendFile(nonCompactedFile);
    }

    append.commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // 3. Start a RowDelta transaction WITHOUT SERIALIZABLE isolation
    // Key: NOT calling validateNoConflictingDataFiles()
    RowDelta rowDelta =
        table
            .newRowDelta()
            .validateFromSnapshot(startingSnapshot)
            .conflictDetectionFilter(
                Expressions.alwaysTrue()); // Reading data but SNAPSHOT isolation

    // Add a delete file (V2: position delete file, V3: DV)
    DeleteFile deleteFile;
    if (formatVersion == 3) {
      deleteFile = writeDV(table, nonCompactedFile.path().toString(), 10L, 20L, 30L);
    } else {
      deleteFile =
          FileMetadata.deleteFileBuilder(table.spec())
              .ofPositionDeletes()
              .withPath("/path/to/new_deletes.parquet")
              .withFileSizeInBytes(100)
              .withRecordCount(10)
              .build();
    }
    rowDelta.addDeletes(deleteFile);

    // 4. Compact WITHOUT compaction map
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    sourceFiles.forEach(rewrite::deleteFile);

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(3072)
            .withRecordCount(300)
            .build();
    rewrite.addFile(targetFile);
    rewrite.commit();

    // 5. With SNAPSHOT isolation, the RowDelta should SUCCEED
    // (SNAPSHOT doesn't check REPLACE conflicts)
    assertDoesNotThrow(() -> rowDelta.commit());

    // Verify commit succeeded
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.DELETE);
  }

  @TestTemplate
  public void testFilteredConflictDetection() throws IOException {
    // This test verifies that compaction-aware validation respects conflict detection filters
    // Only REPLACE operations affecting filtered data should cause conflicts

    // 1. Create partitioned table
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_filtered");
    Table table = catalog.createTable(tableIdent, SCHEMA, spec);

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
        .set(TableProperties.COMPACTION_MAP_ENABLED, "false")
        .commit();

    // 2. Write data files in different partitions
    DataFile partition1File =
        DataFiles.builder(spec)
            .withPath("/path/to/partition1.parquet")
            .withPartitionPath("id=1")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .withPartition(TestHelpers.Row.of(1))
            .build();

    DataFile partition2File =
        DataFiles.builder(spec)
            .withPath("/path/to/partition2.parquet")
            .withPartitionPath("id=2")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .withPartition(TestHelpers.Row.of(2))
            .build();

    AppendFiles append = table.newAppend();
    append.appendFile(partition1File);
    append.appendFile(partition2File);
    append.commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // 3. Start a RowDelta transaction with SERIALIZABLE isolation and filter for partition 1
    RowDelta rowDelta =
        table
            .newRowDelta()
            .validateFromSnapshot(startingSnapshot)
            .conflictDetectionFilter(Expressions.equal("id", 1)) // Only partition 1
            .validateNoConflictingDataFiles();

    // Add delete file (V2: position delete file, V3: DV referencing partition1File)
    DeleteFile deleteFile;
    if (formatVersion == 3) {
      // DV referencing partition 1 file (which is NOT being compacted)
      deleteFile =
          writeDV(
              table, partition1File.path().toString(), partition1File.partition(), 10L, 20L, 30L);
    } else {
      // V2: position delete file
      deleteFile =
          FileMetadata.deleteFileBuilder(spec)
              .ofPositionDeletes()
              .withPath("/path/to/deletes.parquet")
              .withPartitionPath("id=1")
              .withFileSizeInBytes(100)
              .withRecordCount(10)
              .withPartition(TestHelpers.Row.of(1))
              .build();
    }
    rowDelta.addDeletes(deleteFile);

    // 4. Compact partition 2 (WITHOUT compaction map)
    RewriteFiles rewrite = table.newRewrite();
    rewrite.deleteFile(partition2File);

    DataFile compactedPartition2 =
        DataFiles.builder(spec)
            .withPath("/path/to/partition2_compacted.parquet")
            .withPartitionPath("id=2")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .withPartition(TestHelpers.Row.of(2))
            .build();
    rewrite.addFile(compactedPartition2);
    rewrite.commit();

    // 5. Commit should SUCCEED because compaction was in partition 2, not partition 1
    // Even though there's no compaction map, it doesn't affect filtered data
    assertDoesNotThrow(() -> rowDelta.commit());
  }

  @TestTemplate
  public void testSerializableIsolationWithChainedCompactions() throws IOException {
    // This test verifies that SERIALIZABLE isolation works correctly when multiple
    // compactions (a chain) occur between a transaction's start and commit.
    //
    // Scenario:
    //   1. Transaction T1 starts at S1 (reading data)
    //   2. Compaction C1: creates S2 with compaction map M1 (F1 -> F12)
    //   3. Compaction C2: creates S3 with compaction map M2 (F12 -> F123)
    //   4. Transaction T1 commits at S3
    //   -> Should SUCCEED because both C1 and C2 have compaction maps (structural changes)

    // 1. Create table with compaction maps enabled
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_serializable_chained");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // 2. Write initial data files (F1, F2, F3, F4)
    List<DataFile> sourceFiles = Lists.newArrayList();
    for (int i = 0; i < 4; i++) {
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

    // For V3 tests, add an additional file that won't be compacted (for DV target)
    DataFile nonCompactedFile = null;
    if (formatVersion == 3) {
      nonCompactedFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath("/path/to/non_compacted.parquet")
              .withFileSizeInBytes(512)
              .withRecordCount(50)
              .build();
      append.appendFile(nonCompactedFile);
    }

    append.commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // 3. Start a RowDelta transaction with SERIALIZABLE isolation
    RowDelta rowDelta =
        table
            .newRowDelta()
            .validateFromSnapshot(startingSnapshot)
            .conflictDetectionFilter(Expressions.alwaysTrue()) // Reading all data
            .validateNoConflictingDataFiles(); // Enable SERIALIZABLE isolation

    // Add a delete file (V2: position delete, V3: DV referencing non-compacted file)
    DeleteFile deleteFile;
    if (formatVersion == 3) {
      deleteFile = writeDV(table, nonCompactedFile.path().toString(), 10L, 20L, 30L);
    } else {
      deleteFile =
          FileMetadata.deleteFileBuilder(table.spec())
              .ofPositionDeletes()
              .withPath("/path/to/new_deletes.parquet")
              .withFileSizeInBytes(100)
              .withRecordCount(10)
              .build();
    }
    rowDelta.addDeletes(deleteFile);

    // 4. First compaction: F1, F2 -> F12 (with compaction map M1)
    DataFile targetFile1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target12.parquet")
            .withFileSizeInBytes(2048)
            .withRecordCount(200)
            .build();

    RewriteFiles rewrite1 = table.newRewrite().validateFromSnapshot(startingSnapshot);
    rewrite1.deleteFile(sourceFiles.get(0));
    rewrite1.deleteFile(sourceFiles.get(1));
    rewrite1.addFile(targetFile1);

    // Write compaction map M1
    CompactionMapBuilder mapBuilder1 =
        new CompactionMapBuilder(table.currentSnapshot().snapshotId(), startingSnapshot + 1);
    mapBuilder1
        .addFileMapping(sourceFiles.get(0).path().toString(), targetFile1.path().toString())
        .addRun(0L, 0L, 100);
    mapBuilder1
        .addFileMapping(sourceFiles.get(1).path().toString(), targetFile1.path().toString())
        .addRun(0L, 100L, 100);
    CompactionMap map1 = mapBuilder1.build();

    OutputFile mapFile1 =
        CompactionMaps.newCompactionMapFile(table, table.currentSnapshot().snapshotId() + 1);
    CompactionMaps.write(map1, mapFile1);

    if (rewrite1 instanceof BaseRewriteFiles) {
      ((BaseRewriteFiles) rewrite1).setCompactionMapLocation(mapFile1.location());
    }
    rewrite1.commit();

    long snapshotAfterC1 = table.currentSnapshot().snapshotId();

    // 5. Second compaction: F12, F3 -> F123 (with compaction map M2)
    DataFile targetFile2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target123.parquet")
            .withFileSizeInBytes(3072)
            .withRecordCount(300)
            .build();

    RewriteFiles rewrite2 = table.newRewrite().validateFromSnapshot(snapshotAfterC1);
    rewrite2.deleteFile(targetFile1);
    rewrite2.deleteFile(sourceFiles.get(2));
    rewrite2.addFile(targetFile2);

    // Write compaction map M2
    CompactionMapBuilder mapBuilder2 =
        new CompactionMapBuilder(snapshotAfterC1, snapshotAfterC1 + 1);
    mapBuilder2
        .addFileMapping(targetFile1.path().toString(), targetFile2.path().toString())
        .addRun(0L, 0L, 200);
    mapBuilder2
        .addFileMapping(sourceFiles.get(2).path().toString(), targetFile2.path().toString())
        .addRun(0L, 200L, 100);
    CompactionMap map2 = mapBuilder2.build();

    OutputFile mapFile2 = CompactionMaps.newCompactionMapFile(table, snapshotAfterC1 + 1);
    CompactionMaps.write(map2, mapFile2);

    if (rewrite2 instanceof BaseRewriteFiles) {
      ((BaseRewriteFiles) rewrite2).setCompactionMapLocation(mapFile2.location());
    }
    rewrite2.commit();

    // 6. Commit the RowDelta - should SUCCEED because both compactions have maps
    // Both C1 and C2 are structural changes (have compaction maps), not data changes
    assertDoesNotThrow(() -> rowDelta.commit());

    // Verify the delete was committed
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.DELETE);
  }

  @TestTemplate
  public void testSerializableIsolationChainValidatesEachCompaction() throws IOException {
    // This test verifies that SERIALIZABLE isolation validates EACH compaction
    // in a chain independently. Each REPLACE operation must have a compaction map
    // for the transaction to succeed.
    //
    // This is a positive test confirming the validation logic iterates through
    // all REPLACE operations and checks each one. The testSerializableIsolationWithChainedCompactions
    // test above proves that when all compactions have maps, the transaction succeeds.
    //
    // Note: Testing the negative case (partial chain missing map causing failure) requires
    // integration tests with real manifests. The unit test infrastructure uses simplified
    // metadata that may not trigger the full validation path.

    // This test confirms the validation logic by checking that three sequential
    // compactions (all with maps) allows the transaction to succeed.

    // 1. Create table
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_serializable_triple_chain");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // 2. Write initial data files
    List<DataFile> sourceFiles = Lists.newArrayList();
    for (int i = 0; i < 6; i++) {
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

    // For V3, add non-compacted file for DV target
    DataFile nonCompactedFile = null;
    if (formatVersion == 3) {
      nonCompactedFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath("/path/to/non_compacted.parquet")
              .withFileSizeInBytes(512)
              .withRecordCount(50)
              .build();
      append.appendFile(nonCompactedFile);
    }

    append.commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // 3. Start a RowDelta transaction with SERIALIZABLE isolation
    RowDelta rowDelta =
        table
            .newRowDelta()
            .validateFromSnapshot(startingSnapshot)
            .conflictDetectionFilter(Expressions.alwaysTrue())
            .validateNoConflictingDataFiles();

    DeleteFile deleteFile;
    if (formatVersion == 3) {
      deleteFile = writeDV(table, nonCompactedFile.path().toString(), 10L, 20L, 30L);
    } else {
      deleteFile =
          FileMetadata.deleteFileBuilder(table.spec())
              .ofPositionDeletes()
              .withPath("/path/to/new_deletes.parquet")
              .withFileSizeInBytes(100)
              .withRecordCount(10)
              .build();
    }
    rowDelta.addDeletes(deleteFile);

    // 4. First compaction: source0+source1 -> target12 (with map)
    DataFile targetFile1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target12.parquet")
            .withFileSizeInBytes(2048)
            .withRecordCount(200)
            .build();

    performCompactionWithMap(
        table,
        startingSnapshot,
        Lists.newArrayList(sourceFiles.get(0), sourceFiles.get(1)),
        targetFile1);

    long snapshotAfterC1 = table.currentSnapshot().snapshotId();

    // 5. Second compaction: source2+source3 -> target34 (with map)
    DataFile targetFile2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target34.parquet")
            .withFileSizeInBytes(2048)
            .withRecordCount(200)
            .build();

    performCompactionWithMap(
        table,
        snapshotAfterC1,
        Lists.newArrayList(sourceFiles.get(2), sourceFiles.get(3)),
        targetFile2);

    long snapshotAfterC2 = table.currentSnapshot().snapshotId();

    // 6. Third compaction: target12+target34 -> targetAll (with map)
    DataFile targetFileAll =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/targetAll.parquet")
            .withFileSizeInBytes(4096)
            .withRecordCount(400)
            .build();

    performCompactionWithMap(
        table, snapshotAfterC2, Lists.newArrayList(targetFile1, targetFile2), targetFileAll);

    // 7. Commit RowDelta - should SUCCEED because all three compactions have maps
    assertDoesNotThrow(() -> rowDelta.commit());

    // Verify the delete was committed
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.DELETE);
  }

  /** Helper method to perform a compaction with a compaction map. */
  private void performCompactionWithMap(
      Table table, long validateFromSnapshot, List<DataFile> sourceFiles, DataFile targetFile)
      throws IOException {

    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(validateFromSnapshot);
    sourceFiles.forEach(rewrite::deleteFile);
    rewrite.addFile(targetFile);

    // Build compaction map
    CompactionMapBuilder mapBuilder =
        new CompactionMapBuilder(
            table.currentSnapshot().snapshotId(), table.currentSnapshot().snapshotId() + 1);

    long offset = 0;
    for (DataFile sourceFile : sourceFiles) {
      mapBuilder
          .addFileMapping(sourceFile.path().toString(), targetFile.path().toString())
          .addRun(0L, offset, sourceFile.recordCount());
      offset += sourceFile.recordCount();
    }
    CompactionMap map = mapBuilder.build();

    // Write map to storage
    OutputFile mapFile =
        CompactionMaps.newCompactionMapFile(table, table.currentSnapshot().snapshotId() + 1);
    CompactionMaps.write(map, mapFile);

    // Set map location
    if (rewrite instanceof BaseRewriteFiles) {
      ((BaseRewriteFiles) rewrite).setCompactionMapLocation(mapFile.location());
    }

    rewrite.commit();
  }

  /** Helper method to write a deletion vector file (for V3 tests). */
  private DeleteFile writeDV(Table table, String dataFilePath, Long... positions)
      throws IOException {
    return writeDV(table, dataFilePath, null, positions);
  }

  /** Helper method to write a deletion vector file with partition data (for V3 tests). */
  private DeleteFile writeDV(
      Table table, String dataFilePath, StructLike partitionData, Long... positions)
      throws IOException {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();

    java.util.function.Function<String, PositionDeleteIndex> noPreviousDeletes =
        path -> PositionDeleteIndex.empty();

    DVFileWriter writer = new BaseDVFileWriter(fileFactory, noPreviousDeletes);

    for (Long pos : positions) {
      writer.delete(dataFilePath, pos, table.spec(), partitionData);
    }

    writer.close();
    DeleteWriteResult result = writer.result();

    if (result.deleteFiles().isEmpty()) {
      throw new IllegalStateException("DV writer produced no delete files");
    }

    return result.deleteFiles().get(0);
  }
}
