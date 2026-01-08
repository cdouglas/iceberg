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
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for SERIALIZABLE isolation level with compaction-aware conflict detection.
 *
 * <p>These tests verify that SERIALIZABLE isolation correctly handles concurrent REPLACE
 * operations (compactions) by distinguishing between:
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
public class TestSerializableIsolationWithCompaction {

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
  public void testSerializableIsolationWithCompactionMap() throws IOException {
    // This test verifies that a SERIALIZABLE DELETE operation SUCCEEDS when concurrent
    // REPLACE has a compaction map (structure-only change, no logical data change)

    // 1. Create table with compaction maps enabled
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_serializable_with_map");
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

    // 3. Start a RowDelta transaction with SERIALIZABLE isolation
    // The key is using conflictDetectionFilter (reading data) and validateNoConflictingDataFiles()
    RowDelta rowDelta =
        table
            .newRowDelta()
            .validateFromSnapshot(startingSnapshot)
            .conflictDetectionFilter(Expressions.alwaysTrue()) // Reading all data
            .validateNoConflictingDataFiles(); // Enable SERIALIZABLE isolation

    // Add a NEW delete file (not referencing any of the source files)
    // This simulates a DELETE WHERE id > 500 operation that doesn't reference specific files
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/new_deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .build();
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

  @Test
  public void testSerializableIsolationWithoutCompactionMap() throws IOException {
    // This test verifies that a SERIALIZABLE DELETE operation FAILS when concurrent
    // REPLACE does NOT have a compaction map (actual data change)

    // 1. Create table with compaction maps DISABLED
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_serializable_without_map");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
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
    append.commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // 3. Start a RowDelta transaction with SERIALIZABLE isolation
    RowDelta rowDelta =
        table
            .newRowDelta()
            .validateFromSnapshot(startingSnapshot)
            .conflictDetectionFilter(Expressions.alwaysTrue()) // Reading all data
            .validateNoConflictingDataFiles(); // Enable SERIALIZABLE isolation

    // Add a NEW delete file
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/new_deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .build();
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

  @Test
  public void testSnapshotIsolationIgnoresREPLACE() throws IOException {
    // This test verifies that SNAPSHOT isolation level does NOT check for
    // REPLACE conflicts at all (compaction map or not)

    // 1. Create table with SNAPSHOT isolation (don't call validateNoConflictingDataFiles())
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_snapshot_isolation");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
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
    append.commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // 3. Start a RowDelta transaction WITHOUT SERIALIZABLE isolation
    // Key: NOT calling validateNoConflictingDataFiles()
    RowDelta rowDelta =
        table
            .newRowDelta()
            .validateFromSnapshot(startingSnapshot)
            .conflictDetectionFilter(Expressions.alwaysTrue()); // Reading data but SNAPSHOT isolation

    // Add a NEW delete file
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/new_deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .build();
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

  @Test
  public void testFilteredConflictDetection() throws IOException {
    // This test verifies that compaction-aware validation respects conflict detection filters
    // Only REPLACE operations affecting filtered data should cause conflicts

    // 1. Create partitioned table
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_filtered");
    Table table = catalog.createTable(tableIdent, SCHEMA, spec);

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
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

    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(spec)
            .ofPositionDeletes()
            .withPath("/path/to/deletes.parquet")
            .withPartitionPath("id=1")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .withPartition(TestHelpers.Row.of(1))
            .build();
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
}
