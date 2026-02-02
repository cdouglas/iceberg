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
package org.apache.iceberg.spark.actions;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CompactionConflictDetector;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMapBuilder;
import org.apache.iceberg.CompactionMapChain;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteConflictInfo;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for compaction conflict resolution with position deletes and deletion vectors.
 *
 * <p>These tests verify that when a compaction operation encounters position deletes or deletion
 * vectors that reference files being compacted, the deletes can be remapped to reference the new
 * compacted files.
 *
 * <p>Format version 2 uses position delete files, while format version 3 uses deletion vectors
 * (DVs). Both are supported by the conflict resolution infrastructure.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkCompactionConflictResolution extends TestBase {

  @TempDir private File tableDir;

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private static final int SCALE = 1000;

  @Parameter private int formatVersion;

  @Parameters(name = "formatVersion = {0}")
  protected static List<Integer> parameters() {
    // V2 uses position delete files, V3 uses deletion vectors
    return Arrays.asList(2, 3);
  }

  private String tableLocation = null;

  @BeforeAll
  public static void setupSpark() {
    spark.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false");
  }

  @BeforeEach
  public void setupTableLocation() {
    this.tableLocation = tableDir.toURI().toString();
  }

  @TestTemplate
  public void testConflictResolutionDisabledByDefault() {
    // Create table with data and deletes
    Table table = createTableWithData(2);
    List<DataFile> dataFiles = TestHelpers.dataFiles(table);

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Add position delete referencing first data file
    DeleteFile deleteFile = writePositionDelete(table, dataFiles.get(0), 0);
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Verify conflict exists
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());

    Set<String> filesToCompact = ImmutableSet.of(dataFiles.get(0).path().toString());
    DeleteConflictInfo conflicts = detector.detectConflicts(filesToCompact);
    assertThat(conflicts.hasConflicts()).isTrue();

    // Conflict resolution is disabled by default
    assertThat(
            table
                .properties()
                .getOrDefault(
                    TableProperties.COMPACTION_RESOLVE_DELETE_CONFLICTS,
                    String.valueOf(TableProperties.COMPACTION_RESOLVE_DELETE_CONFLICTS_DEFAULT)))
        .isEqualTo("false");
  }

  @TestTemplate
  public void testConflictResolutionEnabled() {
    // Create table with conflict resolution enabled
    Table table = createTableWithData(2);
    table
        .updateProperties()
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .set(TableProperties.COMPACTION_RESOLVE_DELETE_CONFLICTS, "true")
        .commit();

    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    long recordsBefore = dataFiles.stream().mapToLong(ContentFile::recordCount).sum();

    // Record expected data before any changes
    List<Object[]> expectedRecords = currentData();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Add position delete referencing first data file at position 0
    DeleteFile deleteFile = writePositionDelete(table, dataFiles.get(0), 0);
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Expected records should now exclude the deleted row
    expectedRecords = currentData();
    long recordsAfterDelete = expectedRecords.size();
    assertThat(recordsAfterDelete).isEqualTo(recordsBefore - 1);

    // Rewrite should succeed with conflict resolution
    RewriteDataFiles.Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .execute();

    // Verify rewrite completed
    assertThat(result.rewrittenDataFilesCount()).isGreaterThan(0);

    // Verify data is still correct after rewrite
    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match after conflict resolution", expectedRecords, actualRecords);
  }

  @TestTemplate
  public void testConflictResolutionWithMultipleDeletes() {
    // Create table with conflict resolution enabled
    Table table = createTableWithData(4);
    table
        .updateProperties()
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .set(TableProperties.COMPACTION_RESOLVE_DELETE_CONFLICTS, "true")
        .commit();

    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Add multiple position deletes
    RowDelta rowDelta = table.newRowDelta();
    for (int i = 0; i < 3; i++) {
      DeleteFile deleteFile = writePositionDelete(table, dataFiles.get(i), 0);
      rowDelta.addDeletes(deleteFile);
    }
    rowDelta.commit();

    // Record expected data
    List<Object[]> expectedRecords = currentData();

    // Rewrite should succeed with conflict resolution
    RewriteDataFiles.Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .execute();

    // Verify data is still correct
    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match after conflict resolution", expectedRecords, actualRecords);
  }

  @TestTemplate
  public void testConflictResolutionMaxFilesExceeded() {
    // This test verifies that when too many conflicting delete files are detected,
    // the detector returns the correct count which can be checked against max-files.
    // The actual enforcement happens in
    // SparkRewriteDataFilesCommitManager.detectAndResolveConflicts().
    Table table = createTableWithData(4);
    table
        .updateProperties()
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .set(TableProperties.COMPACTION_RESOLVE_DELETE_CONFLICTS, "true")
        .set(TableProperties.COMPACTION_RESOLVE_DELETE_CONFLICTS_MAX_FILES, "1")
        .commit();

    List<DataFile> dataFiles = TestHelpers.dataFiles(table);

    // Record the snapshot BEFORE adding deletes - this simulates a compaction
    // that started before the deletes were added
    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Add multiple position deletes (more than max)
    RowDelta rowDelta = table.newRowDelta();
    for (int i = 0; i < 3; i++) {
      DeleteFile deleteFile = writePositionDelete(table, dataFiles.get(i), 0);
      rowDelta.addDeletes(deleteFile);
    }
    rowDelta.commit();

    // Detect conflicts using the detector directly
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());

    Set<String> filesToCompact =
        dataFiles.stream()
            .map(f -> f.path().toString())
            .collect(java.util.stream.Collectors.toSet());
    DeleteConflictInfo conflicts = detector.detectConflicts(filesToCompact);

    // Verify we detected the conflicts
    assertThat(conflicts.hasConflicts()).isTrue();
    assertThat(conflicts.deleteFileCount()).isEqualTo(3);

    // Verify the max-files property is set correctly
    int maxFiles =
        Integer.parseInt(
            table
                .properties()
                .getOrDefault(
                    TableProperties.COMPACTION_RESOLVE_DELETE_CONFLICTS_MAX_FILES, "100"));
    assertThat(maxFiles).isEqualTo(1);

    // The conflict count exceeds max-files, so resolution would fail
    assertThat(conflicts.deleteFileCount()).isGreaterThan(maxFiles);
  }

  @TestTemplate
  public void testSparkCompactionConflictResolverDirectly() {
    // Test the resolver class directly
    Table table = createTableWithData(2);
    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    DataFile dataFile = dataFiles.get(0);

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Add position delete
    DeleteFile deleteFile = writePositionDelete(table, dataFile, 0);
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Create compaction map
    CompactionMapBuilder builder = new CompactionMapBuilder(startingSnapshot, startingSnapshot + 1);
    builder
        .addFileMapping(dataFile.path().toString(), "/new/compacted/file.parquet")
        .addRun(0, 0, dataFile.recordCount());
    CompactionMap compactionMap = builder.build();

    // Detect conflicts
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());

    Set<String> filesToCompact = ImmutableSet.of(dataFile.path().toString());
    DeleteConflictInfo conflicts = detector.detectConflicts(filesToCompact);

    assertThat(conflicts.hasConflicts()).isTrue();
    assertThat(conflicts.deleteFileCount()).isEqualTo(1);
  }

  @TestTemplate
  public void testNoConflictsWhenNoOverlap() {
    // Create table with conflict resolution enabled
    Table table = createTableWithData(4);
    table
        .updateProperties()
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .set(TableProperties.COMPACTION_RESOLVE_DELETE_CONFLICTS, "true")
        .commit();

    List<DataFile> dataFiles = TestHelpers.dataFiles(table);

    // Add position delete on file that won't be compacted (last file)
    DeleteFile deleteFile = writePositionDelete(table, dataFiles.get(3), 0);
    table.newRowDelta().addDeletes(deleteFile).commit();

    List<Object[]> expectedRecords = currentData();

    // Rewrite only first 2 files - no conflict with delete on file 3
    RewriteDataFiles.Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isGreaterThan(0);

    // Data should be unchanged
    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @TestTemplate
  public void testSequentialCompactionsWithDeletesBetween() {
    // This test verifies that position deletes added between two compactions
    // are correctly handled. The scenario is:
    //   1. Start with original data files F1, F2, F3, F4
    //   2. First compaction: F1, F2 -> F12
    //   3. Add position deletes referencing F12
    //   4. Second compaction: F12, F3 -> F123
    //   5. Deletes should be remapped F12 -> F123

    Table table = createTableWithData(4);
    table
        .updateProperties()
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .set(TableProperties.COMPACTION_RESOLVE_DELETE_CONFLICTS, "true")
        .commit();

    // Record initial data
    List<Object[]> expectedRecords = currentData();
    long initialRecordCount = expectedRecords.size();

    // First compaction - compacts some files
    RewriteDataFiles.Result result1 =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "2")
            .option(SizeBasedFileRewritePlanner.MAX_FILE_GROUP_SIZE_BYTES, String.valueOf(SCALE * 100))
            .execute();

    // Add position delete referencing one of the compacted files
    table.refresh();
    List<DataFile> filesAfterFirstCompaction = TestHelpers.dataFiles(table);
    assertThat(filesAfterFirstCompaction).isNotEmpty();

    // Delete first row of first file
    DeleteFile deleteFile = writePositionDelete(table, filesAfterFirstCompaction.get(0), 0);
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Record expected data after delete
    expectedRecords = currentData();
    assertThat(expectedRecords.size()).isEqualTo(initialRecordCount - 1);

    // Second compaction - may need to rebase deletes
    RewriteDataFiles.Result result2 =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .execute();

    // Verify data correctness after both compactions
    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match after sequential compactions", expectedRecords, actualRecords);
  }

  @TestTemplate
  public void testDeletesAddedBeforeMultipleCompactions() {
    // This test verifies that position deletes added before any compaction
    // are correctly remapped through a chain of compactions:
    //   1. Start with data files F1, F2, F3, F4
    //   2. Add position deletes referencing F1
    //   3. First compaction: F1, F2 -> F12 (creates M1: F1->F12)
    //   4. Second compaction: F12, F3 -> F123 (creates M2: F12->F123)
    //   5. Deletes need chain: F1 -> F12 -> F123

    Table table = createTableWithData(4);
    table
        .updateProperties()
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .set(TableProperties.COMPACTION_RESOLVE_DELETE_CONFLICTS, "true")
        .commit();

    List<DataFile> originalDataFiles = TestHelpers.dataFiles(table);
    long initialRecordCount = currentData().size();

    // Add position delete BEFORE any compaction
    DeleteFile deleteFile = writePositionDelete(table, originalDataFiles.get(0), 0);
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Record expected data after delete
    List<Object[]> expectedRecords = currentData();
    assertThat(expectedRecords.size()).isEqualTo(initialRecordCount - 1);

    // First compaction
    RewriteDataFiles.Result result1 =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "2")
            .option(SizeBasedFileRewritePlanner.MAX_FILE_GROUP_SIZE_BYTES, String.valueOf(SCALE * 100))
            .execute();

    // Verify data still correct after first compaction
    List<Object[]> afterFirst = currentData();
    assertEquals("Rows must match after first compaction", expectedRecords, afterFirst);

    // Second compaction
    RewriteDataFiles.Result result2 =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .execute();

    // Verify data still correct after second compaction
    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match after chained compactions", expectedRecords, actualRecords);
  }

  @TestTemplate
  public void testCompactionMapChainComposition() {
    // Unit test for CompactionMapChain with the Spark resolver
    // Tests that chains are correctly composed and position deletes remapped

    Table table = createTableWithData(2);
    List<DataFile> dataFiles = TestHelpers.dataFiles(table);

    // Create a chain: F1 -> F2 -> F3
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder
        .addFileMapping(dataFiles.get(0).path().toString(), "intermediate.parquet")
        .addRun(0, 0, dataFiles.get(0).recordCount());
    CompactionMap m1 = m1Builder.build();

    CompactionMapBuilder m2Builder = new CompactionMapBuilder(2L, 3L);
    m2Builder
        .addFileMapping("intermediate.parquet", "final.parquet")
        .addRun(0, 100, dataFiles.get(0).recordCount()); // Offset by 100 in target
    CompactionMap m2 = m2Builder.build();

    // Build chain
    CompactionMapChain chain = CompactionMapChain.build(java.util.List.of(m1, m2));

    // Verify chain properties
    assertThat(chain.size()).isEqualTo(2);
    assertThat(chain.firstSourceSnapshotId()).isEqualTo(1L);
    assertThat(chain.lastTargetSnapshotId()).isEqualTo(3L);

    // Verify F1 maps through chain to final target
    assertThat(chain.containsSource(dataFiles.get(0).path().toString())).isTrue();

    CompactionMap.FileMapping mapping = chain.mappingForFile(dataFiles.get(0).path().toString());
    assertThat(mapping).isNotNull();
    assertThat(mapping.targetFile()).isEqualTo("final.parquet");

    // Verify position mapping: position 50 in F1 -> 50 in intermediate -> 150 in final
    CompactionMap.Run run = mapping.runForPosition(50);
    assertThat(run).isNotNull();
    assertThat(run.mapPosition(50)).isEqualTo(150);
  }

  // Helper methods

  private Table createTableWithData(int numFiles) {
    Map<String, String> options =
        ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    Table table = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), options, tableLocation);

    // Write data files
    for (int i = 0; i < numFiles; i++) {
      writeRecords(i * SCALE, SCALE);
    }

    return table;
  }

  private void writeRecords(int start, int count) {
    List<String> data = Lists.newArrayList();
    for (int i = start; i < start + count; i++) {
      data.add(String.format("%d,data-%d", i, i));
    }

    Dataset<Row> df =
        spark
            .read()
            .schema("id INT, data STRING")
            .csv(spark.createDataset(data, org.apache.spark.sql.Encoders.STRING()));

    df.write()
        .format("iceberg")
        .mode("append")
        .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
        .save(tableLocation);
  }

  private List<Object[]> currentData() {
    return rowsToJava(
        spark.read().format("iceberg").load(tableLocation).orderBy("id").collectAsList());
  }

  /**
   * Writes a delete for the given data file and position.
   *
   * <p>For V2 format, writes a position delete file. For V3 format, writes a deletion vector.
   */
  private DeleteFile writePositionDelete(Table table, DataFile dataFile, long position) {
    if (formatVersion >= 3) {
      return writeDeletionVector(table, dataFile, position);
    } else {
      return writePositionDeleteFile(table, dataFile, position);
    }
  }

  /** Writes a position delete file (V2 format). */
  private DeleteFile writePositionDeleteFile(Table table, DataFile dataFile, long position) {
    OutputFile outputFile =
        table
            .io()
            .newOutputFile(
                table
                    .locationProvider()
                    .newDataLocation(
                        FileFormat.PARQUET.addExtension(UUID.randomUUID().toString())));
    EncryptedOutputFile encryptedOutputFile =
        EncryptedFiles.encryptedOutput(outputFile, EncryptionKeyMetadata.EMPTY);

    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec(), null, null, null);
    PositionDeleteWriter<Record> posDeleteWriter =
        appenderFactory
            .set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "full")
            .newPosDeleteWriter(encryptedOutputFile, FileFormat.PARQUET, dataFile.partition());

    PositionDelete<Record> posDelete = PositionDelete.create();
    posDeleteWriter.write(posDelete.set(dataFile.path().toString(), position, null));

    try {
      posDeleteWriter.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return posDeleteWriter.toDeleteFile();
  }

  /** Writes a deletion vector (V3 format). */
  private DeleteFile writeDeletionVector(Table table, DataFile dataFile, long position) {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();

    StructLike partition = dataFile.partition();
    String path = dataFile.path().toString();

    DVFileWriter writer = new BaseDVFileWriter(fileFactory, p -> null);
    try {
      writer.delete(path, position, table.spec(), partition);
      writer.close();
      List<DeleteFile> deleteFiles = writer.result().deleteFiles();
      assertThat(deleteFiles).hasSize(1);
      return deleteFiles.get(0);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
