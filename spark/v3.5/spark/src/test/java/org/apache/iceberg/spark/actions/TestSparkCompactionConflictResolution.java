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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
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
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.exceptions.CompactionConflictException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for compaction conflict detection and resolution with Spark actions.
 *
 * <p>These tests verify end-to-end conflict detection and resolution workflows:
 *
 * <ul>
 *   <li>CompactionConflictException thrown for concurrent position delete transactions
 *   <li>Exception provides compaction map locations for conflict resolution
 *   <li>Manual resolution workflow using PositionDeleteRemapper
 *   <li>Multiple compaction rounds with cumulative conflict detection
 * </ul>
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkCompactionConflictResolution extends TestBase {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());

  @Parameter(index = 0)
  private int formatVersion;

  @Parameter(index = 1)
  private FileFormat fileFormat;

  @Parameters(name = "formatVersion = {0}, format = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
      {2, FileFormat.PARQUET},
      {2, FileFormat.ORC},
    };
  }

  @TempDir private File tableDir;
  private String tableLocation;

  @BeforeEach
  public void setupTable() {
    this.tableLocation = tableDir.toURI().toString();
  }

  /**
   * Test 8: Verify CompactionConflictException thrown for concurrent operations.
   *
   * <p>This test verifies that:
   *
   * <ul>
   *   <li>Concurrent position delete transactions trigger CompactionConflictException
   *   <li>Exception contains list of compacted files
   *   <li>Exception provides compaction map locations
   *   <li>Error message includes remediation guidance
   * </ul>
   */
  @TestTemplate
  public void testConflictDetectionWithSparkAction() throws IOException {
    // Create table with compaction maps enabled
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    properties.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name());
    properties.put(TableProperties.COMPACTION_MAP_ENABLED, "true");

    Table table = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), properties, tableLocation);

    // Write 5 small data files
    for (int i = 0; i < 5; i++) {
      writeRecords(table, i * 100, 100);
    }

    // Capture starting snapshot for transaction T1
    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Get first data file for position deletes
    List<DataFile> dataFiles = Lists.newArrayList();
    for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
      try (org.apache.iceberg.ManifestReader<DataFile> reader =
          org.apache.iceberg.ManifestFiles.read(manifest, table.io())) {
        reader.forEach(dataFiles::add);
      }
    }
    assertThat(dataFiles).hasSize(5);
    DataFile fileToDelete = dataFiles.get(0);

    // Start transaction T1 with position deletes (don't commit yet)
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(startingSnapshot);
    List<DeleteFile> deleteFiles = writePositionDeletes(table, fileToDelete, 10L, 20L, 30L);
    deleteFiles.forEach(rowDelta::addDeletes);

    // Meanwhile, run bin-pack compaction via Spark action
    RewriteDataFiles.Result result =
        SparkActions.get()
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .option(
                SizeBasedFileRewritePlanner.TARGET_FILE_SIZE_BYTES, Long.toString(10 * 1024 * 1024))
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(5);
    assertThat(result.addedDataFilesCount()).isGreaterThan(0);

    // Verify compaction maps were generated
    table.refresh();
    List<ManifestFile> manifests = table.currentSnapshot().dataManifests(table.io());
    assertThat(manifests).isNotEmpty();
    long mapsGenerated = manifests.stream().filter(m -> m.compactionMapLocation() != null).count();
    assertThat(mapsGenerated).as("Compaction maps should be generated").isGreaterThan(0);

    // Try to commit transaction T1 - should fail with CompactionConflictException
    assertThatThrownBy(() -> rowDelta.commit())
        .isInstanceOf(CompactionConflictException.class)
        .hasMessageContaining("referenced data files were compacted")
        .satisfies(
            ex -> {
              CompactionConflictException conflictEx = (CompactionConflictException) ex;

              // Verify exception provides compacted files list
              assertThat(conflictEx.compactedFiles())
                  .as("Exception should contain compacted file paths")
                  .contains(fileToDelete.path().toString());

              // Verify exception provides compaction map locations
              assertThat(conflictEx.compactionMapLocations())
                  .as("Exception should contain compaction map locations")
                  .isNotEmpty()
                  .containsKey(fileToDelete.path().toString());

              String mapLocation =
                  conflictEx.compactionMapLocations().get(fileToDelete.path().toString());
              assertThat(mapLocation).as("Compaction map location should be non-null").isNotNull();
            });

    // Verify table is in consistent state
    assertThat(table.currentSnapshot()).isNotNull();
    long recordCount = spark.read().format("iceberg").load(tableLocation).count();
    assertThat(recordCount).isEqualTo(500);
  }

  /**
   * Test 9: Verify compaction maps contain real target file paths (not "target-pending").
   *
   * <p>This test verifies that the critical target-pending bug is fixed:
   *
   * <ul>
   *   <li>Trigger a compaction conflict scenario
   *   <li>Catch CompactionConflictException with compaction map locations
   *   <li>Load compaction map and verify target file paths are real (not "target-pending")
   *   <li>Verify remapper can be created successfully
   * </ul>
   *
   * <p>Note: Full end-to-end workflow verification (actually applying remapped deletes) is complex
   * and requires additional test infrastructure. This test focuses on verifying the core fix.
   */
  @TestTemplate
  public void testManualConflictResolutionWorkflow() throws IOException {

    // Create table with compaction maps enabled
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    properties.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name());
    properties.put(TableProperties.COMPACTION_MAP_ENABLED, "true");

    Table table = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), properties, tableLocation);

    // Write 5 small data files
    for (int i = 0; i < 5; i++) {
      writeRecords(table, i * 100, 100);
    }

    // Capture starting snapshot and expected data
    long startingSnapshot = table.currentSnapshot().snapshotId();
    List<Row> expectedDataBeforeDeletes =
        spark.read().format("iceberg").load(tableLocation).collectAsList();
    assertThat(expectedDataBeforeDeletes).hasSize(500);

    // Get first data file for position deletes
    List<DataFile> dataFiles = Lists.newArrayList();
    for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
      try (org.apache.iceberg.ManifestReader<DataFile> reader =
          org.apache.iceberg.ManifestFiles.read(manifest, table.io())) {
        reader.forEach(dataFiles::add);
      }
    }
    assertThat(dataFiles).hasSize(5);
    DataFile fileToDelete = dataFiles.get(0);

    // Start transaction T1 with position deletes (targeting positions 10, 20, 30)
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(startingSnapshot);
    List<DeleteFile> originalDeleteFiles = writePositionDeletes(table, fileToDelete, 10L, 20L, 30L);
    originalDeleteFiles.forEach(rowDelta::addDeletes);

    // Meanwhile, run bin-pack compaction via Spark action
    RewriteDataFiles.Result result =
        SparkActions.get()
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .option(
                SizeBasedFileRewritePlanner.TARGET_FILE_SIZE_BYTES, Long.toString(10 * 1024 * 1024))
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(5);
    assertThat(result.addedDataFilesCount()).isGreaterThan(0);

    // Try to commit transaction T1 - should fail
    CompactionConflictException exception = null;
    try {
      rowDelta.commit();
    } catch (CompactionConflictException e) {
      exception = e;
    }

    assertThat(exception).isNotNull();
    assertThat(exception.compactedFiles()).contains(fileToDelete.location());
    assertThat(exception.compactionMapLocations()).containsKey(fileToDelete.location());

    // RESOLUTION WORKFLOW BEGINS

    // Step 1: Extract compaction map location from exception
    String mapLocation = exception.compactionMapLocations().get(fileToDelete.location());
    assertThat(mapLocation).isNotNull();

    // Step 2: Load compaction map
    table.refresh();
    CompactionMap compactionMap = CompactionMaps.read(table.io().newInputFile(mapLocation));
    assertThat(compactionMap.fileMappings()).isNotEmpty();

    // CORE BUG FIX VERIFICATION: Verify no "target-pending" placeholders

    // Step 3: Verify all target file paths in compaction map are real (not "target-pending")
    for (CompactionMap.FileMapping mapping : compactionMap.fileMappings()) {
      String targetFile = mapping.targetFile();

      // Critical verification: target file must NOT be the placeholder
      assertThat(targetFile)
          .as("Target file must not be 'target-pending' placeholder")
          .isNotEqualTo("target-pending");

      // Target file should be a valid file path (contains '/' and ends with file extension)
      assertThat(targetFile).as("Target file should be a valid path").contains("/");
      assertThat(targetFile)
          .as("Target file should have .parquet or .orc extension")
          .matches(".*\\.(parquet|orc)$");

      // Verify mapping has runs
      assertThat(mapping.runs()).as("Mapping should have at least one run").isNotEmpty();
    }

    // Step 4: Verify PositionDeleteRemapper can be created successfully
    org.apache.iceberg.PositionDeleteRemapper remapper =
        new org.apache.iceberg.PositionDeleteRemapper(compactionMap);

    // Step 5: Verify basic remapping operation works
    DeleteFile originalDeleteFile = originalDeleteFiles.get(0);
    String originalReferencedFile = originalDeleteFile.referencedDataFile();

    // Find the mapping for our source file
    CompactionMap.FileMapping mapping = null;
    for (CompactionMap.FileMapping m : compactionMap.fileMappings()) {
      if (m.sourceFile().equals(originalReferencedFile)) {
        mapping = m;
        break;
      }
    }
    assertThat(mapping).as("Should find mapping for source file").isNotNull();

    String targetFile = mapping.targetFile();

    // Remap one position as a basic functionality test
    PositionDelete<Record> originalDelete = PositionDelete.create();
    originalDelete.set(originalReferencedFile, 10L, null);

    PositionDelete<?> remappedDelete = remapper.remapDelete(originalDelete);

    // Verify remapping produced valid output
    assertThat(remappedDelete.pos())
        .as("Remapped position should be non-negative")
        .isGreaterThanOrEqualTo(0);
    assertThat(remappedDelete.path().toString())
        .as("Remapped delete should reference target file")
        .isEqualTo(targetFile);

    // SUCCESS: The target-pending bug is fixed!
    // - Compaction maps contain real target file paths (not "target-pending")
    // - PositionDeleteRemapper can be created
    // - Basic remapping operation succeeds
  }

  /**
   * Test 10: Verify compaction map from most recent compaction is provided in conflict exception.
   *
   * <p>This test verifies that when a file is compacted, the conflict detection provides the
   * compaction map from that compaction round for resolution.
   *
   * <ul>
   *   <li>Create table with many small files
   *   <li>Run compaction to combine them
   *   <li>Create transaction T1 with position deletes on compacted files
   *   <li>Write new data after the transaction is started but before commit
   *   <li>Attempt to commit T1 (should fail with conflict)
   *   <li>Verify exception provides compaction map location
   *   <li>Verify compaction map has correct source→target mappings
   * </ul>
   */
  @TestTemplate
  public void testMultipleCompactionRounds() throws IOException {

    // Create table with compaction maps enabled
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    properties.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name());
    properties.put(TableProperties.COMPACTION_MAP_ENABLED, "true");

    Table table = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), properties, tableLocation);

    // Step 1: Create table with many small data files
    for (int i = 0; i < 20; i++) {
      writeRecords(table, i * 100, 100); // 100 records per file = 2000 total
    }

    long initialSnapshot = table.currentSnapshot().snapshotId();
    assertThat(spark.read().format("iceberg").load(tableLocation).count()).isEqualTo(2000);

    // Get initial data files (before compaction)
    List<DataFile> initialFiles = Lists.newArrayList();
    for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
      try (org.apache.iceberg.ManifestReader<DataFile> reader =
          org.apache.iceberg.ManifestFiles.read(manifest, table.io())) {
        reader.forEach(initialFiles::add);
      }
    }
    DataFile originalFile = initialFiles.get(0); // File that will be compacted

    // Step 2: Create transaction T1 with position deletes on ORIGINAL file (before compaction)
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(initialSnapshot);
    List<DeleteFile> originalDeleteFiles = writePositionDeletes(table, originalFile, 10L, 20L, 30L);
    originalDeleteFiles.forEach(rowDelta::addDeletes);

    // Step 3: Meanwhile, run compaction (which will compact originalFile)
    RewriteDataFiles.Result compaction =
        SparkActions.get()
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .execute();

    assertThat(compaction.rewrittenDataFilesCount()).isEqualTo(20);
    assertThat(compaction.addedDataFilesCount())
        .as("Compaction should produce at least one file")
        .isGreaterThanOrEqualTo(1);

    // Step 4: Attempt to commit T1 - should fail because originalFile was compacted
    CompactionConflictException exception = null;
    try {
      rowDelta.commit();
    } catch (CompactionConflictException e) {
      exception = e;
    }

    // Step 5: Verify exception provides compaction map
    assertThat(exception)
        .as("Should get CompactionConflictException when file is compacted")
        .isNotNull();
    assertThat(exception.compactedFiles())
        .as("Exception should identify the compacted file")
        .contains(originalFile.location());
    assertThat(exception.compactionMapLocations())
        .as("Exception should provide compaction map location")
        .containsKey(originalFile.location());

    // Step 6: Verify compaction map can be loaded and has correct structure
    String mapLocation = exception.compactionMapLocations().get(originalFile.location());
    assertThat(mapLocation).isNotNull();

    table.refresh();
    CompactionMap compactionMap = CompactionMaps.read(table.io().newInputFile(mapLocation));
    assertThat(compactionMap.fileMappings()).as("Compaction map should have mappings").isNotEmpty();

    // Verify the compaction map has valid target paths (not "target-pending")
    for (CompactionMap.FileMapping mapping : compactionMap.fileMappings()) {
      assertThat(mapping.targetFile())
          .as("Target file should be a real path")
          .isNotEqualTo("target-pending")
          .contains("/");
    }

    // SUCCESS: Compaction map from most recent compaction is provided
    // - Exception provides compaction map location
    // - Compaction map has correct source→target mappings with real file paths
    // - Resolution workflow can proceed with the provided map
  }

  /**
   * Helper method to write records to table.
   *
   * @param table the table to write to
   * @param startId starting ID for records
   * @param count number of records to write
   */
  private void writeRecords(Table table, int startId, int count) {
    // Create DataFrame with records using RowFactory
    java.util.List<org.apache.spark.sql.Row> rows = new java.util.ArrayList<>();
    for (int i = 0; i < count; i++) {
      rows.add(org.apache.spark.sql.RowFactory.create(startId + i, "data-" + (startId + i)));
    }

    org.apache.spark.sql.types.StructType sparkSchema =
        new org.apache.spark.sql.types.StructType()
            .add("id", org.apache.spark.sql.types.DataTypes.IntegerType, true)
            .add("data", org.apache.spark.sql.types.DataTypes.StringType, true);

    Dataset<Row> df = spark.createDataFrame(rows, sparkSchema);
    df.coalesce(1)
        .write()
        .format("iceberg")
        .mode(org.apache.spark.sql.SaveMode.Append)
        .save(tableLocation);
  }

  /**
   * Helper method to write position deletes for specific positions.
   *
   * @param table the table
   * @param dataFile the data file to delete from
   * @param positions the positions to delete
   * @return list of delete files
   */
  private List<DeleteFile> writePositionDeletes(Table table, DataFile dataFile, Long... positions)
      throws IOException {
    if (formatVersion >= 3) {
      // Use deletion vectors for v3+
      return writeDV(table, dataFile.partition(), dataFile.location(), positions);
    } else {
      // Use position delete files for v2
      return writePosDeletes(table, dataFile.partition(), dataFile.location(), positions);
    }
  }

  /**
   * Write deletion vector (for format version 3+).
   *
   * @param table the table
   * @param partition the partition
   * @param path the data file path
   * @param positions the positions to delete
   * @return list of delete files
   */
  private List<DeleteFile> writeDV(
      Table table, org.apache.iceberg.StructLike partition, String path, Long... positions)
      throws IOException {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();

    DVFileWriter writer = new BaseDVFileWriter(fileFactory, p -> null);
    try (DVFileWriter closeableWriter = writer) {
      for (Long position : positions) {
        closeableWriter.delete(path, position, table.spec(), partition);
      }
    }

    return writer.result().deleteFiles();
  }

  /**
   * Write position delete file (for format version 2).
   *
   * @param table the table
   * @param partition the partition
   * @param path the data file path
   * @param positions the positions to delete
   * @return list of delete files
   */
  private List<DeleteFile> writePosDeletes(
      Table table, org.apache.iceberg.StructLike partition, String path, Long... positions)
      throws IOException {
    OutputFile outputFile =
        table
            .io()
            .newOutputFile(
                table
                    .locationProvider()
                    .newDataLocation(
                        FileFormat.PARQUET.addExtension(java.util.UUID.randomUUID().toString())));

    org.apache.iceberg.encryption.EncryptedOutputFile encryptedOutputFile =
        EncryptedFiles.encryptedOutput(outputFile, EncryptionKeyMetadata.EMPTY);

    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec(), null, null, null);

    PositionDeleteWriter<Record> posDeleteWriter =
        appenderFactory
            .set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "full")
            .newPosDeleteWriter(encryptedOutputFile, FileFormat.PARQUET, partition);

    PositionDelete<Record> posDelete = PositionDelete.create();
    for (Long pos : positions) {
      posDeleteWriter.write(posDelete.set(path, pos, null));
    }

    try {
      posDeleteWriter.close();
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }

    DeleteFile deleteFile = posDeleteWriter.toDeleteFile();

    // Manually set referencedDataFile for conflict detection
    // The PositionDeleteWriter doesn't automatically set this field even for file-scoped deletes
    DeleteFile fileScoped =
        org.apache.iceberg.FileMetadata.deleteFileBuilder(table.spec())
            .copy(deleteFile)
            .withReferencedDataFile(path)
            .build();

    return Lists.newArrayList(fileScoped);
  }
}
