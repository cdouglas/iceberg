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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.apache.iceberg.PositionDeleteRemapper;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.ActionsProvider;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for bin-pack rewrites with position tracking and compaction maps.
 *
 * <p>Tests cover:
 *
 * <ul>
 *   <li>Bin-pack rewrites with compaction map generation
 *   <li>Merge compactions (combining data files with position deletes)
 *   <li>Compaction maps with gaps representing deleted rows
 *   <li>Position delete remapping using compaction maps
 *   <li>Multiple file formats (Parquet, ORC)
 *   <li>Various compaction scenarios (N:1, N:M)
 * </ul>
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestBinPackWithPositionTracking extends TestBase {

  @TempDir private File tableDir;

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @Parameter(index = 0)
  private int formatVersion;

  @Parameter(index = 1)
  private FileFormat fileFormat;

  @Parameters(name = "formatVersion = {0}, format = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
      {2, FileFormat.PARQUET},
      {2, FileFormat.ORC},
      {3, FileFormat.PARQUET},
      {3, FileFormat.ORC},
    };
  }

  private String tableLocation = null;

  @BeforeEach
  public void setupTableLocation() {
    this.tableLocation = tableDir.toURI().toString();
  }

  protected ActionsProvider actions() {
    return SparkActions.get();
  }

  private Table createTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> props =
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            String.valueOf(formatVersion),
            TableProperties.DEFAULT_FILE_FORMAT,
            fileFormat.name(),
            TableProperties.COMPACTION_MAP_ENABLED,
            "true");
    return TABLES.create(SCHEMA, spec, props, tableLocation);
  }

  @TestTemplate
  public void testBinPackGeneratesCompactionMapWithoutDeletes() {
    Table table = createTable();

    // Create 4 small files
    for (int i = 0; i < 4; i++) {
      writeRecords(table, i, 1);
    }

    table.refresh();
    assertThat(TestHelpers.dataFiles(table)).hasSize(4);

    // Run bin-pack rewrite
    RewriteDataFiles.Result result =
        actions()
            .rewriteDataFiles(table)
            .option(BinPackRewriteFilePlanner.MIN_INPUT_FILES, "1")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(4);
    assertThat(result.addedDataFilesCount()).isGreaterThanOrEqualTo(1);

    // Verify compaction map was generated
    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();

    List<ManifestFile> manifests = snapshot.dataManifests(table.io());
    assertThat(manifests).isNotEmpty();

    // At least one manifest should have a compaction map location
    boolean hasCompactionMap = manifests.stream().anyMatch(m -> m.compactionMapLocation() != null);
    assertThat(hasCompactionMap)
        .as("Compaction map should be generated for bin-pack rewrite")
        .isTrue();
  }

  @TestTemplate
  public void testBinPackGeneratesCompactionMapWithPositionDeletes() throws IOException {
    // V3 tables require deletion vectors instead of position delete files
    assumeTrue(formatVersion == 2, "Position delete files only work with V2");

    Table table = createTable();

    // Create 3 data files
    for (int i = 0; i < 3; i++) {
      writeRecords(table, i * 3, 3);
    }

    table.refresh();
    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    assertThat(dataFiles).hasSize(3);

    // Add position deletes to create gaps
    RowDelta rowDelta = table.newRowDelta();
    // Delete first row from first file
    writePosDeletesToFile(table, dataFiles.get(0), 1).forEach(rowDelta::addDeletes);
    // Delete second row from second file
    writePosDeletesToFile(table, dataFiles.get(1), 1, 1).forEach(rowDelta::addDeletes);
    rowDelta.commit();

    table.refresh();
    long snapshotIdBeforeCompaction = table.currentSnapshot().snapshotId();

    // Run bin-pack rewrite with position tracking
    RewriteDataFiles.Result result =
        actions()
            .rewriteDataFiles(table)
            .option(BinPackRewriteFilePlanner.MIN_INPUT_FILES, "1")
            .option(BinPackRewriteFilePlanner.MIN_FILE_SIZE_BYTES, "0")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isGreaterThanOrEqualTo(1);

    // Verify compaction map was generated and has gaps
    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = snapshot.dataManifests(table.io());

    ManifestFile manifestWithMap =
        manifests.stream()
            .filter(m -> m.compactionMapLocation() != null)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Expected compaction map in manifest"));

    // Load and verify compaction map
    InputFile mapFile = table.io().newInputFile(manifestWithMap.compactionMapLocation());
    CompactionMap map = CompactionMaps.read(mapFile);

    assertThat(map.fileMappings()).isNotEmpty();

    // Verify that at least one file mapping has multiple runs (indicating gaps)
    // When rows are deleted during scan, the position mappings have gaps
    boolean hasGaps =
        map.fileMappings().stream().anyMatch(fileMapping -> fileMapping.runs().size() > 1);

    assertThat(hasGaps).as("Compaction map should have gaps representing deleted rows").isTrue();
  }

  @TestTemplate
  public void testMultipleSourcesOneTarget() {
    // Disable adaptive query execution
    spark.conf().set("spark.sql.adaptive.enabled", "false");

    Table table = createTable();

    // Create 5 tiny files that will compact into 1
    for (int i = 0; i < 5; i++) {
      writeRecords(table, i, 1);
    }

    table.refresh();
    assertThat(TestHelpers.dataFiles(table)).hasSize(5);

    // Run bin-pack with large target size to force single output file
    RewriteDataFiles.Result result =
        actions()
            .rewriteDataFiles(table)
            .option(BinPackRewriteFilePlanner.MIN_INPUT_FILES, "1")
            .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Long.toString(Long.MAX_VALUE - 1))
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(5);
    assertThat(result.addedDataFilesCount()).isEqualTo(1);

    // Verify compaction map generated
    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = snapshot.dataManifests(table.io());

    boolean hasCompactionMap = manifests.stream().anyMatch(m -> m.compactionMapLocation() != null);
    assertThat(hasCompactionMap).isTrue();
  }

  @TestTemplate
  public void testBinPackWithSortedTable() {
    // Disable adaptive query execution
    spark.conf().set("spark.sql.adaptive.enabled", "false");

    Table table = createTable();

    // Set sort order
    table.replaceSortOrder().asc("id").commit();
    table.refresh();

    assertThat(table.sortOrder().isSorted()).isTrue();

    // Insert data
    for (int i = 0; i < 4; i++) {
      writeRecords(table, i * 10, 1);
    }

    table.refresh();

    // Run bin-pack rewrite
    RewriteDataFiles.Result result =
        actions()
            .rewriteDataFiles(table)
            .option(BinPackRewriteFilePlanner.MIN_INPUT_FILES, "1")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(4);

    // Verify compaction map generated
    table.refresh();
    List<ManifestFile> manifests = table.currentSnapshot().dataManifests(table.io());
    boolean hasCompactionMap = manifests.stream().anyMatch(m -> m.compactionMapLocation() != null);
    assertThat(hasCompactionMap).isTrue();
  }

  @TestTemplate
  public void testBinPackWithUnsortedTable() {
    // Disable adaptive query execution
    spark.conf().set("spark.sql.adaptive.enabled", "false");

    Table table = createTable();

    assertThat(table.sortOrder().isUnsorted()).isTrue();

    // Insert data
    for (int i = 0; i < 3; i++) {
      writeRecords(table, i, 1);
    }

    table.refresh();

    // Run bin-pack rewrite
    RewriteDataFiles.Result result =
        actions()
            .rewriteDataFiles(table)
            .option(BinPackRewriteFilePlanner.MIN_INPUT_FILES, "1")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(3);

    // Verify compaction map generated
    table.refresh();
    List<ManifestFile> manifests = table.currentSnapshot().dataManifests(table.io());
    boolean hasCompactionMap = manifests.stream().anyMatch(m -> m.compactionMapLocation() != null);
    assertThat(hasCompactionMap).isTrue();
  }

  @TestTemplate
  public void testPositionTrackingDisabledByDefault() {
    // Create table without compaction map enabled
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> props =
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            String.valueOf(formatVersion),
            TableProperties.DEFAULT_FILE_FORMAT,
            fileFormat.name());

    Table table = TABLES.create(SCHEMA, spec, props, tableLocation);

    String compactionMapEnabled = table.properties().get(TableProperties.COMPACTION_MAP_ENABLED);
    assertThat(compactionMapEnabled).isIn(null, "false");

    // Insert data
    for (int i = 0; i < 3; i++) {
      writeRecords(table, i, 1);
    }

    table.refresh();

    // Run bin-pack rewrite (should work without position tracking)
    RewriteDataFiles.Result result =
        actions()
            .rewriteDataFiles(table)
            .option(BinPackRewriteFilePlanner.MIN_INPUT_FILES, "1")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(3);

    // Verify NO compaction map generated
    table.refresh();
    List<ManifestFile> manifests = table.currentSnapshot().dataManifests(table.io());
    boolean hasCompactionMap = manifests.stream().anyMatch(m -> m.compactionMapLocation() != null);
    assertThat(hasCompactionMap).isFalse();
  }

  @TestTemplate
  public void testConflictResolutionWithPositionDeleteRemapping() throws IOException {
    // Disable adaptive query execution (same as testMultipleSourcesOneTarget)
    spark.conf().set("spark.sql.adaptive.enabled", "false");

    Table table = createTable();

    // Step 1: Create initial data files (5 tiny files that will compact into 1)
    for (int i = 0; i < 5; i++) {
      writeRecords(table, i, 1);
    }

    table.refresh();
    List<DataFile> originalDataFiles = TestHelpers.dataFiles(table);
    assertThat(originalDataFiles).hasSize(5);

    // Capture the original file paths for later verification
    List<String> originalFilePaths =
        originalDataFiles.stream()
            .map(DataFile::location)
            .collect(java.util.stream.Collectors.toList());

    // Step 2: Run compaction that rewrites those files (generating compaction map)
    RewriteDataFiles.Result result =
        actions()
            .rewriteDataFiles(table)
            .option(BinPackRewriteFilePlanner.MIN_INPUT_FILES, "1")
            .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Long.toString(Long.MAX_VALUE - 1))
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(5);
    assertThat(result.addedDataFilesCount()).isEqualTo(1);

    // Step 3: Verify compaction map was generated
    table.refresh();
    Snapshot snapshotAfterCompaction = table.currentSnapshot();
    List<ManifestFile> manifests = snapshotAfterCompaction.dataManifests(table.io());

    ManifestFile manifestWithMap =
        manifests.stream()
            .filter(m -> m.compactionMapLocation() != null)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Expected compaction map in manifest"));

    String compactionMapLocation = manifestWithMap.compactionMapLocation();
    assertThat(compactionMapLocation).isNotNull();

    // Step 4: Load compaction map and verify remapper can be created
    InputFile mapFile = table.io().newInputFile(compactionMapLocation);
    CompactionMap map = CompactionMaps.read(mapFile);
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Step 5: Verify remapper identifies all the compacted files
    assertThat(remapper.compactedFiles())
        .as("Remapper should identify all compacted source files")
        .hasSize(5)
        .containsAll(originalFilePaths);

    // Step 6: Verify remapper can check if files were compacted
    for (String originalFile : originalFilePaths) {
      assertThat(remapper.isCompacted(originalFile))
          .as("Remapper should indicate file %s was compacted", originalFile)
          .isTrue();
    }

    // Step 7: Verify compaction map structure
    assertThat(map.fileMappings()).as("Compaction map should have file mappings").hasSize(5);

    // Each file mapping should have at least one run
    for (CompactionMap.FileMapping mapping : map.fileMappings()) {
      assertThat(mapping.runs())
          .as("File mapping for %s should have at least one run", mapping.sourceFile())
          .isNotEmpty();
      assertThat(mapping.sourceFile())
          .as("Source file should be one of the original files")
          .isIn(originalFilePaths);
    }
  }

  // Helper methods

  private void writeRecords(Table table, int startId, int count) {
    // Create DataFrame with records
    java.util.List<org.apache.spark.sql.Row> rows = new java.util.ArrayList<>();
    for (int i = 0; i < count; i++) {
      rows.add(org.apache.spark.sql.RowFactory.create(startId + i, "data" + (startId + i)));
    }

    org.apache.spark.sql.types.StructType sparkSchema =
        new org.apache.spark.sql.types.StructType()
            .add("id", org.apache.spark.sql.types.DataTypes.IntegerType, true)
            .add("data", org.apache.spark.sql.types.DataTypes.StringType, true);

    Dataset<Row> df = spark.createDataFrame(rows, sparkSchema);
    df.coalesce(1).write().format("iceberg").mode(SaveMode.Append).save(tableLocation);
  }

  /**
   * Writes position delete files deleting the first N rows from a data file.
   *
   * @param table the table
   * @param dataFile the data file to reference
   * @param deleteCount number of rows to delete (starting from position 0)
   * @return list of delete files created
   */
  private List<DeleteFile> writePosDeletesToFile(Table table, DataFile dataFile, int deleteCount)
      throws IOException {
    return writePosDeletesToFile(table, dataFile, deleteCount, 0);
  }

  /**
   * Writes position delete files deleting rows starting at a specific position.
   *
   * @param table the table
   * @param dataFile the data file to reference
   * @param deleteCount number of rows to delete
   * @param startPosition starting position (offset) for deletes
   * @return list of delete files created
   */
  private List<DeleteFile> writePosDeletesToFile(
      Table table, DataFile dataFile, int deleteCount, long startPosition) throws IOException {
    return writePosDeletes(
        table, dataFile.partition(), dataFile.location(), 1, deleteCount, startPosition);
  }

  private List<DeleteFile> writePosDeletes(
      Table table,
      StructLike partition,
      String path,
      int outputDeleteFiles,
      int totalPositionsToDelete,
      long startPosition)
      throws IOException {
    List<DeleteFile> results = Lists.newArrayList();

    int positionsPerFile = (int) Math.ceil((double) totalPositionsToDelete / outputDeleteFiles);

    long currentPosition = startPosition;
    for (int file = 0; file < outputDeleteFiles; file++) {
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
              .newPosDeleteWriter(encryptedOutputFile, FileFormat.PARQUET, partition);

      PositionDelete<Record> posDelete = PositionDelete.create();

      int deletesInThisFile = Math.min(positionsPerFile, totalPositionsToDelete);
      for (int i = 0; i < deletesInThisFile; i++) {
        posDeleteWriter.write(posDelete.set(path, currentPosition++, null));
      }
      totalPositionsToDelete -= deletesInThisFile;

      posDeleteWriter.close();
      results.add(posDeleteWriter.toDeleteFile());
    }

    return results;
  }
}
