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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteDataFiles.Result;
import org.apache.iceberg.actions.RewritePositionDeleteFiles;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.exceptions.CompactionConflictException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for compaction maps with Spark rewrite actions.
 *
 * <p>These tests verify that:
 *
 * <ol>
 *   <li>Compaction maps are automatically generated during rewrite operations
 *   <li>Position mappings are correctly tracked for bin-pack rewrites
 *   <li>Concurrent position deletes are detected and handled
 *   <li>Compaction maps work with both PARQUET and ORC formats
 * </ol>
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestRewriteDataFilesActionWithCompactionMaps extends TestBase {

  @TempDir private File tableDir;

  private static final int SCALE = 400000;
  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "c1", Types.IntegerType.get()),
          Types.NestedField.optional(2, "c2", Types.StringType.get()),
          Types.NestedField.optional(3, "c3", Types.StringType.get()));

  @Parameter(index = 0)
  private FileFormat fileFormat;

  @Parameters(name = "fileFormat = {0}")
  protected static List<Object[]> parameters() {
    return Lists.newArrayList(new Object[] {FileFormat.PARQUET}, new Object[] {FileFormat.ORC});
  }

  private String tableLocation = null;

  @BeforeAll
  public static void setupSpark() {
    // disable AQE as tests assume that writes generate a particular number of files
    spark.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false");
  }

  @BeforeEach
  public void setupTableLocation() {
    this.tableLocation = tableDir.toURI().toString();
  }

  /**
   * Create a V2 table with compaction maps enabled.
   *
   * @return the created table
   */
  protected Table createTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options =
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            "2",
            TableProperties.COMPACTION_MAP_ENABLED,
            "true",
            TableProperties.DEFAULT_FILE_FORMAT,
            fileFormat.name());

    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);
    assertThat(table.currentSnapshot()).as("Table must be empty").isNull();
    return table;
  }

  /**
   * Create a V2 table with compaction maps enabled and write data files.
   *
   * @param files number of files to create
   * @return the created table
   */
  protected Table createTable(int files) {
    Table table = createTable();
    writeRecords(files, SCALE);
    return table;
  }

  /**
   * Write records to the table in the specified number of files.
   *
   * @param files number of files to create
   * @param numRecords total number of records
   */
  private void writeRecords(int files, int numRecords) {
    List<ThreeColumnRecord> records = Lists.newArrayList();
    int rowDimension = (int) Math.ceil(Math.sqrt(numRecords));

    IntStream.range(0, rowDimension)
        .forEach(
            x ->
                IntStream.range(0, rowDimension)
                    .forEach(y -> records.add(new ThreeColumnRecord(x, "foo" + x, "bar" + y))));

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).repartition(files);
    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .option(SparkWriteOptions.FANOUT_ENABLED, "true")
        .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
        .save(tableLocation);
  }

  /**
   * Get current data from the table.
   *
   * @return list of rows
   */
  private List<Object[]> currentData() {
    return rowsToJava(
        spark.read().format("iceberg").load(tableLocation).sort("c1", "c2", "c3").collectAsList());
  }

  /**
   * Assert that the table has the expected number of data files.
   *
   * @param table the table to check
   * @param expectedFiles expected number of files
   */
  private void shouldHaveFiles(Table table, int expectedFiles) {
    table.refresh();
    long dataFiles = Streams.stream(table.currentSnapshot().addedDataFiles(table.io())).count();
    assertThat(dataFiles)
        .as("Table should have %d data files", expectedFiles)
        .isEqualTo(expectedFiles);
  }

  /**
   * Get all data files from the current snapshot.
   *
   * @param table the table
   * @return list of data files
   */
  private List<DataFile> getDataFiles(Table table) {
    table.refresh();
    return Lists.newArrayList(table.currentSnapshot().addedDataFiles(table.io()));
  }

  @TestTemplate
  public void testRewriteGeneratesCompactionMap() throws IOException {
    // 1. Create table with compaction maps enabled
    Table table = createTable(5);
    shouldHaveFiles(table, 5);
    List<Object[]> expectedRecords = currentData();
    long startingSnapshot = table.currentSnapshot().snapshotId();

    // 2. Execute rewrite action
    Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "2")
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .execute();

    assertThat(result.rewrittenDataFilesCount())
        .as("Action should rewrite 5 data files")
        .isEqualTo(5);
    assertThat(result.addedDataFilesCount()).as("Action should add 1 data file").isOne();

    shouldHaveFiles(table, 1);

    // 3. Verify data is preserved
    List<Object[]> actualRecords = currentData();
    assertThat(actualRecords)
        .as("Rows must match after rewrite")
        .containsExactlyElementsOf(expectedRecords);

    // 4. Verify compaction map exists
    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = snapshot.dataManifests(table.io());

    ManifestFile addedManifest =
        manifests.stream()
            .filter(ManifestFile::hasAddedFiles)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No manifest with added files found"));

    assertThat(addedManifest.compactionMapLocation())
        .as("Manifest should have compaction map location")
        .isNotNull();

    // 5. Load and verify map structure
    CompactionMap map =
        CompactionMaps.read(table.io().newInputFile(addedManifest.compactionMapLocation()));

    assertThat(map).as("Compaction map should be readable").isNotNull();
    assertThat(map.sourceSnapshotId())
        .as("Map should reference starting snapshot")
        .isEqualTo(startingSnapshot);
    assertThat(map.targetSnapshotId())
        .as("Map should reference the compaction's snapshot")
        .isEqualTo(snapshot.snapshotId());
    assertThat(map.fileMappings())
        .as("Map should have 5 file mappings (one per source file)")
        .hasSize(5);

    // Verify each mapping has a run
    map.fileMappings()
        .forEach(
            mapping -> {
              assertThat(mapping.runs())
                  .as("Each file mapping should have at least one run")
                  .isNotEmpty();
            });
  }

  @TestTemplate
  public void testBinPackMultipleFiles() throws IOException {
    // Test that bin-packing multiple source files into a single target creates correct mappings
    Table table = createTable(3);
    shouldHaveFiles(table, 3);

    List<DataFile> sourceFiles = getDataFiles(table);
    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Execute bin-pack rewrite
    Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(3);
    assertThat(result.addedDataFilesCount()).isOne();

    // Verify compaction map has correct structure
    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    ManifestFile manifest =
        snapshot.dataManifests(table.io()).stream()
            .filter(ManifestFile::hasAddedFiles)
            .findFirst()
            .orElseThrow();

    CompactionMap map =
        CompactionMaps.read(table.io().newInputFile(manifest.compactionMapLocation()));

    assertThat(map.fileMappings()).hasSize(3);

    // Verify all source files are mapped
    List<String> mappedSourceFiles =
        map.fileMappings().stream()
            .map(CompactionMap.FileMapping::sourceFile)
            .collect(Collectors.toList());

    List<String> expectedSourceFiles =
        sourceFiles.stream().map(f -> f.path().toString()).collect(Collectors.toList());

    assertThat(mappedSourceFiles).containsExactlyInAnyOrderElementsOf(expectedSourceFiles);

    // All mappings should point to the same target file
    List<String> targetFiles =
        map.fileMappings().stream()
            .map(CompactionMap.FileMapping::targetFile)
            .distinct()
            .collect(Collectors.toList());

    assertThat(targetFiles).as("All mappings should point to same target file").hasSize(1);
  }

  @TestTemplate
  public void testCompactionMapsDisabled() throws IOException {
    // Verify that when compaction maps are disabled, no map is generated
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options =
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            "2",
            TableProperties.COMPACTION_MAP_ENABLED,
            "false",
            TableProperties.DEFAULT_FILE_FORMAT,
            fileFormat.name());

    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    // Write and rewrite data
    writeRecords(3, SCALE);

    Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isGreaterThan(0);

    // Verify no compaction map location
    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = snapshot.dataManifests(table.io());

    for (ManifestFile manifest : manifests) {
      assertThat(manifest.compactionMapLocation())
          .as("Manifest should not have compaction map when feature disabled")
          .isNull();
    }
  }

  /**
   * A sort rewrite reorders rows, so it records no positions and writes no map. It still reads the
   * rewrite table of a table that keeps maps, source positions included, and must write only the
   * table's columns.
   */
  @TestTemplate
  public void testSortRewriteWritesNoMap() {
    Table table = createTable(3);
    table.refresh();
    List<Object[]> expected = currentData();

    Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .sort(SortOrder.builderFor(table.schema()).asc("c2").build())
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .execute();

    assertReorderedWithoutMap(table, result, expected);
  }

  /** As a sort rewrite: a z-order rewrite reorders rows, and writes no map. */
  @TestTemplate
  public void testZOrderRewriteWritesNoMap() {
    Table table = createTable(3);
    table.refresh();
    List<Object[]> expected = currentData();

    Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .zOrder("c2", "c3")
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .execute();

    assertReorderedWithoutMap(table, result, expected);
  }

  /**
   * The position deletes table keeps no compaction map of its own, so rewriting a table's delete
   * files reads no source positions and writes as it would without maps.
   */
  @TestTemplate
  public void testPositionDeleteRewriteReadsNoSourcePositions() throws IOException {
    Table table = createTable(1);
    table.refresh();
    DataFile dataFile = getDataFiles(table).get(0);
    RowDelta rowDelta = table.newRowDelta();
    for (long pos = 0; pos < 2; pos += 1) {
      rowDelta.addDeletes(
          FileHelpers.writeDeleteFile(
                  table,
                  table
                      .io()
                      .newOutputFile(
                          tableLocation + "/data/" + fileFormat.addExtension("deletes-" + pos)),
                  ImmutableList.of(Pair.of(dataFile.location(), pos)))
              .first());
    }

    rowDelta.commit();
    List<Object[]> expected = currentData();

    RewritePositionDeleteFiles.Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .execute();

    assertThat(result.rewrittenDeleteFilesCount()).isEqualTo(2);
    assertThat(result.addedDeleteFilesCount()).isOne();
    assertEquals("Rows must match", expected, currentData());
  }

  private void assertReorderedWithoutMap(Table table, Result result, List<Object[]> expected) {
    assertThat(result.rewrittenDataFilesCount()).isGreaterThan(0);
    table.refresh();
    for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
      assertThat(manifest.compactionMapLocation())
          .as("A reordering rewrite writes no compaction map")
          .isNull();
    }

    assertThat(table.schema().asStruct()).isEqualTo(SCHEMA.asStruct());
    assertEquals("Rows must match", expected, currentData());
  }

  @TestTemplate
  public void testConcurrentDeletesDetected() throws IOException {
    // Test that concurrent position deletes are detected when compaction occurs
    Table table = createTable(3);
    List<DataFile> sourceFiles = getDataFiles(table);
    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Start a RowDelta transaction with position deletes
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(startingSnapshot);

    DataFile fileToDelete = sourceFiles.get(0);
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath(tableLocation + "/deletes-" + System.currentTimeMillis() + ".parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .withReferencedDataFile(fileToDelete.path().toString())
            .build();

    rowDelta.addDeletes(deleteFile);

    // Meanwhile, compact the table with Spark action
    Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(3);

    // Verify compaction map was generated
    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    ManifestFile manifest =
        snapshot.dataManifests(table.io()).stream()
            .filter(ManifestFile::hasAddedFiles)
            .findFirst()
            .orElseThrow();

    assertThat(manifest.compactionMapLocation()).isNotNull();

    // Try to commit the RowDelta - should fail with CompactionConflictException
    CompactionConflictException exception =
        assertThrows(CompactionConflictException.class, () -> rowDelta.commit());

    assertThat(exception.getMessage()).contains("referenced data files were compacted");
    assertThat(exception.compactedFiles()).contains(fileToDelete.path().toString());
    assertThat(exception.compactionMapLocations())
        .as("Exception should provide compaction map locations for resolution")
        .isNotEmpty()
        .containsKey(fileToDelete.path().toString());
  }

  @TestTemplate
  public void testRewriteWithNoFiles() {
    // Test that rewriting an empty table doesn't create a compaction map
    Table table = createTable();

    assertThat(table.currentSnapshot()).as("Table must be empty").isNull();

    Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isZero();
    assertThat(result.addedDataFilesCount()).isZero();
    assertThat(table.currentSnapshot()).as("Table must stay empty").isNull();
  }

  @TestTemplate
  public void testRewritePreservesFileFormat() throws IOException {
    // Verify that rewrite preserves the file format and compaction maps work with it
    Table table = createTable(5);
    shouldHaveFiles(table, 5);

    Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "2")
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .execute();

    // Verify some files were rewritten
    assertThat(result.rewrittenDataFilesCount()).as("Should rewrite files").isGreaterThan(0);

    // Verify new files use the correct format
    List<DataFile> newFiles = getDataFiles(table);
    assertThat(newFiles).isNotEmpty();

    DataFile newFile = newFiles.get(0);
    assertThat(newFile.format()).as("New file should use configured format").isEqualTo(fileFormat);

    // Verify compaction map exists
    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    ManifestFile manifest =
        snapshot.dataManifests(table.io()).stream()
            .filter(ManifestFile::hasAddedFiles)
            .findFirst()
            .orElseThrow();

    assertThat(manifest.compactionMapLocation())
        .as("Compaction map should exist for %s format", fileFormat)
        .isNotNull();

    // Verify map is readable
    CompactionMap map =
        CompactionMaps.read(table.io().newInputFile(manifest.compactionMapLocation()));
    assertThat(map.fileMappings()).as("Map should have file mappings").isNotEmpty();
  }

  /**
   * A bin-pack into a new partition spec shuffles its rows into their new partitions, which
   * reorders them as a sort does, so it writes no map. It once tracked them: a task wrote several
   * partitions, which a fanout writer interleaves, and the map sent rows to the wrong positions.
   */
  @TestTemplate
  public void testBinPackIntoANewSpecWritesNoMap() {
    Table table = createTable();
    writeRows(table, 16);
    table.updateSpec().addField("c1").commit();
    List<Object[]> expected = currentData();

    // Few shuffle partitions, so each task writes several partitions
    String shufflePartitions = spark.conf().get(SQLConf.SHUFFLE_PARTITIONS().key());
    spark.conf().set(SQLConf.SHUFFLE_PARTITIONS().key(), "2");
    Result result;
    try {
      result =
          SparkActions.get(spark)
              .rewriteDataFiles(table)
              .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
              .execute();
    } finally {
      spark.conf().set(SQLConf.SHUFFLE_PARTITIONS().key(), shufflePartitions);
    }

    assertReorderedWithoutMap(table, result, expected);
  }

  /** A bin-pack of a partitioned table maps each partition's rows to where they landed. */
  @TestTemplate
  public void testBinPackOfAPartitionedTableMapsEveryRow() {
    Map<String, String> options =
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            "2",
            TableProperties.COMPACTION_MAP_ENABLED,
            "true",
            TableProperties.DEFAULT_FILE_FORMAT,
            fileFormat.name());
    Table table =
        TABLES.create(
            SCHEMA,
            PartitionSpec.builderFor(SCHEMA).identity("c1").build(),
            options,
            tableLocation);
    writeRows(table, 2);
    Map<Pair<String, Long>, String> before = rowsByPosition();

    Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .execute();
    assertThat(result.rewrittenDataFilesCount()).isGreaterThan(2);

    table.refresh();
    // Manifests of one commit can share a map
    Set<String> mapLocations =
        table.currentSnapshot().dataManifests(table.io()).stream()
            .map(ManifestFile::compactionMapLocation)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    List<CompactionMap.FileMapping> mappings = Lists.newArrayList();
    for (String location : mapLocations) {
      mappings.addAll(CompactionMaps.read(table.io().newInputFile(location)).fileMappings());
    }

    assertThat(mappings).hasSize(result.rewrittenDataFilesCount());
    assertMapsEveryRow(mappings, before);
  }

  /**
   * Writes 4000 rows in four tasks, each writing every partition, with unique c2 and c1 cycling
   * through the given number of values.
   */
  private void writeRows(Table table, int c1Values) {
    List<ThreeColumnRecord> records =
        IntStream.range(0, 4000)
            .mapToObj(i -> new ThreeColumnRecord(i % c1Values, "row-" + i, "bar"))
            .collect(Collectors.toList());
    spark
        .createDataFrame(records, ThreeColumnRecord.class)
        .repartition(4)
        .select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .option(SparkWriteOptions.FANOUT_ENABLED, "true")
        .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
        .save(tableLocation);
    table.refresh();
  }

  /**
   * Checks that the mappings send each live source row to the target position that now holds it.
   */
  private void assertMapsEveryRow(
      List<CompactionMap.FileMapping> mappings, Map<Pair<String, Long>, String> before) {
    Map<Pair<String, Long>, String> after = rowsByPosition();
    assertThat(after).hasSameSizeAs(before);

    Map<Pair<String, Long>, String> mapped = Maps.newHashMap();
    for (CompactionMap.FileMapping mapping : mappings) {
      for (CompactionMap.Run run : mapping.runs()) {
        String targetFile = run.targetFile() != null ? run.targetFile() : mapping.targetFile();
        for (long offset = 0; offset < run.length(); offset += 1) {
          String row = before.get(Pair.of(mapping.sourceFile(), run.sourcePosition() + offset));
          assertThat(row).as("Mapped source row must be live").isNotNull();
          assertThat(mapped.put(Pair.of(targetFile, run.targetPosition() + offset), row))
              .as("Target row must be mapped once")
              .isNull();
        }
      }
    }

    // c2 is unique, so this also maps every live source row exactly once
    assertThat(mapped).isEqualTo(after);
  }

  /** Returns each live row's c2, which is unique, by its file and position. */
  private Map<Pair<String, Long>, String> rowsByPosition() {
    Map<Pair<String, Long>, String> rows = Maps.newHashMap();
    for (Row row :
        spark
            .read()
            .format("iceberg")
            .load(tableLocation)
            .select("_file", "_pos", "c2")
            .collectAsList()) {
      rows.put(Pair.of(row.getString(0), row.getLong(1)), row.getString(2));
    }

    return rows;
  }
}
