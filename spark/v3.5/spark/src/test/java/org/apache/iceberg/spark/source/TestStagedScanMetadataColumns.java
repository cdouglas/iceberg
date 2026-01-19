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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkTestHelperBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests to verify staged scan behavior with metadata columns.
 *
 * <p>These tests document the current limitation: staged scans cannot properly expose metadata
 * columns (_file, _pos) to Spark's physical planner. The root cause is that Spark's
 * V2ScanRelationPushDown optimization prunes metadata columns, and PushDownUtils.toOutputAttrs
 * cannot map them when building the physical plan.
 */
public class TestStagedScanMetadataColumns extends SparkTestHelperBase {

  private static final Schema ICEBERG_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  private static final StructType SPARK_SCHEMA =
      new StructType()
          .add("id", DataTypes.IntegerType, false)
          .add("data", DataTypes.StringType, false);

  private static SparkSession spark;
  private static HadoopTables tables;

  @TempDir static java.nio.file.Path tempDir;

  private String tableLocation;
  private Table table;

  @BeforeAll
  public static void setupSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("TestStagedScanMetadataColumns")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate();
    tables = new HadoopTables(spark.sessionState().newHadoopConf());
  }

  @AfterAll
  public static void teardownSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @BeforeEach
  public void createTable() throws IOException {
    // Create a fresh table location for each test
    tableLocation = Files.createTempDirectory(tempDir, "test_table").toString();

    // Create table using HadoopTables directly
    table = tables.create(ICEBERG_SCHEMA, tableLocation);
  }

  @AfterEach
  public void cleanup() {
    // Clean up staged tasks if any remain
    ScanTaskSetManager.get().removeTasks(table, "test-staged-scan-basic");
    ScanTaskSetManager.get().removeTasks(table, "test-staged-scan-metadata");
    ScanTaskSetManager.get().removeTasks(table, "test-staged-scan-position-tracking");
  }

  private void insertTestData() {
    List<Row> data =
        Lists.newArrayList(
            RowFactory.create(1, "a"), RowFactory.create(2, "b"), RowFactory.create(3, "c"));
    Dataset<Row> df = spark.createDataFrame(data, SPARK_SCHEMA);
    df.write().format("iceberg").mode("append").save(tableLocation);
    table.refresh();
  }

  @Test
  public void testNormalScanWithMetadataColumnsWorks() {
    insertTestData();

    // Normal scan with metadata columns - this should work
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .load(tableLocation)
            .selectExpr("id", "data", "_file", "_pos");

    List<Row> rows = df.collectAsList();

    assertThat(rows).hasSize(3);
    // Verify _file and _pos are populated
    for (Row row : rows) {
      assertThat(row.getString(2)).as("_file should be populated").isNotNull();
      assertThat(row.getLong(3)).as("_pos should be non-negative").isGreaterThanOrEqualTo(0);
    }
  }

  @Test
  public void testStagedScanWithoutMetadataColumnsWorks() throws IOException {
    insertTestData();

    // Stage the scan tasks
    String taskSetId = "test-staged-scan-basic";
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> fileTasks = table.newScan().planFiles()) {
      fileTasks.forEach(tasks::add);
    }
    ScanTaskSetManager.get().stageTasks(table, taskSetId, tasks);

    try {
      // Staged scan without metadata columns - this should work
      Dataset<Row> df =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.SCAN_TASK_SET_ID, taskSetId)
              .load(tableLocation);

      List<Row> rows = df.collectAsList();
      assertThat(rows).hasSize(3);
    } finally {
      ScanTaskSetManager.get().removeTasks(table, taskSetId);
    }
  }

  @Test
  public void testStagedScanWithMetadataColumnsWorks() throws IOException {
    // NOTE: This test verifies that staged scans CAN expose metadata columns!
    // The original investigation in staged_scan_investigation.md suggested this would fail,
    // but testing shows it works correctly in Spark 3.5.
    insertTestData();

    // Stage the scan tasks
    String taskSetId = "test-staged-scan-metadata";
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> fileTasks = table.newScan().planFiles()) {
      fileTasks.forEach(tasks::add);
    }
    ScanTaskSetManager.get().stageTasks(table, taskSetId, tasks);

    try {
      // Staged scan WITH metadata columns - this now works!
      Dataset<Row> df =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.SCAN_TASK_SET_ID, taskSetId)
              .load(tableLocation)
              .selectExpr("id", "data", "_file", "_pos");

      List<Row> rows = df.collectAsList();

      assertThat(rows).hasSize(3);
      // Verify _file and _pos are populated
      for (Row row : rows) {
        assertThat(row.getString(2)).as("_file should be populated").isNotNull();
        assertThat(row.getLong(3)).as("_pos should be non-negative").isGreaterThanOrEqualTo(0);
      }

    } finally {
      ScanTaskSetManager.get().removeTasks(table, taskSetId);
    }
  }

  @Test
  public void testStagedScanWithPositionTrackingOptionWorks() throws IOException {
    // NOTE: This test verifies that staged scans with position tracking enabled also work.
    insertTestData();

    // Stage the scan tasks
    String taskSetId = "test-staged-scan-position-tracking";
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> fileTasks = table.newScan().planFiles()) {
      fileTasks.forEach(tasks::add);
    }
    ScanTaskSetManager.get().stageTasks(table, taskSetId, tasks);

    try {
      // Try to use TRACK_SOURCE_POSITIONS with staged scan
      Dataset<Row> df =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.SCAN_TASK_SET_ID, taskSetId)
              .option(SparkReadOptions.TRACK_SOURCE_POSITIONS, "true")
              .load(tableLocation)
              .selectExpr("id", "data", "_file", "_pos");

      List<Row> rows = df.collectAsList();

      assertThat(rows).hasSize(3);
      // Verify _file and _pos are populated
      for (Row row : rows) {
        assertThat(row.getString(2)).as("_file should be populated").isNotNull();
        assertThat(row.getLong(3)).as("_pos should be non-negative").isGreaterThanOrEqualTo(0);
      }

    } finally {
      ScanTaskSetManager.get().removeTasks(table, taskSetId);
    }
  }
}
