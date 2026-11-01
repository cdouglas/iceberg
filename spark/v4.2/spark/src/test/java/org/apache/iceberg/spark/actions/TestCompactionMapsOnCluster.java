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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
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
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Rewrites a table with compaction maps on a local cluster, whose executors run in their own JVMs.
 *
 * <p>In local mode, tasks run in the driver's JVM, so a map comes out right even if the source
 * positions a task tracks reach the driver only through static state. Here they must travel with
 * the task's commit message.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestCompactionMapsOnCluster {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));
  private static final int FILES = 4;
  private static final int ROWS_PER_FILE = 5000;

  private static SparkSession spark;

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Lists.newArrayList(2, 3);
  }

  @Parameter private int formatVersion;

  @TempDir private File tableDir;

  @BeforeAll
  public static void startCluster() {
    spark =
        SparkSession.builder()
            .master("local-cluster[2,1,1024]")
            .appName(TestCompactionMapsOnCluster.class.getSimpleName())
            // Executors load this JVM's classes; the launcher finds no Spark distribution to add
            // jars from, which it allows only when testing, and no build to take the Scala
            // version from
            .config("spark.executor.extraClassPath", System.getProperty("java.class.path"))
            .config("spark.executorEnv.SPARK_TESTING", "1")
            .config("spark.executorEnv.SPARK_SCALA_VERSION", scalaBinaryVersion())
            .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false")
            .config(TestBase.DISABLE_UI)
            .getOrCreate();
  }

  @AfterAll
  public static void stopCluster() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }

    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
  }

  @TestTemplate
  public void testBinPackOnAClusterWritesTheMap() throws IOException {
    Table table = createTable();

    assertRewriteMapsEveryRow(table);
  }

  @TestTemplate
  public void testMergeOnAClusterWritesTheMap() throws IOException {
    Table table = createTable();
    RowDelta rowDelta = table.newRowDelta();
    int deleteFiles = 0;
    for (DataFile file : table.currentSnapshot().addedDataFiles(table.io())) {
      // Gaps of one row and one long gap, which the map must skip
      List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
      for (long pos = 0; pos < file.recordCount(); pos += 1) {
        if (pos % 10 == 3 || (pos >= 2000 && pos < 2500)) {
          deletes.add(Pair.of(file.location(), pos));
        }
      }

      rowDelta.addDeletes(
          FileHelpers.writeDeleteFile(
                  table,
                  table
                      .io()
                      .newOutputFile(
                          tableDir
                              + "/data/"
                              + FileFormat.PARQUET.addExtension("deletes-" + deleteFiles++)),
                  null,
                  deletes,
                  formatVersion)
              .first());
    }

    rowDelta.commit();

    assertRewriteMapsEveryRow(table);
  }

  private Table createTable() {
    Map<String, String> properties =
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            String.valueOf(formatVersion),
            TableProperties.COMPACTION_MAP_ENABLED,
            "true",
            // Many row groups, so the rewrite can split each source file between tasks
            TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
            "4096");
    Table table =
        TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), properties, tableDir.toString());

    spark
        .range(0, FILES * ROWS_PER_FILE, 1, FILES)
        .withColumn("data", functions.concat(functions.lit("row-"), functions.col("id")))
        .write()
        .format("iceberg")
        .mode("append")
        .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
        .save(tableDir.toString());

    table.refresh();
    assertThat(table.currentSnapshot().addedDataFiles(table.io())).hasSize(FILES);
    return table;
  }

  /**
   * Rewrites every file of the table into about three, and checks that the compaction map sends
   * each live source row to the target position that now holds it.
   */
  private void assertRewriteMapsEveryRow(Table table) throws IOException {
    Map<Pair<String, Long>, Long> before = idsByPosition();
    long sourceBytes = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        sourceBytes += task.file().fileSizeInBytes();
      }
    }

    RewriteDataFiles.Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .option(
                SizeBasedFileRewritePlanner.TARGET_FILE_SIZE_BYTES, String.valueOf(sourceBytes / 3))
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(FILES);
    assertThat(result.addedDataFilesCount()).isGreaterThan(1);

    table.refresh();
    ManifestFile added =
        table.currentSnapshot().dataManifests(table.io()).stream()
            .filter(ManifestFile::hasAddedFiles)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No manifest of the rewrite's files"));
    assertThat(added.compactionMapLocation())
        .as("A rewrite on a cluster must write a compaction map")
        .isNotNull();

    CompactionMap map = CompactionMaps.read(table.io().newInputFile(added.compactionMapLocation()));
    assertThat(map.fileMappings()).hasSize(FILES);
    assertThat(map.fileMappings())
        .as("Some source file must be split between tasks, and so between target files")
        .anyMatch(mapping -> targetFiles(mapping).size() > 1);

    Map<Pair<String, Long>, Long> after = idsByPosition();
    assertThat(after).hasSameSizeAs(before);

    Map<Pair<String, Long>, Long> mapped = Maps.newHashMap();
    for (CompactionMap.FileMapping mapping : map.fileMappings()) {
      for (CompactionMap.Run run : mapping.runs()) {
        String targetFile = run.targetFile() != null ? run.targetFile() : mapping.targetFile();
        for (long offset = 0; offset < run.length(); offset += 1) {
          Long id = before.get(Pair.of(mapping.sourceFile(), run.sourcePosition() + offset));
          assertThat(id)
              .as("Mapped row %s of %s must be live", run.sourcePosition() + offset, mapping)
              .isNotNull();
          Long previous = mapped.put(Pair.of(targetFile, run.targetPosition() + offset), id);
          assertThat(previous).as("Target row must be mapped once").isNull();
        }
      }
    }

    // Ids are unique, so this also maps every live source row exactly once
    assertThat(mapped).isEqualTo(after);
  }

  private static List<String> targetFiles(CompactionMap.FileMapping mapping) {
    return mapping.runs().stream()
        .map(run -> run.targetFile() != null ? run.targetFile() : mapping.targetFile())
        .distinct()
        .collect(Collectors.toList());
  }

  private Map<Pair<String, Long>, Long> idsByPosition() {
    Map<Pair<String, Long>, Long> ids = Maps.newHashMap();
    for (Row row :
        spark
            .read()
            .format("iceberg")
            .load(tableDir.toString())
            .select("_file", "_pos", "id")
            .collectAsList()) {
      ids.put(Pair.of(row.getString(0), row.getLong(1)), row.getLong(2));
    }

    return ids;
  }

  private static String scalaBinaryVersion() {
    String version = scala.util.Properties.versionNumberString();
    return version.substring(0, version.lastIndexOf('.'));
  }
}
