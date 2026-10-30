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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericSnapshotRewriteIO;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.snaprewrite.SnapshotReconstructResult;
import org.apache.iceberg.snaprewrite.SnapshotRewrite;
import org.apache.iceberg.snaprewrite.SnapshotRewriteReconstruct;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Snapshot rewriting onto compactions Spark wrote, rather than the test compactor.
 *
 * <p>The snapshot rewrite suites in the data module build every compaction and every map with
 * {@code LocalCompactor}, which is correct by construction. This runs the same rewrite onto {@link
 * RewriteDataFilesSparkAction} output with compaction maps enabled, and answers what those suites
 * cannot: on v3, whether Spark's output records {@code _last_updated_sequence_number} in its
 * metrics for every row, which precondition P1c reads, and whether its maps join rows with equal
 * {@code _row_id}, which the rewrite now checks before writing anything. Either failing would make
 * every v3 rewrite onto a Spark compaction refuse.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkSnapshotRewrite extends TestBase {
  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir private File tableDir;

  @Parameter private int formatVersion;

  @Parameters(name = "formatVersion = {0}")
  protected static List<Integer> parameters() {
    return Arrays.asList(2, 3);
  }

  private String tableLocation = null;
  private Table table = null;

  @BeforeAll
  public static void setupSpark() {
    spark.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false");
  }

  @BeforeEach
  public void createTable() {
    this.tableLocation = tableDir.toURI().toString();
    this.table =
        TABLES.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION,
                String.valueOf(formatVersion),
                TableProperties.COMPACTION_MAP_ENABLED,
                "true"),
            tableLocation);
  }

  /**
   * Two Spark compactions with appends and deletes between them, rewritten onto the second: every
   * snapshot reads the same rows, with the same lineage on v3, after the rewrite and, on v3, after
   * reclaim and reconstruction too.
   */
  @TestTemplate
  public void rewritesOntoASparkCompaction() throws IOException {
    writeRecords(0, 300);
    writeRecords(300, 300);
    Snapshot floor = compact();

    writeRecords(600, 200);
    DataFile appended = firstAddedFile(table.currentSnapshot());
    delete(appended, 3, 4, 5);
    delete(firstAddedFile(floor), 10, 11, 12, 13);
    writeRecords(800, 200);
    Snapshot target = compact();

    if (formatVersion >= 3) {
      int lastUpdated = MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId();
      for (DataFile file : target.addedDataFiles(table.io())) {
        assertThat(file.valueCounts())
            .as("Spark records _last_updated_sequence_number in the metrics of %s", file.location())
            .containsKey(lastUpdated);
        assertThat(file.nullValueCounts().get(lastUpdated))
            .as("and writes it for every row")
            .isZero();
      }
    }

    Map<Long, List<String>> before = rowsBySnapshot(table);
    SnapshotRewriteResult rewritten =
        SnapshotRewrite.forTable(table, new GenericSnapshotRewriteIO(table))
            .onCompaction(target.snapshotId())
            .maxDeadRatio(Double.MAX_VALUE)
            .materialize();
    assertThat(rewritten.plan().resurrectedRows())
        .as("the rows deleted inside the window are recovered")
        .isEqualTo(7);
    assertThat(rowsBySnapshot(rewritten.asTable())).isEqualTo(before);

    TableOperations ops = ((HasTableOperations) table).operations();
    rewritten.commit(ops);
    trimMetadataLog();
    assertThat(rewritten.reclaim(ops).withheld()).isEmpty();
    assertThat(rowsBySnapshot(table)).isEqualTo(before);

    if (formatVersion >= 3) {
      assertThat(rewritten.ledger()).as(rewritten.plan().ledgerUnavailable()).isNotNull();
      table.refresh();
      SnapshotReconstructResult reconstructed =
          SnapshotRewriteReconstruct.forTable(table, new GenericSnapshotRewriteIO(table))
              .materialize();
      assertThat(rowsBySnapshot(reconstructed.asTable())).isEqualTo(before);

      reconstructed.commit(ops);
      trimMetadataLog();
      assertThat(reconstructed.reclaim(ops).withheld()).isEmpty();
      assertThat(rowsBySnapshot(table)).isEqualTo(before);
    }
  }

  private Snapshot compact() {
    SparkActions.get(spark)
        .rewriteDataFiles(table)
        .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
        .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
        .execute();
    table.refresh();
    return table.currentSnapshot();
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
            .csv(spark.createDataset(data, Encoders.STRING()));
    df.write()
        .format("iceberg")
        .mode("append")
        .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
        .save(tableLocation);
    table.refresh();
  }

  /** Deletes positions of one file, as a deletion vector on v3 and a position delete file on v2. */
  private void delete(DataFile file, long... positions) throws IOException {
    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    for (long position : positions) {
      deletes.add(Pair.of(file.location(), position));
    }

    DeleteFile deleteFile =
        FileHelpers.writeDeleteFile(
                table,
                table
                    .io()
                    .newOutputFile(
                        tableLocation + "/data/deletes-" + UUID.randomUUID() + ".parquet"),
                null,
                deletes,
                formatVersion)
            .first();
    table
        .newRowDelta()
        .validateFromSnapshot(table.currentSnapshot().snapshotId())
        .addDeletes(deleteFile)
        .commit();
    table.refresh();
  }

  private DataFile firstAddedFile(Snapshot snapshot) {
    List<DataFile> added = Lists.newArrayList(snapshot.addedDataFiles(table.io()));
    added.sort((one, two) -> one.location().compareTo(two.location()));
    assertThat(added).isNotEmpty();
    assertThat(added.get(0).recordCount()).isGreaterThan(13);
    return added.get(0);
  }

  /**
   * Every snapshot's rows, sorted. On v3 each row carries its {@code _row_id} and {@code
   * _last_updated_sequence_number}, since a rewrite must keep both.
   */
  private Map<Long, List<String>> rowsBySnapshot(Table target) throws IOException {
    Schema projection =
        formatVersion >= 3 ? MetadataColumns.schemaWithRowLineage(target.schema()) : SCHEMA;
    Map<Long, List<String>> rows = Maps.newTreeMap();
    for (Snapshot snapshot : target.snapshots()) {
      List<String> snapshotRows = Lists.newArrayList();
      try (CloseableIterable<Record> records =
          IcebergGenerics.read(target)
              .useSnapshot(snapshot.snapshotId())
              .project(projection)
              .build()) {
        for (Record record : records) {
          List<Object> values = Lists.newArrayList();
          for (Types.NestedField field : projection.columns()) {
            values.add(record.getField(field.name()));
          }

          snapshotRows.add(values.toString());
        }
      }

      Collections.sort(snapshotRows);
      rows.put(snapshot.snapshotId(), ImmutableList.copyOf(snapshotRows));
    }

    return rows;
  }

  /** Reclaim leaves alone what the metadata log still reaches (rewrite errata 5). */
  private void trimMetadataLog() {
    table.refresh();
    table
        .updateProperties()
        .set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1")
        .set("test.trim", UUID.randomUUID().toString())
        .commit();
    table.refresh();
  }
}
