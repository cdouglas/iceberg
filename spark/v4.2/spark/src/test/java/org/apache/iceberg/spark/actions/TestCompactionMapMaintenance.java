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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Table maintenance keeps a compaction map while a manifest of a retained snapshot names it, and
 * deletes it once none does.
 */
public class TestCompactionMapMaintenance extends TestBase {
  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  @TempDir private File tableDir;
  private String tableLocation;

  @BeforeEach
  public void setupTableLocation() {
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  public void orphanRemovalKeepsAMapThatAManifestNames() throws IOException {
    Table table = createTable();
    writeRecords(0, 2);
    writeRecords(2, 2);
    String named = compact(table);

    OutputFile unnamed =
        table.io().newOutputFile(table.location() + "/metadata/compaction-map-0-unnamed.avro");
    try (PositionOutputStream out = unnamed.create()) {
      out.write(0);
    }

    waitUntilAfter(System.currentTimeMillis());
    DeleteOrphanFiles.Result result =
        SparkActions.get().deleteOrphanFiles(table).olderThan(System.currentTimeMillis()).execute();

    assertThat(result.orphanFileLocations())
        .as("orphan removal deletes a map that no manifest names")
        .anyMatch(location -> location.endsWith("compaction-map-0-unnamed.avro"))
        .as("and keeps the map that the compaction's manifest names")
        .noneMatch(location -> location.endsWith(fileName(named)));
    assertThat(table.io().newInputFile(named).exists()).isTrue();
  }

  @Test
  public void expiryDeletesAMapOnceNoRetainedManifestNamesIt() {
    Table table = createTable();
    writeRecords(0, 2);
    writeRecords(2, 2);
    String first = compact(table);
    writeRecords(4, 2);
    String second = compact(table);

    // The next commit drops the manifest that marked the first compaction's output deleted
    writeRecords(6, 2);
    table.refresh();
    assertThat(mapsNamedBy(table, table.currentSnapshot())).containsExactly(second);

    waitUntilAfter(table.currentSnapshot().timestampMillis());
    SparkActions.get().expireSnapshots(table).expireOlderThan(System.currentTimeMillis()).execute();

    assertThat(table.io().newInputFile(first).exists())
        .as("no retained manifest names the first compaction's map")
        .isFalse();
    assertThat(table.io().newInputFile(second).exists())
        .as("the current snapshot names the second compaction's map")
        .isTrue();
  }

  private Table createTable() {
    Map<String, String> props =
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION, "2", TableProperties.COMPACTION_MAP_ENABLED, "true");
    return TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), props, tableLocation);
  }

  /** Bin-packs every data file, and returns the location of the map the compaction wrote. */
  private String compact(Table table) {
    table.refresh();
    SparkActions.get()
        .rewriteDataFiles(table)
        .option(BinPackRewriteFilePlanner.MIN_INPUT_FILES, "1")
        .execute();

    table.refresh();
    Snapshot compaction = table.currentSnapshot();
    List<String> written =
        compaction.dataManifests(table.io()).stream()
            .filter(manifest -> manifest.snapshotId() == compaction.snapshotId())
            .filter(ManifestFile::hasAddedFiles)
            .map(ManifestFile::compactionMapLocation)
            .distinct()
            .collect(Collectors.toList());
    assertThat(written).as("the compaction wrote one map").hasSize(1).doesNotContainNull();
    return written.get(0);
  }

  private static Set<String> mapsNamedBy(Table table, Snapshot snapshot) {
    return snapshot.dataManifests(table.io()).stream()
        .map(ManifestFile::compactionMapLocation)
        .filter(location -> location != null)
        .collect(Collectors.toSet());
  }

  private static String fileName(String location) {
    return location.substring(location.lastIndexOf('/') + 1);
  }

  private void writeRecords(int startId, int count) {
    List<Row> rows = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      rows.add(RowFactory.create(startId + i, "data" + (startId + i)));
    }

    StructType sparkSchema =
        new StructType()
            .add("id", DataTypes.IntegerType, true)
            .add("data", DataTypes.StringType, true);
    Dataset<Row> df = spark.createDataFrame(rows, sparkSchema);
    df.coalesce(1).write().format("iceberg").mode(SaveMode.Append).save(tableLocation);
  }
}
