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
package org.apache.iceberg.jdbc;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * A commit's table operations hold the table as the commit wrote it, as REST's hold a commit's
 * response, instead of reading back the metadata file it wrote on their next use: the conditional
 * UPDATE that changed one row from the base's metadata location to the new one proves the table is
 * now that file.
 */
public class TestJdbcCommitAdoption {
  static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get(), "unique ID"),
          required(2, "data", Types.StringType.get()));
  static final DataFile FILE_A =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(2)
          .build();

  @TempDir java.nio.file.Path tableDir;
  private JdbcCatalog catalog;

  @BeforeEach
  public void setupTable() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(
        CatalogProperties.URI,
        "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", ""));
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, tableDir.toAbsolutePath().toString());
    properties.put("type", "jdbc");
    catalog =
        (JdbcCatalog)
            CatalogUtil.buildIcebergCatalog("test_jdbc_catalog", properties, new Configuration());
  }

  @Test
  public void testCommitDoesNotReadBackItsMetadata() {
    TableIdentifier tableIdent = TableIdentifier.of("db", "tbl");
    Table table = catalog.buildTable(tableIdent, SCHEMA).create();
    TableOperations ops = ((BaseTable) table).operations();
    ops.current();

    try (MockedStatic<TableMetadataParser> parser =
        Mockito.mockStatic(TableMetadataParser.class, Mockito.CALLS_REAL_METHODS)) {
      table.newAppend().appendFile(FILE_A).commit();
      table.updateProperties().set("k", "v").commit();
      table.currentSnapshot();

      // each commit wrote a metadata file and none was read back
      parser.verify(() -> TableMetadataParser.read(any(FileIO.class), anyString()), never());
    }

    TableMetadata committed = ops.current();
    assertThat(committed.changes()).isEmpty();
    assertThat(committed.properties()).containsEntry("k", "v");
    // the catalog's row names the file the operations hold: a refresh keeps the same object
    assertThat(ops.refresh()).isSameAs(committed);
    // and the operations hold exactly what that file contains
    assertThat(TableMetadataParser.toJson(committed))
        .isEqualTo(
            TableMetadataParser.toJson(
                TableMetadataParser.read(ops.io(), committed.metadataFileLocation())));
  }

  @Test
  public void testCreateDoesNotReadBackItsMetadata() {
    TableIdentifier tableIdent = TableIdentifier.of("db", "tbl");
    try (MockedStatic<TableMetadataParser> parser =
        Mockito.mockStatic(TableMetadataParser.class, Mockito.CALLS_REAL_METHODS)) {
      Table table = catalog.buildTable(tableIdent, SCHEMA).create();
      table.newAppend().appendFile(FILE_A).commit();
      parser.verify(() -> TableMetadataParser.read(any(FileIO.class), anyString()), never());
    }
  }

  @Test
  public void testFailedCommitKeepsCurrentMetadata() {
    TableIdentifier tableIdent = TableIdentifier.of("db", "tbl");
    catalog.buildTable(tableIdent, SCHEMA).create();
    Table first = catalog.loadTable(tableIdent);
    Table second = catalog.loadTable(tableIdent);
    TableOperations ops = ((BaseTable) second).operations();

    // an update that does not refresh before it commits, staged before another writer's commit
    UpdateSchema update = second.updateSchema().addColumn("n", Types.IntegerType.get());
    first.newAppend().appendFile(FILE_A).commit();
    TableMetadata before = ops.current();

    assertThatThrownBy(update::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("has changed from");
    assertThat(ops.current()).isSameAs(before);
    second.refresh();
    assertThat(second.currentSnapshot()).isNotNull();
  }

  @Test
  public void testCommitRefreshesFromCatalogOnce() {
    TableIdentifier tableIdent = TableIdentifier.of("db", "tbl");
    Table table = catalog.buildTable(tableIdent, SCHEMA).create();
    table.refresh();

    try (MockedStatic<JdbcUtil> jdbc =
        Mockito.mockStatic(JdbcUtil.class, Mockito.CALLS_REAL_METHODS)) {
      table.newAppend().appendFile(FILE_A).commit();
      // apply()'s refresh and doCommit's check; SnapshotProducer finds the committed snapshot in
      // current(), which holds the table as committed (stock 1.12.0 refreshes it: 3)
      jdbc.verify(() -> JdbcUtil.loadTable(any(), any(), any(), any()), times(2));
    }
  }
}
