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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * A bin-pack rewrite with compaction maps, on a table in each test catalog. The rewrite reads and
 * writes its file group by group id through the table cache; a table loaded by its location instead
 * resolves only in a Hadoop catalog.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestRewriteDataFilesProcedureWithCompactionMaps extends ExtensionsTestBase {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testRewriteRecordsCompactionMapV2() {
    checkRewriteRecordsCompactionMap(2);
  }

  @TestTemplate
  public void testRewriteRecordsCompactionMapV3() {
    checkRewriteRecordsCompactionMap(3);
  }

  private void checkRewriteRecordsCompactionMap(int formatVersion) {
    sql(
        "CREATE TABLE %s (id INT, data STRING) USING iceberg "
            + "TBLPROPERTIES ('format-version'='%d', '%s'='true')",
        tableName, formatVersion, TableProperties.COMPACTION_MAP_ENABLED);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    sql("INSERT INTO %s VALUES (3, 'c'), (4, 'd')", tableName);
    long dataFiles = (long) scalarSql("SELECT count(*) FROM %s.files", tableName);
    List<Object[]> expected = sql("SELECT * FROM %s ORDER BY id", tableName);

    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files(table => '%s', options => map('min-input-files', '2'))",
            catalogName, tableIdent);

    assertEquals(
        "Rewrite must replace every data file with one",
        row((int) dataFiles, 1),
        Arrays.copyOf(output.get(0), 2));
    assertEquals("Rows must not change", expected, sql("SELECT * FROM %s ORDER BY id", tableName));
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.currentSnapshot().allManifests(table.io()))
        .as("The rewrite must record a compaction map")
        .anyMatch(manifest -> manifest.compactionMapLocation() != null);
  }
}
