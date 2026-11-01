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

import java.util.Set;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PositionDeletesTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkRewriteTable extends BaseSparkTable implements SupportsRead, SupportsWrite {

  private static final Set<TableCapability> CAPABILITIES =
      ImmutableSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE);

  private final String groupId;

  public SparkRewriteTable(Table table, String groupId) {
    super(table, readSchema(table));
    this.groupId = groupId;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new SparkStagedScanBuilder(spark(), table(), groupId, options);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    if (table() instanceof PositionDeletesTable) {
      return new SparkPositionDeletesRewriteBuilder(spark(), table(), groupId, info);
    } else {
      return new SparkRewriteWriteBuilder(
          spark(), table(), rewriteSchema(table()), groupId, info, readsSourcePositions(table()));
    }
  }

  private static Schema rewriteSchema(Table table) {
    if (TableUtil.supportsRowLineage(table)) {
      return MetadataColumns.schemaWithRowLineage(table.schema());
    } else {
      return table.schema();
    }
  }

  /**
   * What a rewrite reads: the rows it writes, and, where the table keeps compaction maps, each
   * row's source file and position. Those come last: a position-tracked writer records them for the
   * compaction map, and every writer writes only the columns before them.
   */
  private static Schema readSchema(Table table) {
    Schema schema = rewriteSchema(table);
    if (readsSourcePositions(table)) {
      return TypeUtil.join(
          schema, new Schema(MetadataColumns.FILE_PATH, MetadataColumns.ROW_POSITION));
    } else {
      return schema;
    }
  }

  private static boolean readsSourcePositions(Table table) {
    return !(table instanceof PositionDeletesTable)
        && PropertyUtil.propertyAsBoolean(
            table.properties(),
            TableProperties.COMPACTION_MAP_ENABLED,
            TableProperties.COMPACTION_MAP_ENABLED_DEFAULT);
  }
}
