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
package org.apache.iceberg.catalog;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Consumer;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** {@link TableRead#unchangedIn}: what a change to a table does to a read of it. */
public class TestTableRead {
  private static final TableIdentifier ID = TableIdentifier.of("ns", "t");
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(2, "data", Types.StringType.get()),
          optional(3, "count", Types.IntegerType.get()));

  private InMemoryCatalog catalog;
  private Table table;

  @BeforeEach
  public void before() {
    this.catalog = new InMemoryCatalog();
    catalog.initialize("mem", ImmutableMap.of());
    catalog.createNamespace(ID.namespace());
    this.table = catalog.createTable(ID, SCHEMA, PartitionSpec.unpartitioned());
    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/data-1.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build())
        .commit();
    table.manageSnapshots().createTag("t", table.currentSnapshot().snapshotId()).commit();
  }

  @AfterEach
  public void after() throws Exception {
    catalog.close();
  }

  private TableMetadata current() {
    table.refresh();
    return ((HasTableOperations) table).operations().current();
  }

  private TableRead scanOf(String ref) {
    return TableRead.scan(ID, current(), ref, Expressions.alwaysTrue(), true);
  }

  /** A change to the table's schema, which a scan of a branch reads with. */
  enum SchemaChange {
    RENAME(tbl -> tbl.updateSchema().renameColumn("data", "payload").commit()),
    PROMOTION(tbl -> tbl.updateSchema().updateColumn("count", Types.LongType.get()).commit()),
    DROP_AND_ADD_AGAIN(
        tbl -> {
          tbl.updateSchema().deleteColumn("data").commit();
          tbl.updateSchema().addColumn("data", Types.StringType.get()).commit();
        });

    private final Consumer<Table> apply;

    SchemaChange(Consumer<Table> apply) {
      this.apply = apply;
    }
  }

  @ParameterizedTest
  @EnumSource(SchemaChange.class)
  public void aSchemaChangeChangesABranchScansRead(SchemaChange change) {
    TableRead read = scanOf(SnapshotRef.MAIN_BRANCH);
    TableMetadata before = current();

    change.apply.accept(table);

    TableMetadata after = current();
    assertThat(after.ref(SnapshotRef.MAIN_BRANCH).snapshotId())
        .as("the change leaves the branch alone")
        .isEqualTo(before.ref(SnapshotRef.MAIN_BRANCH).snapshotId());
    assertThat(read.unchangedIn(after)).isFalse();
  }

  @ParameterizedTest
  @EnumSource(SchemaChange.class)
  public void aSchemaChangeChangesAScanOfAMissingRef(SchemaChange change) {
    // a branch with no snapshot reads with the table's schema too
    TableRead read = scanOf("missing");

    change.apply.accept(table);

    assertThat(read.unchangedIn(current())).isFalse();
  }

  @ParameterizedTest
  @EnumSource(SchemaChange.class)
  public void aSchemaChangeLeavesATagScansReadAlone(SchemaChange change) {
    // a scan of a tag reads with its snapshot's schema
    TableRead read = scanOf("t");

    change.apply.accept(table);

    assertThat(read.unchangedIn(current())).isTrue();
  }

  @Test
  public void aPropertyChangeLeavesAScanReadAlone() {
    TableRead read = scanOf(SnapshotRef.MAIN_BRANCH);

    table.updateProperties().set("k", "v").commit();

    assertThat(read.unchangedIn(current())).isTrue();
  }

  @Test
  public void aDeclaredReadHoldsTheWholeVersion() {
    TableRead read = TableRead.whole(ID, current());
    assertThat(read.unchangedIn(current())).isTrue();

    table.updateProperties().set("k", "v").commit();

    assertThat(read.unchangedIn(current())).isFalse();
  }
}
