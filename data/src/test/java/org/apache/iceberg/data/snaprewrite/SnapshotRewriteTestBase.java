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
package org.apache.iceberg.data.snaprewrite;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Tables;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.GenericSnapshotRewriteIO;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.snaprewrite.SnapshotRewrite;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

/**
 * Scaffolding for snapshot rewrite tests: build a history, compact it, rewrite it, and check that
 * every snapshot still reads the same.
 *
 * <p>Nothing here mutates a table under test after the rewrite runs. The rewrite materializes into
 * a shadow table, so a failing case leaves both the original and the rewritten representation
 * intact.
 */
public abstract class SnapshotRewriteTestBase {
  protected static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  private static final Configuration CONF = new Configuration();
  private static final Tables TABLES = new HadoopTables(CONF);

  @TempDir protected File tempDir;

  protected Table table;

  @BeforeEach
  public void createTable() {
    this.table =
        TABLES.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION,
                "2",
                TableProperties.DEFAULT_FILE_FORMAT,
                "parquet",
                "write.compaction-map.enabled",
                "true"),
            tempDir.toString() + "/tbl");
  }

  // ------------------------------------------------------------------ building a history

  protected static Record record(int id, String data) {
    Record record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  protected static List<Record> records(int fromId, int count, String tag) {
    List<Record> rows = Lists.newArrayList();
    for (int i = 0; i < count; i += 1) {
      rows.add(record(fromId + i, tag + "-" + (fromId + i)));
    }

    return rows;
  }

  /** Writes a data file without committing it. */
  protected DataFile writeData(List<Record> rows) throws IOException {
    return FileHelpers.writeDataFile(table, newOutput("data"), rows);
  }

  /** Writes a position delete file without committing it. */
  protected DeleteFile writeDeletes(List<Pair<CharSequence, Long>> positions) throws IOException {
    return FileHelpers.writeDeleteFile(table, newOutput("deletes"), positions).first();
  }

  /** Commits an append. */
  protected DataFile append(List<Record> rows) throws IOException {
    DataFile file = writeData(rows);
    table.newFastAppend().appendFile(file).commit();
    return file;
  }

  /** Commits position deletes. */
  protected void delete(List<Pair<CharSequence, Long>> positions) throws IOException {
    table.newRowDelta().addDeletes(writeDeletes(positions)).commit();
  }

  /** Commits an insert and a delete together. */
  protected DataFile appendAndDelete(List<Record> rows, List<Pair<CharSequence, Long>> positions)
      throws IOException {
    DataFile file = writeData(rows);
    table.newRowDelta().addRows(file).addDeletes(writeDeletes(positions)).commit();
    return file;
  }

  /** Removes a whole data file. */
  protected void removeFile(DataFile file) {
    table.newDelete().deleteFile(file).commit();
  }

  protected static Pair<CharSequence, Long> at(DataFile file, long position) {
    return Pair.of(file.location(), position);
  }

  protected Snapshot compact() {
    return LocalCompactor.compact(table);
  }

  // ------------------------------------------------------------------ rewriting and checking

  protected SnapshotRewrite rewriter() {
    return SnapshotRewrite.forTable(table, new GenericSnapshotRewriteIO(table))
        .onLatestCompaction()
        .maxDeadRatio(Double.MAX_VALUE);
  }

  protected SnapshotRewriteResult rewrite() {
    return rewriter().materialize();
  }

  /**
   * Rows a snapshot returns, as a multiset. Duplicate rows are legal and must be preserved.
   *
   * <p>Only the schema's own columns are compared. {@code IcebergGenerics} reads with the schema
   * the delete filter requires and does not strip the extra {@code _pos} column afterwards, so a
   * snapshot that carries deletes yields wider records than one that does not -- and a rewrite
   * turns delete-free snapshots into delete-bearing ones. Projecting explicitly keeps the oracle
   * about table contents rather than about read plumbing.
   */
  protected static List<String> rowsAt(Table target, long snapshotId) {
    List<String> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(target).useSnapshot(snapshotId).build()) {
      for (Record record : records) {
        StringBuilder row = new StringBuilder();
        for (Types.NestedField field : SCHEMA.columns()) {
          row.append(field.name()).append('=').append(record.getField(field.name())).append(' ');
        }

        rows.add(row.toString());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Collections.sort(rows);
    return rows;
  }

  /**
   * Asserts that every snapshot in the table reads identically before and after the rewrite.
   *
   * <p>Every snapshot, not just the rewritten window: a rewrite that corrupted an untouched
   * snapshot would otherwise go unnoticed.
   */
  protected void assertLossless(SnapshotRewriteResult result) {
    Table shadow = result.asTable();
    for (Snapshot snapshot : table.snapshots()) {
      long id = snapshot.snapshotId();
      assertThat(rowsAt(shadow, id))
          .as("snapshot %s must read the same after the rewrite", id)
          .isEqualTo(rowsAt(table, id));
    }
  }

  /**
   * Asserts the rewritten snapshots keep their identity: id, parent, sequence number, timestamp.
   */
  protected void assertIdentityPreserved(SnapshotRewriteResult result) {
    for (Snapshot original : table.snapshots()) {
      Snapshot rewritten = result.metadata().snapshot(original.snapshotId());
      assertThat(rewritten).as("snapshot %s must survive", original.snapshotId()).isNotNull();
      assertThat(rewritten.sequenceNumber()).isEqualTo(original.sequenceNumber());
      assertThat(rewritten.parentId()).isEqualTo(original.parentId());
      assertThat(rewritten.timestampMillis()).isEqualTo(original.timestampMillis());
    }
  }

  private org.apache.iceberg.io.OutputFile newOutput(String prefix) {
    return table
        .io()
        .newOutputFile(
            table.location()
                + "/data/"
                + FileFormat.PARQUET.addExtension(prefix + "-" + UUID.randomUUID()));
  }
}
