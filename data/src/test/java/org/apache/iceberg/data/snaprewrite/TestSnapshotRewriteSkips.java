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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Tables;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.GenericSnapshotRewriteIO;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.snaprewrite.RewriteRefusal;
import org.apache.iceberg.snaprewrite.RewriteRefusedException;
import org.apache.iceberg.snaprewrite.SnapshotRewrite;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * A window that cannot be rewritten losslessly must be refused whole.
 *
 * <p>Partial success is not a useful outcome here: a rewrite that is lossless for most snapshots
 * and lossy for one still silently changes what a time-travel read returns. Every case checks that
 * the refusal names the right reason and that nothing was written.
 */
public class TestSnapshotRewriteSkips extends SnapshotRewriteTestBase {

  @Test
  public void refusesFormatVersionThree() throws IOException {
    Table v3 = createTableWith(ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
    appendTo(v3, records(1, 4, "base"));
    LocalCompactor.compact(v3);
    appendTo(v3, records(10, 2, "alpha"));
    LocalCompactor.compact(v3);

    assertRefusedWith(v3, RewriteRefusal.FORMAT_VERSION);
  }

  @Test
  public void refusesEqualityDeletes() throws IOException {
    append(records(1, 4, "base"));
    compact();

    append(records(10, 2, "alpha"));
    table.newRowDelta().addDeletes(equalityDelete(1)).commit();
    compact();

    assertRefusedWith(table, RewriteRefusal.EQUALITY_DELETES);
  }

  @Test
  public void refusesSchemaChange() throws IOException {
    append(records(1, 4, "base"));
    compact();

    append(records(10, 2, "alpha"));
    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    table.refresh();
    appendWithCurrentSchema(20, 2);
    compact();

    assertRefusedWith(table, RewriteRefusal.SCHEMA_CHANGED);
  }

  @Test
  public void refusesPartitionSpecChange() throws IOException {
    usePartitionSpec(PartitionSpec.builderFor(SCHEMA).identity("data").build());
    append(partitionValue("east"), records(1, 3, "east"));
    compact();

    append(partitionValue("east"), records(10, 2, "east"));
    table.updateSpec().removeField("data").commit();
    table.refresh();

    // Written under the new spec, so the window spans two layouts. A resurrection file can only be
    // partitioned one way, and rows recovered from the old layout have no place in the new one.
    append(records(20, 2, "flat"));
    compact();

    assertRefusedWith(table, RewriteRefusal.SPEC_CHANGED);
  }

  /**
   * The target compaction must carry a map; there is nothing to rewrite onto without one.
   *
   * <p>A map-less replace *inside* the window is a different matter and is not refused: see {@code
   * TestSnapshotRewriteLayouts#mapLessReplaceInsideTheWindowFallsBackToCopying}.
   */
  @Test
  public void refusesATargetCompactionWithoutAMap() throws IOException {
    append(records(1, 4, "base"));
    compact();

    append(records(10, 2, "alpha"));
    Snapshot mapless = LocalCompactor.compact(table, false);

    assertThatThrownBy(
            () ->
                SnapshotRewrite.forTable(table, new GenericSnapshotRewriteIO(table))
                    .onCompaction(mapless.snapshotId())
                    .maxDeadRatio(Double.MAX_VALUE)
                    .plan())
        .isInstanceOf(RewriteRefusedException.class)
        .hasMessageContaining("Cannot rewrite snapshots")
        .extracting(e -> ((RewriteRefusedException) e).refusal())
        .isEqualTo(RewriteRefusal.MISSING_COMPACTION_MAP);
  }

  @Test
  public void refusesWhenASourceFileIsGone() throws IOException {
    append(records(1, 4, "base"));
    compact();

    DataFile alpha = append(records(10, 3, "alpha"));
    delete(ImmutableList.of(at(alpha, 0)));
    compact();

    // The row to recover lives only in this file; without it the state before the delete cannot be
    // reconstructed, and guessing is worse than refusing.
    table.io().deleteFile(alpha.location());

    assertRefusedWith(table, RewriteRefusal.MISSING_SOURCE_FILE);
  }

  @Test
  public void refusesRecentCompactions() throws IOException {
    append(records(1, 4, "base"));
    compact();
    append(records(10, 2, "alpha"));
    compact();

    assertThatThrownBy(
            () ->
                SnapshotRewrite.forTable(table, new GenericSnapshotRewriteIO(table))
                    .onLatestCompaction()
                    .minAgeMs(Long.MAX_VALUE)
                    .plan())
        .isInstanceOf(RewriteRefusedException.class)
        .hasMessageContaining("Cannot rewrite snapshots")
        .extracting(e -> ((RewriteRefusedException) e).refusal())
        .isEqualTo(RewriteRefusal.TOO_RECENT);
  }

  @Test
  public void refusesWhenTooLittleWouldBeReclaimed() throws IOException {
    append(records(1, 4, "base"));
    compact();

    DataFile alpha = append(records(10, 4, "alpha"));
    delete(ImmutableList.of(at(alpha, 0), at(alpha, 1), at(alpha, 2)));
    compact();

    assertThatThrownBy(
            () ->
                SnapshotRewrite.forTable(table, new GenericSnapshotRewriteIO(table))
                    .onLatestCompaction()
                    .maxDeadRatio(0.01)
                    .plan())
        .isInstanceOf(RewriteRefusedException.class)
        .hasMessageContaining("Cannot rewrite snapshots")
        .extracting(e -> ((RewriteRefusedException) e).refusal())
        .isEqualTo(RewriteRefusal.DEAD_RATIO);
  }

  @Test
  public void refusesWhenThereIsNoCompaction() throws IOException {
    append(records(1, 4, "base"));
    append(records(10, 2, "alpha"));

    assertThatThrownBy(
            () ->
                SnapshotRewrite.forTable(table, new GenericSnapshotRewriteIO(table))
                    .onLatestCompaction())
        .isInstanceOf(RewriteRefusedException.class)
        .hasMessageContaining("Cannot rewrite snapshots")
        .extracting(e -> ((RewriteRefusedException) e).refusal())
        .isEqualTo(RewriteRefusal.NO_COMPACTION);
  }

  // ------------------------------------------------------------------ helpers

  private StructLike partitionValue(String value) {
    PartitionKey key = new PartitionKey(table.spec(), SCHEMA);
    key.partition(new InternalRecordWrapper(SCHEMA.asStruct()).wrap(record(0, value)));
    return key;
  }

  private void assertRefusedWith(Table target, RewriteRefusal expected) {
    List<String> before = filesUnder(target.location());

    assertThatThrownBy(
            () ->
                SnapshotRewrite.forTable(target, new GenericSnapshotRewriteIO(target))
                    .onLatestCompaction()
                    .maxDeadRatio(Double.MAX_VALUE)
                    .plan())
        .isInstanceOf(RewriteRefusedException.class)
        .hasMessageContaining(expected.description())
        .extracting(e -> ((RewriteRefusedException) e).refusal())
        .isEqualTo(expected);

    assertThat(filesUnder(target.location()))
        .as("a refused rewrite writes nothing")
        .isEqualTo(before);
  }

  private static List<String> filesUnder(String location) {
    List<String> paths = Lists.newArrayList();
    collect(new File(location.replaceFirst("^file:", "")), paths);
    paths.sort(String::compareTo);
    return paths;
  }

  private static void collect(File directory, List<String> paths) {
    File[] children = directory.listFiles();
    if (children == null) {
      return;
    }

    for (File child : children) {
      if (child.isDirectory()) {
        collect(child, paths);
      } else {
        paths.add(child.getAbsolutePath());
      }
    }
  }

  private Table createTableWith(java.util.Map<String, String> properties) {
    Tables tables = new HadoopTables(new Configuration());
    java.util.Map<String, String> all =
        ImmutableMap.<String, String>builder()
            .putAll(properties)
            .put(TableProperties.DEFAULT_FILE_FORMAT, "parquet")
            .put("write.compaction-map.enabled", "true")
            .build();
    return tables.create(
        SCHEMA,
        PartitionSpec.unpartitioned(),
        all,
        tempDir.toString() + "/other-" + UUID.randomUUID());
  }

  private void appendTo(Table target, List<Record> rows) throws IOException {
    DataFile file =
        FileHelpers.writeDataFile(
            target,
            target
                .io()
                .newOutputFile(
                    target.location()
                        + "/data/"
                        + FileFormat.PARQUET.addExtension("d-" + UUID.randomUUID())),
            rows);
    target.newFastAppend().appendFile(file).commit();
  }

  /** Appends rows shaped to whatever schema the table currently has. */
  private void appendWithCurrentSchema(int fromId, int count) throws IOException {
    Schema current = table.schema();
    List<Record> rows = Lists.newArrayList();
    for (int i = 0; i < count; i += 1) {
      Record row = GenericRecord.create(current);
      row.setField("id", fromId + i);
      row.setField("data", "evolved-" + (fromId + i));
      rows.add(row);
    }

    DataFile file =
        FileHelpers.writeDataFile(
            table,
            table
                .io()
                .newOutputFile(
                    table.location()
                        + "/data/"
                        + FileFormat.PARQUET.addExtension("evolved-" + UUID.randomUUID())),
            rows);
    table.newFastAppend().appendFile(file).commit();
  }

  private DeleteFile equalityDelete(int id) throws IOException {
    Schema deleteSchema = table.schema().select("id");
    Record delete = GenericRecord.create(deleteSchema);
    delete.setField("id", id);
    return FileHelpers.writeDeleteFile(
        table,
        table
            .io()
            .newOutputFile(
                table.location()
                    + "/data/"
                    + FileFormat.PARQUET.addExtension("eq-" + UUID.randomUUID())),
        ImmutableList.of(delete),
        deleteSchema);
  }
}
