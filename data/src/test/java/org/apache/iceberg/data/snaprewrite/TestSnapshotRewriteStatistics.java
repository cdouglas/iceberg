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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStatsHandler;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.snaprewrite.SnapshotRewriteRestore;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.junit.jupiter.api.Test;

/**
 * What a rewrite does to statistics.
 *
 * <p>Statistics are keyed by snapshot id, and a rewrite preserves snapshot ids, so every statistics
 * file stays attached to the snapshot it names unless something detaches it. Nothing in Iceberg
 * validates a statistics file against the snapshot it describes -- statistics are advisory, and a
 * reader may ignore them -- so a stale entry is not rejected, it is believed. That makes this a
 * silent-wrongness risk rather than a crash risk, and the reason it needs a test at all.
 *
 * <p>The two kinds part company:
 *
 * <ul>
 *   <li><b>Table-level</b> statistics are theta sketches: distinct-value counts over the rows live
 *       at a snapshot. A rewrite preserves the live row set exactly, so they stay valid and stay
 *       attached.
 *   <li><b>Partition</b> statistics are file counts, byte totals, and delete counts -- properties
 *       of the layout, which a rewrite replaces. They are dropped, and recorded in the snapshot
 *       that replaced them so a restore can put them back.
 * </ul>
 */
public class TestSnapshotRewriteStatistics extends SnapshotRewriteTestBase {

  /** Partition statistics go for the rewritten snapshots only; table statistics all survive. */
  @Test
  public void partitionStatisticsAreDroppedAndTableStatisticsKept() throws IOException {
    buildPartitionedWindow();
    attachStatisticsToEverySnapshot();

    Set<Long> allIds = Sets.newHashSet();
    for (Snapshot snapshot : table.snapshots()) {
      allIds.add(snapshot.snapshotId());
    }

    Set<Long> before = partitionStatSnapshotIds(currentMetadata());
    assertThat(before).as("every snapshot starts with partition statistics").isEqualTo(allIds);

    SnapshotRewriteResult result = rewrite();
    Set<Long> rewrittenIds = Sets.newHashSet();
    for (Snapshot snapshot : result.plan().window()) {
      rewrittenIds.add(snapshot.snapshotId());
    }

    Set<Long> after = partitionStatSnapshotIds(result.metadata());
    assertThat(after)
        .as("partition statistics survive for exactly the snapshots that were not rewritten")
        .isEqualTo(Sets.difference(before, rewrittenIds));

    // The compaction is not rewritten, so its own statistics describe a layout that still stands.
    assertThat(after).contains(result.plan().compaction().snapshotId());

    assertThat(tableStatSnapshotIds(result.metadata()))
        .as("table statistics are row-derived and survive the rewrite")
        .isEqualTo(tableStatSnapshotIds(currentMetadata()));
  }

  /** The dropped entries come back on restore, identically, without being read. */
  @Test
  public void restoreReattachesPartitionStatistics() throws IOException {
    buildPartitionedWindow();
    attachStatisticsToEverySnapshot();

    Map<Long, String> before = partitionStatsBySnapshot(currentMetadata());

    SnapshotRewriteResult result = rewrite();
    result.commit(((HasTableOperations) table).operations());
    table.refresh();

    assertThat(partitionStatsBySnapshot(currentMetadata()))
        .as("a committed rewrite leaves fewer partition statistics attached")
        .hasSizeLessThan(before.size());

    TableMetadata rewritten = currentMetadata();
    ((HasTableOperations) table)
        .operations()
        .commit(rewritten, SnapshotRewriteRestore.restore(rewritten));
    table.refresh();

    assertThat(partitionStatsBySnapshot(currentMetadata()))
        .as("restore re-attaches every dropped entry, with its original path and size")
        .isEqualTo(before);
  }

  /**
   * Reclaim deletes the detached statistics file, and withholds it while anything still names it.
   *
   * <p>A statistics file hangs off table metadata rather than off a snapshot, so walking snapshots
   * does not find it. Without the metadata-level check a retained metadata-log entry that still
   * names the file would not count as a reference, and reclaim would delete a file the table can
   * still reach.
   */
  @Test
  public void reclaimDeletesTheDetachedStatisticsFile() throws IOException {
    buildPartitionedWindow();

    // After buildPartitionedWindow, not before: usePartitionSpec recreates the table, which would
    // discard the property. A short metadata log is what lets reclaim delete anything at all.
    table.updateProperties().set("write.metadata.previous-versions-max", "1").commit();
    attachStatisticsToEverySnapshot();

    Map<Long, String> pathsBefore = partitionStatPathsBySnapshot(currentMetadata());

    SnapshotRewriteResult result = rewrite();
    Set<Long> rewrittenIds = Sets.newHashSet();
    for (Snapshot snapshot : result.plan().window()) {
      rewrittenIds.add(snapshot.snapshotId());
    }

    List<String> detached = Lists.newArrayList();
    for (Map.Entry<Long, String> entry : pathsBefore.entrySet()) {
      if (rewrittenIds.contains(entry.getKey())) {
        detached.add(entry.getValue());
      }
    }

    assertThat(detached).as("the window carried partition statistics").isNotEmpty();

    result.commit(((HasTableOperations) table).operations());
    table.refresh();

    // Still reachable from the retained pre-rewrite metadata document.
    SnapshotRewriteResult.ReclaimResult held =
        result.reclaim(((HasTableOperations) table).operations());
    assertThat(held.withheld())
        .as("withheld while a metadata-log entry still names it")
        .containsAll(detached);
    assertThat(held.deleted()).doesNotContainAnyElementsOf(detached);
    for (String path : detached) {
      assertThat(table.io().newInputFile(path).exists()).isTrue();
    }

    // Push the pre-rewrite document out of the metadata log, then reclaim for real.
    table.updateProperties().set("unrelated", "1").commit();
    table.refresh();

    SnapshotRewriteResult.ReclaimResult reclaimed =
        result.reclaim(((HasTableOperations) table).operations());
    assertThat(reclaimed.deleted())
        .as("the detached statistics file belongs to the old layout")
        .containsAll(detached);
    for (String path : detached) {
      assertThat(table.io().newInputFile(path).exists()).isFalse();
    }

    // Statistics still attached are never candidates.
    for (PartitionStatisticsFile file : currentMetadata().partitionStatisticsFiles()) {
      assertThat(reclaimed.deleted()).doesNotContain(file.path());
    }
  }

  /** A table with no statistics at all is unaffected, and the summary stays clean. */
  @Test
  public void noStatisticsMeansNoSummaryKeys() throws IOException {
    buildPartitionedWindow();

    SnapshotRewriteResult result = rewrite();
    assertThat(result.metadata().partitionStatisticsFiles()).isEmpty();
    assertThat(result.metadata().statisticsFiles()).isEmpty();

    for (Snapshot snapshot : result.metadata().snapshots()) {
      assertThat(snapshot.summary().keySet())
          .as("nothing detached, so nothing recorded")
          .noneMatch(key -> key.startsWith("snapshot-rewrite-detached"));
    }
  }

  private void buildPartitionedWindow() throws IOException {
    usePartitionSpec(PartitionSpec.builderFor(SCHEMA).identity("data").build());

    append(partition("east"), rows(1, 4, "east"));
    append(partition("west"), rows(10, 4, "west"));
    compact();

    DataFile east = append(partition("east"), rows(20, 3, "east"));
    delete(partition("east"), ImmutableList.of(at(east, 0)));
    compact();
  }

  /**
   * Gives every snapshot in the history both kinds of statistics.
   *
   * <p>Partition statistics are computed for real, because reclaim has to delete an actual file.
   * Table statistics are a real file with placeholder contents: nothing in the rewrite path reads a
   * theta sketch, and writing one would test the sketch library rather than this code.
   */
  private void attachStatisticsToEverySnapshot() throws IOException {
    List<Long> ids = Lists.newArrayList();
    for (Snapshot snapshot : table.snapshots()) {
      ids.add(snapshot.snapshotId());
    }

    for (long id : ids) {
      PartitionStatisticsFile partitionStats =
          PartitionStatsHandler.computeAndWriteStatsFile(table, id);
      assertThat(partitionStats).as("partition statistics for snapshot %s", id).isNotNull();
      table.updatePartitionStatistics().setPartitionStatistics(partitionStats).commit();
      table.updateStatistics().setStatistics(tableStatistics(id)).commit();
    }

    table.refresh();
  }

  private StatisticsFile tableStatistics(long snapshotId) throws IOException {
    String path =
        table.location() + "/metadata/test-table-stats-" + snapshotId + ".puffin-placeholder";
    OutputFile file = table.io().newOutputFile(path);
    byte[] contents = ("table statistics for " + snapshotId).getBytes(StandardCharsets.UTF_8);
    try (OutputStream out = file.createOrOverwrite()) {
      out.write(contents);
    }

    return new GenericStatisticsFile(snapshotId, path, contents.length, 0, ImmutableList.of());
  }

  private TableMetadata currentMetadata() {
    return ((HasTableOperations) table).operations().current();
  }

  private static Set<Long> partitionStatSnapshotIds(TableMetadata metadata) {
    Set<Long> ids = Sets.newHashSet();
    for (PartitionStatisticsFile file : metadata.partitionStatisticsFiles()) {
      ids.add(file.snapshotId());
    }

    return ids;
  }

  private static Set<Long> tableStatSnapshotIds(TableMetadata metadata) {
    Set<Long> ids = Sets.newHashSet();
    for (StatisticsFile file : metadata.statisticsFiles()) {
      ids.add(file.snapshotId());
    }

    return ids;
  }

  private static Map<Long, String> partitionStatPathsBySnapshot(TableMetadata metadata) {
    Map<Long, String> byId = Maps.newHashMap();
    for (PartitionStatisticsFile file : metadata.partitionStatisticsFiles()) {
      byId.put(file.snapshotId(), file.path());
    }

    return byId;
  }

  /** Snapshot id to "path@size", so a re-attached entry has to match in every field. */
  private static Map<Long, String> partitionStatsBySnapshot(TableMetadata metadata) {
    Map<Long, String> byId = Maps.newHashMap();
    for (PartitionStatisticsFile file : metadata.partitionStatisticsFiles()) {
      byId.put(file.snapshotId(), file.path() + "@" + file.fileSizeInBytes());
    }

    return byId;
  }

  private StructLike partition(String value) {
    PartitionKey key = new PartitionKey(table.spec(), SCHEMA);
    Record row = record(0, value);
    key.partition(new InternalRecordWrapper(SCHEMA.asStruct()).wrap(row));
    return key;
  }

  private static List<Record> rows(int fromId, int count, String data) {
    List<Record> result = Lists.newArrayList();
    for (int i = 0; i < count; i += 1) {
      result.add(record(fromId + i, data));
    }

    return result;
  }
}
