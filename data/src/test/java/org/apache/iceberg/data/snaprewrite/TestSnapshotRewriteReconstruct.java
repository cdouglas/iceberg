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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.snaprewrite.RewriteRefusal;
import org.apache.iceberg.snaprewrite.RewriteRefusedException;
import org.apache.iceberg.snaprewrite.SnapshotReconstructReport;
import org.apache.iceberg.snaprewrite.SnapshotReconstructResult;
import org.apache.iceberg.snaprewrite.SnapshotRewriteReconstruct;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Rewrite, commit, reclaim, reconstruct: the original layout comes back without the original files
 * (SNAPREWRITE_SPEC.md 14).
 *
 * <p>Every case reclaims before reconstructing and asserts that the originals are gone, so nothing
 * here can pass by pointing back at them. The comparison is {@link LayoutFingerprint}: file by file
 * and position by position, not only what each snapshot reads.
 */
public class TestSnapshotRewriteReconstruct extends SnapshotRewriteTestBase {

  /**
   * A history with no holes comes back exactly: every file at its own path, every position, id and
   * sequence number, which commit added it, and the summaries.
   */
  @Test
  public void reconstructsTheOriginalLayoutAfterReclaim() throws IOException {
    useFormatVersion(3);
    Snapshot floor = buildHistoryWithoutHoles();
    String floorMap = compactionMapOf(floor);
    assertThat(floorMap).isNotNull();

    LayoutFingerprint before = LayoutFingerprint.of(table);
    Map<Long, Set<String>> appendedBefore = appendedFiles();

    SnapshotRewriteResult rewritten = rewrite();
    commitAndReclaim(rewritten);

    SnapshotReconstructResult result = reconstruct();
    LayoutFingerprint.Comparison shadow = before.assertReproducedBy(result.asTable());
    assertThat(shadow.moved()).isZero();

    SnapshotReconstructReport report = result.report();
    assertThat(report.snapshots()).isEqualTo(rewritten.plan().window().size());
    assertThat(report.rebuiltInPlace()).isEqualTo(3);
    assertThat(report.rebuiltMoved()).isZero();
    assertThat(report.holes()).isZero();
    assertThat(report.rebuiltBytes()).isPositive();
    assertThat(report.reclaimableBytes()).isPositive();

    commitAndReclaim(result);
    before.assertReproducedBy(table);

    // Provenance is back: an incremental scan sees each commit's own files again.
    assertThat(appendedFiles()).isEqualTo(appendedBefore);

    // So is the floor's compaction map, which still describes its files: they kept their paths.
    assertThat(compactionMapOf(table.snapshot(floor.snapshotId()))).isEqualTo(floorMap);
  }

  /**
   * A row inserted and deleted by one commit was never readable, so the rewritten layout never held
   * it. Its file comes back without it, at a new path, and every row it did hold keeps its id.
   */
  @Test
  public void aHoleIsLeftOutAndItsFileMoves() throws IOException {
    useFormatVersion(3);
    buildHistoryWithAHole();

    LayoutFingerprint before = LayoutFingerprint.of(table);
    commitAndReclaim(rewrite());

    SnapshotReconstructResult result = reconstruct();
    LayoutFingerprint.Comparison shadow = before.assertReproducedBy(result.asTable());
    assertThat(shadow.moved()).isOne();
    assertThat(result.report().rebuiltMoved()).isOne();
    assertThat(result.report().holes()).isOne();

    commitAndReclaim(result);
    before.assertReproducedBy(table);
  }

  /** Two compactions in the window: each comes back with the map that names its files. */
  @Test
  public void compactionsInsideTheWindowKeepTheirMaps() throws IOException {
    useFormatVersion(3);

    append(records(1, 6, "base"));
    Snapshot floor = compact();
    DataFile alpha = append(records(10, 4, "alpha"));
    delete(ImmutableList.of(at(alpha, 0)));
    Snapshot middle = compact();
    append(records(20, 3, "beta"));
    compact();

    LayoutFingerprint before = LayoutFingerprint.of(table);
    String middleMap = compactionMapOf(middle);
    SnapshotRewriteResult rewritten = rewriter().floor(floor.snapshotId()).materialize();
    assertLossless(rewritten);
    commitAndReclaim(rewritten);

    SnapshotReconstructResult result = reconstruct();
    before.assertReproducedBy(result.asTable());
    commitAndReclaim(result);
    before.assertReproducedBy(table);
    assertThat(compactionMapOf(table.snapshot(middle.snapshotId()))).isEqualTo(middleMap);
  }

  /** Rebuilt files land in their own partitions, and their deletion vectors with them. */
  @Test
  public void partitionedFilesComeBackInTheirPartitions() throws IOException {
    useSchema(SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("data").build(), 3);

    append(partition("east"), tagged(1, 4, "east"));
    append(partition("west"), tagged(10, 4, "west"));
    compact();
    DataFile east = append(partition("east"), tagged(20, 3, "east"));
    delete(partition("east"), ImmutableList.of(at(east, 1)));
    append(partition("west"), tagged(30, 2, "west"));
    compact();

    LayoutFingerprint before = LayoutFingerprint.of(table);
    commitAndReclaim(rewrite());

    SnapshotReconstructResult result = reconstruct();
    assertThat(before.assertReproducedBy(result.asTable()).moved()).isZero();
    commitAndReclaim(result);
    before.assertReproducedBy(table);
  }

  /**
   * A partial compaction leaves a file the compaction still holds, so reclaim never deletes it. It
   * is referenced again as it is, not rebuilt.
   */
  @Test
  public void aFileThePartialCompactionKeptIsReused() throws IOException {
    useFormatVersion(3);

    append(records(1, 4, "base"));
    compact();
    DataFile untouched = append(records(10, 3, "keep"));
    DataFile alpha = append(records(20, 3, "alpha"));
    delete(ImmutableList.of(at(alpha, 0)));
    delete(ImmutableList.of(at(untouched, 2)));
    LocalCompactor.compact(table, true, file -> !file.location().equals(untouched.location()));

    LayoutFingerprint before = LayoutFingerprint.of(table);
    SnapshotRewriteResult rewritten = rewrite();
    assertLossless(rewritten);
    commitAndReclaim(rewritten);
    assertThat(table.io().newInputFile(untouched.location()).exists()).isTrue();

    SnapshotReconstructResult result = reconstruct();
    assertThat(result.report().reusedFiles()).isOne();
    before.assertReproducedBy(result.asTable());
    commitAndReclaim(result);
    before.assertReproducedBy(table);
  }

  /**
   * A replace with no map inside the window: the rewrite copied every row it moved, so each has two
   * physical copies with one id, live at different times. Each original file gets the copy that was
   * live where its rows were born.
   */
  @Test
  public void aReplaceWithoutAMapComesBack() throws IOException {
    useFormatVersion(3);

    append(records(1, 6, "base"));
    Snapshot floor = compact();
    append(records(10, 3, "alpha"));
    LocalCompactor.compact(table, false);
    append(records(20, 2, "beta"));
    compact();

    LayoutFingerprint before = LayoutFingerprint.of(table);
    SnapshotRewriteResult rewritten = rewriter().floor(floor.snapshotId()).materialize();
    assertLossless(rewritten);
    commitAndReclaim(rewritten);

    SnapshotReconstructResult result = reconstruct();
    before.assertReproducedBy(result.asTable());
    commitAndReclaim(result);
    before.assertReproducedBy(table);
  }

  /**
   * Rewriting a reconstructed history again works, and reads like the first rewrite did: the
   * compaction maps still name the files, because the files kept their paths.
   */
  @Test
  public void aReconstructionCanBeRewrittenAgain() throws IOException {
    useFormatVersion(3);
    buildHistoryWithoutHoles();
    Map<Long, List<String>> rows = allSnapshotRows(table);

    commitAndReclaim(rewrite());
    commitAndReclaim(reconstruct());

    SnapshotRewriteResult again = rewrite();
    assertLossless(again);
    assertThat(again.ledger()).isNotNull();
    for (Map.Entry<Long, List<String>> entry : rows.entrySet()) {
      assertThat(rowsAt(again.asTable(), entry.getKey())).isEqualTo(entry.getValue());
    }
  }

  /**
   * A file that moved is one the compaction's map no longer names, so rewriting onto that
   * compaction again must refuse rather than mistake the file for one the compaction left alone.
   *
   * <p>Without the refusal the rewrite would delete the moved file's rows from a file no rewritten
   * snapshot holds, masking nothing, and they would show up before the commit that inserted them.
   */
  @Test
  public void rewritingAgainPastAMovedFileIsRefused() throws IOException {
    useFormatVersion(3);
    buildHistoryWithAHole();
    commitAndReclaim(rewrite());
    commitAndReclaim(reconstruct());

    assertThatThrownBy(this::rewrite)
        .isInstanceOf(RewriteRefusedException.class)
        .satisfies(
            e ->
                assertThat(((RewriteRefusedException) e).refusal())
                    .isEqualTo(RewriteRefusal.UNLOCATABLE_ROW))
        .hasMessageContaining("neither the compaction's nor a recovered file");
  }

  /**
   * Two passes, reconstructed in one step.
   *
   * <p>The second pass rewrote the first pass's output, so the first pass's snapshots carry both
   * ledgers. The first one reaches the original, and because a rewrite never changes a row's id it
   * applies directly to the second pass's layout: there is no need to undo the passes in turn.
   */
  @Test
  public void twoPassesAreReconstructedInOneStep() throws IOException {
    useFormatVersion(3);
    Snapshot floor = buildHistoryWithoutHoles();
    LayoutFingerprint firstWindow = LayoutFingerprint.of(table);

    SnapshotRewriteResult first = rewrite();
    commitAndReclaim(first);

    Set<Long> rewrittenOnce = Sets.newHashSet();
    for (Snapshot snapshot : first.plan().window()) {
      rewrittenOnce.add(snapshot.snapshotId());
    }

    DataFile gamma = append(records(30, 3, "gamma"));
    delete(ImmutableList.of(at(gamma, 2)));
    compact();

    List<Long> rest = Lists.newArrayList();
    for (Snapshot snapshot : table.snapshots()) {
      if (!rewrittenOnce.contains(snapshot.snapshotId())) {
        rest.add(snapshot.snapshotId());
      }
    }

    LayoutFingerprint secondWindow = LayoutFingerprint.of(table, rest);

    SnapshotRewriteResult second = rewriter().floor(floor.snapshotId()).materialize();
    assertLossless(second);
    commitAndReclaim(second);

    table.refresh();
    assertThat(SnapshotRewriteReconstruct.forTable(table, null).ledgers().keySet())
        .containsExactlyInAnyOrder(first.ledgerLocation(), second.ledgerLocation());

    SnapshotReconstructResult result = reconstruct();
    firstWindow.assertReproducedBy(result.asTable());
    secondWindow.assertReproducedBy(result.asTable());

    commitAndReclaim(result);
    firstWindow.assertReproducedBy(table);
    secondWindow.assertReproducedBy(table);
  }

  /** A reconstruction can be undone by pointing back, until the rewritten layout is reclaimed. */
  @Test
  public void aReconstructionCanBeUndone() throws IOException {
    useFormatVersion(3);
    buildHistoryWithoutHoles();
    Map<Long, List<String>> rows = allSnapshotRows(table);

    commitAndReclaim(rewrite());
    Map<Long, String> rewrittenLists = Maps.newHashMap();
    for (Snapshot snapshot : table.snapshots()) {
      rewrittenLists.put(snapshot.snapshotId(), snapshot.manifestListLocation());
    }

    SnapshotReconstructResult result = reconstruct();
    TableOperations ops = ((HasTableOperations) table).operations();
    result.commit(ops);
    table.refresh();

    TableMetadata reconstructed = ops.current();
    ops.commit(reconstructed, SnapshotRewriteReconstruct.undo(reconstructed));
    table.refresh();

    for (Snapshot snapshot : table.snapshots()) {
      assertThat(snapshot.manifestListLocation())
          .isEqualTo(rewrittenLists.get(snapshot.snapshotId()));
      assertThat(rowsAt(table, snapshot.snapshotId())).isEqualTo(rows.get(snapshot.snapshotId()));
    }
  }

  @Test
  public void refusesWithoutALedger() throws IOException {
    useFormatVersion(3);
    buildHistoryWithoutHoles();

    assertThatThrownBy(this::reconstruct)
        .isInstanceOf(RewriteRefusedException.class)
        .satisfies(
            e ->
                assertThat(((RewriteRefusedException) e).refusal())
                    .isEqualTo(RewriteRefusal.NO_LEDGER))
        .hasMessageContaining("no snapshot names a ledger");
  }

  @Test
  public void refusesOnceTheSchemaHasChanged() throws IOException {
    useFormatVersion(3);
    buildHistoryWithoutHoles();
    commitAndReclaim(rewrite());

    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();

    assertThatThrownBy(this::reconstruct)
        .isInstanceOf(RewriteRefusedException.class)
        .satisfies(
            e ->
                assertThat(((RewriteRefusedException) e).refusal())
                    .isEqualTo(RewriteRefusal.SCHEMA_CHANGED))
        .hasMessageContaining("was written at schema");
  }

  // ------------------------------------------------------------------ histories

  /**
   * A floor compaction, a row that dies inside the window both in an appended file and in the
   * compaction's own output, and nothing inserted and deleted together.
   */
  private Snapshot buildHistoryWithoutHoles() throws IOException {
    append(records(1, 6, "base"));
    Snapshot floor = compact();
    DataFile compacted = LayoutFingerprint.dataFiles(table, floor).get(0);

    DataFile alpha = append(records(10, 4, "alpha"));
    delete(ImmutableList.of(at(alpha, 1)));
    append(records(20, 3, "beta"));
    delete(ImmutableList.of(at(compacted, 2)));
    compact();
    return floor;
  }

  /** As in {@link TestSnapshotRewriteLedger}: one row is inserted and deleted by one commit. */
  private void buildHistoryWithAHole() throws IOException {
    append(records(1, 6, "base"));
    compact();

    DataFile alpha = append(records(10, 4, "alpha"));
    delete(ImmutableList.of(at(alpha, 1)));

    DataFile gamma = writeData(records(20, 3, "gamma"));
    table
        .newRowDelta()
        .addRows(gamma)
        .addDeletes(writeDeletes(ImmutableList.of(at(gamma, 1))))
        .commit();

    append(records(30, 2, "delta"));
    compact();
  }

  // ------------------------------------------------------------------ helpers

  private StructLike partition(String value) {
    PartitionData partition = new PartitionData(table.spec().partitionType());
    partition.set(0, value);
    return partition;
  }

  /** Rows that all carry the same {@code data}, so an identity partition on it holds them all. */
  private static List<Record> tagged(int fromId, int count, String data) {
    List<Record> rows = Lists.newArrayList();
    for (int i = 0; i < count; i += 1) {
      rows.add(record(fromId + i, data));
    }

    return rows;
  }

  private String compactionMapOf(Snapshot snapshot) {
    for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
      if (manifest.compactionMapLocation() != null) {
        return manifest.compactionMapLocation();
      }
    }

    return null;
  }

  /** The data files an incremental append scan sees each snapshot add, by snapshot. */
  private Map<Long, Set<String>> appendedFiles() throws IOException {
    Map<Long, Set<String>> appended = Maps.newHashMap();
    for (Snapshot snapshot : table.snapshots()) {
      if (snapshot.parentId() == null) {
        continue;
      }

      Set<String> paths = Sets.newHashSet();
      try (CloseableIterable<FileScanTask> tasks =
          table
              .newIncrementalAppendScan()
              .fromSnapshotExclusive(snapshot.parentId())
              .toSnapshot(snapshot.snapshotId())
              .planFiles()) {
        for (FileScanTask task : tasks) {
          paths.add(task.file().location());
        }
      }

      appended.put(snapshot.snapshotId(), paths);
    }

    return appended;
  }
}
