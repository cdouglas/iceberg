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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericSnapshotRewriteIO;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.primitives.Longs;
import org.apache.iceberg.snaprewrite.PositionDeleteRequest;
import org.apache.iceberg.snaprewrite.ResurrectionRequest;
import org.apache.iceberg.snaprewrite.SnapshotRewrite;
import org.apache.iceberg.snaprewrite.SnapshotRewriteIO;
import org.apache.iceberg.snaprewrite.SnapshotRewriteLedger;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.junit.jupiter.api.Test;

/**
 * The ledger a rewrite writes, checked against the layout it describes while that layout still
 * exists (SNAPREWRITE_SPEC.md 14.3).
 *
 * <p>Validated on its own and not only through a reconstruction: a reconstruction that came back
 * right could still be resting on a ledger that is right only where the test happened to look.
 * Every expectation here is computed from the original files and from scans of the original
 * snapshots, never from the planner's own bookkeeping.
 */
public class TestSnapshotRewriteLedger extends SnapshotRewriteTestBase {

  @Test
  public void ledgerDescribesTheOriginalLayout() throws IOException {
    useFormatVersion(3);
    buildHistoryWithAHole();

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);
    assertThat(result.plan().ledgerUnavailable()).isNull();
    SnapshotRewriteLedger ledger = result.ledger();
    assertThat(ledger).isNotNull();

    List<Snapshot> window = result.plan().window();
    List<Long> windowIds = Lists.newArrayList();
    for (Snapshot snapshot : window) {
      windowIds.add(snapshot.snapshotId());
    }

    List<Long> ledgerIds = Lists.newArrayList();
    for (SnapshotRewriteLedger.WindowSnapshot snapshot : ledger.window()) {
      ledgerIds.add(snapshot.snapshotId());
    }

    assertThat(ledgerIds).isEqualTo(windowIds);
    assertThat(ledger.window().get(0).compactionMapLocation())
        .as("the floor is a compaction, and the ledger keeps its map")
        .isNotNull();

    // Which window snapshots referenced each file, from the original manifests.
    Map<String, DataFile> originals = Maps.newHashMap();
    Map<String, List<Integer>> referencedAt = Maps.newHashMap();
    for (int index = 0; index < window.size(); index += 1) {
      for (DataFile file : dataFiles(table, window.get(index))) {
        originals.putIfAbsent(file.location(), file);
        referencedAt.computeIfAbsent(file.location(), path -> Lists.newArrayList()).add(index);
      }
    }

    Set<String> described = Sets.newHashSet();
    int holes = 0;
    for (SnapshotRewriteLedger.Entry entry : ledger.entries()) {
      String path = entry.file().location();
      described.add(path);
      DataFile original = originals.get(path);

      List<Integer> span = Lists.newArrayList();
      for (int index = entry.first(); index <= entry.last(); index += 1) {
        span.add(index);
      }

      assertThat(referencedAt.get(path)).as("span of %s", path).isEqualTo(span);
      assertThat(entry.dataSequenceNumber()).isEqualTo(original.dataSequenceNumber());
      assertThat(entry.fileSequenceNumber()).isEqualTo(original.fileSequenceNumber());
      assertThat(entry.file().firstRowId()).isEqualTo(original.firstRowId());
      assertThat(entry.file().recordCount()).isEqualTo(original.recordCount());
      assertThat(addedPaths(table.snapshot(entry.addedSnapshotId())))
          .as("the snapshot recorded as adding %s added it", path)
          .contains(path);

      List<Long> ids = Lists.newArrayList();
      for (Record row : RawFiles.readAllWithLineage(table.io(), original, table.schema())) {
        ids.add((Long) row.getField(MetadataColumns.ROW_ID.name()));
      }

      assertThat(Longs.asList(entry.rowIds())).as("row ids of %s", path).isEqualTo(ids);

      // Holes are what no window snapshot reads, found by scanning every one that references it.
      Set<Long> read = Sets.newHashSet();
      for (int index : span) {
        read.addAll(
            livePositions(table, window.get(index).snapshotId()).getOrDefault(path, Set.of()));
      }

      Set<Long> expectedHoles = Sets.newHashSet();
      for (long pos = 0; pos < original.recordCount(); pos += 1) {
        if (!read.contains(pos)) {
          expectedHoles.add(pos);
        }
      }

      Set<Long> recordedHoles = Sets.newHashSet();
      entry.holes().forEach(recordedHoles::add);
      assertThat(recordedHoles).as("holes in %s", path).isEqualTo(expectedHoles);
      holes += recordedHoles.size();
    }

    assertThat(described).isEqualTo(referencedAt.keySet());
    assertThat(holes).as("the row inserted and deleted by one commit is the only hole").isOne();

    // What was written is what reads back, and every rewritten snapshot names it.
    SnapshotRewriteLedger reread =
        SnapshotRewriteLedger.read(table.io().newInputFile(result.ledgerLocation()), table.specs());
    assertThat(reread.compactionSnapshotId()).isEqualTo(ledger.compactionSnapshotId());
    assertThat(reread.entries()).hasSameSizeAs(ledger.entries());
    for (int i = 0; i < ledger.entries().size(); i += 1) {
      SnapshotRewriteLedger.Entry written = ledger.entries().get(i);
      SnapshotRewriteLedger.Entry read = reread.entries().get(i);
      assertThat(read.file().location()).isEqualTo(written.file().location());
      assertThat(read.file().partition()).isEqualTo(written.file().partition());
      assertThat(read.file().firstRowId()).isEqualTo(written.file().firstRowId());
      assertThat(read.rowIds()).isEqualTo(written.rowIds());
      assertThat(read.holes().toString()).isEqualTo(written.holes().toString());
      assertThat(read.first()).isEqualTo(written.first());
      assertThat(read.last()).isEqualTo(written.last());
      assertThat(read.addedSnapshotId()).isEqualTo(written.addedSnapshotId());
    }

    for (long id : windowIds) {
      assertThat(result.metadata().snapshot(id).summary())
          .containsEntry(SnapshotRewriteLedger.SUMMARY_KEY, result.ledgerLocation());
    }
  }

  /**
   * Only files whose ids may be written per row are read.
   *
   * <p>An appended file's ids all derive from its range, and its metrics say so, so the ledger
   * describes it with one run and no read. A compaction's output writes its ids out, because they
   * came from unrelated ranges, so it has to be read.
   */
  @Test
  public void onlyFilesThatWriteTheirIdsAreRead() throws IOException {
    useFormatVersion(3);

    append(records(1, 6, "base"));
    Snapshot floor = compact();
    append(records(10, 3, "alpha"));
    append(records(20, 3, "beta"));
    compact();

    CountingIO io = new CountingIO(new GenericSnapshotRewriteIO(table));
    SnapshotRewriteResult result =
        SnapshotRewrite.forTable(table, io)
            .onLatestCompaction()
            .maxDeadRatio(Double.MAX_VALUE)
            .materialize();

    assertThat(io.read).as("only the floor compaction's output").isEqualTo(addedPaths(floor));
    for (SnapshotRewriteLedger.Entry entry : result.ledger().entries()) {
      if (!io.read.contains(entry.file().location())) {
        assertThat(entry.rowIdRunCount()).as("an appended file is one run").isOne();
      }
    }
  }

  @Test
  public void noLedgerBelowV3() throws IOException {
    append(records(1, 6, "base"));
    compact();
    append(records(10, 3, "alpha"));
    compact();

    SnapshotRewriteResult result = rewrite();
    assertThat(result.ledger()).isNull();
    assertThat(result.plan().ledgerUnavailable()).contains("v3");
    for (Snapshot snapshot : result.plan().window()) {
      assertThat(result.metadata().snapshot(snapshot.snapshotId()).summary())
          .doesNotContainKey(SnapshotRewriteLedger.SUMMARY_KEY);
    }
  }

  /** A window still rewrites without a ledger; it just cannot be reconstructed once reclaimed. */
  @Test
  public void noLedgerWhenTheIoCannotPreserveLineage() throws IOException {
    useFormatVersion(3);

    append(records(1, 6, "base"));
    compact();
    append(records(10, 3, "alpha"));
    compact();

    SnapshotRewriteIO blind =
        new CountingIO(new GenericSnapshotRewriteIO(table)) {
          @Override
          public boolean preservesRowLineage() {
            return false;
          }
        };
    SnapshotRewriteResult result =
        SnapshotRewrite.forTable(table, blind)
            .onLatestCompaction()
            .maxDeadRatio(Double.MAX_VALUE)
            .materialize();

    assertLossless(result);
    assertThat(result.ledger()).isNull();
    assertThat(result.plan().ledgerUnavailable()).contains("lineage");
  }

  /**
   * A floor compaction, a row that dies inside the window, and a row inserted and deleted by one
   * commit -- the one row no window snapshot can read.
   */
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

  static List<DataFile> dataFiles(Table target, Snapshot snapshot) throws IOException {
    List<DataFile> files = Lists.newArrayList();
    for (ManifestFile manifest : snapshot.dataManifests(target.io())) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, target.io(), target.specs())) {
        for (DataFile file : reader) {
          files.add(file.copy());
        }
      }
    }

    return files;
  }

  private Set<String> addedPaths(Snapshot snapshot) {
    Set<String> paths = Sets.newHashSet();
    for (DataFile file : snapshot.addedDataFiles(table.io())) {
      paths.add(file.location());
    }

    return paths;
  }

  /** Positions a snapshot reads, by file: what a scan returns, not what the manifests say. */
  static Map<String, Set<Long>> livePositions(Table target, long snapshotId) throws IOException {
    List<org.apache.iceberg.types.Types.NestedField> columns =
        Lists.newArrayList(target.schema().columns());
    columns.add(MetadataColumns.FILE_PATH);
    columns.add(MetadataColumns.ROW_POSITION);
    Map<String, Set<Long>> positions = Maps.newHashMap();
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(target).useSnapshot(snapshotId).project(new Schema(columns)).build()) {
      for (Record record : records) {
        positions
            .computeIfAbsent(
                record.getField(MetadataColumns.FILE_PATH.name()).toString(),
                path -> Sets.newHashSet())
            .add((Long) record.getField(MetadataColumns.ROW_POSITION.name()));
      }
    }

    return positions;
  }

  /** Forwards everything, and records which files had their row ids read. */
  private static class CountingIO implements SnapshotRewriteIO {
    private final SnapshotRewriteIO delegate;
    private final Set<String> read = Sets.newHashSet();

    CountingIO(SnapshotRewriteIO delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean preservesRowLineage() {
      return delegate.preservesRowLineage();
    }

    @Override
    public long[] readRowIdRuns(DataFile file) {
      read.add(file.location());
      return delegate.readRowIdRuns(file);
    }

    @Override
    public PositionDeleteIndex loadPositionDeletes(
        Iterable<DeleteFile> deleteFiles, CharSequence dataFilePath) {
      return delegate.loadPositionDeletes(deleteFiles, dataFilePath);
    }

    @Override
    public DataFile resurrect(ResurrectionRequest request) {
      return delegate.resurrect(request);
    }

    @Override
    public DeleteFile writePositionDeletes(PositionDeleteRequest request) {
      return delegate.writePositionDeletes(request);
    }
  }
}
