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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMapBuilder;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.data.GenericSnapshotRewriteIO;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.snaprewrite.PositionDeleteRequest;
import org.apache.iceberg.snaprewrite.ResurrectionRequest;
import org.apache.iceberg.snaprewrite.RewriteRefusal;
import org.apache.iceberg.snaprewrite.RewriteRefusedException;
import org.apache.iceberg.snaprewrite.SnapshotRewrite;
import org.apache.iceberg.snaprewrite.SnapshotRewriteIO;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.apache.iceberg.util.ContentFileUtil;
import org.junit.jupiter.api.Test;

/**
 * Format v3: snapshots rewrite to deletion vectors.
 *
 * <p>Supported exactly as far as it is lossless. Rows that survived the compaction keep their
 * identity for free -- a rewritten snapshot points at the compaction's own files at the same
 * offsets, so {@code first_row_id + pos} yields what it always did. A row that has to be recovered
 * is different: it lands in a file this rewrite writes, and its {@code _row_id} would be derived
 * from that file instead. So a v3 window that needs no recovery rewrites; one that does is refused
 * rather than allowed through with silently renumbered rows.
 */
public class TestSnapshotRewriteV3 extends SnapshotRewriteTestBase {

  /**
   * The design's headline case on v3: an insert-only window compresses to deletion vectors.
   *
   * <p>The compaction map alone places every inserted row, so no data is read and no data file is
   * written -- only bitmaps.
   */
  @Test
  public void insertOnlyWindowBecomesDeletionVectors() throws IOException {
    useFormatVersion(3);

    append(records(1, 6, "base"));
    compact();
    append(records(10, 3, "alpha"));
    append(records(20, 3, "beta"));
    compact();

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);
    assertIdentityPreserved(result);

    assertThat(result.plan().resurrections()).isEmpty();
    assertThat(result.report().deletePositions()).isPositive();

    // Every delete written must be a deletion vector bound to one data file, not a position delete
    // file spanning a partition.
    for (Snapshot original : result.plan().window()) {
      Snapshot rewritten = result.metadata().snapshot(original.snapshotId());
      for (DeleteFile delete : deleteFilesOf(rewritten, result.metadata())) {
        assertThat(ContentFileUtil.isDV(delete))
            .as("snapshot %s must delete with a DV", original.snapshotId())
            .isTrue();
        assertThat(delete.referencedDataFile()).isNotNull();
      }
    }
  }

  /** Row lineage is preserved because the rewrite points at the compaction's own files. */
  @Test
  public void rowLineageAccountingIsUntouched() throws IOException {
    useFormatVersion(3);

    append(records(1, 5, "base"));
    compact();
    append(records(10, 3, "alpha"));
    compact();

    TableMetadata before = ((HasTableOperations) table).operations().current();
    SnapshotRewriteResult result = rewrite();

    assertThat(result.metadata().nextRowId()).isEqualTo(before.nextRowId());
    for (Snapshot original : table.snapshots()) {
      Snapshot rewritten = result.metadata().snapshot(original.snapshotId());
      assertThat(rewritten.firstRowId()).isEqualTo(original.firstRowId());
      assertThat(rewritten.addedRows()).isEqualTo(original.addedRows());
    }
  }

  /**
   * The row ids themselves, not just the snapshot-level accounting.
   *
   * <p>A row's id derives from its data file's {@code first_row_id} plus its offset. A rewritten
   * snapshot holds the compaction's own files at their own offsets, so the claim is that the
   * derived ids are identical -- but that depends on the manifest writer emitting each file's
   * {@code first_row_id} explicitly rather than letting the manifest list re-assign it from the
   * snapshot's range, which would renumber every surviving row. Checked directly.
   */
  @Test
  public void dataFileRowIdRangesSurviveTheRewrite() throws IOException {
    useFormatVersion(3);

    append(records(1, 6, "base"));
    compact();
    append(records(10, 3, "alpha"));
    append(records(20, 3, "beta"));
    Snapshot compaction = compact();

    Map<String, Long> beforeRewrite = Maps.newHashMap();
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    for (DataFile file : dataFilesOf(compaction, metadata)) {
      assertThat(file.firstRowId()).as("the compaction assigns row ids").isNotNull();
      beforeRewrite.put(file.location(), file.firstRowId());
    }

    SnapshotRewriteResult result = rewrite();

    for (Snapshot original : result.plan().window()) {
      Snapshot rewritten = result.metadata().snapshot(original.snapshotId());
      for (DataFile file : dataFilesOf(rewritten, result.metadata())) {
        assertThat(file.firstRowId())
            .as("file %s in rewritten snapshot %s", file.location(), original.snapshotId())
            .isEqualTo(beforeRewrite.get(file.location()));
      }
    }
  }

  /**
   * Row identity, at the level the rewrite is responsible for.
   *
   * <p>The shared oracle compares values, so rows holding equal values could in principle swap
   * places unnoticed. Under v3 there is a stronger check: every row carries a {@code _row_id} meant
   * to be independent of layout, so comparing those compares identities.
   *
   * <p>What the rewrite guarantees is that a rewritten snapshot reports the identities <i>the
   * compaction</i> reports -- it points at the compaction's files at their own offsets and invents
   * nothing. In an insert-only window every row live at a rewritten snapshot is also live at the
   * compaction, so each identity it returns must be one of the compaction's.
   *
   * <p>It deliberately does <b>not</b> assert that these ids match what the snapshot reported
   * before the rewrite. That is a property of the compaction, not of the rewrite: a compaction
   * preserves row lineage only if it carries {@code first_row_id} forward or materializes {@code
   * _row_id}, and {@link LocalCompactor} does neither -- it lets Iceberg assign a fresh range,
   * renumbering every row at every compaction. Iceberg's Spark rewrite action does preserve
   * lineage; the generic writers have no way to. So end-to-end identity across a history needs a
   * lineage-preserving compaction, and this harness cannot supply one.
   */
  @Test
  public void rewrittenSnapshotsReportTheCompactionsIdentities() throws IOException {
    useFormatVersion(3);

    append(records(1, 6, "base"));
    compact();
    append(records(10, 3, "alpha"));
    append(records(20, 3, "beta"));
    Snapshot compaction = compact();

    SnapshotRewriteResult result = rewrite();
    Table shadow = result.asTable();

    List<String> fromCompaction = identitiesAt(table, compaction.snapshotId());
    assertThat(fromCompaction).hasSize(12);

    for (Snapshot original : result.plan().window()) {
      List<String> rewritten = identitiesAt(shadow, original.snapshotId());
      assertThat(rewritten).as("snapshot %s returns rows", original.snapshotId()).isNotEmpty();
      assertThat(fromCompaction)
          .as("every identity in snapshot %s is one the compaction reports", original.snapshotId())
          .containsAll(rewritten);
    }
  }

  /**
   * Every row's lineage -- {@code _row_id} and {@code _last_updated_sequence_number} -- paired with
   * its values, sorted.
   *
   * <p>Both columns, because relocating a row must preserve both. A null {@code
   * _last_updated_sequence_number} is filled in from the file's sequence number on read, and every
   * operation here moves rows into files with new sequence numbers, so a writer that drops the
   * value changes what the row reports even though the row itself is untouched.
   */
  private List<String> identitiesAt(Table target, long snapshotId) throws IOException {
    Schema withLineage = MetadataColumns.schemaWithRowLineage(target.schema());
    List<String> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(target).useSnapshot(snapshotId).project(withLineage).build()) {
      for (Record record : records) {
        rows.add(
            record.getField(MetadataColumns.ROW_ID.name())
                + " seq="
                + record.getField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name())
                + " => id="
                + record.getField("id")
                + " data="
                + record.getField("data"));
      }
    }

    Collections.sort(rows);
    return rows;
  }

  /**
   * The end-to-end property: identities unchanged across the whole history.
   *
   * <p>This is what the rewrite is ultimately claiming, and until the harness could preserve row
   * lineage through a compaction it was not testable -- {@link LocalCompactor} let Iceberg assign a
   * fresh range and renumbered every row at every compaction, so the ids differed for reasons that
   * had nothing to do with rewriting. With the compaction materializing ids, this compares what
   * each snapshot reported before the rewrite against what it reports after.
   */
  @Test
  public void rowIdentitiesAreUnchangedAcrossTheHistory() throws IOException {
    useFormatVersion(3);

    append(records(1, 6, "base"));
    compact();
    append(records(10, 3, "alpha"));
    append(records(20, 3, "beta"));
    compact();

    Map<Long, List<String>> before = Maps.newLinkedHashMap();
    for (Snapshot snapshot : table.snapshots()) {
      before.put(snapshot.snapshotId(), identitiesAt(table, snapshot.snapshotId()));
    }

    SnapshotRewriteResult result = rewrite();
    Table shadow = result.asTable();

    for (Map.Entry<Long, List<String>> entry : before.entrySet()) {
      assertThat(identitiesAt(shadow, entry.getKey()))
          .as("snapshot %s must report the same row ids after the rewrite", entry.getKey())
          .isEqualTo(entry.getValue());
    }
  }

  /**
   * A v3 window that deletes rows now rewrites, with identities intact.
   *
   * <p>Recovering a row writes it into a new file, so its identity survives only if the id is
   * written out per row. With that in place the whole window rewrites -- the case P1b used to
   * refuse -- and every snapshot still reports the same row ids it always did, recovered rows
   * included.
   */
  @Test
  public void v3WindowWithDeletesRewritesAndKeepsIdentities() throws IOException {
    useFormatVersion(3);

    append(records(1, 6, "base"));
    compact();

    DataFile alpha = append(records(10, 4, "alpha"));
    delete(ImmutableList.of(at(alpha, 1)));
    append(records(20, 3, "beta"));
    compact();

    Map<Long, List<String>> before = Maps.newLinkedHashMap();
    for (Snapshot snapshot : table.snapshots()) {
      before.put(snapshot.snapshotId(), identitiesAt(table, snapshot.snapshotId()));
    }

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);
    assertIdentityPreserved(result);

    assertThat(result.plan().resurrectedRows()).isEqualTo(1);
    assertThat(result.plan().resurrections()).hasSize(1);

    Table shadow = result.asTable();
    for (Map.Entry<Long, List<String>> entry : before.entrySet()) {
      assertThat(identitiesAt(shadow, entry.getKey()))
          .as("snapshot %s keeps its row ids, including the recovered row", entry.getKey())
          .isEqualTo(entry.getValue());
    }
  }

  /**
   * A compaction that leaves {@code _last_updated_sequence_number} null is refused. Its rows read
   * the compaction's own sequence number, which is already wrong, and every rewritten snapshot
   * would read them at the shared base instead, changing what those snapshots report.
   */
  @Test
  public void aCompactionThatDropsLastUpdatedIsRefused() throws IOException {
    useFormatVersion(3);
    append(records(1, 6, "base"));
    compact();
    append(records(10, 3, "alpha"));
    LocalCompactor.compactDroppingLastUpdated(table);

    assertThatThrownBy(this::rewrite)
        .isInstanceOf(RewriteRefusedException.class)
        .satisfies(
            e ->
                assertThat(((RewriteRefusedException) e).refusal())
                    .isEqualTo(RewriteRefusal.LAST_UPDATED_NOT_PRESERVED))
        .hasMessageContaining("null in 9 of 9 rows");
  }

  /**
   * A file a partial compaction kept is restamped at the shared base with the compaction's own
   * output. An appended file never writes {@code _last_updated_sequence_number}, so its rows read
   * the file's sequence number, and in every rewritten snapshot they would read the base. On v3
   * that compaction is refused; {@link #aKeptAppendedFileRewritesOnV2} rewrites the same history.
   */
  @Test
  public void aKeptAppendedFileIsRefused() throws IOException {
    useFormatVersion(3);
    DataFile kept = partialCompactionKeepingAnAppendedFile();

    assertThatThrownBy(this::rewrite)
        .isInstanceOf(RewriteRefusedException.class)
        .satisfies(
            e ->
                assertThat(((RewriteRefusedException) e).refusal())
                    .isEqualTo(RewriteRefusal.LAST_UPDATED_NOT_PRESERVED))
        .hasMessageContaining(kept.location())
        .hasMessageContaining("out of its metrics");
  }

  /** Without row lineage there is nothing to lose, so the history above rewrites on v2. */
  @Test
  public void aKeptAppendedFileRewritesOnV2() throws IOException {
    useFormatVersion(2);
    partialCompactionKeepingAnAppendedFile();
    assertLossless(rewrite());
  }

  private DataFile partialCompactionKeepingAnAppendedFile() throws IOException {
    append(records(1, 6, "base"));
    compact();
    DataFile kept = append(records(10, 3, "keep"));
    append(records(20, 3, "alpha"));
    LocalCompactor.compact(table, true, file -> !file.location().equals(kept.location()));
    return kept;
  }

  /**
   * A map that is wrong but stays inside the compaction's files passes every guard the planner has:
   * each row it places lands in a file the compaction wrote. Under v3 it is refused, because the
   * run it shifts joins rows with different ids. Nothing is written.
   */
  @Test
  public void aWrongMapIsRefused() throws IOException {
    useFormatVersion(3);
    historyWithAShiftedMap();
    List<String> before = filesUnderTable();

    assertThatThrownBy(this::rewrite)
        .isInstanceOf(RewriteRefusedException.class)
        .satisfies(
            e ->
                assertThat(((RewriteRefusedException) e).refusal())
                    .isEqualTo(RewriteRefusal.MAP_MISMATCH))
        .hasMessageContaining("(row id ");
    assertThat(filesUnderTable()).as("a refused rewrite writes nothing").isEqualTo(before);
  }

  /**
   * On v2 there are no ids to check a map against, so the same map is followed and the rewrite
   * reads the wrong rows. This pins the limit rather than endorsing it: a v2 rewrite is only as
   * right as the maps it follows.
   */
  @Test
  public void aWrongMapGoesUnnoticedOnV2() throws IOException {
    useFormatVersion(2);
    historyWithAShiftedMap();
    Map<Long, List<String>> before = allSnapshotRows(table);

    Table shadow = rewrite().asTable();
    long wrong =
        before.entrySet().stream()
            .filter(entry -> !rowsAt(shadow, entry.getKey()).equals(entry.getValue()))
            .count();
    assertThat(wrong).as("snapshots the shifted map makes read the wrong rows").isPositive();
  }

  /**
   * A history whose final compaction's map sends the run at the tail of its output one row early,
   * onto a row of another file. Every row it places still lands inside the output.
   *
   * <p>The tail, because that run belongs to a file appended inside the window. The rewrite asks a
   * map only where a row inserted inside the window went, to hide it from the snapshots before its
   * insert; a row live throughout is whatever the compaction holds that nothing hides. So a wrong
   * run for the floor's rows would change nothing on v2. The v3 check refuses it all the same.
   */
  private void historyWithAShiftedMap() throws IOException {
    append(records(1, 6, "base"));
    compact();
    DataFile alpha = append(records(10, 3, "alpha"));
    delete(ImmutableList.of(at(alpha, 1)));
    append(records(20, 3, "beta"));
    Snapshot compaction = compact();

    // Its own map, found by what it targets: a manifest the compaction rewrote carries the floor's
    // map location too, stamped with the compaction's id.
    Set<String> added = Sets.newHashSet();
    for (DataFile file : compaction.addedDataFiles(table.io())) {
      added.add(file.location());
    }

    String location = null;
    CompactionMap map = null;
    for (ManifestFile manifest : compaction.dataManifests(table.io())) {
      String candidate = manifest.compactionMapLocation();
      if (candidate != null) {
        CompactionMap read = CompactionMaps.read(table.io().newInputFile(candidate));
        if (added.contains(read.fileMappings().get(0).targetFile())) {
          location = candidate;
          map = read;
        }
      }
    }

    assertThat(map).as("the compaction wrote a map").isNotNull();
    long tail = 0;
    for (CompactionMap.FileMapping mapping : map.fileMappings()) {
      for (CompactionMap.Run run : mapping.runs()) {
        tail = Math.max(tail, run.targetPosition());
      }
    }

    CompactionMapBuilder shifted =
        new CompactionMapBuilder(map.sourceSnapshotId(), map.targetSnapshotId());
    for (CompactionMap.FileMapping mapping : map.fileMappings()) {
      CompactionMapBuilder.FileMappingBuilder files =
          shifted.addFileMapping(mapping.sourceFile(), mapping.targetFile());
      for (CompactionMap.Run run : mapping.runs()) {
        long target = run.targetPosition() == tail ? tail - 1 : run.targetPosition();
        files.addRun(run.sourcePosition(), target, run.length(), run.targetFile());
      }
    }

    table.io().deleteFile(location);
    CompactionMaps.write(shifted.build(), table.io().newOutputFile(location));
  }

  /** Every file under the table's location, data and metadata alike. */
  private List<String> filesUnderTable() throws IOException {
    try (java.util.stream.Stream<java.nio.file.Path> paths =
        java.nio.file.Files.walk(
            java.nio.file.Paths.get(table.location().replaceFirst("^file:", "")))) {
      return paths
          .filter(java.nio.file.Files::isRegularFile)
          .map(java.nio.file.Path::toString)
          .sorted()
          .collect(java.util.stream.Collectors.toList());
    }
  }

  /** A recovered row's id is written into the file, not derived from it. */
  @Test
  public void recoveredRowsCarryMaterializedIds() throws IOException {
    useFormatVersion(3);

    append(records(1, 5, "base"));
    compact();
    DataFile compacted = onlyDataFileOfCurrentSnapshot();

    // Delete the last two rows, so a derived id would differ from the original by a visible amount.
    delete(ImmutableList.of(at(compacted, 3), at(compacted, 4)));
    compact();

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);

    ResurrectionRequest recovered = result.plan().resurrections().get(0);
    assertThat(recovered.rowCount()).isEqualTo(2);
    assertThat(recovered.sourceFirstRowIds())
        .as("the planner must pass the source range through")
        .isNotEmpty();

    // The two recovered rows report the ids they had under the previous compaction, which are not
    // the ids their new file's range would imply.
    List<String> ids = Lists.newArrayList();
    for (String identity :
        identitiesAt(result.asTable(), result.plan().window().get(0).snapshotId())) {
      ids.add(identity.substring(0, identity.indexOf(' ')));
    }

    assertThat(ids).containsExactly("0", "1", "2", "3", "4");
  }

  /**
   * Moving a row is not updating it.
   *
   * <p>The compaction and the rewrite both put rows into files with sequence numbers of their own,
   * and a null {@code _last_updated_sequence_number} reads as the file's number. Unless the value
   * is written out, a compaction reports every row it copied as updated by itself, and a rewritten
   * snapshot reports whatever number the rewrite stamped. Pinned against the appends' own numbers,
   * not only compared before and after, so both sides going wrong the same way cannot pass.
   */
  @Test
  public void relocatedRowsKeepTheirLastUpdatedSequenceNumber() throws IOException {
    useFormatVersion(3);

    Map<String, Long> writtenAt = Maps.newHashMap();
    append(records(1, 3, "base"));
    writtenAt.put("base", table.currentSnapshot().sequenceNumber());
    compact();

    DataFile alpha = append(records(10, 3, "alpha"));
    writtenAt.put("alpha", table.currentSnapshot().sequenceNumber());
    // Deleting one makes the rewrite recover it, so the resurrection path is covered too.
    delete(ImmutableList.of(at(alpha, 1)));
    append(records(20, 2, "beta"));
    writtenAt.put("beta", table.currentSnapshot().sequenceNumber());
    Snapshot compaction = compact();

    assertLastUpdatedByAppend(table, compaction.snapshotId(), writtenAt);

    SnapshotRewriteResult result = rewrite();
    assertThat(result.plan().resurrectedRows()).isEqualTo(1);
    for (Snapshot original : result.plan().window()) {
      assertLastUpdatedByAppend(result.asTable(), original.snapshotId(), writtenAt);
    }
  }

  private void assertLastUpdatedByAppend(Table target, long snapshotId, Map<String, Long> writtenAt)
      throws IOException {
    Schema withLineage = MetadataColumns.schemaWithRowLineage(target.schema());
    int rows = 0;
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(target).useSnapshot(snapshotId).project(withLineage).build()) {
      for (Record record : records) {
        String data = (String) record.getField("data");
        assertThat(record.getField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()))
            .as("%s in snapshot %s was last updated by the append that wrote it", data, snapshotId)
            .isEqualTo(writtenAt.get(data.substring(0, data.indexOf('-'))));
        rows += 1;
      }
    }

    assertThat(rows).as("snapshot %s returns rows", snapshotId).isPositive();
  }

  /**
   * An implementation that cannot preserve identities is still refused.
   *
   * <p>The gate is the capability, not the format version, so a v2-only implementation stays usable
   * on v2 and is stopped before it renumbers rows on v3.
   */
  @Test
  public void refusesV3RecoveryWhenTheWriterCannotPreserveIds() throws IOException {
    useFormatVersion(3);

    append(records(1, 5, "base"));
    compact();

    DataFile alpha = append(records(10, 4, "alpha"));
    delete(ImmutableList.of(at(alpha, 1)));
    compact();

    SnapshotRewriteIO lineageBlind =
        new ForwardingSnapshotRewriteIO(new GenericSnapshotRewriteIO(table)) {
          @Override
          public boolean preservesRowLineage() {
            return false;
          }
        };

    assertThatThrownBy(
            () ->
                SnapshotRewrite.forTable(table, lineageBlind)
                    .onLatestCompaction()
                    .maxDeadRatio(Double.MAX_VALUE)
                    .plan())
        .isInstanceOf(RewriteRefusedException.class)
        .hasMessageContaining("does not preserve row ids")
        .extracting(e -> ((RewriteRefusedException) e).refusal())
        .isEqualTo(RewriteRefusal.ROW_LINEAGE);
  }

  /** Delegates everything, so a test can vary one answer. */
  private static class ForwardingSnapshotRewriteIO implements SnapshotRewriteIO {
    private final SnapshotRewriteIO delegate;

    ForwardingSnapshotRewriteIO(SnapshotRewriteIO delegate) {
      this.delegate = delegate;
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

  private DataFile onlyDataFileOfCurrentSnapshot() throws IOException {
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    List<DataFile> files = dataFilesOf(table.currentSnapshot(), metadata);
    assertThat(files).hasSize(1);
    return files.get(0);
  }

  /**
   * On v3 a mis-stamped rewrite fails its own stamping check, and cannot be committed.
   *
   * <p>{@code DeleteFileIndex.findDV} would refuse to plan the result, since a deletion vector
   * sorts below the data file it references, but only after the rewrite had committed it. The
   * stamping check refuses it first, and the table is left as it was.
   */
  @Test
  public void misStampedV3RewriteFailsItsStampingCheck() throws IOException {
    useFormatVersion(3);

    append(records(1, 6, "base"));
    compact();
    append(records(10, 3, "alpha"));
    compact();

    SnapshotRewriteResult broken =
        org.apache.iceberg.snaprewrite.RewriteStampingHook.materializeMisStamped(rewriter());

    assertThatThrownBy(broken::checkStamping)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Mis-stamped rewrite");
    TableMetadata before = ((HasTableOperations) table).operations().refresh();
    assertThatThrownBy(() -> broken.commit(((HasTableOperations) table).operations()))
        .isInstanceOf(IllegalStateException.class);
    assertThat(((HasTableOperations) table).operations().refresh().metadataFileLocation())
        .as("a refused commit leaves the table as it was")
        .isEqualTo(before.metadataFileLocation());

    broken.discard();
  }

  private List<DataFile> dataFilesOf(Snapshot snapshot, TableMetadata metadata) throws IOException {
    List<DataFile> files = Lists.newArrayList();
    for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, table.io(), metadata.specsById())) {
        for (DataFile file : reader) {
          files.add(file);
        }
      }
    }

    return files;
  }

  private List<DeleteFile> deleteFilesOf(Snapshot snapshot, TableMetadata metadata)
      throws IOException {
    List<DeleteFile> files = Lists.newArrayList();
    for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), metadata.specsById())) {
        for (DeleteFile file : reader) {
          files.add(file);
        }
      }
    }

    return files;
  }
}
