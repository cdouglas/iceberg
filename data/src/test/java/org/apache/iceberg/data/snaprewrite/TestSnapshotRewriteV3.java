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
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.snaprewrite.RewriteRefusal;
import org.apache.iceberg.snaprewrite.RewriteRefusedException;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.apache.iceberg.util.ContentFileUtil;
import org.junit.jupiter.api.Test;

/**
 * Format v3: snapshots rewrite to deletion vectors.
 *
 * <p>Supported exactly as far as it is lossless. Rows that survived the compaction keep their
 * identity for free -- a rewritten snapshot points at the compaction's own files at the same offsets,
 * so {@code first_row_id + pos} yields what it always did. A row that has to be recovered is
 * different: it lands in a file this rewrite writes, and its {@code _row_id} would be derived from
 * that file instead. So a v3 window that needs no recovery rewrites; one that does is refused rather
 * than allowed through with silently renumbered rows.
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
   * snapshot holds the compaction's own files at their own offsets, so the claim is that the derived
   * ids are identical -- but that depends on the manifest writer emitting each file's
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
   * <p>It deliberately does <b>not</b> assert that these ids match what the snapshot reported before
   * the rewrite. That is a property of the compaction, not of the rewrite: a compaction preserves row
   * lineage only if it carries {@code first_row_id} forward or materializes {@code _row_id}, and
   * {@link LocalCompactor} does neither -- it lets Iceberg assign a fresh range, renumbering every
   * row at every compaction. Iceberg's Spark rewrite action does preserve lineage; the generic
   * writers have no way to. So end-to-end identity across a history needs a lineage-preserving
   * compaction, and this harness cannot supply one.
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

  /** Every row's {@code _row_id} paired with its values, sorted. */
  private List<String> identitiesAt(Table target, long snapshotId) throws IOException {
    Schema withLineage = MetadataColumns.schemaWithRowLineage(target.schema());
    List<String> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(target).useSnapshot(snapshotId).project(withLineage).build()) {
      for (Record record : records) {
        rows.add(
            record.getField(MetadataColumns.ROW_ID.name())
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
   * had nothing to do with rewriting. With the compaction materializing ids, this compares what each
   * snapshot reported before the rewrite against what it reports after.
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
   * A v3 window that would have to recover rows is refused.
   *
   * <p>Recovering a row means writing it into a new file, and without a materialized {@code _row_id}
   * that changes its identity. Renumbering rows in a historical snapshot is exactly the kind of loss
   * this rewrite exists to avoid, so it refuses rather than proceeds.
   */
  @Test
  public void refusesWhenV3WouldHaveToRecoverRows() throws IOException {
    useFormatVersion(3);

    append(records(1, 5, "base"));
    compact();

    DataFile alpha = append(records(10, 4, "alpha"));
    delete(ImmutableList.of(at(alpha, 1)));
    compact();

    assertThatThrownBy(() -> rewriter().plan())
        .isInstanceOf(RewriteRefusedException.class)
        .hasMessageContaining("row ids")
        .extracting(e -> ((RewriteRefusedException) e).refusal())
        .isEqualTo(RewriteRefusal.ROW_LINEAGE);
  }

  /**
   * On v3 a mis-stamped rewrite fails loudly rather than silently.
   *
   * <p>{@code DeleteFileIndex.findDV} raises when a deletion vector sorts below the data file it
   * references, where v2 simply drops the delete from the returned slice and the rows reappear. Same
   * mistake, opposite failure mode -- worth pinning, because it is the reason the v2 oracle has to
   * carry the weight that v3 gets from the format.
   */
  @Test
  public void misStampedV3RewriteFailsLoudly() throws IOException {
    useFormatVersion(3);

    append(records(1, 6, "base"));
    compact();
    append(records(10, 3, "alpha"));
    compact();

    SnapshotRewriteResult broken =
        org.apache.iceberg.snaprewrite.RewriteStampingHook.materializeMisStamped(rewriter());

    assertThatThrownBy(() -> assertLossless(broken))
        .as("v3 refuses to plan a scan whose DV sorts below its data file")
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("DV data sequence number");

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
