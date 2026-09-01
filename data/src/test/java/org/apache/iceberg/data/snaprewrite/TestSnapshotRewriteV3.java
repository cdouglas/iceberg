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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
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
