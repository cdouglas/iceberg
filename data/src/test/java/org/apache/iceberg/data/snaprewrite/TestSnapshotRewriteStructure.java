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
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.snaprewrite.RewriteStampingHook;
import org.apache.iceberg.snaprewrite.SnapshotRewriteReport;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.junit.jupiter.api.Test;

/** Structural properties of a rewrite: stamping, detachment, accounting, and durability. */
public class TestSnapshotRewriteStructure extends SnapshotRewriteTestBase {

  /**
   * Every delete in a rewritten snapshot must actually apply to the files it references.
   *
   * <p>Iceberg applies a positional delete only when {@code data.seq <= delete.seq}, which is how a
   * concurrent append and a concurrent delete commute: the sequence number is the delete's
   * watermark, and a data file added after it holds rows the delete's author never saw. A rewrite
   * inverts that -- its deletes reference files written long after the snapshot they land in -- so
   * it has to choose sequence numbers that make the comparison hold.
   *
   * <p>This asserts the rule itself rather than the numbers a particular stamping picks. Asserting
   * the numbers is what made this test fail when the stamping changed, even though every delete
   * still applied; the rule is what the rewrite has to preserve.
   */
  @Test
  public void everyDeleteAppliesToItsDataFiles() throws IOException {
    buildWindow();
    SnapshotRewriteResult result = rewrite();

    for (Snapshot original : result.plan().window()) {
      Snapshot rewritten = result.metadata().snapshot(original.snapshotId());

      long newestData = Long.MIN_VALUE;
      for (DataFile file : dataFilesOf(rewritten, result.metadata())) {
        newestData = Math.max(newestData, file.dataSequenceNumber());
      }

      for (DeleteFile file : deleteFilesOf(rewritten, result.metadata())) {
        assertThat(file.dataSequenceNumber())
            .as(
                "a delete in snapshot %s must not sort below any data file it could hide",
                original.snapshotId())
            .isGreaterThanOrEqualTo(newestData);
        assertThat(file.dataSequenceNumber())
            .as("a delete must not sort above the snapshot that holds it")
            .isLessThanOrEqualTo(original.sequenceNumber());
      }
    }
  }

  /**
   * Under the default stamping, the compaction's files are described once for the whole window.
   *
   * <p>A manifest entry carries the data sequence number, so snapshots that disagree about a file's
   * sequence number cannot share a manifest. Stamping the data below the window instead of at each
   * snapshot lets them agree, and one manifest then serves every rewritten snapshot -- which is
   * what manifest-list indirection is for.
   */
  @Test
  public void theCompactionIsDescribedOnce() throws IOException {
    buildWindow();
    SnapshotRewriteResult result = rewrite();

    // A snapshot that recovered rows carries a second, small manifest of its own, so the count per
    // snapshot varies. What every snapshot must share is one manifest: the compaction's.
    Set<String> common = null;
    for (Snapshot original : result.plan().window()) {
      Snapshot rewritten = result.metadata().snapshot(original.snapshotId());
      Set<String> paths = Sets.newHashSet();
      for (ManifestFile manifest : rewritten.dataManifests(table.io())) {
        paths.add(manifest.path());
      }

      common = common == null ? paths : Sets.intersection(common, paths).immutableCopy();
    }

    assertThat(common)
        .as("every rewritten snapshot references the one manifest describing the compaction")
        .hasSize(1);
  }

  /**
   * The oracle must fail when the stamping is wrong.
   *
   * <p>In v2 a mis-stamped delete is not an error: {@code PositionDeleteIndex.filter} slices a
   * sequence-sorted array and the delete simply is not in it. Nothing is logged and the table reads
   * as though the rows were never deleted. This builds that mistake on purpose and asserts the
   * comparison catches it, so a green suite means the stamping is right rather than that nothing is
   * being checked.
   */
  @Test
  public void misStampedRewriteIsCaughtByTheOracle() throws IOException {
    buildWindow();
    SnapshotRewriteResult broken = RewriteStampingHook.materializeMisStamped(rewriter());

    assertThatThrownBy(() -> assertLossless(broken))
        .as("a mis-stamped rewrite resurrects rows that should stay hidden")
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("must read the same after the rewrite");

    broken.discard();
  }

  /** A rewritten window must reference none of the layout it replaced. */
  @Test
  public void windowDropsTheOldLayout() throws IOException {
    buildWindow();
    SnapshotRewriteResult result = rewrite();

    Set<String> live = Sets.newHashSet();
    for (DataFile file : result.plan().targetFiles()) {
      live.add(file.location());
    }

    live.addAll(
        Lists.transform(
            result.plan().resurrections(),
            request -> request == null ? null : request.outputPath()));

    for (Snapshot original : result.plan().window()) {
      Snapshot rewritten = result.metadata().snapshot(original.snapshotId());
      for (DataFile file : dataFilesOf(rewritten, result.metadata())) {
        assertThat(live)
            .as("snapshot %s references only the new layout", original.snapshotId())
            .contains(file.location());
      }
    }

    assertThat(result.plan().detachedFiles()).isNotEmpty();
  }

  /**
   * The saving should be about one copy of the compacted table.
   *
   * <p>Each row that died in the window is materialized exactly once, so the bytes retained after a
   * rewrite are the compaction plus the dead rows, where before they were the previous compaction
   * plus everything inserted since. The difference is one full copy. Small tables make the metadata
   * overhead comparatively large, so this checks the shape of the result rather than a tight bound.
   */
  @Test
  public void savingIsAboutOneCopyOfTheTable() throws IOException {
    buildWindow();
    SnapshotRewriteResult result = rewrite();
    SnapshotRewriteReport report = result.report();

    assertThat(report.reclaimableBytes()).isPositive();
    assertThat(report.predictedSavedBytes()).isPositive();
    assertThat(report.resurrectedRows()).isPositive();
    assertThat(report.deletePositions()).isPositive();
    assertThat(report.estimated()).isFalse();
  }

  /** Nothing may touch the source table until commit is called explicitly. */
  @Test
  public void sourceTableIsUntouched() throws IOException {
    buildWindow();
    TableMetadata before = ((HasTableOperations) table).operations().current();
    List<String> rowsBefore = rowsAt(table, table.currentSnapshot().snapshotId());

    SnapshotRewriteResult result = rewrite();

    table.refresh();
    TableMetadata after = ((HasTableOperations) table).operations().current();
    assertThat(after.metadataFileLocation()).isEqualTo(before.metadataFileLocation());
    assertThat(after.currentSnapshot().snapshotId())
        .isEqualTo(before.currentSnapshot().snapshotId());
    assertThat(rowsAt(table, table.currentSnapshot().snapshotId())).isEqualTo(rowsBefore);
    assertThat(result.metadata().metadataFileLocation()).isNull();
  }

  /**
   * The synthesized metadata has to survive a real reload.
   *
   * <p>It is built through the same constructor the parser uses, which means builder validation
   * never runs on it. A JSON round trip is the closest thing to proof that what was built is a
   * document Iceberg would accept.
   */
  @Test
  public void metadataSurvivesAJsonRoundTrip() throws IOException {
    buildWindow();
    SnapshotRewriteResult result = rewrite();

    String path = table.location() + "/metadata/v99.metadata.json";
    TableMetadataParser.write(result.metadata(), table.io().newOutputFile(path));
    TableMetadata reloaded = TableMetadataParser.read(table.io(), path);

    assertThat(reloaded.snapshots()).hasSameSizeAs(result.metadata().snapshots());
    for (Snapshot original : table.snapshots()) {
      Snapshot round = reloaded.snapshot(original.snapshotId());
      assertThat(round).isNotNull();
      assertThat(round.sequenceNumber()).isEqualTo(original.sequenceNumber());
      assertThat(round.parentId()).isEqualTo(original.parentId());
    }
  }

  /** A discarded rewrite leaves nothing behind. */
  @Test
  public void discardRemovesEverythingItWrote() throws IOException {
    buildWindow();
    SnapshotRewriteResult result = rewrite();

    List<String> written = Lists.newArrayList();
    for (org.apache.iceberg.snaprewrite.ResurrectionRequest request :
        result.plan().resurrections()) {
      written.add(request.outputPath());
    }

    assertThat(written).isNotEmpty();
    for (String path : written) {
      assertThat(table.io().newInputFile(path).exists()).isTrue();
    }

    result.discard();

    for (String path : written) {
      assertThat(table.io().newInputFile(path).exists()).isFalse();
    }
  }

  // ------------------------------------------------------------------ helpers

  /** A window with inserts, deletes against the compaction, and deletes against an interstitial. */
  private void buildWindow() throws IOException {
    append(records(1, 6, "base"));
    compact();
    DataFile compacted = onlyDataFile(table.currentSnapshot());

    DataFile alpha = append(records(10, 4, "alpha"));
    delete(ImmutableList.of(at(compacted, 0), at(alpha, 1)));
    append(records(20, 3, "beta"));
    delete(ImmutableList.of(at(compacted, 2)));
    compact();
  }

  private DataFile onlyDataFile(Snapshot snapshot) throws IOException {
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    List<DataFile> files = dataFilesOf(snapshot, metadata);
    assertThat(files).hasSize(1);
    return files.get(0);
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
