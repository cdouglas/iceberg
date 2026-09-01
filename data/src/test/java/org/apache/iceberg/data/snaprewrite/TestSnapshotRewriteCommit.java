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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.junit.jupiter.api.Test;

/**
 * Committing a rewrite, and reclaiming what it detaches.
 *
 * <p>This is the only destructive step in the design, and it is the one Iceberg has no API for: the
 * whole metadata document is swapped, which no REST catalog can express. Everything up to here runs
 * against a shadow table and leaves the source alone.
 */
public class TestSnapshotRewriteCommit extends SnapshotRewriteTestBase {

  /** After committing, every snapshot still reads what it read before. */
  @Test
  public void committedRewriteReadsTheSame() throws IOException {
    buildWindow();

    Map<Long, List<String>> before = allSnapshotRows(table);
    long current = table.currentSnapshot().snapshotId();

    SnapshotRewriteResult result = rewrite();
    result.commit(((HasTableOperations) table).operations());
    table.refresh();

    assertThat(table.currentSnapshot().snapshotId())
        .as("the compaction is still the head")
        .isEqualTo(current);

    for (Map.Entry<Long, List<String>> entry : before.entrySet()) {
      assertThat(rowsAt(table, entry.getKey()))
          .as("snapshot %s after commit", entry.getKey())
          .isEqualTo(entry.getValue());
    }
  }

  /** Identity survives the commit: same ids, parents, sequence numbers, timestamps. */
  @Test
  public void committedSnapshotsKeepTheirIdentity() throws IOException {
    buildWindow();

    Map<Long, Snapshot> before =
        org.apache.iceberg.relocated.com.google.common.collect.Maps.newHashMap();
    for (Snapshot snapshot : table.snapshots()) {
      before.put(snapshot.snapshotId(), snapshot);
    }

    SnapshotRewriteResult result = rewrite();
    result.commit(((HasTableOperations) table).operations());
    table.refresh();

    for (Snapshot after : table.snapshots()) {
      Snapshot original = before.get(after.snapshotId());
      assertThat(original).as("snapshot %s must survive", after.snapshotId()).isNotNull();
      assertThat(after.sequenceNumber()).isEqualTo(original.sequenceNumber());
      assertThat(after.parentId()).isEqualTo(original.parentId());
      assertThat(after.timestampMillis()).isEqualTo(original.timestampMillis());
    }

    assertThat(table.snapshots()).hasSameSizeAs(before.values());
  }

  /**
   * Reclaim withholds everything an older metadata document still references.
   *
   * <p>Right after a commit the metadata log still points at the document describing the old
   * layout, so nothing is safe to delete yet. Expiring snapshots would not help: it deletes only
   * what the expired snapshots reach, and after a rewrite the old files are reached by no snapshot
   * at all. Reporting what was withheld is more useful than deleting optimistically.
   */
  @Test
  public void reclaimWithholdsWhatOldMetadataStillReferences() throws IOException {
    buildWindow();

    SnapshotRewriteResult result = rewrite();
    result.commit(((HasTableOperations) table).operations());
    table.refresh();

    SnapshotRewriteResult.ReclaimResult reclaimed =
        result.reclaim(((HasTableOperations) table).operations());

    assertThat(reclaimed.withheld())
        .as("the previous metadata document still describes the old layout")
        .isNotEmpty();

    for (String path : reclaimed.withheld()) {
      assertThat(table.io().newInputFile(path).exists())
          .as("%s must not be deleted while something still points at it", path)
          .isTrue();
    }
  }

  /**
   * Once the metadata log no longer reaches the old layout, reclaim deletes it.
   *
   * <p>This is the delay the design accepts rather than trimming history out from under the table.
   */
  @Test
  public void reclaimDeletesOnceTheMetadataLogMovesOn() throws IOException {
    table.updateProperties().set("write.metadata.previous-versions-max", "1").commit();
    buildWindow();

    SnapshotRewriteResult result = rewrite();
    result.commit(((HasTableOperations) table).operations());
    table.refresh();

    // One more commit pushes the pre-rewrite metadata document out of the retained log.
    table.updateProperties().set("unrelated", "1").commit();
    table.refresh();

    Map<Long, List<String>> before = allSnapshotRows(table);
    SnapshotRewriteResult.ReclaimResult reclaimed =
        result.reclaim(((HasTableOperations) table).operations());

    assertThat(reclaimed.deleted()).as("the old layout is now unreachable").isNotEmpty();
    for (String path : reclaimed.deleted()) {
      assertThat(table.io().newInputFile(path).exists()).isFalse();
    }

    // Reclaiming must not disturb what any snapshot returns.
    for (Map.Entry<Long, List<String>> entry : before.entrySet()) {
      assertThat(rowsAt(table, entry.getKey()))
          .as("snapshot %s after reclaim", entry.getKey())
          .isEqualTo(entry.getValue());
    }
  }

  /** Expiring snapshots after a rewrite must not break the ones that remain. */
  @Test
  public void expireSnapshotsAfterRewrite() throws IOException {
    buildWindow();

    SnapshotRewriteResult result = rewrite();
    result.commit(((HasTableOperations) table).operations());
    table.refresh();

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Snapshot oldest = snapshots.get(0);
    Snapshot second = snapshots.get(1);

    table.expireSnapshots().expireSnapshotId(oldest.snapshotId()).cleanExpiredFiles(true).commit();
    table.refresh();

    assertThat(table.snapshot(oldest.snapshotId())).isNull();

    // Everything still present must still read, including the snapshot that followed the expired
    // one.
    for (Snapshot snapshot : table.snapshots()) {
      assertThat(rowsAt(table, snapshot.snapshotId()))
          .as("snapshot %s survives expiry", snapshot.snapshotId())
          .isNotNull();
    }

    assertThat(table.snapshot(second.snapshotId())).isNotNull();
  }

  private void buildWindow() throws IOException {
    append(records(1, 6, "base"));
    compact();
    DataFile compacted = onlyCompactedFile();

    DataFile alpha = append(records(10, 4, "alpha"));
    delete(ImmutableList.of(at(compacted, 0), at(alpha, 1)));
    append(records(20, 3, "beta"));
    compact();
  }

  private DataFile onlyCompactedFile() throws IOException {
    List<DataFile> files =
        org.apache.iceberg.relocated.com.google.common.collect.Lists.newArrayList();
    for (org.apache.iceberg.ManifestFile manifest :
        table.currentSnapshot().dataManifests(table.io())) {
      try (org.apache.iceberg.ManifestReader<DataFile> reader =
          org.apache.iceberg.ManifestFiles.read(
              manifest,
              table.io(),
              ((HasTableOperations) table).operations().current().specsById())) {
        for (DataFile file : reader) {
          files.add(file);
        }
      }
    }

    assertThat(files).hasSize(1);
    return files.get(0);
  }
}
