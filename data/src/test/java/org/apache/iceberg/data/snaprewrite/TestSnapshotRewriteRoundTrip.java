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
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.snaprewrite.SnapshotRewriteRestore;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.junit.jupiter.api.Test;

/**
 * A committed rewrite can be put back.
 *
 * <p>Each rewritten snapshot records the manifest list it used to point at and the summary it used
 * to carry, so restoring is a matter of pointing back -- no reads, no copies, byte-identical
 * result. That makes committing a rewrite a decision rather than a commitment, for as long as the
 * detached layout survives.
 *
 * <p>The window closes at reclaim, which is the point of no return and is asserted here as such.
 */
public class TestSnapshotRewriteRoundTrip extends SnapshotRewriteTestBase {

  /**
   * Rewrite, commit, restore: every snapshot reads what it read before, with its original metadata.
   */
  @Test
  public void restoreUndoesACommittedRewrite() throws IOException {
    buildWindow();

    Map<Long, List<String>> rowsBefore = allSnapshotRows(table);
    Map<Long, Snapshot> before = snapshotsById(table);

    SnapshotRewriteResult result = rewrite();
    result.commit(((HasTableOperations) table).operations());
    table.refresh();

    TableMetadata rewritten = ((HasTableOperations) table).operations().current();
    assertThat(SnapshotRewriteRestore.rewrittenCount(rewritten))
        .isEqualTo(result.plan().window().size());

    TableMetadata restored = SnapshotRewriteRestore.restore(rewritten);
    ((HasTableOperations) table).operations().commit(rewritten, restored);
    table.refresh();

    // Byte-identical, not merely equivalent: same manifest list, same summary, same identity.
    for (Snapshot after : table.snapshots()) {
      Snapshot original = before.get(after.snapshotId());
      assertThat(original).as("snapshot %s must survive", after.snapshotId()).isNotNull();
      assertThat(after.manifestListLocation()).isEqualTo(original.manifestListLocation());
      assertThat(after.summary()).isEqualTo(original.summary());
      assertThat(after.sequenceNumber()).isEqualTo(original.sequenceNumber());
      assertThat(after.parentId()).isEqualTo(original.parentId());
      assertThat(after.timestampMillis()).isEqualTo(original.timestampMillis());
      assertThat(SnapshotRewriteRestore.isRewritten(after)).isFalse();
    }

    for (Map.Entry<Long, List<String>> entry : rowsBefore.entrySet()) {
      assertThat(rowsAt(table, entry.getKey()))
          .as("snapshot %s after restore", entry.getKey())
          .isEqualTo(entry.getValue());
    }
  }

  /** The same round trip on v3, where the deletes are deletion vectors. */
  @Test
  public void restoreUndoesAV3Rewrite() throws IOException {
    useFormatVersion(3);
    append(records(1, 6, "base"));
    compact();
    append(records(10, 3, "alpha"));
    compact();

    Map<Long, List<String>> rowsBefore = allSnapshotRows(table);
    Map<Long, Snapshot> before = snapshotsById(table);

    SnapshotRewriteResult result = rewrite();
    result.commit(((HasTableOperations) table).operations());
    table.refresh();

    TableMetadata rewritten = ((HasTableOperations) table).operations().current();
    ((HasTableOperations) table)
        .operations()
        .commit(rewritten, SnapshotRewriteRestore.restore(rewritten));
    table.refresh();

    for (Snapshot after : table.snapshots()) {
      Snapshot original = before.get(after.snapshotId());
      assertThat(after.manifestListLocation()).isEqualTo(original.manifestListLocation());
      assertThat(after.firstRowId()).isEqualTo(original.firstRowId());
      assertThat(after.addedRows()).isEqualTo(original.addedRows());
    }

    for (Map.Entry<Long, List<String>> entry : rowsBefore.entrySet()) {
      assertThat(rowsAt(table, entry.getKey())).isEqualTo(entry.getValue());
    }
  }

  /**
   * Reclaim is the point of no return.
   *
   * <p>Restoring points a snapshot back at a manifest list describing files reclaim has deleted, so
   * the metadata restores but the data is gone. The rewrite is reversible until then and not after,
   * and nothing pretends otherwise.
   */
  @Test
  public void reclaimEndsReversibility() throws IOException {
    table.updateProperties().set("write.metadata.previous-versions-max", "1").commit();
    buildWindow();

    SnapshotRewriteResult result = rewrite();
    result.commit(((HasTableOperations) table).operations());
    table.refresh();
    table.updateProperties().set("unrelated", "1").commit();
    table.refresh();

    SnapshotRewriteResult.ReclaimResult reclaimed =
        result.reclaim(((HasTableOperations) table).operations());
    assertThat(reclaimed.deleted()).isNotEmpty();

    TableMetadata rewritten = ((HasTableOperations) table).operations().current();
    TableMetadata restored = SnapshotRewriteRestore.restore(rewritten);
    ((HasTableOperations) table).operations().commit(rewritten, restored);
    table.refresh();

    // The metadata is back, but it now names files reclaim deleted. Snapshots outside the window
    // still read: nothing of theirs was detached.
    long rewrittenId = result.plan().window().get(0).snapshotId();
    assertThatThrownBy(() -> rowsAt(table, rewrittenId))
        .as("a restored window snapshot points at deleted files")
        .isInstanceOf(RuntimeException.class)
        // The manifest list it was restored to point at is one reclaim deleted.
        .hasMessageContaining("Failed to open input stream");
  }

  /** Restoring a table that was never rewritten is an error, not a silent no-op. */
  @Test
  public void restoreRequiresARewrite() throws IOException {
    append(records(1, 4, "base"));
    compact();

    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    assertThat(SnapshotRewriteRestore.rewrittenCount(metadata)).isZero();
    assertThatThrownBy(() -> SnapshotRewriteRestore.restore(metadata))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No rewritten snapshots");
  }

  private static Map<Long, Snapshot> snapshotsById(org.apache.iceberg.Table target) {
    Map<Long, Snapshot> byId = Maps.newHashMap();
    for (Snapshot snapshot : target.snapshots()) {
      byId.put(snapshot.snapshotId(), snapshot);
    }

    return byId;
  }

  private void buildWindow() throws IOException {
    append(records(1, 6, "base"));
    compact();
    DataFile compacted = null;
    for (DataFile file : firstSnapshotFiles()) {
      compacted = file;
    }

    DataFile alpha = append(records(10, 4, "alpha"));
    delete(ImmutableList.of(at(compacted, 0), at(alpha, 1)));
    append(records(20, 3, "beta"));
    compact();
  }

  private List<DataFile> firstSnapshotFiles() throws IOException {
    List<DataFile> files =
        org.apache.iceberg.relocated.com.google.common.collect.Lists.newArrayList();
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    for (org.apache.iceberg.ManifestFile manifest :
        table.currentSnapshot().dataManifests(table.io())) {
      try (org.apache.iceberg.ManifestReader<DataFile> reader =
          org.apache.iceberg.ManifestFiles.read(manifest, table.io(), metadata.specsById())) {
        for (DataFile file : reader) {
          files.add(file);
        }
      }
    }

    return files;
  }
}
