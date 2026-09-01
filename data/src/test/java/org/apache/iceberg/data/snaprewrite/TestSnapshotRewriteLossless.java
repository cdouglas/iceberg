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
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.snaprewrite.ResurrectionRequest;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.junit.jupiter.api.Test;

/** Every rewritten snapshot must read exactly what it read before. */
public class TestSnapshotRewriteLossless extends SnapshotRewriteTestBase {

  /**
   * The worked example from the design: a compaction, two delta commits, another compaction.
   *
   * <p>Inverting the second transaction turns its inserts into deletes against the new compaction
   * and recovers the two rows it deleted. Inverting the first then has to find one of its own
   * inserted rows -- the one the second transaction killed -- inside that recovered file rather than
   * in the compaction. That cross-reference is the part of the design that does not follow from the
   * compaction map alone.
   */
  @Test
  public void figureExample() throws IOException {
    DataFile initial = append(ImmutableList.of(record(1, "a"), record(2, "b"), record(3, "c")));
    compact();
    DataFile compacted = onlyDataFile(table.currentSnapshot());

    // T-alpha: insert {d, e}, delete c
    DataFile alpha =
        appendAndDelete(
            ImmutableList.of(record(4, "d"), record(5, "e")),
            ImmutableList.of(at(compacted, 2)));

    // T-beta: insert {f, g}, delete b (from the compaction) and d (from T-alpha)
    appendAndDelete(
        ImmutableList.of(record(6, "f"), record(7, "g")),
        ImmutableList.of(at(compacted, 1), at(alpha, 0)));

    assertThat(rowsAt(table, table.currentSnapshot().snapshotId()))
        .containsExactlyInAnyOrder(
            "id=1 data=a ", "id=5 data=e ", "id=6 data=f ", "id=7 data=g ");

    compact();

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);
    assertIdentityPreserved(result);

    // b and d recovered by inverting T-beta, c by inverting T-alpha: three rows, never more.
    List<ResurrectionRequest> resurrections = result.plan().resurrections();
    assertThat(resurrections).hasSize(2);
    assertThat(result.plan().resurrectedRows()).isEqualTo(3);
    assertThat(resurrections.get(0).rowCount()).isEqualTo(2);
    assertThat(resurrections.get(1).rowCount()).isEqualTo(1);

    assertNoDependencyOnOldLayout(result, initial, compacted, alpha);
  }

  /**
   * An insert-only window whose rows all survive costs nothing to invert.
   *
   * <p>The compaction map alone places every inserted row, so the whole snapshot compresses to a
   * delete vector. No data is read and no data file is written -- the case the design leads with.
   */
  @Test
  public void insertOnlyWindowWritesNoDataFiles() throws IOException {
    append(records(1, 3, "base"));
    compact();
    append(records(10, 2, "alpha"));
    append(records(20, 2, "beta"));
    compact();

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);

    assertThat(result.plan().resurrections()).isEmpty();
    assertThat(result.plan().resurrectedRows()).isZero();
    assertThat(result.report().resurrectedRows()).isZero();
  }

  /** An insert whose rows are killed later must be recovered by the transaction that killed them. */
  @Test
  public void insertsDeletedBeforeTheCompaction() throws IOException {
    append(records(1, 3, "base"));
    compact();

    DataFile alpha = append(records(10, 3, "alpha"));
    delete(ImmutableList.of(at(alpha, 0), at(alpha, 2)));
    compact();

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);
    assertThat(result.plan().resurrectedRows()).isEqualTo(2);
  }

  /** Deleting rows of the previous compaction's own output. */
  @Test
  public void deletesAgainstThePreviousCompaction() throws IOException {
    append(records(1, 5, "base"));
    compact();
    DataFile compacted = onlyDataFile(table.currentSnapshot());

    delete(ImmutableList.of(at(compacted, 1), at(compacted, 3)));
    compact();

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);
    assertThat(result.plan().resurrectedRows()).isEqualTo(2);
  }

  /** Inserting and deleting in one commit: rows dead on arrival are not "inserted" by the induction. */
  @Test
  public void insertAndDeleteInOneCommit() throws IOException {
    append(records(1, 3, "base"));
    compact();

    DataFile alpha = writeData(records(10, 4, "alpha"));
    table
        .newRowDelta()
        .addRows(alpha)
        .addDeletes(writeDeletes(ImmutableList.of(at(alpha, 1))))
        .commit();
    compact();

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);
  }

  /** Removing a whole data file: the rows to recover are the ones still live, not all of them. */
  @Test
  public void wholeFileRemoval() throws IOException {
    append(records(1, 3, "base"));
    compact();

    DataFile alpha = append(records(10, 4, "alpha"));
    delete(ImmutableList.of(at(alpha, 0)));
    removeFile(alpha);
    compact();

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);

    // Every row of the removed file dies inside the window, but by two different transactions and so
    // through two different inverses: the delete kills one, the removal kills the other three. Each
    // is materialized exactly once, which is what bounds the cost of a rewrite.
    assertThat(result.plan().resurrectedRows()).isEqualTo(4);
    assertThat(result.plan().resurrections()).hasSize(2);
  }

  /**
   * Re-deleting an already-dead position must not resurrect it.
   *
   * <p>Delete files are idempotent and overlapping delete sets are legal, so the rows a transaction
   * removed have to be computed as a difference of live sets rather than read off the delete files it
   * added. Taking the commit delta here would recover a row that was not live in the preceding state
   * and insert it into a state it never belonged to.
   */
  @Test
  public void reDeleteOfAnAlreadyDeadPosition() throws IOException {
    append(records(1, 5, "base"));
    compact();
    DataFile compacted = onlyDataFile(table.currentSnapshot());

    delete(ImmutableList.of(at(compacted, 1)));
    delete(ImmutableList.of(at(compacted, 1), at(compacted, 2)));
    compact();

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);

    // Position 1 died once. The second commit removed only position 2.
    assertThat(result.plan().resurrectedRows()).isEqualTo(2);
  }

  /** A transaction all of whose inserts die before the compaction. */
  @Test
  public void everyInsertedRowDies() throws IOException {
    append(records(1, 3, "base"));
    compact();

    DataFile alpha = append(records(10, 2, "alpha"));
    delete(ImmutableList.of(at(alpha, 0), at(alpha, 1)));
    compact();

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);
    assertThat(result.plan().resurrectedRows()).isEqualTo(2);
  }

  /** A file emptied by deletes disappears from the compaction entirely. */
  @Test
  public void fileEmptiedByDeletes() throws IOException {
    append(records(1, 3, "base"));
    compact();
    DataFile compacted = onlyDataFile(table.currentSnapshot());

    append(records(10, 2, "alpha"));
    delete(ImmutableList.of(at(compacted, 0), at(compacted, 1), at(compacted, 2)));
    compact();

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);
    assertThat(result.plan().resurrectedRows()).isEqualTo(3);
  }

  /** An empty commit in the middle of the window. */
  @Test
  public void noOpCommitInWindow() throws IOException {
    append(records(1, 3, "base"));
    compact();

    append(records(10, 2, "alpha"));
    table.newAppend().commit();
    append(records(20, 2, "beta"));
    compact();

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);
  }

  /** Duplicate rows are legal and are preserved by position, so the oracle compares multisets. */
  @Test
  public void duplicateRows() throws IOException {
    append(ImmutableList.of(record(1, "dup"), record(1, "dup"), record(2, "x")));
    compact();

    DataFile alpha = append(ImmutableList.of(record(1, "dup"), record(1, "dup")));
    delete(ImmutableList.of(at(alpha, 0)));
    compact();

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);
  }

  /** Interleaved commits from several writers. */
  @Test
  public void interleavedWriters() throws IOException {
    append(records(1, 4, "base"));
    compact();
    DataFile compacted = onlyDataFile(table.currentSnapshot());

    DataFile first = append(records(10, 3, "w1"));
    DataFile second = append(records(20, 3, "w2"));
    delete(ImmutableList.of(at(first, 1), at(compacted, 0)));
    append(records(30, 2, "w1"));
    delete(ImmutableList.of(at(second, 2)));
    compact();

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);
    assertThat(result.plan().resurrectedRows()).isEqualTo(3);
  }

  // ------------------------------------------------------------------ helpers

  private DataFile onlyDataFile(Snapshot snapshot) throws IOException {
    List<DataFile> files = dataFiles(snapshot);
    assertThat(files).hasSize(1);
    return files.get(0);
  }

  private List<DataFile> dataFiles(Snapshot snapshot) throws IOException {
    List<DataFile> files = Lists.newArrayList();
    for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, table.io(), ((org.apache.iceberg.HasTableOperations) table)
              .operations()
              .current()
              .specsById())) {
        for (DataFile file : reader) {
          files.add(file);
        }
      }
    }

    return files;
  }

  /**
   * Asserts the rewritten window references none of the layout it replaced.
   *
   * <p>Reclaiming the old layout is the point of the rewrite, so a rewrite that still pins any of it
   * has not done its job even if it reads correctly.
   */
  private void assertNoDependencyOnOldLayout(SnapshotRewriteResult result, DataFile... obsolete)
      throws IOException {
    Set<String> forbidden = Sets.newHashSet();
    for (DataFile file : obsolete) {
      forbidden.add(file.location());
    }

    for (Snapshot original : result.plan().window()) {
      Snapshot rewritten = result.metadata().snapshot(original.snapshotId());
      for (ManifestFile manifest : rewritten.dataManifests(table.io())) {
        try (ManifestReader<DataFile> reader =
            ManifestFiles.read(manifest, table.io(), result.metadata().specsById())) {
          for (DataFile file : reader) {
            assertThat(forbidden)
                .as("snapshot %s must not reference the old layout", original.snapshotId())
                .doesNotContain(file.location());
          }
        }
      }
    }
  }

}
