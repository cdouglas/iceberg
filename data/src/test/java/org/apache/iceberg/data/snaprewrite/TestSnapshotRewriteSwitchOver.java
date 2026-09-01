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
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.data.GenericSnapshotRewriteIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.snaprewrite.SnapshotRewrite;
import org.apache.iceberg.snaprewrite.SnapshotRewriteRestore;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.junit.jupiter.api.Test;

/**
 * Running a rewrite as a recurring pass, over history a previous pass already rewrote.
 *
 * <p>This is the deployment the design points at: a compaction happens, and some time later a
 * separate pass switches the snapshots it superseded over to the newer layout. Keeping the two apart
 * is what lets the age threshold hold -- a fused pass would have to rewrite immediately, which is
 * exactly what P7 exists to prevent -- and it is the only model that works on history that already
 * exists, since a table adopting this feature is full of compactions that predate it.
 *
 * <p>The pass therefore has to be able to run over its own output. A snapshot rewritten onto one
 * compaction still pins that compaction's files, so when a newer compaction supersedes them the same
 * snapshot has to move again.
 */
public class TestSnapshotRewriteSwitchOver extends SnapshotRewriteTestBase {

  /**
   * A second pass re-expresses snapshots a first pass already moved.
   *
   * <p>Nothing about a rewritten snapshot makes it special to the planner: it references data files
   * and delete files like any other, and the newer compaction's map covers those files. So the second
   * pass treats it as ordinary input, and every snapshot still reads what it always did.
   */
  @Test
  public void aSecondPassRewritesWhatTheFirstPassWrote() throws IOException {
    append(records(1, 6, "base"));
    Snapshot firstCompaction = compact();
    DataFile firstOutput = onlyDataFile(firstCompaction);

    DataFile alpha = append(records(10, 4, "alpha"));
    delete(ImmutableList.of(at(firstOutput, 0), at(alpha, 1)));
    append(records(20, 3, "beta"));
    compact();

    Map<Long, List<String>> original = allSnapshotRows(table);

    // Pass one.
    SnapshotRewriteResult first = rewrite();
    first.commit(((HasTableOperations) table).operations());
    table.refresh();
    assertUnchanged(original);

    long oldestRewritten = first.plan().window().get(0).snapshotId();

    // More work lands, and another compaction supersedes the layout pass one moved everything onto.
    DataFile gamma = append(records(30, 3, "gamma"));
    delete(ImmutableList.of(at(gamma, 0)));
    compact();

    // Pass two, reaching back over pass one's output so those snapshots move too. Without the floor
    // the window would stop at the previous compaction and leave them pinning it.
    SnapshotRewriteResult second =
        SnapshotRewrite.forTable(table, new GenericSnapshotRewriteIO(table))
            .onLatestCompaction()
            .floor(oldestRewritten)
            .maxDeadRatio(Double.MAX_VALUE)
            .materialize();

    assertThat(second.plan().window())
        .as("the second window must include what the first pass rewrote")
        .anySatisfy(snapshot -> assertThat(snapshot.snapshotId()).isEqualTo(oldestRewritten));

    second.commit(((HasTableOperations) table).operations());
    table.refresh();
    assertUnchanged(original);

    // And the layout pass one had moved everything onto is now itself detachable.
    assertThat(second.plan().detachedFiles()).isNotEmpty();
  }

  /**
   * A pass that reaches back leaves nothing pinning the older compaction.
   *
   * <p>Reclamation is the point, so what matters is that after the second pass no snapshot still
   * references the layout the first pass had moved everything onto.
   */
  @Test
  public void reachingBackReleasesTheOlderCompaction() throws IOException {
    append(records(1, 5, "base"));
    compact();

    append(records(10, 3, "alpha"));
    Snapshot middleCompaction = compact();
    List<String> middleOutput = Lists.newArrayList();
    for (DataFile file : dataFiles(middleCompaction)) {
      middleOutput.add(file.location());
    }

    Map<Long, List<String>> original = allSnapshotRows(table);

    SnapshotRewriteResult first = rewrite();
    first.commit(((HasTableOperations) table).operations());
    table.refresh();
    long oldestRewritten = first.plan().window().get(0).snapshotId();

    append(records(20, 3, "beta"));
    compact();

    SnapshotRewriteResult second =
        SnapshotRewrite.forTable(table, new GenericSnapshotRewriteIO(table))
            .onLatestCompaction()
            .floor(oldestRewritten)
            .maxDeadRatio(Double.MAX_VALUE)
            .materialize();
    second.commit(((HasTableOperations) table).operations());
    table.refresh();

    assertUnchanged(original);

    // Every snapshot's referenced data files, checked against the layout that should now be free.
    for (Snapshot snapshot : table.snapshots()) {
      if (!SnapshotRewriteRestore.isRewritten(snapshot)) {
        continue;
      }

      for (DataFile file : dataFiles(snapshot)) {
        assertThat(middleOutput)
            .as("rewritten snapshot %s must not pin the superseded layout", snapshot.snapshotId())
            .doesNotContain(file.location());
      }
    }
  }

  /**
   * Repeating a pass costs the whole retained history, not just the new window.
   *
   * <p>Every rewritten snapshot deletes every row inserted after it, so a pass that reaches back over
   * earlier passes pays for that reach again. This measures it rather than asserting it is small: the
   * second pass writes strictly more delete positions than the first, over the same table, because it
   * covers more snapshots against a newer compaction. It is the operational cost of continuing to
   * reclaim, and the reason the report exists.
   */
  @Test
  public void repeatedPassesPayForTheirReach() throws IOException {
    append(records(1, 8, "base"));
    compact();
    append(records(10, 3, "alpha"));
    append(records(20, 3, "beta"));
    compact();

    SnapshotRewriteResult first = rewrite();
    long firstPositions = first.report().deletePositions();
    first.commit(((HasTableOperations) table).operations());
    table.refresh();
    long oldestRewritten = first.plan().window().get(0).snapshotId();

    append(records(30, 3, "gamma"));
    compact();

    SnapshotRewriteResult second =
        SnapshotRewrite.forTable(table, new GenericSnapshotRewriteIO(table))
            .onLatestCompaction()
            .floor(oldestRewritten)
            .maxDeadRatio(Double.MAX_VALUE)
            .materialize();

    assertThat(second.report().deletePositions())
        .as("reaching back over an earlier pass re-pays for those snapshots")
        .isGreaterThan(firstPositions);
    assertThat(second.plan().window().size())
        .isGreaterThan(first.plan().window().size());
  }

  // ------------------------------------------------------------------ helpers

  private void assertUnchanged(Map<Long, List<String>> original) {
    for (Map.Entry<Long, List<String>> entry : original.entrySet()) {
      assertThat(rowsAt(table, entry.getKey()))
          .as("snapshot %s must read the same", entry.getKey())
          .isEqualTo(entry.getValue());
    }
  }

  private DataFile onlyDataFile(Snapshot snapshot) throws IOException {
    List<DataFile> files = dataFiles(snapshot);
    assertThat(files).hasSize(1);
    return files.get(0);
  }

  private List<DataFile> dataFiles(Snapshot snapshot) throws IOException {
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
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
}
