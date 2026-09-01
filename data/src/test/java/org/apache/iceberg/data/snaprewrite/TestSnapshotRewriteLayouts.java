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
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.data.GenericSnapshotRewriteIO;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.snaprewrite.SnapshotRewrite;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.junit.jupiter.api.Test;

/** Layouts beyond the simplest case: partitioned tables, partial compactions, chained maps. */
public class TestSnapshotRewriteLayouts extends SnapshotRewriteTestBase {

  /**
   * A partitioned table: resurrection files have to land in the partition their rows belong to.
   *
   * <p>Rows cannot be concatenated across partitions, so each inverse writes one file per partition
   * it recovers rows from, and each rewritten snapshot carries one delete file per partition.
   */
  @Test
  public void partitionedTable() throws IOException {
    usePartitionSpec(PartitionSpec.builderFor(SCHEMA).identity("data").build());

    DataFile first = append(partition("east"), rows(1, 4, "east"));
    DataFile second = append(partition("west"), rows(10, 4, "west"));
    compact();

    List<DataFile> compacted = dataFiles(table.currentSnapshot());
    assertThat(compacted).as("one target file per partition").hasSize(2);

    append(partition("east"), rows(20, 2, "east"));
    delete(partition("east"), ImmutableList.of(at(compacted.get(0), 0)));
    delete(partition("west"), ImmutableList.of(at(compacted.get(1), 1)));
    compact();

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);
    assertIdentityPreserved(result);
    assertThat(result.plan().resurrectedRows()).isEqualTo(2);

    // Each recovered row keeps its partition; a resurrection file spanning partitions would be
    // unreadable under the spec.
    for (org.apache.iceberg.snaprewrite.ResurrectionRequest request :
        result.plan().resurrections()) {
      assertThat(request.partition()).isNotNull();
    }

    assertThat(first.location()).isNotEqualTo(second.location());
  }

  /**
   * A partial compaction leaves files where they are.
   *
   * <p>The compaction map says nothing about an untouched file because nothing moved, so the
   * locator has to treat those rows as already in place rather than as missing.
   */
  @Test
  public void partialCompaction() throws IOException {
    append(records(1, 4, "base"));
    compact();
    DataFile compacted = onlyDataFile(table.currentSnapshot());

    DataFile untouched = append(records(10, 3, "keep"));
    DataFile alpha = append(records(20, 3, "alpha"));
    delete(ImmutableList.of(at(alpha, 0)));
    delete(ImmutableList.of(at(compacted, 1)));

    // Compact everything except one file, which stays in the layout unchanged.
    LocalCompactor.compact(table, true, file -> !file.location().equals(untouched.location()));

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);

    // The untouched file is still referenced, so it is not reclaimable.
    assertThat(result.plan().detachedFiles()).doesNotContainKey(untouched.location());
  }

  /**
   * Two compactions inside one window: the maps have to compose.
   *
   * <p>Reachable only with an explicit floor, since a window otherwise stops at the first
   * compaction it meets going back. A row inserted before the intermediate compaction was relocated
   * twice, and placing it means following both maps.
   */
  @Test
  public void chainedCompactionsInsideTheWindow() throws IOException {
    append(records(1, 4, "base"));
    Snapshot floor = compact();

    DataFile alpha = append(records(10, 4, "alpha"));
    delete(ImmutableList.of(at(alpha, 0)));
    compact();

    DataFile beta = append(records(20, 3, "beta"));
    delete(ImmutableList.of(at(beta, 1)));
    compact();

    SnapshotRewriteResult result =
        SnapshotRewrite.forTable(table, new GenericSnapshotRewriteIO(table))
            .onLatestCompaction()
            .floor(floor.snapshotId())
            .maxDeadRatio(Double.MAX_VALUE)
            .materialize();

    assertLossless(result);
    assertIdentityPreserved(result);

    // The window reaches back past the intermediate compaction, so its output is reclaimable too.
    assertThat(result.plan().window()).hasSizeGreaterThan(3);
    assertThat(result.plan().resurrectedRows()).isEqualTo(2);
  }

  /** A window reaching back through an older compaction reclaims that compaction's output. */
  @Test
  public void recursiveWindowReclaimsTheOlderCompaction() throws IOException {
    append(records(1, 4, "base"));
    Snapshot floor = compact();
    DataFile firstCompaction = onlyDataFile(table.currentSnapshot());

    append(records(10, 2, "alpha"));
    compact();
    DataFile secondCompaction = onlyDataFile(table.currentSnapshot());

    append(records(20, 2, "beta"));
    compact();

    SnapshotRewriteResult result =
        SnapshotRewrite.forTable(table, new GenericSnapshotRewriteIO(table))
            .onLatestCompaction()
            .floor(floor.snapshotId())
            .maxDeadRatio(Double.MAX_VALUE)
            .materialize();

    assertLossless(result);
    assertThat(result.plan().detachedFiles()).containsKey(secondCompaction.location());
    assertThat(firstCompaction.location()).isNotEqualTo(secondCompaction.location());
  }

  /**
   * A replace inside the window with no map is copied through rather than refused.
   *
   * <p>Nothing can follow rows across a layout change that left no record of it, but diffing the
   * snapshot by file and position is still correct: every row it moved is recovered into a
   * resurrection file and every row it wrote becomes a delete. The result reconstructs the
   * preceding state from copies instead of from the map. It costs a full copy of the live table at
   * that boundary, which is what the dead-ratio guard exists to price -- so this is a cost
   * question, not a correctness one, and refusing outright would have been the wrong answer.
   */
  @Test
  public void mapLessReplaceInsideTheWindowFallsBackToCopying() throws IOException {
    append(records(1, 6, "base"));
    Snapshot floor = compact();

    append(records(10, 3, "alpha"));
    LocalCompactor.compact(table, false);
    append(records(20, 2, "beta"));
    compact();

    SnapshotRewriteResult result =
        SnapshotRewrite.forTable(table, new GenericSnapshotRewriteIO(table))
            .onLatestCompaction()
            .floor(floor.snapshotId())
            .maxDeadRatio(Double.MAX_VALUE)
            .materialize();

    assertLossless(result);
    assertIdentityPreserved(result);

    // Nine rows were live when the map-less replace ran, and all nine had to be copied because no
    // map describes where it put them.
    assertThat(result.plan().resurrectedRows()).isEqualTo(9);
  }

  /**
   * A compaction that rolls its output mid-source-file.
   *
   * <p>Real compactions roll at a target size, so one source file's rows routinely land in two
   * output files and the map records a per-run {@code targetFile}. A rewrite that read only the
   * mapping's default target would send a remapped position into the wrong file -- same arithmetic,
   * different file, and the row it hides is not the row it meant to hide. The existing
   * compaction-map suite pins that shape at the map level; this pins it end to end through a
   * rewrite.
   */
  @Test
  public void multiTargetCompaction() throws IOException {
    append(records(1, 9, "base"));

    // Roll every four rows, so the nine base rows span three target files.
    LocalCompactor.compact(table, true, file -> true, 4);
    List<DataFile> rolled = dataFiles(table.currentSnapshot());
    assertThat(rolled).as("the compaction must actually roll").hasSizeGreaterThan(1);
    assertMultiTargetMap(table.currentSnapshot());

    DataFile alpha = append(records(20, 5, "alpha"));
    delete(ImmutableList.of(at(rolled.get(0), 1), at(rolled.get(1), 0), at(alpha, 2)));
    append(records(30, 3, "beta"));

    // Roll again, so the window's rows are relocated across a multi-target boundary twice.
    LocalCompactor.compact(table, true, file -> true, 5);
    assertThat(dataFiles(table.currentSnapshot())).hasSizeGreaterThan(1);
    assertMultiTargetMap(table.currentSnapshot());

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);
    assertIdentityPreserved(result);
    assertThat(result.plan().resurrectedRows()).isEqualTo(3);
  }

  /** Rolling and partitioning together: each partition rolls independently. */
  @Test
  public void multiTargetCompactionOnAPartitionedTable() throws IOException {
    usePartitionSpec(PartitionSpec.builderFor(SCHEMA).identity("data").build());

    append(partition("east"), rows(1, 6, "east"));
    append(partition("west"), rows(10, 6, "west"));
    LocalCompactor.compact(table, true, file -> true, 4);

    List<DataFile> compacted = dataFiles(table.currentSnapshot());
    assertThat(compacted).as("two partitions, each rolled").hasSizeGreaterThan(2);

    append(partition("east"), rows(20, 2, "east"));
    delete(partition("east"), ImmutableList.of(at(compacted.get(0), 0)));
    LocalCompactor.compact(table, true, file -> true, 4);

    SnapshotRewriteResult result = rewrite();
    assertLossless(result);
    assertThat(result.plan().resurrectedRows()).isEqualTo(1);
  }

  // ------------------------------------------------------------------ helpers

  /**
   * Asserts the compaction really produced a map where one source file spans several targets.
   *
   * <p>Without this the rolling tests could pass while every run still carried the mapping's single
   * default target, which is the shape that hides the bug they exist to catch.
   */
  private void assertMultiTargetMap(Snapshot snapshot) {
    CompactionMap map = null;
    for (ManifestFile manifest : snapshot.allManifests(table.io())) {
      if (manifest.compactionMapLocation() != null) {
        map = CompactionMaps.read(table.io().newInputFile(manifest.compactionMapLocation()));
        break;
      }
    }

    assertThat(map).as("the compaction must have written a map").isNotNull();

    boolean spansTargets = false;
    for (CompactionMap.FileMapping mapping : map.fileMappings()) {
      Set<String> targets = Sets.newHashSet();
      for (CompactionMap.Run run : mapping.runs()) {
        targets.add(run.targetFile() != null ? run.targetFile() : mapping.targetFile());
      }

      if (targets.size() > 1) {
        spansTargets = true;
      }
    }

    assertThat(spansTargets)
        .as("at least one source file's rows must land in more than one target")
        .isTrue();
  }

  private org.apache.iceberg.StructLike partition(String value) {
    PartitionSpec spec = table.spec();
    PartitionKey key = new PartitionKey(spec, SCHEMA);
    Record row = record(0, value);
    key.partition(new InternalRecordWrapper(SCHEMA.asStruct()).wrap(row));
    return key;
  }

  private static List<Record> rows(int fromId, int count, String data) {
    List<Record> result = Lists.newArrayList();
    for (int i = 0; i < count; i += 1) {
      result.add(record(fromId + i, data));
    }

    return result;
  }

  private DataFile onlyDataFile(Snapshot snapshot) throws IOException {
    List<DataFile> files = dataFiles(snapshot);
    assertThat(files).hasSize(1);
    return files.get(0);
  }

  private List<DataFile> dataFiles(Snapshot snapshot) throws IOException {
    List<DataFile> files = Lists.newArrayList();
    for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(
              manifest,
              table.io(),
              ((HasTableOperations) table).operations().current().specsById())) {
        for (DataFile file : reader) {
          files.add(file);
        }
      }
    }

    files.sort(java.util.Comparator.comparing(DataFile::location));
    return files;
  }
}
