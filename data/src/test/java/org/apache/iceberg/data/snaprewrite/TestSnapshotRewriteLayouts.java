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
   * <p>The compaction map says nothing about an untouched file because nothing moved, so the locator
   * has to treat those rows as already in place rather than as missing.
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
   * <p>Reachable only with an explicit floor, since a window otherwise stops at the first compaction
   * it meets going back. A row inserted before the intermediate compaction was relocated twice, and
   * placing it means following both maps.
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

  // ------------------------------------------------------------------ helpers

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
