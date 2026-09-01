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
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.data.WorkloadGenerator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.snaprewrite.SnapshotRewriteReport;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.Test;

/**
 * The accounting, measured at a scale where file bytes dominate metadata.
 *
 * <p>Every claim in the design about what a rewrite saves has so far rested on arithmetic plus
 * measurements from tables of a few dozen rows, where a manifest outweighs the data it describes. The
 * only way to settle it is on a real history; short of that, this is the largest scale that runs in a
 * unit test, and it reports what it measures rather than only asserting bounds.
 *
 * <p>It also times the induction, because the planner works a row at a time and that is the property
 * a distributed implementation would have to change.
 */
public class TestSnapshotRewriteScale extends SnapshotRewriteTestBase {

  private static final int BASE_ROWS = 60_000;
  private static final int PER_TRANSACTION = 4_000;
  private static final int TRANSACTIONS = 5;

  @Test
  public void accountingHoldsWhereDataOutweighsMetadata() throws IOException {
    useSchema(WorkloadGenerator.SCHEMA, PartitionSpec.unpartitioned());

    long seed = 20260831L;
    append(WorkloadGenerator.generateRows(seed++, BASE_ROWS));
    compact();
    DataFile compacted = onlyDataFile(table.currentSnapshot());

    // Distinct positions, not the count generated: the clustered generator is called once per
    // transaction against the same space, so runs overlap and re-deleting a dead position kills
    // nothing new. Counting generated positions would over-count the deaths.
    Set<Long> died = Sets.newHashSet();
    for (int i = 0; i < TRANSACTIONS; i += 1) {
      append(WorkloadGenerator.generateRows(seed++, PER_TRANSACTION));

      // Clustered deletes against the compaction, the shape a purge has.
      long[] positions =
          WorkloadGenerator.generateClusteredPositions(seed++, BASE_ROWS, 200, 50);
      List<Pair<CharSequence, Long>> targets = Lists.newArrayList();
      for (long position : positions) {
        targets.add(Pair.of(compacted.location(), position));
      }

      delete(targets);
      for (long position : positions) {
        died.add(position);
      }
    }

    compact();

    long planStart = System.nanoTime();
    SnapshotRewriteResult result = rewrite();
    long planMillis = (System.nanoTime() - planStart) / 1_000_000;

    SnapshotRewriteReport report = result.report();
    System.out.printf(
        "SCALE base=%d perTx=%d txs=%d died=%d resurrected=%d deletePositions=%d "
            + "reclaimable=%d added=%d saved=%d predicted=%d deleteBytes=%d metadataBytes=%d "
            + "millis=%d%n",
        BASE_ROWS,
        PER_TRANSACTION,
        TRANSACTIONS,
        died.size(),
        report.resurrectedRows(),
        report.deletePositions(),
        report.reclaimableBytes(),
        report.addedBytes(),
        report.savedBytes(),
        report.predictedSavedBytes(),
        report.deleteBytes(),
        report.metadataBytes(),
        planMillis);

    // Each row that died is recovered exactly once -- the bound the whole cost argument rests on.
    assertThat(report.resurrectedRows()).isEqualTo(died.size());

    // At this scale the rewrite pays, and metadata is no longer the dominant term.
    assertThat(report.savedBytes()).isPositive();
    assertThat(report.metadataBytes()).isLessThan(report.reclaimableBytes() / 4);

    // Setting the delete vectors aside, the saving is about one copy of the compacted table.
    double withoutDeletes =
        (double) (report.savedBytes() + report.deleteBytes()) / report.predictedSavedBytes();
    assertThat(withoutDeletes).isBetween(0.75, 1.35);

    assertLossless(result);
  }

  private DataFile onlyDataFile(Snapshot snapshot) throws IOException {
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

    assertThat(files).hasSize(1);
    return files.get(0);
  }
}
