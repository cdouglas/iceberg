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
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.data.GenericSnapshotRewriteIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.snaprewrite.RewriteRefusal;
import org.apache.iceberg.snaprewrite.SnapshotRewrite;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.apache.iceberg.snaprewrite.SnapshotRewriteSurvey;
import org.junit.jupiter.api.Test;

/**
 * Choosing how far back a pass should reach.
 *
 * <p>Reaching further releases older layouts and costs more than proportionally, and neither side of
 * that has a right answer in general. The survey prices each reach so the decision is made per table
 * from measurement rather than from a rule.
 */
public class TestSnapshotRewriteSurvey extends SnapshotRewriteTestBase {

  /** One priced candidate per older compaction, shortest reach first, with cost rising. */
  @Test
  public void pricesEveryReach() throws IOException {
    threeCompactions();

    List<SnapshotRewriteSurvey.Candidate> candidates =
        SnapshotRewriteSurvey.survey(table, new GenericSnapshotRewriteIO(table));

    assertThat(candidates).as("two older compactions, two reaches").hasSize(2);
    for (SnapshotRewriteSurvey.Candidate candidate : candidates) {
      assertThat(candidate.plannable()).isTrue();
      assertThat(candidate.estimate()).isNotNull();
      assertThat(candidate.alreadyCurrent()).isFalse();
    }

    // Shortest first, and reaching further covers more snapshots and writes more deletes.
    assertThat(candidates.get(1).snapshotCount())
        .isGreaterThan(candidates.get(0).snapshotCount());
    assertThat(candidates.get(1).estimate().deletePositions())
        .as("a longer reach re-pays for the snapshots it covers")
        .isGreaterThan(candidates.get(0).estimate().deletePositions());
  }

  /** Every candidate targets the newest compaction, because nothing else can release its inputs. */
  @Test
  public void allCandidatesTargetTheNewestCompaction() throws IOException {
    Snapshot newest = threeCompactions();

    for (SnapshotRewriteSurvey.Candidate candidate :
        SnapshotRewriteSurvey.survey(table, new GenericSnapshotRewriteIO(table))) {
      assertThat(candidate.compaction().snapshotId()).isEqualTo(newest.snapshotId());
    }
  }

  /**
   * After a pass, the reach it took reports itself as already current.
   *
   * <p>This is the condition a recurring pass skips on, and it is not "has this been rewritten": a
   * snapshot moved onto an older compaction has been rewritten and still needs moving. What matters
   * is whether it still references anything the newest compaction replaced.
   */
  @Test
  public void aCompletedReachReportsItselfCurrent() throws IOException {
    threeCompactions();

    GenericSnapshotRewriteIO io = new GenericSnapshotRewriteIO(table);
    List<SnapshotRewriteSurvey.Candidate> before = SnapshotRewriteSurvey.survey(table, io);
    SnapshotRewriteSurvey.Candidate longest = before.get(before.size() - 1);
    assertThat(longest.alreadyCurrent()).isFalse();

    SnapshotRewriteResult result =
        SnapshotRewrite.forTable(table, io)
            .onCompaction(longest.compaction().snapshotId())
            .floor(longest.floor().snapshotId())
            .maxDeadRatio(Double.MAX_VALUE)
            .materialize();
    result.commit(((HasTableOperations) table).operations());
    table.refresh();

    for (SnapshotRewriteSurvey.Candidate after :
        SnapshotRewriteSurvey.survey(table, new GenericSnapshotRewriteIO(table))) {
      assertThat(after.alreadyCurrent())
          .as("reach to %s is done", after.floor().snapshotId())
          .isTrue();
    }
  }

  /** A reach that cannot be planned reports why, rather than throwing out of the survey. */
  @Test
  public void anUnplannableReachReportsItsRefusal() throws IOException {
    append(records(1, 5, "base"));
    compact();

    append(records(10, 3, "alpha"));
    compact();

    // An equality delete in the window makes the longer reach unplannable, and the survey has to
    // price what it can rather than abandoning the whole table.
    table.newRowDelta().addDeletes(equalityDelete()).commit();
    append(records(20, 3, "beta"));
    compact();

    List<SnapshotRewriteSurvey.Candidate> candidates =
        SnapshotRewriteSurvey.survey(table, new GenericSnapshotRewriteIO(table));

    assertThat(candidates).isNotEmpty();
    assertThat(candidates)
        .anySatisfy(
            candidate -> {
              assertThat(candidate.plannable()).isFalse();
              assertThat(candidate.refusal()).isEqualTo(RewriteRefusal.EQUALITY_DELETES);
              assertThat(candidate.estimate()).isNull();
            });
  }

  /** A table with no compaction map has nothing to survey, and says so instead of failing. */
  @Test
  public void aTableWithoutCompactionsHasNoCandidates() throws IOException {
    append(records(1, 4, "base"));
    append(records(10, 2, "alpha"));

    assertThat(SnapshotRewriteSurvey.survey(table, new GenericSnapshotRewriteIO(table))).isEmpty();
  }

  /** Rendering a candidate shows the reach, its size, and its price. */
  @Test
  public void candidatesRenderTheirReachAndPrice() throws IOException {
    threeCompactions();

    String text = SnapshotRewriteSurvey.survey(table, new GenericSnapshotRewriteIO(table)).get(0).toString();
    assertThat(text).contains("reach to");
    assertThat(text).contains("snapshots");
    assertThat(text).contains("reclaimable");
  }

  // ------------------------------------------------------------------ helpers

  /** Three compactions with work between each, so the survey has two reaches to price. */
  private Snapshot threeCompactions() throws IOException {
    append(records(1, 8, "base"));
    compact();
    DataFile first = onlyDataFile(table.currentSnapshot());

    DataFile alpha = append(records(100, 4, "alpha"));
    delete(ImmutableList.of(at(first, 0), at(alpha, 1)));
    compact();

    DataFile beta = append(records(200, 4, "beta"));
    delete(ImmutableList.of(at(beta, 0)));
    return compact();
  }

  private org.apache.iceberg.DeleteFile equalityDelete() throws IOException {
    org.apache.iceberg.Schema deleteSchema = table.schema().select("id");
    org.apache.iceberg.data.Record delete =
        org.apache.iceberg.data.GenericRecord.create(deleteSchema);
    delete.setField("id", 1);
    return org.apache.iceberg.data.FileHelpers.writeDeleteFile(
        table,
        table
            .io()
            .newOutputFile(
                table.location()
                    + "/data/"
                    + org.apache.iceberg.FileFormat.PARQUET.addExtension(
                        "eq-" + java.util.UUID.randomUUID())),
        ImmutableList.of(delete),
        deleteSchema);
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
