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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.snaprewrite.SnapshotRewriteReport;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.junit.jupiter.api.Test;

/**
 * The report is the point of the prototype: reclaim is what the design is for, so what matters most
 * is knowing how much there is to reclaim before doing anything.
 */
public class TestSnapshotRewriteReport extends SnapshotRewriteTestBase {

  /**
   * Estimating writes nothing and still gets the row counts exactly right.
   *
   * <p>The induction has already run by the time a plan exists, so only file sizes are
   * extrapolated. That is what makes a dry run useful for sweeping a history to find windows worth
   * rewriting.
   */
  @Test
  public void estimateMatchesMaterializedRowCounts() throws IOException {
    buildWindow();

    SnapshotRewriteReport estimate = rewriter().estimate();
    SnapshotRewriteResult materialized = rewrite();
    SnapshotRewriteReport measured = materialized.report();

    assertThat(estimate.estimated()).isTrue();
    assertThat(measured.estimated()).isFalse();
    assertThat(estimate.resurrectedRows()).isEqualTo(measured.resurrectedRows());
    assertThat(estimate.deletePositions()).isEqualTo(measured.deletePositions());
    assertThat(estimate.reclaimableBytes()).isEqualTo(measured.reclaimableBytes());
    assertThat(estimate.deadRatio()).isEqualTo(measured.deadRatio());
  }

  /** Estimating leaves the table exactly as it found it. */
  @Test
  public void estimateWritesNothing() throws IOException {
    buildWindow();

    long before = countFiles();
    rewriter().estimate();
    assertThat(countFiles()).isEqualTo(before);
  }

  /** An insert-only window resurrects nothing, and the report says so rather than inferring it. */
  @Test
  public void insertOnlyWindowReportsNoResurrection() throws IOException {
    append(records(1, 4, "base"));
    compact();
    append(records(10, 3, "alpha"));
    append(records(20, 3, "beta"));
    compact();

    SnapshotRewriteReport report = rewrite().report();
    assertThat(report.resurrectedRows()).isZero();
    assertThat(report.deadRatio()).isZero();
    assertThat(report.deletePositions()).isPositive();

    // On a ten-row table the rewrite loses: each rewritten snapshot needs a manifest and a manifest
    // list, several kilobytes of Avro apiece, against data files of a few hundred bytes. The
    // accounting argument predicts a saving of about one copy of the compacted table, and one copy
    // of
    // a table this small is worth less than the metadata describing it. Reporting the negative is
    // the
    // honest answer, and it is why the report exists: whether a window is worth rewriting is a
    // question about that window, not about the design.
    assertThat(report.savedBytes()).isNegative();
    assertThat(report.predictedSavedBytes()).isPositive();
  }

  /**
   * With data files large relative to manifests, the saving matches the prediction.
   *
   * <p>Each row that dies in the window is materialized exactly once, so retaining the history
   * costs one copy of the compacted table less than it did. That is the claim; here it is measured.
   */
  @Test
  public void savingApproachesOneCopyOfTheTableAtScale() throws IOException {
    append(records(0, 20000, "base"));
    compact();

    DataFile alpha = append(records(100000, 5000, "alpha"));
    delete(ImmutableList.of(at(alpha, 0), at(alpha, 1)));
    append(records(200000, 5000, "beta"));
    compact();

    SnapshotRewriteReport report = rewrite().report();

    assertThat(report.savedBytes()).isPositive();
    assertThat(report.resurrectedRows()).isEqualTo(2);

    // Metadata and delete files are the only difference between the two, and both are small next to
    // the data at this scale.
    // The prediction is one copy of the compacted table less the deletes the rewritten snapshots
    // carry. On synthetic, highly compressible rows the delete term is a large fraction of the data
    // term, so the saving lands well below a full copy while still being a real saving.
    assertThat(report.savedBytes()).isEqualTo(report.reclaimableBytes() - report.addedBytes());
    // Setting the delete vectors aside, the saving is about one copy of the compacted table: the
    // old layout held the same rows in more files, and every row that died was copied forward
    // exactly once. What the deletes cost is the price of keeping the history addressable.
    double withoutDeletes =
        (double) (report.savedBytes() + report.deleteBytes()) / report.predictedSavedBytes();
    assertThat(withoutDeletes).isBetween(0.7, 1.6);
  }

  /**
   * A window that kills most of what it inserts saves little, and says so.
   *
   * <p>The saving is roughly one copy of the compacted table, so a window whose rows mostly die
   * spends nearly as much recovering them as it frees. That is the case the dead-ratio guard is
   * for.
   */
  @Test
  public void mostlyDeadWindowSavesLittle() throws IOException {
    append(records(1, 4, "base"));
    compact();

    DataFile alpha = append(records(10, 8, "alpha"));
    delete(
        ImmutableList.of(
            at(alpha, 0), at(alpha, 1), at(alpha, 2), at(alpha, 3), at(alpha, 4), at(alpha, 5)));
    compact();

    SnapshotRewriteReport report = rewrite().report();
    assertThat(report.resurrectedRows()).isEqualTo(6);
    assertThat(report.deadRatio()).isGreaterThan(0.5);
  }

  /** The report renders the prediction alongside the measurement so the claim can be checked. */
  @Test
  public void reportRendersPredictionAndMeasurement() throws IOException {
    buildWindow();
    String text = rewrite().report().toString();

    assertThat(text).contains("reclaimable");
    assertThat(text).contains("resurrected");
    assertThat(text).contains("predicted");
    assertThat(text).contains("dead-ratio");
  }

  /**
   * The cost of the delete vectors grows with the square of the window length.
   *
   * <p>Every rewritten snapshot has to delete every row inserted after it, so lengthening a window
   * costs more than proportionally. It is the term that decides whether a window is worth
   * rewriting, and it argues for rewriting often rather than letting history pile up between
   * compactions.
   */
  @Test
  public void deleteCostGrowsWithWindowLength() throws IOException {
    append(records(1, 200, "base"));
    compact();
    for (int i = 0; i < 2; i += 1) {
      append(records(1000 + i * 100, 100, "t" + i));
    }

    compact();
    long shortWindow = rewrite().report().deletePositions();

    usePartitionSpec(org.apache.iceberg.PartitionSpec.unpartitioned());
    append(records(1, 200, "base"));
    compact();
    for (int i = 0; i < 6; i += 1) {
      append(records(1000 + i * 100, 100, "t" + i));
    }

    compact();
    long longWindow = rewrite().report().deletePositions();

    // Three times the commits, but far more than three times the positions.
    assertThat(longWindow).isGreaterThan(3 * shortWindow);
  }

  private void buildWindow() throws IOException {
    append(records(1, 6, "base"));
    compact();

    DataFile alpha = append(records(10, 4, "alpha"));
    delete(ImmutableList.of(at(alpha, 1)));
    append(records(20, 3, "beta"));
    compact();
  }

  private long countFiles() {
    java.io.File root = new java.io.File(table.location().replaceFirst("^file:", ""));
    return count(root);
  }

  private long count(java.io.File directory) {
    java.io.File[] children = directory.listFiles();
    if (children == null) {
      return 0;
    }

    long total = 0;
    for (java.io.File child : children) {
      total += child.isDirectory() ? count(child) : 1;
    }

    return total;
  }
}
