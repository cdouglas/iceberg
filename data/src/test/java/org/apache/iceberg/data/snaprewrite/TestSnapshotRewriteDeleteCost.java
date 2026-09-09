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
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.WorkloadGenerator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.snaprewrite.SnapshotRewriteReport;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.junit.jupiter.api.Test;

/**
 * What the delete vectors actually cost, measured rather than estimated.
 *
 * <p>{@code SnapshotRewrite.estimate()} prices a delete position at a flat 8 bytes, which is what a
 * {@code long[]} costs and is not what a delete vector costs. The position count over a window of
 * {@code m} transactions grows like {@code m^2}, so the estimate makes a long window look ruinous.
 * This reads the sizes off the files the rewrite actually wrote.
 *
 * <p>The shape matters. A rewritten snapshot masks every row inserted after it, and an
 * order-preserving compaction lays those rows out consecutively, so what each snapshot masks is a
 * contiguous range of the compaction. Roaring encodes a range as a run, not as positions, so the
 * bytes track the number of ranges rather than the number of rows in them.
 */
public class TestSnapshotRewriteDeleteCost extends SnapshotRewriteTestBase {

  /** Insert-only window: no recovery, so the delete vectors are the entire cost of the rewrite. */
  @Test
  public void insertOnlyWindowDeleteCostIsMeasured() throws IOException {
    System.out.printf(
        "%n%-6s %-6s %-9s %11s %11s %11s %9s %9s %11s%n",
        "fmt",
        "cols",
        "txs",
        "positions",
        "dv-bytes",
        "b/position",
        "meta-bytes",
        "reclaim",
        "saved-real");

    for (int formatVersion : new int[] {3, 2}) {
      for (boolean wide : new boolean[] {true, false}) {
        for (int transactions : new int[] {2, 4, 8}) {
          measure(formatVersion, wide, transactions);
        }
      }
    }
  }

  private void measure(int formatVersion, boolean wide, int transactions) throws IOException {
    Schema schema = wide ? WorkloadGenerator.SCHEMA : SCHEMA;
    useSchema(schema, PartitionSpec.unpartitioned(), formatVersion);

    long seed = 20260909L + transactions + (wide ? 100 : 0) + formatVersion * 1000L;
    append(rows(schema, seed++, 20_000));
    compact();

    for (int i = 0; i < transactions; i += 1) {
      append(rows(schema, seed++, 2_000));
    }

    compact();

    SnapshotRewriteReport dryRun = rewriter().estimate();
    SnapshotRewriteResult result = rewrite();
    SnapshotRewriteReport report = result.report();

    // No row dies in an insert-only window, so nothing is recovered and the deletes are the cost.
    assertThat(report.resurrectedRows()).isZero();

    long deleteBytes = actualDeleteBytes(result);
    long positions = report.deletePositions();
    double bytesPerPosition = positions == 0 ? 0 : (double) deleteBytes / positions;
    double estimatedPerPosition = positions == 0 ? 0 : (double) dryRun.deleteBytes() / positions;
    long savedReal = report.reclaimableBytes() - deleteBytes;
    assertThat(estimatedPerPosition).isGreaterThan(bytesPerPosition);

    System.out.printf(
        "%-6d %-6d %-9d %11d %11d %11.3f %11d %11d %11d%n",
        formatVersion,
        schema.columns().size(),
        transactions,
        positions,
        deleteBytes,
        bytesPerPosition,
        report.metadataBytes(),
        report.reclaimableBytes(),
        savedReal - report.metadataBytes());

    // The point of the measurement: the real cost is well under the flat-8-bytes estimate.
    assertThat(deleteBytes)
        .as("real delete bytes must be well under the flat 8-bytes-per-position estimate")
        .isLessThan(dryRun.deleteBytes());

    // And the rewrite pays: what it frees exceeds what its deletes cost.
    assertThat(savedReal).as("insert-only window must save bytes").isPositive();

    result.discard();
  }

  /**
   * Per-snapshot delete sizes for one window, oldest first.
   *
   * <p>The oldest snapshot in the window masks every row the window inserted; the newest masks
   * almost none. If a delete vector cost anything per position, the first row of this table would
   * dwarf the last.
   */
  @Test
  public void deleteSizePerSnapshotIsFlat() throws IOException {
    useSchema(WorkloadGenerator.SCHEMA, PartitionSpec.unpartitioned(), 3);

    long seed = 20260909L;
    append(WorkloadGenerator.generateRows(seed++, 20_000));
    compact();
    for (int i = 0; i < 8; i += 1) {
      append(WorkloadGenerator.generateRows(seed++, 2_000));
    }

    compact();

    SnapshotRewriteResult result = rewrite();
    TableMetadata rewritten = result.metadata();

    System.out.printf("%n%-4s %11s %11s %11s%n", "k", "positions", "dv-bytes", "b/position");
    List<Long> sizes = Lists.newArrayList();
    int index = 0;
    for (Snapshot original : result.plan().window()) {
      Snapshot snapshot = rewritten.snapshot(original.snapshotId());
      long bytes = 0;
      long positions = 0;
      for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
        try (ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(manifest, table.io(), rewritten.specsById())) {
          for (DeleteFile file : reader) {
            bytes += file.fileSizeInBytes();
            positions += file.recordCount();
          }
        }
      }

      System.out.printf(
          "%-4d %11d %11d %11.4f%n",
          index, positions, bytes, positions == 0 ? 0 : (double) bytes / positions);
      if (positions > 0) {
        sizes.add(bytes);
      }

      index += 1;
    }

    // The oldest snapshot masks many times what the newest does. The vectors must not scale with
    // that: a contiguous range is a run, and a run is a constant.
    long smallest = sizes.get(sizes.size() - 1);
    long largest = sizes.get(0);
    assertThat(largest)
        .as("a vector masking the whole window must not cost much more than one masking a slice")
        .isLessThan(smallest * 3);

    result.discard();
  }

  /** Sizes of the delete files the rewrite wrote, counted once per distinct path. */
  private long actualDeleteBytes(SnapshotRewriteResult result) throws IOException {
    TableMetadata rewritten = result.metadata();
    Map<String, Long> byPath = Maps.newHashMap();
    for (Snapshot snapshot : rewritten.snapshots()) {
      for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
        try (ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(manifest, table.io(), rewritten.specsById())) {
          for (DeleteFile file : reader) {
            byPath.put(file.location(), file.fileSizeInBytes());
          }
        }
      }
    }

    long total = 0;
    for (long size : byPath.values()) {
      total += size;
    }

    return total;
  }

  private static List<Record> rows(Schema schema, long seed, int count) {
    if (schema == WorkloadGenerator.SCHEMA) {
      return WorkloadGenerator.generateRows(seed, count);
    }

    List<Record> result = Lists.newArrayList();
    for (int i = 0; i < count; i += 1) {
      result.add(record((int) (seed * 1_000_000 + i), "row-" + seed + "-" + i));
    }

    return result;
  }
}
