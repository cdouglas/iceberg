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
package org.apache.iceberg.snaprewrite;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Rewrites committed snapshots to reference a later compaction.
 *
 * <p>A compaction is a logical no-op that relocates rows, and the compaction map records where each
 * run went. The same translation that lets a concurrent transaction rebase onto a new layout also
 * lets an already-committed snapshot be re-expressed against it, so the history stays addressable
 * while the layouts it used to pin become garbage.
 *
 * <p>Nothing here commits. {@link #materialize()} writes files and builds metadata; reading the
 * result and committing it are separate, explicit steps.
 *
 * <pre>{@code
 * SnapshotRewriteResult result = SnapshotRewrite.forTable(table, io).onLatestCompaction().materialize();
 * System.out.print(result.report());
 * Table shadow = result.asTable();   // verify before deciding
 * }</pre>
 */
public class SnapshotRewrite {
  // Calibrated against TestSnapshotRewriteDeleteCost rather than guessed. The two delete formats
  // behave nothing alike, and pricing both at a flat rate per position -- which this used to do --
  // overstated a v3 window's delete cost by roughly fifty times.
  //
  // A rewritten snapshot masks the rows inserted after it. An order-preserving compaction lays
  // those out consecutively, so the masked set is a contiguous range, and Roaring stores a range as
  // a run: a deletion vector costs about the same whether it hides two thousand rows or sixteen
  // thousand. What is left is Puffin overhead, so v3 is priced per file and not per position.
  private static final long DV_BYTES_ESTIMATE = 512;

  // A v2 position-delete file stores a row per position, so it really is priced per position.
  private static final long V2_BYTES_PER_POSITION_ESTIMATE = 2;
  private static final long DELETE_FILE_OVERHEAD_ESTIMATE = 1024;

  // The compaction's data manifest, written once for the window and shared by every rewritten
  // snapshot. Measured at ~9.5 KiB for a single entry: an Avro manifest embeds the schema and
  // partition spec in its header, so even a tiny one costs kilobytes.
  private static final long SHARED_MANIFEST_BYTES_ESTIMATE = 10 * 1024;

  // A delete manifest plus a manifest list, which every rewritten snapshot needs of its own. This
  // is the term that decides whether a small window is worth anything: it outweighs the delete
  // vectors themselves by an order of magnitude or two.
  private static final long METADATA_BYTES_PER_SNAPSHOT_ESTIMATE = 14 * 1024;

  private final Table table;
  private final TableMetadata base;
  private final FileIO io;
  private final SnapshotRewriteIO rewriteIO;
  private Long compactionSnapshotId;
  private Long floorSnapshotId;
  private long minAgeMs = 0;
  private double maxDeadRatio = 0.5;

  private SnapshotRewrite(Table table, SnapshotRewriteIO rewriteIO) {
    this.table = table;
    this.base = ((HasTableOperations) table).operations().current();
    this.io = table.io();
    this.rewriteIO = rewriteIO;
  }

  public static SnapshotRewrite forTable(Table table, SnapshotRewriteIO rewriteIO) {
    return new SnapshotRewrite(table, rewriteIO);
  }

  /** Rewrites the window ending at this compaction. */
  public SnapshotRewrite onCompaction(long snapshotId) {
    this.compactionSnapshotId = snapshotId;
    return this;
  }

  /** Rewrites the window ending at the newest snapshot that carries a compaction map. */
  public SnapshotRewrite onLatestCompaction() {
    for (Snapshot snapshot : reverseAncestry()) {
      if (hasCompactionMap(snapshot)) {
        this.compactionSnapshotId = snapshot.snapshotId();
        return this;
      }
    }

    throw new RewriteRefusedException(
        RewriteRefusal.NO_COMPACTION, "no snapshot in the table carries a compaction map");
  }

  /** The oldest snapshot to rewrite. Defaults to the previous compaction. */
  public SnapshotRewrite floor(long snapshotId) {
    this.floorSnapshotId = snapshotId;
    return this;
  }

  /**
   * Refuses to rewrite a compaction younger than this.
   *
   * <p>A snapshot that could still be the base of an in-flight transaction should not be rewritten:
   * that transaction's validation would run against a history that no longer describes what
   * happened.
   */
  public SnapshotRewrite minAgeMs(long millis) {
    this.minAgeMs = millis;
    return this;
  }

  /** Refuses when the rows to resurrect exceed this fraction of the rows that survived. */
  public SnapshotRewrite maxDeadRatio(double ratio) {
    this.maxDeadRatio = ratio;
    return this;
  }

  /** Runs the induction without writing anything. */
  public SnapshotRewritePlan plan() {
    Preconditions.checkState(
        compactionSnapshotId != null, "Set a compaction with onCompaction or onLatestCompaction");
    return new SnapshotRewritePlanner(base, io, rewriteIO, minAgeMs, maxDeadRatio)
        .plan(compactionSnapshotId, floorSnapshotId);
  }

  /**
   * Reports what the rewrite would save without writing anything.
   *
   * <p>Row counts are exact: the induction has already run. File sizes are extrapolated from the
   * compaction's bytes per row, so the byte figures are indicative.
   */
  public SnapshotRewriteReport estimate() {
    SnapshotRewritePlan estimated = plan();

    long targetBytes = 0;
    for (DataFile file : estimated.targetFiles()) {
      targetBytes += file.fileSizeInBytes();
    }

    long positions = 0;
    int deleteFiles = 0;
    for (SnapshotRewritePlan.RewrittenSnapshot snapshot : estimated.rewrittenSnapshots()) {
      positions += snapshot.deletedPositions();
      deleteFiles += snapshot.deleteRequests().size();
    }

    double bytesPerRow =
        estimated.targetRows() == 0 ? 0 : (double) targetBytes / estimated.targetRows();
    long reclaimable = 0;
    for (long size : estimated.detachedFiles().values()) {
      reclaimable += size;
    }

    return new SnapshotRewriteReport(
        estimated.window().size(),
        reclaimable,
        Math.round(estimated.resurrectedRows() * bytesPerRow),
        estimated.resurrections().size(),
        estimated.resurrectedRows(),
        estimatedDeleteBytes(positions, deleteFiles),
        deleteFiles,
        positions,
        SHARED_MANIFEST_BYTES_ESTIMATE
            + (long) estimated.window().size() * METADATA_BYTES_PER_SNAPSHOT_ESTIMATE,
        targetBytes,
        estimated.targetRows(),
        true);
  }

  /**
   * What the rewrite's delete files will cost, by format.
   *
   * <p>Under v3 the answer barely depends on the position count; under v2 it is proportional to it.
   * See the constants for why.
   */
  private long estimatedDeleteBytes(long positions, int deleteFiles) {
    if (base.formatVersion() >= 3) {
      return deleteFiles * DV_BYTES_ESTIMATE;
    }

    return positions * V2_BYTES_PER_POSITION_ESTIMATE
        + (long) deleteFiles * DELETE_FILE_OVERHEAD_ESTIMATE;
  }

  /** Runs the induction, writes the rewritten layout, and returns it uncommitted. */
  public SnapshotRewriteResult materialize() {
    return materialize(SnapshotRewriteWriter.Stamping.SHARED_BASE);
  }

  SnapshotRewriteResult materialize(SnapshotRewriteWriter.Stamping stamping) {
    SnapshotRewritePlan materialized = plan();

    Map<String, DataFile> resurrected = Maps.newHashMap();
    for (ResurrectionRequest request : materialized.resurrections()) {
      resurrected.put(request.outputPath(), rewriteIO.resurrect(request));
    }

    Map<String, DeleteFile> deleteFiles = Maps.newHashMap();
    for (SnapshotRewritePlan.RewrittenSnapshot snapshot : materialized.rewrittenSnapshots()) {
      for (PositionDeleteRequest request : snapshot.deleteRequests()) {
        deleteFiles.put(request.outputPath(), rewriteIO.writePositionDeletes(request));
      }
    }

    SnapshotRewriteWriter writer =
        new SnapshotRewriteWriter(base, io, materialized, resurrected, deleteFiles, stamping);
    TableMetadata rewritten = writer.rewriteMetadata();

    return new SnapshotRewriteResult(
        table.name(),
        base,
        rewritten,
        io,
        materialized,
        resurrected,
        deleteFiles,
        writer.writtenPaths());
  }

  private List<Snapshot> reverseAncestry() {
    List<Snapshot> ancestors = Lists.newArrayList();
    Snapshot current = base.currentSnapshot();
    while (current != null) {
      ancestors.add(current);
      current = current.parentId() == null ? null : base.snapshot(current.parentId());
    }

    return ancestors;
  }

  private boolean hasCompactionMap(Snapshot snapshot) {
    return new CompactionMapLookup(io).forSnapshot(snapshot) != null;
  }
}
