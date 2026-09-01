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
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * A complete description of a rewrite, produced without writing anything.
 *
 * <p>The plan resolves every row location, so the only thing materialization adds is file metrics.
 * That is why {@code --dry-run} can stop here and still report exact row counts: the induction has
 * already run.
 */
public class SnapshotRewritePlan {
  private final TableMetadata base;
  private final Snapshot compaction;
  private final List<Snapshot> window;
  private final List<DataFile> targetFiles;
  private final Map<Long, RewrittenSnapshot> rewritten;
  private final List<ResurrectionRequest> resurrections;
  private final Map<String, Long> detachedFiles;
  private final long resurrectedRows;
  private final long targetRows;

  SnapshotRewritePlan(
      TableMetadata base,
      Snapshot compaction,
      List<Snapshot> window,
      List<DataFile> targetFiles,
      Map<Long, RewrittenSnapshot> rewritten,
      List<ResurrectionRequest> resurrections,
      Map<String, Long> detachedFiles,
      long resurrectedRows,
      long targetRows) {
    this.base = base;
    this.compaction = compaction;
    this.window = ImmutableList.copyOf(window);
    this.targetFiles = ImmutableList.copyOf(targetFiles);
    this.rewritten = ImmutableMap.copyOf(rewritten);
    this.resurrections = ImmutableList.copyOf(resurrections);
    this.detachedFiles = ImmutableMap.copyOf(detachedFiles);
    this.resurrectedRows = resurrectedRows;
    this.targetRows = targetRows;
  }

  public TableMetadata base() {
    return base;
  }

  /** The compaction the window is rewritten onto. Not itself rewritten. */
  public Snapshot compaction() {
    return compaction;
  }

  /** Snapshots to rewrite, oldest first. The oldest is the previous compaction. */
  public List<Snapshot> window() {
    return window;
  }

  /** The compaction's live data files, carried into every rewritten snapshot. */
  public List<DataFile> targetFiles() {
    return targetFiles;
  }

  public RewrittenSnapshot forSnapshot(long snapshotId) {
    return rewritten.get(snapshotId);
  }

  /** The rewritten form of every snapshot in the window, oldest first. */
  public List<RewrittenSnapshot> rewrittenSnapshots() {
    ImmutableList.Builder<RewrittenSnapshot> builder = ImmutableList.builder();
    for (Snapshot snapshot : window) {
      builder.add(rewritten.get(snapshot.snapshotId()));
    }

    return builder.build();
  }

  public List<ResurrectionRequest> resurrections() {
    return resurrections;
  }

  /** Files reachable from the window before the rewrite and from nothing after it, by size. */
  public Map<String, Long> detachedFiles() {
    return detachedFiles;
  }

  /** Rows that died inside the window and must be copied forward. */
  public long resurrectedRows() {
    return resurrectedRows;
  }

  /** Rows in the compaction's output. */
  public long targetRows() {
    return targetRows;
  }

  /** One rewritten snapshot: which resurrection files it sees, and what it deletes. */
  public static class RewrittenSnapshot {
    private final Snapshot original;
    private final List<String> resurrectionPaths;
    private final List<PositionDeleteRequest> deleteRequests;

    RewrittenSnapshot(
        Snapshot original,
        List<String> resurrectionPaths,
        List<PositionDeleteRequest> deleteRequests) {
      this.original = original;
      this.resurrectionPaths = ImmutableList.copyOf(resurrectionPaths);
      this.deleteRequests = ImmutableList.copyOf(deleteRequests);
    }

    public Snapshot original() {
      return original;
    }

    /**
     * Resurrection files present in this snapshot: those created by inverting later transactions.
     */
    public List<String> resurrectionPaths() {
      return resurrectionPaths;
    }

    public List<PositionDeleteRequest> deleteRequests() {
      return deleteRequests;
    }

    public long deletedPositions() {
      return deleteRequests.stream().mapToLong(PositionDeleteRequest::positionCount).sum();
    }
  }
}
