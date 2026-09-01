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

import java.util.Locale;

/**
 * What a rewrite would reclaim, and what it costs to reclaim it.
 *
 * <p>Reclaim is the point of the design, so this is the primary output. It prints the measurement
 * next to the prediction from the design's accounting argument -- each row that died in the window
 * is materialized exactly once, so retaining the history should cost one full copy of the table
 * less than it did -- which makes that claim falsifiable on real tables rather than only in tests.
 */
public class SnapshotRewriteReport {
  private static final double MIB = 1024 * 1024;

  private final long snapshotCount;
  private final long reclaimableBytes;
  private final long resurrectedBytes;
  private final int resurrectedFiles;
  private final long resurrectedRows;
  private final long deleteBytes;
  private final int deleteFiles;
  private final long deletePositions;
  private final long metadataBytes;
  private final long targetBytes;
  private final long targetRows;
  private final boolean estimated;

  SnapshotRewriteReport(
      long snapshotCount,
      long reclaimableBytes,
      long resurrectedBytes,
      int resurrectedFiles,
      long resurrectedRows,
      long deleteBytes,
      int deleteFiles,
      long deletePositions,
      long metadataBytes,
      long targetBytes,
      long targetRows,
      boolean estimated) {
    this.snapshotCount = snapshotCount;
    this.reclaimableBytes = reclaimableBytes;
    this.resurrectedBytes = resurrectedBytes;
    this.resurrectedFiles = resurrectedFiles;
    this.resurrectedRows = resurrectedRows;
    this.deleteBytes = deleteBytes;
    this.deleteFiles = deleteFiles;
    this.deletePositions = deletePositions;
    this.metadataBytes = metadataBytes;
    this.targetBytes = targetBytes;
    this.targetRows = targetRows;
    this.estimated = estimated;
  }

  public long reclaimableBytes() {
    return reclaimableBytes;
  }

  public long addedBytes() {
    return resurrectedBytes + deleteBytes + metadataBytes;
  }

  /** Bytes saved by the rewrite. Positive means the rewrite frees space. */
  public long savedBytes() {
    return reclaimableBytes - addedBytes();
  }

  /** The design's prediction: the saving should be about one full copy of the compacted table. */
  public long predictedSavedBytes() {
    return targetBytes;
  }

  public long resurrectedRows() {
    return resurrectedRows;
  }

  public long deletePositions() {
    return deletePositions;
  }

  /** Rows that died in the window relative to rows that survived it. */
  public double deadRatio() {
    return targetRows == 0 ? 0.0 : (double) resurrectedRows / targetRows;
  }

  /** True when file sizes are extrapolated because nothing was materialized. */
  public boolean estimated() {
    return estimated;
  }

  @Override
  public String toString() {
    StringBuilder text = new StringBuilder();
    text.append(
        String.format(
            Locale.ROOT,
            "window        %d snapshots%s%n",
            snapshotCount,
            estimated ? "  (estimated -- nothing materialized)" : ""));
    text.append(String.format(Locale.ROOT, "  reclaimable  %10.1f MiB%n", -reclaimableBytes / MIB));
    text.append(
        String.format(
            Locale.ROOT,
            "  resurrected  %+10.1f MiB   (%d files, %d rows)%n",
            resurrectedBytes / MIB,
            resurrectedFiles,
            resurrectedRows));
    text.append(
        String.format(
            Locale.ROOT,
            "  deletes      %+10.1f MiB   (%d files, %d positions)%n",
            deleteBytes / MIB,
            deleteFiles,
            deletePositions));
    text.append(String.format(Locale.ROOT, "  metadata     %+10.1f MiB%n", metadataBytes / MIB));
    text.append(String.format(Locale.ROOT, "  net          %+10.1f MiB%n", -savedBytes() / MIB));
    text.append(
        String.format(
            Locale.ROOT,
            "  predicted    %+10.1f MiB   (one copy of the compacted table; residual %+.1f MiB)%n",
            -predictedSavedBytes() / MIB,
            (savedBytes() - predictedSavedBytes()) / MIB));
    text.append(
        String.format(
            Locale.ROOT,
            "  rows         live %d   resurrected %d   dead-ratio %.3f%n",
            targetRows,
            resurrectedRows,
            deadRatio()));
    return text.toString();
  }
}
