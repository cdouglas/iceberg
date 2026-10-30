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
 * What a reconstruction rebuilt, what it reused, and what it costs.
 *
 * <p>A reconstruction undoes a rewrite's saving, so it spends roughly what the rewrite reclaimed:
 * one copy of the original layout's detached files. {@link #predictedBytes()} is that figure, read
 * from the ledger's record of the original files, and {@link #rebuiltBytes()} is what was actually
 * written. They differ by the holes left out and by the writer, not by the algorithm.
 */
public class SnapshotReconstructReport {
  private static final double MIB = 1024.0 * 1024.0;

  private final int snapshots;
  private final int reused;
  private final int rebuiltInPlace;
  private final int rebuiltMoved;
  private final int skipped;
  private final long holes;
  private final long rebuiltRows;
  private final long rebuiltBytes;
  private final long predictedBytes;
  private final int vectors;
  private final long vectorBytes;
  private final long metadataBytes;
  private final long reclaimableBytes;

  SnapshotReconstructReport(
      int snapshots,
      int reused,
      int rebuiltInPlace,
      int rebuiltMoved,
      int skipped,
      long holes,
      long rebuiltRows,
      long rebuiltBytes,
      long predictedBytes,
      int vectors,
      long vectorBytes,
      long metadataBytes,
      long reclaimableBytes) {
    this.snapshots = snapshots;
    this.reused = reused;
    this.rebuiltInPlace = rebuiltInPlace;
    this.rebuiltMoved = rebuiltMoved;
    this.skipped = skipped;
    this.holes = holes;
    this.rebuiltRows = rebuiltRows;
    this.rebuiltBytes = rebuiltBytes;
    this.predictedBytes = predictedBytes;
    this.vectors = vectors;
    this.vectorBytes = vectorBytes;
    this.metadataBytes = metadataBytes;
    this.reclaimableBytes = reclaimableBytes;
  }

  /** Snapshots reconstructed. */
  public int snapshots() {
    return snapshots;
  }

  /** Original files that still existed, and are referenced again as they are. */
  public int reusedFiles() {
    return reused;
  }

  /**
   * Files rebuilt at their original path: they had no holes, so they come back with the same rows
   * at the same positions with the same ids.
   */
  public int rebuiltInPlace() {
    return rebuiltInPlace;
  }

  /** Files rebuilt at a new path, because leaving out their holes moved the rows after them. */
  public int rebuiltMoved() {
    return rebuiltMoved;
  }

  /** Files left out entirely, because no window snapshot could read any of their rows. */
  public int skippedFiles() {
    return skipped;
  }

  /** Rows no window snapshot could read, so not rebuilt. */
  public long holes() {
    return holes;
  }

  public long rebuiltRows() {
    return rebuiltRows;
  }

  public long rebuiltBytes() {
    return rebuiltBytes;
  }

  /** The size of the original files rebuilt, as the ledger recorded them. */
  public long predictedBytes() {
    return predictedBytes;
  }

  public int deletionVectors() {
    return vectors;
  }

  public long deletionVectorBytes() {
    return vectorBytes;
  }

  public long metadataBytes() {
    return metadataBytes;
  }

  /** The rewritten layout's files that nothing references once this is committed. */
  public long reclaimableBytes() {
    return reclaimableBytes;
  }

  /** Bytes the reconstruction adds, net of what it lets reclaim delete. */
  public long netBytes() {
    return rebuiltBytes + vectorBytes + metadataBytes - reclaimableBytes;
  }

  @Override
  public String toString() {
    StringBuilder text = new StringBuilder();
    text.append(String.format(Locale.ROOT, "reconstructed %d snapshots%n", snapshots));
    text.append(
        String.format(
            Locale.ROOT,
            "  files        reused %d   rebuilt in place %d   moved %d   skipped %d%n",
            reused,
            rebuiltInPlace,
            rebuiltMoved,
            skipped));
    text.append(
        String.format(
            Locale.ROOT,
            "  rebuilt      %+10.1f MiB   (%d rows, %d holes left out)%n",
            rebuiltBytes / MIB,
            rebuiltRows,
            holes));
    text.append(
        String.format(
            Locale.ROOT,
            "  predicted    %+10.1f MiB   (the original files, per the ledger)%n",
            predictedBytes / MIB));
    text.append(
        String.format(
            Locale.ROOT,
            "  deletes      %+10.1f MiB   (%d deletion vectors)%n",
            vectorBytes / MIB,
            vectors));
    text.append(String.format(Locale.ROOT, "  metadata     %+10.1f MiB%n", metadataBytes / MIB));
    text.append(String.format(Locale.ROOT, "  reclaimable  %10.1f MiB%n", -reclaimableBytes / MIB));
    text.append(String.format(Locale.ROOT, "  net          %+10.1f MiB%n", netBytes() / MIB));
    return text.toString();
  }
}
