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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRewriteUnsafe;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * Writes the manifests, manifest lists, and snapshots of a rewrite.
 *
 * <h2>Sequence number stamping</h2>
 *
 * <p>Every data file in the window -- the compaction's files and the recovery files -- is stamped
 * at one sequence number, {@code baseSequenceNumber}, chosen one below the oldest snapshot in the
 * window. Each snapshot's delete files are stamped at that snapshot's own number.
 *
 * <p>This is load-bearing and it fails open. Iceberg applies a positional delete to a data file
 * only when {@code data.dataSequenceNumber <= delete.dataSequenceNumber}, which is how a concurrent
 * append and a concurrent delete commute: the sequence number is the delete's "as-of" watermark,
 * and a data file added after it holds rows the delete's author never saw. A rewrite inverts that
 * on purpose -- its deletes reference files written long after the snapshot they land in -- so
 * leaving the compaction's real (higher) sequence numbers in place would make every delete inert.
 *
 * <p>In v2 that failure is silent: {@code DeleteFileIndex.PositionDeletes.filter} slices a
 * sequence-sorted array and simply omits deletes that sort too low. No exception, no log line, just
 * rows reappearing in a time-travel read.
 *
 * <h2>Why one shared number rather than each snapshot's own</h2>
 *
 * <p>Stamping each file at its own snapshot's number also satisfies the rule, by equality, and was
 * the original design. It costs more than it looks. A manifest <em>entry</em> carries the data
 * sequence number, so two snapshots that disagree about a file's number cannot share a manifest --
 * and per-snapshot stamping makes every rewritten snapshot disagree with every other about every
 * file in the compaction. Each therefore needs its own copy of a manifest describing the whole
 * compaction, which is precisely what the manifest list's indirection exists to avoid.
 *
 * <p>A single number below the window satisfies the rule by strict inequality instead, so the
 * entries agree and one manifest serves the window: {@code files + window} rather than {@code files
 * x window}. Measured, a six-snapshot window over a 26-file compaction writes 2.2 times less
 * metadata this way, and the ratio grows with the compaction's file count. It also reads as the
 * truth about these rows -- they were all present before the window began -- and leaves each
 * physical file with two sequence numbers table-wide rather than one per rewritten snapshot.
 *
 * <p>{@link Stamping#OWN} keeps the superseded mode so the difference stays measurable.
 */
class SnapshotRewriteWriter {

  /**
   * How data files are stamped in a rewritten snapshot.
   *
   * <p>{@link #SHARED_BASE} is the default and the one to use. {@link #OWN} is also correct but
   * costs a copy of the compaction's data manifests per rewritten snapshot; it is kept so the
   * difference can be measured. {@link #SOURCE} is wrong on purpose: because v2 drops inert deletes
   * without complaint, a suite that only ever exercises correct stamping cannot tell whether it
   * would notice the incorrect one.
   */
  enum Stamping {
    /**
     * Every data file in the window is stamped at one sequence number below the window, and each
     * snapshot's deletes at that snapshot's own. The delete rule holds by strict inequality, and
     * because the entries no longer differ between snapshots, they can share one data manifest.
     */
    SHARED_BASE,
    /**
     * Every file is stamped at its own snapshot's sequence number, so the delete rule holds by
     * equality. Correct, but no two snapshots can then share a manifest.
     */
    OWN,
    /** The compaction's real sequence numbers, which leaves every delete inert. */
    SOURCE
  }

  private final TableMetadata base;
  private final FileIO io;
  private final SnapshotRewritePlan plan;
  private final Map<String, DataFile> resurrected;
  private final Map<String, DeleteFile> deleteFiles;
  private final Stamping stamping;
  private final Map<Long, PartitionStatisticsFile> detachedPartitionStats;
  private final long baseSequenceNumber;
  private ManifestFile sharedTargetManifest;
  private final List<String> writtenPaths = Lists.newArrayList();

  SnapshotRewriteWriter(
      TableMetadata base,
      FileIO io,
      SnapshotRewritePlan plan,
      Map<String, DataFile> resurrected,
      Map<String, DeleteFile> deleteFiles,
      Stamping stamping) {
    this.base = base;
    this.io = io;
    this.plan = plan;
    this.resurrected = resurrected;
    this.deleteFiles = deleteFiles;
    this.stamping = stamping;
    this.detachedPartitionStats = detachedPartitionStats(base, plan);
    this.baseSequenceNumber = baseSequenceNumber(plan);
  }

  /**
   * The sequence number every data file in a shared-base rewrite is stamped at.
   *
   * <p>One below the oldest snapshot in the window, so that every rewritten snapshot's deletes --
   * stamped at that snapshot's own number -- satisfy the delete rule strictly. It also reads as the
   * truth about these rows: they were all present before the window began.
   */
  private static long baseSequenceNumber(SnapshotRewritePlan plan) {
    long oldest = Long.MAX_VALUE;
    for (Snapshot snapshot : plan.window()) {
      oldest = Math.min(oldest, snapshot.sequenceNumber());
    }

    return Math.max(0, oldest - 1);
  }

  /**
   * The partition statistics a rewrite invalidates, by the snapshot they describe.
   *
   * <p>Partition statistics are per-partition file counts, byte totals, and delete counts. All of
   * those describe a layout, and a rewritten snapshot holds a different one -- the whole
   * compaction, with most of it masked. Nothing in Iceberg checks a statistics file against the
   * snapshot it names, so leaving these attached would not fail; it would simply be believed. They
   * are dropped instead, and recorded in the snapshot's summary so a restore can put them back.
   *
   * <p>Table-level statistics are not dropped. The standard blob type is a theta sketch, which
   * counts distinct values among the rows live at a snapshot, and a rewrite preserves exactly that.
   */
  private static Map<Long, PartitionStatisticsFile> detachedPartitionStats(
      TableMetadata base, SnapshotRewritePlan plan) {
    Set<Long> rewrittenIds = Sets.newHashSet();
    for (Snapshot snapshot : plan.window()) {
      rewrittenIds.add(snapshot.snapshotId());
    }

    Map<Long, PartitionStatisticsFile> detached = Maps.newHashMap();
    for (PartitionStatisticsFile file : base.partitionStatisticsFiles()) {
      if (rewrittenIds.contains(file.snapshotId())) {
        detached.put(file.snapshotId(), file);
      }
    }

    return detached;
  }

  /** Paths of the statistics files this rewrite detached, for reclaim to delete. */
  List<String> detachedStatisticsPaths() {
    List<String> paths = Lists.newArrayList();
    for (PartitionStatisticsFile file : detachedPartitionStats.values()) {
      paths.add(file.path());
    }

    return paths;
  }

  /**
   * Returns metadata whose window snapshots reference the compaction's layout. Nothing is
   * committed.
   */
  TableMetadata rewriteMetadata() {
    Map<Long, Snapshot> replacements = Maps.newHashMap();
    for (Snapshot original : plan.window()) {
      replacements.put(original.snapshotId(), rewriteSnapshot(original));
    }

    List<Snapshot> snapshots = Lists.newArrayList();
    for (Snapshot snapshot : base.snapshots()) {
      Snapshot replacement = replacements.get(snapshot.snapshotId());
      snapshots.add(replacement != null ? replacement : snapshot);
    }

    List<PartitionStatisticsFile> partitionStats = Lists.newArrayList();
    for (PartitionStatisticsFile file : base.partitionStatisticsFiles()) {
      if (!detachedPartitionStats.containsKey(file.snapshotId())) {
        partitionStats.add(file);
      }
    }

    return SnapshotRewriteUnsafe.replaceSnapshots(
        base, snapshots, base.statisticsFiles(), partitionStats);
  }

  /** Metadata files this writer created, for reclaim accounting and for cleanup on failure. */
  List<String> writtenPaths() {
    return ImmutableList.copyOf(writtenPaths);
  }

  private Snapshot rewriteSnapshot(Snapshot original) {
    SnapshotRewritePlan.RewrittenSnapshot rewritten = plan.forSnapshot(original.snapshotId());
    long sequenceNumber = original.sequenceNumber();
    PartitionSpec spec = base.spec();

    // What the snapshot holds, for its summary totals: the compaction plus whatever it recovered.
    // How that is split across manifests depends on the stamping, but the contents do not.
    List<DataFile> dataFiles = Lists.newArrayList(plan.targetFiles());
    for (String path : rewritten.resurrectionPaths()) {
      dataFiles.add(resurrected.get(path));
    }

    List<ManifestFile> manifests = Lists.newArrayList();
    if (stamping == Stamping.SHARED_BASE) {
      // One manifest for the compaction's files, written once and referenced by every rewritten
      // snapshot. Its entries name the compaction as the snapshot that added them, which is true.
      manifests.add(sharedTargetManifest(spec));

      List<DataFile> recovered = Lists.newArrayList();
      for (String path : rewritten.resurrectionPaths()) {
        recovered.add(resurrected.get(path));
      }

      if (!recovered.isEmpty()) {
        manifests.add(
            writeDataManifest(
                "recovered-" + original.snapshotId(),
                original.snapshotId(),
                spec,
                baseSequenceNumber,
                recovered));
      }
    } else {
      manifests.add(
          writeDataManifest(
              "data-" + original.snapshotId(),
              original.snapshotId(),
              spec,
              sequenceNumber,
              dataFiles));
    }

    List<DeleteFile> deletes = Lists.newArrayList();
    for (PositionDeleteRequest request : rewritten.deleteRequests()) {
      deletes.add(deleteFiles.get(request.outputPath()));
    }

    if (!deletes.isEmpty()) {
      manifests.add(writeDeleteManifest(original, spec, sequenceNumber, deletes));
    }

    String manifestListPath = newMetadataPath("snaprewrite-list-" + original.snapshotId(), "avro");
    OutputFile manifestList = io.newOutputFile(manifestListPath);
    try {
      SnapshotRewriteUnsafe.writeManifestList(
          base.formatVersion(),
          manifestList,
          original.snapshotId(),
          original.parentId(),
          sequenceNumber,
          original.firstRowId(),
          manifests);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    writtenPaths.add(manifestListPath);

    return SnapshotRewriteUnsafe.newSnapshot(
        sequenceNumber,
        original.snapshotId(),
        original.parentId(),
        original.timestampMillis(),
        original.operation(),
        summary(original, dataFiles, deletes),
        original.schemaId(),
        manifestListPath,
        original.firstRowId(),
        original.addedRows());
  }

  /**
   * The compaction's files, in a manifest every rewritten snapshot can reference.
   *
   * <p>Written on the first snapshot that needs it and reused thereafter. It carries an assigned
   * sequence number so the manifest-list writers do not try to stamp it per snapshot, which is the
   * whole point: one manifest, one sequence number, m references.
   */
  private ManifestFile sharedTargetManifest(PartitionSpec spec) {
    if (sharedTargetManifest == null) {
      long compactionId = plan.compaction().snapshotId();
      this.sharedTargetManifest =
          SnapshotRewriteUnsafe.assignSequenceNumber(
              writeDataManifest(
                  "targets", compactionId, spec, baseSequenceNumber, plan.targetFiles()),
              baseSequenceNumber);
    }

    return sharedTargetManifest;
  }

  private ManifestFile writeDataManifest(
      String name,
      long manifestSnapshotId,
      PartitionSpec spec,
      long sequenceNumber,
      List<DataFile> files) {
    String path = newMetadataPath("snaprewrite-" + name, "avro");
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(base.formatVersion(), spec, io.newOutputFile(path), manifestSnapshotId);
    try {
      for (DataFile file : files) {
        long dataSequenceNumber = dataSequenceNumber(file, sequenceNumber);
        writer.existing(file, manifestSnapshotId, dataSequenceNumber, dataSequenceNumber);
      }
    } finally {
      close(writer);
    }

    writtenPaths.add(path);
    return writer.toManifestFile();
  }

  private long dataSequenceNumber(DataFile file, long snapshotSequenceNumber) {
    if (stamping == Stamping.SOURCE && file.dataSequenceNumber() != null) {
      return file.dataSequenceNumber();
    }

    return snapshotSequenceNumber;
  }

  private ManifestFile writeDeleteManifest(
      Snapshot original, PartitionSpec spec, long sequenceNumber, List<DeleteFile> files) {
    String path = newMetadataPath("snaprewrite-deletes-" + original.snapshotId(), "avro");
    ManifestWriter<DeleteFile> writer =
        ManifestFiles.writeDeleteManifest(
            base.formatVersion(), spec, io.newOutputFile(path), original.snapshotId());
    try {
      for (DeleteFile file : files) {
        writer.existing(file, original.snapshotId(), sequenceNumber, sequenceNumber);
      }
    } finally {
      close(writer);
    }

    writtenPaths.add(path);
    return writer.toManifestFile();
  }

  /**
   * Recomputes the snapshot's totals for the new layout.
   *
   * <p>{@code total-records} counts records in live data files, so it is a property of the layout
   * rather than of the state and legitimately changes: a rewritten snapshot holds the whole
   * compaction and masks most of it. What is preserved is the set of rows a scan returns, which
   * only a scan can check.
   *
   * <p>Per-commit {@code added-*} and {@code deleted-*} fields are dropped rather than fabricated.
   * They described the original transaction, and the rewritten snapshot is not the result of that
   * transaction applied to its parent.
   */
  private Map<String, String> summary(
      Snapshot original, List<DataFile> dataFiles, List<DeleteFile> deletes) {
    long records = 0;
    long size = 0;
    for (DataFile file : dataFiles) {
      records += file.recordCount();
      size += file.fileSizeInBytes();
    }

    long positions = 0;
    for (DeleteFile file : deletes) {
      positions += file.recordCount();
      size += file.fileSizeInBytes();
    }

    Map<String, String> summary = Maps.newHashMap();
    summary.put(SnapshotSummary.TOTAL_DATA_FILES_PROP, String.valueOf(dataFiles.size()));
    summary.put(SnapshotSummary.TOTAL_DELETE_FILES_PROP, String.valueOf(deletes.size()));
    summary.put(SnapshotSummary.TOTAL_RECORDS_PROP, String.valueOf(records));
    summary.put(SnapshotSummary.TOTAL_FILE_SIZE_PROP, String.valueOf(size));
    summary.put(SnapshotSummary.TOTAL_POS_DELETES_PROP, String.valueOf(positions));
    summary.put(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0");

    // Everything needed to put this snapshot back the way it was: the manifest list it used to
    // point
    // at, and the summary it used to carry. A rewrite is reversible while the old files survive,
    // and
    // that is worth preserving even though most of these fields describe a layout that no longer
    // exists here.
    summary.put(SnapshotRewriteRestore.ORIGINAL_MANIFEST_LIST, original.manifestListLocation());

    // Partition statistics are dropped because a rewrite makes them wrong, but a restore has to be
    // able to put them back, and they live in table metadata rather than in the snapshot. Recording
    // them here keeps undo self-contained: everything needed is in the snapshot that replaced them.
    PartitionStatisticsFile detached = detachedPartitionStats.get(original.snapshotId());
    if (detached != null) {
      summary.put(SnapshotRewriteRestore.DETACHED_PARTITION_STATS_PATH, detached.path());
      summary.put(
          SnapshotRewriteRestore.DETACHED_PARTITION_STATS_SIZE,
          String.valueOf(detached.fileSizeInBytes()));
    }

    if (original.summary() != null) {
      for (Map.Entry<String, String> entry : original.summary().entrySet()) {
        summary.put(SnapshotRewriteRestore.ORIGINAL_PREFIX + entry.getKey(), entry.getValue());
      }
    }

    return summary;
  }

  private String newMetadataPath(String name, String extension) {
    String configured = base.property(TableProperties.WRITE_METADATA_LOCATION, null);
    String directory = configured != null ? configured : base.location() + "/metadata";
    return directory + "/" + name + "-" + UUID.randomUUID() + "." + extension;
  }

  private void close(ManifestWriter<?> writer) {
    try {
      writer.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
