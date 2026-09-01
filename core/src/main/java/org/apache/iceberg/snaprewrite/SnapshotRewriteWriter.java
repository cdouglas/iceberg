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
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
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

/**
 * Writes the manifests, manifest lists, and snapshots of a rewrite.
 *
 * <h2>Sequence number stamping</h2>
 *
 * <p>Every file in a rewritten snapshot -- the compaction's data files, the resurrection files, and
 * the delete files -- is stamped at that snapshot's own sequence number.
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
 * rows reappearing in a time-travel read. Stamping everything at the snapshot's own sequence number
 * makes the comparison an equality, which passes, and makes each rewritten snapshot internally
 * uniform: it reads as though every file it holds had been added by it.
 */
class SnapshotRewriteWriter {

  /**
   * How data files are stamped in a rewritten snapshot.
   *
   * <p>{@link #OWN} is the only correct setting. {@link #SOURCE} exists so tests can build the
   * mistake on purpose and confirm the oracle catches it: because v2 drops inert deletes without
   * complaint, a suite that only ever exercises the correct stamping cannot tell whether it would
   * notice the incorrect one.
   */
  enum Stamping {
    OWN,
    SOURCE
  }

  private final TableMetadata base;
  private final FileIO io;
  private final SnapshotRewritePlan plan;
  private final Map<String, DataFile> resurrected;
  private final Map<String, DeleteFile> deleteFiles;
  private final Stamping stamping;
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

    return SnapshotRewriteUnsafe.replaceSnapshots(base, snapshots);
  }

  /** Metadata files this writer created, for reclaim accounting and for cleanup on failure. */
  List<String> writtenPaths() {
    return ImmutableList.copyOf(writtenPaths);
  }

  private Snapshot rewriteSnapshot(Snapshot original) {
    SnapshotRewritePlan.RewrittenSnapshot rewritten = plan.forSnapshot(original.snapshotId());
    long sequenceNumber = original.sequenceNumber();
    PartitionSpec spec = base.spec();

    List<DataFile> dataFiles = Lists.newArrayList(plan.targetFiles());
    for (String path : rewritten.resurrectionPaths()) {
      dataFiles.add(resurrected.get(path));
    }

    List<ManifestFile> manifests = Lists.newArrayList();
    manifests.add(writeDataManifest(original, spec, sequenceNumber, dataFiles));

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

  private ManifestFile writeDataManifest(
      Snapshot original, PartitionSpec spec, long sequenceNumber, List<DataFile> files) {
    String path = newMetadataPath("snaprewrite-data-" + original.snapshotId(), "avro");
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(
            base.formatVersion(), spec, io.newOutputFile(path), original.snapshotId());
    try {
      for (DataFile file : files) {
        long dataSequenceNumber = dataSequenceNumber(file, sequenceNumber);
        writer.existing(file, original.snapshotId(), dataSequenceNumber, dataSequenceNumber);
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

    // Everything needed to put this snapshot back the way it was: the manifest list it used to point
    // at, and the summary it used to carry. A rewrite is reversible while the old files survive, and
    // that is worth preserving even though most of these fields describe a layout that no longer
    // exists here.
    summary.put(SnapshotRewriteRestore.ORIGINAL_MANIFEST_LIST, original.manifestListLocation());
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
