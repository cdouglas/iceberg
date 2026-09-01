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
package org.apache.iceberg;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * The single escape hatch used by snapshot rewriting to reach package-private Iceberg state.
 *
 * <p>Snapshot rewriting is "unsafe" in the Rust sense: it asserts soundness rather than satisfying
 * what the framework can prove. Iceberg deliberately offers no way to replace a committed snapshot,
 * because a snapshot's manifest list, sequence numbers, and file provenance are supposed to be
 * immutable once written. A rewrite violates that on purpose, licensed by a compaction map proving
 * the relocated rows are the same rows.
 *
 * <p>Every such violation is confined to this class, one method per escape hatch, so the audit
 * surface for "what did we assert that Iceberg would otherwise check" is a single file. Nothing in
 * {@code org.apache.iceberg.snaprewrite} touches package-private state directly.
 *
 * <p>This class is not part of the public API and carries no compatibility guarantee.
 */
public class SnapshotRewriteUnsafe {

  private SnapshotRewriteUnsafe() {}

  /**
   * Builds a snapshot with a caller-chosen identity.
   *
   * <p><b>Asserted:</b> the caller preserves {@code snapshotId}, {@code parentId}, {@code
   * sequenceNumber}, and {@code timestampMillis} from the snapshot being rewritten, so the
   * rewritten history remains addressable by the same ids and timestamps. Only {@code manifestList}
   * and the summary totals may change.
   *
   * <p>{@code firstRowId} and {@code addedRows} are carried through from the snapshot being
   * rewritten. A rewritten snapshot must not disturb row lineage: the rows it holds are the same
   * rows, and under v3 their ids derive from the data files' own {@code first_row_id}, which the
   * rewrite does not change. Anything else here would renumber history.
   */
  public static Snapshot newSnapshot(
      long sequenceNumber,
      long snapshotId,
      Long parentId,
      long timestampMillis,
      String operation,
      Map<String, String> summary,
      Integer schemaId,
      String manifestList,
      Long firstRowId,
      Long addedRows) {
    return new BaseSnapshot(
        sequenceNumber,
        snapshotId,
        parentId,
        timestampMillis,
        operation,
        summary,
        schemaId,
        manifestList,
        firstRowId,
        addedRows,
        null);
  }

  /**
   * Rebuilds table metadata with a replaced snapshot list.
   *
   * <p>{@code TableMetadata.Builder} cannot express this: {@link
   * TableMetadata.Builder#addSnapshot(Snapshot)} rejects a duplicate snapshot id and requires a
   * strictly increasing sequence number. This uses the package-private constructor instead, which
   * is the same path {@code TableMetadataParser.fromJson} takes, so the result survives a JSON
   * round trip without re-running builder validation.
   *
   * <p><b>Asserted:</b> {@code snapshots} contains an entry for every id referenced by the base
   * metadata's refs and snapshot log, each with its original sequence number.
   *
   * <p>The result has a null metadata file location and no pending changes: it is an uncommitted
   * shadow, suitable for reading but not yet written anywhere.
   */
  public static TableMetadata replaceSnapshots(TableMetadata base, List<Snapshot> snapshots) {
    return new TableMetadata(
        null,
        base.formatVersion(),
        base.uuid(),
        base.location(),
        base.lastSequenceNumber(),
        base.lastUpdatedMillis(),
        base.lastColumnId(),
        base.currentSchemaId(),
        base.schemas(),
        base.defaultSpecId(),
        base.specs(),
        base.lastAssignedPartitionId(),
        base.defaultSortOrderId(),
        base.sortOrders(),
        base.properties(),
        base.currentSnapshot() == null ? -1 : base.currentSnapshot().snapshotId(),
        snapshots,
        null,
        base.snapshotLog(),
        base.previousFiles(),
        base.refs(),
        base.statisticsFiles(),
        base.partitionStatisticsFiles(),
        base.nextRowId(),
        base.encryptionKeys(),
        ImmutableList.of());
  }

  /**
   * Writes a manifest list for a rewritten snapshot.
   *
   * <p><b>Asserted:</b> every manifest in {@code manifests} was written by this rewrite and stamps
   * its entries at {@code sequenceNumber}, so that within the resulting snapshot every delete file
   * applies to every data file it references. See {@code SnapshotRewriteWriter} for the stamping
   * rule and why a violation fails silently in v2.
   */
  public static void writeManifestList(
      int formatVersion,
      OutputFile out,
      long snapshotId,
      Long parentSnapshotId,
      long sequenceNumber,
      Long firstRowId,
      List<ManifestFile> manifests)
      throws IOException {
    try (ManifestListWriter writer =
        ManifestLists.write(
            formatVersion, out, snapshotId, parentSnapshotId, sequenceNumber, firstRowId)) {
      writer.addAll(manifests);
    }
  }

  /**
   * Indexes the delete files of a snapshot exactly as scan planning would.
   *
   * <p>{@link DeleteFileIndex} is package-private. Using the real index rather than reimplementing
   * its matching rules is deliberate: the live sets computed here must agree with what a reader
   * will see, or the rewrite reconstructs a state that never existed.
   *
   * <p><b>Asserted:</b> the result is used only to compute live row sets during planning, never to
   * validate a commit.
   */
  public static DeleteIndex deleteIndex(
      FileIO io, List<ManifestFile> deleteManifests, Map<Integer, PartitionSpec> specsById) {
    DeleteFileIndex index =
        DeleteFileIndex.builderFor(io, deleteManifests).specsById(specsById).build();
    return index::forDataFile;
  }

  /** The subset of {@link DeleteFileIndex} that snapshot rewriting needs. */
  @FunctionalInterface
  public interface DeleteIndex {
    DeleteFile[] forDataFile(DataFile file);
  }
}
