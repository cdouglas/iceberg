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
import java.util.Set;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * A materialized rewrite that has not been committed.
 *
 * <p>Every file exists and the metadata is complete, but the source table is untouched: {@link
 * #asTable()} reads the result, {@link #report()} says what it would save, and {@link
 * #commit(TableOperations)} is a separate, explicit step.
 */
public class SnapshotRewriteResult {
  private final String name;
  private final TableMetadata base;
  private final TableMetadata rewritten;
  private final FileIO io;
  private final SnapshotRewritePlan plan;
  private final Map<String, DataFile> resurrected;
  private final Map<String, DeleteFile> deleteFiles;
  private final List<String> metadataPaths;

  SnapshotRewriteResult(
      String name,
      TableMetadata base,
      TableMetadata rewritten,
      FileIO io,
      SnapshotRewritePlan plan,
      Map<String, DataFile> resurrected,
      Map<String, DeleteFile> deleteFiles,
      List<String> metadataPaths) {
    this.name = name;
    this.base = base;
    this.rewritten = rewritten;
    this.io = io;
    this.plan = plan;
    this.resurrected = ImmutableMap.copyOf(resurrected);
    this.deleteFiles = ImmutableMap.copyOf(deleteFiles);
    this.metadataPaths = ImmutableList.copyOf(metadataPaths);
  }

  /**
   * The rewritten metadata. Uncommitted: it has no metadata file location and no pending changes.
   */
  public TableMetadata metadata() {
    return rewritten;
  }

  public SnapshotRewritePlan plan() {
    return plan;
  }

  /**
   * A read-only table over the rewritten metadata, for verifying the result before committing it.
   */
  public Table asTable() {
    return new BaseTable(new ShadowTableOperations(rewritten, io), name + "#rewritten");
  }

  public SnapshotRewriteReport report() {
    long resurrectedBytes = 0;
    long resurrectedRows = 0;
    for (DataFile file : resurrected.values()) {
      resurrectedBytes += file.fileSizeInBytes();
      resurrectedRows += file.recordCount();
    }

    long deleteBytes = 0;
    long deletePositions = 0;
    for (DeleteFile file : deleteFiles.values()) {
      deleteBytes += file.fileSizeInBytes();
      deletePositions += file.recordCount();
    }

    long metadataBytes = 0;
    for (String path : metadataPaths) {
      metadataBytes += io.newInputFile(path).getLength();
    }

    long reclaimable = 0;
    for (long size : plan.detachedFiles().values()) {
      reclaimable += size;
    }

    long targetBytes = 0;
    for (DataFile file : plan.targetFiles()) {
      targetBytes += file.fileSizeInBytes();
    }

    return new SnapshotRewriteReport(
        plan.window().size(),
        reclaimable,
        resurrectedBytes,
        resurrected.size(),
        resurrectedRows,
        deleteBytes,
        deleteFiles.size(),
        deletePositions,
        metadataBytes,
        targetBytes,
        plan.targetRows(),
        false);
  }

  /**
   * Replaces the window's snapshots in the source table.
   *
   * <p>This is the one destructive step. It swaps the whole metadata document, which no REST
   * catalog can express -- there is no metadata update for replacing a snapshot -- so it works only
   * against operations that accept a whole-metadata commit.
   */
  public void commit(TableOperations ops) {
    ops.commit(base, rewritten);
  }

  /**
   * Deletes the files the rewrite detached, withholding any that are still reachable.
   *
   * <p>Expiring snapshots does not clean these up: it deletes only what the snapshots being expired
   * reach, and after a rewrite the old files are reached by no snapshot at all.
   *
   * <p>Reachability is evaluated now, not as of the rewrite. The table's metadata log still points
   * at documents whose snapshots describe the old layout, and how many of those are retained
   * changes with every commit, so a file safe to delete today was not safe yesterday. Anything
   * still reachable is reported rather than deleted.
   */
  public ReclaimResult reclaim(TableOperations ops) {
    TableMetadata current = ops.refresh();
    Set<String> candidates = Sets.newLinkedHashSet(plan.detachedFiles().keySet());
    candidates.addAll(detachedStatisticsPaths());

    Set<String> retained = Sets.newHashSet();
    collectStatistics(current, retained);
    for (Snapshot snapshot : current.snapshots()) {
      collect(snapshot, current, retained);
    }

    for (TableMetadata.MetadataLogEntry entry : current.previousFiles()) {
      try {
        TableMetadata previous = TableMetadataParser.read(io, entry.file());
        collectStatistics(previous, retained);
        for (Snapshot snapshot : previous.snapshots()) {
          collect(snapshot, previous, retained);
        }
      } catch (RuntimeException e) {
        // An unreadable metadata document is not proof that nothing references these files.
        // Withhold
        // the whole detached set rather than risk deleting something still reachable.
        return new ReclaimResult(ImmutableList.of(), candidates);
      }
    }

    List<String> deleted = Lists.newArrayList();
    Set<String> withheld = Sets.newHashSet();
    for (String path : candidates) {
      if (retained.contains(path)) {
        withheld.add(path);
      } else {
        io.deleteFile(path);
        deleted.add(path);
      }
    }

    return new ReclaimResult(deleted, withheld);
  }

  /**
   * Statistics files the rewrite detached, read back from the snapshots that replaced them.
   *
   * <p>These describe the old layout as surely as its manifests do, so they belong to the set
   * reclaim deletes. Restoring re-attaches them, which is why they survive until reclaim and not
   * past it.
   */
  private List<String> detachedStatisticsPaths() {
    List<String> paths = Lists.newArrayList();
    for (Snapshot snapshot : rewritten.snapshots()) {
      if (SnapshotRewriteRestore.isRewritten(snapshot)) {
        String path = snapshot.summary().get(SnapshotRewriteRestore.DETACHED_PARTITION_STATS_PATH);
        if (path != null) {
          paths.add(path);
        }
      }
    }

    return paths;
  }

  private void collect(Snapshot snapshot, TableMetadata metadata, Set<String> paths) {
    Map<String, Long> sizes = Maps.newHashMap();
    SnapshotFiles.collect(snapshot, io, metadata.specsById(), sizes);
    paths.addAll(sizes.keySet());
  }

  /**
   * Adds the statistics files a metadata document references.
   *
   * <p>Statistics hang off table metadata rather than off a snapshot, so walking snapshots does not
   * find them. Without this a retained metadata-log entry that still names a detached statistics
   * file would not count as a reference to it.
   */
  private void collectStatistics(TableMetadata metadata, Set<String> paths) {
    for (StatisticsFile file : metadata.statisticsFiles()) {
      paths.add(file.path());
    }

    for (PartitionStatisticsFile file : metadata.partitionStatisticsFiles()) {
      paths.add(file.path());
    }
  }

  /** Deletes everything this rewrite wrote, leaving the source table as it was. */
  public void discard() {
    for (String path : metadataPaths) {
      io.deleteFile(path);
    }

    for (DataFile file : resurrected.values()) {
      io.deleteFile(file.location());
    }

    for (DeleteFile file : deleteFiles.values()) {
      io.deleteFile(file.location());
    }
  }

  /** What {@link #reclaim(TableOperations)} deleted, and what it declined to delete. */
  public static class ReclaimResult {
    private final List<String> deleted;
    private final Set<String> withheld;

    ReclaimResult(List<String> deleted, Set<String> withheld) {
      this.deleted = ImmutableList.copyOf(deleted);
      this.withheld = Sets.newHashSet(withheld);
    }

    public List<String> deleted() {
      return deleted;
    }

    /** Detached files still reachable from a retained metadata log entry. */
    public Set<String> withheld() {
      return withheld;
    }
  }
}
