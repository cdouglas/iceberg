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
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRewriteUnsafe;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * Puts rewritten snapshots back the way they were.
 *
 * <p>A rewrite is reversible for as long as the layout it detached still exists: each rewritten
 * snapshot records the manifest list it used to point at, and restoring it is a matter of pointing
 * back. Nothing needs to be read or copied, and the restored snapshot is byte-identical to the
 * original -- same manifest list, same summary, same identity.
 *
 * <p>One thing does have to be put back rather than pointed at: partition statistics, which a
 * rewrite drops because they describe a layout it replaced. Those live in table metadata rather
 * than in a snapshot, so the rewrite records each one in the snapshot that replaced it and this
 * re-attaches them. Undo stays self-contained -- everything needed to reverse a rewrite is in the
 * snapshots the rewrite wrote.
 *
 * <p>That window closes when {@link SnapshotRewriteResult#reclaim} deletes the old files. Before
 * then this is the undo button, which is what makes committing a rewrite a decision rather than a
 * commitment.
 */
public class SnapshotRewriteRestore {
  static final String ORIGINAL_MANIFEST_LIST = "snapshot-rewritten-from";
  static final String ORIGINAL_PREFIX = "snapshot-rewritten-from.";

  // Deliberately outside ORIGINAL_PREFIX. Everything under that prefix is replayed verbatim into
  // the
  // restored summary, and these two keys describe the rewrite rather than the snapshot it replaced.
  static final String DETACHED_PARTITION_STATS_PATH = "snapshot-rewrite-detached-partition-stats";
  static final String DETACHED_PARTITION_STATS_SIZE =
      "snapshot-rewrite-detached-partition-stats-size-bytes";

  private SnapshotRewriteRestore() {}

  /** Whether this snapshot was produced by a rewrite and records how to undo it. */
  public static boolean isRewritten(Snapshot snapshot) {
    return snapshot.summary() != null && snapshot.summary().containsKey(ORIGINAL_MANIFEST_LIST);
  }

  /**
   * Returns metadata with every rewritten snapshot restored to its original form.
   *
   * <p>Uncommitted, like a rewrite's own output: the caller decides whether to commit it.
   *
   * @throws IllegalStateException if no snapshot in the metadata was rewritten
   */
  public static TableMetadata restore(TableMetadata base) {
    List<Snapshot> restored = Lists.newArrayList();
    List<PartitionStatisticsFile> partitionStats =
        Lists.newArrayList(base.partitionStatisticsFiles());
    Set<Long> described = Sets.newHashSet();
    for (PartitionStatisticsFile file : partitionStats) {
      described.add(file.snapshotId());
    }

    int count = 0;
    for (Snapshot snapshot : base.snapshots()) {
      if (isRewritten(snapshot)) {
        restored.add(original(snapshot));
        count += 1;

        PartitionStatisticsFile detached = detachedPartitionStats(snapshot);
        if (detached != null && described.add(snapshot.snapshotId())) {
          partitionStats.add(detached);
        }
      } else {
        restored.add(snapshot);
      }
    }

    if (count == 0) {
      throw new IllegalStateException("No rewritten snapshots to restore");
    }

    return SnapshotRewriteUnsafe.replaceSnapshots(
        base, restored, base.statisticsFiles(), partitionStats);
  }

  /**
   * The partition statistics a rewrite detached from this snapshot, or null if it had none.
   *
   * <p>Reconstructed from the summary rather than read: a {@code PartitionStatisticsFile} is three
   * fields, and the snapshot id is the snapshot's own, so recording the path and size is enough to
   * rebuild it exactly. Whether the file itself still exists is a separate question -- reclaim
   * deletes it, which is the same point at which pointing back at a manifest list stops working.
   */
  private static PartitionStatisticsFile detachedPartitionStats(Snapshot rewritten) {
    String path = rewritten.summary().get(DETACHED_PARTITION_STATS_PATH);
    String size = rewritten.summary().get(DETACHED_PARTITION_STATS_SIZE);
    if (path == null || size == null) {
      return null;
    }

    return ImmutableGenericPartitionStatisticsFile.builder()
        .snapshotId(rewritten.snapshotId())
        .path(path)
        .fileSizeInBytes(Long.parseLong(size))
        .build();
  }

  /** How many snapshots {@link #restore} would put back. */
  public static int rewrittenCount(TableMetadata base) {
    int count = 0;
    for (Snapshot snapshot : base.snapshots()) {
      if (isRewritten(snapshot)) {
        count += 1;
      }
    }

    return count;
  }

  private static Snapshot original(Snapshot rewritten) {
    Map<String, String> summary = Maps.newHashMap();
    for (Map.Entry<String, String> entry : rewritten.summary().entrySet()) {
      if (entry.getKey().startsWith(ORIGINAL_PREFIX)) {
        summary.put(entry.getKey().substring(ORIGINAL_PREFIX.length()), entry.getValue());
      }
    }

    return SnapshotRewriteUnsafe.newSnapshot(
        rewritten.sequenceNumber(),
        rewritten.snapshotId(),
        rewritten.parentId(),
        rewritten.timestampMillis(),
        rewritten.operation(),
        summary,
        rewritten.schemaId(),
        rewritten.summary().get(ORIGINAL_MANIFEST_LIST),
        rewritten.firstRowId(),
        rewritten.addedRows());
  }
}
