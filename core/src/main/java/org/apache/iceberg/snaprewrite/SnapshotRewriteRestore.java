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
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRewriteUnsafe;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Puts rewritten snapshots back the way they were.
 *
 * <p>A rewrite is reversible for as long as the layout it detached still exists: each rewritten
 * snapshot records the manifest list it used to point at, and restoring it is a matter of pointing
 * back. Nothing needs to be read or copied, and the restored snapshot is byte-identical to the
 * original -- same manifest list, same summary, same identity.
 *
 * <p>That window closes when {@link SnapshotRewriteResult#reclaim} deletes the old files. Before
 * then this is the undo button, which is what makes committing a rewrite a decision rather than a
 * commitment.
 */
public class SnapshotRewriteRestore {
  static final String ORIGINAL_MANIFEST_LIST = "snapshot-rewritten-from";
  static final String ORIGINAL_PREFIX = "snapshot-rewritten-from.";

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
    int count = 0;

    for (Snapshot snapshot : base.snapshots()) {
      if (isRewritten(snapshot)) {
        restored.add(original(snapshot));
        count += 1;
      } else {
        restored.add(snapshot);
      }
    }

    if (count == 0) {
      throw new IllegalStateException("No rewritten snapshots to restore");
    }

    return SnapshotRewriteUnsafe.replaceSnapshots(base, restored);
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
