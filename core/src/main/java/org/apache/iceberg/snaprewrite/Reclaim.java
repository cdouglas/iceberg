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
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * Deletes files nothing reaches any more: shared by a rewrite and by a reconstruction, which each
 * leave behind the layout they replaced.
 *
 * <p>Reachability is evaluated when this runs, not when the files were detached. The table's
 * metadata log still points at documents whose snapshots describe the replaced layout, and how many
 * of those are retained changes with every commit, so a file safe to delete today was not safe
 * yesterday. Anything still reachable is reported rather than deleted.
 */
class Reclaim {
  private Reclaim() {}

  static SnapshotRewriteResult.ReclaimResult unreachable(
      TableOperations ops, FileIO io, Set<String> candidates) {
    TableMetadata current = ops.refresh();

    Set<String> retained = Sets.newHashSet();
    collectStatistics(current, retained);
    for (Snapshot snapshot : current.snapshots()) {
      collect(snapshot, current, io, retained);
    }

    for (TableMetadata.MetadataLogEntry entry : current.previousFiles()) {
      try {
        TableMetadata previous = TableMetadataParser.read(io, entry.file());
        collectStatistics(previous, retained);
        for (Snapshot snapshot : previous.snapshots()) {
          collect(snapshot, previous, io, retained);
        }
      } catch (RuntimeException e) {
        // An unreadable metadata document is not proof that nothing references these files.
        // Withhold the whole set rather than risk deleting something still reachable.
        return new SnapshotRewriteResult.ReclaimResult(ImmutableList.of(), candidates);
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

    return new SnapshotRewriteResult.ReclaimResult(deleted, withheld);
  }

  private static void collect(
      Snapshot snapshot, TableMetadata metadata, FileIO io, Set<String> paths) {
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
  private static void collectStatistics(TableMetadata metadata, Set<String> paths) {
    for (StatisticsFile file : metadata.statisticsFiles()) {
      paths.add(file.path());
    }

    for (PartitionStatisticsFile file : metadata.partitionStatisticsFiles()) {
      paths.add(file.path());
    }
  }
}
