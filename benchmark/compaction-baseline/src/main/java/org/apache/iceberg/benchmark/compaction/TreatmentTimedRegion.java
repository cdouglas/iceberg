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
package org.apache.iceberg.benchmark.compaction;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CompactionConflictDetector;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteConflictInfo;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.actions.SparkCompactionConflictResolver;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes the spec's treatment timed region:
 *
 * <pre>
 * CompactionMap map = CompactionMaps.read(...);            // not timed (setup)
 * DeleteConflictInfo conflicts = buildConflictInfo(...);   // not timed (setup)
 *
 * // START TIMER
 * SparkCompactionConflictResolver resolver = new SparkCompactionConflictResolver(spark, table);
 * List&lt;DeleteFile&gt; newDeletes = resolver.resolve(map, conflicts);
 * RowDelta delta = table.newRowDelta();
 * newDeletes.forEach(delta::addDeletes);
 * delta.commit();
 * // STOP TIMER
 * </pre>
 *
 * <p>Loading the map and computing {@link DeleteConflictInfo} are outside the timer because in
 * production they would have already happened during the (sunk) compaction commit. This class does
 * that prep in {@link #prepare(Table)} and the actual time-bracketed work in {@link
 * #runTimed(Table, Prep, IterationResult)}.
 */
final class TreatmentTimedRegion {

  private static final Logger LOG = LoggerFactory.getLogger(TreatmentTimedRegion.class);

  private final SparkSession spark;
  private final StageMetricsCollector collector;

  TreatmentTimedRegion(SparkSession spark, StageMetricsCollector collector) {
    this.spark = spark;
    this.collector = collector;
  }

  /**
   * Untimed: locate the compaction map, read it, build conflict info. Returns a token to hand back
   * to {@link #runTimed(Table, Prep, IterationResult)}.
   */
  Prep prepare(Table table) {
    String mapPath = locateCompactionMap(table);
    if (mapPath == null) {
      throw new IllegalStateException(
          "Treatment scenario requires a committed compaction map but none was found in any "
              + "manifest of the current snapshot. Was the warehouse restored from a treatment "
              + "tarball?");
    }
    CompactionMap map = CompactionMaps.read(table.io().newInputFile(mapPath));

    long compactSnapshotId = findReplaceSnapshotId(table);
    long startingSnapshotId = table.snapshot(compactSnapshotId).parentId();
    Snapshot currentSnapshot = table.currentSnapshot();
    TableMetadata base = ((HasTableOperations) table).operations().current();

    Set<String> sourceFiles = Sets.newHashSet();
    for (CompactionMap.FileMapping mapping : map.fileMappings()) {
      sourceFiles.add(mapping.sourceFile());
    }
    DeleteConflictInfo conflicts =
        new CompactionConflictDetector(table.io(), base, startingSnapshotId, currentSnapshot)
            .detectConflicts(sourceFiles);
    LOG.info(
        "Treatment prep: map runs={} sourceFiles={} conflictingDeleteFiles={}",
        map.fileMappings().stream().mapToInt(m -> m.runs().size()).sum(),
        sourceFiles.size(),
        conflicts.conflictingDeleteFiles().size());
    return new Prep(map, conflicts, sourceFiles.size());
  }

  /** Run the bracketed region. {@code result} is mutated with all the spec's metrics. */
  void runTimed(Table table, Prep prep, IterationResult result) {
    collector.startTracking();
    long startNanos = System.nanoTime();

    SparkCompactionConflictResolver resolver = new SparkCompactionConflictResolver(spark, table);

    long resolveStart = System.nanoTime();
    List<DeleteFile> newDeletes = resolver.resolve(prep.map, prep.conflicts);
    long resolveEnd = System.nanoTime();

    long commitStart = System.nanoTime();
    RowDelta delta = table.newRowDelta();
    newDeletes.forEach(delta::addDeletes);
    delta.commit();
    long commitEnd = System.nanoTime();

    long endNanos = System.nanoTime();
    collector.stopTracking();

    result.wallClockMs(TimeUnit.NANOSECONDS.toMillis(endNanos - startNanos));
    // resolve() does the read_dv + remap + write_dv work as a single Spark job; we can't cleanly
    // separate the three sub-stages without instrumenting the resolver internally. Roll them up
    // under "scan_write" plus expose the resolver-vs-commit split as additional entries.
    result.stageMs("resolve", TimeUnit.NANOSECONDS.toMillis(resolveEnd - resolveStart));
    result.stageMs("commit", TimeUnit.NANOSECONDS.toMillis(commitEnd - commitStart));
    result.stageMs("scan_write", collector.totalExecutorRunMs());

    result.filesRead(prep.conflicts.conflictingDeleteFiles().size());
    result.filesWritten(newDeletes.size());
    result.inputDataBytes(collector.totalBytesRead());
    result.outputDataBytes(collector.totalBytesWritten());

    table.refresh();
    if (table.currentSnapshot() != null) {
      result.snapshotIdAfter(table.currentSnapshot().snapshotId());
    }
  }

  private static String locateCompactionMap(Table table) {
    Snapshot current = table.currentSnapshot();
    if (current == null) {
      return null;
    }
    for (Snapshot snapshot : table.snapshots()) {
      for (ManifestFile manifest : snapshot.allManifests(table.io())) {
        String location = manifest.compactionMapLocation();
        if (location != null) {
          return location;
        }
      }
    }
    return null;
  }

  private static long findReplaceSnapshotId(Table table) {
    for (Snapshot snapshot : table.snapshots()) {
      if (DataOperations.REPLACE.equals(snapshot.operation())) {
        return snapshot.snapshotId();
      }
    }
    throw new IllegalStateException(
        "No REPLACE snapshot found — treatment warehouse must contain a committed compaction.");
  }

  /** Untimed prep handed from {@link #prepare(Table)} to {@link #runTimed}. */
  static final class Prep {
    private final CompactionMap map;
    private final DeleteConflictInfo conflicts;
    private final int sourceFileCount;

    Prep(CompactionMap map, DeleteConflictInfo conflicts, int sourceFileCount) {
      this.map = map;
      this.conflicts = conflicts;
      this.sourceFileCount = sourceFileCount;
    }

    int compactionMapRuns() {
      int total = 0;
      for (CompactionMap.FileMapping mapping : map.fileMappings()) {
        total += mapping.runs().size();
      }
      return total;
    }

    int conflictingDeleteFileCount() {
      return conflicts.conflictingDeleteFiles().size();
    }

    int sourceFileCount() {
      return sourceFileCount;
    }
  }
}
