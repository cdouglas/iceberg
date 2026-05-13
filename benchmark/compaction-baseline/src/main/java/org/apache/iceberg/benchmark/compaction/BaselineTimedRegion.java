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

import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;

/**
 * Executes the spec's baseline timed region: {@code SparkActions.get(spark).rewriteDataFiles(table)
 * .execute()}. The rewrite reads pre-compaction data files, applies all deletes (including the
 * late-tx DV), writes compacted output, and commits the snapshot — all part of the single action
 * call.
 *
 * <p>The {@link IterationResult} is populated in-place with wall-clock + listener-derived stage
 * metrics + Iceberg's own returned {@link RewriteDataFiles.Result#rewrittenDataFilesCount()} /
 * {@code addedDataFilesCount()} for files-read/-written. The stage breakdown is best-effort: the
 * planner runs synchronously on the driver and not under a Spark stage, so {@code stage_ms.plan}
 * stays unset; everything Iceberg+Spark does shows up under {@code scan_write}; commit is its own
 * driver-side phase, so we bracket it manually around the action.
 */
final class BaselineTimedRegion {

  private final SparkSession spark;
  private final StageMetricsCollector collector;

  BaselineTimedRegion(SparkSession spark, StageMetricsCollector collector) {
    this.spark = spark;
    this.collector = collector;
  }

  /** Run the timed region and write metrics into {@code result}. */
  void run(Table table, IterationResult result) {
    collector.startTracking();
    long startNanos = System.nanoTime();
    RewriteDataFiles.Result rewrite =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            // The setup workload writes a small number of source files; without this the planner
            // may decide there isn't enough to compact and skip. The runner needs an actual
            // compaction to happen.
            .option(org.apache.iceberg.actions.SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .execute();
    long endNanos = System.nanoTime();
    collector.stopTracking();

    result.wallClockMs(TimeUnit.NANOSECONDS.toMillis(endNanos - startNanos));
    result.stageMs("scan_write", collector.totalExecutorRunMs());
    // Iceberg's RewriteDataFiles result is authoritative for file counts.
    result.filesRead(rewrite.rewrittenDataFilesCount());
    result.filesWritten(rewrite.addedDataFilesCount());
    result.inputDataBytes(collector.totalBytesRead());
    result.outputDataBytes(collector.totalBytesWritten());

    table.refresh();
    if (table.currentSnapshot() != null) {
      result.snapshotIdAfter(table.currentSnapshot().snapshotId());
    }
  }
}
