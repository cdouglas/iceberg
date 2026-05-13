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

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.SparkSession;

/**
 * Spark listener that aggregates per-stage I/O metrics over a {@link #startTracking()} / {@link
 * #stopTracking()} bracket. Stages started outside the bracket are ignored so we don't
 * cross-contaminate with warmup runs or unrelated Spark activity.
 *
 * <p>Returned numbers are best-effort: stage timings come from {@code executorRunTime} (across
 * tasks, so they over-count on multi-core local Spark) and bytes are summed across input/output
 * metrics. {@link IterationResult#wallClockMs()} remains authoritative — see COMPACT_SPEC.md
 * §"Measurement Per Iteration".
 */
public final class StageMetricsCollector extends SparkListener {

  private volatile boolean tracking = false;
  private final Set<Integer> bracketStageIds = Sets.newConcurrentHashSet();

  private final AtomicLong totalExecutorRunMs = new AtomicLong();
  private final AtomicLong totalBytesRead = new AtomicLong();
  private final AtomicLong totalBytesWritten = new AtomicLong();
  private final AtomicLong totalRecordsRead = new AtomicLong();
  private final AtomicLong totalRecordsWritten = new AtomicLong();
  private final AtomicLong stageCount = new AtomicLong();

  /** Register this collector with the supplied SparkSession. */
  public void attach(SparkSession spark) {
    spark.sparkContext().addSparkListener(this);
  }

  public void detach(SparkSession spark) {
    spark.sparkContext().removeSparkListener(this);
  }

  /** Reset counters and start tracking. Call once per iteration's timed region. */
  public void startTracking() {
    bracketStageIds.clear();
    totalExecutorRunMs.set(0);
    totalBytesRead.set(0);
    totalBytesWritten.set(0);
    totalRecordsRead.set(0);
    totalRecordsWritten.set(0);
    stageCount.set(0);
    tracking = true;
  }

  /** Stop accepting new stage submissions; already-running stages still report on completion. */
  public void stopTracking() {
    tracking = false;
  }

  public long totalExecutorRunMs() {
    return totalExecutorRunMs.get();
  }

  public long totalBytesRead() {
    return totalBytesRead.get();
  }

  public long totalBytesWritten() {
    return totalBytesWritten.get();
  }

  public long totalRecordsRead() {
    return totalRecordsRead.get();
  }

  public long totalRecordsWritten() {
    return totalRecordsWritten.get();
  }

  public long stageCount() {
    return stageCount.get();
  }

  @Override
  public void onStageSubmitted(SparkListenerStageSubmitted event) {
    if (tracking) {
      bracketStageIds.add(event.stageInfo().stageId());
    }
  }

  @Override
  public void onStageCompleted(SparkListenerStageCompleted event) {
    StageInfo info = event.stageInfo();
    if (!bracketStageIds.remove(info.stageId())) {
      return;
    }
    stageCount.incrementAndGet();

    // taskMetrics aggregates across all tasks in the stage.
    long execMs = info.taskMetrics().executorRunTime();
    long bytesIn = info.taskMetrics().inputMetrics().bytesRead();
    long bytesOut = info.taskMetrics().outputMetrics().bytesWritten();
    long recordsIn = info.taskMetrics().inputMetrics().recordsRead();
    long recordsOut = info.taskMetrics().outputMetrics().recordsWritten();

    totalExecutorRunMs.addAndGet(execMs);
    totalBytesRead.addAndGet(bytesIn);
    totalBytesWritten.addAndGet(bytesOut);
    totalRecordsRead.addAndGet(recordsIn);
    totalRecordsWritten.addAndGet(recordsOut);
  }
}
