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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Mutable accumulator for one timed iteration. Lifecycle: the timed region constructs it, the Spark
 * listener populates {@link #stageMs}/{@link #filesRead}/{@link #inputDataBytes}/etc., and {@link
 * RunMain} hands it to {@link #appendJsonl(Writer)}.
 *
 * <p>Fields and column order match the JSON schema in {@code COMPACT_SPEC.md} §"Measurement Per
 * Iteration". Treatment iterations populate the {@code read_dv}/{@code remap}/{@code write_dv}
 * stage entries in addition to the baseline-shared {@code plan}/{@code scan_write}/{@code commit}.
 */
public final class IterationResult {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    // Without this, ObjectMapper.writeValue(writer, ...) closes the underlying writer after each
    // call, which breaks the JSONL append pattern.
    MAPPER.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
  }

  private final String variant;
  private final int kLateTxDeletes;
  private final int iteration;
  private final boolean warmup;

  private long wallClockMs;
  private final Map<String, Long> stageMs = new LinkedHashMap<>();
  private long snapshotIdAfter;
  private long inputDataBytes;
  private long outputDataBytes;
  private int filesRead;
  private int filesWritten;
  private int compactionMapRuns;
  private int snPlusOneRuns;
  private boolean valid = true;
  private String invalidReason;

  public IterationResult(String variant, int kLateTxDeletes, int iteration, boolean warmup) {
    this.variant = variant;
    this.kLateTxDeletes = kLateTxDeletes;
    this.iteration = iteration;
    this.warmup = warmup;
  }

  public String variant() {
    return variant;
  }

  public int kLateTxDeletes() {
    return kLateTxDeletes;
  }

  public int iteration() {
    return iteration;
  }

  public boolean warmup() {
    return warmup;
  }

  public long wallClockMs() {
    return wallClockMs;
  }

  public void wallClockMs(long value) {
    this.wallClockMs = value;
  }

  public Map<String, Long> stageMs() {
    return stageMs;
  }

  public void stageMs(String stage, long value) {
    stageMs.put(stage, value);
  }

  public long snapshotIdAfter() {
    return snapshotIdAfter;
  }

  public void snapshotIdAfter(long value) {
    this.snapshotIdAfter = value;
  }

  public long inputDataBytes() {
    return inputDataBytes;
  }

  public void inputDataBytes(long value) {
    this.inputDataBytes = value;
  }

  public long outputDataBytes() {
    return outputDataBytes;
  }

  public void outputDataBytes(long value) {
    this.outputDataBytes = value;
  }

  public int filesRead() {
    return filesRead;
  }

  public void filesRead(int value) {
    this.filesRead = value;
  }

  public int filesWritten() {
    return filesWritten;
  }

  public void filesWritten(int value) {
    this.filesWritten = value;
  }

  public int compactionMapRuns() {
    return compactionMapRuns;
  }

  public void compactionMapRuns(int value) {
    this.compactionMapRuns = value;
  }

  public int snPlusOneRuns() {
    return snPlusOneRuns;
  }

  public void snPlusOneRuns(int value) {
    this.snPlusOneRuns = value;
  }

  public boolean valid() {
    return valid;
  }

  public String invalidReason() {
    return invalidReason;
  }

  /** Mark this iteration as invalid (M6 soundness assertion failed). Idempotent. */
  public void invalidate(String reason) {
    this.valid = false;
    this.invalidReason = reason;
  }

  /** Append this iteration as a single JSON object followed by a newline. */
  public void appendJsonl(Writer out) throws IOException {
    Map<String, Object> record = new LinkedHashMap<>();
    record.put("variant", variant);
    record.put("k", kLateTxDeletes);
    record.put("iteration", iteration);
    record.put("warmup", warmup);
    record.put("wall_clock_ms", wallClockMs);
    record.put("stage_ms", stageMs);
    record.put("snapshot_id_after", snapshotIdAfter);
    record.put("input_data_bytes", inputDataBytes);
    record.put("output_data_bytes", outputDataBytes);
    record.put("files_read", filesRead);
    record.put("files_written", filesWritten);
    record.put("compaction_map_runs", compactionMapRuns);
    record.put("sn_plus_one_runs", snPlusOneRuns);
    record.put("valid", valid);
    if (invalidReason != null) {
      record.put("invalid_reason", invalidReason);
    }
    MAPPER.writeValue(out, record);
    out.write('\n');
  }
}
