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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * A request to materialize rows that died inside the rewrite window.
 *
 * <p>Rows deleted by a transaction in the window are absent from the compaction's output and from
 * every other resurrection file, so reconstructing the state before that transaction requires
 * copying them out of the original layout into a new data file.
 *
 * <p>The output path is chosen by the planner rather than the writer, so the plan can resolve row
 * locations before any data is read. That is what lets {@code plan()} be complete and side-effect
 * free while {@code materialize()} only fills in file metrics.
 */
public class ResurrectionRequest {
  private final long snapshotId;
  private final PartitionSpec spec;
  private final StructLike partition;
  private final Schema schema;
  private final List<RowRef> sources;
  private final Map<String, Long> sourceFirstRowIds;
  private final String outputPath;

  ResurrectionRequest(
      long snapshotId,
      PartitionSpec spec,
      StructLike partition,
      Schema schema,
      List<RowRef> sources,
      Map<String, Long> sourceFirstRowIds,
      String outputPath) {
    this.snapshotId = snapshotId;
    this.spec = spec;
    this.partition = partition;
    this.schema = schema;
    this.sources = ImmutableList.copyOf(sources);
    this.sourceFirstRowIds = ImmutableMap.copyOf(sourceFirstRowIds);
    this.outputPath = outputPath;
  }

  /** The snapshot whose inverse creates this file: the transaction that deleted these rows. */
  public long snapshotId() {
    return snapshotId;
  }

  public PartitionSpec spec() {
    return spec;
  }

  /** The partition all of these rows belong to, or null when the spec is unpartitioned. */
  public StructLike partition() {
    return partition;
  }

  public Schema schema() {
    return schema;
  }

  /**
   * The rows to copy, in the order they must appear in the output file.
   *
   * <p>Position {@code i} of the resurrection file holds {@code sources.get(i)}, and the plan's row
   * locations depend on that correspondence.
   */
  public List<RowRef> sources() {
    return sources;
  }

  /**
   * The {@code first_row_id} of each source file, by path, where the table assigns them.
   *
   * <p>Needed to read a source row's identity at all: a row id is either written into the file or
   * derived from this value plus the row's offset, and the reader needs it supplied as a constant
   * either way. Empty when the table does not track row lineage.
   */
  public Map<String, Long> sourceFirstRowIds() {
    return sourceFirstRowIds;
  }

  public String outputPath() {
    return outputPath;
  }

  public int rowCount() {
    return sources.size();
  }
}
