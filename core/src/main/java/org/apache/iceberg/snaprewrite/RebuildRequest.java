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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * A request to write one data file of a reconstructed layout.
 *
 * <p>Everything about the file is decided by the reconstruction: its path, its rows in order, and
 * its {@code first_row_id}. The schema includes the lineage columns only when some row's lineage
 * cannot be derived from the file, so a file whose ids and sequence numbers all derive comes back
 * without them, as an append would have written it.
 */
public class RebuildRequest {
  private final PartitionSpec spec;
  private final StructLike partition;
  private final Schema schema;
  private final String outputPath;
  private final long firstRowId;
  private final List<Record> rows;

  RebuildRequest(
      PartitionSpec spec,
      StructLike partition,
      Schema schema,
      String outputPath,
      long firstRowId,
      List<Record> rows) {
    this.spec = spec;
    this.partition = partition;
    this.schema = schema;
    this.outputPath = outputPath;
    this.firstRowId = firstRowId;
    this.rows = ImmutableList.copyOf(rows);
  }

  public PartitionSpec spec() {
    return spec;
  }

  /** The partition every row belongs to, or null when the spec is unpartitioned. */
  public StructLike partition() {
    return partition;
  }

  /** The schema to write, with the lineage columns appended only if any row needs them. */
  public Schema schema() {
    return schema;
  }

  public String outputPath() {
    return outputPath;
  }

  /** The {@code first_row_id} the written file must carry, whether or not ids are written out. */
  public long firstRowId() {
    return firstRowId;
  }

  /** The rows to write, in order: position {@code i} of the file holds {@code rows().get(i)}. */
  public List<Record> rows() {
    return rows;
  }
}
