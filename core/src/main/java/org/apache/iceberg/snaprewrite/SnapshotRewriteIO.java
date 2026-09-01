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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.deletes.PositionDeleteIndex;

/**
 * The data-touching operations snapshot rewriting cannot perform itself.
 *
 * <p>{@code iceberg-core} has no record reader, so both reading v2 position delete files and
 * copying rows between data files must be delegated. {@code iceberg-data} provides the generic
 * implementation; a Spark implementation would slot in the same way for scale.
 */
public interface SnapshotRewriteIO {

  /**
   * Whether {@link #resurrect} preserves each row's {@code _row_id}.
   *
   * <p>Recovering a row means writing it into a new file, and under v3 its identity survives only
   * if the id is written out per row. An implementation that cannot do that is still usable on v2,
   * where there is no lineage to lose; the planner refuses a v3 window that would recover rows
   * rather than letting it through with silently renumbered rows.
   *
   * <p>Defaults to false so that an implementation has to claim the capability deliberately.
   */
  default boolean preservesRowLineage() {
    return false;
  }

  /**
   * Loads the positions deleted in a data file by the given delete files.
   *
   * @param deleteFiles delete files that apply to the data file
   * @param dataFilePath the data file whose deleted positions to load
   * @return an index of deleted positions, empty if none apply
   */
  PositionDeleteIndex loadPositionDeletes(
      Iterable<DeleteFile> deleteFiles, CharSequence dataFilePath);

  /**
   * Copies rows out of the original layout into a new data file.
   *
   * <p>The rows must be read directly from their source files, ignoring any delete files that
   * apply: the request names rows that are dead in the current snapshot, and the point is to
   * recover them. The output must contain exactly {@link ResurrectionRequest#sources()} in that
   * order, written to {@link ResurrectionRequest#outputPath()}.
   */
  DataFile resurrect(ResurrectionRequest request);

  /**
   * Writes the position deletes one rewritten snapshot applies to one partition.
   *
   * <p>Records must be sorted by data file path, then by position, and written to {@link
   * PositionDeleteRequest#outputPath()}.
   */
  DeleteFile writePositionDeletes(PositionDeleteRequest request);
}
