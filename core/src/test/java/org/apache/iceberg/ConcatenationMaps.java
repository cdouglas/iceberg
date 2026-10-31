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
package org.apache.iceberg;

/**
 * Compaction maps for the metadata-only rewrites in core tests.
 *
 * <p>A direct {@link RewriteFiles} records no map, because only the writer of the new files knows
 * where their rows came from. The fake rewrites in these tests stand for bin-pack compactions, so a
 * test that needs a map states that layout here: each source in order, concatenated into one
 * target.
 */
final class ConcatenationMaps {
  private ConcatenationMaps() {}

  /**
   * Gives {@code rewrite} the map of concatenating {@code sources}, in order, into {@code target}.
   *
   * <p>The map's source snapshot is the table's current snapshot. Its target snapshot is the
   * rewrite's own, which {@link BaseRewriteFiles#setCompactionMap} fills in when it commits.
   *
   * @return {@code rewrite}, for chaining
   */
  static RewriteFiles concatenate(
      Table table, RewriteFiles rewrite, Iterable<DataFile> sources, DataFile target) {
    Snapshot current = table.currentSnapshot();
    long sourceSnapshotId = current != null ? current.snapshotId() : -1L;
    CompactionMapBuilder builder = new CompactionMapBuilder(sourceSnapshotId, sourceSnapshotId);

    long offset = 0;
    for (DataFile source : sources) {
      builder
          .addFileMapping(source.location(), target.location())
          .addRun(0L, offset, source.recordCount());
      offset += source.recordCount();
    }

    ((BaseRewriteFiles) rewrite).setCompactionMap(builder.build());
    return rewrite;
  }
}
