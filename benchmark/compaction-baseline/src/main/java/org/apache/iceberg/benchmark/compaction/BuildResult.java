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

import java.util.Collections;
import java.util.List;

/**
 * Captured metadata for one warehouse build. Feeds {@code setup_manifest.json}.
 *
 * <p>{@link #compactionMapPath()} and {@link #compactionMapRunCount()} are zero/null for the
 * baseline variant (no compaction is performed). For the treatment variant they reflect the map
 * produced by the offline {@code compact(S_0..S_n)} step.
 */
public final class BuildResult {
  private final String variant;
  private final int kLateTxDeletes;
  private final long seed;
  private final List<Long> snapshotIds;
  private final int dataFileCount;
  private final int deleteFileCount;
  private final long totalLiveRows;
  private final String compactionMapPath;
  private final int compactionMapRunCount;

  public BuildResult(
      String variant,
      int kLateTxDeletes,
      long seed,
      List<Long> snapshotIds,
      int dataFileCount,
      int deleteFileCount,
      long totalLiveRows,
      String compactionMapPath,
      int compactionMapRunCount) {
    this.variant = variant;
    this.kLateTxDeletes = kLateTxDeletes;
    this.seed = seed;
    this.snapshotIds = Collections.unmodifiableList(snapshotIds);
    this.dataFileCount = dataFileCount;
    this.deleteFileCount = deleteFileCount;
    this.totalLiveRows = totalLiveRows;
    this.compactionMapPath = compactionMapPath;
    this.compactionMapRunCount = compactionMapRunCount;
  }

  public String variant() {
    return variant;
  }

  public int kLateTxDeletes() {
    return kLateTxDeletes;
  }

  public long seed() {
    return seed;
  }

  public List<Long> snapshotIds() {
    return snapshotIds;
  }

  public int dataFileCount() {
    return dataFileCount;
  }

  public int deleteFileCount() {
    return deleteFileCount;
  }

  public long totalLiveRows() {
    return totalLiveRows;
  }

  public String compactionMapPath() {
    return compactionMapPath;
  }

  public int compactionMapRunCount() {
    return compactionMapRunCount;
  }
}
