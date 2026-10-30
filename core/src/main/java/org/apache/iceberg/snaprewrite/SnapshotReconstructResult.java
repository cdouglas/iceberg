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
import java.util.Set;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/**
 * A materialized reconstruction that has not been committed.
 *
 * <p>Like a rewrite's result: every file exists and the metadata is complete, the table is
 * untouched, {@link #asTable()} reads the result, and committing it is a separate step.
 */
public class SnapshotReconstructResult {
  private final String name;
  private final TableMetadata base;
  private final TableMetadata reconstructed;
  private final FileIO io;
  private final SnapshotReconstructReport report;
  private final Set<String> replaced;
  private final List<String> written;

  SnapshotReconstructResult(
      String name,
      TableMetadata base,
      TableMetadata reconstructed,
      FileIO io,
      SnapshotReconstructReport report,
      Set<String> replaced,
      List<String> written) {
    this.name = name;
    this.base = base;
    this.reconstructed = reconstructed;
    this.io = io;
    this.report = report;
    this.replaced = ImmutableSet.copyOf(replaced);
    this.written = ImmutableList.copyOf(written);
  }

  /** The reconstructed metadata. Uncommitted. */
  public TableMetadata metadata() {
    return reconstructed;
  }

  /** A read-only table over the reconstructed metadata, for verifying it before committing. */
  public Table asTable() {
    return new BaseTable(new ShadowTableOperations(reconstructed, io), name + "#reconstructed");
  }

  public SnapshotReconstructReport report() {
    return report;
  }

  /**
   * Files of the rewritten layout that nothing references once this is committed: recovery files,
   * the rewrite's deletion vectors, manifests and manifest lists.
   */
  public Set<String> replacedFiles() {
    return replaced;
  }

  /**
   * Replaces the rewritten snapshots with their reconstructions.
   *
   * <p>A whole-metadata swap, like committing a rewrite, and with the same limit: no REST catalog
   * can express it.
   */
  public void commit(TableOperations ops) {
    ops.commit(base, reconstructed);
  }

  /** Deletes the rewritten layout's files, withholding any that are still reachable. */
  public SnapshotRewriteResult.ReclaimResult reclaim(TableOperations ops) {
    return Reclaim.unreachable(ops, io, replaced);
  }

  /** Deletes everything this reconstruction wrote, leaving the table as it was. */
  public void discard() {
    for (String path : written) {
      io.deleteFile(path);
    }
  }
}
