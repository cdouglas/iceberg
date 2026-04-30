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

import java.util.List;

/**
 * Optional capability mix-in for {@link TableOperations}. When a {@code TableOperations}
 * implementation also implements this interface, {@link SnapshotProducer#apply()} hands a
 * <b>manifest list delta</b> (added + removed entries relative to the parent snapshot) to the sink
 * instead of writing the full list to a separate Avro object.
 *
 * <p>This enables a catalog to store manifest list state inline with its own state, storing only
 * the per-commit change rather than the whole list. The reconstructed full list is exposed to the
 * engine via {@link Snapshot#allManifests(org.apache.iceberg.io.FileIO)} on the {@link Snapshot}
 * returned by {@code apply()} (which is an in-memory snapshot with the list already populated); on
 * subsequent loads from storage, the catalog is responsible for replaying its stored deltas
 * (typically starting from a checkpoint) to rebuild each snapshot's list.
 *
 * <p>When a sink is active, the returned {@link Snapshot}'s {@link Snapshot#manifestListLocation()}
 * is {@code null} ("not separate").
 */
public interface ManifestListSink {

  /**
   * A manifest list delta: entries to add and entries to remove (by path) relative to the parent
   * snapshot's manifest list. Each {@code ManifestFile} in {@code added} is finalized (sequence
   * numbers assigned, v3+ {@code first_row_id} assigned) — semantically identical to what would be
   * written into the Avro file for a brand-new manifest list entry.
   *
   * <p>If the parent snapshot is {@code null} (first commit), {@code removed} is empty and {@code
   * added} contains the complete initial manifest list.
   */
  final class ManifestListDelta {
    private final List<ManifestFile> added;
    private final List<String> removedPaths;

    public ManifestListDelta(List<ManifestFile> added, List<String> removedPaths) {
      this.added = added;
      this.removedPaths = removedPaths;
    }

    public List<ManifestFile> added() {
      return added;
    }

    public List<String> removedPaths() {
      return removedPaths;
    }
  }

  /**
   * Accept a manifest list delta for a snapshot.
   *
   * @param sequenceNumber sequence number being assigned to this commit
   * @param snapshotId id of the snapshot being committed
   * @param parentSnapshotId id of the parent snapshot, or null
   * @param nextRowId next-row-id at the start of this commit (v3+), or null for v1/v2
   * @param delta manifests added and removed (by path) relative to the parent snapshot
   * @param nextRowIdAfter next-row-id after this commit (v3+), or null for v1/v2
   */
  void stageManifestListDelta(
      long sequenceNumber,
      long snapshotId,
      Long parentSnapshotId,
      Long nextRowId,
      ManifestListDelta delta,
      Long nextRowIdAfter);
}
