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
 * implementation also implements this interface, {@link SnapshotProducer#apply()} will hand the
 * finalized manifest list directly to the sink instead of writing it to a separate Avro object.
 *
 * <p>This enables a catalog to store manifest lists inline with its own state (for example, in a
 * single-file catalog that absorbs the table's first-level metadata), eliminating the separate
 * {@code snap-*.avro} write per commit.
 *
 * <p>When a sink is active, the {@link Snapshot} returned by {@code SnapshotProducer.apply()} has
 * {@link Snapshot#manifestListLocation()} set to {@code null}; callers must read manifests through
 * {@link Snapshot#allManifests(org.apache.iceberg.io.FileIO)} / {@code dataManifests} / {@code
 * deleteManifests}, which the catalog is responsible for satisfying without a file read.
 */
public interface ManifestListSink {

  /**
   * Accept the finalized manifest list for a snapshot. The list has already had sequence numbers
   * and first-row-id values assigned per the table's format version; it is semantically identical
   * to what {@link ManifestListWriter} would write to an Avro file.
   *
   * @param sequenceNumber sequence number being assigned to this commit
   * @param snapshotId id of the snapshot being committed
   * @param parentSnapshotId id of the parent snapshot, or null
   * @param nextRowId next-row-id at the start of this commit (v3+), or null for v1/v2
   * @param manifests the finalized manifest list in the order it would be written
   * @param nextRowIdAfter next-row-id after this commit (v3+), or null for v1/v2
   */
  void stageManifestList(
      long sequenceNumber,
      long snapshotId,
      Long parentSnapshotId,
      Long nextRowId,
      List<ManifestFile> manifests,
      Long nextRowIdAfter);
}
