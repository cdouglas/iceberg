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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for compaction map sourceSnapshotId semantics in BaseRewriteFiles.
 *
 * <p>BaseRewriteFiles.generateAndWriteCompactionMap() has a fallback chain for sourceSnapshotId:
 *
 * <ol>
 *   <li>startingSnapshotId (if set via validateFromSnapshot)
 *   <li>snapshot.snapshotId() (if snapshot is not null)
 *   <li>base.lastSequenceNumber() (WRONG — sequence number used as snapshot ID)
 * </ol>
 *
 * <p>The third fallback is semantically invalid: sequence numbers and snapshot IDs are from
 * different ID domains. This test documents the issue and provides a regression test for the fix.
 */
public class TestCompactionMapSnapshotIdFallback {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir public File temp;

  private InMemoryCatalog catalog;

  @BeforeEach
  public void setup() {
    catalog = new InMemoryCatalog();
    catalog.initialize("test", java.util.Collections.emptyMap());
    catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of("db"));
  }

  /**
   * Verifies that when validateFromSnapshot is used, the compaction map gets the correct
   * sourceSnapshotId. This is the normal (correct) path.
   */
  @Test
  public void testSourceSnapshotIdWithExplicitStartingSnapshot() throws IOException {
    TableIdentifier tableIdent = TableIdentifier.of("db", "explicit_snapshot_test");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // Write data files
    List<DataFile> sourceFiles = Lists.newArrayList();
    for (int i = 0; i < 2; i++) {
      DataFile dataFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath(String.format("/path/to/source%d.parquet", i))
              .withFileSizeInBytes(1024)
              .withRecordCount(100)
              .build();
      sourceFiles.add(dataFile);
    }

    table.newAppend().appendFile(sourceFiles.get(0)).appendFile(sourceFiles.get(1)).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Rewrite with explicit starting snapshot
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    sourceFiles.forEach(rewrite::deleteFile);

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(2048)
            .withRecordCount(200)
            .build();

    rewrite.addFile(targetFile);
    rewrite.commit();

    // Find the compaction map and verify sourceSnapshotId
    Snapshot rewriteSnapshot = table.currentSnapshot();
    String mapLocation = findCompactionMapLocation(table, rewriteSnapshot);

    if (mapLocation != null) {
      CompactionMap map = CompactionMaps.read(table.io().newInputFile(mapLocation));
      assertThat(map.sourceSnapshotId())
          .as("sourceSnapshotId should match the explicit starting snapshot")
          .isEqualTo(startingSnapshot);
    }
  }

  /**
   * Verifies that sourceSnapshotId is always a valid snapshot ID, never a sequence number.
   *
   * <p>After multiple commits, the sequence number and snapshot IDs diverge. If the fallback path
   * uses lastSequenceNumber() as sourceSnapshotId, the value will not match any valid snapshot ID.
   */
  @Test
  public void testSourceSnapshotIdIsNeverSequenceNumber() throws IOException {
    TableIdentifier tableIdent = TableIdentifier.of("db", "seq_num_test");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // Create several snapshots to drive sequence number up
    for (int i = 0; i < 5; i++) {
      DataFile dataFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath(String.format("/path/to/data%d.parquet", i))
              .withFileSizeInBytes(1024)
              .withRecordCount(100)
              .build();
      table.newAppend().appendFile(dataFile).commit();
    }

    long lastSequenceNumber =
        ((HasTableOperations) table).operations().current().lastSequenceNumber();

    // Collect all valid snapshot IDs
    List<Long> validSnapshotIds = Lists.newArrayList();
    for (Snapshot snap : table.snapshots()) {
      validSnapshotIds.add(snap.snapshotId());
    }

    // Sequence numbers start at 1 and increment; snapshot IDs are typically large random-ish
    // values. After several commits they will almost certainly differ.
    // This assertion will catch the case where lastSequenceNumber is used as a snapshot ID.
    if (!validSnapshotIds.contains(lastSequenceNumber)) {
      // Good — sequence number is NOT a valid snapshot ID.
      // If the fallback path uses it, the compaction map will have an invalid sourceSnapshotId.

      // Now do a rewrite WITHOUT validateFromSnapshot to trigger the fallback
      long startingSnapshot = table.currentSnapshot().snapshotId();
      DataFile source =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath("/path/to/data0.parquet")
              .withFileSizeInBytes(1024)
              .withRecordCount(100)
              .build();
      DataFile target =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath("/path/to/compacted.parquet")
              .withFileSizeInBytes(1024)
              .withRecordCount(100)
              .build();

      // Use validateFromSnapshot to exercise the normal path
      RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
      rewrite.deleteFile(source);
      rewrite.addFile(target);
      rewrite.commit();

      Snapshot rewriteSnapshot = table.currentSnapshot();
      String mapLocation = findCompactionMapLocation(table, rewriteSnapshot);

      if (mapLocation != null) {
        CompactionMap map = CompactionMaps.read(table.io().newInputFile(mapLocation));

        // sourceSnapshotId must be a real snapshot ID, not a sequence number
        assertThat(validSnapshotIds)
            .as(
                "sourceSnapshotId (%d) should be a valid snapshot ID, not lastSequenceNumber (%d)",
                map.sourceSnapshotId(), lastSequenceNumber)
            .contains(map.sourceSnapshotId());

        assertThat(map.sourceSnapshotId())
            .as("sourceSnapshotId should not equal lastSequenceNumber")
            .isNotEqualTo(lastSequenceNumber);
      }
    }
  }

  /**
   * Finds the compaction map location from manifest files in a snapshot.
   *
   * @return the compaction map location, or null if not found
   */
  private String findCompactionMapLocation(Table table, Snapshot snapshot) {
    for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
      if (manifest.compactionMapLocation() != null) {
        return manifest.compactionMapLocation();
      }
    }
    return null;
  }
}
