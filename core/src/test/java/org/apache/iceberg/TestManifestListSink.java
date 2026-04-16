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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * End-to-end verification of the {@link ManifestListSink} extension point.
 *
 * <p>Wraps {@link TestTables.TestTableOperations} in an ops that also implements {@link
 * ManifestListSink}, runs commits through the refactored {@link SnapshotProducer#apply()}, and
 * verifies that:
 *
 * <ul>
 *   <li>The sink receives finalized manifest lists (sequence numbers and first-row-id assigned)
 *   <li>{@code Snapshot.manifestListLocation()} is null when a sink is active
 *   <li>Manifest lists captured by the sink are field-identical to what would have been written to
 *       an Avro file (compared by running the same commit without a sink)
 *   <li>Multiple commits work correctly and produce the expected per-snapshot manifest list
 * </ul>
 */
public class TestManifestListSink {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).build();

  private static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(100)
          .build();

  private static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(20)
          .withRecordCount(200)
          .build();

  private static final DataFile FILE_C =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-c.parquet")
          .withFileSizeInBytes(30)
          .withRecordCount(300)
          .build();

  @TempDir Path sinkDir;

  @TempDir Path referenceDir;

  /** A TableOperations that captures manifest lists instead of writing them to Avro files. */
  static class CapturingOps implements TableOperations, ManifestListSink {

    private final TableOperations delegate;
    final Map<Long, List<ManifestFile>> capturedBySnapshot = new HashMap<>();
    Long lastSequenceNumber;
    Long lastSnapshotId;
    Long lastParentSnapshotId;
    Long lastNextRowId;
    Long lastNextRowIdAfter;

    CapturingOps(TableOperations delegate) {
      this.delegate = delegate;
    }

    @Override
    public void stageManifestList(
        long sequenceNumber,
        long snapshotId,
        Long parentSnapshotId,
        Long nextRowId,
        List<ManifestFile> manifests,
        Long nextRowIdAfter) {
      this.lastSequenceNumber = sequenceNumber;
      this.lastSnapshotId = snapshotId;
      this.lastParentSnapshotId = parentSnapshotId;
      this.lastNextRowId = nextRowId;
      this.lastNextRowIdAfter = nextRowIdAfter;
      capturedBySnapshot.put(snapshotId, new ArrayList<>(manifests));
    }

    // TableOperations delegation
    @Override
    public TableMetadata current() {
      return delegate.current();
    }

    @Override
    public TableMetadata refresh() {
      return delegate.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      delegate.commit(base, metadata);
    }

    @Override
    public FileIO io() {
      return delegate.io();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return delegate.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
      return delegate.locationProvider();
    }

    @Override
    public long newSnapshotId() {
      return delegate.newSnapshotId();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3})
  public void testSinkCapturesFastAppendManifests(int formatVersion) throws Exception {
    File sinkTableDir = sinkDir.resolve("sink-table-" + formatVersion).toFile();
    File refTableDir = referenceDir.resolve("ref-table-" + formatVersion).toFile();
    assertThat(sinkTableDir.mkdir()).isTrue();
    assertThat(refTableDir.mkdir()).isTrue();

    // Two tables with identical config
    TestTables.TestTable sinkTable =
        TestTables.create(sinkTableDir, "sink", SCHEMA, SPEC, formatVersion);
    TestTables.TestTable refTable =
        TestTables.create(refTableDir, "ref", SCHEMA, SPEC, formatVersion);

    try {
      // Wrap the sink table's ops so it captures manifest lists
      CapturingOps capturing = new CapturingOps(sinkTable.operations());
      BaseTable sinkTableWithSink = new BaseTable(capturing, "sink");

      // Commit identically through both: reference writes Avro, sink captures in-memory
      sinkTableWithSink.newFastAppend().appendFile(FILE_A).commit();
      refTable.newFastAppend().appendFile(FILE_A).commit();

      Snapshot sinkSnap = sinkTableWithSink.currentSnapshot();
      Snapshot refSnap = refTable.currentSnapshot();

      // Sink path: no external manifest list file
      assertThat(sinkSnap.manifestListLocation())
          .as("Snapshot.manifestListLocation() must be null when a sink is active")
          .isNull();

      // Sink captured the manifest list
      List<ManifestFile> captured = capturing.capturedBySnapshot.get(sinkSnap.snapshotId());
      assertThat(captured).as("Sink received manifests for the committed snapshot").isNotNull();

      // Reference path: read the Avro manifest list
      List<ManifestFile> fromAvro =
          ManifestLists.read(refTable.io().newInputFile(refSnap.manifestListLocation()));

      // Both should have the same number of entries
      assertThat(captured).hasSameSizeAs(fromAvro);

      // Compare field-by-field (paths differ because they're in different dirs,
      // but the structure and sequence numbers / row ids should match)
      for (int i = 0; i < captured.size(); i++) {
        ManifestFile c = captured.get(i);
        ManifestFile r = fromAvro.get(i);
        assertThat(c.partitionSpecId())
            .as("partitionSpecId [%d]", i)
            .isEqualTo(r.partitionSpecId());
        assertThat(c.content()).as("content [%d]", i).isEqualTo(r.content());
        assertThat(c.sequenceNumber())
            .as("sequenceNumber [%d]", i)
            .isEqualTo(r.sequenceNumber());
        assertThat(c.minSequenceNumber())
            .as("minSequenceNumber [%d]", i)
            .isEqualTo(r.minSequenceNumber());
        assertThat(c.addedFilesCount())
            .as("addedFilesCount [%d]", i)
            .isEqualTo(r.addedFilesCount());
        assertThat(c.existingFilesCount())
            .as("existingFilesCount [%d]", i)
            .isEqualTo(r.existingFilesCount());
        assertThat(c.deletedFilesCount())
            .as("deletedFilesCount [%d]", i)
            .isEqualTo(r.deletedFilesCount());
        assertThat(c.addedRowsCount())
            .as("addedRowsCount [%d]", i)
            .isEqualTo(r.addedRowsCount());
        assertThat(c.existingRowsCount())
            .as("existingRowsCount [%d]", i)
            .isEqualTo(r.existingRowsCount());
        assertThat(c.deletedRowsCount())
            .as("deletedRowsCount [%d]", i)
            .isEqualTo(r.deletedRowsCount());

        assertThat(c.sequenceNumber())
            .as("sequence numbers must be finalized")
            .isNotEqualTo(ManifestWriter.UNASSIGNED_SEQ);
        assertThat(c.minSequenceNumber())
            .as("min sequence numbers must be finalized")
            .isNotEqualTo(ManifestWriter.UNASSIGNED_SEQ);

        if (formatVersion >= 3 && c.content() == ManifestContent.DATA) {
          assertThat(c.firstRowId()).as("firstRowId must be assigned [%d]", i).isNotNull();
          assertThat(c.firstRowId())
              .as("firstRowId must match writer [%d]", i)
              .isEqualTo(r.firstRowId());
        }
      }

      // Sink was given the correct metadata
      assertThat(capturing.lastSnapshotId).isEqualTo(sinkSnap.snapshotId());
      assertThat(capturing.lastSequenceNumber).isEqualTo(sinkSnap.sequenceNumber());
    } finally {
      TestTables.clearTables();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3})
  public void testSinkCapturesMultipleCommits(int formatVersion) throws Exception {
    File sinkTableDir = sinkDir.resolve("sink-multi-" + formatVersion).toFile();
    assertThat(sinkTableDir.mkdir()).isTrue();

    TestTables.TestTable sinkTable =
        TestTables.create(sinkTableDir, "multi", SCHEMA, SPEC, formatVersion);

    try {
      CapturingOps capturing = new CapturingOps(sinkTable.operations());
      BaseTable t = new BaseTable(capturing, "multi");

      t.newFastAppend().appendFile(FILE_A).commit();
      long snap1Id = t.currentSnapshot().snapshotId();

      t.newFastAppend().appendFile(FILE_B).commit();
      long snap2Id = t.currentSnapshot().snapshotId();

      t.newFastAppend().appendFile(FILE_C).commit();
      long snap3Id = t.currentSnapshot().snapshotId();

      // Each commit should have been captured
      assertThat(capturing.capturedBySnapshot.get(snap1Id)).hasSize(1);
      assertThat(capturing.capturedBySnapshot.get(snap2Id)).hasSize(2);
      assertThat(capturing.capturedBySnapshot.get(snap3Id)).hasSize(3);

      // All snapshots have null manifestListLocation
      for (Snapshot snap : t.snapshots()) {
        assertThat(snap.manifestListLocation())
            .as("Snapshot %d has null manifestListLocation", snap.snapshotId())
            .isNull();
      }

      // Each captured manifest list is fully finalized
      for (List<ManifestFile> manifests : capturing.capturedBySnapshot.values()) {
        for (ManifestFile mf : manifests) {
          assertThat(mf.sequenceNumber()).isNotEqualTo(ManifestWriter.UNASSIGNED_SEQ);
          assertThat(mf.minSequenceNumber()).isNotEqualTo(ManifestWriter.UNASSIGNED_SEQ);
          if (formatVersion >= 3 && mf.content() == ManifestContent.DATA) {
            assertThat(mf.firstRowId()).isNotNull();
          }
        }
      }

      // No manifest list Avro files were written for the sink table
      File metadataDir = new File(sinkTableDir, "metadata");
      File[] metadataFiles = metadataDir.listFiles();
      assertThat(metadataFiles).isNotNull();
      long snapAvroCount =
          java.util.Arrays.stream(metadataFiles)
              .filter(f -> f.getName().startsWith("snap-") && f.getName().endsWith(".avro"))
              .count();
      assertThat(snapAvroCount)
          .as("No snap-*.avro manifest list files should be written when sink is active")
          .isZero();
    } finally {
      TestTables.clearTables();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3})
  public void testSinkAndWriterProduceEquivalentSequenceNumbers(int formatVersion) throws Exception {
    File sinkTableDir = sinkDir.resolve("sink-seq-" + formatVersion).toFile();
    File refTableDir = referenceDir.resolve("ref-seq-" + formatVersion).toFile();
    assertThat(sinkTableDir.mkdir()).isTrue();
    assertThat(refTableDir.mkdir()).isTrue();

    TestTables.TestTable sinkTable =
        TestTables.create(sinkTableDir, "sinkseq", SCHEMA, SPEC, formatVersion);
    TestTables.TestTable refTable =
        TestTables.create(refTableDir, "refseq", SCHEMA, SPEC, formatVersion);

    try {
      CapturingOps capturing = new CapturingOps(sinkTable.operations());
      BaseTable sinkT = new BaseTable(capturing, "sinkseq");

      // Several commits, then compare sequence-number progression
      sinkT.newFastAppend().appendFile(FILE_A).commit();
      refTable.newFastAppend().appendFile(FILE_A).commit();
      sinkT.newFastAppend().appendFile(FILE_B).commit();
      refTable.newFastAppend().appendFile(FILE_B).commit();
      sinkT.newFastAppend().appendFile(FILE_C).commit();
      refTable.newFastAppend().appendFile(FILE_C).commit();

      // Compare final snapshot's manifest list
      Snapshot sinkSnap = sinkT.currentSnapshot();
      Snapshot refSnap = refTable.currentSnapshot();

      List<ManifestFile> captured = capturing.capturedBySnapshot.get(sinkSnap.snapshotId());
      List<ManifestFile> fromAvro =
          ManifestLists.read(refTable.io().newInputFile(refSnap.manifestListLocation()));

      assertThat(captured).hasSameSizeAs(fromAvro);

      // The key assertion: finalized sequence numbers and (v3+) first_row_ids
      // must match between sink and writer paths for each manifest in order.
      for (int i = 0; i < captured.size(); i++) {
        ManifestFile c = captured.get(i);
        ManifestFile r = fromAvro.get(i);
        assertThat(c.sequenceNumber())
            .as("finalized sequenceNumber must match [%d]", i)
            .isEqualTo(r.sequenceNumber());
        assertThat(c.minSequenceNumber())
            .as("finalized minSequenceNumber must match [%d]", i)
            .isEqualTo(r.minSequenceNumber());

        if (formatVersion >= 3 && c.content() == ManifestContent.DATA) {
          assertThat(c.firstRowId())
              .as("finalized firstRowId must match [%d]", i)
              .isEqualTo(r.firstRowId());
        }
      }
    } finally {
      TestTables.clearTables();
    }
  }
}
