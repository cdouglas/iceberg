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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.exceptions.CompactionConflictException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for compaction conflict detection with deletion vectors (DVs).
 *
 * <p>These tests verify that concurrent transactions are correctly detected when DVs reference data
 * files that have been compacted.
 */
public class TestCompactionConflictDetectionDV {

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

  @Test
  public void testCompactionConflictDetectedWithDV() throws IOException {
    // 1. Create table with compaction maps enabled (V3 supports DVs)
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table_dv");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "3")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // 2. Write initial data files (use multiple files like the v2 test)
    List<DataFile> sourceFiles = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      DataFile dataFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath(String.format("/path/to/source%d.parquet", i))
              .withFileSizeInBytes(1024)
              .withRecordCount(100)
              .build();
      sourceFiles.add(dataFile);
    }

    AppendFiles append = table.newAppend();
    sourceFiles.forEach(append::appendFile);
    append.commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // 3. Start a RowDelta transaction (don't commit yet)
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(startingSnapshot);

    // Create DV for the first source file
    DataFile fileToDelete = sourceFiles.get(0);
    DeleteFile dv = writeDV(table, fileToDelete.path().toString(), 10L, 20L, 30L);
    rowDelta.addDeletes(dv);

    // 4. Meanwhile, another transaction compacts the data
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    sourceFiles.forEach(rewrite::deleteFile);

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(3072)
            .withRecordCount(300)
            .build();

    rewrite.addFile(targetFile);
    rewrite.commit(); // Compaction commits successfully

    // 5. Try to commit the RowDelta - should fail with CompactionConflictException
    CompactionConflictException exception =
        assertThrows(CompactionConflictException.class, () -> rowDelta.commit());

    assertThat(exception.getMessage()).contains("referenced data files were compacted");

    // Exception provides programmatic access to conflict details
    assertThat(exception.compactedFiles()).contains(fileToDelete.path().toString());

    assertThat(exception.compactionMapLocations())
        .isNotEmpty()
        .containsKey(fileToDelete.path().toString());
  }

  /**
   * Regression test: {@code ManifestReader.entries()} reuses one {@link ManifestEntry} (and its
   * contained {@code DeleteFile}) across iterations for allocation reasons. If
   * {@link CompactionConflictDetector} retains {@code entry.file()} without copying, every
   * conflict it reports collapses to the LAST file read from the manifest — the resolver then
   * remaps the wrong source DV against the right target and the row multiset diverges. The fix
   * is one line: {@code entry.file().copy(false)} before retaining.
   *
   * <p>This test commits three DVs against three different source files in a single delete
   * manifest (one snapshot), runs the detector, and asserts every conflicting DV carries the
   * source-file path that matches its actual reference. Pre-fix, all three would be the same
   * (the last-read) DV.
   */
  @Test
  public void testDetectorReturnsDistinctEntriesAcrossManifest() throws IOException {
    TableIdentifier tableIdent = TableIdentifier.of("db", "detector_distinct_entries");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "3")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // Three independent source files — important: they must all end up in ONE delete manifest
    // for this test to actually exercise the entry-reuse path. A single RowDelta commit with
    // three addDeletes() does that.
    List<DataFile> sourceFiles = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      DataFile dataFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath(String.format("/path/to/distinct_source_%d.parquet", i))
              .withFileSizeInBytes(1024)
              .withRecordCount(100)
              .build();
      sourceFiles.add(dataFile);
    }
    AppendFiles append = table.newAppend();
    sourceFiles.forEach(append::appendFile);
    append.commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Commit three DVs (one per source file) in a single RowDelta — they land in the same
    // delete manifest, which is exactly the shape that exposed the entry-reuse bug.
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(startingSnapshot);
    rowDelta.addDeletes(writeDV(table, sourceFiles.get(0).path().toString(), 10L));
    rowDelta.addDeletes(writeDV(table, sourceFiles.get(1).path().toString(), 20L));
    rowDelta.addDeletes(writeDV(table, sourceFiles.get(2).path().toString(), 30L));
    rowDelta.commit();

    Snapshot postDeltaSnapshot = table.currentSnapshot();
    TableMetadata base = ((HasTableOperations) table).operations().current();

    java.util.Set<String> filesToCompact = new java.util.HashSet<>();
    sourceFiles.forEach(f -> filesToCompact.add(f.path().toString()));

    CompactionConflictDetector detector =
        new CompactionConflictDetector(table.io(), base, startingSnapshot, postDeltaSnapshot);
    DeleteConflictInfo info = detector.detectConflicts(filesToCompact);

    assertThat(info.conflictingDeleteFiles())
        .as("detector should report one conflicting DV per source file")
        .hasSize(3);

    // The asserting evidence the fix is in place: the reported DVs must reference all three
    // distinct source files, not three copies of whichever file the manifest reader happened
    // to land on last. Pre-fix, this set would collapse to a single path.
    java.util.Set<String> referencedSources = new java.util.HashSet<>();
    for (DeleteFile dv : info.conflictingDeleteFiles()) {
      referencedSources.add(dv.referencedDataFile());
    }
    assertThat(referencedSources)
        .as("each conflicting DV must reference its own distinct source file path")
        .containsExactlyInAnyOrder(
            sourceFiles.get(0).path().toString(),
            sourceFiles.get(1).path().toString(),
            sourceFiles.get(2).path().toString());
  }

  /** Helper method to write a deletion vector file. */
  private DeleteFile writeDV(Table table, String dataFilePath, Long... positions)
      throws IOException {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();

    java.util.function.Function<String, PositionDeleteIndex> noPreviousDeletes =
        path -> PositionDeleteIndex.empty();

    DVFileWriter writer = new BaseDVFileWriter(fileFactory, noPreviousDeletes);

    for (Long pos : positions) {
      writer.delete(dataFilePath, pos, table.spec(), null);
    }

    writer.close();
    DeleteWriteResult result = writer.result();

    if (result.deleteFiles().isEmpty()) {
      throw new IllegalStateException("DV writer produced no delete files");
    }

    return result.deleteFiles().get(0);
  }
}
