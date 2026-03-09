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
import org.apache.iceberg.exceptions.CompactionConflictException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for CompactionMapValidator handling of multi-file position deletes.
 *
 * <p>Position deletes are writes with physical {@code (file_path, position)} addresses. When files
 * are compacted, these addresses must be rebased through the compaction map. For file-scoped
 * deletes (referencedDataFile set), the validator checks directly against compacted files. For
 * multi-file deletes (referencedDataFile null), the validator conservatively treats all compacted
 * files as conflicts because it cannot determine the delete's targets from metadata alone.
 *
 * <p>This conservative approach is correct: SERIALIZABLE isolation's "structural change = no
 * conflict" optimization applies only to reads, not to writes. Position deletes with stale physical
 * addresses would cause missed deletions if allowed to commit.
 */
public class TestCompactionMapValidatorMultiFileDeletes {

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
   * Verifies that file-scoped position deletes (with referencedDataFile set) are correctly detected
   * by the validator when they reference compacted files. This is the baseline happy path.
   */
  @Test
  public void testFileScopedPositionDeleteConflictDetected() throws IOException {
    TableIdentifier tableIdent = TableIdentifier.of("db", "file_scoped_test");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // Write initial data files
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

    AppendFiles append = table.newAppend();
    sourceFiles.forEach(append::appendFile);
    append.commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Start a RowDelta with a file-scoped position delete (referencedDataFile set)
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(startingSnapshot);

    DeleteFile fileScopedDelete =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(5)
            .withReferencedDataFile(sourceFiles.get(0).path().toString())
            .build();

    rowDelta.addDeletes(fileScopedDelete);

    // Compact the data
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    sourceFiles.forEach(rewrite::deleteFile);

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(2048)
            .withRecordCount(200)
            .build();

    rewrite.addFile(targetFile);

    // Build and attach explicit compaction map
    {
      CompactionMapBuilder cmb = new CompactionMapBuilder(startingSnapshot, startingSnapshot + 1);
      long off = 0;
      for (DataFile sf : sourceFiles) {
        cmb.addFileMapping(sf.path().toString(), targetFile.path().toString())
            .addRun(0L, off, sf.recordCount());
        off += sf.recordCount();
      }
      CompactionMap cmap = cmb.build();
      OutputFile cmf = CompactionMaps.newCompactionMapFile(table, startingSnapshot + 1);
      CompactionMaps.write(cmap, cmf);
      ((BaseRewriteFiles) rewrite).setCompactionMapLocation(cmf.location());
    }

    rewrite.commit();

    // Validator should detect the conflict
    boolean conflictDetected = false;
    try {
      rowDelta.commit();
    } catch (CompactionConflictException e) {
      conflictDetected = true;
      assertThat(e.compactedFiles()).contains(sourceFiles.get(0).path().toString());
    }

    assertThat(conflictDetected)
        .as("File-scoped position delete conflict should be detected")
        .isTrue();
  }

  /**
   * Verifies that multi-file position deletes (without referencedDataFile) are conservatively
   * detected as conflicts when compacted files exist.
   *
   * <p>Position deletes are writes with physical {@code (file_path, position)} addresses. When
   * files are compacted, these addresses become stale and must be rebased through the compaction
   * map. For multi-file deletes (referencedDataFile == null), we cannot determine which files they
   * reference from metadata alone, so we conservatively treat all compacted files as conflicts.
   * This forces the caller to rebase the deletes — if the delete doesn't actually reference
   * compacted files, rebasing is a no-op and the retry succeeds.
   */
  @Test
  public void testMultiFilePositionDeleteConflictDetectedConservatively() throws IOException {
    TableIdentifier tableIdent = TableIdentifier.of("db", "multi_file_gap_test");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // Write initial data files
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

    AppendFiles append = table.newAppend();
    sourceFiles.forEach(append::appendFile);
    append.commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Start a RowDelta with a multi-file position delete (NO referencedDataFile)
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(startingSnapshot);

    // Multi-file position delete: referencedDataFile is NOT set
    DeleteFile multiFileDelete =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("/path/to/multi_deletes.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .build();

    rowDelta.addDeletes(multiFileDelete);

    // Compact the data
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    sourceFiles.forEach(rewrite::deleteFile);

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(2048)
            .withRecordCount(200)
            .build();

    rewrite.addFile(targetFile);

    // Build and attach explicit compaction map
    {
      CompactionMapBuilder cmb = new CompactionMapBuilder(startingSnapshot, startingSnapshot + 1);
      long off = 0;
      for (DataFile sf : sourceFiles) {
        cmb.addFileMapping(sf.path().toString(), targetFile.path().toString())
            .addRun(0L, off, sf.recordCount());
        off += sf.recordCount();
      }
      CompactionMap cmap = cmb.build();
      OutputFile cmf = CompactionMaps.newCompactionMapFile(table, startingSnapshot + 1);
      CompactionMaps.write(cmap, cmf);
      ((BaseRewriteFiles) rewrite).setCompactionMapLocation(cmf.location());
    }

    rewrite.commit();

    // Multi-file position deletes are conservatively treated as conflicts because
    // position deletes are writes (physical addresses), not reads. The validator cannot
    // determine which files they reference from metadata, so it forces rebasing.
    boolean conflictDetected = false;
    try {
      rowDelta.commit();
    } catch (CompactionConflictException e) {
      conflictDetected = true;
    }

    assertThat(conflictDetected)
        .as(
            "Multi-file position delete conflict must be detected: position deletes are writes "
                + "with physical addresses that must be rebased through the compaction map")
        .isTrue();
  }
}
