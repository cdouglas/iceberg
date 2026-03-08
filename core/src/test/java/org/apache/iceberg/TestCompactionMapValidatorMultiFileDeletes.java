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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for the CompactionMapValidator gap with multi-file position deletes.
 *
 * <p>CompactionMapValidator.findConflicts() only checks referencedDataFile, which means multi-file
 * position deletes (those without a single referencedDataFile) are silently skipped. This test
 * documents the gap: when multi-file position deletes reference compacted files, the validator does
 * not detect the conflict.
 *
 * <p>Compare with {@link TestCompactionConflictDetector#testMultiFilePositionDeletesDetected()}
 * which verifies that CompactionConflictDetector correctly handles this case.
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
   * Documents the known gap: multi-file position deletes (without referencedDataFile) are NOT
   * detected by CompactionMapValidator.findConflicts().
   *
   * <p>The multi-file position delete references rows in files that were compacted, but the
   * validator only checks referencedDataFile (which is null for multi-file deletes). A conservative
   * approach (treating all compacted files as conflicts) was tried but rejected because it produces
   * false positives that break SERIALIZABLE isolation for V2 tables — unrelated position deletes
   * would be incorrectly flagged as conflicting.
   *
   * <p>Compare with {@link TestCompactionConflictDetector#testMultiFilePositionDeletesDetected()}
   * which verifies that CompactionConflictDetector correctly handles this case by scanning manifest
   * content.
   */
  @Test
  public void testMultiFilePositionDeleteConflictNotDetected() throws IOException {
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
    rewrite.commit();

    // Known gap: multi-file position deletes are NOT detected because the validator
    // only checks referencedDataFile. The conservative approach (flag all compacted files)
    // was rejected because it breaks SERIALIZABLE isolation for V2 tables.
    boolean conflictDetected = false;
    try {
      rowDelta.commit();
    } catch (CompactionConflictException e) {
      conflictDetected = true;
    }

    assertThat(conflictDetected)
        .as(
            "Multi-file position delete conflict is NOT detected by validator (known gap). "
                + "CompactionConflictDetector handles this case via manifest scanning instead.")
        .isFalse();
  }
}
