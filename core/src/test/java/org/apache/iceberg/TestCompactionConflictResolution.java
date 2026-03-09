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
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.exceptions.CompactionConflictException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for compaction conflict resolution.
 *
 * <p>These tests demonstrate how to resolve compaction conflicts by remapping position deletes
 * using compaction maps. The workflow is:
 *
 * <ol>
 *   <li>Detect conflict via CompactionConflictException
 *   <li>Extract compaction map locations from exception
 *   <li>Load compaction maps
 *   <li>Use PositionDeleteRemapper to remap deletes
 *   <li>Retry transaction with remapped deletes
 * </ol>
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestCompactionConflictResolution {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @Parameters(name = "formatVersion = {0}")
  public static Object[][] parameters() {
    return new Object[][] {{2}, {3}};
  }

  @Parameter(index = 0)
  private int formatVersion;

  @TempDir public File temp;

  private InMemoryCatalog catalog;

  @BeforeEach
  public void setup() {
    catalog = new InMemoryCatalog();
    catalog.initialize("test", java.util.Collections.emptyMap());
    catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of("db"));
  }

  @TestTemplate
  public void testConflictResolutionWorkflow() throws IOException {
    // Demonstrate the conflict resolution workflow
    // This test shows the API pattern even though full remapping requires actual delete files

    // 1. Create table with compaction maps enabled
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_resolution_v" + formatVersion);
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // 2. Write initial data files
    List<DataFile> sourceFiles = Lists.newArrayList();
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

    // 3. Start a RowDelta transaction
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(startingSnapshot);

    DataFile fileToDelete = sourceFiles.get(0);
    DeleteFile deleteFile;
    if (formatVersion == 3) {
      // V3: Use DV
      deleteFile = writeDV(table, fileToDelete.path().toString(), 10L, 20L, 30L);
    } else {
      // V2: Position delete file
      deleteFile =
          FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
              .ofPositionDeletes()
              .withPath("/path/to/deletes.parquet")
              .withFileSizeInBytes(100)
              .withRecordCount(10)
              .withReferencedDataFile(fileToDelete.path().toString())
              .build();
    }

    rowDelta.addDeletes(deleteFile);

    // 4. Meanwhile, compact the data
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    sourceFiles.forEach(rewrite::deleteFile);

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(3072)
            .withRecordCount(300)
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

    // 5. Try to commit and catch conflict
    CompactionConflictException conflict = null;
    try {
      rowDelta.commit();
    } catch (CompactionConflictException e) {
      conflict = e;
    }

    assertThat(conflict).isNotNull();

    // 6. Demonstrate conflict resolution workflow
    // Extract compaction map locations from exception
    Map<String, String> compactionMapLocations = conflict.compactionMapLocations();
    assertThat(compactionMapLocations).isNotEmpty();

    // Get the compaction map location for the conflicting file
    String mapLocation = compactionMapLocations.get(fileToDelete.path().toString());
    assertThat(mapLocation).isNotNull();

    // Load the compaction map
    CompactionMap map = CompactionMaps.read(table.io().newInputFile(mapLocation));
    assertThat(map).isNotNull();
    assertThat(map.sourceSnapshotId()).isEqualTo(startingSnapshot);

    // Create a remapper
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Verify the remapper recognizes the compacted file
    assertThat(remapper.isCompacted(fileToDelete.path().toString())).isTrue();
    assertThat(remapper.needsRemapping(deleteFile)).isTrue();

    // Demonstrate position remapping (conceptual - requires actual delete file content)
    // In a real implementation, you would:
    // 1. Read position deletes from the delete file
    // 2. Use remapper.remapDelete(delete) for each delete
    // 3. Write remapped deletes to a new delete file
    // 4. Create new DeleteFile metadata pointing to remapped file
    // 5. Retry the transaction with remapped delete file

    // For this test, we demonstrate the API:
    PositionDelete<?> exampleDelete = PositionDelete.create();
    exampleDelete.set(fileToDelete.path().toString(), 50L, null);

    PositionDelete<?> remappedDelete = remapper.remapDelete(exampleDelete);
    assertThat(remappedDelete).isNotNull();
    assertThat(remappedDelete.path().toString()).isEqualTo(targetFile.path().toString());
    // The exact position depends on the compaction map runs
  }

  @TestTemplate
  public void testMultipleCompactionMaps() throws IOException {
    // Test handling multiple compaction maps when multiple files are compacted
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_multiple_maps_v" + formatVersion);
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // Write and compact files in multiple rounds
    DataFile file1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/file1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(file1).commit();
    long snap1 = table.currentSnapshot().snapshotId();

    // First compaction
    RewriteFiles rewrite1 = table.newRewrite().validateFromSnapshot(snap1);
    rewrite1.deleteFile(file1);
    DataFile target1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    rewrite1.addFile(target1);

    // Build and attach explicit compaction map
    {
      CompactionMapBuilder cmb = new CompactionMapBuilder(snap1, snap1 + 1);
      cmb.addFileMapping(file1.path().toString(), target1.path().toString())
          .addRun(0L, 0L, file1.recordCount());
      CompactionMap cmap = cmb.build();
      OutputFile cmf = CompactionMaps.newCompactionMapFile(table, snap1 + 1);
      CompactionMaps.write(cmap, cmf);
      ((BaseRewriteFiles) rewrite1).setCompactionMapLocation(cmf.location());
    }

    rewrite1.commit();

    // Add another file
    DataFile file2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/file2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(file2).commit();
    long snap2 = table.currentSnapshot().snapshotId();

    // Start transaction with deletes on both original files
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(snap1);

    DeleteFile delete1;
    DeleteFile delete2;
    if (formatVersion == 3) {
      // V3: Use DVs
      delete1 = writeDV(table, file1.path().toString(), 10L, 20L);
      delete2 = writeDV(table, file2.path().toString(), 30L, 40L);
    } else {
      // V2: Position delete files
      delete1 =
          FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
              .ofPositionDeletes()
              .withPath("/path/to/delete1.parquet")
              .withFileSizeInBytes(100)
              .withRecordCount(10)
              .withReferencedDataFile(file1.path().toString())
              .build();

      delete2 =
          FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
              .ofPositionDeletes()
              .withPath("/path/to/delete2.parquet")
              .withFileSizeInBytes(100)
              .withRecordCount(10)
              .withReferencedDataFile(file2.path().toString())
              .build();
    }

    rowDelta.addDeletes(delete1);
    rowDelta.addDeletes(delete2);

    // Second compaction
    RewriteFiles rewrite2 = table.newRewrite().validateFromSnapshot(snap2);
    rewrite2.deleteFile(file2);
    DataFile target2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    rewrite2.addFile(target2);

    // Build and attach explicit compaction map
    {
      CompactionMapBuilder cmb = new CompactionMapBuilder(snap2, snap2 + 1);
      cmb.addFileMapping(file2.path().toString(), target2.path().toString())
          .addRun(0L, 0L, file2.recordCount());
      CompactionMap cmap = cmb.build();
      OutputFile cmf = CompactionMaps.newCompactionMapFile(table, snap2 + 1);
      CompactionMaps.write(cmap, cmf);
      ((BaseRewriteFiles) rewrite2).setCompactionMapLocation(cmf.location());
    }

    rewrite2.commit();

    // Try to commit - should detect conflicts with both compactions
    CompactionConflictException conflict = null;
    try {
      rowDelta.commit();
    } catch (CompactionConflictException e) {
      conflict = e;
    }

    assertThat(conflict).isNotNull();

    // Verify both files are in the conflict
    assertThat(conflict.compactedFiles())
        .contains(file1.path().toString(), file2.path().toString());

    // Verify we have compaction map locations for both
    Map<String, String> mapLocations = conflict.compactionMapLocations();
    assertThat(mapLocations).containsKeys(file1.path().toString(), file2.path().toString());

    // In a real resolution, you would:
    // 1. Load both compaction maps
    // 2. Create remappers for each
    // 3. Remap deletes for each file
    // 4. Retry with all remapped deletes
  }

  @TestTemplate
  public void testPartialConflictResolution() throws IOException {
    // Test where only some delete files conflict with compaction
    TableIdentifier tableIdent =
        TableIdentifier.of("db", "test_partial_conflict_v" + formatVersion);
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // Write two data files
    DataFile file1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/file1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    DataFile file2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/file2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(file1).appendFile(file2).commit();
    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Start transaction with deletes on both files
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(startingSnapshot);

    DeleteFile delete1;
    DeleteFile delete2;
    if (formatVersion == 3) {
      // V3: Use DVs
      delete1 = writeDV(table, file1.path().toString(), 10L, 20L);
      delete2 = writeDV(table, file2.path().toString(), 30L, 40L);
    } else {
      // V2: Position delete files
      delete1 =
          FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
              .ofPositionDeletes()
              .withPath("/path/to/delete1.parquet")
              .withFileSizeInBytes(100)
              .withRecordCount(10)
              .withReferencedDataFile(file1.path().toString())
              .build();

      delete2 =
          FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
              .ofPositionDeletes()
              .withPath("/path/to/delete2.parquet")
              .withFileSizeInBytes(100)
              .withRecordCount(10)
              .withReferencedDataFile(file2.path().toString())
              .build();
    }

    rowDelta.addDeletes(delete1);
    rowDelta.addDeletes(delete2);

    // Compact only file1
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    rewrite.deleteFile(file1);
    DataFile target =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    rewrite.addFile(target);

    // Build and attach explicit compaction map
    {
      CompactionMapBuilder cmb = new CompactionMapBuilder(startingSnapshot, startingSnapshot + 1);
      cmb.addFileMapping(file1.path().toString(), target.path().toString())
          .addRun(0L, 0L, file1.recordCount());
      CompactionMap cmap = cmb.build();
      OutputFile cmf = CompactionMaps.newCompactionMapFile(table, startingSnapshot + 1);
      CompactionMaps.write(cmap, cmf);
      ((BaseRewriteFiles) rewrite).setCompactionMapLocation(cmf.location());
    }

    rewrite.commit();

    // Try to commit - should only conflict on file1
    CompactionConflictException conflict = null;
    try {
      rowDelta.commit();
    } catch (CompactionConflictException e) {
      conflict = e;
    }

    assertThat(conflict).isNotNull();

    // Verify only file1 is in the conflict (file2 is not compacted)
    assertThat(conflict.compactedFiles()).contains(file1.path().toString());
    assertThat(conflict.compactedFiles()).doesNotContain(file2.path().toString());

    // Resolution workflow:
    // 1. Identify which delete files need remapping (delete1 only)
    // 2. Load compaction map for file1
    // 3. Remap deletes from delete1
    // 4. Retry with remapped delete1 + original delete2
  }

  @TestTemplate
  public void testFromConflictFactoryMethod() throws IOException {
    // Test the PositionDeleteRemapper.fromConflict() convenience method
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_from_conflict_v" + formatVersion);
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // Create multiple source files
    DataFile file1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    DataFile file2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(file1).appendFile(file2).commit();
    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Start a transaction with deletes
    RowDelta rowDelta = table.newRowDelta().validateFromSnapshot(startingSnapshot);

    DeleteFile deleteFile;
    if (formatVersion == 3) {
      deleteFile = writeDV(table, file1.path().toString(), 10L, 20L);
    } else {
      deleteFile =
          FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
              .ofPositionDeletes()
              .withPath("/path/to/deletes.parquet")
              .withFileSizeInBytes(100)
              .withRecordCount(2)
              .withReferencedDataFile(file1.path().toString())
              .build();
    }
    rowDelta.addDeletes(deleteFile);

    // Compact both files into one
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    rewrite.deleteFile(file1);
    rewrite.deleteFile(file2);

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
      cmb.addFileMapping(file1.path().toString(), targetFile.path().toString())
          .addRun(0L, off, file1.recordCount());
      off += file1.recordCount();
      cmb.addFileMapping(file2.path().toString(), targetFile.path().toString())
          .addRun(0L, off, file2.recordCount());
      CompactionMap cmap = cmb.build();
      OutputFile cmf = CompactionMaps.newCompactionMapFile(table, startingSnapshot + 1);
      CompactionMaps.write(cmap, cmf);
      ((BaseRewriteFiles) rewrite).setCompactionMapLocation(cmf.location());
    }

    rewrite.commit();

    // Catch the conflict
    CompactionConflictException conflict = null;
    try {
      rowDelta.commit();
    } catch (CompactionConflictException e) {
      conflict = e;
    }

    assertThat(conflict).isNotNull();

    // Use the fromConflict factory method to create remappers
    Map<String, PositionDeleteRemapper> remappers =
        PositionDeleteRemapper.fromConflict(conflict, table.io());

    // Should have remapper for file1 (the file referenced by the delete)
    // file2 is not in the conflict since no deletes reference it
    assertThat(remappers).containsKey(file1.path().toString());

    // Verify remapper works correctly
    PositionDeleteRemapper remapper = remappers.get(file1.path().toString());
    assertThat(remapper).isNotNull();
    assertThat(remapper.isCompacted(file1.path().toString())).isTrue();

    // The remapper should be able to remap deletes to the target file
    PositionDelete<?> delete = PositionDelete.create().set(file1.path().toString(), 50L);
    PositionDelete<?> remapped = remapper.remapDelete(delete);
    assertThat(remapped.path().toString()).isEqualTo(targetFile.path().toString());

    // The remapper also knows about file2 (same compaction map covers both)
    assertThat(remapper.isCompacted(file2.path().toString())).isTrue();

    // And can remap deletes for file2 even though file2 wasn't in the exception
    PositionDelete<?> delete2 = PositionDelete.create().set(file2.path().toString(), 50L);
    PositionDelete<?> remapped2 = remapper.remapDelete(delete2);
    assertThat(remapped2.path().toString()).isEqualTo(targetFile.path().toString());
    // File2's position 50 should map to 150 in target (100 offset from file1's rows)
    assertThat(remapped2.pos()).isEqualTo(150L);
  }

  /** Helper method to write a deletion vector file (for V3 tests). */
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
