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
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for compaction map commit flow.
 *
 * <p>These tests verify that explicitly provided compaction maps are correctly referenced in
 * ManifestFile records when data files are rewritten.
 */
public class TestCompactionMapCommitFlow {

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
  public void testRewriteWithExplicitCompactionMap() throws IOException {
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "4")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

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

    long snapshotBeforeRewrite = table.currentSnapshot().snapshotId();

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(3072)
            .withRecordCount(300)
            .build();

    // Build compaction map explicitly
    CompactionMapBuilder builder =
        new CompactionMapBuilder(snapshotBeforeRewrite, snapshotBeforeRewrite + 1);
    long offset = 0;
    for (DataFile sf : sourceFiles) {
      builder
          .addFileMapping(sf.path().toString(), targetFile.path().toString())
          .addRun(0L, offset, sf.recordCount());
      offset += sf.recordCount();
    }

    CompactionMap map = builder.build();
    OutputFile mapFile = CompactionMaps.newCompactionMapFile(table, snapshotBeforeRewrite + 1);
    CompactionMaps.write(map, mapFile);

    // Perform rewrite with explicit map
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(snapshotBeforeRewrite);
    sourceFiles.forEach(rewrite::deleteFile);
    rewrite.addFile(targetFile);
    ((BaseRewriteFiles) rewrite).setCompactionMapLocation(mapFile.location());
    rewrite.commit();

    // Verify compaction map was attached
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.REPLACE);

    ManifestFile addedManifest =
        snapshot.dataManifests(table.io()).stream()
            .filter(ManifestFile::hasAddedFiles)
            .findFirst()
            .orElse(null);

    assertThat(addedManifest).isNotNull();
    assertThat(addedManifest.compactionMapLocation()).isNotNull();

    // Verify map content
    CompactionMap loadedMap =
        CompactionMaps.read(table.io().newInputFile(addedManifest.compactionMapLocation()));

    assertThat(loadedMap.sourceSnapshotId()).isEqualTo(snapshotBeforeRewrite);
    assertThat(loadedMap.fileMappings()).hasSize(3);

    Set<String> sourcePaths =
        sourceFiles.stream()
            .map(f -> f.path().toString())
            .collect(java.util.stream.Collectors.toSet());

    for (CompactionMap.FileMapping mapping : loadedMap.fileMappings()) {
      assertThat(sourcePaths).contains(mapping.sourceFile());
      assertThat(mapping.targetFile()).isEqualTo(targetFile.path().toString());
      assertThat(mapping.runs()).isNotEmpty();
    }
  }

  @Test
  public void testBackwardCompatibilityWithoutCompactionMaps() throws IOException {
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table_compat");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    DataFile sourceFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(sourceFile).commit();

    RewriteFiles rewrite = table.newRewrite();
    rewrite.deleteFile(sourceFile);

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    rewrite.addFile(targetFile);
    rewrite.commit();

    // Verify NO compaction map was written
    for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
      assertThat(manifest.compactionMapLocation())
          .as("Manifest should not have compaction map location when feature is disabled")
          .isNull();
    }
  }

  @Test
  public void testCompactionMapLocationPersistence() throws IOException {
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table_persist");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "4")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    DataFile sourceFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(sourceFile).commit();
    long startingSnapshot = table.currentSnapshot().snapshotId();

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    // Build and write explicit map
    CompactionMapBuilder builder = new CompactionMapBuilder(startingSnapshot, startingSnapshot + 1);
    builder
        .addFileMapping(sourceFile.path().toString(), targetFile.path().toString())
        .addRun(0, 0, 100);
    CompactionMap map = builder.build();
    OutputFile mapFile = CompactionMaps.newCompactionMapFile(table, startingSnapshot + 1);
    CompactionMaps.write(map, mapFile);

    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    rewrite.deleteFile(sourceFile);
    rewrite.addFile(targetFile);
    ((BaseRewriteFiles) rewrite).setCompactionMapLocation(mapFile.location());
    rewrite.commit();

    String mapLocation =
        table.currentSnapshot().dataManifests(table.io()).stream()
            .filter(ManifestFile::hasAddedFiles)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No manifest with added files found"))
            .compactionMapLocation();

    assertThat(mapLocation).isNotNull();

    // Reload table and verify persistence
    Table reloadedTable = catalog.loadTable(tableIdent);

    String reloadedMapLocation =
        reloadedTable.currentSnapshot().dataManifests(table.io()).stream()
            .filter(ManifestFile::hasAddedFiles)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No manifest with added files found"))
            .compactionMapLocation();

    assertThat(reloadedMapLocation)
        .as("Compaction map location should persist across table reloads")
        .isEqualTo(mapLocation);
  }

  @Test
  public void testMultipleRewritesWithExplicitMaps() throws IOException {
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table_multiple");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "4")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // First rewrite with explicit map
    DataFile source1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(source1).commit();
    long snap1 = table.currentSnapshot().snapshotId();

    DataFile target1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    CompactionMapBuilder b1 = new CompactionMapBuilder(snap1, snap1 + 1);
    b1.addFileMapping(source1.path().toString(), target1.path().toString()).addRun(0, 0, 100);
    OutputFile mf1 = CompactionMaps.newCompactionMapFile(table, snap1 + 1);
    CompactionMaps.write(b1.build(), mf1);

    RewriteFiles rewrite1 = table.newRewrite().validateFromSnapshot(snap1);
    rewrite1.deleteFile(source1);
    rewrite1.addFile(target1);
    ((BaseRewriteFiles) rewrite1).setCompactionMapLocation(mf1.location());
    rewrite1.commit();

    String mapLocation1 =
        table.currentSnapshot().dataManifests(table.io()).stream()
            .filter(ManifestFile::hasAddedFiles)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No manifest with added files found"))
            .compactionMapLocation();

    assertThat(mapLocation1).isNotNull();

    // Second rewrite with explicit map
    DataFile source2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(source2).commit();
    long snap2 = table.currentSnapshot().snapshotId();

    DataFile target2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    CompactionMapBuilder b2 = new CompactionMapBuilder(snap2, snap2 + 1);
    b2.addFileMapping(source2.path().toString(), target2.path().toString()).addRun(0, 0, 100);
    OutputFile mf2 = CompactionMaps.newCompactionMapFile(table, snap2 + 1);
    CompactionMaps.write(b2.build(), mf2);

    RewriteFiles rewrite2 = table.newRewrite().validateFromSnapshot(snap2);
    rewrite2.deleteFile(source2);
    rewrite2.addFile(target2);
    ((BaseRewriteFiles) rewrite2).setCompactionMapLocation(mf2.location());
    rewrite2.commit();

    String mapLocation2 =
        table.currentSnapshot().dataManifests(table.io()).stream()
            .filter(ManifestFile::hasAddedFiles)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No manifest with added files found"))
            .compactionMapLocation();

    assertThat(mapLocation2).isNotNull();

    assertThat(mapLocation2)
        .as("Each rewrite should have a unique compaction map")
        .isNotEqualTo(mapLocation1);
  }
}
