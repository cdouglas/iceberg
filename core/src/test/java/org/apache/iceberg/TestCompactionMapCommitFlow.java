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
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for compaction map commit flow.
 *
 * <p>These tests verify that compaction maps are automatically generated and referenced in
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
  public void testRewriteGeneratesCompactionMap() throws IOException {
    // 1. Create table with compaction maps enabled
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table.updateProperties()
        .set(TableProperties.FORMAT_VERSION, "4")
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

    // Commit source files
    AppendFiles append = table.newAppend();
    sourceFiles.forEach(append::appendFile);
    append.commit();

    long snapshotBeforeRewrite = table.currentSnapshot().snapshotId();

    // 3. Perform rewrite via RewriteFiles API
    RewriteFiles rewrite = table.newRewrite();
    sourceFiles.forEach(rewrite::deleteFile);

    // Create target file (bin-pack of all 3 source files)
    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(3072)
            .withRecordCount(300) // Sum of source files
            .build();

    rewrite.addFile(targetFile);
    rewrite.commit();

    // 4. Verify compaction map was written
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.REPLACE);

    List<ManifestFile> manifests = snapshot.dataManifests(table.io());
    assertThat(manifests).isNotEmpty();

    // Find manifest with added files
    ManifestFile addedManifest =
        manifests.stream().filter(m -> m.hasAddedFiles()).findFirst().orElse(null);

    assertThat(addedManifest).isNotNull();
    assertThat(addedManifest.compactionMapLocation())
        .as("Manifest should have compaction map location")
        .isNotNull();

    // 5. Load and verify map structure
    CompactionMap map =
        CompactionMaps.read(table.io().newInputFile(addedManifest.compactionMapLocation()));

    assertThat(map.sourceSnapshotId()).isEqualTo(snapshotBeforeRewrite);
    assertThat(map.fileMappings()).hasSize(3); // 3 source files

    // Verify each source file is mapped
    Set<String> sourcePaths =
        sourceFiles.stream()
            .map(f -> f.path().toString())
            .collect(java.util.stream.Collectors.toSet());

    for (CompactionMap.FileMapping mapping : map.fileMappings()) {
      assertThat(sourcePaths).contains(mapping.sourceFile());
      assertThat(mapping.targetFile()).isEqualTo(targetFile.path().toString());
      assertThat(mapping.runs()).isNotEmpty();
    }
  }

  @Test
  public void testBackwardCompatibilityWithoutCompactionMaps() throws IOException {
    // Test that compaction maps are NOT generated when property is disabled
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table_compat");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    // Do NOT enable compaction maps (test default behavior)

    // Write and rewrite files
    DataFile sourceFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    AppendFiles append = table.newAppend();
    append.appendFile(sourceFile);
    append.commit();

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
    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = snapshot.dataManifests(table.io());

    for (ManifestFile manifest : manifests) {
      assertThat(manifest.compactionMapLocation())
          .as("Manifest should not have compaction map location when feature is disabled")
          .isNull();
    }
  }

  @Test
  public void testCompactionMapLocationPersistence() throws IOException {
    // Test that compaction map location survives table reload
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table_persist");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table.updateProperties()
        .set(TableProperties.FORMAT_VERSION, "4")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // Write and rewrite files
    DataFile sourceFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    AppendFiles append = table.newAppend();
    append.appendFile(sourceFile);
    append.commit();

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

    // Get compaction map location from manifest with added files
    String mapLocation =
        table.currentSnapshot().dataManifests(table.io()).stream()
            .filter(m -> m.hasAddedFiles())
            .findFirst()
            .orElseThrow(() -> new AssertionError("No manifest with added files found"))
            .compactionMapLocation();

    assertThat(mapLocation).isNotNull();

    // Reload table
    Table reloadedTable = catalog.loadTable(tableIdent);

    // Verify compaction map location is still present
    String reloadedMapLocation =
        reloadedTable.currentSnapshot().dataManifests(table.io()).stream()
            .filter(m -> m.hasAddedFiles())
            .findFirst()
            .orElseThrow(() -> new AssertionError("No manifest with added files found"))
            .compactionMapLocation();

    assertThat(reloadedMapLocation)
        .as("Compaction map location should persist across table reloads")
        .isEqualTo(mapLocation);
  }

  @Test
  public void testMultipleRewritesGenerateMultipleMaps() throws IOException {
    // Test that multiple rewrite operations each generate their own compaction map
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table_multiple");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table.updateProperties()
        .set(TableProperties.FORMAT_VERSION, "4")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // First rewrite
    DataFile source1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    AppendFiles append1 = table.newAppend();
    append1.appendFile(source1);
    append1.commit();

    RewriteFiles rewrite1 = table.newRewrite();
    rewrite1.deleteFile(source1);

    DataFile target1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    rewrite1.addFile(target1);
    rewrite1.commit();

    String mapLocation1 =
        table.currentSnapshot().dataManifests(table.io()).stream()
            .filter(m -> m.hasAddedFiles())
            .findFirst()
            .orElseThrow(() -> new AssertionError("No manifest with added files found"))
            .compactionMapLocation();

    assertThat(mapLocation1).isNotNull();

    // Second rewrite
    DataFile source2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    AppendFiles append2 = table.newAppend();
    append2.appendFile(source2);
    append2.commit();

    RewriteFiles rewrite2 = table.newRewrite();
    rewrite2.deleteFile(source2);

    DataFile target2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    rewrite2.addFile(target2);
    rewrite2.commit();

    String mapLocation2 =
        table.currentSnapshot().dataManifests(table.io()).stream()
            .filter(m -> m.hasAddedFiles())
            .findFirst()
            .orElseThrow(() -> new AssertionError("No manifest with added files found"))
            .compactionMapLocation();

    assertThat(mapLocation2).isNotNull();

    // Verify the two maps are different
    assertThat(mapLocation2)
        .as("Each rewrite should generate a unique compaction map")
        .isNotEqualTo(mapLocation1);
  }
}
