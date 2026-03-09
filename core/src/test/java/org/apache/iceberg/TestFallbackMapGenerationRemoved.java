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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.actions.ImmutableRewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFilesCommitManager;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests verifying that action-level commit managers (RewriteDataFilesCommitManager) do NOT generate
 * fallback compaction maps when position mappings are absent.
 *
 * <p>The fallback was removed because it assumed bin-pack concatenation order, which cannot be
 * verified without explicit position tracking. A sort or z-order rewrite preserving record count
 * would produce an incorrect positional map.
 *
 * <p>Contrast with {@link BaseRewriteFiles} which retains a low-level fallback with a record-count
 * precondition for direct API callers doing bin-pack operations.
 */
public class TestFallbackMapGenerationRemoved {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir public File temp;

  private InMemoryCatalog catalog;

  @BeforeEach
  public void setup() {
    catalog = new InMemoryCatalog();
    catalog.initialize("test", Collections.emptyMap());
    catalog.createNamespace(Namespace.of("db"));
  }

  /**
   * When compaction maps are enabled but no position mappings exist in the file group, the
   * action-level commit manager should NOT generate a fallback map. The resulting manifest should
   * have no compaction map location.
   */
  @Test
  public void testNoFallbackMapWithoutPositionMappings() throws IOException {
    TableIdentifier tableIdent = TableIdentifier.of("db", "no_fallback");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "4")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    // Add source files
    DataFile source1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/data/source1.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    DataFile source2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/data/source2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(source1).appendFile(source2).commit();

    long startingSnapshotId = table.currentSnapshot().snapshotId();

    // Create target file (same total record count — the old fallback would have generated a map)
    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/data/target.parquet")
            .withFileSizeInBytes(2048)
            .withRecordCount(200) // 100 + 100
            .build();

    // Build a RewriteFileGroup WITHOUT position mappings
    RewriteFileGroup group = createFileGroup(table, List.of(source1, source2), targetFile);
    // Explicitly verify no position mappings
    assertThat(group.positionMappings()).isEmpty();

    // Commit through the action-level commit manager
    RewriteDataFilesCommitManager commitManager =
        new RewriteDataFilesCommitManager(table, startingSnapshotId);
    commitManager.commitFileGroups(Set.of(group));

    // Verify: no compaction map should exist in the manifest
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.REPLACE);

    List<ManifestFile> manifests = snapshot.dataManifests(table.io());
    for (ManifestFile manifest : manifests) {
      assertThat(manifest.compactionMapLocation())
          .as(
              "Action-level commit manager should NOT generate fallback maps "
                  + "without position tracking")
          .isNull();
    }
  }

  /**
   * When position mappings ARE present, the commit manager should generate a compaction map. This
   * is the expected path for Spark bin-pack with PositionTrackingDataWriter.
   */
  @Test
  public void testMapGeneratedWithPositionMappings() throws IOException {
    TableIdentifier tableIdent = TableIdentifier.of("db", "with_mappings");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "4")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    DataFile source =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/data/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(source).commit();

    long startingSnapshotId = table.currentSnapshot().snapshotId();

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/data/target.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    // Build file group WITH position mappings
    RewriteFileGroup group = createFileGroup(table, List.of(source), targetFile);
    group.setPositionMappings(
        Map.of(
            source.path().toString(),
            new RewriteFileGroup.FilePositionMapping(
                source.path().toString(),
                targetFile.path().toString(),
                List.of(new RewriteFileGroup.FilePositionMapping.Run(0, 0, 100)))));

    assertThat(group.positionMappings()).isNotEmpty();

    RewriteDataFilesCommitManager commitManager =
        new RewriteDataFilesCommitManager(table, startingSnapshotId);
    commitManager.commitFileGroups(Set.of(group));

    // Verify: compaction map SHOULD exist
    Snapshot snapshot = table.currentSnapshot();
    ManifestFile addedManifest =
        snapshot.dataManifests(table.io()).stream()
            .filter(ManifestFile::hasAddedFiles)
            .findFirst()
            .orElse(null);

    assertThat(addedManifest).isNotNull();
    assertThat(addedManifest.compactionMapLocation())
        .as("Commit manager should generate map when position mappings are provided")
        .isNotNull();

    // Verify map content
    CompactionMap map =
        CompactionMaps.read(table.io().newInputFile(addedManifest.compactionMapLocation()));
    assertThat(map.fileMappings()).hasSize(1);
    assertThat(map.fileMappings().get(0).sourceFile()).isEqualTo(source.path().toString());
    assertThat(map.fileMappings().get(0).targetFile()).isEqualTo(targetFile.path().toString());
  }

  /**
   * BaseRewriteFiles (low-level API) no longer auto-generates fallback compaction maps. Callers
   * must provide maps explicitly via {@link BaseRewriteFiles#setCompactionMapLocation(String)}.
   * This prevents silently incorrect maps from sort/z-order rewrites that happen to preserve record
   * counts.
   */
  @Test
  public void testBaseRewriteFilesNoAutoGeneration() throws IOException {
    TableIdentifier tableIdent = TableIdentifier.of("db", "base_no_auto");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "4")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    DataFile source =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/data/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(source).commit();

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/data/target.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    // Use low-level BaseRewriteFiles API (table.newRewrite()) without providing a map
    RewriteFiles rewrite = table.newRewrite();
    rewrite.deleteFile(source);
    rewrite.addFile(targetFile);
    rewrite.commit();

    // No auto-generation: manifest should NOT have a compaction map
    ManifestFile addedManifest =
        table.currentSnapshot().dataManifests(table.io()).stream()
            .filter(ManifestFile::hasAddedFiles)
            .findFirst()
            .orElse(null);

    assertThat(addedManifest).isNotNull();
    assertThat(addedManifest.compactionMapLocation())
        .as("BaseRewriteFiles should NOT auto-generate maps; callers must provide them explicitly")
        .isNull();
  }

  private RewriteFileGroup createFileGroup(
      Table table, List<DataFile> sourceFiles, DataFile targetFile) {
    RewriteDataFiles.FileGroupInfo info =
        ImmutableRewriteDataFiles.FileGroupInfo.builder()
            .globalIndex(0)
            .partitionIndex(0)
            .partition(org.apache.iceberg.data.GenericRecord.create(table.spec().partitionType()))
            .build();

    // Create FileScanTasks from source files
    String schemaString = SchemaParser.toJson(table.schema());
    String specString = PartitionSpecParser.toJson(table.spec());
    List<FileScanTask> scanTasks =
        sourceFiles.stream()
            .map(
                f ->
                    (FileScanTask)
                        new BaseFileScanTask(
                            f,
                            new DeleteFile[0],
                            schemaString,
                            specString,
                            ResidualEvaluator.unpartitioned(
                                org.apache.iceberg.expressions.Expressions.alwaysTrue())))
            .collect(java.util.stream.Collectors.toList());

    RewriteFileGroup group =
        new RewriteFileGroup(info, scanTasks, table.spec().specId(), 1024 * 1024, 1024 * 1024, 1);
    group.setOutputFiles(Set.of(targetFile));

    return group;
  }
}
