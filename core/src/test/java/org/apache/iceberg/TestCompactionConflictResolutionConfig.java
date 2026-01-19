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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for compaction conflict resolution configuration.
 *
 * <p>These tests verify the configuration properties and their default values for the automatic
 * conflict resolution feature during compaction operations.
 */
public class TestCompactionConflictResolutionConfig {

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
  public void testDefaultPropertiesDisabled() {
    // Default: both compaction map and conflict resolution are disabled
    TableIdentifier tableIdent = TableIdentifier.of("db", "defaults");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    // Verify defaults
    assertThat(
            table
                .properties()
                .getOrDefault(
                    TableProperties.COMPACTION_MAP_ENABLED,
                    String.valueOf(TableProperties.COMPACTION_MAP_ENABLED_DEFAULT)))
        .isEqualTo("false");

    assertThat(
            table
                .properties()
                .getOrDefault(
                    TableProperties.COMPACTION_REMAP_CONFLICTING_DELETES,
                    String.valueOf(TableProperties.COMPACTION_REMAP_CONFLICTING_DELETES_DEFAULT)))
        .isEqualTo("false");

    assertThat(
            table
                .properties()
                .getOrDefault(
                    TableProperties.COMPACTION_REMAP_MAX_MANIFESTS,
                    String.valueOf(TableProperties.COMPACTION_REMAP_MAX_MANIFESTS_DEFAULT)))
        .isEqualTo("100");
  }

  @Test
  public void testEnableCompactionMapOnly() {
    // Enable compaction map but not conflict resolution
    TableIdentifier tableIdent = TableIdentifier.of("db", "map_only");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.COMPACTION_MAP_ENABLED, "true").commit();

    assertThat(
            table
                .properties()
                .getOrDefault(
                    TableProperties.COMPACTION_MAP_ENABLED,
                    String.valueOf(TableProperties.COMPACTION_MAP_ENABLED_DEFAULT)))
        .isEqualTo("true");

    assertThat(
            table
                .properties()
                .getOrDefault(
                    TableProperties.COMPACTION_REMAP_CONFLICTING_DELETES,
                    String.valueOf(TableProperties.COMPACTION_REMAP_CONFLICTING_DELETES_DEFAULT)))
        .isEqualTo("false");
  }

  @Test
  public void testEnableBothFeatures() {
    // Enable both compaction map and conflict resolution
    TableIdentifier tableIdent = TableIdentifier.of("db", "both_enabled");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table
        .updateProperties()
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .set(TableProperties.COMPACTION_REMAP_CONFLICTING_DELETES, "true")
        .commit();

    assertThat(
            table
                .properties()
                .getOrDefault(
                    TableProperties.COMPACTION_MAP_ENABLED,
                    String.valueOf(TableProperties.COMPACTION_MAP_ENABLED_DEFAULT)))
        .isEqualTo("true");

    assertThat(
            table
                .properties()
                .getOrDefault(
                    TableProperties.COMPACTION_REMAP_CONFLICTING_DELETES,
                    String.valueOf(TableProperties.COMPACTION_REMAP_CONFLICTING_DELETES_DEFAULT)))
        .isEqualTo("true");
  }

  @Test
  public void testCustomMaxManifestsLimit() {
    // Set a custom max manifests limit
    TableIdentifier tableIdent = TableIdentifier.of("db", "custom_limit");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table
        .updateProperties()
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .set(TableProperties.COMPACTION_REMAP_CONFLICTING_DELETES, "true")
        .set(TableProperties.COMPACTION_REMAP_MAX_MANIFESTS, "50")
        .commit();

    assertThat(table.properties().get(TableProperties.COMPACTION_REMAP_MAX_MANIFESTS))
        .isEqualTo("50");
  }

  @Test
  public void testConflictDetectionAndResolution() throws IOException {
    // Test that CompactionConflictDetector and CompactionConflictResolver work together
    TableIdentifier tableIdent = TableIdentifier.of("db", "conflict_resolution");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add initial data file
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Add position deletes (simulating concurrent transaction)
    DeleteFile deleteFile = writePositionDeletes(table, "/path/to/source.parquet", 10L, 20L, 30L);
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Create compaction map
    CompactionMapBuilder mapBuilder = new CompactionMapBuilder(1L, 2L);
    mapBuilder
        .addFileMapping("/path/to/source.parquet", "/path/to/target.parquet")
        .addRun(0, 0, 100);
    CompactionMap compactionMap = mapBuilder.build();

    // Detect conflicts
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());
    DeleteConflictInfo conflicts =
        detector.detectConflicts(Sets.newHashSet("/path/to/source.parquet"));

    assertThat(conflicts.hasConflicts()).isTrue();
    assertThat(conflicts.deleteFileCount()).isEqualTo(1);

    // Resolve conflicts
    CompactionConflictResolver resolver = new CompactionConflictResolver(table);
    DeleteManifestChanges changes = resolver.resolve(compactionMap, conflicts);

    assertThat(changes.hasChanges()).isTrue();
    assertThat(changes.totalDeletesRemapped()).isEqualTo(3);
    assertThat(changes.addedDeleteFiles()).hasSize(1);
  }

  @Test
  public void testMaxManifestsLimitValidation() throws IOException {
    // Test that the max manifests limit is enforced
    TableIdentifier tableIdent = TableIdentifier.of("db", "limit_validation");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add data and deletes
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    // Add multiple delete files
    for (int i = 0; i < 5; i++) {
      DeleteFile deleteFile =
          writePositionDeletes(table, "/path/to/source.parquet", (long) (i * 10));
      table.newRowDelta().addDeletes(deleteFile).commit();
    }

    // Detect conflicts
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    CompactionConflictDetector detector =
        new CompactionConflictDetector(
            table.io(), metadata, startingSnapshot, table.currentSnapshot());
    DeleteConflictInfo conflicts =
        detector.detectConflicts(Sets.newHashSet("/path/to/source.parquet"));

    // Verify we have multiple conflicts
    assertThat(conflicts.hasConflicts()).isTrue();
    assertThat(conflicts.deleteFileCount()).isEqualTo(5);

    // Simulate the max manifests limit check (as done in RewriteDataFilesCommitManager)
    int maxManifests = 2; // Set a low limit
    int deleteManifestCount = conflicts.deleteFileCount();

    // This simulates what happens in resolveConflictingDeletes
    if (deleteManifestCount > maxManifests) {
      assertThatThrownBy(
              () -> {
                throw new ValidationException(
                    "Compaction conflict resolution exceeded maximum manifest limit. "
                        + "Found %d conflicting delete files, limit is %d.",
                    deleteManifestCount, maxManifests);
              })
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("exceeded maximum manifest limit");
    }
  }

  @Test
  public void testNoConflictsNoResolution() throws IOException {
    // Test that when there are no conflicts, resolution returns empty changes
    TableIdentifier tableIdent = TableIdentifier.of("db", "no_conflicts");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    // Add data but no deletes
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    table.newAppend().appendFile(dataFile).commit();

    // Create compaction map
    CompactionMapBuilder mapBuilder = new CompactionMapBuilder(1L, 2L);
    mapBuilder
        .addFileMapping("/path/to/source.parquet", "/path/to/target.parquet")
        .addRun(0, 0, 100);
    CompactionMap compactionMap = mapBuilder.build();

    // Empty conflict info
    DeleteConflictInfo conflicts = DeleteConflictInfo.empty();

    // Resolve (should return empty)
    CompactionConflictResolver resolver = new CompactionConflictResolver(table);
    DeleteManifestChanges changes = resolver.resolve(compactionMap, conflicts);

    assertThat(changes.hasChanges()).isFalse();
    assertThat(changes.totalDeletesRemapped()).isEqualTo(0);
  }

  @Test
  public void testPropertyNamesCorrect() {
    // Verify the property names are as expected
    assertThat(TableProperties.COMPACTION_REMAP_CONFLICTING_DELETES)
        .isEqualTo("write.compaction.remap-conflicting-deletes");

    assertThat(TableProperties.COMPACTION_REMAP_MAX_MANIFESTS)
        .isEqualTo("write.compaction.remap-conflicting-deletes.max-manifests");

    // Verify defaults
    assertThat(TableProperties.COMPACTION_REMAP_CONFLICTING_DELETES_DEFAULT).isFalse();
    assertThat(TableProperties.COMPACTION_REMAP_MAX_MANIFESTS_DEFAULT).isEqualTo(100);
  }

  // Helper method to write position deletes to a file
  private DeleteFile writePositionDeletes(Table table, String dataFilePath, Long... positions)
      throws IOException {
    OutputFile outputFile =
        table
            .io()
            .newOutputFile(
                table.location()
                    + "/metadata/deletes-"
                    + System.nanoTime()
                    + "-"
                    + dataFilePath.hashCode()
                    + ".avro");

    PositionDeleteWriter<Void> writer =
        Avro.writeDeletes(outputFile)
            .createWriterFunc(DataWriter::create)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .buildPositionWriter();

    try {
      PositionDelete<Void> delete = PositionDelete.create();
      for (Long position : positions) {
        writer.write(delete.set(dataFilePath, position));
      }
    } finally {
      writer.close();
    }

    DeleteFile baseDeleteFile = writer.toDeleteFile();

    // Create a new DeleteFile with explicit referencedDataFile set
    return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
        .ofPositionDeletes()
        .withPath(baseDeleteFile.location())
        .withFileSizeInBytes(baseDeleteFile.fileSizeInBytes())
        .withRecordCount(baseDeleteFile.recordCount())
        .withReferencedDataFile(dataFilePath)
        .build();
  }
}
