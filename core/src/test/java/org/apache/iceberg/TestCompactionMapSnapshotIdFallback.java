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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests verifying that BaseRewriteFiles does NOT auto-generate compaction maps.
 *
 * <p>Auto-generation was removed because it assumed bin-pack concatenation order, which cannot be
 * verified without explicit position tracking. Callers must provide compaction maps explicitly via
 * {@link BaseRewriteFiles#setCompactionMapLocation(String)}.
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
   * Verifies that BaseRewriteFiles does not auto-generate a compaction map even when
   * validateFromSnapshot is used and compaction maps are enabled.
   */
  @Test
  public void testNoAutoGenerationWithExplicitStartingSnapshot() throws IOException {
    TableIdentifier tableIdent = TableIdentifier.of("db", "no_auto_gen_test");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    DataFile source =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(source).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    rewrite.deleteFile(source);
    rewrite.addFile(targetFile);
    rewrite.commit();

    // No auto-generation: manifest should NOT have a compaction map
    String mapLocation = findCompactionMapLocation(table, table.currentSnapshot());
    assertThat(mapLocation)
        .as("BaseRewriteFiles should not auto-generate compaction maps")
        .isNull();
  }

  /** Verifies that an explicitly provided compaction map location is preserved in the manifest. */
  @Test
  public void testExplicitMapLocationPreserved() throws IOException {
    TableIdentifier tableIdent = TableIdentifier.of("db", "explicit_map_test");
    Table table = catalog.createTable(tableIdent, SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.COMPACTION_MAP_ENABLED, "true")
        .commit();

    DataFile source =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/source.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    table.newAppend().appendFile(source).commit();

    long startingSnapshot = table.currentSnapshot().snapshotId();

    DataFile targetFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/target.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    // Build and write a compaction map explicitly
    CompactionMapBuilder builder = new CompactionMapBuilder(startingSnapshot, startingSnapshot + 1);
    builder
        .addFileMapping(source.path().toString(), targetFile.path().toString())
        .addRun(0, 0, 100);
    CompactionMap map = builder.build();

    org.apache.iceberg.io.OutputFile mapFile =
        CompactionMaps.newCompactionMapFile(table, startingSnapshot + 1);
    CompactionMaps.write(map, mapFile);

    // Provide map location explicitly
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshot);
    rewrite.deleteFile(source);
    rewrite.addFile(targetFile);
    ((BaseRewriteFiles) rewrite).setCompactionMapLocation(mapFile.location());
    rewrite.commit();

    // Explicitly provided map should be in the manifest
    String location = findCompactionMapLocation(table, table.currentSnapshot());
    assertThat(location)
        .as("Explicitly provided compaction map location should be preserved")
        .isEqualTo(mapFile.location());
  }

  private String findCompactionMapLocation(Table table, Snapshot snapshot) {
    for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
      if (manifest.compactionMapLocation() != null) {
        return manifest.compactionMapLocation();
      }
    }
    return null;
  }
}
