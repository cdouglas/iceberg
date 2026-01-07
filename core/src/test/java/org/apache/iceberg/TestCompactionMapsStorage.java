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
import java.util.regex.Pattern;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestCompactionMapsStorage {

  @TempDir public File temp;

  @Test
  public void testTablePropertiesDefaults() {
    // Verify compaction map properties are defined with correct defaults
    assertThat(TableProperties.COMPACTION_MAP_ENABLED).isEqualTo("write.compaction-map.enabled");
    assertThat(TableProperties.COMPACTION_MAP_ENABLED_DEFAULT).isFalse();

    assertThat(TableProperties.COMPACTION_MAP_TARGET_SIZE_BYTES)
        .isEqualTo("write.compaction-map.target-size-bytes");
    assertThat(TableProperties.COMPACTION_MAP_TARGET_SIZE_BYTES_DEFAULT)
        .isEqualTo(8 * 1024 * 1024); // 8 MB
  }

  @Test
  public void testCompactionMapFileLocationGeneration() {
    // Create test catalog and table
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("test", java.util.Collections.emptyMap());

    // Create namespace
    catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of("db"));

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table");
    Table table = catalog.createTable(tableIdent, schema);

    // Generate a compaction map file location
    long snapshotId = 12345L;
    OutputFile outputFile = CompactionMaps.newCompactionMapFile(table, snapshotId);

    // Verify the location follows the expected pattern
    String location = outputFile.location();

    // Should contain "metadata" directory
    assertThat(location).contains("/metadata/");

    // Should match pattern: compaction-map-<snapshotId>-<uuid>.avro
    String fileName = location.substring(location.lastIndexOf('/') + 1);
    Pattern expectedPattern =
        Pattern.compile("compaction-map-" + snapshotId + "-[0-9a-f\\-]+\\.avro");
    assertThat(fileName).matches(expectedPattern);

    catalog.dropTable(tableIdent);
  }

  @Test
  public void testCompactionMapFileLocationWithCustomMetadataPath() {
    // Create test catalog and table with custom metadata location
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("test", java.util.Collections.emptyMap());

    // Create namespace
    catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of("db"));

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    String customMetadataPath = "s3://custom-bucket/metadata";

    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table_custom");
    Table table =
        catalog
            .buildTable(tableIdent, schema)
            .withProperty(TableProperties.WRITE_METADATA_LOCATION, customMetadataPath)
            .create();

    // Generate a compaction map file location
    long snapshotId = 67890L;
    OutputFile outputFile = CompactionMaps.newCompactionMapFile(table, snapshotId);

    // Verify the location uses custom metadata path
    String location = outputFile.location();

    // Should start with custom metadata path
    assertThat(location).startsWith(customMetadataPath);

    // Should still match the file name pattern
    String fileName = location.substring(location.lastIndexOf('/') + 1);
    Pattern expectedPattern =
        Pattern.compile("compaction-map-" + snapshotId + "-[0-9a-f\\-]+\\.avro");
    assertThat(fileName).matches(expectedPattern);

    catalog.dropTable(tableIdent);
  }

  @Test
  public void testMultipleCompactionMapFilesHaveUniqueNames() {
    // Create test catalog and table
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("test", java.util.Collections.emptyMap());

    // Create namespace
    catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of("db"));

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table_unique");
    Table table = catalog.createTable(tableIdent, schema);

    long snapshotId = 11111L;

    // Generate multiple locations for the same snapshot
    OutputFile outputFile1 = CompactionMaps.newCompactionMapFile(table, snapshotId);
    OutputFile outputFile2 = CompactionMaps.newCompactionMapFile(table, snapshotId);
    OutputFile outputFile3 = CompactionMaps.newCompactionMapFile(table, snapshotId);

    // All locations should be unique (due to UUID)
    assertThat(outputFile1.location()).isNotEqualTo(outputFile2.location());
    assertThat(outputFile1.location()).isNotEqualTo(outputFile3.location());
    assertThat(outputFile2.location()).isNotEqualTo(outputFile3.location());

    // But all should have the same snapshot ID in the name
    assertThat(outputFile1.location()).contains("compaction-map-" + snapshotId);
    assertThat(outputFile2.location()).contains("compaction-map-" + snapshotId);
    assertThat(outputFile3.location()).contains("compaction-map-" + snapshotId);

    catalog.dropTable(tableIdent);
  }
}
