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
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for compaction map support in manifests and rewrite operations.
 *
 * <p>These tests demonstrate the integration points for compaction maps without requiring full
 * Spark-level instrumentation. The tests show:
 *
 * <ul>
 *   <li>ManifestWriter can store compaction map locations in manifest files
 *   <li>BaseRewriteFiles provides API for associating maps with rewrites
 *   <li>The infrastructure is ready for Spark-level integration
 * </ul>
 */
public class TestCompactionMapIntegration {

  @TempDir public File temp;

  @Test
  public void testManifestWriterWithCompactionMap() throws IOException {
    // Create a simple table for testing
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("test", java.util.Collections.emptyMap());
    catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of("db"));

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table");
    Table table = catalog.createTable(tableIdent, schema);

    // Generate a compaction map location
    long snapshotId = 12345L;
    String compactionMapLocation =
        CompactionMaps.newCompactionMapFile(table, snapshotId).location();

    // Create a manifest writer and set the compaction map location
    PartitionSpec spec = PartitionSpec.unpartitioned();
    OutputFile manifestFile = org.apache.iceberg.Files.localOutput(new File(temp, "manifest.avro"));

    ManifestWriter<DataFile> writer = ManifestFiles.write(2, spec, manifestFile, snapshotId);
    writer.setCompactionMapLocation(compactionMapLocation);

    // Add a data file
    DataFile dataFile =
        DataFiles.builder(spec)
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    writer.add(dataFile);
    writer.close();

    // Verify the manifest contains the compaction map location
    ManifestFile manifest = writer.toManifestFile();
    assertThat(manifest.compactionMapLocation()).isEqualTo(compactionMapLocation);

    catalog.dropTable(tableIdent);
  }

  @Test
  public void testBaseRewriteFilesCompactionMapAPI() {
    // Create a simple table
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("test", java.util.Collections.emptyMap());
    catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of("db"));

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table");
    Table table = catalog.createTable(tableIdent, schema);

    // Generate a compaction map location
    String compactionMapLocation = "s3://bucket/metadata/compaction-map-123.avro";

    // Create a rewrite operation
    RewriteFiles rewrite = table.newRewrite();

    // Cast to BaseRewriteFiles to access the API (this would be done internally)
    BaseRewriteFiles baseRewrite = (BaseRewriteFiles) rewrite;

    // Set the compaction map location
    baseRewrite.setCompactionMapLocation(compactionMapLocation);

    // Verify it can be retrieved
    assertThat(baseRewrite.compactionMapLocation()).isEqualTo(compactionMapLocation);

    // Verify fluent API works
    BaseRewriteFiles result = baseRewrite.setCompactionMapLocation("new-location");
    assertThat(result).isSameAs(baseRewrite);
    assertThat(baseRewrite.compactionMapLocation()).isEqualTo("new-location");

    // Verify null clears the location
    baseRewrite.setCompactionMapLocation(null);
    assertThat(baseRewrite.compactionMapLocation()).isNull();

    catalog.dropTable(tableIdent);
  }

  @Test
  public void testManifestWriterDefaultsToNullCompactionMap() throws IOException {
    // Create a simple table for testing
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("test", java.util.Collections.emptyMap());
    catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of("db"));

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    TableIdentifier tableIdent = TableIdentifier.of("db", "test_table");
    Table table = catalog.createTable(tableIdent, schema);

    // Create a manifest writer WITHOUT setting compaction map
    PartitionSpec spec = PartitionSpec.unpartitioned();
    OutputFile manifestFile =
        org.apache.iceberg.Files.localOutput(new File(temp, "manifest2.avro"));

    ManifestWriter<DataFile> writer = ManifestFiles.write(2, spec, manifestFile, 67890L);

    // Add a data file
    DataFile dataFile =
        DataFiles.builder(spec)
            .withPath("/path/to/data2.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();
    writer.add(dataFile);
    writer.close();

    // Verify the manifest has null compaction map location (backward compatibility)
    ManifestFile manifest = writer.toManifestFile();
    assertThat(manifest.compactionMapLocation()).isNull();

    catalog.dropTable(tableIdent);
  }
}
