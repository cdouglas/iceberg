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
package org.apache.iceberg.benchmark.compaction;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that {@link WarehouseBuilder} honors {@link BuildConfig#formatVersion()} and
 * {@link BuildConfig#upgradeAfterChain()}. Bypasses Spark and the full {@code buildBaseline} flow
 * to keep this a fast unit-style check; we only need to assert that the chain commits succeed
 * and that the resulting on-disk metadata says what we expect.
 */
class TestWarehouseBuilderFormatMix {

  @TempDir File warehouseDir;

  @Test
  void v3BuildsTableAtFormatVersionThree() throws IOException {
    BuildConfig cfg =
        BuildConfig.builder()
            .seed(1L)
            .s0Rows(2_000L)
            .snapshotChainLength(2)
            .perSnapshotRows(200L)
            .perSnapshotDeletes(3)
            .rowsPerFile(500)
            .lateTxDeletes(0)
            .lateTxRunLength(1)
            .formatVersion(3)
            .upgradeAfterChain(false)
            .build();

    Table table = buildChain(cfg, "v3");
    assertThat(formatVersion(table)).isEqualTo(3);
    // V3 emits DVs as deletes; no parquet position-delete files in the snapshot chain.
    assertThat(deleteFileContents(table)).containsOnly(FileContent.POSITION_DELETES);
    assertThat(allDeletesAreDvs(table)).isTrue();
  }

  @Test
  void v2BuildsTableAtFormatVersionTwoWithPositionDeleteFiles() throws IOException {
    BuildConfig cfg =
        BuildConfig.builder()
            .seed(2L)
            .s0Rows(2_000L)
            .snapshotChainLength(2)
            .perSnapshotRows(200L)
            .perSnapshotDeletes(3)
            .rowsPerFile(500)
            .lateTxDeletes(0)
            .lateTxRunLength(1)
            .formatVersion(2)
            .upgradeAfterChain(false)
            .build();

    Table table = buildChain(cfg, "v2");
    assertThat(formatVersion(table)).isEqualTo(2);
    // V2 emits parquet position-delete files, not DVs.
    assertThat(allDeletesAreDvs(table)).isFalse();
  }

  @Test
  void upgradeAfterChainBumpsFormatVersionToThree() throws IOException {
    BuildConfig cfg =
        BuildConfig.builder()
            .seed(3L)
            .s0Rows(2_000L)
            .snapshotChainLength(2)
            .perSnapshotRows(200L)
            .perSnapshotDeletes(3)
            .rowsPerFile(500)
            .lateTxDeletes(0)
            .lateTxRunLength(1)
            .formatVersion(2)
            .upgradeAfterChain(true)
            .build();

    HadoopCatalog catalog =
        new HadoopCatalog(new Configuration(), warehouseDir.toURI().toString());
    TableIdentifier ident = TableIdentifier.of("db", "upgrade");
    WarehouseBuilder builder = new WarehouseBuilder(catalog, ident, cfg);
    Table table = builder.createTable(false /* compactionMapEnabled */);
    builder.buildSnapshotChain(table);
    assertThat(formatVersion(table)).isEqualTo(2);
    // Pre-upgrade chain wrote PD files.
    boolean preUpgradeAllDvs = allDeletesAreDvs(table);
    assertThat(preUpgradeAllDvs).isFalse();

    builder.maybeUpgradeFormat(table);
    table.refresh();
    assertThat(formatVersion(table)).isEqualTo(3);
    // Pre-upgrade PD files remain in the manifest after upgrade (they're not migrated).
    assertThat(allDeletesAreDvs(table)).isFalse();
  }

  @Test
  void formatVersionFiveRejectedByBuildConfig() {
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> BuildConfig.builder().formatVersion(5).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void upgradeFlagOnlyValidForV2() {
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                BuildConfig.builder()
                    .formatVersion(3)
                    .upgradeAfterChain(true)
                    .build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  private Table buildChain(BuildConfig cfg, String name) throws IOException {
    HadoopCatalog catalog =
        new HadoopCatalog(new Configuration(), warehouseDir.toURI().toString());
    TableIdentifier ident = TableIdentifier.of("db", name);
    WarehouseBuilder builder = new WarehouseBuilder(catalog, ident, cfg);
    Table table = builder.createTable(false /* compactionMapEnabled */);
    builder.buildSnapshotChain(table);
    return table;
  }

  private static int formatVersion(Table table) {
    return ((HasTableOperations) table).operations().current().formatVersion();
  }

  private static java.util.EnumSet<FileContent> deleteFileContents(Table table) throws IOException {
    java.util.EnumSet<FileContent> contents = java.util.EnumSet.noneOf(FileContent.class);
    if (table.currentSnapshot() == null) {
      return contents;
    }
    for (ManifestFile manifest : table.currentSnapshot().deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), null)) {
        for (DeleteFile df : reader) {
          contents.add(df.content());
        }
      }
    }
    return contents;
  }

  private static boolean allDeletesAreDvs(Table table) throws IOException {
    if (table.currentSnapshot() == null) {
      return false;
    }
    boolean sawAny = false;
    for (ManifestFile manifest : table.currentSnapshot().deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), null)) {
        for (DeleteFile df : reader) {
          sawAny = true;
          if (!org.apache.iceberg.util.ContentFileUtil.isDV(df)) {
            return false;
          }
        }
      }
    }
    return sawAny;
  }
}
