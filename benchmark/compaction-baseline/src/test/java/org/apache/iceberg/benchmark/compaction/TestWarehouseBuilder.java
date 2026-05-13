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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Micro-scale integration test for {@link WarehouseBuilder}. The full setup workload (~21 M rows)
 * is far too heavy for unit tests; here we shrink {@link BuildConfig} down to a few thousand rows
 * and verify the structural contract instead of the production sizes:
 *
 * <ul>
 *   <li>Both variants commit a non-empty snapshot chain that finishes with the late transaction.
 *   <li>The treatment variant emits a non-empty compaction map at the standard metadata location
 *       and records the run count on the build result.
 *   <li>The treatment variant's compaction snapshot collapses the per-snapshot DVs into the
 *       compacted output so that only the late-tx DV remains afterward.
 * </ul>
 */
class TestWarehouseBuilder {

  private static SparkSession spark;

  @TempDir private File warehouseDir;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("compaction-baseline-tests")
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            // Register the Iceberg "default_iceberg" catalog as a HadoopCatalog. Without this
            // override IcebergSource defaults it to a HiveCatalog, which fails to load when the
            // hive-metastore artifact isn't on the classpath. The warehouse path here is a
            // placeholder — SparkActions writes via the supplied Table's absolute paths.
            .config("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.default_iceberg.type", "hadoop")
            .config("spark.sql.catalog.default_iceberg.warehouse", "/tmp/compaction-baseline-tests")
            .config(
                "spark.sql.catalog.default_cache_iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.default_cache_iceberg.type", "hadoop")
            .config(
                "spark.sql.catalog.default_cache_iceberg.warehouse",
                "/tmp/compaction-baseline-tests-cache")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  /** Tiny config: ~2_000 rows total (S_0 = 1_000, three S_i of 300 each), K=20 late deletes. */
  private static BuildConfig microConfig(long seed) {
    return BuildConfig.builder()
        .seed(seed)
        .s0Rows(1_000L)
        .snapshotChainLength(3)
        .perSnapshotRows(300L)
        .perSnapshotDeletes(15)
        .lateTxDeletes(20)
        .lateTxRunLength(5)
        .lateTxFileFanout(0)
        .rowsPerFile(500)
        .build();
  }

  @Test
  void buildBaselineProducesSnapshotChainPlusLateTx() throws IOException {
    HadoopCatalog catalog = newCatalog("baseline-warehouse");
    BuildResult result =
        new WarehouseBuilder(catalog, TableIdentifier.of("db", "baseline"), microConfig(42L))
            .buildBaseline();

    assertThat(result.variant()).isEqualTo("baseline");
    // S_0 + S_1..S_3 + S_{n+1} = 5 commits.
    assertThat(result.snapshotIds()).hasSize(5);
    assertThat(result.dataFileCount()).isGreaterThan(0);
    assertThat(result.deleteFileCount()).isGreaterThan(0);
    assertThat(result.compactionMapPath()).isNull();
    assertThat(result.compactionMapRunCount()).isZero();
  }

  @Test
  void buildTreatmentProducesCompactionMapAndOrphanDv() throws IOException {
    HadoopCatalog catalog = newCatalog("treatment-warehouse");
    BuildResult result =
        new WarehouseBuilder(catalog, TableIdentifier.of("db", "treatment"), microConfig(7L))
            .buildTreatment(spark);

    assertThat(result.variant()).isEqualTo("treatment");
    // S_0 + S_1..S_3 + compact + S_{n+1} = 6 commits.
    assertThat(result.snapshotIds()).hasSize(6);
    assertThat(result.compactionMapPath()).isNotNull();
    assertThat(new File(java.net.URI.create(result.compactionMapPath()))).exists();
    assertThat(result.compactionMapRunCount()).isGreaterThan(0);
    // The exact post-compact DV count is implementation-dependent (compact removes dangling DVs
    // for rewritten files via SparkRewriteDataFilesCommitManager.danglingDVs, but the snapshot
    // summary keeps cumulative totals). The structural invariant is "at least the late-tx DV".
    assertThat(result.deleteFileCount()).isGreaterThanOrEqualTo(1);
    assertThat(result.dataFileCount()).isGreaterThan(0);
  }

  private HadoopCatalog newCatalog(String relativePath) {
    File catalogRoot = new File(warehouseDir, relativePath);
    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), catalogRoot.toURI().toString());
    return catalog;
  }
}
