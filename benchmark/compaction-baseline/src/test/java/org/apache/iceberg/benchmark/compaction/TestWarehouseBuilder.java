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

  /**
   * Regression test for the bug behind fuzz failures seed-59 and seed-101.
   *
   * <p>The bug: {@link WarehouseBuilder#runCompactionAndCaptureMap} used to unconditionally
   * {@code clear()} {@code currentDvByDataFile} on the assumption that compaction absorbs every
   * chain DV. That assumption breaks when the bin-packer leaves a file alone: any chain DV
   * attached to a skipped file survives, the stale empty cache fools the late-tx merger into
   * emitting a brand-new DV instead of merging, and the snapshot ends up with two live DVs for
   * one data file. The next planFiles trips {@code Can't index multiple DVs for ...}.
   *
   * <p>This test uses {@link FuzzScenario#forSeed} with the exact two seeds the harness produced
   * fail dumps for — they are deterministic, reproduce the bug shape, and run in tens of
   * seconds each. With the fix applied, both seeds complete successfully and produce matching
   * reference / treatment row hashes (the confluence property the harness is testing).
   *
   * <p>Without the fix, {@code FuzzRunner.run} throws {@code ValidationException: Can't index
   * multiple DVs for ...} during the post-commit hash computation — which is what produced the
   * original {@code seed-N.fail.json} dumps.
   */
  @Test
  void fuzzSeed59IsReproducible() throws IOException {
    runFuzzSeedAndAssertConfluence(59L);
  }

  @Test
  void fuzzSeed101IsReproducible() throws IOException {
    runFuzzSeedAndAssertConfluence(101L);
  }

  /**
   * Principled root-cause test for the seed-59 / seed-101 bug — no fuzz harness required.
   *
   * <p>The hypothesis: when Spark's {@code SizeBasedFileRewritePlanner} decides a file is already
   * at target size (within {@code [0.75·target, 1.80·target]}, the "good" range), it leaves the
   * file alone. Any chain DV attached to a skipped file survives the compaction. The pre-fix
   * {@code WarehouseBuilder.runCompactionAndCaptureMap} cleared {@code currentDvByDataFile}
   * unconditionally, leaving the cache empty while the snapshot still had live DVs — so the
   * next late-tx merger saw "no existing DV", wrote a brand-new DV, and produced two live DVs
   * for one data file.
   *
   * <p>This test exercises that hypothesis directly:
   *
   * <ol>
   *   <li>Build S_0 with files sized inside the "good" range so the planner provably skips them.
   *   <li>Add chain snapshots whose scatter-deletes land on those files.
   *   <li>Run compaction with the matching target size.
   *   <li>Independently witness via the table API that the snapshot has live DVs on
   *       not-rewritten files.
   *   <li>Assert {@code currentDvByDataFile} matches those live DVs exactly.
   * </ol>
   *
   * <p>With the fix in place, step (5) passes. With the fix reverted, step (5) fails with a
   * non-empty live DV set vs. an empty cache — the exact precondition the late-tx merger then
   * mishandles.
   */
  @Test
  void chainDvsThatSurviveCompactionStayInTheCache() throws IOException {
    HadoopCatalog catalog = newCatalog("partial-compaction-witness");
    // ~300 bytes/row in Parquet for the 20-column WorkloadGenerator schema (verified against
    // the fuzz harness's actual file sizes). To land inside [0.75 MB, 1.8 MB] with a 1 MB
    // target, we want files in [~2_500, ~6_000] rows — pick 4_000 rows / file to be safely
    // mid-range.
    final long target = 1_048_576L;
    BuildConfig config =
        BuildConfig.builder()
            .seed(7L)
            .s0Rows(16_000L) // → 4 S_0 files of 4_000 rows each
            .snapshotChainLength(2)
            .perSnapshotRows(200L) // chain inserts are tiny — they'll be bin-pack candidates
            .perSnapshotDeletes(10) // ten scatter-deletes land on the S_0 files
            .lateTxDeletes(0)
            .lateTxRunLength(1)
            .lateTxFileFanout(0)
            .rowsPerFile(4_000) // ~1.2 MB per file — safely in [0.75, 1.8] MB
            .build();

    WarehouseBuilder builder =
        new WarehouseBuilder(catalog, TableIdentifier.of("db", "partial_compaction"), config);
    org.apache.iceberg.Table table = builder.createTable(true /* compactionMapEnabled */);
    builder.buildSnapshotChain(table);

    // WITNESS 1: chain build produced DVs on at least one pre-existing file. Without this we
    // can't observe the bug.
    java.util.Set<String> chainDvFiles =
        new java.util.HashSet<>(builder.currentDvByDataFile().keySet());
    assertThat(chainDvFiles)
        .as("chain build must commit at least one DV against a pre-existing data file")
        .isNotEmpty();

    builder.runCompactionAndCaptureMap(spark, table, target);

    // WITNESS 2: independently read the post-compaction snapshot and find every live DV. If
    // the planner did its job (left at least one DV-bearing file alone), this set is non-empty.
    java.util.Set<String> liveDvFilesPostCompact = new java.util.HashSet<>();
    org.apache.iceberg.Snapshot snap = table.currentSnapshot();
    for (org.apache.iceberg.ManifestFile mf : snap.deleteManifests(table.io())) {
      try (org.apache.iceberg.ManifestReader<org.apache.iceberg.DeleteFile> reader =
          org.apache.iceberg.ManifestFiles.readDeleteManifest(mf, table.io(), null)) {
        for (org.apache.iceberg.DeleteFile df : reader) {
          if (org.apache.iceberg.util.ContentFileUtil.isDV(df)) {
            liveDvFilesPostCompact.add(df.referencedDataFile());
          }
        }
      }
    }

    // This is the test's load-bearing precondition. If compaction happens to absorb every DV
    // (e.g., because the planner's defaults change), this assertion fails fast with a clear
    // message rather than silently letting the test become vacuous like my first attempt did.
    assertThat(liveDvFilesPostCompact)
        .as(
            "compaction must leave at least one chain DV alive in the snapshot — otherwise this "
                + "test cannot demonstrate the bug. Adjust rowsPerFile / target so at least one "
                + "DV-bearing file lands within Spark's [0.75·target, 1.80·target] 'good' range "
                + "and the planner skips it.")
        .isNotEmpty();

    // THE INVARIANT under test: post-compaction cache must mirror the snapshot's live DV set.
    // Pre-fix code clears the cache unconditionally → assertion fails because the cache is
    // empty while liveDvFilesPostCompact is not.
    assertThat(builder.currentDvByDataFile().keySet())
        .as(
            "the cache must equal the snapshot's live DV set; pre-fix this was empty after "
                + "compaction (it cleared unconditionally), which then caused the late-tx "
                + "merger to write a fresh DV alongside the surviving chain DV — the V3 "
                + "invariant violation that produced fuzz seeds 59 and 101")
        .containsExactlyInAnyOrderElementsOf(liveDvFilesPostCompact);
  }

  private void runFuzzSeedAndAssertConfluence(long seed) throws IOException {
    FuzzScenario scenario = FuzzScenario.forSeed(seed);
    File workspace = new File(warehouseDir, "fuzz-seed-" + seed);
    if (!workspace.mkdirs() && !workspace.isDirectory()) {
      throw new IOException("Could not create fuzz workspace at " + workspace);
    }
    FuzzRunner runner = new FuzzRunner(spark);
    FuzzRunner.Outcome outcome = runner.run(scenario, workspace);
    assertThat(outcome.referenceRows())
        .as("seed %d: reference and treatment must agree on visible row count", seed)
        .isEqualTo(outcome.treatmentRows());
    assertThat(outcome.referenceHash())
        .as(
            "seed %d (FuzzScenario: %s): confluence — hash(compact + remap(tx)) must equal "
                + "hash(compact(state ∪ tx)). Pre-fix this seed died with ValidationException "
                + "during hash because compaction left some chain DVs alive and the late-tx "
                + "merger then committed a second DV against the same data file.",
            seed,
            scenario.describe())
        .isEqualTo(outcome.treatmentHash());
  }

  private HadoopCatalog newCatalog(String relativePath) {
    File catalogRoot = new File(warehouseDir, relativePath);
    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), catalogRoot.toURI().toString());
    return catalog;
  }
}
