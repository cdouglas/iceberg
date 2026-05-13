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
import static org.mockito.Mockito.mockConstruction;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PositionDeleteRemapper;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

/**
 * M4 — Code-Path Verification (per {@code COMPACT_SPEC.md} §M4).
 *
 * <p>Guards against silent path swaps (e.g., a stub that pre-computes the result in untimed
 * setup). Runs {@link TreatmentTimedRegion} against a small fixture and verifies execution
 * actually flows through:
 *
 * <ul>
 *   <li>{@link org.apache.iceberg.spark.actions.SparkCompactionConflictResolver#resolve}
 *   <li>{@link PositionDeleteRemapper#remapDVBulk}
 * </ul>
 *
 * <p>Implementation: Mockito's {@link MockedConstruction} intercepts every {@code new
 * PositionDeleteRemapper(...)} call inside the JVM during the timed region, replacing the
 * constructed instance with a mock that delegates to a hand-instantiated real remapper. After
 * the timed region we verify both that the constructor fired and that {@code remapDVBulk} was
 * actually invoked on the resulting mock.
 */
class TestCodePathVerification {

  private static SparkSession spark;

  @TempDir File tempDir;

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
            .config("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.default_iceberg.type", "hadoop")
            .config(
                "spark.sql.catalog.default_iceberg.warehouse",
                "/tmp/compaction-baseline-tests-m4")
            .config(
                "spark.sql.catalog.default_cache_iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.default_cache_iceberg.type", "hadoop")
            .config(
                "spark.sql.catalog.default_cache_iceberg.warehouse",
                "/tmp/compaction-baseline-tests-m4-cache")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  private static BuildConfig microConfig() {
    return BuildConfig.builder()
        .seed(444L)
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
  void treatmentTimedRegionExercisesResolverAndRemapper() throws IOException {
    // Build a small treatment warehouse so the timed region has something to do.
    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), tempDir.toURI().toString());
    WarehouseBuilder builder =
        new WarehouseBuilder(catalog, TableIdentifier.of("db", "m4"), microConfig());
    builder.buildTreatment(spark);
    Table table = catalog.loadTable(TableIdentifier.of("db", "m4"));

    StageMetricsCollector collector = new StageMetricsCollector();
    collector.attach(spark);
    try {
      TreatmentTimedRegion region = new TreatmentTimedRegion(spark, collector);
      TreatmentTimedRegion.Prep prep = region.prepare(table);

      AtomicInteger remapBulkInvocations = new AtomicInteger();
      AtomicBoolean constructorFired = new AtomicBoolean();

      try (MockedConstruction<PositionDeleteRemapper> mocked =
          mockConstruction(
              PositionDeleteRemapper.class,
              (mockInstance, context) -> {
                constructorFired.set(true);
                // Construct a real PositionDeleteRemapper using the same constructor arg the
                // resolver passed (the CompactionMap), and have the mock's remapDVBulk delegate
                // to the real impl so the rest of the pipeline still produces valid output.
                Object firstArg = context.arguments().get(0);
                PositionDeleteRemapper real;
                if (firstArg instanceof org.apache.iceberg.CompactionMap) {
                  real = new PositionDeleteRemapper((org.apache.iceberg.CompactionMap) firstArg);
                } else {
                  real =
                      new PositionDeleteRemapper(
                          (org.apache.iceberg.CompactionMapChain) firstArg);
                }
                Mockito.doAnswer(
                        invocation -> {
                          remapBulkInvocations.incrementAndGet();
                          return real.remapDVBulk(
                              invocation.getArgument(0), invocation.getArgument(1));
                        })
                    .when(mockInstance)
                    .remapDVBulk(Mockito.any(), Mockito.any());
              })) {

        IterationResult result =
            new IterationResult(
                "treatment", microConfig().lateTxDeletes(), 1 /* iteration */, false);
        region.runTimed(table, prep, result);

        // 1. The resolver's constructor for PositionDeleteRemapper fired (it would not have
        //    fired if the resolver was stubbed to return a precomputed list).
        assertThat(constructorFired.get())
            .as("PositionDeleteRemapper ctor must have fired inside the timed region")
            .isTrue();
        // 2. The list of intercepted constructions is non-empty.
        assertThat(mocked.constructed())
            .as("MockedConstruction.constructed should record at least one PositionDeleteRemapper")
            .isNotEmpty();
        // 3. remapDVBulk was actually invoked (not just constructed).
        assertThat(remapBulkInvocations.get())
            .as("PositionDeleteRemapper.remapDVBulk must have been called at least once")
            .isGreaterThan(0);
        // 4. Mockito-side cross-check: every constructed mock had remapDVBulk verified.
        for (PositionDeleteRemapper mock : mocked.constructed()) {
          Mockito.verify(mock, Mockito.atLeastOnce())
              .remapDVBulk(Mockito.any(), Mockito.any());
        }
      }
    } finally {
      collector.detach(spark);
    }
  }

  // Touch the static method on PositionDeleteRemapper so the test continues to compile if its
  // return type changes — catches API drift that would invalidate this code-path guard.
  @SuppressWarnings("unused")
  private static Map<String, Set<Long>> typeCheck(PositionDeleteRemapper r) {
    return r.remapDVBulk(null, null);
  }
}
