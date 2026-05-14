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
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Determinism guard for the M1 fuzz harness, per {@code COMPACT_SPEC.md} §M1:
 * "Given a seed, two consecutive runs MUST produce identical operation sequences and identical
 * hashes."
 *
 * <p>The test runs the SAME seed twice through both {@link FuzzScenario#forSeed} (operation
 * sequence) and {@link FuzzRunner#run} (resulting hashes). Byte-identical equality of both is
 * the test bar — any deviation indicates an unseeded source of nondeterminism (clocks, hash-map
 * iteration order, thread scheduling).
 */
class TestFuzzDeterminism {

  private static SparkSession spark;

  @TempDir File tempDir;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("compaction-baseline-fuzz-determinism")
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.default_iceberg.type", "hadoop")
            .config(
                "spark.sql.catalog.default_iceberg.warehouse",
                "/tmp/compaction-baseline-fuzz-determinism")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  void scenarioGenerationIsDeterministic() {
    FuzzScenario a = FuzzScenario.forSeed(7L);
    FuzzScenario b = FuzzScenario.forSeed(7L);
    // The description includes every parameter that materially affects execution; if two runs
    // describe the same scenario byte-for-byte, the resulting operation sequence is identical.
    assertThat(b.describe()).isEqualTo(a.describe());
    assertThat(b.opsCount()).isEqualTo(a.opsCount());
    assertThat(b.buildConfig().s0Rows()).isEqualTo(a.buildConfig().s0Rows());
    assertThat(b.buildConfig().snapshotChainLength())
        .isEqualTo(a.buildConfig().snapshotChainLength());
    assertThat(b.lateTxOps().size()).isEqualTo(a.lateTxOps().size());
    for (int i = 0; i < a.lateTxOps().size(); i++) {
      assertThat(b.lateTxOps().get(i).opSeed()).isEqualTo(a.lateTxOps().get(i).opSeed());
      assertThat(b.lateTxOps().get(i).sliceOffsetFraction())
          .isEqualTo(a.lateTxOps().get(i).sliceOffsetFraction());
      assertThat(b.lateTxOps().get(i).sliceWidthFraction())
          .isEqualTo(a.lateTxOps().get(i).sliceWidthFraction());
      assertThat(b.lateTxOps().get(i).kind()).isEqualTo(a.lateTxOps().get(i).kind());
      // Per-kind payload equality is asserted transitively via describe() above — that string
      // includes every concrete subtype field, so byte-identical describe + identical kind
      // suffices to prove the operation sequences are byte-identical.
    }
  }

  @Test
  void seedHashesAreReproducible() throws IOException {
    // Use a constrained config that mimics the harness's original v3-DV-only-disjoint shape.
    // The point of this test is to assert DETERMINISM (same seed produces same hashes across
    // runs), not adversarial breadth — confluence under the full default config is exercised
    // separately by the smoke fuzz runs in scripts/fuzz.sh. Pinning to the historically-passing
    // shape keeps the test's confluence sanity-check (first.passed()) meaningful so any failure
    // here implicates the refactor, not the new code paths.
    Path configFile = tempDir.toPath().resolve("v3-pd-disjoint.json");
    Files.writeString(
        configFile,
        "{\"formatWeights\": {\"v3\": 1.0},"
            + " \"opKindWeights\": {\"positionDelete\": 1.0},"
            + " \"overlapProbability\": 0.0,"
            + " \"lateTxCount\": {\"min\": 1, \"max\": 2}}");
    FuzzConfig cfg = FuzzConfig.load(configFile);

    long detSeed = 42L;
    FuzzScenario scenario = FuzzScenario.forSeed(detSeed, cfg);
    org.slf4j.LoggerFactory.getLogger(TestFuzzDeterminism.class)
        .info("determinism-seed scenario: {}", scenario.describe());
    FuzzRunner runner = new FuzzRunner(spark);

    File firstRun = new File(tempDir, "run1");
    File secondRun = new File(tempDir, "run2");
    if (!firstRun.mkdirs() || !secondRun.mkdirs()) {
      throw new IOException("Could not create workspace dirs");
    }

    FuzzRunner.Outcome first = runner.run(scenario, firstRun);
    FuzzRunner.Outcome second = runner.run(scenario, secondRun);
    org.slf4j.LoggerFactory.getLogger(TestFuzzDeterminism.class)
        .info(
            "determinism-seed hashes: ref1={} trt1={} ref2={} trt2={}",
            first.referenceHash(),
            first.treatmentHash(),
            second.referenceHash(),
            second.treatmentHash());

    // The same seed must produce the same hashes across runs, in BOTH paths. This is the
    // strict M1 determinism property — any drift here means an unseeded source of randomness
    // (clock, hash-map iteration, thread scheduling) has crept into the workload or the
    // resolver/remapper pipeline.
    assertThat(second.referenceHash()).isEqualTo(first.referenceHash());
    assertThat(second.treatmentHash()).isEqualTo(first.treatmentHash());
    // And the property under test should hold for this representative seed in the constrained
    // baseline shape — if it doesn't, the refactor has broken the existing v3-DV path.
    assertThat(first.passed())
        .as(
            "fuzz seed %d must satisfy the confluence property under the v3-PD-disjoint config "
                + "for the determinism test to be meaningful",
            detSeed)
        .isTrue();
  }
}
