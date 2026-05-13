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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test for {@link RunMain} at micro scale. Builds a baseline + treatment warehouse via
 * {@link WarehouseBuilder}, tars each, invokes {@link RunMain#run} in-process, and checks the
 * resulting {@code results.jsonl} structure.
 */
class TestRunMain {

  private static SparkSession spark;

  @TempDir Path tempDir;

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
                "spark.sql.catalog.default_iceberg.warehouse", "/tmp/compaction-baseline-tests-run")
            .config(
                "spark.sql.catalog.default_cache_iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.default_cache_iceberg.type", "hadoop")
            .config(
                "spark.sql.catalog.default_cache_iceberg.warehouse",
                "/tmp/compaction-baseline-tests-run-cache")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

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
  void runBaselineProducesValidJsonl() throws IOException {
    Path tar = buildAndTar("baseline", microConfig(101L));
    Path workingDir = tempDir.resolve("run-baseline");
    Path output = workingDir.resolve("results.jsonl");

    int exit = invokeRunMain("baseline", tar, workingDir, output, 2 /* iterations */);
    assertThat(exit).isZero();

    List<JsonNode> records = readJsonl(output);
    assertThat(records).hasSize(2);
    for (int i = 0; i < records.size(); i++) {
      JsonNode rec = records.get(i);
      assertThat(rec.get("variant").asText()).isEqualTo("baseline");
      assertThat(rec.get("iteration").asInt()).isEqualTo(i + 1);
      assertThat(rec.get("warmup").asBoolean()).isFalse();
      assertThat(rec.get("wall_clock_ms").asLong()).isGreaterThan(0L);
      // Micro fixtures don't hit M6's production thresholds; runner marks them invalid but still
      // emits the line. Check the file shape, not the value of `valid`.
      assertThat(rec.has("stage_ms")).isTrue();
      assertThat(rec.has("files_read")).isTrue();
      assertThat(rec.has("files_written")).isTrue();
      assertThat(rec.has("snapshot_id_after")).isTrue();
    }
  }

  @Test
  void runTreatmentProducesValidJsonlWithStages() throws IOException {
    Path tar = buildAndTar("treatment", microConfig(202L));
    Path workingDir = tempDir.resolve("run-treatment");
    Path output = workingDir.resolve("results.jsonl");

    int exit = invokeRunMain("treatment", tar, workingDir, output, 2 /* iterations */);
    assertThat(exit).isZero();

    List<JsonNode> records = readJsonl(output);
    assertThat(records).hasSize(2);
    for (JsonNode rec : records) {
      assertThat(rec.get("variant").asText()).isEqualTo("treatment");
      assertThat(rec.get("wall_clock_ms").asLong()).isGreaterThan(0L);
      assertThat(rec.get("compaction_map_runs").asInt()).isGreaterThan(0);
      // Treatment exposes resolve/commit/scan_write stage entries (set in TreatmentTimedRegion).
      JsonNode stages = rec.get("stage_ms");
      assertThat(stages.has("resolve")).isTrue();
      assertThat(stages.has("commit")).isTrue();
      // Treatment iterations are not subject to M6 (baseline-only); they should be valid.
      assertThat(rec.get("valid").asBoolean()).isTrue();
    }
  }

  private Path buildAndTar(String variant, BuildConfig config) throws IOException {
    Path catalogRoot = tempDir.resolve("fixture-" + variant + "-catalog");
    Files.createDirectories(catalogRoot);
    try (HadoopCatalog catalog =
        new HadoopCatalog(new Configuration(), catalogRoot.toUri().toString())) {
      WarehouseBuilder builder =
          new WarehouseBuilder(catalog, TableIdentifier.of("db", variant), config);
      if ("baseline".equals(variant)) {
        builder.buildBaseline();
      } else {
        builder.buildTreatment(spark);
      }
    }
    Path tar = tempDir.resolve("fixture-" + variant + ".tar");
    TarUtils.tarDirectory(catalogRoot, tar, "warehouse");
    return tar;
  }

  private int invokeRunMain(String variant, Path tar, Path workingDir, Path output, int iterations)
      throws IOException {
    String[] argv = {
      "--state-tar",
      tar.toString(),
      "--working-dir",
      workingDir.toString(),
      "--variant",
      variant,
      "--k",
      "20",
      "--iterations",
      Integer.toString(iterations),
      "--warmup",
      "1",
      "--output",
      output.toString(),
      "--table-name",
      "db." + variant,
      "--driver-memory",
      "1g",
      "--spark-master",
      "local[2]",
      "--skip-m6",
    };
    return RunMain.runWithSpark(RunMain.Args.parse(argv), spark);
  }

  private List<JsonNode> readJsonl(Path path) throws IOException {
    assertThat(path).exists();
    ObjectMapper mapper = new ObjectMapper();
    List<JsonNode> records = new java.util.ArrayList<>();
    for (String line : Files.readAllLines(path)) {
      if (line.isBlank()) {
        continue;
      }
      records.add(mapper.readTree(line));
    }
    return records;
  }
}
