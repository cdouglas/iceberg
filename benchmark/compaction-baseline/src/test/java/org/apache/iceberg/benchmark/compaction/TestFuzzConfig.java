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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestFuzzConfig {

  @TempDir File tempDir;

  @Test
  void defaultsAreUniformAcrossFormatsAndOps() {
    FuzzConfig cfg = FuzzConfig.defaults();
    // Three format buckets, all equal weight → normalized to 1/3.
    assertThat(cfg.formatWeights()).hasSize(3);
    for (double w : cfg.formatWeights().values()) {
      assertThat(w).isCloseTo(1.0 / 3.0, org.assertj.core.data.Offset.offset(1e-9));
    }
    // Four op kinds, all equal weight → normalized to 1/4.
    assertThat(cfg.opKindWeights()).hasSize(4);
    for (double w : cfg.opKindWeights().values()) {
      assertThat(w).isCloseTo(0.25, org.assertj.core.data.Offset.offset(1e-9));
    }
    assertThat(cfg.lateTxCount().min()).isEqualTo(1);
    assertThat(cfg.lateTxCount().max()).isEqualTo(8);
    assertThat(cfg.overlapProbability()).isEqualTo(0.5);
  }

  @Test
  void nullPathYieldsDefaults() throws IOException {
    FuzzConfig cfg = FuzzConfig.load(null);
    assertThat(cfg.lateTxCount().min()).isEqualTo(FuzzConfig.defaults().lateTxCount().min());
    assertThat(cfg.lateTxCount().max()).isEqualTo(FuzzConfig.defaults().lateTxCount().max());
  }

  @Test
  void emptyFileYieldsDefaults() throws IOException {
    File f = new File(tempDir, "empty.json");
    Files.write(f.toPath(), new byte[0]);
    FuzzConfig cfg = FuzzConfig.load(f.toPath());
    assertThat(cfg.overlapProbability()).isEqualTo(FuzzConfig.defaults().overlapProbability());
  }

  @Test
  void partialConfigOverridesOnlyNamedFields() throws IOException {
    String json =
        "{\"overlapProbability\": 0.9, \"lateTxCount\": {\"min\": 2, \"max\": 4}}";
    File f = new File(tempDir, "partial.json");
    Files.writeString(f.toPath(), json);
    FuzzConfig cfg = FuzzConfig.load(f.toPath());
    assertThat(cfg.overlapProbability()).isEqualTo(0.9);
    assertThat(cfg.lateTxCount().min()).isEqualTo(2);
    assertThat(cfg.lateTxCount().max()).isEqualTo(4);
    // Other fields fall back to defaults.
    assertThat(cfg.formatWeights()).hasSize(3);
  }

  @Test
  void formatWeightsAreNormalized() throws IOException {
    String json =
        "{\"formatWeights\": {\"v2\": 2.0, \"v3\": 1.0, \"v2ThenUpgradeToV3\": 1.0}}";
    File f = new File(tempDir, "weighted.json");
    Files.writeString(f.toPath(), json);
    FuzzConfig cfg = FuzzConfig.load(f.toPath());
    assertThat(cfg.formatWeights().get(FuzzConfig.FormatBucket.V2)).isEqualTo(0.5);
    assertThat(cfg.formatWeights().get(FuzzConfig.FormatBucket.V3)).isEqualTo(0.25);
    assertThat(cfg.formatWeights().get(FuzzConfig.FormatBucket.V2_THEN_UPGRADE_TO_V3))
        .isEqualTo(0.25);
  }

  @Test
  void singleFormatBucketSurvivesNormalization() throws IOException {
    String json = "{\"formatWeights\": {\"v3\": 5.0}}";
    File f = new File(tempDir, "v3-only.json");
    Files.writeString(f.toPath(), json);
    FuzzConfig cfg = FuzzConfig.load(f.toPath());
    assertThat(cfg.formatWeights().get(FuzzConfig.FormatBucket.V2)).isEqualTo(0.0);
    assertThat(cfg.formatWeights().get(FuzzConfig.FormatBucket.V3)).isEqualTo(1.0);
  }

  @Test
  void negativeWeightRejected() {
    Map<FuzzConfig.FormatBucket, Double> weights = new HashMap<>();
    weights.put(FuzzConfig.FormatBucket.V2, -1.0);
    weights.put(FuzzConfig.FormatBucket.V3, 1.0);
    weights.put(FuzzConfig.FormatBucket.V2_THEN_UPGRADE_TO_V3, 1.0);
    FuzzConfig.JsonShape shape = new FuzzConfig.JsonShape();
    shape.formatWeights = new HashMap<>();
    shape.formatWeights.put("v2", -1.0);
    shape.formatWeights.put("v3", 1.0);
    shape.formatWeights.put("v2ThenUpgradeToV3", 1.0);
    assertThatThrownBy(shape::toConfig).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void overlapProbabilityOutOfRangeRejected() {
    FuzzConfig.JsonShape shape = new FuzzConfig.JsonShape();
    shape.overlapProbability = 1.5;
    assertThatThrownBy(shape::toConfig).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void intRangeSamplesWithinBounds() {
    FuzzConfig.IntRange range = new FuzzConfig.IntRange(3, 7);
    Random rng = new Random(0xDEADBEEFL);
    for (int i = 0; i < 1000; i++) {
      int v = range.sample(rng);
      assertThat(v).isGreaterThanOrEqualTo(3).isLessThanOrEqualTo(7);
    }
  }

  @Test
  void intRangeRejectsMinAboveMax() {
    assertThatThrownBy(() -> new FuzzConfig.IntRange(5, 4))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void weightedSamplingHitsAllBucketsForUniformConfig() {
    FuzzConfig cfg = FuzzConfig.defaults();
    Random rng = new Random(0xABCDEFL);
    int[] formatCounts = new int[FuzzConfig.FormatBucket.values().length];
    int[] opCounts = new int[FuzzConfig.OpKind.values().length];
    for (int i = 0; i < 4000; i++) {
      formatCounts[cfg.sampleFormat(rng).ordinal()]++;
      opCounts[cfg.sampleOpKind(rng).ordinal()]++;
    }
    for (int c : formatCounts) {
      assertThat(c).isGreaterThan(0);
    }
    for (int c : opCounts) {
      assertThat(c).isGreaterThan(0);
    }
  }
}
