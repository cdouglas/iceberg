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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Validates that {@link FuzzScenario#forSeed} emits each op kind and format bucket the config
 * allows, across reasonable seed sweeps. Catches accidental dead branches in the scenario sampler.
 */
class TestFuzzScenarioOpKinds {

  @TempDir Path tempDir;

  @Test
  void defaultsCoverAllOpKindsAndFormatsInASmallSweep() {
    FuzzConfig cfg = FuzzConfig.defaults();
    Set<FuzzConfig.OpKind> kindsSeen = EnumSet.noneOf(FuzzConfig.OpKind.class);
    Set<FuzzConfig.FormatBucket> formatsSeen = EnumSet.noneOf(FuzzConfig.FormatBucket.class);

    for (long seed = 0; seed < 200; seed++) {
      FuzzScenario s = FuzzScenario.forSeed(seed, cfg);
      formatsSeen.add(s.formatBucket());
      for (LateTxOp op : s.lateTxOps()) {
        kindsSeen.add(opKindOf(op));
      }
    }
    assertThat(formatsSeen).containsExactlyInAnyOrder(FuzzConfig.FormatBucket.values());
    assertThat(kindsSeen).containsExactlyInAnyOrder(FuzzConfig.OpKind.values());
  }

  @Test
  void v3OnlyConfigStillCoversAllOpKinds() throws IOException {
    Path f = tempDir.resolve("v3-only.json");
    Files.writeString(f, "{\"formatWeights\": {\"v3\": 1.0}}");
    FuzzConfig cfg = FuzzConfig.load(f);

    Set<FuzzConfig.OpKind> kindsSeen = EnumSet.noneOf(FuzzConfig.OpKind.class);
    for (long seed = 0; seed < 200; seed++) {
      FuzzScenario s = FuzzScenario.forSeed(seed, cfg);
      assertThat(s.formatBucket()).isEqualTo(FuzzConfig.FormatBucket.V3);
      for (LateTxOp op : s.lateTxOps()) {
        kindsSeen.add(opKindOf(op));
      }
    }
    assertThat(kindsSeen).containsExactlyInAnyOrder(FuzzConfig.OpKind.values());
  }

  @Test
  void v2OnlyConfigSelectsV2FormatBucket() throws IOException {
    Path f = tempDir.resolve("v2-only.json");
    Files.writeString(f, "{\"formatWeights\": {\"v2\": 1.0}}");
    FuzzConfig cfg = FuzzConfig.load(f);

    for (long seed = 0; seed < 100; seed++) {
      FuzzScenario s = FuzzScenario.forSeed(seed, cfg);
      assertThat(s.formatBucket()).isEqualTo(FuzzConfig.FormatBucket.V2);
      assertThat(s.buildConfig().formatVersion()).isEqualTo(2);
      assertThat(s.buildConfig().upgradeAfterChain()).isFalse();
    }
  }

  @Test
  void upgradeBucketYieldsV2WithUpgradeFlag() throws IOException {
    Path f = tempDir.resolve("upgrade-only.json");
    Files.writeString(f, "{\"formatWeights\": {\"v2ThenUpgradeToV3\": 1.0}}");
    FuzzConfig cfg = FuzzConfig.load(f);

    for (long seed = 0; seed < 100; seed++) {
      FuzzScenario s = FuzzScenario.forSeed(seed, cfg);
      assertThat(s.formatBucket()).isEqualTo(FuzzConfig.FormatBucket.V2_THEN_UPGRADE_TO_V3);
      assertThat(s.buildConfig().formatVersion()).isEqualTo(2);
      assertThat(s.buildConfig().upgradeAfterChain()).isTrue();
    }
  }

  @Test
  void appendHeavyConfigBiasesTowardAppendOps() throws IOException {
    Path f = tempDir.resolve("append-heavy.json");
    Files.writeString(
        f,
        "{\"opKindWeights\": {\"append\": 10.0, \"positionDelete\": 1.0,"
            + " \"rowReplacement\": 1.0, \"equalityDelete\": 1.0}}");
    FuzzConfig cfg = FuzzConfig.load(f);

    int append = 0;
    int total = 0;
    for (long seed = 0; seed < 200; seed++) {
      FuzzScenario s = FuzzScenario.forSeed(seed, cfg);
      for (LateTxOp op : s.lateTxOps()) {
        total++;
        if (op instanceof AppendOp) {
          append++;
        }
      }
    }
    // With weight 10/13 ≈ 0.77 we'd expect ~77% appends; allow a generous window.
    assertThat((double) append / total).isGreaterThan(0.6);
  }

  private static FuzzConfig.OpKind opKindOf(LateTxOp op) {
    if (op instanceof PositionDeleteOp) {
      return FuzzConfig.OpKind.POSITION_DELETE;
    }
    if (op instanceof AppendOp) {
      return FuzzConfig.OpKind.APPEND;
    }
    if (op instanceof RowReplacementOp) {
      return FuzzConfig.OpKind.ROW_REPLACEMENT;
    }
    if (op instanceof EqualityDeleteOp) {
      return FuzzConfig.OpKind.EQUALITY_DELETE;
    }
    throw new IllegalStateException("unknown op: " + op);
  }
}
