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
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Validates that {@link FuzzConfig#overlapProbability()} controls slice overlap in the expected
 * direction without needing to spin up Spark or a real Iceberg catalog.
 */
class TestFuzzScenarioOverlap {

  @TempDir Path tempDir;

  @Test
  void zeroProbabilityProducesDisjointIntervals() throws IOException {
    FuzzConfig cfg = withOverlapProbability(0.0);
    int disjointSeeds = 0;
    int multiOpSeeds = 0;
    for (long seed = 0; seed < 200; seed++) {
      FuzzScenario s = FuzzScenario.forSeed(seed, cfg);
      if (s.lateTxOps().size() < 2) {
        continue;
      }
      multiOpSeeds++;
      if (allDisjoint(s.lateTxOps())) {
        disjointSeeds++;
      }
    }
    assertThat(multiOpSeeds).isGreaterThan(50); // sanity: most seeds in 1..8 are multi-op
    assertThat(disjointSeeds).isEqualTo(multiOpSeeds);
  }

  @Test
  void oneProbabilityProducesAtLeastOneOverlapPerMultiOpScenario() throws IOException {
    FuzzConfig cfg = withOverlapProbability(1.0);
    int overlappingSeeds = 0;
    int multiOpSeeds = 0;
    for (long seed = 0; seed < 200; seed++) {
      FuzzScenario s = FuzzScenario.forSeed(seed, cfg);
      if (s.lateTxOps().size() < 2) {
        continue;
      }
      multiOpSeeds++;
      if (hasAnyOverlap(s.lateTxOps())) {
        overlappingSeeds++;
      }
    }
    assertThat(multiOpSeeds).isGreaterThan(50);
    assertThat(overlappingSeeds).isEqualTo(multiOpSeeds);
  }

  @Test
  void halfProbabilityProducesAMixOfBothShapes() throws IOException {
    FuzzConfig cfg = FuzzConfig.defaults(); // overlapProbability = 0.5
    int overlapping = 0;
    int disjoint = 0;
    int multiOpSeeds = 0;
    for (long seed = 0; seed < 500; seed++) {
      FuzzScenario s = FuzzScenario.forSeed(seed, cfg);
      if (s.lateTxOps().size() < 2) {
        continue;
      }
      multiOpSeeds++;
      if (hasAnyOverlap(s.lateTxOps())) {
        overlapping++;
      } else {
        disjoint++;
      }
    }
    assertThat(multiOpSeeds).isGreaterThan(100);
    // Both shapes must appear; we don't pin a specific ratio (it depends on n distribution),
    // just that neither side is zero.
    assertThat(overlapping).isGreaterThan(10);
    assertThat(disjoint).isGreaterThan(10);
  }

  private FuzzConfig withOverlapProbability(double p) throws IOException {
    Path f = tempDir.resolve("p" + p + ".json");
    Files.writeString(f, "{\"overlapProbability\": " + p + "}");
    return FuzzConfig.load(f);
  }

  private static boolean allDisjoint(List<LateTxOp> ops) {
    for (int i = 0; i < ops.size(); i++) {
      double aStart = ops.get(i).sliceOffsetFraction();
      double aEnd = aStart + ops.get(i).sliceWidthFraction();
      for (int j = i + 1; j < ops.size(); j++) {
        double bStart = ops.get(j).sliceOffsetFraction();
        double bEnd = bStart + ops.get(j).sliceWidthFraction();
        if (aStart < bEnd && bStart < aEnd) {
          return false;
        }
      }
    }
    return true;
  }

  private static boolean hasAnyOverlap(List<LateTxOp> ops) {
    return !allDisjoint(ops);
  }
}
