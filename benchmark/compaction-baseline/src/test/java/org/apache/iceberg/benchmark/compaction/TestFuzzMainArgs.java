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

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link FuzzMain.Args} parsing — covers the three seed-selection modes (random
 * anchor when neither flag is given, deterministic anchor via {@code --seed-start}, and explicit
 * list via {@code --seeds}) and their mutual exclusion.
 */
class TestFuzzMainArgs {

  @Test
  void defaultsHaveNoSeedStartAndNoSeedsList() {
    FuzzMain.Args args = FuzzMain.Args.parse(new String[] {});
    assertThat(args.seedStart())
        .as("default --seed-start must be empty so the harness picks a random anchor")
        .isEmpty();
    assertThat(args.seeds()).as("default --seeds must be null").isNull();
    assertThat(args.seedCount()).isEqualTo(100);
    assertThat(args.workers()).isEqualTo(1);
  }

  @Test
  void seedStartIsRememberedAsAnchor() {
    FuzzMain.Args args =
        FuzzMain.Args.parse(new String[] {"--seed-start", "12345", "--seed-count", "10"});
    assertThat(args.seedStart()).isPresent();
    assertThat(args.seedStart().getAsLong()).isEqualTo(12345L);
    assertThat(args.seedCount()).isEqualTo(10);
    assertThat(args.seeds()).isNull();
  }

  @Test
  void seedsParsesCsvIntoExplicitList() {
    FuzzMain.Args args = FuzzMain.Args.parse(new String[] {"--seeds", "1369, 42,7"});
    assertThat(args.seeds()).containsExactly(1369L, 42L, 7L);
    assertThat(args.seedStart()).isEmpty();
  }

  @Test
  void seedsAndSeedStartTogetherAreRejected() {
    assertThatThrownBy(
            () ->
                FuzzMain.Args.parse(
                    new String[] {"--seeds", "1", "--seed-start", "42"}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("mutually exclusive");

    assertThatThrownBy(
            () ->
                FuzzMain.Args.parse(
                    new String[] {"--seed-start", "42", "--seeds", "1"}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("mutually exclusive");
  }
}
