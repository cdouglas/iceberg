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

import org.apache.iceberg.data.WorkloadGenerator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Determinism guard for {@link WorkloadGenerator}. The fuzz harness, the prebuilt warehouses, and
 * the correctness check all depend on byte-identical output for a given seed; if this test starts
 * failing the entire benchmark stops being reproducible.
 */
class TestWorkloadGeneratorDeterminism {

  @Test
  void schemaShape() {
    Schema schema = WorkloadGenerator.SCHEMA;
    List<Types.NestedField> fields = schema.columns();
    assertThat(fields).hasSize(20);

    int idx = 0;
    for (int i = 0; i < 4; i++, idx++) {
      assertThat(fields.get(idx).name()).isEqualTo("uuid_" + i);
      assertThat(fields.get(idx).type()).isEqualTo(Types.StringType.get());
    }
    for (int i = 0; i < 8; i++, idx++) {
      assertThat(fields.get(idx).name()).isEqualTo("long_" + i);
      assertThat(fields.get(idx).type()).isEqualTo(Types.LongType.get());
    }
    for (int i = 0; i < 4; i++, idx++) {
      assertThat(fields.get(idx).name()).isEqualTo("short_" + i);
      assertThat(fields.get(idx).type()).isEqualTo(Types.StringType.get());
    }
    for (int i = 0; i < 4; i++, idx++) {
      assertThat(fields.get(idx).name()).isEqualTo("dbl_" + i);
      assertThat(fields.get(idx).type()).isEqualTo(Types.DoubleType.get());
    }

    // Field IDs are contiguous and 1-based — preserve this; many Iceberg readers key on them.
    for (int i = 0; i < fields.size(); i++) {
      assertThat(fields.get(i).fieldId()).isEqualTo(i + 1);
    }
  }

  @Test
  void rowsAreDeterministicForSameSeed() {
    List<Record> first = WorkloadGenerator.generateRows(42L, 200);
    List<Record> second = WorkloadGenerator.generateRows(42L, 200);
    assertThat(first).hasSize(200);
    assertThat(first).isEqualTo(second);
  }

  @Test
  void rowsDifferAcrossSeeds() {
    List<Record> first = WorkloadGenerator.generateRows(1L, 50);
    List<Record> second = WorkloadGenerator.generateRows(2L, 50);
    assertThat(first).isNotEqualTo(second);
  }

  @Test
  void rowFieldShapesMatchSchema() {
    List<Record> rows = WorkloadGenerator.generateRows(7L, 5);
    for (Record row : rows) {
      assertThat(row.size()).isEqualTo(20);
      int idx = 0;
      // 4 UUID-shaped strings (canonical 36-char form).
      for (int i = 0; i < 4; i++) {
        Object value = row.get(idx++);
        assertThat(value).isInstanceOf(String.class);
        assertThat((String) value).hasSize(36);
      }
      // 8 longs.
      for (int i = 0; i < 8; i++) {
        assertThat(row.get(idx++)).isInstanceOf(Long.class);
      }
      // 4 short strings of fixed length 16.
      for (int i = 0; i < 4; i++) {
        Object value = row.get(idx++);
        assertThat(value).isInstanceOf(String.class);
        assertThat((String) value).hasSize(16);
      }
      // 4 doubles.
      for (int i = 0; i < 4; i++) {
        assertThat(row.get(idx++)).isInstanceOf(Double.class);
      }
    }

    // Spot-check that the recorded types in the schema match too.
    List<Types.NestedField> fields = WorkloadGenerator.SCHEMA.columns();
    for (int i = 0; i < fields.size(); i++) {
      Type type = fields.get(i).type();
      assertThat(type.typeId()).isIn(Type.TypeID.STRING, Type.TypeID.LONG, Type.TypeID.DOUBLE);
    }
  }

  @Test
  void clusteredPositionsAreDeterministicForSameSeed() {
    long[] first = WorkloadGenerator.generateClusteredPositions(99L, 1_000_000L, 10_000, 100);
    long[] second = WorkloadGenerator.generateClusteredPositions(99L, 1_000_000L, 10_000, 100);
    assertThat(first).containsExactly(second);
  }

  @Test
  void clusteredPositionsAreSortedAndDistinct() {
    long[] positions = WorkloadGenerator.generateClusteredPositions(3L, 100_000L, 1000, 100);
    assertThat(positions).isSorted();
    for (int i = 1; i < positions.length; i++) {
      assertThat(positions[i]).isGreaterThan(positions[i - 1]);
    }
    // All within bounds.
    assertThat(positions[0]).isGreaterThanOrEqualTo(0);
    assertThat(positions[positions.length - 1]).isLessThan(100_000L);
  }

  @Test
  void clusteredPositionsApproximateRequestedCount() {
    // Clustered runs may collide and dedupe, but at low density (1k of 1M with run length 100)
    // collisions should be rare. Allow up to 5% loss as a sanity bound.
    long[] positions = WorkloadGenerator.generateClusteredPositions(13L, 1_000_000L, 1000, 100);
    assertThat(positions.length).isBetween(950, 1000);
  }

  @Test
  void clusteredPositionsScatterWithRunLengthOne() {
    // Run length 1 = uniform scatter (the shape used in the pre-compaction snapshots).
    long[] positions = WorkloadGenerator.generateClusteredPositions(21L, 1_000_000L, 500, 1);
    assertThat(positions).isSorted();
    // No two adjacent entries should be consecutive (would mean an unintended run); allow a
    // handful of accidental adjacencies from random collisions.
    int consecutive = 0;
    for (int i = 1; i < positions.length; i++) {
      if (positions[i] == positions[i - 1] + 1) {
        consecutive++;
      }
    }
    assertThat(consecutive).isLessThan(positions.length / 50);
  }

  @Test
  void clusteredPositionsRejectsDegenerateInputs() {
    assertThatThrownBy(() -> WorkloadGenerator.generateClusteredPositions(0L, 10L, 100, 10))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxPosition");
    assertThatThrownBy(() -> WorkloadGenerator.generateClusteredPositions(0L, 1000L, 100, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("targetRunLength");
  }

  @Test
  void emptyDeleteRequestReturnsEmptyArray() {
    long[] positions = WorkloadGenerator.generateClusteredPositions(5L, 1_000L, 0, 100);
    assertThat(positions).isEmpty();
  }
}
