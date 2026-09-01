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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.iceberg.data.WorkloadGenerator;

/**
 * Equality-delete op — commits an equality-delete file on {@code long_0} (schema field id 5)
 * containing {@code rowsPerOp} predicate values drawn from the op's seeded RNG. Predicate values
 * are random {@code long}s, which almost certainly do not match any actual row content; the op
 * therefore commits cleanly but typically deletes nothing.
 *
 * <p>This is intentional: the goal is to exercise the equality-delete commit + conflict detection
 * + resolver code paths in a v2 / v3 / upgraded-table mix without coupling predicate selection
 * to the seeded row-generation state of {@code WorkloadGenerator}. The row-multiset confluence
 * property is trivially preserved when no rows match; when matches do occur, they occur in both
 * reference and treatment paths consistently because the predicate is content-based, not
 * position-based.
 */
public final class EqualityDeleteOp extends LateTxOp {
  private final int rowsPerOp;

  @JsonCreator
  public EqualityDeleteOp(
      @JsonProperty("opSeed") long opSeed,
      @JsonProperty("sliceOffsetFraction") double sliceOffsetFraction,
      @JsonProperty("sliceWidthFraction") double sliceWidthFraction,
      @JsonProperty("rowsPerOp") int rowsPerOp) {
    super(opSeed, sliceOffsetFraction, sliceWidthFraction);
    this.rowsPerOp = rowsPerOp;
  }

  public int rowsPerOp() {
    return rowsPerOp;
  }

  @Override
  public String kind() {
    return "equalityDelete";
  }
}
