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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * One late-transaction operation applied during a {@link FuzzScenario}. Concrete subtypes carry
 * per-kind payload; the shared {@code opSeed}, {@code sliceOffsetFraction}, and {@code
 * sliceWidthFraction} fields exist on every kind so the overlap-aware slice allocation in {@link
 * FuzzScenario#forSeed} can operate uniformly without knowing the op kind.
 *
 * <p>The runtime writer that an op resolves to may depend on the table's current format version
 * (e.g., {@link PositionDeleteOp} writes a Puffin DV against a v3 table and a parquet position
 * delete file against a v2 table). {@link FuzzRunner#applyLateTx} performs that dispatch.
 *
 * <p>Jackson polymorphism is enabled so scenarios are JSON-serializable end-to-end; this is not
 * strictly required by the current failure-record format but is cheap insurance for future
 * triage workflows that may want to dump scenario plans.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
@JsonSubTypes({
  @JsonSubTypes.Type(value = PositionDeleteOp.class, name = "positionDelete"),
  @JsonSubTypes.Type(value = AppendOp.class, name = "append"),
  @JsonSubTypes.Type(value = RowReplacementOp.class, name = "rowReplacement"),
  @JsonSubTypes.Type(value = EqualityDeleteOp.class, name = "equalityDelete")
})
public abstract class LateTxOp {
  private final long opSeed;
  private final double sliceOffsetFraction;
  private final double sliceWidthFraction;

  protected LateTxOp(long opSeed, double sliceOffsetFraction, double sliceWidthFraction) {
    this.opSeed = opSeed;
    this.sliceOffsetFraction = sliceOffsetFraction;
    this.sliceWidthFraction = sliceWidthFraction;
  }

  public long opSeed() {
    return opSeed;
  }

  public double sliceOffsetFraction() {
    return sliceOffsetFraction;
  }

  public double sliceWidthFraction() {
    return sliceWidthFraction;
  }

  /** Short human-readable kind tag for {@link FuzzScenario#describe}. */
  public abstract String kind();
}
