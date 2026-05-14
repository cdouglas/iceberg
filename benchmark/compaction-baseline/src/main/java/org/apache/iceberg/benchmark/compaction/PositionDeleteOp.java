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

/**
 * Position-delete op. Resolves at runtime to either a v3 deletion-vector commit or a v2 parquet
 * position-delete-file commit, depending on the table's current format version when the op is
 * applied. The same op object can therefore appear in a v2-only, v3-only, or v2-then-upgrade
 * scenario without modification.
 */
public final class PositionDeleteOp extends LateTxOp {
  private final int deletesPerOp;

  @JsonCreator
  public PositionDeleteOp(
      @JsonProperty("opSeed") long opSeed,
      @JsonProperty("sliceOffsetFraction") double sliceOffsetFraction,
      @JsonProperty("sliceWidthFraction") double sliceWidthFraction,
      @JsonProperty("deletesPerOp") int deletesPerOp) {
    super(opSeed, sliceOffsetFraction, sliceWidthFraction);
    this.deletesPerOp = deletesPerOp;
  }

  public int deletesPerOp() {
    return deletesPerOp;
  }

  @Override
  public String kind() {
    return "positionDelete";
  }
}
