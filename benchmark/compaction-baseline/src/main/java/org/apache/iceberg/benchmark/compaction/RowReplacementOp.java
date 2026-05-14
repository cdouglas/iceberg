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
 * Row-replacement op — commits a {@code RowDelta} that both adds new rows and writes position
 * deletes against a pre-compaction slice. The data-file portion is not validated by {@code
 * CompactionConflictDetector} (which only inspects delete files), so this op stresses the delete-
 * portion remap path but not the data-file conflict path. The harness asserts the row-multiset
 * hash matches; the bypassed validation is documented in this Javadoc rather than papered over.
 */
public final class RowReplacementOp extends LateTxOp {
  private final int deletesPerOp;
  private final long replacementRows;

  @JsonCreator
  public RowReplacementOp(
      @JsonProperty("opSeed") long opSeed,
      @JsonProperty("sliceOffsetFraction") double sliceOffsetFraction,
      @JsonProperty("sliceWidthFraction") double sliceWidthFraction,
      @JsonProperty("deletesPerOp") int deletesPerOp,
      @JsonProperty("replacementRows") long replacementRows) {
    super(opSeed, sliceOffsetFraction, sliceWidthFraction);
    this.deletesPerOp = deletesPerOp;
    this.replacementRows = replacementRows;
  }

  public int deletesPerOp() {
    return deletesPerOp;
  }

  public long replacementRows() {
    return replacementRows;
  }

  @Override
  public String kind() {
    return "rowReplacement";
  }
}
