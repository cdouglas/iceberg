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
 * Append op — writes {@code rows} new rows in one or more data files and commits them via {@code
 * AppendFiles}. The slice fractions are carried for shape uniformity but are ignored by the
 * append writer; appended files live outside any pre-compaction slice.
 */
public final class AppendOp extends LateTxOp {
  private final long rows;
  private final int rowsPerFile;

  @JsonCreator
  public AppendOp(
      @JsonProperty("opSeed") long opSeed,
      @JsonProperty("sliceOffsetFraction") double sliceOffsetFraction,
      @JsonProperty("sliceWidthFraction") double sliceWidthFraction,
      @JsonProperty("rows") long rows,
      @JsonProperty("rowsPerFile") int rowsPerFile) {
    super(opSeed, sliceOffsetFraction, sliceWidthFraction);
    this.rows = rows;
    this.rowsPerFile = rowsPerFile;
  }

  public long rows() {
    return rows;
  }

  public int rowsPerFile() {
    return rowsPerFile;
  }

  @Override
  public String kind() {
    return "append";
  }
}
