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
package org.apache.iceberg.snaprewrite;

import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/** A request to write the position deletes one rewritten snapshot applies to one partition. */
public class PositionDeleteRequest {
  private final long snapshotId;
  private final PartitionSpec spec;
  private final StructLike partition;
  private final Map<String, PositionSet> deletes;
  private final String outputPath;

  PositionDeleteRequest(
      long snapshotId,
      PartitionSpec spec,
      StructLike partition,
      Map<String, PositionSet> deletes,
      String outputPath) {
    this.snapshotId = snapshotId;
    this.spec = spec;
    this.partition = partition;
    this.deletes = ImmutableMap.copyOf(deletes);
    this.outputPath = outputPath;
  }

  public long snapshotId() {
    return snapshotId;
  }

  public PartitionSpec spec() {
    return spec;
  }

  public StructLike partition() {
    return partition;
  }

  /** Deleted positions by data file path. Must be written sorted by path, then position. */
  public Map<String, PositionSet> deletes() {
    return deletes;
  }

  public String outputPath() {
    return outputPath;
  }

  public long positionCount() {
    return deletes.values().stream().mapToLong(PositionSet::size).sum();
  }
}
