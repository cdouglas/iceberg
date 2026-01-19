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
package org.apache.iceberg;

import java.io.Serializable;
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * Immutable record representing a position delete entry.
 *
 * <p>Used for compaction conflict recovery to store position deletes that need to be remapped from
 * source to target files.
 */
public class PositionDeleteRecord implements Serializable {
  private final String dataFilePath;
  private final long position;
  private final StructLike partitionData;
  private final StructLike rowData;

  /**
   * Create a position delete record without partition or row data.
   *
   * @param dataFilePath the path to the data file this delete references
   * @param position the position in the data file
   */
  public PositionDeleteRecord(String dataFilePath, long position) {
    this(dataFilePath, position, null, null);
  }

  /**
   * Create a position delete record with partition data.
   *
   * @param dataFilePath the path to the data file this delete references
   * @param position the position in the data file
   * @param partitionData the partition data for this delete, may be null
   */
  public PositionDeleteRecord(String dataFilePath, long position, StructLike partitionData) {
    this(dataFilePath, position, partitionData, null);
  }

  /**
   * Create a position delete record with partition and row data.
   *
   * @param dataFilePath the path to the data file this delete references
   * @param position the position in the data file
   * @param partitionData the partition data for this delete, may be null
   * @param rowData the optional row data for this delete, may be null
   */
  public PositionDeleteRecord(
      String dataFilePath, long position, StructLike partitionData, StructLike rowData) {
    this.dataFilePath = dataFilePath;
    this.position = position;
    this.partitionData = partitionData;
    this.rowData = rowData;
  }

  /** Returns the path to the data file this delete references. */
  public String dataFilePath() {
    return dataFilePath;
  }

  /** Returns the position in the data file. */
  public long position() {
    return position;
  }

  /** Returns the partition data for this delete, may be null. */
  public StructLike partitionData() {
    return partitionData;
  }

  /** Returns the optional row data for this delete, may be null. */
  public StructLike rowData() {
    return rowData;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PositionDeleteRecord)) {
      return false;
    }
    PositionDeleteRecord that = (PositionDeleteRecord) o;
    return position == that.position
        && Objects.equals(dataFilePath, that.dataFilePath)
        && Objects.equals(partitionData, that.partitionData)
        && Objects.equals(rowData, that.rowData);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataFilePath, position, partitionData, rowData);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("dataFilePath", dataFilePath)
        .add("position", position)
        .add("partitionData", partitionData)
        .add("rowData", rowData != null ? "<row-data>" : null)
        .toString();
  }
}
