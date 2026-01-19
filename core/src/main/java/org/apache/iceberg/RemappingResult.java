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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Result of remapping position deletes, including metrics about skipped deletes.
 *
 * <p>This class provides detailed information about the remapping process, including:
 *
 * <ul>
 *   <li>Successfully remapped deletes grouped by target file
 *   <li>Count of deletes skipped because the file was not in the compaction map
 *   <li>Count of deletes skipped because the position was filtered during compaction
 *   <li>Count of deletes skipped due to invalid positions (negative values)
 *   <li>Count of duplicate deletes that were deduplicated
 * </ul>
 */
public class RemappingResult {
  private final Map<String, List<PositionDeleteRecord>> remappedDeletes;
  private final int skippedNotCompacted;
  private final int skippedFilteredRows;
  private final int skippedInvalidPositions;
  private final int duplicatesRemoved;

  private RemappingResult(
      Map<String, List<PositionDeleteRecord>> remappedDeletes,
      int skippedNotCompacted,
      int skippedFilteredRows,
      int skippedInvalidPositions,
      int duplicatesRemoved) {
    this.remappedDeletes = remappedDeletes;
    this.skippedNotCompacted = skippedNotCompacted;
    this.skippedFilteredRows = skippedFilteredRows;
    this.skippedInvalidPositions = skippedInvalidPositions;
    this.duplicatesRemoved = duplicatesRemoved;
  }

  /** Returns the remapped deletes grouped by target file path. */
  public Map<String, List<PositionDeleteRecord>> remappedDeletes() {
    return remappedDeletes;
  }

  /** Returns the count of deletes skipped because the file was not in the compaction map. */
  public int skippedNotCompacted() {
    return skippedNotCompacted;
  }

  /** Returns the count of deletes skipped because the row was filtered during compaction. */
  public int skippedFilteredRows() {
    return skippedFilteredRows;
  }

  /** Returns the count of deletes skipped due to invalid (negative) positions. */
  public int skippedInvalidPositions() {
    return skippedInvalidPositions;
  }

  /** Returns the count of duplicate deletes that were removed. */
  public int duplicatesRemoved() {
    return duplicatesRemoved;
  }

  /** Returns the total count of deletes that were skipped for any reason. */
  public int totalSkipped() {
    return skippedNotCompacted + skippedFilteredRows + skippedInvalidPositions;
  }

  /** Returns the total count of successfully remapped deletes. */
  public int totalRemapped() {
    return remappedDeletes.values().stream().mapToInt(List::size).sum();
  }

  /** Returns true if any deletes were successfully remapped. */
  public boolean hasRemappedDeletes() {
    return !remappedDeletes.isEmpty();
  }

  /** Returns true if any deletes were skipped. */
  public boolean hasSkippedDeletes() {
    return totalSkipped() > 0;
  }

  /** Returns an empty result with no remapped deletes. */
  public static RemappingResult empty() {
    return new RemappingResult(ImmutableMap.of(), 0, 0, 0, 0);
  }

  /** Creates a new builder for RemappingResult. */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("totalRemapped", totalRemapped())
        .add("targetFiles", remappedDeletes.size())
        .add("skippedNotCompacted", skippedNotCompacted)
        .add("skippedFilteredRows", skippedFilteredRows)
        .add("skippedInvalidPositions", skippedInvalidPositions)
        .add("duplicatesRemoved", duplicatesRemoved)
        .toString();
  }

  /** Builder for creating RemappingResult instances. */
  public static class Builder {
    private Map<String, List<PositionDeleteRecord>> remappedDeletes = ImmutableMap.of();
    private int skippedNotCompacted = 0;
    private int skippedFilteredRows = 0;
    private int skippedInvalidPositions = 0;
    private int duplicatesRemoved = 0;

    private Builder() {}

    public Builder remappedDeletes(Map<String, List<PositionDeleteRecord>> deletes) {
      this.remappedDeletes = ImmutableMap.copyOf(deletes);
      return this;
    }

    public Builder skippedNotCompacted(int count) {
      this.skippedNotCompacted = count;
      return this;
    }

    public Builder skippedFilteredRows(int count) {
      this.skippedFilteredRows = count;
      return this;
    }

    public Builder skippedInvalidPositions(int count) {
      this.skippedInvalidPositions = count;
      return this;
    }

    public Builder duplicatesRemoved(int count) {
      this.duplicatesRemoved = count;
      return this;
    }

    public RemappingResult build() {
      return new RemappingResult(
          remappedDeletes,
          skippedNotCompacted,
          skippedFilteredRows,
          skippedInvalidPositions,
          duplicatesRemoved);
    }
  }
}
