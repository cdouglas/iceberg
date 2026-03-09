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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * Tests documenting the multi-file-per-task position tracking gap.
 *
 * <p>When a single writer task produces multiple output files (due to rollover/splitting), buffered
 * position mappings must be assigned to the correct output file based on which target position
 * range each output file covers. The current implementation in PositionTrackingDataWriter assigns
 * ALL mappings to files[0], which is incorrect when multiple files are produced.
 *
 * <p>This test verifies the correct behavior at the core level: given a set of buffered mappings
 * and multiple output files with known record counts, each mapping should be assigned to the output
 * file whose position range contains the mapping's target position.
 */
public class TestPositionTrackingMultiFileAssignment {

  /**
   * Verifies that position-to-file assignment logic correctly distributes mappings across multiple
   * output files based on cumulative record counts.
   *
   * <p>Given:
   *
   * <ul>
   *   <li>Output file A: 100 records (positions 0-99)
   *   <li>Output file B: 100 records (positions 100-199)
   *   <li>Output file C: 50 records (positions 200-249)
   * </ul>
   *
   * <p>A mapping with targetPos=150 should be assigned to file B.
   */
  @Test
  public void testCorrectFileAssignmentByPositionRange() {
    // Simulate multiple output files with their record counts
    String[] files = {"/target/file-a.parquet", "/target/file-b.parquet", "/target/file-c.parquet"};
    long[] recordCounts = {100, 100, 50};

    // Build cumulative boundaries: [0, 100, 200, 250]
    long[] boundaries = new long[recordCounts.length + 1];
    boundaries[0] = 0;
    for (int i = 0; i < recordCounts.length; i++) {
      boundaries[i + 1] = boundaries[i] + recordCounts[i];
    }

    // Test positions at various points
    assertThat(assignToFile(0, files, boundaries)).isEqualTo(files[0]); // Start of file A
    assertThat(assignToFile(99, files, boundaries)).isEqualTo(files[0]); // End of file A
    assertThat(assignToFile(100, files, boundaries)).isEqualTo(files[1]); // Start of file B
    assertThat(assignToFile(150, files, boundaries)).isEqualTo(files[1]); // Middle of file B
    assertThat(assignToFile(199, files, boundaries)).isEqualTo(files[1]); // End of file B
    assertThat(assignToFile(200, files, boundaries)).isEqualTo(files[2]); // Start of file C
    assertThat(assignToFile(249, files, boundaries)).isEqualTo(files[2]); // End of file C
  }

  /**
   * Verifies that position-to-file assignment produces correct file-local positions by subtracting
   * the cumulative boundary offset from the absolute target position.
   */
  @Test
  public void testAdjustedTargetPositionsAreFileLocal() {
    String[] files = {"/target/file-a.parquet", "/target/file-b.parquet", "/target/file-c.parquet"};
    long[] recordCounts = {100, 100, 50};

    long[] boundaries = new long[recordCounts.length + 1];
    boundaries[0] = 0;
    for (int i = 0; i < recordCounts.length; i++) {
      boundaries[i + 1] = boundaries[i] + recordCounts[i];
    }

    // Absolute position 150 is in file B (boundaries[1]=100), so file-local pos = 150 - 100 = 50
    int fileIdx = findFileIndex(150, boundaries);
    assertThat(files[fileIdx]).isEqualTo(files[1]);
    assertThat(150 - boundaries[fileIdx]).isEqualTo(50);

    // Absolute position 200 is in file C (boundaries[2]=200), so file-local pos = 200 - 200 = 0
    fileIdx = findFileIndex(200, boundaries);
    assertThat(files[fileIdx]).isEqualTo(files[2]);
    assertThat(200 - boundaries[fileIdx]).isEqualTo(0);

    // Absolute position 0 is in file A (boundaries[0]=0), so file-local pos = 0 - 0 = 0
    fileIdx = findFileIndex(0, boundaries);
    assertThat(files[fileIdx]).isEqualTo(files[0]);
    assertThat(0 - boundaries[fileIdx]).isEqualTo(0);
  }

  private int findFileIndex(long targetPos, long[] boundaries) {
    int lo = 0;
    int hi = boundaries.length - 2;
    while (lo < hi) {
      int mid = lo + (hi - lo + 1) / 2;
      if (boundaries[mid] <= targetPos) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }
    return lo;
  }

  /**
   * Verifies that the assignment logic handles the single-file case correctly. When there is only
   * one output file, all mappings should be assigned to it (this is the common bin-pack case and
   * works correctly today).
   */
  @Test
  public void testSingleFileAssignment() {
    String[] files = {"/target/file-a.parquet"};
    long[] boundaries = {0, 100};

    assertThat(assignToFile(0, files, boundaries)).isEqualTo(files[0]);
    assertThat(assignToFile(50, files, boundaries)).isEqualTo(files[0]);
    assertThat(assignToFile(99, files, boundaries)).isEqualTo(files[0]);
  }

  /**
   * Verifies correct assignment when source files have varying sizes, leading to uneven position
   * ranges in the output files.
   */
  @Test
  public void testUnevenFileSizes() {
    String[] files = {"/target/small.parquet", "/target/large.parquet"};
    long[] recordCounts = {10, 1000};

    long[] boundaries = new long[recordCounts.length + 1];
    boundaries[0] = 0;
    for (int i = 0; i < recordCounts.length; i++) {
      boundaries[i + 1] = boundaries[i] + recordCounts[i];
    }

    // Position 5 is in the small file
    assertThat(assignToFile(5, files, boundaries)).isEqualTo(files[0]);
    // Position 9 is the last row in the small file
    assertThat(assignToFile(9, files, boundaries)).isEqualTo(files[0]);
    // Position 10 is the first row in the large file
    assertThat(assignToFile(10, files, boundaries)).isEqualTo(files[1]);
    // Position 500 is well into the large file
    assertThat(assignToFile(500, files, boundaries)).isEqualTo(files[1]);
  }

  /**
   * Assigns a target position to the correct output file based on cumulative record count
   * boundaries. This is the correct implementation that PositionTrackingDataWriter should use.
   *
   * @param targetPos the target position to assign
   * @param files the output file paths
   * @param boundaries cumulative record count boundaries: [0, count0, count0+count1, ...]
   * @return the file path that contains the given target position
   */
  private String assignToFile(long targetPos, String[] files, long[] boundaries) {
    // Binary search for the file containing targetPos
    int idx = Arrays.binarySearch(boundaries, targetPos);
    if (idx < 0) {
      // Not exact match — insertion point is -(idx+1), so the file index is -(idx+1) - 1
      idx = -(idx + 1) - 1;
    }
    // Clamp to valid range
    idx = Math.max(0, Math.min(idx, files.length - 1));
    return files[idx];
  }
}
