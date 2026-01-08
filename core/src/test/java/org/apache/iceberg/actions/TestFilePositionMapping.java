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
package org.apache.iceberg.actions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.actions.RewriteFileGroup.FilePositionMapping;
import org.apache.iceberg.actions.RewriteFileGroup.FilePositionMapping.Run;
import org.junit.jupiter.api.Test;

/** Unit tests for FilePositionMapping multi-run support. */
public class TestFilePositionMapping {

  @Test
  public void testSingleRunConstructor() {
    // Test backward-compatible constructor for simple sequential mapping
    FilePositionMapping mapping =
        new FilePositionMapping("source.parquet", "target.parquet", 100L, 100L, 0L);

    assertThat(mapping.sourceFile()).isEqualTo("source.parquet");
    assertThat(mapping.targetFile()).isEqualTo("target.parquet");
    assertThat(mapping.runs()).hasSize(1);

    Run run = mapping.runs().get(0);
    assertThat(run.sourceOffset()).isEqualTo(0L);
    assertThat(run.targetOffset()).isEqualTo(0L);
    assertThat(run.length()).isEqualTo(100L);
  }

  @Test
  public void testMultiRunConstructor() {
    // Test new constructor for mappings with gaps
    List<Run> runs =
        Arrays.asList(
            new Run(0L, 0L, 50L), // First 50 rows: source 0-49 -> target 0-49
            new Run(75L, 50L, 25L) // Next 25 rows: source 75-99 -> target 50-74
            // Gap at source positions 50-74 (deleted)
            );

    FilePositionMapping mapping = new FilePositionMapping("source.parquet", "target.parquet", runs);

    assertThat(mapping.sourceFile()).isEqualTo("source.parquet");
    assertThat(mapping.targetFile()).isEqualTo("target.parquet");
    assertThat(mapping.runs()).hasSize(2);

    Run run1 = mapping.runs().get(0);
    assertThat(run1.sourceOffset()).isEqualTo(0L);
    assertThat(run1.targetOffset()).isEqualTo(0L);
    assertThat(run1.length()).isEqualTo(50L);

    Run run2 = mapping.runs().get(1);
    assertThat(run2.sourceOffset()).isEqualTo(75L);
    assertThat(run2.targetOffset()).isEqualTo(50L);
    assertThat(run2.length()).isEqualTo(25L);
  }

  @Test
  public void testEmptyRuns() {
    FilePositionMapping mapping =
        new FilePositionMapping("source.parquet", "target.parquet", Collections.emptyList());

    assertThat(mapping.runs()).isEmpty();
  }

  @Test
  public void testRunProperties() {
    Run run = new Run(100L, 200L, 50L);

    assertThat(run.sourceOffset()).isEqualTo(100L);
    assertThat(run.targetOffset()).isEqualTo(200L);
    assertThat(run.length()).isEqualTo(50L);
  }

  @Test
  public void testSingleRunWithOffset() {
    // Test single run with non-zero offsets (e.g., second source file in N:M compaction)
    FilePositionMapping mapping =
        new FilePositionMapping("sourceB.parquet", "target.parquet", 80L, 80L, 120L);

    assertThat(mapping.runs()).hasSize(1);

    Run run = mapping.runs().get(0);
    assertThat(run.sourceOffset()).isEqualTo(0L);
    assertThat(run.targetOffset()).isEqualTo(120L);
    assertThat(run.length()).isEqualTo(80L);
  }

  @Test
  public void testComplexMultiRunScenario() {
    // Simulate a file with heavy deletions resulting in multiple small runs
    List<Run> runs =
        Arrays.asList(
            new Run(0L, 0L, 10L), // Rows 0-9
            new Run(20L, 10L, 5L), // Rows 20-24 (10-19 deleted)
            new Run(30L, 15L, 15L), // Rows 30-44 (25-29 deleted)
            new Run(50L, 30L, 10L) // Rows 50-59 (45-49 deleted)
            );

    FilePositionMapping mapping = new FilePositionMapping("source.parquet", "target.parquet", runs);

    assertThat(mapping.runs()).hasSize(4);

    // Verify total mapped rows
    long totalMappedRows = runs.stream().mapToLong(Run::length).sum();
    assertThat(totalMappedRows).isEqualTo(40L);

    // Verify runs cover expected source positions
    assertThat(mapping.runs().get(0).sourceOffset()).isEqualTo(0L);
    assertThat(mapping.runs().get(1).sourceOffset()).isEqualTo(20L);
    assertThat(mapping.runs().get(2).sourceOffset()).isEqualTo(30L);
    assertThat(mapping.runs().get(3).sourceOffset()).isEqualTo(50L);

    // Verify target positions are sequential
    assertThat(mapping.runs().get(0).targetOffset()).isEqualTo(0L);
    assertThat(mapping.runs().get(1).targetOffset()).isEqualTo(10L);
    assertThat(mapping.runs().get(2).targetOffset()).isEqualTo(15L);
    assertThat(mapping.runs().get(3).targetOffset()).isEqualTo(30L);
  }

  @Test
  public void testRunEquality() {
    Run run1 = new Run(10L, 20L, 30L);
    Run run2 = new Run(10L, 20L, 30L);

    // Note: Run class may not implement equals/hashCode, so we test field equality
    assertThat(run1.sourceOffset()).isEqualTo(run2.sourceOffset());
    assertThat(run1.targetOffset()).isEqualTo(run2.targetOffset());
    assertThat(run1.length()).isEqualTo(run2.length());
  }

  @Test
  public void testLargeRunLength() {
    // Test with large file (e.g., 10 million rows)
    FilePositionMapping mapping =
        new FilePositionMapping(
            "large-source.parquet", "large-target.parquet", 10_000_000L, 10_000_000L, 0L);

    assertThat(mapping.runs()).hasSize(1);
    assertThat(mapping.runs().get(0).length()).isEqualTo(10_000_000L);
  }

  @Test
  public void testZeroLengthRun() {
    // Edge case: run with zero length (should be valid but unusual)
    Run run = new Run(0L, 0L, 0L);

    assertThat(run.length()).isEqualTo(0L);
  }
}
