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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.CompactionMap.FileMapping;
import org.apache.iceberg.CompactionMap.Run;
import org.junit.jupiter.api.Test;

public class TestCompactionMapBuilder {

  @Test
  public void testSimpleSingleFileMapping() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    builder.addFileMapping("file1.parquet", "compacted.parquet").addRun(0, 0, 100);

    CompactionMap map = builder.build();

    assertThat(map.sourceSnapshotId()).isEqualTo(1L);
    assertThat(map.targetSnapshotId()).isEqualTo(2L);
    assertThat(map.fileMappings()).hasSize(1);

    FileMapping mapping = map.fileMappings().get(0);
    assertThat(mapping.sourceFile()).isEqualTo("file1.parquet");
    assertThat(mapping.targetFile()).isEqualTo("compacted.parquet");
    assertThat(mapping.runs()).hasSize(1);

    Run run = mapping.runs().get(0);
    assertThat(run.sourcePosition()).isEqualTo(0);
    assertThat(run.targetPosition()).isEqualTo(0);
    assertThat(run.length()).isEqualTo(100);
  }

  @Test
  public void testConsecutiveRunsMerge() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    builder
        .addFileMapping("file1.parquet", "compacted.parquet")
        .addRun(0, 0, 100)
        .addRun(100, 100, 50)
        .addRun(150, 150, 25);

    CompactionMap map = builder.build();
    FileMapping mapping = map.fileMappings().get(0);

    // All three runs should merge into one
    assertThat(mapping.runs()).hasSize(1);
    Run run = mapping.runs().get(0);
    assertThat(run.sourcePosition()).isEqualTo(0);
    assertThat(run.targetPosition()).isEqualTo(0);
    assertThat(run.length()).isEqualTo(175);
  }

  @Test
  public void testNonConsecutiveRunsDoNotMerge() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    builder
        .addFileMapping("file1.parquet", "compacted.parquet")
        .addRun(0, 0, 100) // Rows 0-99 -> 0-99
        .addRun(100, 200, 50); // Rows 100-149 -> 200-249 (gap in target)

    CompactionMap map = builder.build();
    FileMapping mapping = map.fileMappings().get(0);

    // Should not merge due to gap in target positions
    assertThat(mapping.runs()).hasSize(2);

    Run run1 = mapping.runs().get(0);
    assertThat(run1.sourcePosition()).isEqualTo(0);
    assertThat(run1.targetPosition()).isEqualTo(0);
    assertThat(run1.length()).isEqualTo(100);

    Run run2 = mapping.runs().get(1);
    assertThat(run2.sourcePosition()).isEqualTo(100);
    assertThat(run2.targetPosition()).isEqualTo(200);
    assertThat(run2.length()).isEqualTo(50);
  }

  @Test
  public void testMultipleFileMappings() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    builder.addFileMapping("file1.parquet", "compacted.parquet").addRun(0, 0, 100);

    builder.addFileMapping("file2.parquet", "compacted.parquet").addRun(0, 100, 200);

    CompactionMap map = builder.build();
    assertThat(map.fileMappings()).hasSize(2);

    FileMapping mapping1 = map.fileMappings().get(0);
    assertThat(mapping1.sourceFile()).isEqualTo("file1.parquet");
    assertThat(mapping1.targetFile()).isEqualTo("compacted.parquet");
    assertThat(mapping1.runs()).hasSize(1);
    assertThat(mapping1.runs().get(0).sourcePosition()).isEqualTo(0);
    assertThat(mapping1.runs().get(0).targetPosition()).isEqualTo(0);
    assertThat(mapping1.runs().get(0).length()).isEqualTo(100);

    FileMapping mapping2 = map.fileMappings().get(0);
    assertThat(mapping2.sourceFile()).isEqualTo("file1.parquet");
  }

  @Test
  public void testGapInSourcePositions() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    builder
        .addFileMapping("file1.parquet", "compacted.parquet")
        .addRun(0, 0, 100) // Rows 0-99
        .addRun(200, 100, 50); // Rows 200-249 (skip 100-199)

    CompactionMap map = builder.build();
    FileMapping mapping = map.fileMappings().get(0);

    // Should not merge due to gap in source positions
    assertThat(mapping.runs()).hasSize(2);
  }

  @Test
  public void testFileMappingLookup() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    CompactionMapBuilder.FileMappingBuilder mapping1 =
        builder.addFileMapping("file1.parquet", "compacted.parquet");
    mapping1.addRun(0, 0, 100);

    CompactionMapBuilder.FileMappingBuilder found = builder.getFileMapping("file1.parquet");
    assertThat(found).isSameAs(mapping1);

    CompactionMapBuilder.FileMappingBuilder notFound =
        builder.getFileMapping("nonexistent.parquet");
    assertThat(notFound).isNull();
  }

  @Test
  public void testDuplicateSourceFileThrows() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    builder.addFileMapping("file1.parquet", "compacted1.parquet");

    assertThatThrownBy(() -> builder.addFileMapping("file1.parquet", "compacted2.parquet"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("already exists");
  }

  @Test
  public void testNullSourceFileThrows() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    assertThatThrownBy(() -> builder.addFileMapping(null, "target.parquet"))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testNullTargetFileThrows() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    assertThatThrownBy(() -> builder.addFileMapping("source.parquet", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testNegativeSourcePositionThrows() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    assertThatThrownBy(
            () -> builder.addFileMapping("file1.parquet", "compacted.parquet").addRun(-1, 0, 100))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-negative");
  }

  @Test
  public void testNegativeTargetPositionThrows() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    assertThatThrownBy(
            () -> builder.addFileMapping("file1.parquet", "compacted.parquet").addRun(0, -1, 100))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-negative");
  }

  @Test
  public void testZeroLengthThrows() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    assertThatThrownBy(
            () -> builder.addFileMapping("file1.parquet", "compacted.parquet").addRun(0, 0, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("positive");
  }

  @Test
  public void testComplexMergingPattern() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    builder
        .addFileMapping("file1.parquet", "compacted.parquet")
        .addRun(0, 0, 10) // Will merge
        .addRun(10, 10, 20) // Will merge
        .addRun(30, 50, 15) // Will NOT merge (gap in source)
        .addRun(45, 65, 5) // Will merge with previous (consecutive)
        .addRun(50, 70, 10); // Will merge with previous (consecutive)

    CompactionMap map = builder.build();
    FileMapping mapping = map.fileMappings().get(0);

    // Should have 2 runs: (0,0,30) and (30,50,30)
    assertThat(mapping.runs()).hasSize(2);

    // First merged run: source 0-29, target 0-29
    Run run1 = mapping.runs().get(0);
    assertThat(run1.sourcePosition()).isEqualTo(0);
    assertThat(run1.targetPosition()).isEqualTo(0);
    assertThat(run1.length()).isEqualTo(30);

    // Second merged run: source 30-59, target 50-79
    Run run2 = mapping.runs().get(1);
    assertThat(run2.sourcePosition()).isEqualTo(30);
    assertThat(run2.targetPosition()).isEqualTo(50);
    assertThat(run2.length()).isEqualTo(30);
  }

  @Test
  public void testEmptyBuilder() {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    CompactionMap map = builder.build();

    assertThat(map.sourceSnapshotId()).isEqualTo(1L);
    assertThat(map.targetSnapshotId()).isEqualTo(2L);
    assertThat(map.fileMappings()).isEmpty();
  }
}
