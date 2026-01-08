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

import org.apache.iceberg.CompactionMap.FileMapping;
import org.apache.iceberg.CompactionMap.Run;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for CompactionMapBuilder file-level mapping functionality.
 *
 * <p>These tests validate the builder API for creating compaction maps with various file mapping
 * scenarios, independent of actual data file reading/writing.
 */
public class TestCompactionMapFileMapping {

  @Test
  public void testSingleSourceToSingleTarget() {
    // Simplest case: one source file compacted to one target file
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    builder
        .addFileMapping("s3://bucket/source1.parquet", "s3://bucket/target1.parquet")
        .addRun(0L, 0L, 100L);

    CompactionMap map = builder.build();

    assertThat(map.sourceSnapshotId()).isEqualTo(1L);
    assertThat(map.targetSnapshotId()).isEqualTo(2L);
    assertThat(map.fileMappings()).hasSize(1);

    FileMapping mapping = map.fileMappings().get(0);
    assertThat(mapping.sourceFile()).isEqualTo("s3://bucket/source1.parquet");
    assertThat(mapping.targetFile()).isEqualTo("s3://bucket/target1.parquet");
    assertThat(mapping.runs()).hasSize(1);

    Run run = mapping.runs().get(0);
    assertThat(run.sourcePosition()).isEqualTo(0L);
    assertThat(run.targetPosition()).isEqualTo(0L);
    assertThat(run.length()).isEqualTo(100L);
  }

  @Test
  public void testMultipleSourceToSingleTarget() {
    // Bin-pack scenario: 3 small files compacted into 1 larger file
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    // Source file 1: rows 0-49 -> target rows 0-49
    builder
        .addFileMapping("s3://bucket/source1.parquet", "s3://bucket/target.parquet")
        .addRun(0L, 0L, 50L);

    // Source file 2: rows 0-49 -> target rows 50-99
    builder
        .addFileMapping("s3://bucket/source2.parquet", "s3://bucket/target.parquet")
        .addRun(0L, 50L, 50L);

    // Source file 3: rows 0-49 -> target rows 100-149
    builder
        .addFileMapping("s3://bucket/source3.parquet", "s3://bucket/target.parquet")
        .addRun(0L, 100L, 50L);

    CompactionMap map = builder.build();

    assertThat(map.fileMappings()).hasSize(3);

    // Verify each source file maps to the same target
    for (FileMapping mapping : map.fileMappings()) {
      assertThat(mapping.targetFile()).isEqualTo("s3://bucket/target.parquet");
    }

    // Verify position offsets are correct
    FileMapping mapping1 = map.fileMappings().get(0);
    assertThat(mapping1.sourceFile()).isEqualTo("s3://bucket/source1.parquet");
    assertThat(mapping1.runs().get(0).targetPosition()).isEqualTo(0L);

    FileMapping mapping2 = map.fileMappings().get(1);
    assertThat(mapping2.sourceFile()).isEqualTo("s3://bucket/source2.parquet");
    assertThat(mapping2.runs().get(0).targetPosition()).isEqualTo(50L);

    FileMapping mapping3 = map.fileMappings().get(2);
    assertThat(mapping3.sourceFile()).isEqualTo("s3://bucket/source3.parquet");
    assertThat(mapping3.runs().get(0).targetPosition()).isEqualTo(100L);
  }

  @Test
  public void testOneToOneMapping() {
    // 1:1 scenario: source file mapped directly to target file
    // This represents a file that is rewritten in place (e.g., sorted or filtered)
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    String sourceFile = "s3://bucket/source.parquet";
    String targetFile = "s3://bucket/target.parquet";

    builder.addFileMapping(sourceFile, targetFile).addRun(0L, 0L, 150L);

    CompactionMap map = builder.build();

    assertThat(map.fileMappings()).hasSize(1);

    FileMapping mapping = map.fileMappings().get(0);
    assertThat(mapping.sourceFile()).isEqualTo(sourceFile);
    assertThat(mapping.targetFile()).isEqualTo(targetFile);
    assertThat(mapping.runs()).hasSize(1);

    Run run = mapping.runs().get(0);
    assertThat(run.sourcePosition()).isEqualTo(0L);
    assertThat(run.targetPosition()).isEqualTo(0L);
    assertThat(run.length()).isEqualTo(150L);
  }

  @Test
  public void testRunMergingWithConsecutivePositions() {
    // Test automatic run merging when positions are consecutive
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    CompactionMapBuilder.FileMappingBuilder fileBuilder =
        builder.addFileMapping("s3://bucket/source.parquet", "s3://bucket/target.parquet");

    // Add three consecutive runs - should be merged into one
    fileBuilder.addRun(0L, 0L, 50L);
    fileBuilder.addRun(50L, 50L, 50L); // Consecutive in both source and target
    fileBuilder.addRun(100L, 100L, 50L); // Also consecutive

    CompactionMap map = builder.build();

    FileMapping mapping = map.fileMappings().get(0);

    // Should be merged into a single run
    assertThat(mapping.runs()).hasSize(1);

    Run mergedRun = mapping.runs().get(0);
    assertThat(mergedRun.sourcePosition()).isEqualTo(0L);
    assertThat(mergedRun.targetPosition()).isEqualTo(0L);
    assertThat(mergedRun.length()).isEqualTo(150L);
  }

  @Test
  public void testRunNotMergedWithGaps() {
    // Test that runs with gaps are NOT merged
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    CompactionMapBuilder.FileMappingBuilder fileBuilder =
        builder.addFileMapping("s3://bucket/source.parquet", "s3://bucket/target.parquet");

    // Run 1: rows 0-49 -> 0-49
    fileBuilder.addRun(0L, 0L, 50L);

    // Run 2: rows 100-149 -> 50-99 (gap in source at rows 50-99)
    fileBuilder.addRun(100L, 50L, 50L);

    // Run 3: rows 200-249 -> 100-149 (gap in source at rows 150-199)
    fileBuilder.addRun(200L, 100L, 50L);

    CompactionMap map = builder.build();

    FileMapping mapping = map.fileMappings().get(0);

    // Should remain as three separate runs due to gaps
    assertThat(mapping.runs()).hasSize(3);

    Run run1 = mapping.runs().get(0);
    assertThat(run1.sourcePosition()).isEqualTo(0L);
    assertThat(run1.targetPosition()).isEqualTo(0L);
    assertThat(run1.length()).isEqualTo(50L);

    Run run2 = mapping.runs().get(1);
    assertThat(run2.sourcePosition()).isEqualTo(100L);
    assertThat(run2.targetPosition()).isEqualTo(50L);
    assertThat(run2.length()).isEqualTo(50L);

    Run run3 = mapping.runs().get(2);
    assertThat(run3.sourcePosition()).isEqualTo(200L);
    assertThat(run3.targetPosition()).isEqualTo(100L);
    assertThat(run3.length()).isEqualTo(50L);
  }

  @Test
  public void testRunNotMergedWithTargetGap() {
    // Test that runs are NOT merged if target positions have a gap
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    CompactionMapBuilder.FileMappingBuilder fileBuilder =
        builder.addFileMapping("s3://bucket/source.parquet", "s3://bucket/target.parquet");

    // Run 1: rows 0-49 -> 0-49
    fileBuilder.addRun(0L, 0L, 50L);

    // Run 2: rows 50-99 -> 100-149 (gap in target at rows 50-99)
    fileBuilder.addRun(50L, 100L, 50L);

    CompactionMap map = builder.build();

    FileMapping mapping = map.fileMappings().get(0);

    // Should remain as two separate runs due to target gap
    assertThat(mapping.runs()).hasSize(2);

    Run run1 = mapping.runs().get(0);
    assertThat(run1.sourcePosition()).isEqualTo(0L);
    assertThat(run1.targetPosition()).isEqualTo(0L);
    assertThat(run1.length()).isEqualTo(50L);

    Run run2 = mapping.runs().get(1);
    assertThat(run2.sourcePosition()).isEqualTo(50L);
    assertThat(run2.targetPosition()).isEqualTo(100L);
    assertThat(run2.length()).isEqualTo(50L);
  }

  @Test
  public void testFilePathVariants() {
    // Test that different file path formats are preserved correctly
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    // S3 path
    builder
        .addFileMapping("s3://bucket/path/file1.parquet", "s3://bucket/output/file1.parquet")
        .addRun(0L, 0L, 100L);

    // HDFS path
    builder
        .addFileMapping(
            "hdfs://namenode/path/file2.parquet", "hdfs://namenode/output/file2.parquet")
        .addRun(0L, 0L, 100L);

    // Local file path
    builder
        .addFileMapping("file:///tmp/file3.parquet", "file:///tmp/output/file3.parquet")
        .addRun(0L, 0L, 100L);

    // Relative path (though not recommended)
    builder.addFileMapping("data/file4.parquet", "output/file4.parquet").addRun(0L, 0L, 100L);

    CompactionMap map = builder.build();

    assertThat(map.fileMappings()).hasSize(4);

    // Verify paths are preserved exactly
    assertThat(map.fileMappings().get(0).sourceFile()).isEqualTo("s3://bucket/path/file1.parquet");
    assertThat(map.fileMappings().get(1).sourceFile())
        .isEqualTo("hdfs://namenode/path/file2.parquet");
    assertThat(map.fileMappings().get(2).sourceFile()).isEqualTo("file:///tmp/file3.parquet");
    assertThat(map.fileMappings().get(3).sourceFile()).isEqualTo("data/file4.parquet");
  }

  @Test
  public void testComplexBinPackScenario() {
    // Complex bin-pack scenario: multiple small files compacted into fewer larger files
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    // Files 1 & 2 bin-packed into target1
    builder
        .addFileMapping("s3://bucket/small1.parquet", "s3://bucket/target1.parquet")
        .addRun(0L, 0L, 50L);
    builder
        .addFileMapping("s3://bucket/small2.parquet", "s3://bucket/target1.parquet")
        .addRun(0L, 50L, 50L);

    // Files 3 & 4 bin-packed into target2
    builder
        .addFileMapping("s3://bucket/small3.parquet", "s3://bucket/target2.parquet")
        .addRun(0L, 0L, 75L);
    builder
        .addFileMapping("s3://bucket/small4.parquet", "s3://bucket/target2.parquet")
        .addRun(0L, 75L, 25L);

    // File 5 mapped 1:1 to target3
    builder
        .addFileMapping("s3://bucket/medium.parquet", "s3://bucket/target3.parquet")
        .addRun(0L, 0L, 100L);

    CompactionMap map = builder.build();

    assertThat(map.fileMappings()).hasSize(5);

    // Verify we have mappings from 5 unique source files
    assertThat(map.fileMappings().stream().map(FileMapping::sourceFile).distinct().count())
        .isEqualTo(5);

    // Verify we have mappings to 3 unique target files
    assertThat(map.fileMappings().stream().map(FileMapping::targetFile).distinct().count())
        .isEqualTo(3);

    // Verify bin-pack target1 has correct offsets
    long target1Total =
        map.fileMappings().stream()
            .filter(m -> m.targetFile().equals("s3://bucket/target1.parquet"))
            .mapToLong(m -> m.runs().get(0).length())
            .sum();
    assertThat(target1Total).isEqualTo(100L); // 50 + 50

    // Verify bin-pack target2 has correct offsets
    long target2Total =
        map.fileMappings().stream()
            .filter(m -> m.targetFile().equals("s3://bucket/target2.parquet"))
            .mapToLong(m -> m.runs().get(0).length())
            .sum();
    assertThat(target2Total).isEqualTo(100L); // 75 + 25
  }

  @Test
  public void testRunPositionMapping() {
    // Test the mapPosition utility in Run
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);

    builder
        .addFileMapping("s3://bucket/source.parquet", "s3://bucket/target.parquet")
        .addRun(100L, 0L, 50L); // Source positions 100-149 -> target positions 0-49

    CompactionMap map = builder.build();
    Run run = map.fileMappings().get(0).runs().get(0);

    // Test position mapping
    assertThat(run.mapPosition(100L)).isEqualTo(0L); // First position
    assertThat(run.mapPosition(125L)).isEqualTo(25L); // Middle position
    assertThat(run.mapPosition(149L)).isEqualTo(49L); // Last position
  }
}
