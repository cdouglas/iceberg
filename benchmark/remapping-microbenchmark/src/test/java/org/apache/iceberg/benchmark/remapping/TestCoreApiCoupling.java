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
package org.apache.iceberg.benchmark.remapping;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Map;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMapBuilder;
import org.apache.iceberg.PositionDeleteRemapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

/**
 * Tests to ensure the benchmark code stays coupled with core Iceberg remapping APIs.
 *
 * <p>These tests verify that:
 *
 * <ol>
 *   <li>The benchmark's position extraction produces the same results as core APIs
 *   <li>The benchmark's remapping produces identical results to production code paths
 *   <li>API changes in core are detected by benchmark test failures
 * </ol>
 *
 * <p>The shared code architecture is:
 *
 * <pre>
 * Production (remapDVBulk):           Benchmark:
 * DeleteFile                          RoaringBitmap
 *     |                                   |
 *     v                                   v
 * DVPositionReader                    Direct iteration
 * .readDeletedPositionsPrimitive()    for (int pos : bitmap)
 *     |                                   |
 *     v                                   v
 *     +--------> long[] <-----------------+
 *                  |
 *                  v
 *     remapPositionsBulkPrimitive(sourceFile, long[])  <-- SHARED
 * </pre>
 */
public class TestCoreApiCoupling {

  private static final String SOURCE_FILE_1 = "s3://bucket/data/source-00001.parquet";
  private static final String SOURCE_FILE_2 = "s3://bucket/data/source-00002.parquet";
  private static final String TARGET_FILE = "s3://bucket/data/target-00001.parquet";

  private CompactionMap compactionMap;
  private PositionDeleteRemapper remapper;

  @BeforeEach
  public void setUp() {
    // Create a simple compaction map: 2 source files merged into 1 target
    // Source file 1: positions 0-999 -> target positions 0-999
    // Source file 2: positions 0-999 -> target positions 1000-1999
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping(SOURCE_FILE_1, TARGET_FILE).addRun(0, 0, 1000);
    builder.addFileMapping(SOURCE_FILE_2, TARGET_FILE).addRun(0, 1000, 1000);
    compactionMap = builder.build();

    remapper = new PositionDeleteRemapper(compactionMap);
  }

  /**
   * Verifies that the benchmark's position extraction method (RoaringBitmap -> long[]) produces
   * positions that can be correctly remapped by the core API.
   */
  @Test
  public void testBenchmarkPositionExtractionCompatibleWithCoreApi() {
    // Simulate benchmark's position extraction from RoaringBitmap
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(0, 10, 50, 100, 500, 999);

    // Extract positions the way the benchmark does
    long[] positions = extractPositionsLikeBenchmark(bitmap);

    // Verify the extraction preserves all positions
    assertThat(positions).hasSize(6);
    assertThat(positions).containsExactly(0L, 10L, 50L, 100L, 500L, 999L);

    // Verify the core API accepts these positions and produces correct results
    Map<String, long[]> remapped = remapper.remapPositionsBulkPrimitive(SOURCE_FILE_1, positions);

    assertThat(remapped).containsKey(TARGET_FILE);
    long[] targetPositions = remapped.get(TARGET_FILE);

    // Source file 1 maps positions directly (offset 0)
    assertThat(targetPositions).containsExactly(0L, 10L, 50L, 100L, 500L, 999L);
  }

  /**
   * Verifies that remapping results are identical whether using the primitive long[] API (benchmark
   * path) or the Iterable<Long> API.
   */
  @Test
  public void testPrimitiveAndBoxedApisProduceSameResults() {
    long[] positions = {0, 50, 100, 500, 999};

    // Use primitive API (benchmark path)
    Map<String, long[]> primitiveResult =
        remapper.remapPositionsBulkPrimitive(SOURCE_FILE_1, positions);

    // Use boxed API
    Map<String, java.util.Set<Long>> boxedResult =
        remapper.remapPositionsBulk(SOURCE_FILE_1, Arrays.asList(0L, 50L, 100L, 500L, 999L));

    // Both should produce the same target file
    assertThat(primitiveResult.keySet()).isEqualTo(boxedResult.keySet());

    // Both should produce the same positions
    long[] primitivePositions = primitiveResult.get(TARGET_FILE);
    java.util.Set<Long> boxedPositions = boxedResult.get(TARGET_FILE);

    assertThat(primitivePositions).hasSize(boxedPositions.size());
    for (long pos : primitivePositions) {
      assertThat(boxedPositions).contains(pos);
    }
  }

  /**
   * Verifies that the benchmark's aggregation into RoaringBitmap preserves all remapped positions.
   */
  @Test
  public void testBenchmarkAggregationPreservesPositions() {
    // Remap positions from two source files (simulating multiple DVs)
    long[] positions1 = {0, 100, 500};
    long[] positions2 = {0, 100, 500};

    Map<String, long[]> remapped1 = remapper.remapPositionsBulkPrimitive(SOURCE_FILE_1, positions1);
    Map<String, long[]> remapped2 = remapper.remapPositionsBulkPrimitive(SOURCE_FILE_2, positions2);

    // Aggregate into RoaringBitmap the way the benchmark does
    RoaringBitmap aggregated = new RoaringBitmap();
    for (long pos : remapped1.get(TARGET_FILE)) {
      aggregated.add((int) pos);
    }
    for (long pos : remapped2.get(TARGET_FILE)) {
      aggregated.add((int) pos);
    }

    // Verify all positions are preserved
    // Source 1: 0, 100, 500 -> 0, 100, 500 (offset 0)
    // Source 2: 0, 100, 500 -> 1000, 1100, 1500 (offset 1000)
    assertThat(aggregated.getCardinality()).isEqualTo(6);
    assertThat(aggregated.contains(0)).isTrue();
    assertThat(aggregated.contains(100)).isTrue();
    assertThat(aggregated.contains(500)).isTrue();
    assertThat(aggregated.contains(1000)).isTrue();
    assertThat(aggregated.contains(1100)).isTrue();
    assertThat(aggregated.contains(1500)).isTrue();
  }

  /**
   * Verifies that non-compacted files (not in the compaction map) are handled identically by both
   * benchmark and production paths.
   */
  @Test
  public void testNonCompactedFileHandling() {
    String nonCompactedFile = "s3://bucket/data/non-compacted.parquet";
    long[] positions = {0, 10, 20};

    // The primitive API should return original positions for non-compacted files
    Map<String, long[]> result = remapper.remapPositionsBulkPrimitive(nonCompactedFile, positions);

    assertThat(result).containsKey(nonCompactedFile);
    assertThat(result.get(nonCompactedFile)).containsExactly(0L, 10L, 20L);
  }

  /** Verifies that empty position arrays are handled correctly (edge case). */
  @Test
  public void testEmptyPositionsHandling() {
    long[] emptyPositions = {};

    Map<String, long[]> result =
        remapper.remapPositionsBulkPrimitive(SOURCE_FILE_1, emptyPositions);

    assertThat(result).isEmpty();
  }

  /**
   * Verifies that positions falling in gaps (deleted during merge compaction) are correctly
   * filtered out.
   */
  @Test
  public void testGapPositionsFiltered() {
    // Create a compaction map with a gap (positions 100-199 not mapped)
    CompactionMapBuilder gappyBuilder = new CompactionMapBuilder(1L, 2L);
    gappyBuilder
        .addFileMapping(SOURCE_FILE_1, TARGET_FILE)
        .addRun(0, 0, 100) // positions 0-99
        .addRun(200, 100, 100); // positions 200-299 -> 100-199
    CompactionMap gappyMap = gappyBuilder.build();

    PositionDeleteRemapper gappyRemapper = new PositionDeleteRemapper(gappyMap);

    // Include positions in the gap
    long[] positions = {50, 150, 250}; // 50 mapped, 150 in gap, 250 mapped

    Map<String, long[]> result =
        gappyRemapper.remapPositionsBulkPrimitive(SOURCE_FILE_1, positions);

    // Only non-gap positions should be remapped
    assertThat(result.get(TARGET_FILE)).containsExactly(50L, 150L);
    // Position 150 (source) is in the gap and should be filtered
    // Position 250 (source) maps to 150 (target) via the second run
  }

  /** Verifies that large position counts are handled efficiently without overflow. */
  @Test
  public void testLargePositionCount() {
    // Create positions near int max to test for overflow issues
    RoaringBitmap largeBitmap = new RoaringBitmap();
    largeBitmap.add(0);
    largeBitmap.add(Integer.MAX_VALUE - 1000);
    largeBitmap.add(Integer.MAX_VALUE - 1);

    long[] positions = extractPositionsLikeBenchmark(largeBitmap);

    assertThat(positions).hasSize(3);
    assertThat(positions[0]).isEqualTo(0L);
    assertThat(positions[1]).isEqualTo((long) Integer.MAX_VALUE - 1000);
    assertThat(positions[2]).isEqualTo((long) Integer.MAX_VALUE - 1);
  }

  /**
   * Verifies that the remapPositionsBulkPrimitive API exists and has the expected signature. This
   * test will fail to compile if the API changes.
   */
  @Test
  public void testApiSignatureStability() {
    // This test verifies the API contract at compile time
    // If these methods don't exist or change signature, compilation fails

    // Primitive array API (used by benchmark)
    Map<String, long[]> primitiveResult =
        remapper.remapPositionsBulkPrimitive(SOURCE_FILE_1, new long[] {0, 1, 2});
    assertThat(primitiveResult).isNotNull();

    // Iterable API (alternative)
    Map<String, java.util.Set<Long>> iterableResult =
        remapper.remapPositionsBulk(SOURCE_FILE_1, Arrays.asList(0L, 1L, 2L));
    assertThat(iterableResult).isNotNull();
  }

  /**
   * Extracts positions from a RoaringBitmap the same way the benchmark does. This method should
   * match the implementation in RemappingBenchmarkRunner.remapDeletionVectors().
   */
  private long[] extractPositionsLikeBenchmark(RoaringBitmap bitmap) {
    long[] positions = new long[bitmap.getCardinality()];
    int idx = 0;
    for (int pos : bitmap) {
      positions[idx++] = pos;
    }
    return positions;
  }
}
