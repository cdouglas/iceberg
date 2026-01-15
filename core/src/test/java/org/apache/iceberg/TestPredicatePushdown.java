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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.GenericCompactionMap.GenericRun;
import org.junit.jupiter.api.Test;

public class TestPredicatePushdown {

  @Test
  public void testStreamJoinWithPredicatePushdown() {
    // Create runs covering [0-100), [200-300), [400-500)
    List<Run> runs =
        Arrays.asList(
            new GenericRun(0, 0, 100),
            new GenericRun(200, 100, 100),
            new GenericRun(400, 200, 100));

    StreamJoinStrategy strategy = new StreamJoinStrategy(runs);

    // Positions only in middle range [200-300)
    List<Long> positions = Arrays.asList(210L, 220L, 230L, 240L, 250L);

    Map<Long, Run> results = strategy.runForPositions(positions);

    // Should only match middle run
    assertThat(results).hasSize(5);
    assertThat(results.values()).allMatch(r -> r.sourcePosition() == 200);
  }

  @Test
  public void testRangeQueryWithPredicatePushdown() {
    // Create runs covering [0-100), [200-300), [400-500)
    List<Run> runs =
        Arrays.asList(
            new GenericRun(0, 0, 100),
            new GenericRun(200, 100, 100),
            new GenericRun(400, 200, 100));

    RangeQueryStrategy strategy = new RangeQueryStrategy(runs);

    // Positions only in middle range [200-300)
    List<Long> positions = Arrays.asList(210L, 220L, 230L, 240L, 250L);

    Map<Long, Run> results = strategy.runForPositions(positions);

    // Should only match middle run
    assertThat(results).hasSize(5);
    assertThat(results.values()).allMatch(r -> r.sourcePosition() == 200);
  }

  @Test
  public void testPredicatePushdownWithNoOverlap() {
    // Positions [50-100), runs [200-300), [400-500)
    List<Run> runs =
        Arrays.asList(new GenericRun(200, 0, 100), new GenericRun(400, 100, 100));

    // Positions in range [50-100) don't overlap any runs
    List<Long> positions = Arrays.asList(50L, 60L, 70L, 80L, 90L);

    // Test both strategies
    StreamJoinStrategy streamJoin = new StreamJoinStrategy(runs);
    RangeQueryStrategy rangeQuery = new RangeQueryStrategy(runs);

    Map<Long, Run> streamResults = streamJoin.runForPositions(positions);
    Map<Long, Run> rangeResults = rangeQuery.runForPositions(positions);

    // Should return empty map efficiently (no runs overlap)
    assertThat(streamResults).isEmpty();
    assertThat(rangeResults).isEmpty();
  }

  @Test
  public void testPredicatePushdownFiltersManyRuns() {
    // Create 100 runs, each covering 100 positions with 100-position gaps
    // Run 0: [0-100), Run 1: [200-300), Run 2: [400-500), ...
    List<Run> runs = new java.util.ArrayList<>();
    for (int i = 0; i < 100; i++) {
      long sourcePos = i * 200L; // 100 positions + 100 gap
      long targetPos = i * 100L;
      runs.add(new GenericRun(sourcePos, targetPos, 100));
    }

    // Positions clustered in small range [4000-5000)
    // This overlaps runs at indices 20-24 (5 runs out of 100)
    List<Long> positions = new java.util.ArrayList<>();
    for (long pos = 4000; pos < 5000; pos += 10) {
      positions.add(pos);
    }

    // Test both strategies
    StreamJoinStrategy streamJoin = new StreamJoinStrategy(runs);
    RangeQueryStrategy rangeQuery = new RangeQueryStrategy(runs);

    Map<Long, Run> streamResults = streamJoin.runForPositions(positions);
    Map<Long, Run> rangeResults = rangeQuery.runForPositions(positions);

    // Both should return same results
    assertThat(streamResults).isEqualTo(rangeResults);

    // Verify only overlapping runs were matched
    // Positions [4000-5000) overlap runs: [4000-4100), [4200-4300), [4400-4500), [4600-4700),
    // [4800-4900)
    // That's 5 runs × 10 positions each = 50 matches
    assertThat(streamResults).hasSize(50);
  }

  @Test
  public void testPredicatePushdownAtBoundaries() {
    // Test boundary conditions for run filtering
    List<Run> runs =
        Arrays.asList(
            new GenericRun(0, 0, 100), // [0-100)
            new GenericRun(100, 100, 100), // [100-200)
            new GenericRun(200, 200, 100)); // [200-300)

    StreamJoinStrategy streamJoin = new StreamJoinStrategy(runs);
    RangeQueryStrategy rangeQuery = new RangeQueryStrategy(runs);

    // Test 1: Positions exactly at run boundaries
    List<Long> boundaryPositions = Arrays.asList(0L, 100L, 200L);
    Map<Long, Run> streamBoundary = streamJoin.runForPositions(boundaryPositions);
    Map<Long, Run> rangeBoundary = rangeQuery.runForPositions(boundaryPositions);

    assertThat(streamBoundary).hasSize(3);
    assertThat(streamBoundary).isEqualTo(rangeBoundary);

    // Test 2: Positions spanning first and last run only
    List<Long> edgePositions = Arrays.asList(50L, 250L);
    Map<Long, Run> streamEdge = streamJoin.runForPositions(edgePositions);
    Map<Long, Run> rangeEdge = rangeQuery.runForPositions(edgePositions);

    assertThat(streamEdge).hasSize(2);
    assertThat(streamEdge).isEqualTo(rangeEdge);
    assertThat(streamEdge.get(50L).sourcePosition()).isEqualTo(0);
    assertThat(streamEdge.get(250L).sourcePosition()).isEqualTo(200);
  }

  @Test
  public void testPredicatePushdownWithSparseDeletes() {
    // Create runs covering wide range with gaps
    List<Run> runs =
        Arrays.asList(
            new GenericRun(0, 0, 100),
            new GenericRun(1000, 100, 100),
            new GenericRun(2000, 200, 100),
            new GenericRun(3000, 300, 100),
            new GenericRun(4000, 400, 100));

    // Positions clustered at start and end only
    List<Long> sparsePositions = Arrays.asList(10L, 20L, 30L, 4010L, 4020L, 4030L);

    StreamJoinStrategy streamJoin = new StreamJoinStrategy(runs);
    RangeQueryStrategy rangeQuery = new RangeQueryStrategy(runs);

    Map<Long, Run> streamResults = streamJoin.runForPositions(sparsePositions);
    Map<Long, Run> rangeResults = rangeQuery.runForPositions(sparsePositions);

    // Should match 6 positions (3 in first run, 3 in last run)
    assertThat(streamResults).hasSize(6);
    assertThat(streamResults).isEqualTo(rangeResults);

    // Verify correct runs matched
    assertThat(streamResults.get(10L).sourcePosition()).isEqualTo(0);
    assertThat(streamResults.get(4010L).sourcePosition()).isEqualTo(4000);
  }
}
