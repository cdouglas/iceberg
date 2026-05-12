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
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CompactionMap.Run;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.roaringbitmap.RoaringBitmap;

/**
 * Compares the old long-typed remap-and-bitmap path against the new int-typed DV path end-to-end.
 *
 * <p>Both paths start from a {@link RoaringBitmap} of source positions (the natural form of a V3
 * deletion vector) and finish by depositing the remapped positions into a target
 * {@link RoaringBitmap} via {@code addN}. The difference is whether the remapping API exposes
 * {@code long[]} (with widening at entry and narrowing at exit) or {@code int[]} (skipping the
 * narrowing entirely and using the new sorted-hint path through the selector).
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class DVRemapPathBenchmark {

  @Param({"10000", "100000", "1000000"})
  private int numPositions;

  @Param({"100", "1000"})
  private int numRuns;

  private static final String SOURCE = "s3://bucket/source.parquet";
  private static final String TARGET = "s3://bucket/target.parquet";

  private PositionDeleteRemapper remapper;
  private RoaringBitmap sourceBitmap;

  @Setup(Level.Trial)
  public void setup() {
    List<Run> runs = RemappingBenchmarkUtils.createRunsWithGaps(numRuns, 0.0);
    CompactionMap.FileMapping mapping =
        new GenericCompactionMap.GenericFileMapping(SOURCE, TARGET, runs);
    GenericCompactionMap map = new GenericCompactionMap(1L, 2L, java.util.Collections.singletonList(mapping));
    this.remapper = new PositionDeleteRemapper(map);

    // Build a sorted DV-shaped source bitmap covering positions within run coverage.
    sourceBitmap = new RoaringBitmap();
    long stride = Math.max(1L, runs.get(0).length() * numRuns / (long) numPositions);
    int pos = 0;
    for (int i = 0; i < numPositions; i++) {
      sourceBitmap.add(pos);
      pos += (int) stride;
    }
  }

  /** Old path: extract long[], remapPositionsBulkPrimitive, narrow to int[], addN. */
  @Benchmark
  public RoaringBitmap oldLongPath() {
    long[] positions = new long[sourceBitmap.getCardinality()];
    int idx = 0;
    for (int p : sourceBitmap) {
      positions[idx++] = p;
    }

    Map<String, long[]> remapped = remapper.remapPositionsBulkPrimitive(SOURCE, positions);

    RoaringBitmap target = new RoaringBitmap();
    for (long[] longs : remapped.values()) {
      int[] ints = new int[longs.length];
      for (int i = 0; i < longs.length; i++) {
        ints[i] = (int) longs[i];
      }
      target.addN(ints, 0, ints.length);
    }
    return target;
  }

  /** New path: extract int[], remapPositionsBulkDV, addN directly. */
  @Benchmark
  public RoaringBitmap newIntPath() {
    int[] positions = new int[sourceBitmap.getCardinality()];
    int idx = 0;
    for (int p : sourceBitmap) {
      positions[idx++] = p;
    }

    Map<String, int[]> remapped = remapper.remapPositionsBulkDV(SOURCE, positions);

    RoaringBitmap target = new RoaringBitmap();
    for (int[] ints : remapped.values()) {
      target.addN(ints, 0, ints.length);
    }
    return target;
  }
}
