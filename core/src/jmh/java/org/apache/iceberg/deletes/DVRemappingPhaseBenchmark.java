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
package org.apache.iceberg.deletes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark that measures the cost breakdown of DV (Deletion Vector) remapping phases.
 *
 * <p>This benchmark instruments the individual phases of DV remapping to understand where
 * computation time is spent (excluding I/O):
 *
 * <ul>
 *   <li><b>bitmapDeserialize</b>: Deserialize Roaring bitmap from bytes
 *   <li><b>positionIterate</b>: Iterate all positions from bitmap into a List
 *   <li><b>bitmapConstruct</b>: Build new Roaring bitmap from positions
 *   <li><b>bitmapSerialize</b>: Serialize bitmap to bytes
 *   <li><b>hashSetConstruct</b>: Build HashSet from positions (comparison)
 *   <li><b>fullRoundTrip</b>: Complete deserialize -> iterate -> construct -> serialize
 * </ul>
 *
 * <p>Note: Position lookup/remapping is benchmarked separately in RemappingAlgorithmBenchmark.
 *
 * <p>To run this benchmark:
 *
 * <pre>
 * ./gradlew :iceberg-core:jmh \
 *     -PjmhIncludeRegex=DVRemappingPhaseBenchmark \
 *     -PjmhOutputPath=benchmark/dv-remapping-phase-results.txt
 * </pre>
 *
 * <p>To run specific scenarios:
 *
 * <pre>
 * ./gradlew :iceberg-core:jmh \
 *     -PjmhIncludeRegex=DVRemappingPhaseBenchmark \
 *     -PjmhParams="numDeletes=100000"
 * </pre>
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class DVRemappingPhaseBenchmark {

  @Param({"1000", "10000", "100000", "1000000"})
  private int numDeletes;

  // Lookup ratio: fraction of positions to query (simulates different workloads)
  // 0.01 = 1% of positions, 0.1 = 10%, 1.0 = 100%
  @Param({"0.01", "0.1", "0.5", "1.0"})
  private double lookupRatio;

  // Pre-computed test data
  private byte[] serializedBitmap;
  private RoaringPositionBitmap deserializedBitmap;
  private List<Long> positions;
  private long[] positionsArray;

  // Pre-computed lookup queries (positions to search for)
  private long[] lookupQueries;
  private Set<Long> preBuiltHashSet;

  @Setup(Level.Trial)
  public void setup() {
    // Create sorted positions (simulating real DV iteration)
    positions = new ArrayList<>(numDeletes);
    positionsArray = new long[numDeletes];

    // Generate positions with some gaps (typical DV pattern)
    long pos = 0;
    for (int i = 0; i < numDeletes; i++) {
      positions.add(pos);
      positionsArray[i] = pos;
      // Mix of consecutive and sparse positions
      pos += (i % 10 == 0) ? 100 : 1;
    }

    // Build bitmap with these positions
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    for (Long p : positions) {
      bitmap.set(p);
    }
    deserializedBitmap = bitmap;

    // Serialize bitmap
    serializedBitmap = serializeBitmap(bitmap);

    // Generate lookup queries (mix of hits and misses)
    int numLookups = Math.max(1, (int) (numDeletes * lookupRatio));
    lookupQueries = new long[numLookups];
    Random random = new Random(42); // Fixed seed for reproducibility
    long maxPos = positionsArray[numDeletes - 1];

    for (int i = 0; i < numLookups; i++) {
      if (random.nextBoolean()) {
        // 50% chance: pick an existing position (hit)
        lookupQueries[i] = positionsArray[random.nextInt(numDeletes)];
      } else {
        // 50% chance: pick a random position (may be miss)
        lookupQueries[i] = Math.abs(random.nextLong()) % (maxPos + 1000);
      }
    }

    // Pre-build HashSet for lookup comparison
    preBuiltHashSet = new HashSet<>(numDeletes);
    for (long p : positionsArray) {
      preBuiltHashSet.add(p);
    }
  }

  /** Benchmark: Deserialize Roaring bitmap from bytes. */
  @Benchmark
  public RoaringPositionBitmap bitmapDeserialize() {
    return deserializeBitmap(serializedBitmap);
  }

  /** Benchmark: Iterate all positions from bitmap into a List. */
  @Benchmark
  public List<Long> positionIterate(Blackhole blackhole) {
    List<Long> result = new ArrayList<>(numDeletes);
    deserializedBitmap.forEach(result::add);
    blackhole.consume(result.size());
    return result;
  }

  /** Benchmark: Iterate positions into a primitive array (faster). */
  @Benchmark
  public long[] positionIterateArray(Blackhole blackhole) {
    long[] result = new long[numDeletes];
    int[] idx = {0};
    deserializedBitmap.forEach(
        pos -> {
          if (idx[0] < result.length) {
            result[idx[0]++] = pos;
          }
        });
    blackhole.consume(idx[0]);
    return result;
  }

  /** Benchmark: Build new Roaring bitmap from List of positions. */
  @Benchmark
  public RoaringPositionBitmap bitmapConstructFromList() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    for (Long pos : positions) {
      bitmap.set(pos);
    }
    return bitmap;
  }

  /** Benchmark: Build new Roaring bitmap from primitive array. */
  @Benchmark
  public RoaringPositionBitmap bitmapConstructFromArray() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    for (long pos : positionsArray) {
      bitmap.set(pos);
    }
    return bitmap;
  }

  /** Benchmark: Serialize bitmap to bytes. */
  @Benchmark
  public byte[] bitmapSerialize() {
    return serializeBitmap(deserializedBitmap);
  }

  /** Benchmark: Build HashSet from positions (comparison for remapping output). */
  @Benchmark
  public Set<Long> hashSetConstruct() {
    Set<Long> result = new HashSet<>(numDeletes);
    for (Long pos : positions) {
      result.add(pos);
    }
    return result;
  }

  /** Benchmark: Build HashSet from primitive array. */
  @Benchmark
  public Set<Long> hashSetConstructFromArray() {
    Set<Long> result = new HashSet<>(numDeletes);
    for (long pos : positionsArray) {
      result.add(pos);
    }
    return result;
  }

  /** Benchmark: Full round-trip (deserialize -> iterate -> construct -> serialize). */
  @Benchmark
  public byte[] fullRoundTrip() {
    // 1. Deserialize
    RoaringPositionBitmap inputBitmap = deserializeBitmap(serializedBitmap);

    // 2. Iterate positions
    List<Long> pos = new ArrayList<>(numDeletes);
    inputBitmap.forEach(pos::add);

    // 3. Build output bitmap (simulating remapped positions)
    RoaringPositionBitmap outputBitmap = new RoaringPositionBitmap();
    for (Long p : pos) {
      // Simulate position transformation (add offset)
      outputBitmap.set(p + 1000);
    }

    // 4. Serialize
    return serializeBitmap(outputBitmap);
  }

  /** Benchmark: Full round-trip with intermediate HashSet (like remapping). */
  @Benchmark
  public byte[] fullRoundTripWithHashSet() {
    // 1. Deserialize
    RoaringPositionBitmap inputBitmap = deserializeBitmap(serializedBitmap);

    // 2. Iterate positions into HashSet (like remapping does)
    Set<Long> remapped = new HashSet<>(numDeletes);
    inputBitmap.forEach(
        pos -> {
          // Simulate position transformation
          remapped.add(pos + 1000);
        });

    // 3. Build output bitmap from HashSet
    RoaringPositionBitmap outputBitmap = new RoaringPositionBitmap();
    for (Long p : remapped) {
      outputBitmap.set(p);
    }

    // 4. Serialize
    return serializeBitmap(outputBitmap);
  }

  // ============================================================================
  // HashSet vs Binary Search comparison benchmarks
  // These measure the trade-off between construction cost and lookup cost
  // ============================================================================

  /**
   * Benchmark: Build HashSet and perform lookups.
   *
   * <p>This simulates the current remapping approach: build a HashSet from positions, then check
   * membership for each query position.
   */
  @Benchmark
  public int hashSetBuildAndLookup(Blackhole blackhole) {
    // Build HashSet from scratch (simulates remapping scenario)
    Set<Long> set = new HashSet<>(numDeletes);
    for (long pos : positionsArray) {
      set.add(pos);
    }

    // Perform lookups
    int hits = 0;
    for (long query : lookupQueries) {
      if (set.contains(query)) {
        hits++;
      }
    }
    blackhole.consume(set);
    return hits;
  }

  /**
   * Benchmark: Use binary search on sorted array for lookups.
   *
   * <p>This simulates an alternative approach: keep positions in a sorted array (which they already
   * are from bitmap iteration) and use binary search for membership checks.
   */
  @Benchmark
  public int binarySearchLookup(Blackhole blackhole) {
    // positionsArray is already sorted (from bitmap iteration order)
    // No construction cost - just perform lookups

    int hits = 0;
    for (long query : lookupQueries) {
      if (Arrays.binarySearch(positionsArray, query) >= 0) {
        hits++;
      }
    }
    blackhole.consume(positionsArray);
    return hits;
  }

  /**
   * Benchmark: Lookup only using pre-built HashSet.
   *
   * <p>This isolates the lookup cost when the HashSet is already built. Useful for understanding
   * the per-lookup overhead.
   */
  @Benchmark
  public int hashSetLookupOnly(Blackhole blackhole) {
    int hits = 0;
    for (long query : lookupQueries) {
      if (preBuiltHashSet.contains(query)) {
        hits++;
      }
    }
    blackhole.consume(preBuiltHashSet);
    return hits;
  }

  /**
   * Benchmark: Full round-trip using binary search instead of HashSet.
   *
   * <p>This is the key comparison: can we avoid HashSet entirely by using binary search on the
   * sorted position array? The trade-off is O(log n) lookups vs O(1), but we save O(n) construction
   * time.
   */
  @Benchmark
  public byte[] fullRoundTripBinarySearch() {
    // 1. Deserialize
    RoaringPositionBitmap inputBitmap = deserializeBitmap(serializedBitmap);

    // 2. Iterate positions into sorted array
    long[] sortedPositions = new long[numDeletes];
    int[] idx = {0};
    inputBitmap.forEach(
        pos -> {
          if (idx[0] < sortedPositions.length) {
            sortedPositions[idx[0]++] = pos;
          }
        });

    // 3. Build output bitmap directly (positions are already sorted from bitmap)
    // In real remapping, we'd apply offset transformation here
    RoaringPositionBitmap outputBitmap = new RoaringPositionBitmap();
    for (int i = 0; i < idx[0]; i++) {
      outputBitmap.set(sortedPositions[i] + 1000);
    }

    // 4. Serialize
    return serializeBitmap(outputBitmap);
  }

  /**
   * Benchmark: Direct bitmap-to-bitmap transformation (optimal path).
   *
   * <p>This is the theoretical optimum: iterate source bitmap positions directly into the output
   * bitmap without any intermediate data structure. Only possible when we don't need random access
   * lookups.
   */
  @Benchmark
  public byte[] fullRoundTripDirect() {
    // 1. Deserialize
    RoaringPositionBitmap inputBitmap = deserializeBitmap(serializedBitmap);

    // 2. Direct iteration into output bitmap
    RoaringPositionBitmap outputBitmap = new RoaringPositionBitmap();
    inputBitmap.forEach(pos -> outputBitmap.set(pos + 1000));

    // 3. Serialize
    return serializeBitmap(outputBitmap);
  }

  private byte[] serializeBitmap(RoaringPositionBitmap bitmap) {
    // Serialize using Iceberg's format: 4-byte length + bitmap data + 4-byte CRC
    int bitmapLength = (int) bitmap.serializedSizeInBytes();

    byte[] result = new byte[4 + bitmapLength + 4];
    ByteBuffer buffer = ByteBuffer.wrap(result);
    buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);

    // Write length (big-endian, but we'll use little-endian for simplicity in benchmark)
    buffer.putInt(bitmapLength);

    // Serialize bitmap data
    bitmap.serialize(buffer);

    // Write CRC (placeholder - using 0 for benchmark)
    buffer.putInt(0);

    return result;
  }

  private RoaringPositionBitmap deserializeBitmap(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);

    // Read length
    int length = buffer.getInt();

    // Read bitmap data
    byte[] bitmapData = new byte[length];
    buffer.get(bitmapData);

    // Deserialize bitmap (must also be little-endian)
    ByteBuffer bitmapBuffer = ByteBuffer.wrap(bitmapData);
    bitmapBuffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
    return RoaringPositionBitmap.deserialize(bitmapBuffer);
  }
}
