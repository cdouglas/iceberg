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
import org.apache.iceberg.CompactionMap.FileMapping;
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.GenericCompactionMap.GenericFileMapping;
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

/**
 * A benchmark that evaluates the performance of different remapping strategies.
 *
 * <p>This benchmark measures the performance of various remapping algorithms under different
 * workload characteristics:
 *
 * <ul>
 *   <li>Number of runs (m): 10, 100, 1000
 *   <li>Number of positions (n): 1000, 10000, 100000
 *   <li>Gap ratio: 0.0 (dense), 0.3 (moderate gaps), 0.5 (sparse)
 *   <li>Sortedness: sorted vs unsorted positions
 * </ul>
 *
 * <p>To run this benchmark:
 *
 * <pre>
 * ./gradlew :iceberg-core:jmh \
 *     -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
 *     -PjmhOutputPath=benchmark/remapping-algorithm-benchmark-results.txt
 * </pre>
 *
 * <p>To run specific scenarios:
 *
 * <pre>
 * ./gradlew :iceberg-core:jmh \
 *     -PjmhIncludeRegex=RemappingAlgorithmBenchmark \
 *     -PjmhParams="numRuns=100,numPositions=10000,gapRatio=0.3,sorted=true"
 * </pre>
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RemappingAlgorithmBenchmark {

  @Param({"10", "100", "1000"})
  private int numRuns;

  @Param({"1000", "10000", "100000"})
  private int numPositions;

  @Param({"0.0", "0.3", "0.5"})
  private double gapRatio;

  @Param({"true", "false"})
  private boolean sorted;

  private List<Run> runs;
  private List<Long> positions;
  private FileMapping mapping;

  @Setup(Level.Trial)
  public void setup() {
    // Create runs with specified gap ratio
    runs = RemappingBenchmarkUtils.createRunsWithGaps(numRuns, gapRatio);
    mapping =
        new GenericFileMapping("s3://bucket/source.parquet", "s3://bucket/target.parquet", runs);

    // Create positions (sorted or unsorted)
    if (sorted) {
      positions = RemappingBenchmarkUtils.createSortedPositions(numPositions, numRuns);
    } else {
      positions = RemappingBenchmarkUtils.createRandomPositions(numPositions);
    }
  }

  @Benchmark
  public Map<Long, Run> linearSearch() {
    LinearSearchStrategy strategy = new LinearSearchStrategy(runs);
    return strategy.runForPositions(positions);
  }

  @Benchmark
  public Map<Long, Run> binarySearch() {
    BinarySearchStrategy strategy = new BinarySearchStrategy(runs);
    return strategy.runForPositions(positions);
  }

  @Benchmark
  public Map<Long, Run> intervalTree() {
    IntervalTreeStrategy strategy = new IntervalTreeStrategy(runs);
    return strategy.runForPositions(positions);
  }

  @Benchmark
  public Map<Long, Run> streamJoin() {
    StreamJoinStrategy strategy = new StreamJoinStrategy(runs);
    return strategy.runForPositions(positions);
  }

  @Benchmark
  public Map<Long, Run> rangeQuery() {
    RangeQueryStrategy strategy = new RangeQueryStrategy(runs);
    return strategy.runForPositions(positions);
  }

  @Benchmark
  public Map<Long, Run> smartSelector() {
    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, positions);
    return strategy.runForPositions(positions);
  }
}
