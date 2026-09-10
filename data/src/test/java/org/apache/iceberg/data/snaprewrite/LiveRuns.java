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
package org.apache.iceberg.data.snaprewrite;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;

/**
 * Hands out disjoint clustered runs of still-live positions within one data file.
 *
 * <p>{@code WorkloadGenerator.generateClusteredPositions} draws run starts from the whole position
 * space on every call, so successive transactions collide and re-deleting an already-dead position
 * kills nothing. {@code TestSnapshotRewriteScale} compensates by counting distinct positions after
 * the fact. A sweep parameterized <em>by</em> the number of deaths cannot do that: it needs the
 * count it asked for to be the count that died, so the benchmark can assert the accounting
 * invariant rather than reconstruct it.
 *
 * <p>Runs walk forward over live positions, so they are contiguous while the file is dense and
 * stretch over holes as it empties. That is the right degradation: what drives compaction-map size
 * and deletion-vector encoding is the number of runs, not the number of positions in them.
 */
class LiveRuns {
  private final String path;
  private final int recordCount;
  private final BitSet live;
  private int remaining;

  LiveRuns(String path, int recordCount) {
    this.path = path;
    this.recordCount = recordCount;
    this.live = new BitSet(recordCount);
    this.live.set(0, recordCount);
    this.remaining = recordCount;
  }

  String path() {
    return path;
  }

  int remaining() {
    return remaining;
  }

  int dead() {
    return recordCount - remaining;
  }

  /**
   * Kills exactly {@code count} live positions, in runs of up to {@code runLength}, and returns
   * them sorted.
   */
  long[] takeClustered(Random rng, int count, int runLength) {
    Preconditions.checkArgument(
        count <= remaining, "Only %s live positions in %s, asked for %s", remaining, path, count);
    long[] taken = new long[count];
    int written = 0;
    while (written < count) {
      int pos = live.nextSetBit(rng.nextInt(recordCount));
      if (pos < 0) {
        // Wrapped past the end; remaining > 0 guarantees a hit from the start.
        pos = live.nextSetBit(0);
      }

      for (int j = 0; j < runLength && written < count && pos >= 0; j += 1) {
        taken[written] = pos;
        written += 1;
        live.clear(pos);
        remaining -= 1;
        pos = live.nextSetBit(pos + 1);
      }
    }

    Arrays.sort(taken);
    return taken;
  }

  /** Every position killed so far, as delete targets for this file. */
  List<Pair<CharSequence, Long>> deadSoFar() {
    List<Pair<CharSequence, Long>> positions = Lists.newArrayListWithCapacity(dead());
    for (int pos = live.nextClearBit(0); pos < recordCount; pos = live.nextClearBit(pos + 1)) {
      positions.add(Pair.of(path, (long) pos));
    }

    return positions;
  }

  /** The given positions, as delete targets for this file. */
  List<Pair<CharSequence, Long>> asTargets(long[] positions) {
    List<Pair<CharSequence, Long>> targets = Lists.newArrayListWithCapacity(positions.length);
    for (long pos : positions) {
      targets.add(Pair.of(path, pos));
    }

    return targets;
  }
}
