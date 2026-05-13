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
package org.apache.iceberg.benchmark.compaction;

import java.util.Locale;
import java.util.Random;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import java.util.List;

/**
 * Deterministic plan for a single fuzz iteration.
 *
 * <p>A {@code FuzzScenario} is fully described by a 64-bit seed. Two runs with the same seed
 * produce byte-identical operation sequences and byte-identical output hashes — this is the
 * determinism guarantee called out in {@code COMPACT_SPEC.md} §M1.
 *
 * <p>The scenario covers a small workload (~100k rows, ~4 source files) so each tuple runs in
 * seconds. The randomization knobs are deliberately narrow: the property under test is
 * confluence of the resolver/remapper, not workload diversity for its own sake.
 */
public final class FuzzScenario {

  private final long seed;
  private final BuildConfig buildConfig;
  private final List<LateTxOp> lateTxOps;

  private FuzzScenario(long seed, BuildConfig buildConfig, List<LateTxOp> lateTxOps) {
    this.seed = seed;
    this.buildConfig = buildConfig;
    this.lateTxOps = lateTxOps;
  }

  /** Master seed for this scenario. */
  public long seed() {
    return seed;
  }

  /** Warehouse build config (chain length, s0 rows, per-snapshot config). */
  public BuildConfig buildConfig() {
    return buildConfig;
  }

  /**
   * Late-transaction operation sequence. Each op is a single delete-only commit applied AFTER
   * compaction in the treatment path, or BEFORE compaction in the reference path. Each op is
   * confined to a contiguous slice of pre-compaction data files so the resolver doesn't trip the
   * "multiple resolver outputs targeting the same compacted file" gap that we documented in the
   * M2 test.
   */
  public List<LateTxOp> lateTxOps() {
    return lateTxOps;
  }

  /**
   * Produce a scenario for the given seed. Pure function of {@code seed} — no clocks, no global
   * state. The internal {@link Random} is the sole source of stochasticity, so callers MUST NOT
   * rely on iteration order of any {@code HashMap}/{@code HashSet} populated from this output
   * (the scenario only emits ordered structures).
   */
  public static FuzzScenario forSeed(long seed) {
    Random rng = new Random(seed);
    // ~100k rows S_0, 1-3 chain snapshots, 3-5 source files post-build.
    long s0Rows = 80_000L + rng.nextInt(40_000); // 80k..120k
    int chainLen = 1 + rng.nextInt(3); // 1..3
    long perSnapshotRows = 1_000L + rng.nextInt(2_000); // 1k..3k
    int perSnapshotDeletes = rng.nextInt(20); // 0..19
    int rowsPerFile = 25_000 + rng.nextInt(15_000); // 25k..40k → ~3-5 files
    int numLateTxOps = 1 + rng.nextInt(2); // 1..2 late txs (mostly the M2 shape)
    int lateTxRunLen = 1 + rng.nextInt(4); // 1..4

    BuildConfig config =
        BuildConfig.builder()
            .seed(seed)
            .s0Rows(s0Rows)
            .snapshotChainLength(chainLen)
            .perSnapshotRows(perSnapshotRows)
            .perSnapshotDeletes(perSnapshotDeletes)
            // The fuzz harness drives late txs by hand below, not via WarehouseBuilder's built-in
            // late-tx commit. Setting this to 0 ensures the builder's late-tx code path is
            // skipped.
            .lateTxDeletes(0)
            .lateTxRunLength(lateTxRunLen)
            .lateTxFileFanout(0)
            .rowsPerFile(rowsPerFile)
            .build();

    List<LateTxOp> ops = Lists.newArrayList();
    int deletesPerOp = 5 + rng.nextInt(10); // 5..14 deletes per op
    long opSeed = seed;
    // Slices are emitted DISJOINT across consecutive ops (matching the GDPR-style batch shape
    // that the M2 hand-crafted test exercises). Allowing overlapping slices would cause the same
    // source file to receive DVs from multiple ops, producing a multi-snapshot DV history that
    // the conflict detector + resolver currently surface as a known divergence (logged in
    // KNOWN_FAILURES.md). The harness's job is to vary workload shape within the contract the
    // resolver actually promises — overlap is out of scope.
    double cursor = rng.nextDouble() * 0.2; // 0..0.2 starting offset
    for (int i = 0; i < numLateTxOps; i++) {
      double remaining = 1.0 - cursor;
      double maxThisOp = remaining / (numLateTxOps - i);
      double widthFraction = Math.max(0.05, rng.nextDouble() * maxThisOp);
      ops.add(new LateTxOp(opSeed + 100L * (i + 1), cursor, widthFraction, deletesPerOp));
      cursor += widthFraction;
    }
    return new FuzzScenario(seed, config, ops);
  }

  /** Operation count (chain commits + late txs) — used in the JSON output for triage triage. */
  public int opsCount() {
    return 1 /* S_0 */
        + buildConfig.snapshotChainLength()
        + lateTxOps.size();
  }

  /** Human-readable summary for logs / failure dumps. */
  public String describe() {
    StringBuilder sb = new StringBuilder();
    sb.append(
        String.format(
            Locale.ROOT,
            "seed=%d s0Rows=%d chain=%d perSnapshotRows=%d perSnapshotDeletes=%d rowsPerFile=%d "
                + "lateTxRunLen=%d numLateTxOps=%d",
            seed,
            buildConfig.s0Rows(),
            buildConfig.snapshotChainLength(),
            buildConfig.perSnapshotRows(),
            buildConfig.perSnapshotDeletes(),
            buildConfig.rowsPerFile(),
            buildConfig.lateTxRunLength(),
            lateTxOps.size()));
    for (int i = 0; i < lateTxOps.size(); i++) {
      LateTxOp op = lateTxOps.get(i);
      sb.append(
          String.format(
              Locale.ROOT,
              "%n  op[%d]: opSeed=%d sliceOffsetFrac=%.4f sliceWidthFrac=%.4f deletesPerOp=%d",
              i,
              op.opSeed(),
              op.sliceOffsetFraction(),
              op.sliceWidthFraction(),
              op.deletesPerOp()));
    }
    return sb.toString();
  }

  /**
   * A single late-tx commit. The (offset, width) fractions are translated to a concrete
   * (fileOffset, fileWidth) by the runner at execution time, based on the pre-compaction data
   * file count. This indirection means a scenario plan is self-describing without depending on
   * the warehouse layout.
   */
  public static final class LateTxOp {
    private final long opSeed;
    private final double sliceOffsetFraction;
    private final double sliceWidthFraction;
    private final int deletesPerOp;

    LateTxOp(
        long opSeed, double sliceOffsetFraction, double sliceWidthFraction, int deletesPerOp) {
      this.opSeed = opSeed;
      this.sliceOffsetFraction = sliceOffsetFraction;
      this.sliceWidthFraction = sliceWidthFraction;
      this.deletesPerOp = deletesPerOp;
    }

    public long opSeed() {
      return opSeed;
    }

    public double sliceOffsetFraction() {
      return sliceOffsetFraction;
    }

    public double sliceWidthFraction() {
      return sliceWidthFraction;
    }

    public int deletesPerOp() {
      return deletesPerOp;
    }
  }
}
