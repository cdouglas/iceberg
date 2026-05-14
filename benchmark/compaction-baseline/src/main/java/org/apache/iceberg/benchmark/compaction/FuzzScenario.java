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

import java.util.List;
import java.util.Locale;
import java.util.Random;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Deterministic plan for a single fuzz iteration.
 *
 * <p>A {@code FuzzScenario} is fully described by a 64-bit seed together with the {@link
 * FuzzConfig} in effect at planning time. Two runs with the same {@code (seed, config)} pair
 * produce byte-identical operation sequences and byte-identical output hashes — this is the
 * determinism guarantee called out in {@code COMPACT_SPEC.md} §M1 (extended to also key on the
 * config).
 *
 * <p>The scenario covers a small workload (~100k rows, ~4 source files) so each tuple runs in
 * seconds. Workload diversity now comes from three axes:
 *
 * <ul>
 *   <li>format bucket — v2, v3, or v2-then-upgrade-to-v3 (config weighted);
 *   <li>op kinds — position delete, append, row replacement, equality delete (config weighted);
 *   <li>slice geometry — overlapping vs. disjoint with probability {@link
 *       FuzzConfig#overlapProbability()}.
 * </ul>
 */
public final class FuzzScenario {

  private final long seed;
  private final FuzzConfig.FormatBucket formatBucket;
  private final BuildConfig buildConfig;
  private final List<LateTxOp> lateTxOps;

  private FuzzScenario(
      long seed,
      FuzzConfig.FormatBucket formatBucket,
      BuildConfig buildConfig,
      List<LateTxOp> lateTxOps) {
    this.seed = seed;
    this.formatBucket = formatBucket;
    this.buildConfig = buildConfig;
    this.lateTxOps = lateTxOps;
  }

  public long seed() {
    return seed;
  }

  public FuzzConfig.FormatBucket formatBucket() {
    return formatBucket;
  }

  public BuildConfig buildConfig() {
    return buildConfig;
  }

  public List<LateTxOp> lateTxOps() {
    return lateTxOps;
  }

  /** Produce a scenario using the default config; preserved for back-compat with older tests. */
  public static FuzzScenario forSeed(long seed) {
    return forSeed(seed, FuzzConfig.defaults());
  }

  /**
   * Produce a scenario for {@code (seed, cfg)}. Pure function of its arguments — no clocks, no
   * global state. The internal {@link Random} is the sole source of stochasticity, so callers MUST
   * NOT rely on iteration order of any {@code HashMap}/{@code HashSet} populated from this output.
   */
  public static FuzzScenario forSeed(long seed, FuzzConfig cfg) {
    Random rng = new Random(seed);

    // ~100k rows S_0, 1-3 chain snapshots, 3-5 source files post-build.
    long s0Rows = 80_000L + rng.nextInt(40_000); // 80k..120k
    int chainLen = 1 + rng.nextInt(3); // 1..3
    long perSnapshotRows = 1_000L + rng.nextInt(2_000); // 1k..3k
    int perSnapshotDeletes = rng.nextInt(20); // 0..19
    int rowsPerFile = 25_000 + rng.nextInt(15_000); // 25k..40k → ~3-5 files

    FuzzConfig.FormatBucket formatBucket = cfg.sampleFormat(rng);
    int formatVersion;
    boolean upgradeAfterChain;
    switch (formatBucket) {
      case V2:
        formatVersion = 2;
        upgradeAfterChain = false;
        break;
      case V2_THEN_UPGRADE_TO_V3:
        formatVersion = 2;
        upgradeAfterChain = true;
        break;
      case V3:
      default:
        formatVersion = 3;
        upgradeAfterChain = false;
        break;
    }

    BuildConfig buildConfig =
        BuildConfig.builder()
            .seed(seed)
            .s0Rows(s0Rows)
            .snapshotChainLength(chainLen)
            .perSnapshotRows(perSnapshotRows)
            .perSnapshotDeletes(perSnapshotDeletes)
            // The fuzz harness drives late txs by hand via FuzzRunner.applyLateTx, not via
            // WarehouseBuilder's built-in late-tx commit. Setting this to 0 ensures the
            // builder's late-tx code path is skipped.
            .lateTxDeletes(0)
            .lateTxRunLength(1)
            .lateTxFileFanout(0)
            .rowsPerFile(rowsPerFile)
            .formatVersion(formatVersion)
            .upgradeAfterChain(upgradeAfterChain)
            .build();

    int numLateTxOps = cfg.sampleLateTxCount(rng);
    List<LateTxOp> ops = Lists.newArrayListWithCapacity(numLateTxOps);

    // Overlap-aware slice allocation. The non-overlap arm reuses the original disjoint algorithm
    // (advance a cursor through [0, 1), allocating one slice per op). The overlap arm picks a
    // prior interval and lands the new offset inside it. With p=cfg.overlapProbability() and
    // n>=2 ops, the chance of "no overlap at all in this scenario" is (1-p)^(n-1) — so p=0
    // recovers the old strictly-disjoint shape exactly, while p=1 forces overlap on every op
    // after the first.
    List<double[]> intervals = Lists.newArrayList();
    double cursor = rng.nextDouble() * 0.1;
    for (int i = 0; i < numLateTxOps; i++) {
      boolean overlap =
          !intervals.isEmpty() && rng.nextDouble() < cfg.overlapProbability();
      double offset;
      double width;
      if (overlap) {
        double[] prior = intervals.get(rng.nextInt(intervals.size()));
        offset = Math.min(prior[0] + rng.nextDouble() * Math.max(prior[1], 1e-9), 0.95);
        width = Math.max(0.05, rng.nextDouble() * (1.0 - offset));
      } else {
        double remaining = 1.0 - cursor;
        if (remaining <= 0.05 && !intervals.isEmpty()) {
          // No room left at the end; fall back to overlapping the most recent prior interval
          // rather than emitting a zero-width or out-of-range slice.
          double[] prior = intervals.get(intervals.size() - 1);
          offset = prior[0];
          width = prior[1];
        } else {
          int remainingOps = Math.max(1, numLateTxOps - i);
          double maxThisOp = remaining / remainingOps;
          offset = cursor;
          width = Math.max(0.05, rng.nextDouble() * maxThisOp);
          cursor = Math.min(offset + width, 1.0);
        }
      }
      intervals.add(new double[] {offset, width});
      long opSeed = seed + 100L * (i + 1L);
      ops.add(buildOpForKind(rng, cfg, opSeed, offset, width));
    }
    return new FuzzScenario(seed, formatBucket, buildConfig, ops);
  }

  private static LateTxOp buildOpForKind(
      Random rng, FuzzConfig cfg, long opSeed, double offset, double width) {
    FuzzConfig.OpKind kind = cfg.sampleOpKind(rng);
    switch (kind) {
      case POSITION_DELETE:
        return new PositionDeleteOp(opSeed, offset, width, cfg.deletesPerOp().sample(rng));
      case APPEND:
        int rowsPerFile = 25_000 + rng.nextInt(15_000);
        return new AppendOp(opSeed, offset, width, cfg.appendRowsPerOp().sample(rng), rowsPerFile);
      case ROW_REPLACEMENT:
        return new RowReplacementOp(
            opSeed,
            offset,
            width,
            cfg.deletesPerOp().sample(rng),
            cfg.replacementRows().sample(rng));
      case EQUALITY_DELETE:
      default:
        return new EqualityDeleteOp(
            opSeed, offset, width, cfg.equalityDeleteRowsPerOp().sample(rng));
    }
  }

  /** Operation count (chain commits + late txs) — used in the JSON output for triage. */
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
            "seed=%d format=%s s0Rows=%d chain=%d perSnapshotRows=%d perSnapshotDeletes=%d "
                + "rowsPerFile=%d numLateTxOps=%d",
            seed,
            formatBucket,
            buildConfig.s0Rows(),
            buildConfig.snapshotChainLength(),
            buildConfig.perSnapshotRows(),
            buildConfig.perSnapshotDeletes(),
            buildConfig.rowsPerFile(),
            lateTxOps.size()));
    for (int i = 0; i < lateTxOps.size(); i++) {
      LateTxOp op = lateTxOps.get(i);
      sb.append(
          String.format(
              Locale.ROOT,
              "%n  op[%d]: kind=%s opSeed=%d sliceOffsetFrac=%.4f sliceWidthFrac=%.4f%s",
              i,
              op.kind(),
              op.opSeed(),
              op.sliceOffsetFraction(),
              op.sliceWidthFraction(),
              describeOpPayload(op)));
    }
    return sb.toString();
  }

  private static String describeOpPayload(LateTxOp op) {
    if (op instanceof PositionDeleteOp) {
      return " deletesPerOp=" + ((PositionDeleteOp) op).deletesPerOp();
    } else if (op instanceof AppendOp) {
      AppendOp ap = (AppendOp) op;
      return " rows=" + ap.rows() + " rowsPerFile=" + ap.rowsPerFile();
    } else if (op instanceof RowReplacementOp) {
      RowReplacementOp rr = (RowReplacementOp) op;
      return " deletesPerOp=" + rr.deletesPerOp() + " replacementRows=" + rr.replacementRows();
    } else if (op instanceof EqualityDeleteOp) {
      return " rowsPerOp=" + ((EqualityDeleteOp) op).rowsPerOp();
    }
    return "";
  }
}
