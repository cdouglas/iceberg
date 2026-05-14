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

/**
 * Immutable configuration for one {@link WarehouseBuilder} build. Production sizes (per {@code
 * COMPACT_SPEC.md} §Workload) are produced by {@link #productionConfig(long, int)}; the {@link
 * Builder} defaults are tuned for tests at ~thousands of rows.
 */
public final class BuildConfig {
  private final long seed;
  private final long s0Rows;
  private final int snapshotChainLength;
  private final long perSnapshotRows;
  private final int perSnapshotDeletes;
  private final int lateTxDeletes;
  private final int lateTxRunLength;
  private final int lateTxFileFanout;
  private final int rowsPerFile;
  private final int formatVersion;
  private final boolean upgradeAfterChain;

  private BuildConfig(Builder builder) {
    this.seed = builder.seed;
    this.s0Rows = builder.s0Rows;
    this.snapshotChainLength = builder.snapshotChainLength;
    this.perSnapshotRows = builder.perSnapshotRows;
    this.perSnapshotDeletes = builder.perSnapshotDeletes;
    this.lateTxDeletes = builder.lateTxDeletes;
    this.lateTxRunLength = builder.lateTxRunLength;
    this.lateTxFileFanout = builder.lateTxFileFanout;
    this.rowsPerFile = builder.rowsPerFile;
    this.formatVersion = builder.formatVersion;
    this.upgradeAfterChain = builder.upgradeAfterChain;
  }

  /** Master seed; all per-stage seeds derive deterministically from this value. */
  public long seed() {
    return seed;
  }

  /** Rows in the {@code S_0} insert. Production: 10_000_000. */
  public long s0Rows() {
    return s0Rows;
  }

  /** Number of follow-on snapshots {@code S_1..S_n}. Production: 10. */
  public int snapshotChainLength() {
    return snapshotChainLength;
  }

  /** Rows inserted per follow-on snapshot. Production: 1_100_000. */
  public long perSnapshotRows() {
    return perSnapshotRows;
  }

  /**
   * Position deletes per follow-on snapshot, scattered uniformly across earlier files (run length
   * 1) so they accumulate into ~snapshotChainLength × this many runs in the compaction map.
   * Production: 1000 → ~10_000 runs.
   */
  public int perSnapshotDeletes() {
    return perSnapshotDeletes;
  }

  /** Total deletes in the late transaction {@code S_{n+1}}. K from §Workload (1k/10k/100k/1M). */
  public int lateTxDeletes() {
    return lateTxDeletes;
  }

  /** Run length within {@code S_{n+1}}'s clustered DV (GDPR-style batch shape). Production: 100. */
  public int lateTxRunLength() {
    return lateTxRunLength;
  }

  /**
   * If positive, restrict {@code S_{n+1}}'s deletes to this many of the largest pre-compaction
   * files. The spec's {@code K=1k, 10k} cells use {@code 2}; {@code K=100k, 1M} pass {@code 0}
   * (spread proportionally across all files).
   */
  public int lateTxFileFanout() {
    return lateTxFileFanout;
  }

  /** Approximate target rows per Parquet file. Production: ~1_700_000 (≈ 512 MB at ~304 B/row). */
  public int rowsPerFile() {
    return rowsPerFile;
  }

  /**
   * Iceberg format version the table is created with (2 or 3). Production builds default to 3.
   * Fuzz scenarios may select 2 to exercise position-delete-file write paths, or set this to 2
   * together with {@link #upgradeAfterChain()} to construct a table whose chain history was
   * written as v2 but whose late transactions land after an upgrade to v3.
   */
  public int formatVersion() {
    return formatVersion;
  }

  /**
   * When {@code true} and {@link #formatVersion()} is less than 3, the table is upgraded to v3
   * after the snapshot chain is built and before any late-transaction ops are applied. Has no
   * effect when {@code formatVersion == 3}. Used by the fuzz harness's {@code v2ThenUpgradeToV3}
   * bucket so the chain history contains position-delete files while late ops commit DVs against
   * the same table.
   */
  public boolean upgradeAfterChain() {
    return upgradeAfterChain;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Production config per COMPACT_SPEC.md §Workload, parameterized only by the master seed and the
   * late-transaction K. {@code lateTxFileFanout} is {@code 2} for K ≤ 10000 and {@code 0} (spread
   * across all files) otherwise.
   */
  public static BuildConfig productionConfig(long seed, int lateTxDeletes) {
    return builder()
        .seed(seed)
        .s0Rows(10_000_000L)
        .snapshotChainLength(10)
        .perSnapshotRows(1_100_000L)
        .perSnapshotDeletes(1000)
        .lateTxDeletes(lateTxDeletes)
        .lateTxRunLength(100)
        .lateTxFileFanout(lateTxDeletes <= 10_000 ? 2 : 0)
        .rowsPerFile(1_700_000)
        .build();
  }

  public static final class Builder {
    private long seed = 0L;
    private long s0Rows = 10_000L;
    private int snapshotChainLength = 3;
    private long perSnapshotRows = 1_000L;
    private int perSnapshotDeletes = 50;
    private int lateTxDeletes = 100;
    private int lateTxRunLength = 10;
    private int lateTxFileFanout = 0;
    private int rowsPerFile = 5_000;
    private int formatVersion = 3;
    private boolean upgradeAfterChain = false;

    private Builder() {}

    public Builder seed(long value) {
      this.seed = value;
      return this;
    }

    public Builder s0Rows(long value) {
      this.s0Rows = value;
      return this;
    }

    public Builder snapshotChainLength(int value) {
      this.snapshotChainLength = value;
      return this;
    }

    public Builder perSnapshotRows(long value) {
      this.perSnapshotRows = value;
      return this;
    }

    public Builder perSnapshotDeletes(int value) {
      this.perSnapshotDeletes = value;
      return this;
    }

    public Builder lateTxDeletes(int value) {
      this.lateTxDeletes = value;
      return this;
    }

    public Builder lateTxRunLength(int value) {
      this.lateTxRunLength = value;
      return this;
    }

    public Builder lateTxFileFanout(int value) {
      this.lateTxFileFanout = value;
      return this;
    }

    public Builder rowsPerFile(int value) {
      this.rowsPerFile = value;
      return this;
    }

    public Builder formatVersion(int value) {
      this.formatVersion = value;
      return this;
    }

    public Builder upgradeAfterChain(boolean value) {
      this.upgradeAfterChain = value;
      return this;
    }

    public BuildConfig build() {
      if (snapshotChainLength < 0) {
        throw new IllegalArgumentException("snapshotChainLength must be >= 0");
      }
      if (s0Rows < 0 || perSnapshotRows < 0) {
        throw new IllegalArgumentException("row counts must be >= 0");
      }
      if (lateTxDeletes < 0) {
        throw new IllegalArgumentException("lateTxDeletes must be >= 0");
      }
      if (lateTxRunLength < 1) {
        throw new IllegalArgumentException("lateTxRunLength must be >= 1");
      }
      if (rowsPerFile <= 0) {
        throw new IllegalArgumentException("rowsPerFile must be > 0");
      }
      if (formatVersion != 2 && formatVersion != 3) {
        throw new IllegalArgumentException("formatVersion must be 2 or 3, got " + formatVersion);
      }
      if (upgradeAfterChain && formatVersion >= 3) {
        throw new IllegalArgumentException(
            "upgradeAfterChain only makes sense when formatVersion < 3");
      }
      return new BuildConfig(this);
    }
  }
}
