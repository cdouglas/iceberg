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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 * Tunable knobs that shape a fuzz run. Loaded from JSON via {@link #load(Path)} or constructed
 * via {@link #defaults()}; together with the master seed it forms the determinism key for {@link
 * FuzzScenario#forSeed}.
 *
 * <p>Weights are normalized at load time so callers can use either probabilities (summing to 1)
 * or arbitrary positive numbers (e.g. {@code 1.0, 1.0, 1.0} for uniform). Ranges are inclusive
 * integer pairs; min must be {@code <=} max.
 *
 * <p>Defaults exist for every field, so a partial JSON config that overrides only one or two
 * knobs is well-defined.
 */
public final class FuzzConfig {

  /** Format buckets a scenario can fall into. */
  public enum FormatBucket {
    V2,
    V3,
    V2_THEN_UPGRADE_TO_V3
  }

  /** Op kinds that can be emitted by a scenario. */
  public enum OpKind {
    POSITION_DELETE,
    APPEND,
    ROW_REPLACEMENT,
    EQUALITY_DELETE
  }

  private final Map<FormatBucket, Double> formatWeights;
  private final Map<OpKind, Double> opKindWeights;
  private final IntRange lateTxCount;
  private final double overlapProbability;
  private final IntRange deletesPerOp;
  private final IntRange appendRowsPerOp;
  private final IntRange replacementRows;
  private final IntRange equalityDeleteRowsPerOp;

  private FuzzConfig(
      Map<FormatBucket, Double> formatWeights,
      Map<OpKind, Double> opKindWeights,
      IntRange lateTxCount,
      double overlapProbability,
      IntRange deletesPerOp,
      IntRange appendRowsPerOp,
      IntRange replacementRows,
      IntRange equalityDeleteRowsPerOp) {
    this.formatWeights = normalize(formatWeights);
    this.opKindWeights = normalize(opKindWeights);
    this.lateTxCount = lateTxCount;
    this.overlapProbability = overlapProbability;
    this.deletesPerOp = deletesPerOp;
    this.appendRowsPerOp = appendRowsPerOp;
    this.replacementRows = replacementRows;
    this.equalityDeleteRowsPerOp = equalityDeleteRowsPerOp;
  }

  public Map<FormatBucket, Double> formatWeights() {
    return formatWeights;
  }

  public Map<OpKind, Double> opKindWeights() {
    return opKindWeights;
  }

  public IntRange lateTxCount() {
    return lateTxCount;
  }

  public double overlapProbability() {
    return overlapProbability;
  }

  public IntRange deletesPerOp() {
    return deletesPerOp;
  }

  public IntRange appendRowsPerOp() {
    return appendRowsPerOp;
  }

  public IntRange replacementRows() {
    return replacementRows;
  }

  public IntRange equalityDeleteRowsPerOp() {
    return equalityDeleteRowsPerOp;
  }

  public FormatBucket sampleFormat(Random rng) {
    return weightedSample(rng, formatWeights);
  }

  public OpKind sampleOpKind(Random rng) {
    return weightedSample(rng, opKindWeights);
  }

  public int sampleLateTxCount(Random rng) {
    return lateTxCount.sample(rng);
  }

  /** Defaults: uniform format/op weights; tx count 1..8; ranges per design spec. */
  public static FuzzConfig defaults() {
    Map<FormatBucket, Double> formats = new LinkedHashMap<>();
    formats.put(FormatBucket.V2, 1.0);
    formats.put(FormatBucket.V3, 1.0);
    formats.put(FormatBucket.V2_THEN_UPGRADE_TO_V3, 1.0);

    Map<OpKind, Double> ops = new LinkedHashMap<>();
    ops.put(OpKind.POSITION_DELETE, 1.0);
    ops.put(OpKind.APPEND, 1.0);
    ops.put(OpKind.ROW_REPLACEMENT, 1.0);
    ops.put(OpKind.EQUALITY_DELETE, 1.0);

    return new FuzzConfig(
        formats,
        ops,
        new IntRange(1, 8),
        0.5,
        new IntRange(5, 14),
        new IntRange(500, 3000),
        new IntRange(50, 500),
        new IntRange(1, 20));
  }

  /**
   * Load a JSON config file. Missing top-level fields fall back to {@link #defaults()}, so a
   * partial config is well-defined.
   */
  public static FuzzConfig load(Path path) throws IOException {
    if (path == null) {
      return defaults();
    }
    byte[] bytes = Files.readAllBytes(path);
    if (bytes.length == 0) {
      return defaults();
    }
    ObjectMapper mapper = new ObjectMapper();
    JsonShape raw = mapper.readValue(bytes, JsonShape.class);
    return raw.toConfig();
  }

  // --------- weight + range helpers ---------

  private static <K extends Enum<K>> Map<K, Double> normalize(Map<K, Double> weights) {
    double total = 0.0;
    for (double w : weights.values()) {
      if (w < 0.0) {
        throw new IllegalArgumentException("weights must be >= 0");
      }
      total += w;
    }
    if (total <= 0.0) {
      throw new IllegalArgumentException("at least one weight must be > 0");
    }
    Map<K, Double> normalized = new LinkedHashMap<>(weights.size());
    for (Map.Entry<K, Double> entry : weights.entrySet()) {
      normalized.put(entry.getKey(), entry.getValue() / total);
    }
    return normalized;
  }

  private static <K extends Enum<K>> K weightedSample(Random rng, Map<K, Double> weights) {
    double pick = rng.nextDouble();
    double cum = 0.0;
    K last = null;
    for (Map.Entry<K, Double> entry : weights.entrySet()) {
      cum += entry.getValue();
      last = entry.getKey();
      if (pick < cum) {
        return entry.getKey();
      }
    }
    // Floating-point rounding can leave `pick` >= cum at the last entry; fall back to it.
    return last;
  }

  /** Inclusive {@code [min, max]} integer range with uniform sampling. */
  public static final class IntRange {
    private final int min;
    private final int max;

    @JsonCreator
    public IntRange(@JsonProperty("min") int min, @JsonProperty("max") int max) {
      if (min > max) {
        throw new IllegalArgumentException("min (" + min + ") > max (" + max + ")");
      }
      if (min < 0) {
        throw new IllegalArgumentException("min must be >= 0");
      }
      this.min = min;
      this.max = max;
    }

    public int min() {
      return min;
    }

    public int max() {
      return max;
    }

    public int sample(Random rng) {
      if (min == max) {
        return min;
      }
      return min + rng.nextInt(max - min + 1);
    }
  }

  /** Internal Jackson shape mirroring the JSON layout in README/design docs. */
  static final class JsonShape {
    public Map<String, Double> formatWeights;
    public Map<String, Double> opKindWeights;
    public IntRange lateTxCount;
    public Double overlapProbability;
    public IntRange deletesPerOp;
    public IntRange appendRowsPerOp;
    public IntRange replacementRows;
    public IntRange equalityDeleteRowsPerOp;

    FuzzConfig toConfig() {
      FuzzConfig defaults = defaults();
      Map<FormatBucket, Double> fmt = mergeFormatWeights(formatWeights, defaults.formatWeights);
      Map<OpKind, Double> ops = mergeOpWeights(opKindWeights, defaults.opKindWeights);
      double overlap =
          overlapProbability == null ? defaults.overlapProbability : overlapProbability;
      if (overlap < 0.0 || overlap > 1.0) {
        throw new IllegalArgumentException(
            "overlapProbability must be in [0, 1], got " + overlap);
      }
      return new FuzzConfig(
          fmt,
          ops,
          lateTxCount == null ? defaults.lateTxCount : lateTxCount,
          overlap,
          deletesPerOp == null ? defaults.deletesPerOp : deletesPerOp,
          appendRowsPerOp == null ? defaults.appendRowsPerOp : appendRowsPerOp,
          replacementRows == null ? defaults.replacementRows : replacementRows,
          equalityDeleteRowsPerOp == null
              ? defaults.equalityDeleteRowsPerOp
              : equalityDeleteRowsPerOp);
    }

    private static Map<FormatBucket, Double> mergeFormatWeights(
        Map<String, Double> raw, Map<FormatBucket, Double> fallback) {
      if (raw == null || raw.isEmpty()) {
        return new LinkedHashMap<>(fallback);
      }
      Map<FormatBucket, Double> out = new LinkedHashMap<>();
      out.put(FormatBucket.V2, raw.getOrDefault("v2", 0.0));
      out.put(FormatBucket.V3, raw.getOrDefault("v3", 0.0));
      out.put(FormatBucket.V2_THEN_UPGRADE_TO_V3, raw.getOrDefault("v2ThenUpgradeToV3", 0.0));
      return out;
    }

    private static Map<OpKind, Double> mergeOpWeights(
        Map<String, Double> raw, Map<OpKind, Double> fallback) {
      if (raw == null || raw.isEmpty()) {
        return new LinkedHashMap<>(fallback);
      }
      Map<OpKind, Double> out = new LinkedHashMap<>();
      out.put(OpKind.POSITION_DELETE, raw.getOrDefault("positionDelete", 0.0));
      out.put(OpKind.APPEND, raw.getOrDefault("append", 0.0));
      out.put(OpKind.ROW_REPLACEMENT, raw.getOrDefault("rowReplacement", 0.0));
      out.put(OpKind.EQUALITY_DELETE, raw.getOrDefault("equalityDelete", 0.0));
      return out;
    }
  }
}
