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
package org.apache.iceberg.data;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

/**
 * Deterministic data and delete generators backing both {@link SetupMain} and {@link FuzzMain}.
 *
 * <p>This is the project's single nondeterminism choke point. All randomness flows through {@link
 * Random} instances seeded from explicit longs handed in by the caller. Schema-shape and column
 * order are public-API-stable; reordering would change the byte layout of every fixture.
 *
 * <p>See {@code COMPACT_SPEC.md} §Schema and §Workload for the row contract and the GDPR-style
 * clustered-delete shape this implements.
 *
 * <p>Lives here rather than in the benchmark module because two suites generate workloads from it:
 * the compaction baseline benchmark and the snapshot-rewrite fuzzer. The benchmark hard-targets
 * Spark 3.5, so depending on it from {@code iceberg-data} would make this module's tests require
 * Spark; sharing through test fixtures keeps both callers on the same generator without that.
 */
public final class WorkloadGenerator {

  private static final char[] SHORT_STRING_ALPHABET =
      "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

  private static final int SHORT_STRING_LEN = 16;

  /**
   * Canonical 20-column schema. Order is load-bearing for fixture determinism — do not reorder
   * fields. Field IDs are 1-based and contiguous.
   */
  public static final Schema SCHEMA = buildSchema();

  private WorkloadGenerator() {}

  private static Schema buildSchema() {
    List<Types.NestedField> fields = Lists.newArrayListWithCapacity(20);
    int id = 1;
    for (int i = 0; i < 4; i++) {
      fields.add(Types.NestedField.required(id++, "uuid_" + i, Types.StringType.get()));
    }
    for (int i = 0; i < 8; i++) {
      fields.add(Types.NestedField.required(id++, "long_" + i, Types.LongType.get()));
    }
    for (int i = 0; i < 4; i++) {
      fields.add(Types.NestedField.required(id++, "short_" + i, Types.StringType.get()));
    }
    for (int i = 0; i < 4; i++) {
      fields.add(Types.NestedField.required(id++, "dbl_" + i, Types.DoubleType.get()));
    }
    return new Schema(fields);
  }

  /**
   * Generate {@code count} rows from a seeded RNG. Two calls with the same seed and count yield
   * byte-identical record sequences.
   */
  public static List<Record> generateRows(long seed, int count) {
    Random rng = new Random(seed);
    GenericRecord template = GenericRecord.create(SCHEMA);
    List<Record> rows = Lists.newArrayListWithCapacity(count);
    for (int i = 0; i < count; i++) {
      rows.add(nextRow(rng, template));
    }
    return rows;
  }

  /**
   * Generate a single row using the supplied RNG. Mutates {@code rng}.
   *
   * <p>Public because callers outside this package stream rows one at a time rather than
   * materializing a list; it was package-private when the generator and its callers shared a package.
   */
  public static Record nextRow(Random rng, GenericRecord template) {
    GenericRecord row = template.copy();
    int idx = 0;
    for (int i = 0; i < 4; i++) {
      row.set(idx++, nextUuidString(rng));
    }
    for (int i = 0; i < 8; i++) {
      row.set(idx++, rng.nextLong());
    }
    for (int i = 0; i < 4; i++) {
      row.set(idx++, nextShortString(rng));
    }
    for (int i = 0; i < 4; i++) {
      row.set(idx++, rng.nextDouble());
    }
    return row;
  }

  // UUID.randomUUID() uses SecureRandom internally — we route through Random for determinism.
  private static String nextUuidString(Random rng) {
    long mostSig = rng.nextLong();
    long leastSig = rng.nextLong();
    return new UUID(mostSig, leastSig).toString();
  }

  private static String nextShortString(Random rng) {
    char[] buf = new char[SHORT_STRING_LEN];
    for (int i = 0; i < SHORT_STRING_LEN; i++) {
      buf[i] = SHORT_STRING_ALPHABET[rng.nextInt(SHORT_STRING_ALPHABET.length)];
    }
    return new String(buf);
  }

  /**
   * Generate ascending, deduplicated positions in {@code [0, maxPosition)} clustered into runs of
   * approximately {@code targetRunLength} contiguous deletes each.
   *
   * <p>For the {@code S_{n+1}} cells in COMPACT_SPEC.md §Workload, callers pass {@code
   * targetRunLength = 100}, producing ~10 / 100 / 1k / 10k runs for K = 1k / 10k / 100k / 1M. Pass
   * {@code targetRunLength = 1} to produce a uniformly scattered set (the shape used inside {@code
   * S_1..S_{10}} during pre-compaction state setup).
   *
   * <p>Run starts collide occasionally; duplicates are dropped after sorting, so the returned
   * length may be slightly less than {@code totalDeletes}.
   */
  public static long[] generateClusteredPositions(
      long seed, long maxPosition, int totalDeletes, int targetRunLength) {
    if (totalDeletes <= 0) {
      return new long[0];
    }
    if (targetRunLength < 1) {
      throw new IllegalArgumentException("targetRunLength must be >= 1");
    }
    if (maxPosition < totalDeletes) {
      throw new IllegalArgumentException(
          "maxPosition (" + maxPosition + ") must be >= totalDeletes (" + totalDeletes + ")");
    }

    Random rng = new Random(seed);
    long[] buffer = new long[totalDeletes];
    int written = 0;
    int deletesRemaining = totalDeletes;

    while (deletesRemaining > 0) {
      int runLen = Math.min(deletesRemaining, targetRunLength);
      long maxStart = maxPosition - runLen;
      if (maxStart <= 0) {
        // Table is barely larger than the deletion request: fall back to scattering one delete
        // per remaining slot. Should be unreachable for the spec'd K values but kept as a safety.
        for (long pos = 0; pos < maxPosition && written < totalDeletes; pos++) {
          buffer[written++] = pos;
        }
        break;
      }
      long start = (long) (rng.nextDouble() * maxStart);
      for (int j = 0; j < runLen; j++) {
        buffer[written++] = start + j;
      }
      deletesRemaining -= runLen;
    }

    long[] sorted = Arrays.copyOf(buffer, written);
    Arrays.sort(sorted);
    int unique = 0;
    for (int i = 0; i < sorted.length; i++) {
      if (i == 0 || sorted[i] != sorted[i - 1]) {
        sorted[unique++] = sorted[i];
      }
    }
    return Arrays.copyOf(sorted, unique);
  }
}
