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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Order-independent row-multiset hash for the post-run correctness check called out in
 * COMPACT_SPEC.md §"Correctness Check":
 *
 * <pre>
 * df.selectExpr("xxhash64(struct(*))").agg(sum("xxhash64")).first()
 * </pre>
 *
 * <p>Two tables with identical row multisets (regardless of physical layout, file order, or
 * snapshot history) hash to the same value. The hash is also resilient to row ordering within a
 * partition since {@code sum} is associative + commutative.
 *
 * <p>Used by:
 *
 * <ul>
 *   <li>M2 — assert that {@code resolver.resolve(both)} produces the same row state as a fresh
 *       {@code compact(state ∪ both)} baseline.
 *   <li>M5 — assert that mutating the compaction map or corrupting a Parquet output changes the
 *       hash, proving the check is not vacuous.
 *   <li>Eventually, the runner's post-scenario assertion (Phase 7).
 * </ul>
 */
public final class CorrectnessCheck {

  private CorrectnessCheck() {}

  /**
   * Compute the row-multiset hash of every live row in the table located at the given Iceberg
   * URI (e.g., {@code file:///tmp/warehouse/db.baseline} or {@code s3a://...}). Live rows are
   * those visible at the table's current snapshot, with all delete files applied.
   */
  public static long hash(SparkSession spark, String tableUri) {
    Row aggregated =
        spark
            .read()
            .format("iceberg")
            .load(tableUri)
            .select(functions.expr("xxhash64(struct(*)) AS row_hash"))
            .agg(functions.sum("row_hash"))
            .first();
    if (aggregated == null || aggregated.get(0) == null) {
      // Empty table or all-null aggregation — treat as zero.
      return 0L;
    }
    return aggregated.getLong(0);
  }

  /**
   * Same as {@link #hash(SparkSession, String)} but asserts the two hashes are equal and throws
   * a {@link CorrectnessCheckFailedException} otherwise. Returns the (common) hash on success.
   */
  public static long assertEqual(SparkSession spark, String tableA, String tableB) {
    long hashA = hash(spark, tableA);
    long hashB = hash(spark, tableB);
    if (hashA != hashB) {
      throw new CorrectnessCheckFailedException(tableA, tableB, hashA, hashB);
    }
    return hashA;
  }

  /** Thrown by {@link #assertEqual} when the two tables diverge. */
  public static final class CorrectnessCheckFailedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final String left;
    private final String right;
    private final long leftHash;
    private final long rightHash;

    private CorrectnessCheckFailedException(
        String left, String right, long leftHash, long rightHash) {
      super(
          String.format(
              Locale.ROOT,
              "Correctness check failed: %s hashed to %d, %s hashed to %d",
              left,
              leftHash,
              right,
              rightHash));
      this.left = left;
      this.right = right;
      this.leftHash = leftHash;
      this.rightHash = rightHash;
    }

    public String left() {
      return left;
    }

    public String right() {
      return right;
    }

    public long leftHash() {
      return leftHash;
    }

    public long rightHash() {
      return rightHash;
    }
  }
}
