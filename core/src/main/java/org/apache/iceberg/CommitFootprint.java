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

import java.io.Serializable;
import java.util.List;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.Pair;

/**
 * What a snapshot producer's commit depends on, beyond what it writes: what its retry, on a table
 * that changed since it read it, would check again. A catalog that merges a commit onto the table
 * where it lands, instead of failing it and letting the producer retry, may do so only where that
 * check would pass; this says what the check is, which the metadata the commit writes cannot (an
 * insert-only row delta reports the same operation as an append, and an exactly-once sink's append
 * carries a validator).
 *
 * <ul>
 *   <li>{@link Kind#BLIND}: the commit validates nothing and writes only new manifests: its retry
 *       commits on any version of its branch, with the same files (a fast or merge append);
 *   <li>{@link Kind#VALIDATED}: its retry checks, of the commits since, only the {@link #checks}:
 *       that no files of a kind were added or removed in a scope of the table (the producer's
 *       validations, and the filters and partitions its retry deletes from afresh);
 *   <li>{@link Kind#OPAQUE}: anything else, which the retry may check in ways the footprint does
 *       not say (a custom ancestry validator, an appended manifest that keeps its path, an unknown
 *       producer).
 * </ul>
 */
public final class CommitFootprint implements Serializable {

  /** The kinds of footprint. */
  public enum Kind {
    BLIND,
    VALIDATED,
    OPAQUE
  }

  public static final CommitFootprint BLIND = new CommitFootprint(Kind.BLIND, ImmutableList.of());
  public static final CommitFootprint OPAQUE = new CommitFootprint(Kind.OPAQUE, ImmutableList.of());

  private final Kind kind;
  private final List<Check> checks;

  private CommitFootprint(Kind kind, List<Check> checks) {
    this.kind = kind;
    this.checks = checks;
  }

  /** A validated commit's footprint: its retry checks only {@code checks}. */
  public static CommitFootprint validated(List<Check> checks) {
    return new CommitFootprint(Kind.VALIDATED, ImmutableList.copyOf(checks));
  }

  public Kind kind() {
    return kind;
  }

  /** What a validated commit's retry checks of the commits since; empty for another kind. */
  public List<Check> checks() {
    return checks;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof CommitFootprint
        && ((CommitFootprint) other).kind == kind
        && ((CommitFootprint) other).checks.equals(checks);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(kind, checks);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("kind", kind).add("checks", checks).toString();
  }

  /**
   * One thing a retry checks of the commits since the snapshot it validates from: that none added
   * data files, added delete files, or removed data files, in a {@link Scope}.
   */
  public static final class Check implements Serializable {

    /** What no commit since may have done in the scope. */
    public enum Kind {
      /** Added data files (validateAddedDataFiles; a filter or partition a retry deletes). */
      NO_DATA_ADDED,
      /** Added delete files (validateNoNewDeleteFiles, validateNoNewDeletesForDataFiles). */
      NO_DELETES_ADDED,
      /** Removed data files (validateDeletedDataFiles, validateDataFilesExist). */
      NO_DATA_REMOVED
    }

    private final Kind kind;
    private final Scope scope;

    private Check(Kind kind, Scope scope) {
      this.kind = kind;
      this.scope = scope;
    }

    public static Check of(Kind kind, Scope scope) {
      Preconditions.checkArgument(
          kind != null && scope != null, "Invalid check: %s %s", kind, scope);
      return new Check(kind, scope);
    }

    public Kind kind() {
      return kind;
    }

    public Scope scope() {
      return scope;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Check
          && ((Check) other).kind == kind
          && ((Check) other).scope.equals(scope);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(kind, scope);
    }

    @Override
    public String toString() {
      return kind + " in " + scope;
    }
  }

  /**
   * A scope of a table's files: all of them, those that may hold rows matching a row filter, or
   * those in some partitions, each a partition tuple of a spec.
   */
  public static final class Scope implements Serializable {
    private static final Scope EVERYTHING = new Scope(null, null);

    private final Expression filter;
    private final List<Pair<Integer, StructLike>> partitions;

    private Scope(Expression filter, List<Pair<Integer, StructLike>> partitions) {
      this.filter = filter;
      this.partitions = partitions;
    }

    public static Scope everything() {
      return EVERYTHING;
    }

    /** The files that may hold rows matching {@code filter}; every file for a null filter. */
    public static Scope filter(Expression filter) {
      return filter != null ? new Scope(filter, null) : EVERYTHING;
    }

    /** The files in {@code partitions}, each a spec id and a partition tuple of it. */
    public static Scope partitions(Iterable<Pair<Integer, StructLike>> partitions) {
      return new Scope(null, ImmutableList.copyOf(partitions));
    }

    /** Whether it is every file. */
    public boolean isEverything() {
      return filter == null && partitions == null;
    }

    /** The row filter, or null if it is not a filter. */
    public Expression filter() {
      return filter;
    }

    /** The partitions, or null if it is not partitions. */
    public List<Pair<Integer, StructLike>> partitions() {
      return partitions;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Scope)) {
        return false;
      }
      Scope that = (Scope) other;
      return String.valueOf(filter).equals(String.valueOf(that.filter))
          && Objects.equal(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(String.valueOf(filter), partitions);
    }

    @Override
    public String toString() {
      return isEverything() ? "everything" : filter != null ? filter.toString() : "" + partitions;
    }
  }
}
