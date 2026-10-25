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
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

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
 *   <li>{@link Kind#OPAQUE}: anything else, which the retry may check in ways the footprint does
 *       not say (a custom ancestry validator, an appended manifest that keeps its path, an unknown
 *       producer).
 * </ul>
 */
public final class CommitFootprint implements Serializable {

  /** The kinds of footprint. */
  public enum Kind {
    BLIND,
    OPAQUE
  }

  public static final CommitFootprint BLIND = new CommitFootprint(Kind.BLIND);
  public static final CommitFootprint OPAQUE = new CommitFootprint(Kind.OPAQUE);

  private final Kind kind;

  private CommitFootprint(Kind kind) {
    this.kind = kind;
  }

  public Kind kind() {
    return kind;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof CommitFootprint && ((CommitFootprint) other).kind == kind;
  }

  @Override
  public int hashCode() {
    return kind.hashCode();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("kind", kind).toString();
  }
}
