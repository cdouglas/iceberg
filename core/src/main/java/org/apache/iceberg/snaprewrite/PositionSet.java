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
package org.apache.iceberg.snaprewrite;

import java.util.BitSet;
import java.util.function.LongConsumer;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A set of row positions within a single data file.
 *
 * <p>Backed by a {@link BitSet}, which bounds positions at {@link Integer#MAX_VALUE}. That is well
 * above any realistic per-file record count, and the planner checks it rather than truncating.
 *
 * <p>Iceberg's own bitmap implementations ({@code RoaringPositionBitmap}, {@code
 * BitmapPositionDeleteIndex}) are package-private, and this needs set difference, which {@link
 * PositionDeleteIndex} does not expose.
 */
public class PositionSet {
  private final BitSet bits;

  public PositionSet() {
    this.bits = new BitSet();
  }

  private PositionSet(BitSet bits) {
    this.bits = bits;
  }

  /** Returns the set of positions in {@code [0, recordCount)} that are not deleted. */
  public static PositionSet live(long recordCount, PositionDeleteIndex deletes) {
    PositionSet set = new PositionSet();
    for (long pos = 0; pos < recordCount; pos += 1) {
      if (deletes == null || !deletes.isDeleted(pos)) {
        set.add(pos);
      }
    }

    return set;
  }

  /** Returns the positions marked deleted by an index, restricted to {@code [0, recordCount)}. */
  public static PositionSet deleted(long recordCount, PositionDeleteIndex deletes) {
    PositionSet set = new PositionSet();
    if (deletes != null) {
      for (long pos = 0; pos < recordCount; pos += 1) {
        if (deletes.isDeleted(pos)) {
          set.add(pos);
        }
      }
    }

    return set;
  }

  public void add(long position) {
    Preconditions.checkArgument(
        position >= 0 && position < Integer.MAX_VALUE, "Position out of range: %s", position);
    bits.set((int) position);
  }

  public boolean contains(long position) {
    return position >= 0 && position < Integer.MAX_VALUE && bits.get((int) position);
  }

  public void addAll(PositionSet other) {
    bits.or(other.bits);
  }

  /** Returns a new set holding the positions in this set that are not in {@code other}. */
  public PositionSet minus(PositionSet other) {
    BitSet result = (BitSet) bits.clone();
    result.andNot(other.bits);
    return new PositionSet(result);
  }

  public boolean isEmpty() {
    return bits.isEmpty();
  }

  public int size() {
    return bits.cardinality();
  }

  /** Applies {@code consumer} to each position in ascending order. */
  public void forEach(LongConsumer consumer) {
    for (int pos = bits.nextSetBit(0); pos >= 0; pos = bits.nextSetBit(pos + 1)) {
      consumer.accept(pos);
    }
  }

  public PositionSet copy() {
    return new PositionSet((BitSet) bits.clone());
  }

  @Override
  public String toString() {
    return bits.toString();
  }
}
