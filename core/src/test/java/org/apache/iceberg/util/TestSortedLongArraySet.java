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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class TestSortedLongArraySet {

  @Test
  public void testEmpty() {
    SortedLongArraySet set = new SortedLongArraySet(new long[0]);
    assertThat(set).isEmpty();
    assertThat(set.size()).isEqualTo(0);
    assertThat(set.contains(0L)).isFalse();
    assertThat(set.iterator().hasNext()).isFalse();
  }

  @Test
  public void testSingleElement() {
    SortedLongArraySet set = new SortedLongArraySet(new long[] {42L});
    assertThat(set).hasSize(1);
    assertThat(set.contains(42L)).isTrue();
    assertThat(set.contains(41L)).isFalse();
    assertThat(set.contains(43L)).isFalse();

    Iterator<Long> iter = set.iterator();
    assertThat(iter.hasNext()).isTrue();
    assertThat(iter.next()).isEqualTo(42L);
    assertThat(iter.hasNext()).isFalse();
  }

  @Test
  public void testMultipleElements() {
    SortedLongArraySet set = new SortedLongArraySet(new long[] {1L, 5L, 10L, 20L, 100L});
    assertThat(set).hasSize(5);

    // Test contains
    assertThat(set.contains(1L)).isTrue();
    assertThat(set.contains(5L)).isTrue();
    assertThat(set.contains(10L)).isTrue();
    assertThat(set.contains(20L)).isTrue();
    assertThat(set.contains(100L)).isTrue();

    // Test not contains
    assertThat(set.contains(0L)).isFalse();
    assertThat(set.contains(2L)).isFalse();
    assertThat(set.contains(50L)).isFalse();
    assertThat(set.contains(101L)).isFalse();
  }

  @Test
  public void testIteration() {
    long[] values = {1L, 5L, 10L, 20L, 100L};
    SortedLongArraySet set = new SortedLongArraySet(values);

    List<Long> collected = new ArrayList<>();
    for (Long value : set) {
      collected.add(value);
    }

    assertThat(collected).containsExactly(1L, 5L, 10L, 20L, 100L);
  }

  @Test
  public void testIteratorThrowsOnExhausted() {
    SortedLongArraySet set = new SortedLongArraySet(new long[] {1L});
    Iterator<Long> iter = set.iterator();
    iter.next();

    org.junit.jupiter.api.Assertions.assertThrows(NoSuchElementException.class, iter::next);
  }

  @Test
  public void testFromUnsorted() {
    long[] unsorted = {100L, 1L, 50L, 10L, 5L};
    SortedLongArraySet set = SortedLongArraySet.fromUnsorted(unsorted);

    List<Long> collected = new ArrayList<>();
    for (Long value : set) {
      collected.add(value);
    }

    // Should be sorted after construction
    assertThat(collected).containsExactly(1L, 5L, 10L, 50L, 100L);
  }

  @Test
  public void testToArray() {
    SortedLongArraySet set = new SortedLongArraySet(new long[] {1L, 5L, 10L});

    Object[] array = set.toArray();
    assertThat(array).hasSize(3);
    assertThat(array[0]).isEqualTo(1L);
    assertThat(array[1]).isEqualTo(5L);
    assertThat(array[2]).isEqualTo(10L);
  }

  @Test
  public void testToLongArray() {
    long[] original = {1L, 5L, 10L};
    SortedLongArraySet set = new SortedLongArraySet(original);

    // Should return same array instance
    assertThat(set.toLongArray()).isSameAs(original);
  }

  @Test
  public void testContainsNonLong() {
    SortedLongArraySet set = new SortedLongArraySet(new long[] {1L, 5L, 10L});
    assertThat(set.contains("not a long")).isFalse();
    assertThat(set.contains(null)).isFalse();
  }

  @Test
  public void testEqualsWithHashSet() {
    long[] values = {1L, 5L, 10L, 20L, 100L};
    SortedLongArraySet arraySet = new SortedLongArraySet(values);

    Set<Long> hashSet = new HashSet<>();
    for (long v : values) {
      hashSet.add(v);
    }

    // AbstractSet.equals should work correctly
    assertThat(arraySet).isEqualTo(hashSet);
    assertThat(hashSet).isEqualTo(arraySet);
  }

  @Test
  public void testLargeSet() {
    // Test with 100K elements
    int size = 100_000;
    long[] values = new long[size];
    for (int i = 0; i < size; i++) {
      values[i] = i * 2L; // Even numbers 0, 2, 4, ...
    }

    SortedLongArraySet set = new SortedLongArraySet(values);
    assertThat(set).hasSize(size);

    // Test contains for various values
    assertThat(set.contains(0L)).isTrue();
    assertThat(set.contains(100L)).isTrue();
    assertThat(set.contains(99998L)).isTrue();
    assertThat(set.contains((size - 1) * 2L)).isTrue();

    // Test not contains
    assertThat(set.contains(1L)).isFalse();
    assertThat(set.contains(99L)).isFalse();
    assertThat(set.contains(-1L)).isFalse();
    assertThat(set.contains(size * 2L)).isFalse();
  }
}
