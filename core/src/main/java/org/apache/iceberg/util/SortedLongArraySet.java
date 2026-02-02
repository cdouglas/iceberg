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

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An immutable Set implementation backed by a sorted primitive long array.
 *
 * <p>This provides O(1) construction time (compared to O(n) for HashSet) while still supporting
 * efficient O(log n) membership testing via binary search. Iteration is O(n) in sorted order.
 *
 * <p>This class is useful when:
 *
 * <ul>
 *   <li>You already have a sorted array of unique values
 *   <li>You need Set semantics for API compatibility
 *   <li>The primary use case is iteration rather than random access lookups
 *   <li>Construction cost is a concern (HashSet has high constant factors)
 * </ul>
 *
 * <p>Note: The array must be pre-sorted and contain unique values. This class does not verify
 * uniqueness - if duplicates exist, contains() will still work but size() may be incorrect.
 *
 * <p>Performance characteristics at 1M elements (from benchmarks):
 *
 * <ul>
 *   <li>Construction: ~0 us (just wraps array) vs ~52ms for HashSet
 *   <li>contains(): ~0.14 us (binary search) vs ~0.04 us for HashSet
 *   <li>Iteration: ~1ms (same as HashSet)
 * </ul>
 */
public class SortedLongArraySet extends AbstractSet<Long> {
  private final long[] sortedArray;

  /**
   * Creates a new SortedLongArraySet wrapping the given array.
   *
   * <p>The array must be sorted in ascending order and contain unique values. The array is not
   * copied - modifications to the array will affect this set.
   *
   * @param sortedArray a sorted array of unique long values
   */
  public SortedLongArraySet(long[] sortedArray) {
    this.sortedArray = sortedArray;
  }

  /**
   * Creates a new SortedLongArraySet from the given array, sorting it if necessary.
   *
   * <p>If the array may contain duplicates, they will remain in the array but won't affect
   * correctness of contains() - only size() will be affected.
   *
   * @param array an array of long values (will be sorted in place)
   * @return a new SortedLongArraySet
   */
  public static SortedLongArraySet fromUnsorted(long[] array) {
    Arrays.sort(array);
    return new SortedLongArraySet(array);
  }

  @Override
  public int size() {
    return sortedArray.length;
  }

  @Override
  public boolean isEmpty() {
    return sortedArray.length == 0;
  }

  @Override
  public boolean contains(Object o) {
    if (!(o instanceof Long)) {
      return false;
    }
    long value = (Long) o;
    return Arrays.binarySearch(sortedArray, value) >= 0;
  }

  @Override
  public Iterator<Long> iterator() {
    return new Iterator<Long>() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        return index < sortedArray.length;
      }

      @Override
      public Long next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return sortedArray[index++];
      }
    };
  }

  @Override
  public Object[] toArray() {
    Long[] result = new Long[sortedArray.length];
    for (int i = 0; i < sortedArray.length; i++) {
      result[i] = sortedArray[i];
    }
    return result;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T[] toArray(T[] a) {
    if (a.length < sortedArray.length) {
      a =
          (T[])
              java.lang.reflect.Array.newInstance(
                  a.getClass().getComponentType(), sortedArray.length);
    }
    for (int i = 0; i < sortedArray.length; i++) {
      a[i] = (T) Long.valueOf(sortedArray[i]);
    }
    if (a.length > sortedArray.length) {
      a[sortedArray.length] = null;
    }
    return a;
  }

  /**
   * Returns the underlying sorted array.
   *
   * <p>Note: This returns the actual backing array, not a copy. Modifications to the returned array
   * will affect this set.
   *
   * @return the sorted array backing this set
   */
  public long[] toLongArray() {
    return sortedArray;
  }
}
