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

import java.util.List;
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Interval tree strategy for finding runs containing source positions.
 *
 * <p>Uses a balanced binary search tree where each node represents a run (interval). The tree is
 * augmented with the maximum end position in each subtree for efficient pruning during search.
 *
 * <p><strong>Complexity:</strong>
 *
 * <ul>
 *   <li>Lookup: O(log m) where m = number of runs
 *   <li>Setup: O(m) - builds balanced tree from sorted runs
 *   <li>Memory: O(m) additional for tree nodes
 * </ul>
 *
 * <p><strong>Best for:</strong> Large run counts (m &gt;= 100) where the setup cost is amortized
 * across many lookups. Provides better cache locality than array-based binary search for very large
 * m.
 *
 * <p><strong>Requirements:</strong> Runs must be sorted by sourcePosition (ascending) and
 * non-overlapping. This is validated during construction.
 *
 * <p><strong>Performance:</strong> Similar to binary search for most cases, but provides better
 * worst-case guarantees and is more extensible for future optimizations (e.g., range queries, bulk
 * lookups).
 */
class IntervalTreeStrategy implements RemappingStrategy {
  private final Node root;

  /**
   * Creates an interval tree strategy.
   *
   * @param runs list of runs sorted by sourcePosition (ascending)
   * @throws IllegalArgumentException if runs are not sorted or overlap
   */
  IntervalTreeStrategy(List<Run> runs) {
    validateSorted(runs);
    this.root = buildTree(runs, 0, runs.size() - 1);
  }

  @Override
  public Run runForPosition(long sourcePosition) {
    if (root == null) {
      return null;
    }
    return search(root, sourcePosition);
  }

  @Override
  public String name() {
    return "interval-tree";
  }

  /**
   * Searches the tree for a run containing the given position.
   *
   * @param node current node
   * @param position position to find
   * @return run containing position, or null if not found
   */
  private Run search(Node node, long position) {
    if (node == null) {
      return null;
    }

    long runStart = node.run.sourcePosition();
    long runEnd = runStart + node.run.length();

    // Check if position is in current node's run
    if (position >= runStart && position < runEnd) {
      return node.run;
    }

    // Prune using maxEnd: if position is beyond maxEnd, it can't be in this subtree
    if (position >= node.maxEnd) {
      return null;
    }

    // Search left subtree if position is before current run
    if (position < runStart) {
      return search(node.left, position);
    }

    // Search right subtree if position is after current run
    return search(node.right, position);
  }

  /**
   * Builds a balanced binary search tree from sorted runs.
   *
   * <p>Uses the middle element as root to ensure O(log m) height. Recursively builds left and right
   * subtrees.
   *
   * @param runs sorted list of runs
   * @param start start index (inclusive)
   * @param end end index (inclusive)
   * @return root node of subtree, or null if range is empty
   */
  private Node buildTree(List<Run> runs, int start, int end) {
    if (runs == null || runs.isEmpty() || start > end) {
      return null;
    }

    // Take middle element as root for balanced tree
    int mid = start + (end - start) / 2;
    Run run = runs.get(mid);

    Node node = new Node(run);

    // Recursively build left and right subtrees
    node.left = buildTree(runs, start, mid - 1);
    node.right = buildTree(runs, mid + 1, end);

    // Calculate maxEnd for this subtree
    node.maxEnd = run.sourcePosition() + run.length();
    if (node.left != null) {
      node.maxEnd = Math.max(node.maxEnd, node.left.maxEnd);
    }
    if (node.right != null) {
      node.maxEnd = Math.max(node.maxEnd, node.right.maxEnd);
    }

    return node;
  }

  /**
   * Validates that runs are sorted by sourcePosition and non-overlapping.
   *
   * @param runs runs to validate
   * @throws IllegalArgumentException if runs are not sorted or have overlaps
   */
  private static void validateSorted(List<Run> runs) {
    if (runs == null || runs.size() <= 1) {
      return;
    }

    long prevEnd = runs.get(0).sourcePosition();

    for (int i = 0; i < runs.size(); i++) {
      Run run = runs.get(i);
      long start = run.sourcePosition();
      long end = start + run.length();

      // Check sorting
      Preconditions.checkArgument(
          start >= prevEnd,
          "Runs must be sorted by sourcePosition and non-overlapping. "
              + "Run at index %s has sourcePosition %s but previous run ends at %s",
          i,
          start,
          prevEnd);

      // Check for valid length
      Preconditions.checkArgument(
          run.length() > 0, "Run at index %s has invalid length: %s", i, run.length());

      prevEnd = end;
    }
  }

  /**
   * Node in the interval tree.
   *
   * <p>Stores a run (interval) and maintains the maximum end position in the subtree rooted at this
   * node. This enables efficient pruning during search.
   */
  private static class Node {
    final Run run;
    long maxEnd; // Maximum end position in this subtree
    Node left;
    Node right;

    Node(Run run) {
      this.run = run;
      this.maxEnd = run.sourcePosition() + run.length();
    }
  }
}
