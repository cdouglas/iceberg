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

/**
 * Linear search strategy for finding runs containing source positions.
 *
 * <p>Iterates through all runs sequentially until finding one that contains the position.
 *
 * <p><strong>Complexity:</strong>
 *
 * <ul>
 *   <li>Lookup: O(m) where m = number of runs
 *   <li>Setup: O(1)
 *   <li>Memory: O(1) additional
 * </ul>
 *
 * <p><strong>Best for:</strong> Very small run counts (m &lt; 10) where setup cost of more complex
 * structures isn't justified.
 */
class LinearSearchStrategy implements RemappingStrategy {
  private final List<Run> runs;

  LinearSearchStrategy(List<Run> runs) {
    this.runs = runs;
  }

  @Override
  public Run runForPosition(long sourcePosition) {
    if (runs != null) {
      for (Run run : runs) {
        long runStart = run.sourcePosition();
        long runEnd = runStart + run.length();

        if (sourcePosition >= runStart && sourcePosition < runEnd) {
          return run;
        }
      }
    }
    return null;
  }

  @Override
  public String name() {
    return "linear-search";
  }
}
