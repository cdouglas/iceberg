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
package org.apache.iceberg.exceptions;

import java.util.Map;
import java.util.Set;

/**
 * Exception thrown when a transaction conflicts with a compaction operation.
 *
 * <p>This exception is raised when an operation (such as adding position deletes) references data
 * files that have been compacted in a concurrent transaction. The exception provides detailed
 * information about which files were compacted and where their compaction maps are located,
 * enabling automatic remapping of position deletes to resolve the conflict.
 *
 * <p>Example scenario:
 *
 * <pre>
 * 1. Transaction T1 starts and reads snapshot S1
 * 2. Transaction T1 generates position deletes for files [A, B, C]
 * 3. Concurrent transaction T2 compacts files [A, B, C] into [D] and commits
 * 4. Transaction T1 tries to commit and fails with CompactionConflictException
 * 5. T1 can use the compaction map locations to remap deletes to file D and retry
 * </pre>
 */
public class CompactionConflictException extends ValidationException {
  private final Set<String> compactedFiles;
  private final Map<String, String> compactionMapLocations;

  /**
   * Creates a new CompactionConflictException.
   *
   * @param message the error message describing the conflict
   * @param compactedFiles the set of data file paths that were compacted
   * @param compactionMapLocations map from compacted file paths to compaction map locations
   */
  public CompactionConflictException(
      String message, Set<String> compactedFiles, Map<String, String> compactionMapLocations) {
    super("%s", message);
    this.compactedFiles = compactedFiles;
    this.compactionMapLocations = compactionMapLocations;
  }

  /**
   * Returns the set of data file paths that were compacted.
   *
   * <p>These are the files that the failing transaction referenced but which no longer exist
   * because they were replaced by a compaction operation.
   *
   * @return set of compacted file paths
   */
  public Set<String> compactedFiles() {
    return compactedFiles;
  }

  /**
   * Returns a map from compacted file paths to their compaction map locations.
   *
   * <p>The compaction map locations can be used to load the compaction maps and remap position
   * deletes from the compacted files to their replacement files.
   *
   * <p>Note: Multiple compacted files may share the same compaction map location if they were all
   * part of the same compaction operation.
   *
   * @return map from file path to compaction map location
   */
  public Map<String, String> compactionMapLocations() {
    return compactionMapLocations;
  }
}
