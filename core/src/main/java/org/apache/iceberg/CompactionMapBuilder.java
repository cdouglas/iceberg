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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CompactionMap.FileMapping;
import org.apache.iceberg.CompactionMap.Run;
import org.apache.iceberg.GenericCompactionMap.GenericFileMapping;
import org.apache.iceberg.GenericCompactionMap.GenericRun;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Builder for constructing {@link CompactionMap} instances during compaction operations.
 *
 * <p>This builder tracks position transformations as rows are read from source files and written to
 * target files. It automatically merges consecutive position mappings into efficient run-length
 * encoded runs.
 *
 * <p>Example usage:
 *
 * <pre>
 * CompactionMapBuilder builder = new CompactionMapBuilder(sourceSnapshotId, targetSnapshotId);
 * FileMappingBuilder fileMapping = builder.addFileMapping(sourceFile, targetFile);
 * fileMapping.addRun(0, 0, 100);  // Rows 0-99 from source -> 0-99 in target
 * fileMapping.addRun(100, 200, 50);  // Rows 100-149 from source -> 200-249 in target
 * CompactionMap map = builder.build();
 * </pre>
 */
public class CompactionMapBuilder {
  private final long sourceSnapshotId;
  private final long targetSnapshotId;
  private final Map<String, FileMappingBuilder> fileMappings;

  public CompactionMapBuilder(long sourceSnapshotId, long targetSnapshotId) {
    this.sourceSnapshotId = sourceSnapshotId;
    this.targetSnapshotId = targetSnapshotId;
    this.fileMappings = new LinkedHashMap<>();
  }

  /**
   * Starts tracking a mapping from a source file to a target file.
   *
   * @param sourceFile the source file path (pre-compaction)
   * @param targetFile the target file path (post-compaction)
   * @return a builder for adding position runs to this file mapping
   */
  public FileMappingBuilder addFileMapping(String sourceFile, String targetFile) {
    Preconditions.checkNotNull(sourceFile, "Source file cannot be null");
    Preconditions.checkNotNull(targetFile, "Target file cannot be null");
    Preconditions.checkArgument(
        !fileMappings.containsKey(sourceFile),
        "File mapping for source file %s already exists",
        sourceFile);

    FileMappingBuilder builder = new FileMappingBuilder(sourceFile, targetFile);
    fileMappings.put(sourceFile, builder);
    return builder;
  }

  /**
   * Returns the file mapping builder for the given source file, or null if no mapping exists.
   *
   * @param sourceFile the source file path
   * @return the file mapping builder, or null
   */
  FileMappingBuilder getFileMapping(String sourceFile) {
    return fileMappings.get(sourceFile);
  }

  /**
   * Builds the final {@link CompactionMap}.
   *
   * @return the constructed compaction map
   */
  public CompactionMap build() {
    List<FileMapping> mappings = new ArrayList<>(fileMappings.size());
    for (FileMappingBuilder builder : fileMappings.values()) {
      mappings.add(builder.build());
    }
    return new GenericCompactionMap(sourceSnapshotId, targetSnapshotId, mappings);
  }

  /**
   * Builder for a single file mapping, tracking position runs from source to target.
   *
   * <p>This builder automatically merges consecutive runs for efficiency. For example, adding
   * Run(0, 0, 100) followed by Run(100, 100, 50) will be merged into a single Run(0, 0, 150).
   */
  public static class FileMappingBuilder {
    private final String sourceFile;
    private final String targetFile;
    private final List<RunBuilder> runs;

    FileMappingBuilder(String sourceFile, String targetFile) {
      this.sourceFile = sourceFile;
      this.targetFile = targetFile;
      this.runs = new ArrayList<>();
    }

    /**
     * Adds a run of position mappings from source to target.
     *
     * <p>If this run is consecutive with the previous run, they will be automatically merged.
     *
     * @param sourcePosition starting position in the source file
     * @param targetPosition starting position in the target file
     * @param length number of rows in this run
     * @return this builder for method chaining
     */
    public FileMappingBuilder addRun(long sourcePosition, long targetPosition, long length) {
      Preconditions.checkArgument(sourcePosition >= 0, "Source position must be non-negative");
      Preconditions.checkArgument(targetPosition >= 0, "Target position must be non-negative");
      Preconditions.checkArgument(length > 0, "Run length must be positive");

      // Try to merge with the last run
      if (!runs.isEmpty()) {
        RunBuilder lastRun = runs.get(runs.size() - 1);
        if (lastRun.canMerge(sourcePosition, targetPosition)) {
          lastRun.extend(length);
          return this;
        }
      }

      // Cannot merge, add as new run
      runs.add(new RunBuilder(sourcePosition, targetPosition, length));
      return this;
    }

    FileMapping build() {
      List<Run> builtRuns = new ArrayList<>(runs.size());
      for (RunBuilder runBuilder : runs) {
        builtRuns.add(runBuilder.build());
      }
      return new GenericFileMapping(sourceFile, targetFile, builtRuns);
    }
  }

  /** Internal builder for a single run, supporting automatic merging of consecutive runs. */
  private static class RunBuilder {
    private final long sourcePosition;
    private final long targetPosition;
    private long length;

    RunBuilder(long sourcePosition, long targetPosition, long length) {
      this.sourcePosition = sourcePosition;
      this.targetPosition = targetPosition;
      this.length = length;
    }

    /**
     * Checks if a new run can be merged with this run.
     *
     * <p>Two runs can be merged if they are consecutive in both source and target files.
     */
    boolean canMerge(long nextSourcePosition, long nextTargetPosition) {
      return nextSourcePosition == sourcePosition + length
          && nextTargetPosition == targetPosition + length;
    }

    /** Extends this run by the given length. */
    void extend(long additionalLength) {
      this.length += additionalLength;
    }

    Run build() {
      return new GenericRun(sourcePosition, targetPosition, length);
    }
  }
}
