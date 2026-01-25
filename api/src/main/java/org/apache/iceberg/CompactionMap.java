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
import java.util.Locale;
import org.apache.iceberg.types.Types;

/**
 * A compaction map describes how position references in data files are transformed during a
 * compaction operation.
 *
 * <p>This enables concurrent transactions to remap their position deletes when the data layout
 * changes due to compaction.
 *
 * <p>Compaction maps are stored in Avro format and follow the same serialization patterns as
 * ManifestFile.
 */
public interface CompactionMap {

  // Schema field IDs
  int SOURCE_SNAPSHOT_ID_FIELD_ID = 1;
  int TARGET_SNAPSHOT_ID_FIELD_ID = 2;
  int FILE_MAPPINGS_FIELD_ID = 3;
  int FILE_MAPPINGS_ELEMENT_ID = 4;

  // Top-level fields
  Types.NestedField SOURCE_SNAPSHOT_ID =
      Types.NestedField.required(
          SOURCE_SNAPSHOT_ID_FIELD_ID,
          "source_snapshot_id",
          Types.LongType.get(),
          "Snapshot ID before compaction");

  Types.NestedField TARGET_SNAPSHOT_ID =
      Types.NestedField.required(
          TARGET_SNAPSHOT_ID_FIELD_ID,
          "target_snapshot_id",
          Types.LongType.get(),
          "Snapshot ID after compaction");

  Types.NestedField FILE_MAPPINGS =
      Types.NestedField.required(
          FILE_MAPPINGS_FIELD_ID,
          "file_mappings",
          Types.ListType.ofRequired(FILE_MAPPINGS_ELEMENT_ID, fileMappingType()),
          "List of file mappings in this compaction");

  // FileMapping struct type
  static Types.StructType fileMappingType() {
    return Types.StructType.of(
        Types.NestedField.required(
            5, "source_file", Types.StringType.get(), "Source file path (pre-compaction)"),
        Types.NestedField.required(
            6, "target_file", Types.StringType.get(), "Target file path (post-compaction)"),
        Types.NestedField.required(
            7, "runs", Types.ListType.ofRequired(8, runType()), "List of position mapping runs"));
  }

  // Run struct type
  static Types.StructType runType() {
    return Types.StructType.of(
        Types.NestedField.required(
            9, "source_position", Types.LongType.get(), "Starting position in source file"),
        Types.NestedField.required(
            10, "target_position", Types.LongType.get(), "Starting position in target file"),
        Types.NestedField.required(
            11, "length", Types.LongType.get(), "Number of rows in this run"),
        Types.NestedField.optional(
            12,
            "target_file",
            Types.StringType.get(),
            "Target file for this run (multi-target support). If null, uses parent FileMapping.targetFile"));
  }

  // Schema for the compaction map file
  Schema SCHEMA = new Schema(SOURCE_SNAPSHOT_ID, TARGET_SNAPSHOT_ID, FILE_MAPPINGS);

  static Schema schema() {
    return SCHEMA;
  }

  /** Returns the snapshot ID before compaction. */
  long sourceSnapshotId();

  /** Returns the snapshot ID after compaction. */
  long targetSnapshotId();

  /** Returns the list of file mappings in this compaction. */
  List<FileMapping> fileMappings();

  /**
   * Returns the mapping for a specific source file path, or null if not found.
   *
   * @param sourceFilePath the source file path to look up
   * @return the file mapping, or null if not found
   */
  FileMapping mappingForFile(String sourceFilePath);

  /**
   * Copies this {@link CompactionMap}. Readers can reuse instances; use this method to make
   * defensive copies.
   *
   * @return a copy of this compaction map
   */
  CompactionMap copy();

  /** Represents the mapping for a single data file that was compacted. */
  interface FileMapping {
    /** Returns the source file path (pre-compaction). */
    String sourceFile();

    /** Returns the target file path (post-compaction). */
    String targetFile();

    /** Returns the list of position mapping runs. */
    List<Run> runs();

    /**
     * Returns the Run containing the given source position, or null if not found.
     *
     * @param sourcePosition the position in the source file
     * @return the run containing this position, or null
     */
    Run runForPosition(long sourcePosition);

    /** Copies this {@link FileMapping}. */
    FileMapping copy();
  }

  /**
   * Represents a contiguous run of rows mapped from source to target.
   *
   * <p>A run describes that rows at positions [sourcePosition, sourcePosition + length) in the
   * source file are mapped to [targetPosition, targetPosition + length) in a target file.
   *
   * <p>The target file can be specified per-run (via {@link #targetFile()}) to support multi-target
   * mappings, or null to use the parent FileMapping's targetFile.
   */
  interface Run {
    /** Returns the starting position in source file. */
    long sourcePosition();

    /** Returns the starting position in target file. */
    long targetPosition();

    /** Returns the number of rows in this run. */
    long length();

    /**
     * Returns the target file for this run, or null to use the parent FileMapping's targetFile.
     *
     * <p>This enables multi-target mappings where a source file's rows span multiple target files
     * (e.g., due to target file size limits).
     */
    default String targetFile() {
      return null;
    }

    /**
     * Given a position in the source file (must be within this run), returns the corresponding
     * position in the target file.
     *
     * @param sourcePos the position in the source file
     * @return the corresponding position in the target file
     */
    default long mapPosition(long sourcePos) {
      if (sourcePos < sourcePosition() || sourcePos >= sourcePosition() + length()) {
        throw new IllegalArgumentException(
            String.format(
                Locale.ROOT,
                "Position %d is not within run [%d, %d)",
                sourcePos,
                sourcePosition(),
                sourcePosition() + length()));
      }
      return targetPosition() + (sourcePos - sourcePosition());
    }

    /** Copies this {@link Run}. */
    Run copy();
  }
}
