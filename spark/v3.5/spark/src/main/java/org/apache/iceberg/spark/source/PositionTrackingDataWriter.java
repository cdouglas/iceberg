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
package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.PositionMappingCoordinator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper for DataWriter that tracks position mappings during compaction rewrites.
 *
 * <p>This writer extracts source file and position metadata from each row (_file and _pos columns)
 * and records the mapping to target file positions in the PositionMappingCoordinator. These
 * mappings are used to generate accurate compaction maps with run-based position tracking.
 *
 * <p>Key responsibilities:
 *
 * <ul>
 *   <li>Extract _file (source file path) and _pos (INPUT position in source) from each row
 *   <li>Track OUTPUT position (counter incremented for each row written)
 *   <li>Record (sourceFile, inputPos, targetFile, outputPos) to PositionMappingCoordinator
 *   <li>Project away metadata columns before delegating to wrapped writer
 * </ul>
 */
class PositionTrackingDataWriter implements DataWriter<InternalRow> {
  private static final Logger LOG = LoggerFactory.getLogger(PositionTrackingDataWriter.class);

  private final DataWriter<InternalRow> delegate;
  private final PositionMappingCoordinator coordinator;
  private final Table table;
  private final String fileSetId;
  private final int fileOrdinal;
  private final int posOrdinal;
  private final int numDataColumns;
  private final StructType dsSchema;

  private long outputPosition = 0;

  // Buffer position mappings until commit when we know the actual target file paths
  private final List<BufferedMapping> bufferedMappings = new ArrayList<>();

  /** Represents a position mapping waiting for the actual target file path. */
  private static class BufferedMapping {
    final String sourceFile;
    final long sourcePos;
    final long targetPos;

    BufferedMapping(String sourceFile, long sourcePos, long targetPos) {
      this.sourceFile = sourceFile;
      this.sourcePos = sourcePos;
      this.targetPos = targetPos;
    }
  }

  /**
   * Creates a position tracking wrapper around a delegate writer.
   *
   * @param delegate the wrapped DataWriter
   * @param table the table being written to
   * @param fileSetId unique identifier for this rewrite operation
   * @param dsSchema the DataFrame schema including metadata columns
   */
  PositionTrackingDataWriter(
      DataWriter<InternalRow> delegate, Table table, String fileSetId, StructType dsSchema) {
    this.delegate = delegate;
    this.coordinator = PositionMappingCoordinator.get();
    this.table = table;
    this.fileSetId = fileSetId;
    this.dsSchema = dsSchema;

    // Find ordinals of metadata columns _file and _pos
    this.fileOrdinal = dsSchema.fieldIndex("_file");
    this.posOrdinal = dsSchema.fieldIndex("_pos");

    // Calculate number of data columns (excluding metadata)
    this.numDataColumns = dsSchema.size() - 2; // exclude _file and _pos

    LOG.debug(
        "Created PositionTrackingDataWriter for fileSetId={}, fileOrdinal={}, posOrdinal={}",
        fileSetId,
        fileOrdinal,
        posOrdinal);
  }

  @Override
  public void write(InternalRow row) throws IOException {
    // Extract source metadata
    String sourceFile = row.getUTF8String(fileOrdinal).toString();
    long sourcePos = row.getLong(posOrdinal);

    // Write the full row through delegate
    // The delegate writer's schema determines which columns are actually written
    // The metadata columns (_file, _pos) are not in the writer's schema so they'll be ignored
    delegate.write(row);

    // Buffer the position mapping - we'll record it to the coordinator after commit
    // when we know the actual target file path from the WriterCommitMessage
    bufferedMappings.add(new BufferedMapping(sourceFile, sourcePos, outputPosition));

    outputPosition++;
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    WriterCommitMessage message = delegate.commit();

    // Extract actual target file paths from commit message and record buffered mappings
    // File paths are only known after commit completes
    recordBufferedMappingsWithActualPaths(message);

    // Reset for next file (if writer is reused)
    outputPosition = 0;
    bufferedMappings.clear();

    return message;
  }

  @Override
  public void abort() throws IOException {
    delegate.abort();
    // No need to clear coordinator mappings on abort - they'll be cleared on cleanup
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  /**
   * Records buffered position mappings with actual target file paths from commit message.
   *
   * <p>The WriterCommitMessage (TaskCommit) contains the DataFile objects with actual file paths
   * that were written. When a single writer task produces multiple output files (due to rollover),
   * each buffered mapping is assigned to the correct output file based on its target position and
   * the cumulative record counts of the output files.
   *
   * @param message the commit message from the delegate writer
   */
  private void recordBufferedMappingsWithActualPaths(WriterCommitMessage message) {
    if (bufferedMappings.isEmpty()) {
      LOG.debug("No buffered position mappings to record for fileSetId={}", fileSetId);
      return;
    }

    // Extract actual target file paths from TaskCommit
    if (!(message instanceof SparkWrite.TaskCommit)) {
      LOG.warn(
          "WriterCommitMessage is not a TaskCommit (got {}), cannot extract target file paths. "
              + "Position mappings will not be recorded.",
          message.getClass().getName());
      return;
    }

    SparkWrite.TaskCommit taskCommit = (SparkWrite.TaskCommit) message;
    DataFile[] files = taskCommit.files();

    if (files.length == 0) {
      LOG.warn(
          "TaskCommit has no files, cannot record position mappings for fileSetId={}", fileSetId);
      return;
    }

    if (files.length == 1) {
      // Single output file — assign all mappings directly (common bin-pack case)
      String targetFile = files[0].location();
      LOG.debug(
          "Recording {} buffered position mappings with single target file {} for fileSetId={}",
          bufferedMappings.size(),
          targetFile,
          fileSetId);

      for (BufferedMapping mapping : bufferedMappings) {
        coordinator.recordMapping(
            table, fileSetId, mapping.sourceFile, mapping.sourcePos, targetFile, mapping.targetPos);
      }
    } else {
      // Multiple output files — assign each mapping to the correct file by position range.
      // Build cumulative record-count boundaries: [0, count0, count0+count1, ...]
      long[] boundaries = new long[files.length + 1];
      boundaries[0] = 0;
      for (int i = 0; i < files.length; i++) {
        boundaries[i + 1] = boundaries[i] + files[i].recordCount();
      }

      LOG.debug(
          "Recording {} buffered position mappings across {} target files for fileSetId={}",
          bufferedMappings.size(),
          files.length,
          fileSetId);

      for (BufferedMapping mapping : bufferedMappings) {
        int fileIdx = findTargetFileIndex(mapping.targetPos, boundaries);
        String targetFile = files[fileIdx].location();
        long adjustedTargetPos = mapping.targetPos - boundaries[fileIdx];
        coordinator.recordMapping(
            table,
            fileSetId,
            mapping.sourceFile,
            mapping.sourcePos,
            targetFile,
            adjustedTargetPos);
      }
    }

    LOG.info(
        "Successfully recorded {} position mappings for fileSetId={}, targets={}",
        bufferedMappings.size(),
        fileSetId,
        files.length);
  }

  /**
   * Finds the index of the output file that contains the given target position using binary search
   * on cumulative record-count boundaries.
   *
   * @param targetPos the absolute target position across all output files
   * @param boundaries cumulative boundaries: [0, count0, count0+count1, ..., total]
   * @return the index into the files array
   */
  private static int findTargetFileIndex(long targetPos, long[] boundaries) {
    // Binary search: find the largest boundary <= targetPos
    int lo = 0;
    int hi = boundaries.length - 2; // max valid file index
    while (lo < hi) {
      int mid = lo + (hi - lo + 1) / 2;
      if (boundaries[mid] <= targetPos) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }
    return lo;
  }
}
