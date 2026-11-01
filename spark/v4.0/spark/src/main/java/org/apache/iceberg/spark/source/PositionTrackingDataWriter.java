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
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.PositionMappingCoordinator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper for DataWriter that tracks position mappings during compaction rewrites.
 *
 * <p>This writer extracts source file and position metadata from each row (_file and _pos columns)
 * and returns the mapping to target file positions with its commit message. The rewrite's commit,
 * on the driver, records the mappings of its committed tasks in the PositionMappingCoordinator.
 * These mappings are used to generate accurate compaction maps with run-based position tracking.
 *
 * <p>Key responsibilities:
 *
 * <ul>
 *   <li>Extract _file (source file path) and _pos (INPUT position in source) from each row
 *   <li>Track OUTPUT position (counter incremented for each row written)
 *   <li>Group consecutive rows into runs of (sourceFile, inputPos, outputPos, length)
 *   <li>On commit, split the runs between the files written and return them in a {@link
 *       TrackedTaskCommit}
 * </ul>
 *
 * <p>The delegate writes the leading columns of each row by ordinal, so _file and _pos, which come
 * last, are not written.
 */
class PositionTrackingDataWriter implements DataWriter<InternalRow> {
  private static final Logger LOG = LoggerFactory.getLogger(PositionTrackingDataWriter.class);

  private final DataWriter<InternalRow> delegate;
  private final String fileSetId;
  private final int fileOrdinal;
  private final int posOrdinal;

  private long outputPosition = 0;

  // Runs of rows written so far, in the order written. A run's target position counts rows from
  // the start of the task, since which file each row went to is known only after commit
  private final List<PendingRun> pendingRuns = new ArrayList<>();
  private PendingRun currentRun = null;

  /** Consecutive rows of one source file, waiting for the actual target file path. */
  private static class PendingRun {
    private final UTF8String sourceFile;
    private final long sourcePos;
    private final long targetPos;
    private long length = 1;

    PendingRun(UTF8String sourceFile, long sourcePos, long targetPos) {
      this.sourceFile = sourceFile;
      this.sourcePos = sourcePos;
      this.targetPos = targetPos;
    }

    boolean isContinuedBy(UTF8String file, long pos) {
      return pos == sourcePos + length && file.equals(sourceFile);
    }
  }

  /**
   * Creates a position tracking wrapper around a delegate writer.
   *
   * @param delegate the wrapped DataWriter
   * @param fileSetId unique identifier for this rewrite operation
   * @param dsSchema the DataFrame schema including metadata columns
   */
  PositionTrackingDataWriter(
      DataWriter<InternalRow> delegate, String fileSetId, StructType dsSchema) {
    this.delegate = delegate;
    this.fileSetId = fileSetId;

    // Find ordinals of metadata columns _file and _pos
    this.fileOrdinal = dsSchema.fieldIndex("_file");
    this.posOrdinal = dsSchema.fieldIndex("_pos");

    // The delegate writes the leading columns of each row by ordinal, so _file and _pos must come
    // last; otherwise it would write them as the columns they displace, such as row lineage
    Preconditions.checkArgument(
        fileOrdinal == dsSchema.size() - 2 && posOrdinal == dsSchema.size() - 1,
        "Cannot track source positions: _file and _pos must be the last columns of %s",
        dsSchema.simpleString());

    LOG.debug(
        "Created PositionTrackingDataWriter for fileSetId={}, fileOrdinal={}, posOrdinal={}",
        fileSetId,
        fileOrdinal,
        posOrdinal);
  }

  @Override
  public void write(InternalRow row) throws IOException {
    // Extract source metadata
    UTF8String sourceFile = row.getUTF8String(fileOrdinal);
    long sourcePos = row.getLong(posOrdinal);

    // Write the full row through delegate
    // The delegate writer's schema determines which columns are actually written
    // The metadata columns (_file, _pos) are not in the writer's schema so they'll be ignored
    delegate.write(row);

    if (currentRun == null) {
      // The row may reuse its buffer for the next row, so the run keeps a copy of the path
      currentRun = new PendingRun(sourceFile.copy(), sourcePos, outputPosition);
      pendingRuns.add(currentRun);
    } else if (currentRun.isContinuedBy(sourceFile, sourcePos)) {
      currentRun.length += 1;
    } else {
      // Runs of one source file, split by deleted rows, share a copy of its path
      UTF8String file =
          sourceFile.equals(currentRun.sourceFile) ? currentRun.sourceFile : sourceFile.copy();
      currentRun = new PendingRun(file, sourcePos, outputPosition);
      pendingRuns.add(currentRun);
    }

    outputPosition++;
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    WriterCommitMessage message = delegate.commit();

    try {
      // File paths are only known after commit completes
      return withSourcePositions(message);
    } finally {
      // Reset for next file (if writer is reused)
      outputPosition = 0;
      pendingRuns.clear();
      currentRun = null;
    }
  }

  @Override
  public void abort() throws IOException {
    delegate.abort();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  /**
   * Returns the delegate's commit message with the runs of source positions this task wrote.
   *
   * <p>The WriterCommitMessage (TaskCommit) contains the DataFile objects with actual file paths
   * that were written. When a single writer task produces multiple output files (due to rollover),
   * each run is assigned to the output files it spans, based on its target positions and the
   * cumulative record counts of the output files, which the writer wrote one after another.
   *
   * <p>A map that left out rows of a source file would read as if those rows were deleted, so a
   * task whose rows cannot all be placed fails rather than returning a partial mapping.
   *
   * @param message the commit message from the delegate writer
   */
  private WriterCommitMessage withSourcePositions(WriterCommitMessage message) {
    if (pendingRuns.isEmpty()) {
      LOG.debug("No position mappings to return for fileSetId={}", fileSetId);
      return message;
    }

    Preconditions.checkState(
        message instanceof SparkWrite.TaskCommit,
        "Cannot track source positions: expected a TaskCommit, got %s",
        message.getClass().getName());

    DataFile[] files = ((SparkWrite.TaskCommit) message).files();
    long recordCount = 0;
    for (DataFile file : files) {
      recordCount += file.recordCount();
    }

    Preconditions.checkState(
        recordCount == outputPosition,
        "Cannot track source positions: wrote %s rows, but the task's files hold %s",
        outputPosition,
        recordCount);

    // Each boundary between files splits at most one run
    int capacity = pendingRuns.size() + files.length - 1;
    String[] sourceFiles = new String[capacity];
    long[] sourcePositions = new long[capacity];
    int[] targetFileIndexes = new int[capacity];
    long[] targetPositions = new long[capacity];
    long[] lengths = new long[capacity];
    int runCount = 0;

    int fileIndex = 0;
    long fileStart = 0;
    UTF8String lastFile = null;
    String lastPath = null;
    for (PendingRun run : pendingRuns) {
      if (run.sourceFile != lastFile) {
        lastFile = run.sourceFile;
        lastPath = run.sourceFile.toString();
      }

      long sourcePos = run.sourcePos;
      long targetPos = run.targetPos;
      long remaining = run.length;
      while (remaining > 0) {
        // Skip the files written before this part of the run
        while (targetPos >= fileStart + files[fileIndex].recordCount()) {
          fileStart += files[fileIndex].recordCount();
          fileIndex += 1;
        }

        long length = Math.min(remaining, fileStart + files[fileIndex].recordCount() - targetPos);
        sourceFiles[runCount] = lastPath;
        sourcePositions[runCount] = sourcePos;
        targetFileIndexes[runCount] = fileIndex;
        targetPositions[runCount] = targetPos - fileStart;
        lengths[runCount] = length;
        runCount += 1;

        sourcePos += length;
        targetPos += length;
        remaining -= length;
      }
    }

    TrackedTaskCommit tracked =
        new TrackedTaskCommit(
            files,
            Arrays.copyOf(sourceFiles, runCount),
            Arrays.copyOf(sourcePositions, runCount),
            Arrays.copyOf(targetFileIndexes, runCount),
            Arrays.copyOf(targetPositions, runCount),
            Arrays.copyOf(lengths, runCount));

    LOG.info(
        "Returning {} runs of {} rows for fileSetId={}, targets={}",
        tracked.runCount(),
        outputPosition,
        fileSetId,
        files.length);

    return tracked;
  }

  /**
   * Records the runs of source positions that the committed tasks of a rewrite wrote.
   *
   * <p>Called on the driver when the rewrite commits, with every task's commit message. Executors
   * run in their own JVMs, so a task can only send its runs to the driver with its commit message;
   * the messages are also exactly the tasks whose files the rewrite commits.
   *
   * @param table the table being rewritten
   * @param fileSetId the unique identifier for this rewrite group
   * @param messages the commit messages of the rewrite's tasks
   */
  static void recordSourcePositions(Table table, String fileSetId, WriterCommitMessage[] messages) {
    PositionMappingCoordinator coordinator = PositionMappingCoordinator.get();
    for (WriterCommitMessage message : messages) {
      if (message instanceof TrackedTaskCommit) {
        ((TrackedTaskCommit) message).recordTo(coordinator, table, fileSetId);
      }
    }
  }

  /**
   * A task's commit message that also carries the runs of source positions it wrote to each of its
   * files.
   *
   * <p>A merge compaction can leave many runs, so the runs are kept in parallel arrays.
   */
  static class TrackedTaskCommit extends SparkWrite.TaskCommit {
    private final String[] sourceFiles;
    private final long[] sourcePositions;
    private final int[] targetFileIndexes;
    private final long[] targetPositions;
    private final long[] lengths;

    private TrackedTaskCommit(
        DataFile[] files,
        String[] sourceFiles,
        long[] sourcePositions,
        int[] targetFileIndexes,
        long[] targetPositions,
        long[] lengths) {
      super(files);
      this.sourceFiles = sourceFiles;
      this.sourcePositions = sourcePositions;
      this.targetFileIndexes = targetFileIndexes;
      this.targetPositions = targetPositions;
      this.lengths = lengths;
    }

    int runCount() {
      return lengths.length;
    }

    void recordTo(PositionMappingCoordinator coordinator, Table table, String fileSetId) {
      DataFile[] files = files();
      for (int i = 0; i < lengths.length; i++) {
        coordinator.recordRun(
            table,
            fileSetId,
            sourceFiles[i],
            sourcePositions[i],
            files[targetFileIndexes[i]].location(),
            targetPositions[i],
            lengths[i]);
      }
    }
  }
}
