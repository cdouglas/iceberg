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

  private long outputPosition = 0;
  private String currentTargetFile = null;

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

    // Project away metadata columns to get clean data row
    InternalRow cleanRow = projectDataColumns(row);

    // Write the clean row through delegate
    delegate.write(cleanRow);

    // Track target file (may change on file roll-over)
    // We'll get the current target file path from the delegate's context
    // For now, use a placeholder approach - the actual file path will be determined
    // by examining the WriterCommitMessage after commit
    if (currentTargetFile == null) {
      // Initialize on first write - we'll determine actual path from commit message
      currentTargetFile = "target-pending";
    }

    // Record position mapping
    coordinator.recordMapping(
        table, fileSetId, sourceFile, sourcePos, currentTargetFile, outputPosition);

    outputPosition++;
  }

  /**
   * Projects away metadata columns (_file, _pos) to extract only data columns.
   *
   * <p>Creates a new InternalRow with only the first numDataColumns fields.
   */
  private InternalRow projectDataColumns(InternalRow row) {
    // Create a new row with only data columns (excluding _file and _pos at the end)
    Object[] values = new Object[numDataColumns];
    for (int i = 0; i < numDataColumns; i++) {
      // Copy each field - need to handle different types
      if (row.isNullAt(i)) {
        values[i] = null;
      } else {
        // For simplicity, use generic get() - this works for most types
        // In production, would need type-specific getters for performance
        values[i] = row.get(i, null);
      }
    }
    return new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(values);
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    WriterCommitMessage message = delegate.commit();

    // Extract actual target file paths from commit message
    // This is needed because file paths are only known after commit
    updateTargetFilePaths(message);

    // Reset for next file (if writer is reused)
    outputPosition = 0;
    currentTargetFile = null;

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
   * Updates position mappings with actual target file paths from commit message.
   *
   * <p>The WriterCommitMessage contains the actual file paths written. We need to update our
   * placeholder "target-pending" entries with real paths.
   *
   * <p>TODO: This is a simplified implementation. In production, would need to: 1. Extract actual
   * file paths from WriterCommitMessage 2. Update coordinator mappings with real paths 3. Handle
   * multiple target files if writer rolled over
   */
  private void updateTargetFilePaths(WriterCommitMessage message) {
    // Implementation note: This requires accessing internals of WriterCommitMessage
    // which may vary by Iceberg version. For now, we'll rely on the file path
    // being deterministic based on the write operation.
    //
    // In practice, the coordinator aggregation happens after all writes complete,
    // so we can defer this to Phase 4.2 where we have access to the full
    // commit context.
    LOG.debug(
        "Position tracking recorded {} positions for fileSetId={}", outputPosition, fileSetId);
  }
}
