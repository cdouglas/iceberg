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
package org.apache.iceberg.deletes;

import java.io.IOException;
import java.util.Collection;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Utility for writing deletion vectors (DVs) from a collection of positions.
 *
 * <p>This is a convenience wrapper around DVFileWriter for remapping scenarios where you have a
 * pre-computed set of positions to write.
 *
 * <p>Example usage:
 *
 * <pre>
 * DVPositionWriter writer = new DVPositionWriter(fileFactory, spec, partition, dataFilePath);
 * DeleteFile dv = writer.writePositions(deletedPositions);
 * </pre>
 */
public class DVPositionWriter {
  private final OutputFileFactory fileFactory;
  private final PartitionSpec spec;
  private final StructLike partition;
  private final String dataFilePath;

  public DVPositionWriter(
      OutputFileFactory fileFactory,
      PartitionSpec spec,
      StructLike partition,
      String dataFilePath) {
    this.fileFactory = fileFactory;
    this.spec = spec;
    this.partition = partition;
    this.dataFilePath = dataFilePath;
  }

  /**
   * Writes a collection of deleted positions to a new DV file.
   *
   * @param positions the deleted positions (0-indexed)
   * @return the written DeleteFile, or null if positions is empty
   * @throws IOException if writing fails
   */
  public DeleteFile writePositions(Collection<Long> positions) throws IOException {
    Preconditions.checkNotNull(positions, "positions cannot be null");

    // Empty positions don't need a DV
    if (positions.isEmpty()) {
      return null;
    }

    // Create DV writer with no previous deletes to load
    DVFileWriter writer = new BaseDVFileWriter(fileFactory, path -> PositionDeleteIndex.empty());

    try {
      // Write each position
      for (Long pos : positions) {
        writer.delete(dataFilePath, pos, spec, partition);
      }

      // Close and get result
      writer.close();
      DeleteWriteResult result = writer.result();

      if (result.deleteFiles().isEmpty()) {
        throw new IllegalStateException("DV writer produced no delete files");
      }

      return result.deleteFiles().get(0);
    } finally {
      writer.close();
    }
  }
}
