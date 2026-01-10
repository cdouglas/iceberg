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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Helper for writing remapped DVs after compaction.
 *
 * <p>Takes remapped positions (from PositionDeleteRemapper.remapDV()) and writes new DV files for
 * each target data file.
 *
 * <p>Example usage:
 *
 * <pre>
 * Map&lt;String, Set&lt;Long&gt;&gt; remappedPositions = remapper.remapDV(dvFile, fileIO);
 * RemappedDVWriter writer = new RemappedDVWriter(table, spec, partition);
 * List&lt;DeleteFile&gt; newDVs = writer.writeRemappedDVs(remappedPositions);
 * </pre>
 */
public class RemappedDVWriter {
  private final Table table;
  private final PartitionSpec spec;
  private final StructLike partition;

  public RemappedDVWriter(Table table, PartitionSpec spec, StructLike partition) {
    this.table = table;
    this.spec = spec;
    this.partition = partition;
  }

  /**
   * Writes new DV files for remapped positions.
   *
   * @param remappedPositions map from target file path to deleted positions
   * @return list of newly written DV files (one per target file)
   * @throws IOException if writing fails
   */
  public List<DeleteFile> writeRemappedDVs(Map<String, Set<Long>> remappedPositions)
      throws IOException {
    Preconditions.checkNotNull(remappedPositions, "remappedPositions cannot be null");

    List<DeleteFile> newDVs = new ArrayList<>();

    for (Map.Entry<String, Set<Long>> entry : remappedPositions.entrySet()) {
      String targetFile = entry.getKey();
      Set<Long> positions = entry.getValue();

      if (positions.isEmpty()) {
        // No deletes for this target file, skip
        continue;
      }

      // Create OutputFileFactory for each DV
      // Use unique partition and task IDs to ensure unique file names
      int partitionId = newDVs.size() + 1;
      int taskId = 1;
      OutputFileFactory fileFactory =
          OutputFileFactory.builderFor(table, partitionId, taskId)
              .format(FileFormat.PUFFIN)
              .build();

      // Write DV for this target file
      DVPositionWriter writer = new DVPositionWriter(fileFactory, spec, partition, targetFile);
      DeleteFile dv = writer.writePositions(positions);

      // DVPositionWriter returns null for empty positions, but we already checked above
      if (dv != null) {
        newDVs.add(dv);
      }
    }

    return newDVs;
  }
}
