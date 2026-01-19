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
package org.apache.iceberg.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PositionDeleteRecord;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Writer for remapped position deletes after compaction conflict recovery.
 *
 * <p>This writer takes position deletes that have been remapped from source files to target files
 * and writes them to new delete manifests. Deletes are grouped by partition and sorted by position
 * for efficient storage and lookup.
 *
 * <p>Example usage:
 *
 * <pre>
 * Map&lt;String, List&lt;PositionDeleteRecord&gt;&gt; remappedDeletes = remapper.remapDeletes(deletes);
 *
 * try (RemappedDeleteWriter writer = new RemappedDeleteWriter(table)) {
 *   List&lt;DeleteFile&gt; deleteFiles = writer.writeDeletes(remappedDeletes);
 *   // Add deleteFiles to the commit
 * }
 * </pre>
 */
public class RemappedDeleteWriter implements Closeable {
  private final Table table;
  private final FileFormat deleteFileFormat;
  private final OutputFileFactory outputFileFactory;
  private final List<DeleteFile> writtenDeleteFiles;
  private boolean closed = false;

  /**
   * Creates a writer for remapped position deletes.
   *
   * @param table the table to write delete files for
   */
  public RemappedDeleteWriter(Table table) {
    this(table, 0, 0);
  }

  /**
   * Creates a writer for remapped position deletes with specific partition and task IDs.
   *
   * @param table the table to write delete files for
   * @param partitionId the partition ID for file naming
   * @param taskId the task ID for file naming
   */
  public RemappedDeleteWriter(Table table, int partitionId, long taskId) {
    Preconditions.checkNotNull(table, "table is null");
    this.table = table;
    this.deleteFileFormat = FileFormat.AVRO; // Use Avro format for delete files
    this.outputFileFactory =
        OutputFileFactory.builderFor(table, partitionId, taskId)
            .format(deleteFileFormat)
            .suffix("remapped-deletes")
            .build();
    this.writtenDeleteFiles = Lists.newArrayList();
  }

  /**
   * Writes remapped position deletes to new delete files.
   *
   * <p>Deletes are grouped by partition and sorted by (file_path, position) for efficient storage.
   * Each partition produces a separate delete file.
   *
   * @param remappedDeletes map from target file path to list of remapped position delete records
   * @return list of written delete files (may be empty if no deletes to write)
   * @throws IOException if writing fails
   */
  public List<DeleteFile> writeDeletes(Map<String, List<PositionDeleteRecord>> remappedDeletes)
      throws IOException {
    Preconditions.checkState(!closed, "Writer is already closed");
    Preconditions.checkNotNull(remappedDeletes, "remappedDeletes is null");

    if (remappedDeletes.isEmpty()) {
      return Lists.newArrayList();
    }

    // Collect all deletes and group by partition
    Map<StructLike, List<PositionDeleteRecord>> deletesByPartition =
        groupDeletesByPartition(remappedDeletes);

    // Write delete files for each partition
    for (Map.Entry<StructLike, List<PositionDeleteRecord>> entry : deletesByPartition.entrySet()) {
      StructLike partition = entry.getKey();
      List<PositionDeleteRecord> deletes = entry.getValue();

      if (!deletes.isEmpty()) {
        DeleteFile deleteFile = writeDeleteFile(partition, deletes);
        writtenDeleteFiles.add(deleteFile);
      }
    }

    return Lists.newArrayList(writtenDeleteFiles);
  }

  /**
   * Groups position deletes by their partition.
   *
   * <p>If partition data is not available, deletes are grouped under a null partition key (for
   * unpartitioned tables).
   */
  private Map<StructLike, List<PositionDeleteRecord>> groupDeletesByPartition(
      Map<String, List<PositionDeleteRecord>> remappedDeletes) {
    Map<StructLike, List<PositionDeleteRecord>> byPartition = Maps.newHashMap();

    for (List<PositionDeleteRecord> deletes : remappedDeletes.values()) {
      for (PositionDeleteRecord delete : deletes) {
        StructLike partition = delete.partitionData();
        byPartition.computeIfAbsent(partition, k -> Lists.newArrayList()).add(delete);
      }
    }

    return byPartition;
  }

  /**
   * Writes a delete file for a single partition.
   *
   * <p>Deletes are sorted by (file_path, position) before writing for efficient lookups during read
   * time.
   */
  private DeleteFile writeDeleteFile(StructLike partition, List<PositionDeleteRecord> deletes)
      throws IOException {
    // Sort deletes by (file_path, position) for efficient reads
    deletes.sort(
        Comparator.comparing(PositionDeleteRecord::dataFilePath)
            .thenComparingLong(PositionDeleteRecord::position));

    PartitionSpec spec = table.spec();
    OutputFile outputFile;
    if (partition == null || spec.isUnpartitioned()) {
      outputFile = outputFileFactory.newOutputFile().encryptingOutputFile();
    } else {
      outputFile = outputFileFactory.newOutputFile(spec, partition).encryptingOutputFile();
    }

    // Create position delete writer
    PositionDeleteWriter<StructLike> writer =
        Avro.writeDeletes(outputFile)
            .createWriterFunc(DataWriter::create)
            .overwrite()
            .withSpec(spec)
            .withPartition(partition)
            .buildPositionWriter();

    // Write deletes
    PositionDelete<StructLike> posDelete = PositionDelete.create();
    try {
      for (PositionDeleteRecord delete : deletes) {
        posDelete.set(delete.dataFilePath(), delete.position(), delete.rowData());
        writer.write(posDelete);
      }
    } finally {
      writer.close();
    }

    return writer.toDeleteFile();
  }

  /**
   * Returns the list of delete files written so far.
   *
   * @return list of written delete files
   */
  public List<DeleteFile> writtenDeleteFiles() {
    return Lists.newArrayList(writtenDeleteFiles);
  }

  @Override
  public void close() throws IOException {
    closed = true;
  }
}
