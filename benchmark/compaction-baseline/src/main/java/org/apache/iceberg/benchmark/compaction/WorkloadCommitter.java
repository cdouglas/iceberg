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
package org.apache.iceberg.benchmark.compaction;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Iceberg I/O helpers backing {@link SetupMain}'s warehouse builder. Produces actual {@link
 * DataFile} and {@link DeleteFile} instances; commit composition (one snapshot for an insert vs.
 * one snapshot for an insert + delete bundle) is the caller's responsibility, since the spec's
 * {@code S_1..S_{10}} cells require both in a single {@code RowDelta}.
 *
 * <p>This class is the only Iceberg-touching counterpart to {@link WorkloadGenerator}, which
 * remains pure (no I/O, no Iceberg APIs). Determinism flows from the seeded {@link Random}
 * instances handed in here; file paths themselves carry random UUIDs and are not part of the
 * fixture's hashable surface.
 */
public final class WorkloadCommitter {

  private WorkloadCommitter() {}

  /**
   * Stream {@code totalRows} rows from a seeded RNG into Parquet files of approximately {@code
   * rowsPerFile} rows each, writing under the table's data directory via {@code fileFactory}.
   * Returns the resulting {@link DataFile} handles in write order. The caller commits them (e.g.,
   * via {@code AppendFiles} or {@code RowDelta.addRows}).
   *
   * <p>Rows are streamed one at a time — peak heap is bounded by the appender's row-group buffer,
   * not by {@code totalRows}.
   */
  public static List<DataFile> writeDataFiles(
      Table table, OutputFileFactory fileFactory, long seed, long totalRows, int rowsPerFile)
      throws IOException {
    if (totalRows < 0) {
      throw new IllegalArgumentException("totalRows must be >= 0, got " + totalRows);
    }
    if (rowsPerFile <= 0) {
      throw new IllegalArgumentException("rowsPerFile must be > 0, got " + rowsPerFile);
    }

    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema());
    GenericRecord template = GenericRecord.create(table.schema());
    Random rng = new Random(seed);
    List<DataFile> files = Lists.newArrayList();

    long remaining = totalRows;
    while (remaining > 0) {
      int chunk = (int) Math.min(rowsPerFile, remaining);
      EncryptedOutputFile encrypted = fileFactory.newOutputFile();
      OutputFile outputFile = encrypted.encryptingOutputFile();
      long byteLength;

      try (FileAppender<org.apache.iceberg.data.Record> appender =
          appenderFactory.newAppender(outputFile, FileFormat.PARQUET)) {
        for (int i = 0; i < chunk; i++) {
          appender.add(WorkloadGenerator.nextRow(rng, template));
        }
        // length() is documented as valid only after close, so we read it after the try-block.
        appender.close();
        byteLength = appender.length();
      }

      DataFile dataFile =
          DataFiles.builder(table.spec())
              .withPath(outputFile.location())
              .withFormat(FileFormat.PARQUET)
              .withRecordCount(chunk)
              .withFileSizeInBytes(byteLength)
              .build();
      files.add(dataFile);
      remaining -= chunk;
    }

    return files;
  }

  /**
   * Build a Puffin deletion-vector blob containing every position in {@code deletesByDataFile} and
   * return both the newly-written DVs and any pre-existing DVs that were merged into them.
   *
   * <p>V3 enforces "one DV per data file" — if a data file already has a DV in the table, adding a
   * second one to the same file fails validation. Callers that may target previously- deleted files
   * must therefore (1) pass a {@code loadPreviousDeletes} callback that returns the existing
   * positions for each path, and (2) commit the returned {@link DeleteWriteResult} by adding {@link
   * DeleteWriteResult#deleteFiles()} via {@code RowDelta.addDeletes} AND removing {@link
   * DeleteWriteResult#rewrittenDeleteFiles()} via {@code RowDelta.removeDeletes} in the same
   * commit.
   *
   * <p>Pass {@code path -> PositionDeleteIndex.empty()} when the caller can guarantee no prior DV
   * exists (e.g., the late-tx commit on a freshly compacted table).
   */
  public static DeleteWriteResult writeDeletionVectors(
      Table table,
      OutputFileFactory fileFactory,
      Map<String, long[]> deletesByDataFile,
      Function<String, PositionDeleteIndex> loadPreviousDeletes)
      throws IOException {
    if (deletesByDataFile.isEmpty()) {
      return new DeleteWriteResult(Lists.newArrayList());
    }

    DVFileWriter writer = new BaseDVFileWriter(fileFactory, loadPreviousDeletes);
    for (Map.Entry<String, long[]> entry : deletesByDataFile.entrySet()) {
      String dataFilePath = entry.getKey();
      for (long pos : entry.getValue()) {
        writer.delete(dataFilePath, pos, table.spec(), null);
      }
    }
    writer.close();
    return writer.result();
  }

  /**
   * Convenience: build an {@link OutputFileFactory} for Parquet files under the given table.
   * Distinct {@code partitionId}/{@code taskId} pairs avoid filename collisions across snapshots.
   */
  public static OutputFileFactory parquetFileFactory(Table table, int partitionId, long taskId) {
    return OutputFileFactory.builderFor(table, partitionId, taskId)
        .format(FileFormat.PARQUET)
        .build();
  }

  /** Convenience: build an {@link OutputFileFactory} for Puffin DV blobs under the given table. */
  public static OutputFileFactory puffinFileFactory(Table table, int partitionId, long taskId) {
    return OutputFileFactory.builderFor(table, partitionId, taskId)
        .format(FileFormat.PUFFIN)
        .build();
  }

  /**
   * Write file-scoped V2 parquet position-delete files — one output file per source data file in
   * {@code deletesByDataFile}. File scope is load-bearing: {@link
   * org.apache.iceberg.CompactionConflictDetector} short-circuits PARTITION-granularity PD files
   * (it relies on {@code referencedDataFile()}). The fuzz harness must therefore always emit FILE-
   * granularity outputs so the detector reports each conflict the resolver needs to remap.
   *
   * <p>Unlike {@link #writeDeletionVectors}, V2 has no "one PD file per data file" invariant, so
   * there is no merge/rewrite path here — callers simply add the returned files via {@code
   * RowDelta.addDeletes}.
   */
  public static DeleteWriteResult writePositionDeleteFiles(
      Table table, OutputFileFactory fileFactory, Map<String, long[]> deletesByDataFile)
      throws IOException {
    if (deletesByDataFile.isEmpty()) {
      return new DeleteWriteResult(Lists.newArrayList());
    }

    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema());
    List<DeleteFile> outputs = Lists.newArrayListWithCapacity(deletesByDataFile.size());
    for (Map.Entry<String, long[]> entry : deletesByDataFile.entrySet()) {
      String dataFilePath = entry.getKey();
      long[] positions = entry.getValue();
      if (positions.length == 0) {
        continue;
      }
      EncryptedOutputFile encrypted = fileFactory.newOutputFile();
      PositionDeleteWriter<Record> writer =
          appenderFactory.newPosDeleteWriter(encrypted, FileFormat.PARQUET, null /* partition */);
      PositionDelete<Record> pd = PositionDelete.create();
      try {
        for (long pos : positions) {
          writer.write(pd.set(dataFilePath, pos, null));
        }
      } finally {
        writer.close();
      }
      outputs.add(writer.toDeleteFile());
    }
    return new DeleteWriteResult(outputs);
  }

  /**
   * Write a single equality-delete file targeting {@code equalityFieldIds} (typically just {@code
   * long_0}'s field id, {@code 5}). Values come from the supplied seeded RNG — they are random
   * 64-bit longs and almost certainly do not match real row content. This is intentional: the op
   * exercises the equality-delete commit + conflict-detection + resolver code paths without
   * coupling predicate selection to the row-generation seed.
   */
  public static DeleteFile writeEqualityDeleteFile(
      Table table,
      OutputFileFactory fileFactory,
      int[] equalityFieldIds,
      Schema eqDeleteRowSchema,
      long opSeed,
      int rowsPerOp)
      throws IOException {
    if (rowsPerOp <= 0) {
      throw new IllegalArgumentException("rowsPerOp must be > 0, got " + rowsPerOp);
    }
    GenericAppenderFactory factory =
        new GenericAppenderFactory(
            table.schema(),
            PartitionSpec.unpartitioned(),
            equalityFieldIds,
            eqDeleteRowSchema,
            null);
    EncryptedOutputFile encrypted = fileFactory.newOutputFile();
    EqualityDeleteWriter<Record> writer =
        factory.newEqDeleteWriter(encrypted, FileFormat.PARQUET, null /* partition */);
    java.util.Random rng = new java.util.Random(opSeed);
    GenericRecord template = GenericRecord.create(eqDeleteRowSchema);
    try {
      for (int i = 0; i < rowsPerOp; i++) {
        GenericRecord row = template.copy();
        row.set(0, rng.nextLong());
        writer.write(row);
      }
    } finally {
      writer.close();
    }
    return writer.toDeleteFile();
  }
}
