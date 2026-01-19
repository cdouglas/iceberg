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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PositionDeleteRecord;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Utility for reading position deletes from delete manifests.
 *
 * <p>Reads position delete files and converts them to {@link PositionDeleteRecord} for use in
 * compaction conflict recovery.
 */
public class DeleteManifestReader {

  private final FileIO fileIO;

  public DeleteManifestReader(FileIO fileIO) {
    this.fileIO = fileIO;
  }

  /**
   * Read position deletes from a delete file.
   *
   * @param deleteFile the delete file to read
   * @return list of position delete records
   */
  public List<PositionDeleteRecord> readPositionDeletes(DeleteFile deleteFile) {
    Preconditions.checkNotNull(deleteFile, "deleteFile is null");

    // Get file format from delete file metadata
    FileFormat format = deleteFile.format();

    // Create input file
    InputFile inputFile = fileIO.newInputFile(deleteFile.path().toString());

    // Read position deletes as records
    List<PositionDeleteRecord> records = Lists.newArrayList();

    try {
      CloseableIterable<Record> deletes;

      // For now, we only support Avro format
      // Other formats (Parquet, ORC) can be added later
      if (format == FileFormat.AVRO) {
        // Read only the required fields (path and position)
        // Note: This will NOT preserve row data - that's acceptable for Phase 1
        Schema deleteSchema =
            new Schema(MetadataColumns.DELETE_FILE_PATH, MetadataColumns.DELETE_FILE_POS);

        deletes =
            Avro.read(inputFile).project(deleteSchema).createReaderFunc(DataReader::create).build();
      } else {
        throw new UnsupportedOperationException(
            "Only Avro format is currently supported for reading position deletes, got: " + format);
      }

      try (CloseableIterable<Record> closeableDeletes = deletes) {
        for (Record delete : closeableDeletes) {
          // Extract file path and position from the record
          String dataFilePath = delete.getField(MetadataColumns.DELETE_FILE_PATH.name()).toString();
          long position = (Long) delete.getField(MetadataColumns.DELETE_FILE_POS.name());

          // For now, we don't preserve row data (Phase 1 limitation)
          // Also, we don't capture partition data from the delete file
          // This would require reading partition values from the file metadata
          // or from additional columns in the delete file
          PositionDeleteRecord record =
              new PositionDeleteRecord(dataFilePath, position, null, null);
          records.add(record);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to read position deletes from file: %s", deleteFile.path()), e);
    }

    return records;
  }

  /**
   * Filter position delete records by referenced data files.
   *
   * @param records list of position delete records
   * @param dataFiles set of data file paths to filter by
   * @return filtered list of position delete records that reference the given data files
   */
  public static List<PositionDeleteRecord> filterByReferencedFiles(
      List<PositionDeleteRecord> records, Set<String> dataFiles) {
    Preconditions.checkNotNull(records, "records is null");
    Preconditions.checkNotNull(dataFiles, "dataFiles is null");

    if (dataFiles.isEmpty()) {
      return Lists.newArrayList();
    }

    List<PositionDeleteRecord> filtered = Lists.newArrayList();
    for (PositionDeleteRecord record : records) {
      if (dataFiles.contains(record.dataFilePath())) {
        filtered.add(record);
      }
    }

    return filtered;
  }

  /**
   * Read position deletes from multiple delete files and filter by referenced data files.
   *
   * @param deleteFiles list of delete files to read
   * @param dataFiles set of data file paths to filter by
   * @return filtered list of position delete records
   */
  public List<PositionDeleteRecord> readAndFilterPositionDeletes(
      List<DeleteFile> deleteFiles, Set<String> dataFiles) {
    Preconditions.checkNotNull(deleteFiles, "deleteFiles is null");
    Preconditions.checkNotNull(dataFiles, "dataFiles is null");

    List<PositionDeleteRecord> allRecords = Lists.newArrayList();

    for (DeleteFile deleteFile : deleteFiles) {
      // Skip delete files that don't reference any of our data files
      // (optimization for delete files with referencedDataFile set)
      String referencedFile = deleteFile.referencedDataFile();
      if (referencedFile != null && !dataFiles.contains(referencedFile)) {
        continue;
      }

      List<PositionDeleteRecord> records = readPositionDeletes(deleteFile);
      List<PositionDeleteRecord> filtered = filterByReferencedFiles(records, dataFiles);
      allRecords.addAll(filtered);
    }

    return allRecords;
  }
}
