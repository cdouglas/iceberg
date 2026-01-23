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
package org.apache.iceberg.benchmark.cloud;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;

/**
 * Factory for creating simulated delete files for benchmarking.
 *
 * <p>Simulated delete files are metadata-only representations that satisfy Iceberg APIs without
 * requiring actual delete data. Position counts are specified directly.
 */
public final class SimulatedDeleteFile {

  private static final AtomicLong FILE_COUNTER = new AtomicLong(0);
  private static final long BYTES_PER_DELETE = 16L; // file_path reference + position

  private SimulatedDeleteFile() {}

  /**
   * Create a simulated position delete file.
   *
   * @param spec partition spec
   * @param deleteCount number of position deletes in the file
   * @return a DeleteFile representing position deletes
   */
  public static DeleteFile createPositionDeletes(PartitionSpec spec, long deleteCount) {
    return createPositionDeletes(spec, deleteCount, generateDeleteFilePath());
  }

  /**
   * Create a simulated position delete file with a specific path.
   *
   * @param spec partition spec
   * @param deleteCount number of position deletes in the file
   * @param filePath path for the delete file
   * @return a DeleteFile representing position deletes
   */
  public static DeleteFile createPositionDeletes(
      PartitionSpec spec, long deleteCount, String filePath) {
    return FileMetadata.deleteFileBuilder(spec)
        .ofPositionDeletes()
        .withPath(filePath)
        .withFormat(FileFormat.PARQUET)
        .withRecordCount(deleteCount)
        .withFileSizeInBytes(deleteCount * BYTES_PER_DELETE)
        .build();
  }

  /**
   * Create a simulated position delete file that references a specific data file.
   *
   * @param spec partition spec
   * @param deleteCount number of position deletes in the file
   * @param referencedDataFile the data file these deletes target
   * @return a DeleteFile representing position deletes for a specific data file
   */
  public static DeleteFile createPositionDeletesForFile(
      PartitionSpec spec, long deleteCount, String referencedDataFile) {
    return FileMetadata.deleteFileBuilder(spec)
        .ofPositionDeletes()
        .withPath(generateDeleteFilePath())
        .withFormat(FileFormat.PARQUET)
        .withRecordCount(deleteCount)
        .withFileSizeInBytes(deleteCount * BYTES_PER_DELETE)
        .withReferencedDataFile(referencedDataFile)
        .build();
  }

  /**
   * Create a simulated equality delete file.
   *
   * @param spec partition spec
   * @param deleteCount number of equality deletes in the file
   * @param equalityFieldIds field IDs used for equality comparison
   * @return a DeleteFile representing equality deletes
   */
  public static DeleteFile createEqualityDeletes(
      PartitionSpec spec, long deleteCount, int... equalityFieldIds) {
    return FileMetadata.deleteFileBuilder(spec)
        .ofEqualityDeletes(equalityFieldIds)
        .withPath(generateDeleteFilePath())
        .withFormat(FileFormat.PARQUET)
        .withRecordCount(deleteCount)
        .withFileSizeInBytes(deleteCount * BYTES_PER_DELETE * 2) // Equality deletes are larger
        .build();
  }

  /**
   * Create a batch of simulated position delete files.
   *
   * @param spec partition spec
   * @param numFiles number of delete files to create
   * @param avgDeletesPerFile average deletes per file
   * @param variance variance in delete count (0.0 to 1.0)
   * @param random random number generator for reproducibility
   * @return array of simulated delete files
   */
  public static DeleteFile[] createBatch(
      PartitionSpec spec,
      int numFiles,
      long avgDeletesPerFile,
      double variance,
      java.util.Random random) {
    DeleteFile[] files = new DeleteFile[numFiles];
    for (int i = 0; i < numFiles; i++) {
      double factor = 1.0 + (random.nextDouble() * 2 - 1) * variance;
      long deleteCount = Math.max(1, (long) (avgDeletesPerFile * factor));
      files[i] = createPositionDeletes(spec, deleteCount);
    }
    return files;
  }

  private static String generateDeleteFilePath() {
    return String.format(
        "data/delete-%d-%s.parquet", FILE_COUNTER.incrementAndGet(), UUID.randomUUID());
  }

  /** Reset the file counter (useful for deterministic testing). */
  public static void resetCounter() {
    FILE_COUNTER.set(0);
  }
}
