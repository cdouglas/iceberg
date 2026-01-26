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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;

/**
 * Factory for creating simulated data files for benchmarking.
 *
 * <p>Simulated data files are metadata-only representations that satisfy Iceberg APIs without
 * requiring actual data. Row counts and file sizes are specified directly rather than derived from
 * real file contents.
 */
public final class SimulatedDataFile {

  private static final AtomicLong FILE_COUNTER = new AtomicLong(0);
  private static final long BYTES_PER_ROW = 100L; // Assumed average row size

  private SimulatedDataFile() {}

  /**
   * Create a simulated data file with the specified row count.
   *
   * @param spec partition spec (use PartitionSpec.unpartitioned() for unpartitioned tables)
   * @param rowCount number of rows in the simulated file
   * @return a DataFile with the specified row count
   */
  public static DataFile create(PartitionSpec spec, long rowCount) {
    return create(spec, rowCount, generateFilePath());
  }

  /**
   * Create a simulated data file with a specific path.
   *
   * @param spec partition spec
   * @param rowCount number of rows in the simulated file
   * @param filePath path for the simulated file
   * @return a DataFile with the specified row count and path
   */
  public static DataFile create(PartitionSpec spec, long rowCount, String filePath) {
    return DataFiles.builder(spec)
        .withPath(filePath)
        .withFormat(FileFormat.PARQUET)
        .withRecordCount(rowCount)
        .withFileSizeInBytes(rowCount * BYTES_PER_ROW)
        .build();
  }

  /**
   * Create a simulated data file with partition values.
   *
   * @param spec partition spec
   * @param rowCount number of rows in the simulated file
   * @param partitionPath partition path in format "field1=value1/field2=value2"
   * @return a DataFile with the specified partition
   */
  public static DataFile createPartitioned(
      PartitionSpec spec, long rowCount, String partitionPath) {
    return DataFiles.builder(spec)
        .withPath(generateFilePath())
        .withPartitionPath(partitionPath)
        .withFormat(FileFormat.PARQUET)
        .withRecordCount(rowCount)
        .withFileSizeInBytes(rowCount * BYTES_PER_ROW)
        .build();
  }

  /**
   * Create a batch of simulated data files with varying row counts.
   *
   * @param spec partition spec
   * @param numFiles number of files to create
   * @param avgRowsPerFile average rows per file
   * @param variance variance in row count (0.0 to 1.0)
   * @param random random number generator for reproducibility
   * @return array of simulated data files
   */
  public static DataFile[] createBatch(
      PartitionSpec spec,
      int numFiles,
      long avgRowsPerFile,
      double variance,
      java.util.Random random) {
    DataFile[] files = new DataFile[numFiles];
    for (int i = 0; i < numFiles; i++) {
      double factor = 1.0 + (random.nextDouble() * 2 - 1) * variance;
      long rowCount = Math.max(1, (long) (avgRowsPerFile * factor));
      files[i] = create(spec, rowCount);
    }
    return files;
  }

  private static String generateFilePath() {
    return String.format(
        "data/simulated-%d-%s.parquet", FILE_COUNTER.incrementAndGet(), UUID.randomUUID());
  }

  /** Reset the file counter (useful for deterministic testing). */
  public static void resetCounter() {
    FILE_COUNTER.set(0);
  }
}
