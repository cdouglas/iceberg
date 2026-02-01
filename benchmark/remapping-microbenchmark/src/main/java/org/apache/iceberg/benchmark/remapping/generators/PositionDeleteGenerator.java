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
package org.apache.iceberg.benchmark.remapping.generators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import org.apache.iceberg.Schema;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.Density;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

/**
 * Generates position delete files for benchmarking.
 *
 * <p>Creates Parquet files containing (file_path, position) tuples with configurable:
 *
 * <ul>
 *   <li>Number of deletes
 *   <li>Number of source files referenced
 *   <li>Density (sparse vs dense distribution)
 *   <li>Sorted vs unsorted positions
 * </ul>
 */
public class PositionDeleteGenerator {

  /** Schema for position delete files: (file_path STRING, pos LONG) */
  public static final Schema DELETE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "file_path", Types.StringType.get()),
          Types.NestedField.required(2, "pos", Types.LongType.get()));

  private final Random random;
  private final long maxRowsPerFile;

  public PositionDeleteGenerator(long seed, long maxRowsPerFile) {
    this.random = new Random(seed);
    this.maxRowsPerFile = maxRowsPerFile;
  }

  public PositionDeleteGenerator(long seed) {
    this(seed, 10_000_000L); // 10M rows per file default
  }

  /**
   * Generate a position delete file.
   *
   * @param outputFile where to write the file
   * @param numDeletes total number of position deletes
   * @param numSourceFiles number of source files to distribute deletes across
   * @param density sparse (scattered) or dense (clustered) positions
   * @param sorted whether positions should be sorted within each file
   * @return metadata about the generated file
   */
  public GeneratedDeleteFile generate(
      OutputFile outputFile, int numDeletes, int numSourceFiles, Density density, boolean sorted)
      throws IOException {

    List<PositionDelete<Record>> deletes = new ArrayList<>(numDeletes);
    List<String> sourceFiles = generateSourceFilePaths(numSourceFiles);

    // Distribute deletes across source files
    int deletesPerFile = numDeletes / numSourceFiles;
    int remainder = numDeletes % numSourceFiles;

    for (int fileIdx = 0; fileIdx < numSourceFiles; fileIdx++) {
      String filePath = sourceFiles.get(fileIdx);
      int fileDeletes = deletesPerFile + (fileIdx < remainder ? 1 : 0);

      List<Long> positions = generatePositions(fileDeletes, density);
      if (sorted) {
        Collections.sort(positions);
      }

      for (long pos : positions) {
        PositionDelete<Record> delete = PositionDelete.create();
        delete.set(filePath, pos, null);
        deletes.add(delete);
      }
    }

    // Sort by file path for better compression if requested
    if (sorted) {
      deletes.sort(
          (a, b) -> {
            int cmp = a.path().toString().compareTo(b.path().toString());
            if (cmp != 0) return cmp;
            return Long.compare(a.pos(), b.pos());
          });
    }

    // Write to Parquet
    long fileSize = writePositionDeletes(outputFile, deletes);

    return new GeneratedDeleteFile(
        outputFile.location(), numDeletes, numSourceFiles, fileSize, density, sorted, sourceFiles);
  }

  private List<String> generateSourceFilePaths(int numFiles) {
    List<String> paths = new ArrayList<>(numFiles);
    for (int i = 0; i < numFiles; i++) {
      paths.add(String.format(Locale.ROOT, "s3://bucket/data/file-%05d.parquet", i));
    }
    return paths;
  }

  private List<Long> generatePositions(int count, Density density) {
    List<Long> positions = new ArrayList<>(count);

    switch (density) {
      case SPARSE:
        // Scattered positions across the full range
        for (int i = 0; i < count; i++) {
          positions.add((long) random.nextInt((int) maxRowsPerFile));
        }
        break;

      case DENSE:
        // Clustered positions in contiguous ranges
        int numClusters = Math.max(1, count / 100);
        int positionsPerCluster = count / numClusters;

        for (int cluster = 0; cluster < numClusters; cluster++) {
          long clusterStart = random.nextInt((int) (maxRowsPerFile - positionsPerCluster));
          int clusterSize =
              (cluster == numClusters - 1)
                  ? count - (cluster * positionsPerCluster)
                  : positionsPerCluster;

          for (int i = 0; i < clusterSize; i++) {
            positions.add(clusterStart + i);
          }
        }
        break;
    }

    return positions;
  }

  private long writePositionDeletes(OutputFile outputFile, List<PositionDelete<Record>> deletes)
      throws IOException {

    try (FileAppender<Record> appender =
        Parquet.write(outputFile)
            .schema(DELETE_SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .build()) {

      for (PositionDelete<Record> delete : deletes) {
        GenericRecord record = GenericRecord.create(DELETE_SCHEMA);
        record.setField("file_path", delete.path().toString());
        record.setField("pos", delete.pos());
        appender.add(record);
      }
    }

    // Handle cloud storage eventual consistency
    try {
      return outputFile.toInputFile().getLength();
    } catch (Exception e) {
      return 0; // Size is only for reporting, not critical
    }
  }

  /** Metadata about a generated position delete file. */
  public static class GeneratedDeleteFile {
    private final String path;
    private final int numDeletes;
    private final int numSourceFiles;
    private final long fileSizeBytes;
    private final Density density;
    private final boolean sorted;
    private final List<String> sourceFiles;

    public GeneratedDeleteFile(
        String path,
        int numDeletes,
        int numSourceFiles,
        long fileSizeBytes,
        Density density,
        boolean sorted,
        List<String> sourceFiles) {
      this.path = path;
      this.numDeletes = numDeletes;
      this.numSourceFiles = numSourceFiles;
      this.fileSizeBytes = fileSizeBytes;
      this.density = density;
      this.sorted = sorted;
      this.sourceFiles = Collections.unmodifiableList(new ArrayList<>(sourceFiles));
    }

    public String path() {
      return path;
    }

    public int numDeletes() {
      return numDeletes;
    }

    public int numSourceFiles() {
      return numSourceFiles;
    }

    public long fileSizeBytes() {
      return fileSizeBytes;
    }

    public Density density() {
      return density;
    }

    public boolean sorted() {
      return sorted;
    }

    public List<String> sourceFiles() {
      return sourceFiles;
    }

    @Override
    public String toString() {
      return String.format(
          Locale.ROOT,
          "PositionDeleteFile{path=%s, deletes=%d, files=%d, size=%d, density=%s, sorted=%s}",
          path,
          numDeletes,
          numSourceFiles,
          fileSizeBytes,
          density,
          sorted);
    }
  }
}
