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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Locale;
import java.util.Random;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.Density;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.roaringbitmap.RoaringBitmap;

/**
 * Generates deletion vector files for benchmarking.
 *
 * <p>Creates Puffin files containing Roaring bitmaps with configurable:
 *
 * <ul>
 *   <li>Number of deleted positions
 *   <li>Density (sparse vs dense distribution)
 *   <li>Referenced data file
 * </ul>
 *
 * <p>Each deletion vector references exactly one data file, unlike position delete files which can
 * reference multiple files. This is a key difference that affects remapping: when the source file
 * is split across multiple target files, the DV must be split into multiple DVs.
 */
public class DeletionVectorGenerator {

  private static final String DV_BLOB_TYPE = "deletion-vector-v1";

  private final Random random;
  private final long maxRowsPerFile;

  public DeletionVectorGenerator(long seed, long maxRowsPerFile) {
    this.random = new Random(seed);
    this.maxRowsPerFile = maxRowsPerFile;
  }

  public DeletionVectorGenerator(long seed) {
    this(seed, 10_000_000L); // 10M rows per file default
  }

  /**
   * Generate a deletion vector file.
   *
   * @param outputFile where to write the Puffin file
   * @param numDeletes number of deleted positions
   * @param referencedDataFile the data file this DV applies to
   * @param density sparse (scattered) or dense (clustered) positions
   * @return metadata about the generated file
   */
  public GeneratedDeletionVector generate(
      OutputFile outputFile, int numDeletes, String referencedDataFile, Density density)
      throws IOException {

    RoaringBitmap bitmap = generateBitmap(numDeletes, density);

    // Serialize bitmap
    ByteBuffer serialized = serializeBitmap(bitmap);

    // Write to Puffin file
    long fileSize = writePuffinFile(outputFile, serialized, referencedDataFile);

    return new GeneratedDeletionVector(
        outputFile.location(),
        referencedDataFile,
        numDeletes,
        bitmap.serializedSizeInBytes(),
        fileSize,
        density,
        bitmap.getCardinality());
  }

  /**
   * Generate multiple deletion vectors in a single Puffin file.
   *
   * @param outputFile where to write the Puffin file
   * @param numDeletesPerDV number of deleted positions per DV
   * @param referencedDataFiles list of data files to create DVs for
   * @param density sparse or dense positions
   * @return metadata about the generated file
   */
  public GeneratedDeletionVectorFile generateMultiple(
      OutputFile outputFile,
      int numDeletesPerDV,
      java.util.List<String> referencedDataFiles,
      Density density)
      throws IOException {

    try (PuffinWriter writer = Puffin.write(outputFile).build()) {
      int totalDeletes = 0;
      long totalBitmapSize = 0;

      for (String dataFile : referencedDataFiles) {
        RoaringBitmap bitmap = generateBitmap(numDeletesPerDV, density);
        ByteBuffer serialized = serializeBitmap(bitmap);

        Blob blob =
            new Blob(
                DV_BLOB_TYPE,
                Collections.singletonList(1), // field IDs (not used for DVs)
                0, // snapshot ID
                0, // sequence number
                serialized,
                null, // compression codec
                Collections.singletonMap("referenced-data-file", dataFile));

        writer.add(blob);
        totalDeletes += bitmap.getCardinality();
        totalBitmapSize += bitmap.serializedSizeInBytes();
      }

      writer.finish();

      // Try to get file length, but handle cloud storage eventual consistency gracefully
      long fileLength;
      try {
        fileLength = outputFile.toInputFile().getLength();
      } catch (Exception e) {
        // Fallback to estimated length if file isn't immediately readable (cloud storage)
        fileLength = totalBitmapSize + 1024; // estimate: bitmap size + Puffin overhead
      }

      return new GeneratedDeletionVectorFile(
          outputFile.location(),
          referencedDataFiles.size(),
          totalDeletes,
          totalBitmapSize,
          fileLength,
          density);
    }
  }

  private RoaringBitmap generateBitmap(int numDeletes, Density density) {
    RoaringBitmap bitmap = new RoaringBitmap();

    switch (density) {
      case SPARSE:
        // Scattered positions - will use array containers in Roaring
        for (int i = 0; i < numDeletes; i++) {
          bitmap.add(random.nextInt((int) maxRowsPerFile));
        }
        break;

      case DENSE:
        // Clustered positions - will use bitset containers in Roaring
        int numClusters = Math.max(1, numDeletes / 1000);
        int positionsPerCluster = numDeletes / numClusters;

        for (int cluster = 0; cluster < numClusters; cluster++) {
          int clusterStart = random.nextInt((int) (maxRowsPerFile - positionsPerCluster));
          int clusterSize =
              (cluster == numClusters - 1)
                  ? numDeletes - (cluster * positionsPerCluster)
                  : positionsPerCluster;

          // Add contiguous range (efficient for Roaring)
          bitmap.add((long) clusterStart, (long) (clusterStart + clusterSize));
        }
        break;
    }

    // Optimize the bitmap structure
    bitmap.runOptimize();

    return bitmap;
  }

  private ByteBuffer serializeBitmap(RoaringBitmap bitmap) {
    ByteBuffer buffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes());
    bitmap.serialize(buffer);
    buffer.flip();
    return buffer;
  }

  private long writePuffinFile(OutputFile outputFile, ByteBuffer bitmapData, String referencedFile)
      throws IOException {

    try (PuffinWriter writer = Puffin.write(outputFile).build()) {
      Blob blob =
          new Blob(
              DV_BLOB_TYPE,
              Collections.singletonList(1),
              0,
              0,
              bitmapData,
              null,
              Collections.singletonMap("referenced-data-file", referencedFile));

      writer.add(blob);
      writer.finish();
    }

    // Handle cloud storage eventual consistency
    try {
      return outputFile.toInputFile().getLength();
    } catch (Exception e) {
      return bitmapData.remaining() + 1024L; // Estimate: bitmap size + Puffin overhead
    }
  }

  /** Metadata about a generated deletion vector. */
  public static class GeneratedDeletionVector {
    private final String path;
    private final String referencedDataFile;
    private final int numDeletes;
    private final long bitmapSizeBytes;
    private final long fileSizeBytes;
    private final Density density;
    private final int cardinality;

    public GeneratedDeletionVector(
        String path,
        String referencedDataFile,
        int numDeletes,
        long bitmapSizeBytes,
        long fileSizeBytes,
        Density density,
        int cardinality) {
      this.path = path;
      this.referencedDataFile = referencedDataFile;
      this.numDeletes = numDeletes;
      this.bitmapSizeBytes = bitmapSizeBytes;
      this.fileSizeBytes = fileSizeBytes;
      this.density = density;
      this.cardinality = cardinality;
    }

    public String path() {
      return path;
    }

    public String referencedDataFile() {
      return referencedDataFile;
    }

    public int numDeletes() {
      return numDeletes;
    }

    public long bitmapSizeBytes() {
      return bitmapSizeBytes;
    }

    public long fileSizeBytes() {
      return fileSizeBytes;
    }

    public Density density() {
      return density;
    }

    public int cardinality() {
      return cardinality;
    }

    /** Compression ratio: raw positions size / bitmap size */
    public double compressionRatio() {
      long rawSize = (long) cardinality * 8; // 8 bytes per position
      return bitmapSizeBytes > 0 ? (double) rawSize / bitmapSizeBytes : 0;
    }

    @Override
    public String toString() {
      return String.format(
          Locale.ROOT,
          "DeletionVector{path=%s, ref=%s, deletes=%d, bitmapSize=%d, fileSize=%d, density=%s, compression=%.1fx}",
          path,
          referencedDataFile,
          numDeletes,
          bitmapSizeBytes,
          fileSizeBytes,
          density,
          compressionRatio());
    }
  }

  /** Metadata about a Puffin file containing multiple deletion vectors. */
  public static class GeneratedDeletionVectorFile {
    private final String path;
    private final int numDVs;
    private final int totalDeletes;
    private final long totalBitmapSize;
    private final long fileSizeBytes;
    private final Density density;

    public GeneratedDeletionVectorFile(
        String path,
        int numDVs,
        int totalDeletes,
        long totalBitmapSize,
        long fileSizeBytes,
        Density density) {
      this.path = path;
      this.numDVs = numDVs;
      this.totalDeletes = totalDeletes;
      this.totalBitmapSize = totalBitmapSize;
      this.fileSizeBytes = fileSizeBytes;
      this.density = density;
    }

    public String path() {
      return path;
    }

    public int numDVs() {
      return numDVs;
    }

    public int totalDeletes() {
      return totalDeletes;
    }

    public long totalBitmapSize() {
      return totalBitmapSize;
    }

    public long fileSizeBytes() {
      return fileSizeBytes;
    }

    public Density density() {
      return density;
    }
  }
}
