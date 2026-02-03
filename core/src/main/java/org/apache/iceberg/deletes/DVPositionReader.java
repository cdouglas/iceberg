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
import java.io.UncheckedIOException;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.util.ContentFileUtil;

/**
 * Utility for reading deletion vector (DV) files and extracting deleted positions.
 *
 * <p>Deletion vectors are stored in Puffin format with a Roaring bitmap representing deleted row
 * positions. This reader converts the bitmap into an iterable of explicit positions.
 *
 * <p>Example usage:
 *
 * <pre>
 * DVPositionReader reader = new DVPositionReader(fileIO);
 * try (CloseableIterable&lt;Long&gt; positions = reader.readDeletedPositions(dvFile)) {
 *   for (Long pos : positions) {
 *     // Process deleted position
 *   }
 * }
 * </pre>
 */
public class DVPositionReader {
  private final FileIO fileIO;

  public DVPositionReader(FileIO fileIO) {
    Preconditions.checkNotNull(fileIO, "fileIO is null");
    this.fileIO = fileIO;
  }

  /**
   * Reads a deletion vector file and returns deleted positions.
   *
   * @param dvFile the deletion vector file to read
   * @return iterable of deleted positions (0-indexed) in ascending order
   * @throws IllegalArgumentException if dvFile is not a deletion vector
   * @throws IllegalStateException if DV is missing contentOffset or contentSizeInBytes
   */
  public CloseableIterable<Long> readDeletedPositions(DeleteFile dvFile) {
    Preconditions.checkNotNull(dvFile, "dvFile is null");

    // Validate this is a deletion vector
    if (!ContentFileUtil.isDV(dvFile)) {
      throw new IllegalArgumentException(
          String.format(
              "Not a deletion vector (expected format PUFFIN): %s (format: %s)",
              dvFile.location(), dvFile.format()));
    }

    // Validate required fields are present
    if (dvFile.contentOffset() == null) {
      throw new IllegalStateException(
          String.format("DV missing contentOffset: %s", dvFile.location()));
    }

    if (dvFile.contentSizeInBytes() == null) {
      throw new IllegalStateException(
          String.format("DV missing contentSizeInBytes: %s", dvFile.location()));
    }

    // Read DV blob bytes
    try {
      InputFile inputFile = fileIO.newInputFile(dvFile.location());
      long offset = dvFile.contentOffset();
      int length = dvFile.contentSizeInBytes().intValue();
      byte[] bytes = readBytes(inputFile, offset, length);

      // Deserialize to PositionDeleteIndex
      PositionDeleteIndex index = PositionDeleteIndex.deserialize(bytes, dvFile);

      // Convert to CloseableIterable of positions
      return new PositionIterable(index);

    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read deletion vector: " + dvFile.location(), e);
    }
  }

  /**
   * Reads a deletion vector file and returns deleted positions as a primitive array.
   *
   * <p>This method avoids boxing overhead by collecting positions directly into a primitive long
   * array. For large DVs with millions of positions, this is significantly faster than {@link
   * #readDeletedPositions(DeleteFile)} which returns boxed Longs.
   *
   * @param dvFile the deletion vector file to read
   * @return array of deleted positions (0-indexed) in ascending order
   * @throws IllegalArgumentException if dvFile is not a deletion vector
   * @throws IllegalStateException if DV is missing contentOffset or contentSizeInBytes
   */
  public long[] readDeletedPositionsPrimitive(DeleteFile dvFile) {
    Preconditions.checkNotNull(dvFile, "dvFile is null");

    // Validate this is a deletion vector
    if (!ContentFileUtil.isDV(dvFile)) {
      throw new IllegalArgumentException(
          String.format(
              "Not a deletion vector (expected format PUFFIN): %s (format: %s)",
              dvFile.location(), dvFile.format()));
    }

    // Validate required fields are present
    if (dvFile.contentOffset() == null) {
      throw new IllegalStateException(
          String.format("DV missing contentOffset: %s", dvFile.location()));
    }

    if (dvFile.contentSizeInBytes() == null) {
      throw new IllegalStateException(
          String.format("DV missing contentSizeInBytes: %s", dvFile.location()));
    }

    // Read DV blob bytes
    try {
      InputFile inputFile = fileIO.newInputFile(dvFile.location());
      long offset = dvFile.contentOffset();
      int length = dvFile.contentSizeInBytes().intValue();
      byte[] bytes = readBytes(inputFile, offset, length);

      // Deserialize to PositionDeleteIndex
      PositionDeleteIndex index = PositionDeleteIndex.deserialize(bytes, dvFile);

      // Pre-allocate primitive array using cardinality
      long cardinality = index.cardinality();
      if (cardinality > Integer.MAX_VALUE) {
        throw new IllegalStateException(
            String.format(
                java.util.Locale.ROOT,
                "DV cardinality exceeds max array size: %d (max: %d)",
                cardinality,
                Integer.MAX_VALUE));
      }

      long[] positions = new long[(int) cardinality];

      // Fill array directly without boxing using a mutable index
      int[] idx = {0};
      index.forEach(pos -> positions[idx[0]++] = pos);

      return positions;

    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read deletion vector: " + dvFile.location(), e);
    }
  }

  /**
   * Returns the referenced data file path from the DV.
   *
   * @param dvFile the deletion vector file
   * @return the data file path that this DV applies to, or null if not set
   */
  public String referencedDataFile(DeleteFile dvFile) {
    Preconditions.checkNotNull(dvFile, "dvFile is null");
    return dvFile.referencedDataFile();
  }

  private byte[] readBytes(InputFile inputFile, long offset, int length) throws IOException {
    byte[] bytes = new byte[length];
    try (SeekableInputStream input = inputFile.newStream()) {
      if (input instanceof RangeReadable) {
        ((RangeReadable) input).readFully(offset, bytes);
      } else {
        input.seek(offset);
        ByteStreams.readFully(input, bytes);
      }
    }
    return bytes;
  }

  /**
   * CloseableIterable wrapper around PositionDeleteIndex that provides positions in ascending
   * order.
   */
  private static class PositionIterable implements CloseableIterable<Long> {
    private final PositionDeleteIndex index;

    PositionIterable(PositionDeleteIndex index) {
      this.index = index;
    }

    @Override
    public CloseableIterator<Long> iterator() {
      // Collect all positions using forEach
      java.util.List<Long> positions = Lists.newArrayList();
      index.forEach(positions::add);
      return CloseableIterator.withClose(positions.iterator());
    }

    @Override
    public void close() {
      // PositionDeleteIndex doesn't require closing
    }
  }
}
