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
package org.apache.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CompactionMap.FileMapping;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * Utility for remapping position deletes when data files have been compacted.
 *
 * <p>When data files are compacted, existing position deletes that reference the old data files
 * need to be remapped to reference the new compacted files with updated row positions. This class
 * provides methods to check if remapping is needed and perform the remapping operation.
 *
 * <p>Example usage:
 *
 * <pre>
 * CompactionMap map = readCompactionMap(manifestFile);
 * PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);
 *
 * if (remapper.needsRemapping(deleteFile)) {
 *   List&lt;PositionDelete&lt;?&gt;&gt; remapped = remapper.remapDeletes(deleteFile, io);
 *   // Write remapped deletes to new delete file
 * }
 * </pre>
 */
public class PositionDeleteRemapper {
  private final CompactionMap compactionMap;
  private final Map<String, FileMapping> fileMappingIndex;

  /**
   * Creates a new remapper for the given compaction map.
   *
   * @param compactionMap the compaction map describing file transformations
   */
  public PositionDeleteRemapper(CompactionMap compactionMap) {
    this.compactionMap = compactionMap;
    this.fileMappingIndex = buildFileMappingIndex(compactionMap);
  }

  /**
   * Checks if a delete file contains position deletes that reference compacted files.
   *
   * @param deleteFile the delete file to check
   * @return true if any position deletes in this file need remapping
   */
  public boolean needsRemapping(DeleteFile deleteFile) {
    // If delete file specifies a single referenced data file, check that
    if (deleteFile.referencedDataFile() != null) {
      return fileMappingIndex.containsKey(deleteFile.referencedDataFile());
    }

    // For delete files that may reference multiple data files,
    // we can't determine without reading the file
    return false;
  }

  /**
   * Checks if a specific data file path is in the compaction map.
   *
   * @param dataFilePath the data file path to check
   * @return true if this file was compacted
   */
  public boolean isCompacted(String dataFilePath) {
    return fileMappingIndex.containsKey(dataFilePath);
  }

  /**
   * Returns the set of source file paths that were compacted.
   *
   * @return set of source file paths in the compaction map
   */
  public Set<String> compactedFiles() {
    return fileMappingIndex.keySet();
  }

  /**
   * Remaps a single position delete using the compaction map.
   *
   * <p>If the referenced file was not compacted, returns the original delete. If the position maps
   * to a new file, returns a remapped delete with the new file path and position.
   *
   * @param delete the position delete to remap
   * @return the remapped position delete, or the original if no remapping needed
   * @throws IllegalStateException if the file was compacted but the position is not found in any
   *     run
   */
  public PositionDelete<?> remapDelete(PositionDelete<?> delete) {
    String path = delete.path().toString();
    FileMapping mapping = fileMappingIndex.get(path);

    if (mapping == null) {
      // File was not compacted, return original delete
      return delete;
    }

    // Find the run containing this position
    CompactionMap.Run run = mapping.runForPosition(delete.pos());
    if (run == null) {
      // Position not found in any run - this indicates a problem with the compaction map
      throw new IllegalStateException(
          String.format(
              Locale.ROOT,
              "Position %d not found in compaction map for file %s. "
                  + "This indicates an incomplete or corrupted compaction map.",
              delete.pos(),
              path));
    }

    // Map the position using the run
    long newPosition = run.mapPosition(delete.pos());

    // Create remapped delete with new file and position
    return PositionDelete.create().set(mapping.targetFile(), newPosition, delete.row());
  }

  /**
   * Reads all position deletes from a delete file and checks if any need remapping.
   *
   * <p>This is a convenience method that reads the delete file and checks each delete against the
   * compaction map.
   *
   * @param deleteFilePath the path to the delete file
   * @param io the file IO for reading
   * @return set of data file paths that are both in this delete file and in the compaction map
   */
  public Set<String> findCompactedReferences(String deleteFilePath, FileIO io) {
    Set<String> compacted = Sets.newHashSet();
    InputFile inputFile = io.newInputFile(deleteFilePath);

    try (CloseableIterable<PositionDelete<?>> deletes = readPositionDeletes(inputFile)) {
      for (PositionDelete<?> delete : deletes) {
        String path = delete.path().toString();
        if (fileMappingIndex.containsKey(path)) {
          compacted.add(path);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read position deletes from: " + deleteFilePath, e);
    }

    return compacted;
  }

  /**
   * Returns the compaction map used by this remapper.
   *
   * @return the compaction map
   */
  public CompactionMap compactionMap() {
    return compactionMap;
  }

  private static Map<String, FileMapping> buildFileMappingIndex(CompactionMap map) {
    ImmutableMap.Builder<String, FileMapping> builder = ImmutableMap.builder();
    for (FileMapping mapping : map.fileMappings()) {
      builder.put(mapping.sourceFile(), mapping);
    }
    return builder.build();
  }

  @SuppressWarnings("UnusedVariable")
  private CloseableIterable<PositionDelete<?>> readPositionDeletes(InputFile inputFile) {
    // Read position deletes from the file
    // This will be implemented using Iceberg's existing readers
    // For now, throw as placeholder
    // TODO: Implement proper position delete reading using ParquetAvro or similar
    throw new UnsupportedOperationException(
        "Position delete reading not yet implemented - will be added in integration phase");
  }
}
