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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CompactionMap.FileMapping;
import org.apache.iceberg.deletes.DVPositionReader;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.ContentFileUtil;

/**
 * Utility for remapping position deletes when data files have been compacted.
 *
 * <p>When data files are compacted, existing position deletes that reference the old data files
 * need to be remapped to reference the new compacted files with updated row positions. This class
 * provides methods to check if remapping is needed and perform the remapping operation.
 *
 * <p><b>Handling Unmapped Positions:</b>
 *
 * <p>When a position delete references a position that doesn't exist in the compaction map, this
 * typically means one of:
 *
 * <ul>
 *   <li><b>Merge compaction:</b> The row was deleted during compaction (position deletes were
 *       applied during scan). The position no longer exists in the target file - this is normal and
 *       the delete can be safely dropped (it's a no-op).
 *   <li><b>Corrupted map:</b> The compaction map is incomplete or corrupted. This indicates an
 *       error condition.
 * </ul>
 *
 * <p>The remapping methods handle unmapped positions differently:
 *
 * <ul>
 *   <li>{@link #remapDelete(PositionDelete)} - throws {@link IllegalStateException} for unmapped
 *       positions (strict mode)
 *   <li>{@link #remapDeleteOrNull(PositionDelete)} - returns {@code null} for unmapped positions
 *       (lenient mode, recommended for merge compactions)
 *   <li>{@link #remapDVBulk(DeleteFile, FileIO)} - silently skips unmapped positions (lenient mode)
 * </ul>
 *
 * <p><b>Example usage:</b>
 *
 * <pre>
 * // Load compaction map from exception
 * CompactionMap map = CompactionMaps.read(fileIO.newInputFile(mapLocation));
 * PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);
 *
 * // Check if remapping is needed
 * if (remapper.needsRemapping(deleteFile)) {
 *   // Read and remap position deletes
 *   List&lt;PositionDelete&lt;?&gt;&gt; remapped = new ArrayList&lt;&gt;();
 *   for (PositionDelete&lt;?&gt; delete : readDeletes(deleteFile)) {
 *     PositionDelete&lt;?&gt; result = remapper.remapDeleteOrNull(delete);
 *     if (result != null) {
 *       remapped.add(result);
 *     }
 *     // null means position was filtered during compaction - can safely skip
 *   }
 *   // Write remapped deletes to new delete file
 * }
 * </pre>
 *
 * @see CompactionConflictException for how to get compaction map locations when a conflict occurs
 * @see CompactionMaps#read(InputFile) for loading compaction maps
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
   * Creates remappers from compaction map locations in a conflict exception.
   *
   * <p>This is a convenience method for the common pattern of loading compaction maps after
   * catching a {@link org.apache.iceberg.exceptions.CompactionConflictException}.
   *
   * <p><b>Example usage:</b>
   *
   * <pre>
   * try {
   *   rowDelta.commit();
   * } catch (CompactionConflictException e) {
   *   Map&lt;String, PositionDeleteRemapper&gt; remappers =
   *       PositionDeleteRemapper.fromConflict(e, table.io());
   *
   *   for (PositionDelete&lt;?&gt; delete : readDeletes(deleteFile)) {
   *     PositionDeleteRemapper remapper = remappers.get(delete.path().toString());
   *     if (remapper != null) {
   *       PositionDelete&lt;?&gt; remapped = remapper.remapDeleteOrNull(delete);
   *       if (remapped != null) {
   *         // Add to remapped deletes
   *       }
   *     } else {
   *       // File was not compacted, keep original delete
   *     }
   *   }
   *   // Retry with remapped deletes
   * }
   * </pre>
   *
   * @param conflict the conflict exception containing compaction map locations
   * @param io the file IO for reading compaction maps
   * @return map from source file path to its remapper (may share remappers for files in same
   *     compaction)
   */
  public static Map<String, PositionDeleteRemapper> fromConflict(
      org.apache.iceberg.exceptions.CompactionConflictException conflict, FileIO io) {
    Map<String, String> mapLocations = conflict.compactionMapLocations();
    Map<String, PositionDeleteRemapper> remappers = new HashMap<>();
    Map<String, PositionDeleteRemapper> mapLocationToRemapper = new HashMap<>();

    for (Map.Entry<String, String> entry : mapLocations.entrySet()) {
      String sourceFile = entry.getKey();
      String mapLocation = entry.getValue();

      // Reuse remapper if we've already loaded this map
      PositionDeleteRemapper remapper = mapLocationToRemapper.get(mapLocation);
      if (remapper == null) {
        CompactionMap map = CompactionMaps.read(io.newInputFile(mapLocation));
        remapper = new PositionDeleteRemapper(map);
        mapLocationToRemapper.put(mapLocation, remapper);
      }

      remappers.put(sourceFile, remapper);
    }

    return remappers;
  }

  /**
   * Checks if a delete file contains position deletes that reference compacted files.
   *
   * <p>For file-scoped position deletes (with {@code referencedDataFile} set), this method can
   * definitively determine if remapping is needed. For multi-file position deletes (without {@code
   * referencedDataFile}), this method returns {@code false} because the referenced files cannot be
   * determined without reading the delete file content.
   *
   * <p><b>Note:</b> If this returns {@code false} for a multi-file position delete file, you should
   * either:
   *
   * <ul>
   *   <li>Use {@link #mayNeedRemapping(DeleteFile)} which returns {@code true} for uncertain cases
   *   <li>Read the delete file content and use {@link #isCompacted(String)} to check each file path
   * </ul>
   *
   * @param deleteFile the delete file to check
   * @return true if position deletes definitely need remapping; false if they definitely don't OR
   *     if it cannot be determined without reading the file
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
   * Checks if a delete file may contain position deletes that reference compacted files.
   *
   * <p>This method is more conservative than {@link #needsRemapping(DeleteFile)}:
   *
   * <ul>
   *   <li>For file-scoped position deletes: returns true if the referenced file was compacted
   *   <li>For multi-file position deletes: returns true (may reference compacted files)
   *   <li>For equality deletes: returns false (not file-scoped)
   * </ul>
   *
   * <p>Use this method when you want to identify all delete files that might need processing, then
   * read their content to determine actual overlap.
   *
   * @param deleteFile the delete file to check
   * @return true if position deletes may need remapping (conservative)
   */
  public boolean mayNeedRemapping(DeleteFile deleteFile) {
    // Equality deletes don't reference specific files
    if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
      return false;
    }

    // File-scoped position deletes - check the referenced file
    if (deleteFile.referencedDataFile() != null) {
      return fileMappingIndex.containsKey(deleteFile.referencedDataFile());
    }

    // Multi-file position deletes - may reference compacted files
    // Must read content to determine
    return deleteFile.content() == FileContent.POSITION_DELETES;
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
   * Remaps a single position delete using the compaction map (strict mode).
   *
   * <p>If the referenced file was not compacted, returns the original delete. If the position maps
   * to a new file, returns a remapped delete with the new file path and position.
   *
   * <p><b>Note:</b> This method throws an exception if the position is not found in the compaction
   * map. Use {@link #remapDeleteOrNull(PositionDelete)} if you expect positions to be missing
   * (e.g., after merge compaction where rows were filtered).
   *
   * @param delete the position delete to remap
   * @return the remapped position delete, or the original if no remapping needed
   * @throws IllegalStateException if the file was compacted but the position is not found in any
   *     run (the row may have been filtered during merge compaction)
   */
  public PositionDelete<?> remapDelete(PositionDelete<?> delete) {
    PositionDelete<?> result = remapDeleteOrNull(delete);
    if (result == null) {
      throw new IllegalStateException(
          String.format(
              Locale.ROOT,
              "Position %d not found in compaction map for file %s. "
                  + "This may indicate the row was filtered during merge compaction, "
                  + "or the compaction map is incomplete. Use remapDeleteOrNull() to handle "
                  + "filtered rows gracefully.",
              delete.pos(),
              delete.path()));
    }
    return result;
  }

  /**
   * Remaps a single position delete using the compaction map (lenient mode).
   *
   * <p>If the referenced file was not compacted, returns the original delete. If the position maps
   * to a new file, returns a remapped delete with the new file path and position.
   *
   * <p>If the position is not found in the compaction map, returns {@code null}. This typically
   * happens when:
   *
   * <ul>
   *   <li>The row was deleted during merge compaction (position deletes were applied during scan)
   *   <li>The compaction map is incomplete or corrupted
   * </ul>
   *
   * <p><b>Recommendation:</b> Use this method when resolving conflicts after merge compaction, and
   * skip null results (they represent rows that no longer exist).
   *
   * @param delete the position delete to remap
   * @return the remapped position delete, the original if no remapping needed, or {@code null} if
   *     the position was not found in the compaction map
   */
  public PositionDelete<?> remapDeleteOrNull(PositionDelete<?> delete) {
    String path = delete.path().toString();
    FileMapping mapping = fileMappingIndex.get(path);

    if (mapping == null) {
      // File was not compacted, return original delete
      return delete;
    }

    // Find the run containing this position
    CompactionMap.Run run = mapping.runForPosition(delete.pos());
    if (run == null) {
      // Position not found in any run - row was likely filtered during merge compaction
      return null;
    }

    // Map the position using the run
    long newPosition = run.mapPosition(delete.pos());

    // Get target file: use per-run target if available, otherwise use mapping's default target
    String targetFile = run.targetFile() != null ? run.targetFile() : mapping.targetFile();

    // Create remapped delete with new file and position
    return PositionDelete.create().set(targetFile, newPosition, delete.row());
  }

  /**
   * Remaps positions in a deletion vector using bulk API for optimal performance.
   *
   * <p>This method collects all positions into memory and uses the bulk remapping API for
   * significant performance improvements (5-10x faster than one-by-one remapping).
   *
   * <p>Returns a map from target file path to set of deleted positions in that file. Multiple
   * target files are possible if the source file was split during compaction.
   *
   * <p>If the DV references a file that was not compacted, returns a single-entry map with the
   * original file and all positions.
   *
   * <p>For very large DVs (10M+ positions), consider memory constraints as all positions are loaded
   * into memory.
   *
   * @param dvFile the deletion vector file to remap
   * @param fileIO the file IO for reading the DV
   * @return map from target file path to set of deleted positions in that file
   * @throws IllegalArgumentException if dvFile is not a deletion vector
   * @throws IllegalStateException if DV is missing referencedDataFile
   */
  public Map<String, Set<Long>> remapDVBulk(DeleteFile dvFile, FileIO fileIO) {
    Preconditions.checkNotNull(dvFile, "dvFile is null");
    Preconditions.checkNotNull(fileIO, "fileIO is null");

    if (!ContentFileUtil.isDV(dvFile)) {
      throw new IllegalArgumentException("Not a deletion vector: " + dvFile.location());
    }

    String sourceFile = dvFile.referencedDataFile();
    if (sourceFile == null) {
      throw new IllegalStateException("DV missing referencedDataFile: " + dvFile.location());
    }

    FileMapping mapping = fileMappingIndex.get(sourceFile);

    if (mapping == null) {
      // DV references non-compacted file, return original mapping
      try {
        return Collections.singletonMap(sourceFile, readAllPositions(dvFile, fileIO));
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read DV: " + dvFile.location(), e);
      }
    }

    // Read deleted positions from DV into list
    DVPositionReader reader = new DVPositionReader(fileIO);
    List<Long> positions = new java.util.ArrayList<>();

    try (CloseableIterable<Long> positionIter = reader.readDeletedPositions(dvFile)) {
      positionIter.forEach(positions::add);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read DV: " + dvFile.location(), e);
    }

    if (positions.isEmpty()) {
      return Collections.emptyMap();
    }

    // Use smart selector to choose optimal strategy based on data characteristics
    // Selector considers: run count, position count, sortedness, gap ratio
    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, positions);
    Map<Long, CompactionMap.Run> mappedRuns = strategy.runForPositions(positions);

    // Group mapped positions by target file
    Map<String, Set<Long>> remappedPositions = new HashMap<>();

    for (Map.Entry<Long, CompactionMap.Run> entry : mappedRuns.entrySet()) {
      long sourcePos = entry.getKey();
      CompactionMap.Run run = entry.getValue();

      // Map to target position
      long targetPos = run.mapPosition(sourcePos);

      // Get target file: use per-run target if available, otherwise use mapping's default target
      String targetFile = run.targetFile() != null ? run.targetFile() : mapping.targetFile();

      // Add to result set for target file
      remappedPositions.computeIfAbsent(targetFile, k -> new HashSet<>()).add(targetPos);
    }

    return remappedPositions;
  }

  /**
   * Remaps positions in a deletion vector using the compaction map.
   *
   * <p>Returns a map from target file path to set of deleted positions in that file. Multiple
   * target files are possible if the source file was split during compaction.
   *
   * <p>If the DV references a file that was not compacted, returns a single-entry map with the
   * original file and all positions.
   *
   * @param dvFile the deletion vector file to remap
   * @param fileIO the file IO for reading the DV
   * @return map from target file path to set of deleted positions in that file
   * @throws IllegalArgumentException if dvFile is not a deletion vector
   * @throws IllegalStateException if DV is missing referencedDataFile
   * @deprecated Use {@link #remapDVBulk(DeleteFile, FileIO)} for better performance (5-10x faster).
   *     This method iterates positions one-by-one which is inefficient for large DVs.
   */
  @Deprecated
  public Map<String, Set<Long>> remapDV(DeleteFile dvFile, FileIO fileIO) {
    Preconditions.checkNotNull(dvFile, "dvFile is null");
    Preconditions.checkNotNull(fileIO, "fileIO is null");

    if (!ContentFileUtil.isDV(dvFile)) {
      throw new IllegalArgumentException("Not a deletion vector: " + dvFile.location());
    }

    String sourceFile = dvFile.referencedDataFile();
    if (sourceFile == null) {
      throw new IllegalStateException("DV missing referencedDataFile: " + dvFile.location());
    }

    FileMapping mapping = fileMappingIndex.get(sourceFile);

    if (mapping == null) {
      // DV references non-compacted file, return original mapping
      try {
        return Collections.singletonMap(sourceFile, readAllPositions(dvFile, fileIO));
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read DV: " + dvFile.location(), e);
      }
    }

    // Read deleted positions from DV and remap them
    DVPositionReader reader = new DVPositionReader(fileIO);
    Map<String, Set<Long>> remappedPositions = new HashMap<>();

    try (CloseableIterable<Long> positions = reader.readDeletedPositions(dvFile)) {
      for (Long sourcePos : positions) {
        // Find run containing this position
        CompactionMap.Run run = mapping.runForPosition(sourcePos);

        if (run == null) {
          // Position not in any run - it was already deleted in source
          continue;
        }

        // Map to target position
        long targetPos = run.mapPosition(sourcePos);

        // Get target file: use per-run target if available, otherwise use mapping's default target
        String targetFile = run.targetFile() != null ? run.targetFile() : mapping.targetFile();

        // Add to result set for target file
        remappedPositions.computeIfAbsent(targetFile, k -> new HashSet<>()).add(targetPos);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read DV: " + dvFile.location(), e);
    }

    return remappedPositions;
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

  private Set<Long> readAllPositions(DeleteFile dvFile, FileIO fileIO) throws IOException {
    DVPositionReader reader = new DVPositionReader(fileIO);
    Set<Long> positions = new HashSet<>();
    try (CloseableIterable<Long> iter = reader.readDeletedPositions(dvFile)) {
      iter.forEach(positions::add);
    }
    return positions;
  }
}
