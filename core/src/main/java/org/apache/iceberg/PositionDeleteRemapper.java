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
import org.apache.iceberg.util.SortedLongArraySet;

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
  private final CompactionMapChain chain;
  private final Map<String, FileMapping> fileMappingIndex;

  /**
   * Creates a new remapper for the given compaction map.
   *
   * @param compactionMap the compaction map describing file transformations
   */
  public PositionDeleteRemapper(CompactionMap compactionMap) {
    this.compactionMap = compactionMap;
    this.chain = null;
    this.fileMappingIndex = buildFileMappingIndex(compactionMap);
  }

  /**
   * Creates a new remapper for a chain of compaction maps.
   *
   * <p>Use this constructor when multiple sequential compactions have occurred and mappings need to
   * be composed through the chain. The chain handles lazy composition of mappings on demand.
   *
   * <p><b>Example usage:</b>
   *
   * <pre>
   * // Build chain from exception
   * CompactionMapChain chain = CompactionMapChain.build(chainedException.compactionMaps());
   * PositionDeleteRemapper remapper = new PositionDeleteRemapper(chain);
   *
   * // Remap deletes through the chain
   * PositionDelete&lt;?&gt; remapped = remapper.remapDeleteOrNull(delete);
   * </pre>
   *
   * @param chain the compaction map chain for multi-step remapping
   */
  public PositionDeleteRemapper(CompactionMapChain chain) {
    Preconditions.checkNotNull(chain, "chain cannot be null");
    Preconditions.checkArgument(chain.size() > 0, "chain cannot be empty");

    this.compactionMap = null;
    this.chain = chain;
    // Build index from all source files across the chain
    this.fileMappingIndex = buildFileMappingIndexFromChain(chain);
  }

  /**
   * Creates remappers from a conflict exception.
   *
   * <p>This is a convenience method for the common pattern of loading compaction maps after
   * catching a {@link org.apache.iceberg.exceptions.CompactionConflictException}. It handles both
   * single-map conflicts and chained compactions ({@link
   * org.apache.iceberg.exceptions.ChainedCompactionMapsException}) transparently.
   *
   * <p><b>Example usage:</b>
   *
   * <pre>
   * try {
   *   rowDelta.commit();
   * } catch (CompactionConflictException e) {
   *   // Handles both single and chained compactions
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
   * @param conflict the conflict exception containing compaction map locations (may be a {@link
   *     org.apache.iceberg.exceptions.ChainedCompactionMapsException} for chained compactions)
   * @param io the file IO for reading compaction maps
   * @return map from source file path to its remapper (may share remappers for files in same
   *     compaction)
   */
  public static Map<String, PositionDeleteRemapper> fromConflict(
      org.apache.iceberg.exceptions.CompactionConflictException conflict, FileIO io) {

    // Handle chained compactions: build a chain-based remapper for all affected files
    if (conflict instanceof org.apache.iceberg.exceptions.ChainedCompactionMapsException) {
      org.apache.iceberg.exceptions.ChainedCompactionMapsException chainedConflict =
          (org.apache.iceberg.exceptions.ChainedCompactionMapsException) conflict;
      CompactionMapChain chain = CompactionMapChain.build(chainedConflict.compactionMaps());
      PositionDeleteRemapper chainRemapper = new PositionDeleteRemapper(chain);

      Map<String, PositionDeleteRemapper> remappers = new HashMap<>();
      for (String sourceFile : chainedConflict.chainedFiles()) {
        remappers.put(sourceFile, chainRemapper);
      }

      return remappers;
    }

    // Single-map conflicts: load maps from locations
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
      return isCompacted(deleteFile.referencedDataFile());
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
      return isCompacted(deleteFile.referencedDataFile());
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
    if (chain != null) {
      return chain.containsSource(dataFilePath);
    }
    return fileMappingIndex.containsKey(dataFilePath);
  }

  /**
   * Returns the set of source file paths that were compacted.
   *
   * @return set of source file paths in the compaction map
   */
  public Set<String> compactedFiles() {
    if (chain != null) {
      return chain.sourceFiles();
    }
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
    FileMapping mapping = getMapping(path);

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

    FileMapping mapping = getMapping(sourceFile);

    if (mapping == null) {
      // DV references non-compacted file, return original mapping
      try {
        return Collections.singletonMap(sourceFile, readAllPositions(dvFile, fileIO));
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read DV: " + dvFile.location(), e);
      }
    }

    // Read deleted positions from DV using primitive extraction (avoids boxing)
    DVPositionReader reader = new DVPositionReader(fileIO);
    long[] positions = reader.readDeletedPositionsPrimitive(dvFile);

    if (positions.length == 0) {
      return Collections.emptyMap();
    }

    // Use primitive bulk remapping and wrap result in SortedLongArraySet for API compatibility
    Map<String, long[]> primitiveResult = remapPositionsBulkPrimitive(sourceFile, positions);

    // Convert to Set-based result for backward compatibility
    Map<String, Set<Long>> result = new HashMap<>();
    for (Map.Entry<String, long[]> entry : primitiveResult.entrySet()) {
      result.put(entry.getKey(), new SortedLongArraySet(entry.getValue()));
    }
    return result;
  }

  /**
   * Remaps positions from a source file using the compaction map with bulk operations.
   *
   * <p>This is the recommended API for remapping positions when you have them in memory (e.g., from
   * a RoaringBitmap or other position collection). It provides the same performance benefits as
   * {@link #remapDVBulk(DeleteFile, FileIO)} but doesn't require constructing a DeleteFile object.
   *
   * <p>If the source file is not in the compaction map (not compacted), returns a single-entry map
   * with the original file and all input positions.
   *
   * <p>Positions that fall in gaps between runs (e.g., positions that were deleted during merge
   * compaction) are silently dropped.
   *
   * @param sourceFile the path of the source data file that was compacted
   * @param positions the positions to remap (should be from the source file)
   * @return map from target file path to set of remapped positions in that file
   */
  public Map<String, Set<Long>> remapPositionsBulk(String sourceFile, Iterable<Long> positions) {
    Preconditions.checkNotNull(sourceFile, "sourceFile is null");
    Preconditions.checkNotNull(positions, "positions is null");

    // Collect positions into list for size calculation
    List<Long> positionList = new java.util.ArrayList<>();
    positions.forEach(positionList::add);

    if (positionList.isEmpty()) {
      return Collections.emptyMap();
    }

    // Convert to primitive array
    long[] primitivePositions = new long[positionList.size()];
    for (int i = 0; i < positionList.size(); i++) {
      primitivePositions[i] = positionList.get(i);
    }

    FileMapping mapping = getMapping(sourceFile);

    if (mapping == null) {
      // File wasn't compacted, return original positions (sorted)
      java.util.Arrays.sort(primitivePositions);
      return Collections.singletonMap(sourceFile, new SortedLongArraySet(primitivePositions));
    }

    // Use smart selector to choose optimal strategy (primitive API)
    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, primitivePositions);
    Map<Long, CompactionMap.Run> mappedRuns = strategy.runForPositions(primitivePositions);

    return remapPositionsBulkInternal(mappedRuns, mapping);
  }

  /**
   * Remaps positions from a source file using primitive arrays for maximum efficiency.
   *
   * <p>This method avoids boxing overhead by working directly with primitive long arrays. It's
   * optimized for callers that already have positions in primitive form (e.g., extracted from
   * RoaringBitmap).
   *
   * <p>The returned arrays are sorted in ascending order, ready for direct use in building
   * RoaringBitmaps or other position-indexed structures.
   *
   * <p><b>Performance:</b> For 1M positions, this method is ~10-15x faster than the boxed version
   * due to:
   *
   * <ul>
   *   <li>No boxing/unboxing overhead (saves ~16 bytes per position)
   *   <li>No Set wrapper construction (returns raw arrays)
   *   <li>Cache-friendly sequential array access
   * </ul>
   *
   * @param sourceFile the path of the source data file that was compacted
   * @param positions the positions to remap as a primitive array
   * @return map from target file path to sorted array of remapped positions
   */
  public Map<String, long[]> remapPositionsBulkPrimitive(String sourceFile, long[] positions) {
    Preconditions.checkNotNull(sourceFile, "sourceFile is null");
    Preconditions.checkNotNull(positions, "positions is null");

    if (positions.length == 0) {
      return Collections.emptyMap();
    }

    FileMapping mapping = getMapping(sourceFile);

    if (mapping == null) {
      // File wasn't compacted, return original positions (sorted)
      long[] sorted = positions.clone();
      java.util.Arrays.sort(sorted);
      return Collections.singletonMap(sourceFile, sorted);
    }

    // Use smart selector to choose optimal strategy
    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, positions);

    // Use parallel array API to avoid boxing and HashMap overhead
    CompactionMap.Run[] runs = strategy.runsForPositions(positions);

    return remapWithParallelArrays(positions, runs, mapping);
  }

  /**
   * Remaps positions from a V3 Deletion Vector, staying in {@code int} throughout the output path.
   *
   * <p>DV sources have two structural guarantees the general primitive path doesn't:
   *
   * <ul>
   *   <li>Positions are sorted (Roaring sub-bitmaps yield values in ascending order), so the
   *       selector can skip its sortedness sample.
   *   <li>Positions fit in 32 bits, so the remapped output can be materialized directly as {@code
   *       int[]} per target without going through {@code long[]} narrowing.
   * </ul>
   *
   * <p>The algorithm itself still operates in long (driven by {@link CompactionMap.Run}'s long
   * source/target positions), so a transient widening happens at entry. The win is on the output
   * side: each target array is built as {@code int[]} directly, avoiding both the long[] output
   * allocation and the subsequent narrowing loop that callers used to write themselves.
   *
   * @param sourceFile path of the compacted source data file
   * @param positions sorted source positions extracted from a DV
   * @return map from target file path to sorted {@code int[]} of remapped positions
   */
  public Map<String, int[]> remapPositionsBulkDV(String sourceFile, int[] positions) {
    Preconditions.checkNotNull(sourceFile, "sourceFile is null");
    Preconditions.checkNotNull(positions, "positions is null");

    if (positions.length == 0) {
      return Collections.emptyMap();
    }

    FileMapping mapping = getMapping(sourceFile);

    if (mapping == null) {
      // File wasn't compacted; return original positions (already sorted on the DV).
      int[] copy = positions.clone();
      return Collections.singletonMap(sourceFile, copy);
    }

    // Widen for the algorithm. This is the one transient long[] that remains — eliminating it
    // would require int-typed strategy implementations, which is out of scope here.
    long[] widened = new long[positions.length];
    for (int i = 0; i < positions.length; i++) {
      widened[i] = positions[i] & 0xFFFFFFFFL;
    }

    RemappingAlgorithmSelector selector = new RemappingAlgorithmSelector();
    RemappingStrategy strategy = selector.selectOptimal(mapping, widened, Boolean.TRUE);
    CompactionMap.Run[] runs = strategy.runsForPositions(widened);

    return remapWithParallelArraysInt(widened, runs, mapping);
  }

  /**
   * Like {@link #remapWithParallelArrays} but writes target positions directly into {@code int[]}
   * outputs instead of allocating {@code long[]} per target and then forcing callers to narrow.
   */
  private Map<String, int[]> remapWithParallelArraysInt(
      long[] positions, CompactionMap.Run[] runs, FileMapping mapping) {

    Map<String, Integer> countsByFile = new HashMap<>();
    String defaultTarget = mapping.targetFile();

    for (int i = 0; i < positions.length; i++) {
      CompactionMap.Run run = runs[i];
      if (run != null) {
        String targetFile = run.targetFile() != null ? run.targetFile() : defaultTarget;
        countsByFile.merge(targetFile, 1, Integer::sum);
      }
    }

    if (countsByFile.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, int[]> resultArrays = new HashMap<>();
    Map<String, Integer> writeIndices = new HashMap<>();
    for (Map.Entry<String, Integer> entry : countsByFile.entrySet()) {
      resultArrays.put(entry.getKey(), new int[entry.getValue()]);
      writeIndices.put(entry.getKey(), 0);
    }

    for (int i = 0; i < positions.length; i++) {
      CompactionMap.Run run = runs[i];
      if (run != null) {
        String targetFile = run.targetFile() != null ? run.targetFile() : defaultTarget;
        long sourcePos = positions[i];
        long targetPos = run.targetPosition() + (sourcePos - run.sourcePosition());

        int[] targetArray = resultArrays.get(targetFile);
        int idx = writeIndices.get(targetFile);
        targetArray[idx] = (int) targetPos;
        writeIndices.put(targetFile, idx + 1);
      }
    }

    for (int[] arr : resultArrays.values()) {
      java.util.Arrays.sort(arr);
    }

    return resultArrays;
  }

  /**
   * Remaps positions from a PositionDeleteIndex using the compaction map.
   *
   * <p>This overload accepts a {@link PositionDeleteIndex}, which is the same interface used
   * internally by {@link #remapDVBulk(DeleteFile, FileIO)}. Use this when you have positions in a
   * PositionDeleteIndex (e.g., from a BitmapPositionDeleteIndex) but don't have a full DeleteFile.
   *
   * <p>This is useful for benchmarks that want to test the remapping algorithm using the same
   * interface as production code, without the I/O overhead of reading from storage.
   *
   * @param sourceFile the path of the source data file that was compacted
   * @param positions the position delete index containing positions to remap
   * @return map from target file path to sorted array of remapped positions
   */
  public Map<String, long[]> remapPositionsBulkPrimitive(
      String sourceFile, org.apache.iceberg.deletes.PositionDeleteIndex positions) {
    Preconditions.checkNotNull(sourceFile, "sourceFile is null");
    Preconditions.checkNotNull(positions, "positions is null");

    // Extract positions from index
    long cardinality = positions.cardinality();
    if (cardinality == 0) {
      return Collections.emptyMap();
    }

    if (cardinality > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "Position count exceeds max array size: %d (max: %d)",
              cardinality,
              Integer.MAX_VALUE));
    }

    long[] positionArray = new long[(int) cardinality];
    int[] idx = {0};
    positions.forEach(pos -> positionArray[idx[0]++] = pos);

    return remapPositionsBulkPrimitive(sourceFile, positionArray);
  }

  /**
   * Remaps positions using parallel arrays (positions[i] -> runs[i]).
   *
   * <p>This method avoids the boxing and HashMap overhead of the Map-based approach by working
   * directly with parallel primitive arrays. For 1M positions, this is approximately 3-5x faster.
   *
   * <p>Algorithm:
   *
   * <ol>
   *   <li>First pass: count positions per target file (for pre-sizing)
   *   <li>Second pass: compute target positions and store in pre-sized arrays
   *   <li>Sort each result array
   * </ol>
   *
   * @param positions source positions (parallel with runs array)
   * @param runs runs for each position (null = gap, position filtered)
   * @param mapping file mapping for default target file
   * @return map from target file to sorted array of remapped positions
   */
  private Map<String, long[]> remapWithParallelArrays(
      long[] positions, CompactionMap.Run[] runs, FileMapping mapping) {

    // First pass: count positions per target file for pre-sizing arrays
    Map<String, Integer> countsByFile = new HashMap<>();
    String defaultTarget = mapping.targetFile();

    for (int i = 0; i < positions.length; i++) {
      CompactionMap.Run run = runs[i];
      if (run != null) {
        String targetFile = run.targetFile() != null ? run.targetFile() : defaultTarget;
        countsByFile.merge(targetFile, 1, Integer::sum);
      }
    }

    if (countsByFile.isEmpty()) {
      return Collections.emptyMap();
    }

    // Allocate result arrays
    Map<String, long[]> resultArrays = new HashMap<>();
    Map<String, Integer> writeIndices = new HashMap<>();
    for (Map.Entry<String, Integer> entry : countsByFile.entrySet()) {
      resultArrays.put(entry.getKey(), new long[entry.getValue()]);
      writeIndices.put(entry.getKey(), 0);
    }

    // Second pass: compute target positions and write to arrays
    for (int i = 0; i < positions.length; i++) {
      CompactionMap.Run run = runs[i];
      if (run != null) {
        String targetFile = run.targetFile() != null ? run.targetFile() : defaultTarget;
        long sourcePos = positions[i];

        // targetPos = targetPosition + (sourcePos - sourcePosition)
        long targetPos = run.targetPosition() + (sourcePos - run.sourcePosition());

        long[] targetArray = resultArrays.get(targetFile);
        int idx = writeIndices.get(targetFile);
        targetArray[idx] = targetPos;
        writeIndices.put(targetFile, idx + 1);
      }
    }

    // Sort each result array
    for (long[] arr : resultArrays.values()) {
      java.util.Arrays.sort(arr);
    }

    return resultArrays;
  }

  /**
   * Internal bulk remapping implementation.
   *
   * <p>Instead of adding positions one-by-one to HashSets, this method:
   *
   * <ol>
   *   <li>Groups source positions by their containing run
   *   <li>Computes target positions in bulk per run (simple offset arithmetic)
   *   <li>Wraps sorted arrays in SortedLongArraySet (avoids HashSet construction overhead)
   * </ol>
   *
   * <p>Performance: ~8x faster than HashSet-based approach for large position counts. At 1M
   * positions: ~16ms vs ~137ms (from JMH benchmarks).
   */
  private Map<String, Set<Long>> remapPositionsBulkInternal(
      Map<Long, CompactionMap.Run> mappedRuns, FileMapping mapping) {

    // Step 1: Group source positions by (targetFile, run) for bulk processing
    // Using run identity as key since runs are interned in the compaction map
    Map<String, Map<CompactionMap.Run, List<Long>>> positionsByFileAndRun = new HashMap<>();

    for (Map.Entry<Long, CompactionMap.Run> entry : mappedRuns.entrySet()) {
      long sourcePos = entry.getKey();
      CompactionMap.Run run = entry.getValue();

      String targetFile = run.targetFile() != null ? run.targetFile() : mapping.targetFile();

      positionsByFileAndRun
          .computeIfAbsent(targetFile, k -> new HashMap<>())
          .computeIfAbsent(run, k -> new java.util.ArrayList<>())
          .add(sourcePos);
    }

    // Step 2: Build result sets using SortedLongArraySet (avoids expensive HashSet construction)
    Map<String, Set<Long>> result = new HashMap<>();

    for (Map.Entry<String, Map<CompactionMap.Run, List<Long>>> fileEntry :
        positionsByFileAndRun.entrySet()) {
      String targetFile = fileEntry.getKey();

      // Count total positions for this file to pre-size the array
      int totalPositions = fileEntry.getValue().values().stream().mapToInt(List::size).sum();

      // Collect all target positions into a primitive array
      long[] targetPositions = new long[totalPositions];
      int idx = 0;

      for (Map.Entry<CompactionMap.Run, List<Long>> runEntry : fileEntry.getValue().entrySet()) {
        CompactionMap.Run run = runEntry.getKey();
        List<Long> sourcePositions = runEntry.getValue();

        // Bulk transform: targetPos = targetPosition + (sourcePos - sourcePosition)
        // This is a simple offset calculation that can be done efficiently
        long offset = run.targetPosition() - run.sourcePosition();

        for (Long sourcePos : sourcePositions) {
          targetPositions[idx++] = sourcePos + offset;
        }
      }

      // Sort the array - positions will be iterated in sorted order for DV writing
      java.util.Arrays.sort(targetPositions);

      // Use SortedLongArraySet instead of HashSet
      // This avoids O(n) HashSet construction with expensive hashing/boxing overhead
      // The set is immutable and uses O(log n) binary search for contains()
      result.put(targetFile, new SortedLongArraySet(targetPositions));
    }

    return result;
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

    FileMapping mapping = getMapping(sourceFile);

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
   * Returns the compaction map used by this remapper, or null if using a chain.
   *
   * @return the compaction map, or null if using a chain
   */
  public CompactionMap compactionMap() {
    return compactionMap;
  }

  /**
   * Returns the compaction map chain used by this remapper, or null if using a single map.
   *
   * @return the compaction map chain, or null if using a single map
   */
  public CompactionMapChain chain() {
    return chain;
  }

  private static Map<String, FileMapping> buildFileMappingIndex(CompactionMap map) {
    ImmutableMap.Builder<String, FileMapping> builder = ImmutableMap.builder();
    for (FileMapping mapping : map.fileMappings()) {
      builder.put(mapping.sourceFile(), mapping);
    }
    return builder.build();
  }

  private static Map<String, FileMapping> buildFileMappingIndexFromChain(CompactionMapChain chain) {
    // For chains, we start with a placeholder index that just tracks which files are in the chain
    // Actual mappings are resolved lazily through the chain
    Map<String, FileMapping> index = new HashMap<>();
    for (String sourceFile : chain.sourceFiles()) {
      // Put a placeholder - actual mapping will be fetched from chain when needed
      index.put(sourceFile, null);
    }
    return index;
  }

  /**
   * Gets the file mapping for a source file, using the chain if available.
   *
   * @param sourceFile the source file path
   * @return the file mapping, or null if not found
   */
  private FileMapping getMapping(String sourceFile) {
    if (chain != null) {
      // Use chain to get composed mapping
      return chain.mappingForFile(sourceFile);
    }
    return fileMappingIndex.get(sourceFile);
  }

  private Set<Long> readAllPositions(DeleteFile dvFile, FileIO fileIO) throws IOException {
    DVPositionReader reader = new DVPositionReader(fileIO);
    List<Long> positionList = new java.util.ArrayList<>();
    try (CloseableIterable<Long> iter = reader.readDeletedPositions(dvFile)) {
      iter.forEach(positionList::add);
    }

    // Convert to primitive array and wrap in SortedLongArraySet
    long[] positions = new long[positionList.size()];
    for (int i = 0; i < positionList.size(); i++) {
      positions[i] = positionList.get(i);
    }
    // Positions from DV are already sorted, but sort anyway for safety
    java.util.Arrays.sort(positions);
    return new SortedLongArraySet(positions);
  }
}
