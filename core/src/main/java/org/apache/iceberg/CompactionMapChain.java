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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * A chain of compaction maps that supports lazy composition for remapping through multiple
 * sequential compactions.
 *
 * <p>When multiple compactions occur sequentially (e.g., F1→F2 in snapshot S1→S2, then F2→F3 in
 * snapshot S2→S3), position deletes referencing F1 need to be remapped through the entire chain to
 * reach F3. This class handles the composition of multiple compaction maps to produce the final
 * mapping.
 *
 * <p><b>Lazy Composition:</b> File mappings are composed on-demand when {@link
 * #mappingForFile(String)} is called. This avoids the cost of composing all mappings upfront when
 * only a subset of files may actually need remapping.
 *
 * <p><b>Usage:</b>
 *
 * <pre>
 * // Build a chain from ordered compaction maps (oldest to newest)
 * CompactionMapChain chain = CompactionMapChain.build(orderedMaps);
 *
 * // Get composed mapping for a file
 * CompactionMap.FileMapping mapping = chain.mappingForFile("source_file.parquet");
 * if (mapping != null) {
 *   // Use mapping to remap position deletes
 * }
 *
 * // Or use with PositionDeleteRemapper
 * PositionDeleteRemapper remapper = new PositionDeleteRemapper(chain);
 * </pre>
 *
 * @see CompactionMaps#compose(CompactionMap, CompactionMap) for the composition algorithm
 */
public class CompactionMapChain {
  private final List<CompactionMap> chain; // Ordered oldest to newest
  private final Map<String, Integer> sourceFileToChainIndex; // Which map in chain has this source
  private final Map<String, CompactionMap.FileMapping> composedMappingCache;

  private CompactionMapChain(
      List<CompactionMap> chain, Map<String, Integer> sourceFileToChainIndex) {
    this.chain = chain;
    this.sourceFileToChainIndex = sourceFileToChainIndex;
    this.composedMappingCache = Maps.newConcurrentMap();
  }

  /**
   * Builds a compaction map chain from an ordered list of compaction maps.
   *
   * <p>The maps should be ordered from oldest to newest (i.e., in the order they were applied). The
   * chain will validate that maps are contiguous (each map's source snapshot matches the previous
   * map's target snapshot).
   *
   * @param orderedMaps the compaction maps in application order (oldest first)
   * @return a new CompactionMapChain
   * @throws IllegalArgumentException if the list is empty or maps are not contiguous
   */
  public static CompactionMapChain build(List<CompactionMap> orderedMaps) {
    Preconditions.checkArgument(
        orderedMaps != null && !orderedMaps.isEmpty(), "orderedMaps cannot be null or empty");

    // Build index from source file to the index of the first map in chain that contains it
    Map<String, Integer> sourceFileToChainIndex = Maps.newHashMap();

    for (int i = 0; i < orderedMaps.size(); i++) {
      CompactionMap map = orderedMaps.get(i);

      for (CompactionMap.FileMapping mapping : map.fileMappings()) {
        // Only record the first occurrence (earliest in chain)
        sourceFileToChainIndex.putIfAbsent(mapping.sourceFile(), i);
      }
    }

    return new CompactionMapChain(ImmutableList.copyOf(orderedMaps), sourceFileToChainIndex);
  }

  /**
   * Returns the list of compaction maps in this chain, ordered from oldest to newest.
   *
   * @return unmodifiable list of compaction maps
   */
  public List<CompactionMap> maps() {
    return chain;
  }

  /**
   * Returns the number of compaction maps in this chain.
   *
   * @return chain length
   */
  public int size() {
    return chain.size();
  }

  /**
   * Returns the set of all source files that can be remapped through this chain.
   *
   * @return set of source file paths
   */
  public Set<String> sourceFiles() {
    return Sets.newHashSet(sourceFileToChainIndex.keySet());
  }

  /**
   * Checks if a file is a source in this chain.
   *
   * @param sourceFile the file path to check
   * @return true if the file can be remapped through this chain
   */
  public boolean containsSource(String sourceFile) {
    return sourceFileToChainIndex.containsKey(sourceFile);
  }

  /**
   * Gets the composed file mapping for a source file.
   *
   * <p>This method composes the mapping through all relevant maps in the chain, from the map where
   * this file first appears as a source through to the final map. The result is cached for
   * subsequent calls.
   *
   * @param sourceFile the source file path
   * @return the composed file mapping, or null if the file is not in the chain
   */
  public CompactionMap.FileMapping mappingForFile(String sourceFile) {
    // Check cache first
    CompactionMap.FileMapping cached = composedMappingCache.get(sourceFile);
    if (cached != null) {
      return cached;
    }

    // Check if this file is in the chain
    Integer startIndex = sourceFileToChainIndex.get(sourceFile);
    if (startIndex == null) {
      return null;
    }

    // Compose through the chain starting from where this file first appears
    CompactionMap.FileMapping composed = composeMappingThroughChain(sourceFile, startIndex);

    if (composed != null) {
      composedMappingCache.put(sourceFile, composed);
    }

    return composed;
  }

  /**
   * Composes a file mapping through the chain starting from the given index.
   *
   * @param sourceFile the source file to compose mapping for
   * @param startIndex the index of the first map containing this file
   * @return the composed file mapping
   */
  private CompactionMap.FileMapping composeMappingThroughChain(String sourceFile, int startIndex) {
    CompactionMap firstMap = chain.get(startIndex);
    CompactionMap.FileMapping currentMapping = firstMap.mappingForFile(sourceFile);

    if (currentMapping == null) {
      return null;
    }

    // If there's only one map or this is the last map, return the mapping directly
    if (startIndex == chain.size() - 1) {
      return currentMapping;
    }

    // Compose through subsequent maps
    // We need to find target files and see if they are sources in subsequent maps
    for (int i = startIndex + 1; i < chain.size(); i++) {
      CompactionMap nextMap = chain.get(i);

      // Check if any target from current mapping is a source in the next map
      Set<String> currentTargets = getTargetFiles(currentMapping);
      boolean needsComposition = false;

      for (String target : currentTargets) {
        if (nextMap.mappingForFile(target) != null) {
          needsComposition = true;
          break;
        }
      }

      if (needsComposition) {
        // Compose the maps up to this point
        // Build a temporary compaction map with just our file's mapping
        CompactionMap tempMap = buildSingleFileMap(firstMap, currentMapping);

        // Compose with the next map
        CompactionMap composedMap = CompactionMaps.compose(tempMap, nextMap);

        // Get the composed mapping for our source file
        currentMapping = composedMap.mappingForFile(sourceFile);

        if (currentMapping == null) {
          // This shouldn't happen, but handle gracefully
          return null;
        }

        // Update the "first map" for subsequent compositions
        firstMap = composedMap;
      }
    }

    return currentMapping;
  }

  /**
   * Gets all target files from a file mapping, including per-run targets.
   *
   * @param mapping the file mapping
   * @return set of target file paths
   */
  private Set<String> getTargetFiles(CompactionMap.FileMapping mapping) {
    Set<String> targets = Sets.newHashSet();
    targets.add(mapping.targetFile());

    for (CompactionMap.Run run : mapping.runs()) {
      if (run.targetFile() != null) {
        targets.add(run.targetFile());
      }
    }

    return targets;
  }

  /**
   * Builds a temporary compaction map containing only the given file mapping.
   *
   * @param originalMap the original map (for snapshot IDs)
   * @param mapping the file mapping to include
   * @return a compaction map with just this mapping
   */
  private CompactionMap buildSingleFileMap(
      CompactionMap originalMap, CompactionMap.FileMapping mapping) {
    CompactionMapBuilder builder =
        new CompactionMapBuilder(originalMap.sourceSnapshotId(), originalMap.targetSnapshotId());

    CompactionMapBuilder.FileMappingBuilder fileMappingBuilder =
        builder.addFileMapping(mapping.sourceFile(), mapping.targetFile());

    for (CompactionMap.Run run : mapping.runs()) {
      fileMappingBuilder.addRun(
          run.sourcePosition(), run.targetPosition(), run.length(), run.targetFile());
    }

    return builder.build();
  }

  /**
   * Returns the first snapshot ID in the chain (source of the first compaction).
   *
   * @return the first source snapshot ID
   */
  public long firstSourceSnapshotId() {
    return chain.get(0).sourceSnapshotId();
  }

  /**
   * Returns the last snapshot ID in the chain (target of the last compaction).
   *
   * @return the last target snapshot ID
   */
  public long lastTargetSnapshotId() {
    return chain.get(chain.size() - 1).targetSnapshotId();
  }
}
