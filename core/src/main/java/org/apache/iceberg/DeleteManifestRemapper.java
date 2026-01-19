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
import org.apache.iceberg.CompactionMap.FileMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for remapping position delete records using compaction maps.
 *
 * <p>When compaction conflicts with delete transactions, this class remaps position deletes from
 * source files to target files using the compaction map. Deletes are grouped by target file for
 * efficient writing.
 *
 * <p>Edge cases handled:
 *
 * <ul>
 *   <li>File not compacted: delete is skipped (tracked in metrics)
 *   <li>Position filtered during compaction: delete is skipped (idempotent)
 *   <li>Invalid (negative) positions: delete is skipped with warning
 *   <li>Duplicate deletes: deduplicated (same file/position)
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>
 * List&lt;PositionDeleteRecord&gt; deletes = readDeleteManifests(deleteFiles, io);
 * CompactionMap map = readCompactionMap(compactionMapLocation);
 *
 * DeleteManifestRemapper remapper = new DeleteManifestRemapper(map);
 * RemappingResult result = remapper.remapDeletesWithMetrics(deletes);
 *
 * // Check metrics
 * LOG.info("Remapped: {}, Skipped: {}", result.totalRemapped(), result.totalSkipped());
 *
 * // Write remapped deletes
 * Map&lt;String, List&lt;PositionDeleteRecord&gt;&gt; remapped = result.remappedDeletes();
 * </pre>
 */
public class DeleteManifestRemapper {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteManifestRemapper.class);

  private final CompactionMap compactionMap;
  private final Map<String, FileMapping> fileMappingIndex;

  /**
   * Creates a new remapper for the given compaction map.
   *
   * @param compactionMap the compaction map describing file transformations
   */
  public DeleteManifestRemapper(CompactionMap compactionMap) {
    Preconditions.checkNotNull(compactionMap, "compactionMap is null");
    this.compactionMap = compactionMap;
    this.fileMappingIndex = buildFileMappingIndex(compactionMap);
  }

  /**
   * Remaps position delete records using the compaction map.
   *
   * <p>This method processes each delete record and remaps it from the source file to the target
   * file using the compaction map. Deletes are grouped by target file for efficient writing.
   *
   * <p>Behavior:
   *
   * <ul>
   *   <li>Deletes on rows that were filtered out during compaction are dropped (gaps in runs)
   *   <li>Multiple source files → same target file: deletes are merged
   *   <li>Partition and row data are preserved in remapped deletes
   *   <li>Empty delete lists return empty map
   * </ul>
   *
   * @param deletes list of position delete records to remap
   * @return map from target file path to list of remapped position delete records
   */
  public Map<String, List<PositionDeleteRecord>> remapDeletes(List<PositionDeleteRecord> deletes) {
    return remapDeletesWithMetrics(deletes).remappedDeletes();
  }

  /**
   * Remaps position delete records with detailed metrics about the remapping process.
   *
   * <p>This method provides the same functionality as {@link #remapDeletes(List)} but also returns
   * metrics about skipped deletes, which is useful for logging and debugging.
   *
   * @param deletes list of position delete records to remap
   * @return result containing remapped deletes and metrics
   */
  public RemappingResult remapDeletesWithMetrics(List<PositionDeleteRecord> deletes) {
    Preconditions.checkNotNull(deletes, "deletes is null");

    if (deletes.isEmpty()) {
      return RemappingResult.empty();
    }

    Map<String, List<PositionDeleteRecord>> remappedByTarget = Maps.newHashMap();
    Set<String> seenDeletes = Sets.newHashSet(); // For deduplication

    int skippedNotCompacted = 0;
    int skippedFilteredRows = 0;
    int skippedInvalidPositions = 0;
    int duplicatesRemoved = 0;

    for (PositionDeleteRecord delete : deletes) {
      String sourceFile = delete.dataFilePath();
      long position = delete.position();

      // Validate position
      if (position < 0) {
        LOG.warn(
            "Skipping delete with invalid negative position: file={}, position={}",
            sourceFile,
            position);
        skippedInvalidPositions++;
        continue;
      }

      FileMapping mapping = fileMappingIndex.get(sourceFile);

      if (mapping == null) {
        // File was not compacted - this shouldn't happen in normal use
        // since we filter deletes by compacted files, but we handle it gracefully
        skippedNotCompacted++;
        continue;
      }

      // Find the run containing this position
      CompactionMap.Run run = mapping.runForPosition(position);

      if (run == null) {
        // Position not found in any run - this means the row was filtered out during compaction
        // Drop the delete silently (idempotent - row doesn't exist in target file)
        skippedFilteredRows++;
        continue;
      }

      // Map the position using the run
      long newPosition = run.mapPosition(position);
      String targetFile = mapping.targetFile();

      // Check for duplicates
      String deleteKey = targetFile + ":" + newPosition;
      if (!seenDeletes.add(deleteKey)) {
        duplicatesRemoved++;
        continue;
      }

      // Create remapped delete record
      PositionDeleteRecord remappedDelete =
          new PositionDeleteRecord(
              targetFile, newPosition, delete.partitionData(), delete.rowData());

      // Add to target file's list
      remappedByTarget.computeIfAbsent(targetFile, k -> Lists.newArrayList()).add(remappedDelete);
    }

    // Log summary if any deletes were skipped
    int totalSkipped = skippedNotCompacted + skippedFilteredRows + skippedInvalidPositions;
    if (totalSkipped > 0 || duplicatesRemoved > 0) {
      LOG.debug(
          "Remapping summary: remapped={}, skippedNotCompacted={}, skippedFilteredRows={}, "
              + "skippedInvalidPositions={}, duplicatesRemoved={}",
          remappedByTarget.values().stream().mapToInt(List::size).sum(),
          skippedNotCompacted,
          skippedFilteredRows,
          skippedInvalidPositions,
          duplicatesRemoved);
    }

    return RemappingResult.builder()
        .remappedDeletes(remappedByTarget)
        .skippedNotCompacted(skippedNotCompacted)
        .skippedFilteredRows(skippedFilteredRows)
        .skippedInvalidPositions(skippedInvalidPositions)
        .duplicatesRemoved(duplicatesRemoved)
        .build();
  }

  /**
   * Checks if a data file was compacted according to this compaction map.
   *
   * @param dataFilePath the data file path to check
   * @return true if this file was compacted
   */
  public boolean isCompacted(String dataFilePath) {
    return fileMappingIndex.containsKey(dataFilePath);
  }

  /**
   * Returns the compaction map used by this remapper.
   *
   * @return the compaction map
   */
  public CompactionMap getCompactionMap() {
    return compactionMap;
  }

  /**
   * Returns the number of source files in the compaction map.
   *
   * @return source file count
   */
  public int sourceFileCount() {
    return fileMappingIndex.size();
  }

  private static Map<String, FileMapping> buildFileMappingIndex(CompactionMap compactionMap) {
    Map<String, FileMapping> index = Maps.newHashMap();
    for (FileMapping mapping : compactionMap.fileMappings()) {
      index.put(mapping.sourceFile(), mapping);
    }
    return index;
  }
}
