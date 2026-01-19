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
package org.apache.iceberg.spark.actions;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.DeleteConflictInfo;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PositionDeleteRemapper;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.PositionDeletesScanTasks;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.PositionDeletesRewriteCoordinator;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkTableCache;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves compaction conflicts with position deletes using Spark infrastructure.
 *
 * <p>When a compaction operation detects that concurrent transactions have added position deletes
 * referencing the files being compacted, this resolver remaps those deletes to reference the new
 * compacted files using the compaction map.
 *
 * <p>The resolution flow:
 *
 * <ol>
 *   <li>Read position deletes from conflicting delete files using Spark
 *   <li>Filter deletes to only those referencing compacted source files
 *   <li>Remap positions using the compaction map via {@link PositionDeleteRemapper}
 *   <li>Write new delete files with remapped positions
 * </ol>
 */
public class SparkCompactionConflictResolver implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SparkCompactionConflictResolver.class);

  private final SparkSession spark;
  private final Table table;
  private final SparkTableCache tableCache = SparkTableCache.get();
  private final ScanTaskSetManager taskSetManager = ScanTaskSetManager.get();
  private final PositionDeletesRewriteCoordinator coordinator =
      PositionDeletesRewriteCoordinator.get();

  public SparkCompactionConflictResolver(SparkSession spark, Table table) {
    this.spark = spark.cloneSession();
    // Disable Adaptive Query Execution as this may change output partitioning
    this.spark.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), false);
    this.table = table;
  }

  /**
   * Resolves conflicts by remapping position deletes to reference compacted files.
   *
   * @param compactionMap the compaction map describing file transformations
   * @param conflicts the detected conflicts to resolve
   * @return list of new delete files with remapped positions
   */
  public List<DeleteFile> resolve(CompactionMap compactionMap, DeleteConflictInfo conflicts) {
    if (!conflicts.hasConflicts()) {
      LOG.debug("No conflicts to resolve");
      return Lists.newArrayList();
    }

    LOG.info(
        "Resolving {} conflicting delete files affecting {} data files",
        conflicts.deleteFileCount(),
        conflicts.affectedDataFileCount());

    String groupId = UUID.randomUUID().toString();
    Table deletesTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.POSITION_DELETES);

    try {
      tableCache.add(groupId, deletesTable);

      // Stage the conflicting delete files for reading
      List<PositionDeletesScanTask> tasks = createScanTasks(conflicts);
      taskSetManager.stageTasks(deletesTable, groupId, tasks);

      // Read, remap, and write deletes
      List<DeleteFile> newDeleteFiles = remapAndWriteDeletes(groupId, compactionMap);

      LOG.info("Wrote {} new delete files with remapped positions", newDeleteFiles.size());
      return newDeleteFiles;
    } finally {
      tableCache.remove(groupId);
      taskSetManager.removeTasks(deletesTable, groupId);
      coordinator.clearRewrite(deletesTable, groupId);
    }
  }

  /**
   * Creates scan tasks for the conflicting delete files.
   *
   * @param conflicts the detected conflicts
   * @return list of scan tasks for reading the delete files
   */
  private List<PositionDeletesScanTask> createScanTasks(DeleteConflictInfo conflicts) {
    return PositionDeletesScanTasks.create(conflicts.conflictingDeleteFiles(), table);
  }

  /**
   * Reads position deletes, remaps them, and writes new delete files.
   *
   * @param groupId the unique group ID for this operation
   * @param compactionMap the compaction map
   * @return list of new delete files
   */
  private List<DeleteFile> remapAndWriteDeletes(String groupId, CompactionMap compactionMap) {

    // Get the set of compacted source files for filtering
    Set<String> compactedSourceFiles = getCompactedSourceFiles(compactionMap);

    // Read the position deletes from conflicting files
    Dataset<Row> posDeletes =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SCAN_TASK_SET_ID, groupId)
            .load(groupId);

    // Filter to only deletes referencing compacted files
    Dataset<Row> filteredDeletes =
        posDeletes.filter(posDeletes.col("file_path").isin(compactedSourceFiles.toArray()));

    // Get the schema and create encoder for Row transformation
    StructType schema = filteredDeletes.schema();
    Encoder<Row> encoder = Encoders.row(schema);

    // Get column indices
    int filePathIndex = schema.fieldIndex("file_path");
    int posIndex = schema.fieldIndex("pos");

    // Remap positions using a map function that preserves the schema
    Dataset<Row> remapped =
        filteredDeletes.map(
            new RemapFunction(compactionMap, compactedSourceFiles, filePathIndex, posIndex),
            encoder);

    // Filter out null rows (positions that weren't found in compaction map)
    Dataset<Row> validRemapped = remapped.filter(remapped.col("file_path").isNotNull());

    // If no remapped deletes, return empty
    if (validRemapped.isEmpty()) {
      LOG.info("No position deletes to remap after filtering");
      return Lists.newArrayList();
    }

    // Write the remapped deletes
    Table deletesTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.POSITION_DELETES);

    validRemapped
        .sortWithinPartitions("file_path", "pos")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupId)
        .mode("append")
        .save(groupId);

    // Fetch the new delete files
    return Lists.newArrayList(coordinator.fetchNewFiles(deletesTable, groupId));
  }

  /**
   * Extracts source file paths from the compaction map.
   *
   * @param compactionMap the compaction map
   * @return set of source file paths that were compacted
   */
  private Set<String> getCompactedSourceFiles(CompactionMap compactionMap) {
    Set<String> sourceFiles = Sets.newHashSet();
    for (CompactionMap.FileMapping mapping : compactionMap.fileMappings()) {
      sourceFiles.add(mapping.sourceFile());
    }
    return sourceFiles;
  }

  /**
   * Builds a mapping from source file to target file.
   *
   * @param compactionMap the compaction map
   * @return map from source file path to target file path
   */
  static Map<String, String> buildSourceToTargetMap(CompactionMap compactionMap) {
    Map<String, String> sourceToTarget = Maps.newHashMap();
    for (CompactionMap.FileMapping mapping : compactionMap.fileMappings()) {
      sourceToTarget.put(mapping.sourceFile(), mapping.targetFile());
    }
    return sourceToTarget;
  }

  /** Map function that remaps position deletes using the compaction map. */
  static class RemapFunction implements MapFunction<Row, Row>, Serializable {
    private final CompactionMap compactionMap;
    private final Set<String> compactedSourceFiles;
    private final int filePathIndex;
    private final int posIndex;
    private transient PositionDeleteRemapper remapper;

    RemapFunction(
        CompactionMap compactionMap,
        Set<String> compactedSourceFiles,
        int filePathIndex,
        int posIndex) {
      this.compactionMap = compactionMap;
      this.compactedSourceFiles = compactedSourceFiles;
      this.filePathIndex = filePathIndex;
      this.posIndex = posIndex;
    }

    @Override
    public Row call(Row row) {
      if (remapper == null) {
        remapper = new PositionDeleteRemapper(compactionMap);
      }

      String filePath = row.getString(filePathIndex);
      long position = row.getLong(posIndex);

      // Skip if not in compacted files (should be filtered already, but double-check)
      if (!compactedSourceFiles.contains(filePath)) {
        return null;
      }

      // Create position delete and remap
      PositionDelete<?> delete = PositionDelete.create();
      delete.set(filePath, position, null);

      try {
        PositionDelete<?> remappedDelete = remapper.remapDelete(delete);
        String targetFile = remappedDelete.path().toString();
        long targetPos = remappedDelete.pos();

        // Create a new row with remapped values, preserving other columns
        Object[] values = new Object[row.size()];
        for (int i = 0; i < row.size(); i++) {
          if (i == filePathIndex) {
            values[i] = targetFile;
          } else if (i == posIndex) {
            values[i] = targetPos;
          } else {
            values[i] = row.get(i);
          }
        }

        return RowFactory.create(values);
      } catch (IllegalStateException e) {
        // Position not found in compaction map - row was filtered during compaction
        // This is idempotent - the row doesn't exist in the target file
        LOG.debug("Position {} in {} not found in compaction map, skipping", position, filePath);
        return null;
      }
    }
  }
}
