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

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMapChain;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.DeleteConflictInfo;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PositionDeleteRemapper;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.PositionDeletesScanTasks;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.RemappedDVWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.PositionDeletesRewriteCoordinator;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkTableCache;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.spark.api.java.function.FlatMapFunction;
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
 * Resolves compaction conflicts with position deletes and deletion vectors using Spark
 * infrastructure.
 *
 * <p>When a compaction operation detects that concurrent transactions have added position deletes
 * or deletion vectors (DVs) referencing the files being compacted, this resolver remaps those
 * deletes to reference the new compacted files using the compaction map.
 *
 * <p>The resolution flow for position delete files (V2 format):
 *
 * <ol>
 *   <li>Read position deletes from conflicting delete files using Spark
 *   <li>Filter deletes to only those referencing compacted source files
 *   <li>Remap positions using the compaction map via {@link PositionDeleteRemapper}
 *   <li>Write new delete files with remapped positions
 * </ol>
 *
 * <p>The resolution flow for deletion vectors (V3 format):
 *
 * <ol>
 *   <li>Read deleted positions from DV files using {@link
 *       org.apache.iceberg.deletes.DVPositionReader}
 *   <li>Remap positions using {@link PositionDeleteRemapper#remapDVBulk}
 *   <li>Write new DV files using {@link RemappedDVWriter}
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
   * Resolves conflicts by remapping position deletes and deletion vectors to reference compacted
   * files.
   *
   * <p>This method handles three types of deletes:
   *
   * <ul>
   *   <li>Deletion vectors (DVs) - remapped using core infrastructure without Spark
   *   <li>File-scoped position deletes that definitely conflict (known data file references)
   *   <li>Multi-file position deletes that may conflict (need content-based filtering)
   * </ul>
   *
   * @param compactionMap the compaction map describing file transformations
   * @param conflicts the detected conflicts to resolve
   * @return list of new delete files with remapped positions
   */
  public List<DeleteFile> resolve(CompactionMap compactionMap, DeleteConflictInfo conflicts) {
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(compactionMap);
    return resolveWithRemapper(remapper, conflicts);
  }

  /**
   * Resolves conflicts by remapping position deletes and deletion vectors through a chain of
   * compaction maps.
   *
   * <p>Use this method when multiple sequential compactions have occurred, requiring composition of
   * multiple compaction maps to correctly remap positions.
   *
   * @param chain the compaction map chain for multi-step remapping
   * @param conflicts the detected conflicts to resolve
   * @return list of new delete files with remapped positions
   */
  public List<DeleteFile> resolve(CompactionMapChain chain, DeleteConflictInfo conflicts) {
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(chain);
    return resolveWithRemapper(remapper, conflicts);
  }

  /**
   * Internal method that performs conflict resolution using a provided remapper.
   *
   * @param remapper the remapper to use (may be backed by single map or chain)
   * @param conflicts the detected conflicts to resolve
   * @return list of new delete files with remapped positions
   */
  private List<DeleteFile> resolveWithRemapper(
      PositionDeleteRemapper remapper, DeleteConflictInfo conflicts) {
    if (!conflicts.hasConflicts()) {
      LOG.debug("No conflicts to resolve");
      return Lists.newArrayList();
    }

    // Separate DVs from position delete files
    List<DeleteFile> dvFiles = Lists.newArrayList();
    List<DeleteFile> positionDeleteFiles = Lists.newArrayList();

    for (DeleteFile deleteFile : conflicts.conflictingDeleteFiles()) {
      if (ContentFileUtil.isDV(deleteFile)) {
        dvFiles.add(deleteFile);
      } else {
        positionDeleteFiles.add(deleteFile);
      }
    }

    // Multi-file position deletes are always position delete files (not DVs)
    positionDeleteFiles.addAll(conflicts.multiFilePositionDeletes());

    int dvCount = dvFiles.size();
    int posDeleteCount = positionDeleteFiles.size();
    LOG.info(
        "Resolving conflicts: {} deletion vectors, {} position delete files, {} known affected data files",
        dvCount,
        posDeleteCount,
        conflicts.affectedDataFileCount());

    List<DeleteFile> newDeleteFiles = Lists.newArrayList();

    // Handle DVs using core infrastructure (no Spark needed)
    if (!dvFiles.isEmpty()) {
      List<DeleteFile> remappedDVs = remapDVsWithRemapper(dvFiles, remapper);
      newDeleteFiles.addAll(remappedDVs);
      LOG.info("Wrote {} new deletion vectors from {} original DVs", remappedDVs.size(), dvCount);
    }

    // Handle position delete files using Spark
    if (!positionDeleteFiles.isEmpty()) {
      List<DeleteFile> remappedPosDeletes =
          remapPositionDeletesWithRemapper(positionDeleteFiles, remapper);
      newDeleteFiles.addAll(remappedPosDeletes);
      LOG.info(
          "Wrote {} new position delete files from {} original files",
          remappedPosDeletes.size(),
          posDeleteCount);
    }

    LOG.info("Total: wrote {} new delete files with remapped positions", newDeleteFiles.size());
    return newDeleteFiles;
  }

  /**
   * Remaps deletion vectors using core infrastructure.
   *
   * <p>DVs are handled without Spark because:
   *
   * <ul>
   *   <li>DVs are single-file scoped (one DV per data file)
   *   <li>Core infrastructure provides efficient bulk remapping
   *   <li>No benefit from distributed processing for DV remapping
   * </ul>
   *
   * @param dvFiles the deletion vector files to remap
   * @param compactionMap the compaction map
   * @return list of new DV files with remapped positions
   */
  private List<DeleteFile> remapDVs(List<DeleteFile> dvFiles, CompactionMap compactionMap) {
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(compactionMap);
    return remapDVsWithRemapper(dvFiles, remapper);
  }

  /**
   * Remaps deletion vectors using a provided remapper.
   *
   * @param dvFiles the deletion vector files to remap
   * @param remapper the remapper to use
   * @return list of new DV files with remapped positions
   */
  private List<DeleteFile> remapDVsWithRemapper(
      List<DeleteFile> dvFiles, PositionDeleteRemapper remapper) {
    List<DeleteFile> newDVs = Lists.newArrayList();

    for (DeleteFile dvFile : dvFiles) {
      try {
        // Remap positions using bulk API
        Map<String, Set<Long>> remappedPositions = remapper.remapDVBulk(dvFile, table.io());

        if (remappedPositions.isEmpty()) {
          LOG.debug("No positions to remap for DV: {}", dvFile.location());
          continue;
        }

        // Get partition info from the DV
        PartitionSpec spec = table.specs().get(dvFile.specId());
        StructLike partition = dvFile.partition();

        // Write new DVs for each target file
        RemappedDVWriter writer = new RemappedDVWriter(table, spec, partition);
        List<DeleteFile> writtenDVs = writer.writeRemappedDVs(remappedPositions);
        newDVs.addAll(writtenDVs);

        LOG.debug(
            "Remapped DV {} to {} new DVs targeting {} files",
            dvFile.location(),
            writtenDVs.size(),
            remappedPositions.size());

      } catch (IOException e) {
        throw new RuntimeException("Failed to remap DV: " + dvFile.location(), e);
      }
    }

    return newDVs;
  }

  /**
   * Remaps position delete files using Spark infrastructure.
   *
   * @param positionDeleteFiles the position delete files to remap
   * @param compactionMap the compaction map
   * @return list of new position delete files with remapped positions
   */
  private List<DeleteFile> remapPositionDeletes(
      List<DeleteFile> positionDeleteFiles, CompactionMap compactionMap) {
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(compactionMap);
    return remapPositionDeletesWithRemapper(positionDeleteFiles, remapper);
  }

  /**
   * Remaps position delete files using a provided remapper.
   *
   * @param positionDeleteFiles the position delete files to remap
   * @param remapper the remapper to use
   * @return list of new position delete files with remapped positions
   */
  private List<DeleteFile> remapPositionDeletesWithRemapper(
      List<DeleteFile> positionDeleteFiles, PositionDeleteRemapper remapper) {
    String groupId = UUID.randomUUID().toString();
    Table deletesTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.POSITION_DELETES);

    try {
      tableCache.add(groupId, deletesTable);

      // Create scan tasks for position delete files
      List<PositionDeletesScanTask> tasks =
          PositionDeletesScanTasks.create(positionDeleteFiles, table);
      if (tasks.isEmpty()) {
        LOG.debug("No scan tasks to process for position deletes");
        return Lists.newArrayList();
      }
      taskSetManager.stageTasks(deletesTable, groupId, tasks);

      // Read, remap, and write deletes
      return remapAndWriteDeletesWithRemapper(groupId, remapper);
    } finally {
      tableCache.remove(groupId);
      taskSetManager.removeTasks(deletesTable, groupId);
      coordinator.clearRewrite(deletesTable, groupId);
    }
  }

  /**
   * Reads position deletes, remaps them, and writes new delete files.
   *
   * @param groupId the unique group ID for this operation
   * @param compactionMap the compaction map
   * @return list of new delete files
   */
  private List<DeleteFile> remapAndWriteDeletes(String groupId, CompactionMap compactionMap) {
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(compactionMap);
    return remapAndWriteDeletesWithRemapper(groupId, remapper);
  }

  /**
   * Reads position deletes, remaps them using a provided remapper, and writes new delete files.
   *
   * @param groupId the unique group ID for this operation
   * @param remapper the remapper to use
   * @return list of new delete files
   */
  private List<DeleteFile> remapAndWriteDeletesWithRemapper(
      String groupId, PositionDeleteRemapper remapper) {

    // Get the set of compacted source files for filtering
    Set<String> compactedSourceFiles = remapper.compactedFiles();

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

    // Remap positions using flatMap so unmappable rows (positions filtered during merge
    // compaction — e.g., the row was already deleted by an earlier PD that compaction applied)
    // can be dropped by emitting an empty iterator. The earlier map+filter(isNotNull) pattern
    // crashed Spark's whole-stage codegen: the encoder's serializefromobject step ran before
    // the filter and rejected a null top-level Row with "Null value appeared in non-nullable
    // field: top level Product or row object".
    Dataset<Row> validRemapped =
        filteredDeletes.flatMap(
            new RemapFlatMapFunctionWithRemapper(
                remapper, compactedSourceFiles, filePathIndex, posIndex),
            encoder);

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

  /**
   * Map function that remaps position deletes using a single compaction map.
   *
   * <p>The CompactionMap is serialized to Avro bytes for safe transport to Spark executors, since
   * neither CompactionMap nor PositionDeleteRemapper implement Java Serializable. The remapper is
   * lazily reconstructed on the executor side from the deserialized bytes.
   */
  static class RemapFunction implements MapFunction<Row, Row>, Serializable {
    private final byte[] compactionMapBytes;
    private final Set<String> compactedSourceFiles;
    private final int filePathIndex;
    private final int posIndex;
    private transient PositionDeleteRemapper remapper;

    RemapFunction(
        CompactionMap compactionMap,
        Set<String> compactedSourceFiles,
        int filePathIndex,
        int posIndex) {
      this.compactionMapBytes = CompactionMaps.toBytes(compactionMap);
      this.compactedSourceFiles = compactedSourceFiles;
      this.filePathIndex = filePathIndex;
      this.posIndex = posIndex;
    }

    @Override
    public Row call(Row row) {
      if (remapper == null) {
        remapper = new PositionDeleteRemapper(CompactionMaps.fromBytes(compactionMapBytes));
      }

      return remapRow(row, remapper, compactedSourceFiles, filePathIndex, posIndex);
    }
  }

  /**
   * Map function that remaps position deletes using a pre-configured remapper backed by one or more
   * compaction maps (supporting chained compactions).
   *
   * <p>Compaction maps are serialized to Avro bytes for safe transport to Spark executors. The
   * remapper is lazily reconstructed on the executor side. For chains, a CompactionMapChain is
   * rebuilt from the ordered list of deserialized maps.
   */
  static class RemapFunctionWithRemapper implements MapFunction<Row, Row>, Serializable {
    private final byte[][] compactionMapBytesArray;
    private final Set<String> compactedSourceFiles;
    private final int filePathIndex;
    private final int posIndex;
    private transient PositionDeleteRemapper remapper;

    RemapFunctionWithRemapper(
        PositionDeleteRemapper remapper,
        Set<String> compactedSourceFiles,
        int filePathIndex,
        int posIndex) {
      // Serialize all compaction maps to bytes for safe transport to executors
      if (remapper.chain() != null) {
        List<CompactionMap> maps = remapper.chain().maps();
        this.compactionMapBytesArray = new byte[maps.size()][];
        for (int i = 0; i < maps.size(); i++) {
          this.compactionMapBytesArray[i] = CompactionMaps.toBytes(maps.get(i));
        }
      } else {
        this.compactionMapBytesArray =
            new byte[][] {CompactionMaps.toBytes(remapper.compactionMap())};
      }
      this.compactedSourceFiles = compactedSourceFiles;
      this.filePathIndex = filePathIndex;
      this.posIndex = posIndex;
    }

    @Override
    public Row call(Row row) {
      if (remapper == null) {
        if (compactionMapBytesArray.length == 1) {
          remapper =
              new PositionDeleteRemapper(CompactionMaps.fromBytes(compactionMapBytesArray[0]));
        } else {
          List<CompactionMap> maps = Lists.newArrayListWithCapacity(compactionMapBytesArray.length);
          for (byte[] bytes : compactionMapBytesArray) {
            maps.add(CompactionMaps.fromBytes(bytes));
          }
          remapper = new PositionDeleteRemapper(CompactionMapChain.build(maps));
        }
      }

      return remapRow(row, remapper, compactedSourceFiles, filePathIndex, posIndex);
    }
  }

  /**
   * FlatMap function that remaps position deletes and emits zero or one row per input — used by the
   * resolver's Spark write pipeline so that "no mapping for this position" can be signalled by an
   * empty iterator instead of a null Row. The latter crashes Spark's whole-stage codegen encoder
   * (see remapAndWriteDeletesWithRemapper for context).
   */
  static class RemapFlatMapFunctionWithRemapper implements FlatMapFunction<Row, Row>, Serializable {
    private final RemapFunctionWithRemapper delegate;

    RemapFlatMapFunctionWithRemapper(
        PositionDeleteRemapper remapper,
        Set<String> compactedSourceFiles,
        int filePathIndex,
        int posIndex) {
      this.delegate =
          new RemapFunctionWithRemapper(remapper, compactedSourceFiles, filePathIndex, posIndex);
    }

    @Override
    public Iterator<Row> call(Row row) {
      Row remapped = delegate.call(row);
      return remapped == null
          ? Collections.emptyIterator()
          : Collections.singletonList(remapped).iterator();
    }
  }

  /** Shared remapping logic for both RemapFunction and RemapFunctionWithRemapper. */
  private static Row remapRow(
      Row row,
      PositionDeleteRemapper remapper,
      Set<String> compactedSourceFiles,
      int filePathIndex,
      int posIndex) {
    String filePath = row.getString(filePathIndex);
    long position = row.getLong(posIndex);

    if (!compactedSourceFiles.contains(filePath)) {
      return null;
    }

    PositionDelete<?> delete = PositionDelete.create();
    delete.set(filePath, position, null);

    // Use the lenient remapper: positions that aren't in the compaction map (because the row was
    // already deleted by a chain PD that compaction applied and filtered out) return null. The
    // caller's flatMap wrapper turns that into an empty iterator. The earlier remapDelete +
    // try/catch pattern returned a null Row, which Spark's RowEncoder cannot serialize.
    PositionDelete<?> remappedDelete = remapper.remapDeleteOrNull(delete);
    if (remappedDelete == null) {
      LOG.debug("Position {} in {} not found in compaction map, skipping", position, filePath);
      return null;
    }
    String targetFile = remappedDelete.path().toString();
    long targetPos = remappedDelete.pos();

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
  }
}
