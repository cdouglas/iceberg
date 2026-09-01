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
package org.apache.iceberg.data.snaprewrite;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.iceberg.BaseRewriteFiles;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMapBuilder;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRowLineage;
import org.apache.iceberg.data.GenericSnapshotRewriteIO;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.StructLikeMap;

/**
 * A bin-pack compaction that records where every surviving row went.
 *
 * <p>This is the workload the rewrite is built on, so the tests use a real one: it reads live rows
 * in position order, concatenates them, and emits a compaction map built by {@link
 * CompactionMapBuilder} and attached through the normal rewrite commit path. The map is not
 * hand-written, so a bug in run merging or in map attachment surfaces in these tests rather than
 * being assumed away.
 *
 * <p>Under v3 it also preserves row lineage, writing each surviving row's {@code _row_id} out
 * explicitly. A compaction gathers rows from files whose id ranges are unrelated, so a single
 * {@code first_row_id} cannot describe the result and derived ids would renumber every row. Without
 * this the rewrite's identity guarantee cannot be tested end to end, because the compaction would
 * be the thing losing identities.
 */
class LocalCompactor {
  /** No target size limit: one output file per partition. */
  private static final long UNLIMITED = Long.MAX_VALUE;

  private LocalCompactor() {}

  /** Compacts every live data file into one target file per partition, attaching a map. */
  static Snapshot compact(Table table) {
    return compact(table, true);
  }

  /**
   * Compacts every live data file, optionally attaching a compaction map.
   *
   * @param attachMap whether to emit and attach a map. A replace without one cannot be followed,
   *     and the rewrite falls back to copying rows through it.
   */
  static Snapshot compact(Table table, boolean attachMap) {
    return compact(table, attachMap, file -> true, UNLIMITED);
  }

  /**
   * Compacts the live data files a predicate selects.
   *
   * <p>A partial compaction leaves files untouched, and the rewrite has to treat those as already
   * in place: the map says nothing about them because nothing moved.
   */
  static Snapshot compact(Table table, boolean attachMap, Predicate<DataFile> include) {
    return compact(table, attachMap, include, UNLIMITED);
  }

  /**
   * Compacts with a cap on rows per output file.
   *
   * <p>A cap makes the output roll mid-source-file, so one source file's rows land in two targets
   * and its runs carry per-run target paths. That is the multi-target shape real compactions
   * produce when they hit a target size, and it is the case where a map that recorded only one
   * target per source file would send a remapped position into the wrong file.
   */
  static Snapshot compact(
      Table table, boolean attachMap, Predicate<DataFile> include, long maxRowsPerTarget) {
    Snapshot startingSnapshot = table.currentSnapshot();
    List<FileScanTask> tasks = planFiles(table);
    tasks.sort(Comparator.comparing(task -> task.file().location()));

    CompactionMapBuilder mapBuilder =
        new CompactionMapBuilder(startingSnapshot.snapshotId(), startingSnapshot.snapshotId() + 1);

    Set<DataFile> replacedData = Sets.newHashSet();
    Set<DataFile> targets = Sets.newHashSet();
    Map<String, DeleteFile> deletesByPath = Maps.newHashMap();

    // One target per partition (before any rolling): rows cannot be concatenated across partitions.
    StructLikeMap<List<FileScanTask>> byPartition =
        StructLikeMap.create(table.spec().partitionType());
    for (FileScanTask task : tasks) {
      if (include.test(task.file())) {
        byPartition
            .computeIfAbsent(task.file().partition(), ignored -> Lists.newArrayList())
            .add(task);
      }
    }

    for (Map.Entry<StructLike, List<FileScanTask>> partition : byPartition.entrySet()) {
      targets.addAll(
          compactPartition(
              table,
              partition.getKey(),
              partition.getValue(),
              mapBuilder,
              replacedData,
              deletesByPath,
              maxRowsPerTarget));
    }

    return commit(
        table,
        startingSnapshot,
        mapBuilder,
        replacedData,
        targets,
        deletesByPath,
        tasks,
        include,
        attachMap);
  }

  private static List<DataFile> compactPartition(
      Table table,
      StructLike partition,
      List<FileScanTask> tasks,
      CompactionMapBuilder mapBuilder,
      Set<DataFile> replacedData,
      Map<String, DeleteFile> deletesByPath,
      long maxRowsPerTarget) {
    List<DataFile> written = Lists.newArrayList();
    boolean lineage = GenericRowLineage.tracked(table);
    Schema writeSchema = lineage ? GenericRowLineage.writeSchema(table.schema()) : table.schema();
    GenericAppenderFactory factory =
        new GenericAppenderFactory(writeSchema, table.spec()).setAll(table.properties());

    String targetPath = newTargetPath(table);
    FileAppender<Record> appender =
        factory.newAppender(table.io().newOutputFile(targetPath), FileFormat.PARQUET);
    long targetPosition = 0;
    Long firstRowId = null;

    try {
      for (FileScanTask task : tasks) {
        DataFile file = task.file();
        replacedData.add(file);
        for (DeleteFile delete : task.deletes()) {
          deletesByPath.put(delete.location(), delete);
        }

        PositionDeleteIndex deleted = positionDeletes(table, task);
        List<Record> rows =
            lineage
                ? RawFiles.readAllWithLineage(table.io(), file, table.schema())
                : RawFiles.readAll(table.io(), file.location(), table.schema());
        if (lineage && firstRowId == null) {
          firstRowId = file.firstRowId();
        }
        CompactionMapBuilder.FileMappingBuilder mapping =
            mapBuilder.addFileMapping(file.location(), targetPath);
        Run run = new Run();

        for (long position = 0; position < rows.size(); position += 1) {
          if (deleted != null && deleted.isDeleted(position)) {
            // A delete breaks the run: surviving rows on either side are no longer contiguous.
            run.flush(mapping);
            continue;
          }

          if (targetPosition >= maxRowsPerTarget) {
            // Rolling also breaks the run, and everything after it belongs to a different file.
            run.flush(mapping);
            close(appender);
            written.add(finish(table, partition, targetPath, appender, firstRowId));
            targetPath = newTargetPath(table);
            appender =
                factory.newAppender(table.io().newOutputFile(targetPath), FileFormat.PARQUET);
            targetPosition = 0;
          }

          run.open(position, targetPosition, targetPath);
          Record row = rows.get((int) position);
          if (lineage) {
            // The row keeps the id it already had; a derived id would renumber it, because this
            // file's rows come from ranges that have nothing to do with each other.
            row =
                GenericRowLineage.withRowId(
                    writeSchema, row, GenericRowLineage.resolveRowId(row, file, position));
          }

          appender.add(row);
          targetPosition += 1;
        }

        run.flush(mapping);
      }
    } finally {
      close(appender);
    }

    written.add(finish(table, partition, targetPath, appender, firstRowId));
    return written;
  }

  /** A contiguous stretch of surviving rows landing contiguously in one target file. */
  private static class Run {
    private long sourceStart = -1;
    private long targetStart = -1;
    private long length = 0;
    private String target = null;

    void open(long sourcePosition, long targetPosition, String targetPath) {
      if (length == 0) {
        this.sourceStart = sourcePosition;
        this.targetStart = targetPosition;
        this.target = targetPath;
      }

      length += 1;
    }

    void flush(CompactionMapBuilder.FileMappingBuilder mapping) {
      if (length > 0) {
        mapping.addRun(sourceStart, targetStart, length, target);
        length = 0;
      }
    }
  }

  private static Snapshot commit(
      Table table,
      Snapshot startingSnapshot,
      CompactionMapBuilder mapBuilder,
      Set<DataFile> replacedData,
      Set<DataFile> targets,
      Map<String, DeleteFile> deletesByPath,
      List<FileScanTask> tasks,
      Predicate<DataFile> include,
      boolean attachMap) {
    // A delete file still applying to a file this compaction did not touch must stay. Dropping it
    // would lose those deletes, which a partial compaction makes possible.
    Set<String> stillNeeded = Sets.newHashSet();
    for (FileScanTask task : tasks) {
      if (!include.test(task.file())) {
        for (DeleteFile delete : task.deletes()) {
          stillNeeded.add(delete.location());
        }
      }
    }

    Set<DeleteFile> replacedDeletes = Sets.newHashSet();
    for (Map.Entry<String, DeleteFile> entry : deletesByPath.entrySet()) {
      if (!stillNeeded.contains(entry.getKey())) {
        replacedDeletes.add(entry.getValue());
      }
    }

    String mapPath =
        table.location() + "/metadata/" + FileFormat.AVRO.addExtension("cmap-" + UUID.randomUUID());
    if (attachMap) {
      writeMap(
          table,
          mapBuilder,
          startingSnapshot.snapshotId(),
          startingSnapshot.snapshotId() + 1,
          mapPath);
    }

    BaseRewriteFiles rewrite =
        (BaseRewriteFiles) table.newRewrite().validateFromSnapshot(startingSnapshot.snapshotId());
    rewrite.rewriteFiles(replacedData, replacedDeletes, targets, Sets.newHashSet());
    if (attachMap) {
      rewrite.setCompactionMapLocation(mapPath);
    } else {
      rewrite.disableAutoCompactionMap();
    }

    rewrite.commit();
    table.refresh();
    Snapshot committed = table.currentSnapshot();

    if (attachMap) {
      // Rewrite the map with its real target snapshot id. Applying two maps in sequence does not
      // need the ids to agree, but the recorded ids should still describe what happened, and the id
      // a compaction will be assigned is not known until it commits.
      table.io().deleteFile(mapPath);
      writeMap(table, mapBuilder, startingSnapshot.snapshotId(), committed.snapshotId(), mapPath);
    }

    return committed;
  }

  /**
   * Rebuilds a map with corrected snapshot ids, preserving per-run target files.
   *
   * <p>Dropping the per-run target would leave every run inheriting the mapping's single default,
   * and a position remapped against one target file would be applied to a row in another.
   */
  private static void writeMap(
      Table table,
      CompactionMapBuilder source,
      long sourceSnapshotId,
      long targetSnapshotId,
      String path) {
    CompactionMap built = source.build();
    CompactionMapBuilder rebuilt = new CompactionMapBuilder(sourceSnapshotId, targetSnapshotId);
    for (CompactionMap.FileMapping mapping : built.fileMappings()) {
      CompactionMapBuilder.FileMappingBuilder target =
          rebuilt.addFileMapping(mapping.sourceFile(), mapping.targetFile());
      for (CompactionMap.Run run : mapping.runs()) {
        target.addRun(run.sourcePosition(), run.targetPosition(), run.length(), run.targetFile());
      }
    }

    try {
      CompactionMaps.write(rebuilt.build(), table.io().newOutputFile(path));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static PositionDeleteIndex positionDeletes(Table table, FileScanTask task) {
    // Equality deletes are not applied here. A window containing them cannot be rewritten anyway,
    // and those tests only need such a history to reach the refusal.
    List<DeleteFile> positional = Lists.newArrayList();
    for (DeleteFile delete : task.deletes()) {
      if (delete.content() == FileContent.POSITION_DELETES) {
        positional.add(delete);
      }
    }

    return positional.isEmpty()
        ? null
        : new GenericSnapshotRewriteIO(table)
            .loadPositionDeletes(positional, task.file().location());
  }

  /**
   * Builds the data file for an appender the caller has already closed.
   *
   * <p>A lineage-preserving output still needs a {@code first_row_id}: the reader will not read the
   * materialized column without one, even though nothing derives from it. Any value would do; using
   * a source file's keeps the numbers recognisable.
   */
  private static DataFile finish(
      Table table,
      StructLike partition,
      String path,
      FileAppender<Record> appender,
      Long firstRowId) {
    DataFiles.Builder builder =
        DataFiles.builder(table.spec())
            .withPath(path)
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(appender.length())
            .withMetrics(appender.metrics());
    if (table.spec().isPartitioned()) {
      builder.withPartition(partition);
    }

    if (GenericRowLineage.tracked(table)) {
      builder.withFirstRowId(firstRowId != null ? firstRowId : 0L);
    }

    return builder.build();
  }

  private static List<FileScanTask> planFiles(Table table) {
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
      for (FileScanTask task : planned) {
        tasks.add(task);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return tasks;
  }

  private static String newTargetPath(Table table) {
    return table.location()
        + "/data/"
        + FileFormat.PARQUET.addExtension("compaction-" + UUID.randomUUID());
  }

  private static void close(FileAppender<?> appender) {
    try {
      appender.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
