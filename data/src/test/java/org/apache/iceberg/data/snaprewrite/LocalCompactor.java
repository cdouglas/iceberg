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
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.StructLikeMap;

/**
 * A bin-pack compaction that records where every surviving row went.
 *
 * <p>This is the workload the rewrite is built on, so the tests use a real one: it reads live rows
 * in position order, concatenates them into a single target file, and emits a compaction map built
 * by {@link CompactionMapBuilder} and attached through the normal rewrite commit path. The map is
 * not hand-written, so a bug in run merging or in map attachment shows up in these tests rather
 * than being assumed away.
 */
class LocalCompactor {
  private LocalCompactor() {}

  /** Compacts every live data file into one target file and commits, attaching a compaction map. */
  static Snapshot compact(Table table) {
    return compact(table, true);
  }

  /**
   * Compacts every live data file into one target file and commits.
   *
   * @param attachMap whether to emit and attach a compaction map. A compaction without one is what
   *     a rewrite must refuse to see inside its window, since it has no way to follow rows through
   *     it.
   */
  static Snapshot compact(Table table, boolean attachMap) {
    return compact(table, attachMap, file -> true);
  }

  /**
   * Compacts the live data files a predicate selects.
   *
   * <p>A partial compaction leaves files untouched, and the rewrite has to treat those as already
   * in place: the compaction map says nothing about them because nothing moved.
   */
  static Snapshot compact(Table table, boolean attachMap, Predicate<DataFile> include) {
    Snapshot startingSnapshot = table.currentSnapshot();
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
      for (FileScanTask task : planned) {
        tasks.add(task);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    tasks.sort(Comparator.comparing(task -> task.file().location()));

    CompactionMapBuilder mapBuilder =
        new CompactionMapBuilder(startingSnapshot.snapshotId(), startingSnapshot.snapshotId() + 1);

    Set<DataFile> replacedData = Sets.newHashSet();
    Set<DeleteFile> replacedDeletes = Sets.newHashSet();
    Map<String, DeleteFile> deletesByPath = Maps.newHashMap();
    Set<DataFile> targets = Sets.newHashSet();

    // One target file per partition: a data file belongs to exactly one partition, so rows cannot
    // be
    // concatenated across them.
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
      String targetPath =
          table.location()
              + "/data/"
              + FileFormat.PARQUET.addExtension("compaction-" + UUID.randomUUID());
      OutputFile output = table.io().newOutputFile(targetPath);

      long targetPosition = 0;
      long recordCount = 0;
      GenericAppenderFactory factory =
          new GenericAppenderFactory(table.schema(), table.spec()).setAll(table.properties());
      FileAppender<Record> appender = factory.newAppender(output, FileFormat.PARQUET);

      try {
        for (FileScanTask task : partition.getValue()) {
          DataFile file = task.file();
          replacedData.add(file);
          for (DeleteFile delete : task.deletes()) {
            deletesByPath.put(delete.location(), delete);
          }

          // Equality deletes are not applied here. A window containing them cannot be rewritten
          // anyway, and these tests only need such a history to reach the refusal.
          List<DeleteFile> positionDeletes = Lists.newArrayList();
          for (DeleteFile delete : task.deletes()) {
            if (delete.content() == FileContent.POSITION_DELETES) {
              positionDeletes.add(delete);
            }
          }

          PositionDeleteIndex deleted =
              positionDeletes.isEmpty()
                  ? null
                  : new org.apache.iceberg.data.GenericSnapshotRewriteIO(table)
                      .loadPositionDeletes(positionDeletes, file.location());

          List<Record> rows = RawFiles.readAll(table.io(), file.location(), table.schema());
          CompactionMapBuilder.FileMappingBuilder mapping =
              mapBuilder.addFileMapping(file.location(), targetPath);

          // Surviving rows keep their relative order, so consecutive live positions form one run
          // and
          // the map stays small. A run breaks wherever a delete interrupts the sequence.
          long runStart = -1;
          long runTargetStart = -1;
          long runLength = 0;
          for (long position = 0; position < rows.size(); position += 1) {
            if (deleted != null && deleted.isDeleted(position)) {
              if (runLength > 0) {
                mapping.addRun(runStart, runTargetStart, runLength);
                runLength = 0;
              }

              continue;
            }

            if (runLength == 0) {
              runStart = position;
              runTargetStart = targetPosition;
            }

            runLength += 1;
            appender.add(rows.get((int) position));
            targetPosition += 1;
            recordCount += 1;
          }

          if (runLength > 0) {
            mapping.addRun(runStart, runTargetStart, runLength);
          }
        }
      } finally {
        close(appender);
      }

      DataFiles.Builder builder =
          DataFiles.builder(table.spec())
              .withPath(targetPath)
              .withFormat(FileFormat.PARQUET)
              .withFileSizeInBytes(appender.length())
              .withMetrics(appender.metrics())
              .withRecordCount(recordCount);
      if (table.spec().isPartitioned()) {
        builder.withPartition(partition.getKey());
      }

      targets.add(builder.build());
    }

    // A delete file left applying to a file this compaction did not touch must stay. Dropping it
    // would lose those deletes, which a partial compaction makes possible.
    Set<String> stillNeeded = Sets.newHashSet();
    for (FileScanTask task : tasks) {
      if (!include.test(task.file())) {
        for (DeleteFile delete : task.deletes()) {
          stillNeeded.add(delete.location());
        }
      }
    }

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
      // Rewrite the map with its real target snapshot id. Composing two maps requires the first's
      // target to be the second's source, and the id a compaction will be assigned is not known
      // until it commits. Production code has the same gap and stamps a placeholder; chaining only
      // works once the ids are real.
      table.io().deleteFile(mapPath);
      writeMap(table, mapBuilder, startingSnapshot.snapshotId(), committed.snapshotId(), mapPath);
    }

    return committed;
  }

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
        target.addRun(run.sourcePosition(), run.targetPosition(), run.length());
      }
    }

    try {
      CompactionMaps.write(rebuilt.build(), table.io().newOutputFile(path));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void close(FileAppender<?> appender) {
    try {
      appender.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
