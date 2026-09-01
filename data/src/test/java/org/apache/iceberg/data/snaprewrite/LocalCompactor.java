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
import org.apache.iceberg.BaseRewriteFiles;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMapBuilder;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
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
import org.apache.iceberg.DataFiles;

/**
 * A bin-pack compaction that records where every surviving row went.
 *
 * <p>This is the workload the rewrite is built on, so the tests use a real one: it reads live rows in
 * position order, concatenates them into a single target file, and emits a compaction map built by
 * {@link CompactionMapBuilder} and attached through the normal rewrite commit path. The map is not
 * hand-written, so a bug in run merging or in map attachment shows up in these tests rather than
 * being assumed away.
 */
class LocalCompactor {
  private LocalCompactor() {}

  /** Compacts every live data file into one target file and commits, attaching a compaction map. */
  static Snapshot compact(Table table) {
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

    String targetPath =
        table.location()
            + "/data/"
            + FileFormat.PARQUET.addExtension("compaction-" + UUID.randomUUID());
    OutputFile output = table.io().newOutputFile(targetPath);

    CompactionMapBuilder mapBuilder =
        new CompactionMapBuilder(
            startingSnapshot.snapshotId(), startingSnapshot.snapshotId() + 1);

    Set<DataFile> replacedData = Sets.newHashSet();
    Set<DeleteFile> replacedDeletes = Sets.newHashSet();
    Map<String, DeleteFile> deletesByPath = Maps.newHashMap();

    long targetPosition = 0;
    long recordCount = 0;
    GenericAppenderFactory factory =
        new GenericAppenderFactory(table.schema(), table.spec()).setAll(table.properties());
    FileAppender<Record> appender = factory.newAppender(output, FileFormat.PARQUET);

    try {
      for (FileScanTask task : tasks) {
        DataFile file = task.file();
        replacedData.add(file);
        for (DeleteFile delete : task.deletes()) {
          deletesByPath.put(delete.location(), delete);
        }

        PositionDeleteIndex deleted =
            task.deletes().isEmpty()
                ? null
                : new org.apache.iceberg.data.GenericSnapshotRewriteIO(table)
                    .loadPositionDeletes(task.deletes(), file.location());

        List<Record> rows = RawFiles.readAll(table.io(), file.location(), table.schema());
        CompactionMapBuilder.FileMappingBuilder mapping =
            mapBuilder.addFileMapping(file.location(), targetPath);

        // Surviving rows keep their relative order, so consecutive live positions form one run and
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

    replacedDeletes.addAll(deletesByPath.values());

    DataFile target =
        DataFiles.builder(table.spec())
            .withPath(targetPath)
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(appender.length())
            .withMetrics(appender.metrics())
            .withRecordCount(recordCount)
            .build();

    CompactionMap map = mapBuilder.build();
    String mapPath =
        table.location() + "/metadata/" + FileFormat.AVRO.addExtension("cmap-" + UUID.randomUUID());
    try {
      CompactionMaps.write(map, table.io().newOutputFile(mapPath));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    BaseRewriteFiles rewrite =
        (BaseRewriteFiles) table.newRewrite().validateFromSnapshot(startingSnapshot.snapshotId());
    rewrite.rewriteFiles(replacedData, replacedDeletes, Sets.newHashSet(target), Sets.newHashSet());
    rewrite.setCompactionMapLocation(mapPath);
    rewrite.commit();

    table.refresh();
    return table.currentSnapshot();
  }

  private static void close(FileAppender<?> appender) {
    try {
      appender.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
