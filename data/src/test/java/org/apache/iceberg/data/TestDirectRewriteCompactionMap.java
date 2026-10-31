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
package org.apache.iceberg.data;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PositionDeleteRemapper;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.exceptions.CompactionConflictException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A direct {@code RewriteFiles} call does not say how it laid out its output, so it must not leave
 * a compaction map behind. A map that assumes the wrong layout sends a remapped delete to the wrong
 * row.
 */
public class TestDirectRewriteCompactionMap {
  private static final Schema SCHEMA = new Schema(required(1, "id", Types.IntegerType.get()));

  @TempDir private File tableDir;
  @TempDir private Path temp;

  private Table table;
  private int fileCount = 0;

  @BeforeEach
  public void createTable() {
    table = TestTables.create(tableDir, "test", SCHEMA, PartitionSpec.unpartitioned(), 2);
    table.updateProperties().set(TableProperties.COMPACTION_MAP_ENABLED, "true").commit();
  }

  @AfterEach
  public void clearTables() {
    TestTables.clearTables();
  }

  @Test
  public void aSortedRewriteDoesNotMisplaceARemappedDelete() throws IOException {
    DataFile high = writeDataFile(10, 11, 12);
    DataFile low = writeDataFile(0, 1, 2);
    table.newAppend().appendFile(high).appendFile(low).commit();
    long startingSnapshotId = table.currentSnapshot().snapshotId();

    // A sort compaction: it removes high, then low, and writes low's rows first
    DataFile sorted = writeDataFile(0, 1, 2, 10, 11, 12);
    table
        .newRewrite()
        .validateFromSnapshot(startingSnapshotId)
        .deleteFile(high)
        .deleteFile(low)
        .addFile(sorted)
        .commit();

    // A concurrent transaction deletes id 10, the first row of high, and remaps on a conflict
    try {
      commitDelete(startingSnapshotId, high.location(), 0L);
    } catch (CompactionConflictException e) {
      PositionDeleteRemapper remapper =
          PositionDeleteRemapper.fromConflict(e, table.io()).get(high.location());
      PositionDelete<?> delete =
          remapper.remapDelete(PositionDelete.create().set(high.location(), 0L));
      commitDelete(table.currentSnapshot().snapshotId(), delete.path().toString(), delete.pos());
    } catch (ValidationException e) {
      // Refusing the delete is safe: the transaction re-reads the table and tries again
    }

    assertThat(ids())
        .as("A delete of id 10 may be refused, but must not remove any other row")
        .containsAll(ImmutableList.of(0, 1, 2, 11, 12));
  }

  @Test
  public void aRewriteThatDropsDeletedRowsCanCommit() throws IOException {
    DataFile source = writeDataFile(0, 1, 2, 3);
    table.newAppend().appendFile(source).commit();
    commitDelete(table.currentSnapshot().snapshotId(), source.location(), 1L);

    // A compaction that applies the delete writes one row fewer than its source holds
    DataFile compacted = writeDataFile(0, 2, 3);
    table
        .newRewrite()
        .validateFromSnapshot(table.currentSnapshot().snapshotId())
        .deleteFile(source)
        .addFile(compacted)
        .commit();

    assertThat(ids()).containsExactlyInAnyOrder(0, 2, 3);
  }

  private void commitDelete(long fromSnapshotId, String dataFile, long pos) throws IOException {
    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList(Pair.of(dataFile, pos));
    DeleteFile deleteFile = FileHelpers.writeDeleteFile(table, newOutput(), deletes).first();
    table
        .newRowDelta()
        .validateFromSnapshot(fromSnapshotId)
        .validateDataFilesExist(ImmutableList.of(dataFile))
        .addDeletes(deleteFile)
        .commit();
  }

  private DataFile writeDataFile(int... ids) throws IOException {
    List<Record> rows = Lists.newArrayList();
    for (int id : ids) {
      Record row = GenericRecord.create(SCHEMA);
      row.setField("id", id);
      rows.add(row);
    }

    return FileHelpers.writeDataFile(table, newOutput(), rows);
  }

  private OutputFile newOutput() {
    fileCount += 1;
    return Files.localOutput(temp.resolve("file-" + fileCount + ".parquet").toFile());
  }

  private Set<Integer> ids() throws IOException {
    Set<Integer> ids = Sets.newHashSet();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      rows.forEach(row -> ids.add((Integer) row.getField("id")));
    }

    return ids;
  }
}
