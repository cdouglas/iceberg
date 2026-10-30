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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.exceptions.ChainedCompactionMapsException;
import org.apache.iceberg.exceptions.CompactionConflictException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Which compaction maps the validator collects, and in what order (compaction maps errata 4).
 *
 * <p>A manifest keeps its compaction map location when a later commit carries, filters or merges
 * it, so every snapshot holds the locations of all compactions whose manifests are still live, not
 * only its own. The validator should still consider only compactions committed after the
 * transaction started, and compose chained maps in commit order.
 */
public class TestCompactionMapValidatorHistory {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private InMemoryCatalog catalog;
  private Table table;

  @BeforeEach
  public void createTable() {
    this.catalog = new InMemoryCatalog();
    catalog.initialize("test", ImmutableMap.of());
    catalog.createNamespace(Namespace.of("db"));
    this.table =
        catalog.createTable(
            TableIdentifier.of("db", "history"),
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION,
                "2",
                TableProperties.COMPACTION_MAP_ENABLED,
                "true"));
  }

  /**
   * A compaction before the transaction started cannot have moved anything the transaction read,
   * but its manifest still names its map. A multi-file position delete conflicts with every file
   * the validator believes was compacted, so it must not count that one.
   */
  @Test
  public void aCompactionBeforeTheTransactionIsNotAConflict() {
    DataFile first = dataFile("first", 100);
    DataFile second = dataFile("second", 100);
    table.newAppend().appendFile(first).appendFile(second).commit();
    compact(dataFile("compacted", 200), first, second);

    long start = table.currentSnapshot().snapshotId();
    RowDelta rowDelta =
        table.newRowDelta().validateFromSnapshot(start).addDeletes(multiFileDelete("deletes"));

    table.newAppend().appendFile(dataFile("unrelated", 10)).commit();

    rowDelta.commit();
    assertThat(table.currentSnapshot().addedDeleteFiles(table.io())).hasSize(1);
  }

  /** The same delete conflicts with a compaction committed after the transaction started. */
  @Test
  public void aCompactionAfterTheTransactionIsAConflict() {
    DataFile first = dataFile("first", 100);
    DataFile second = dataFile("second", 100);
    table.newAppend().appendFile(first).appendFile(second).commit();

    long start = table.currentSnapshot().snapshotId();
    RowDelta rowDelta =
        table.newRowDelta().validateFromSnapshot(start).addDeletes(multiFileDelete("deletes"));

    compact(dataFile("compacted", 200), first, second);

    assertThatThrownBy(rowDelta::commit)
        .isInstanceOf(CompactionConflictException.class)
        .hasMessageContaining("referenced data files were compacted")
        .satisfies(
            e ->
                assertThat(((CompactionConflictException) e).compactedFiles())
                    .containsExactlyInAnyOrder(first.location(), second.location()));
  }

  /**
   * Two chained compactions after the transaction started, F1 to F2 to F3, while the partition spec
   * is rolled back in between, so F3 is written in a spec with a lower id than F2's. Manifest lists
   * group manifests by spec id, highest first, so the second compaction's manifest follows the
   * first's. The maps must still compose in commit order, taking a delete on F1 to F3.
   */
  @Test
  public void chainedMapsComposeInCommitOrderAcrossASpecRollback() {
    table.updateSpec().addField(Expressions.bucket("id", 2)).commit();
    PartitionSpec bucketed = table.spec();
    DataFile f1 = dataFile(bucketed, "f1", 100);
    table.newAppend().appendFile(f1).commit();
    long start = table.currentSnapshot().snapshotId();

    DataFile f2 = dataFile(bucketed, "f2", 100);
    compact(f2, f1);

    table.updateSpec().removeField(bucketed.fields().get(0).name()).commit();
    assertThat(table.spec().specId())
        .as("rolling the spec back reuses the unpartitioned spec's id")
        .isLessThan(bucketed.specId());
    DataFile f3 = dataFile(table.spec(), "f3", 100);
    compact(f3, f2);

    assertChainRemapsTo(start, f1, f3);
  }

  /** The same chain without a spec change, with every commit merging manifests. */
  @Test
  public void chainedMapsComposeInCommitOrderWithMergedManifests() {
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "2").commit();
    DataFile f1 = dataFile("f1", 100);
    table.newAppend().appendFile(f1).commit();
    table.newAppend().appendFile(dataFile("other", 10)).commit();
    long start = table.currentSnapshot().snapshotId();

    DataFile f2 = dataFile("f2", 100);
    compact(f2, f1);
    DataFile f3 = dataFile("f3", 100);
    compact(f3, f2);
    table.newAppend().appendFile(dataFile("after", 10)).commit();

    assertChainRemapsTo(start, f1, f3);
  }

  /**
   * Chained compactions with another commit between them. The second compaction's map names that
   * commit as its source, not the first compaction, but its sources are still the first
   * compaction's outputs, so the maps still compose.
   */
  @Test
  public void chainedMapsComposeAcrossAnInterveningCommit() {
    DataFile f1 = dataFile("f1", 100);
    table.newAppend().appendFile(f1).commit();
    long start = table.currentSnapshot().snapshotId();

    DataFile f2 = dataFile("f2", 100);
    compact(f2, f1);
    table.newAppend().appendFile(dataFile("between", 10)).commit();
    DataFile f3 = dataFile("f3", 100);
    compact(f3, f2);

    assertChainRemapsTo(start, f1, f3);
  }

  private void assertChainRemapsTo(long start, DataFile source, DataFile target) {
    RowDelta rowDelta =
        table
            .newRowDelta()
            .validateFromSnapshot(start)
            .addDeletes(fileScopedDelete("deletes", source));

    assertThatThrownBy(rowDelta::commit)
        .isInstanceOf(ChainedCompactionMapsException.class)
        .hasMessageContaining("require chained compaction map composition")
        .satisfies(
            e -> {
              ChainedCompactionMapsException chained = (ChainedCompactionMapsException) e;
              assertThat(chained.compactionMaps())
                  .as("the map that consumed the source comes first")
                  .first()
                  .satisfies(map -> assertThat(map.mappingForFile(source.location())).isNotNull());

              Map<String, PositionDeleteRemapper> remappers =
                  PositionDeleteRemapper.fromConflict(chained, table.io());
              PositionDelete<?> delete = PositionDelete.create();
              delete.set(source.location(), 5L, null);
              PositionDelete<?> remapped =
                  remappers.get(source.location()).remapDeleteOrNull(delete);
              assertThat(remapped).isNotNull();
              assertThat(remapped.path().toString()).isEqualTo(target.location());
              assertThat(remapped.pos()).isEqualTo(5L);
            });
  }

  private void compact(DataFile output, DataFile... inputs) {
    RewriteFiles rewrite = table.newRewrite();
    for (DataFile input : inputs) {
      rewrite.deleteFile(input);
    }

    rewrite.addFile(output).commit();
    List<ManifestFile> manifests = table.currentSnapshot().dataManifests(table.io());
    assertThat(manifests)
        .as("the rewrite wrote a compaction map")
        .anyMatch(manifest -> manifest.compactionMapLocation() != null);
  }

  private DataFile dataFile(String name, long records) {
    return dataFile(table.spec(), name, records);
  }

  private static DataFile dataFile(PartitionSpec spec, String name, long records) {
    DataFiles.Builder builder =
        DataFiles.builder(spec)
            .withPath("/path/to/" + name + ".parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(records);
    if (spec.isPartitioned()) {
      builder.withPartitionPath(spec.fields().get(0).name() + "=0");
    }

    return builder.build();
  }

  private DeleteFile multiFileDelete(String name) {
    return FileMetadata.deleteFileBuilder(table.spec())
        .ofPositionDeletes()
        .withPath("/path/to/" + name + ".parquet")
        .withFileSizeInBytes(100)
        .withRecordCount(5)
        .build();
  }

  private DeleteFile fileScopedDelete(String name, DataFile dataFile) {
    FileMetadata.Builder builder =
        FileMetadata.deleteFileBuilder(table.specs().get(dataFile.specId()))
            .ofPositionDeletes()
            .withPath("/path/to/" + name + ".parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(1)
            .withReferencedDataFile(dataFile.location());
    if (dataFile.partition().size() > 0) {
      builder.withPartition(dataFile.partition());
    }

    return builder.build();
  }
}
