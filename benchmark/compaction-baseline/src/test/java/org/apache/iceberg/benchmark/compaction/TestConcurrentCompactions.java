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
package org.apache.iceberg.benchmark.compaction;

import org.apache.iceberg.data.WorkloadGenerator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * M3 — Concurrent Compactions Test (per {@code COMPACT_SPEC.md} §M3).
 *
 * <p>Verifies the documented contract that two compactions racing on the same data files commit
 * cleanly: the first wins, the second fails with {@link ValidationException}. Compactions on
 * disjoint file sets both succeed and compose.
 *
 * <p>The contract is enforced by {@link RewriteFiles#validateFromSnapshot(long)} in core:
 * checking the "validation history" between {@code startingSnapshotId} and the parent at commit
 * time rejects any rewrite that overlaps with files removed (or added) by an intervening
 * commit. This test exercises that path directly via the core {@code Table.newRewrite()} API
 * (the Spark {@code rewriteDataFiles} action wraps the same machinery; see
 * {@link org.apache.iceberg.spark.actions.SparkRewriteDataFilesCommitManager}).
 *
 * <p>Compaction maps do NOT auto-repair against other compactions per {@code
 * compaction_maps_errata.md}: only late position-delete transactions are auto-repaired.
 */
class TestConcurrentCompactions {

  @TempDir File tempDir;

  @Test
  void overlappingCompactionsRejectsSecondCommit() throws IOException {
    HadoopCatalog catalog = newCatalog("overlap");
    Table table = createTable(catalog, "overlap");
    List<DataFile> files = appendInitialDataFiles(table, 4);
    long startingSnapshotId = table.currentSnapshot().snapshotId();

    // First rewrite: delete files[0], files[1], add a new file.
    DataFile mergedA = makeFakeDataFile("merged_a.parquet", 200);
    RewriteFiles rewriteA =
        table
            .newRewrite()
            .validateFromSnapshot(startingSnapshotId)
            .deleteFile(files.get(0))
            .deleteFile(files.get(1))
            .addFile(mergedA);
    rewriteA.commit();
    table.refresh();
    assertThat(table.currentSnapshot().operation()).isEqualTo("replace");

    // Second rewrite, planned against the SAME starting snapshot (mimics a concurrent compaction
    // that read at the same point in time): targets files[1], files[2] — overlaps with rewriteA
    // on files[1]. The commit must fail; the validation history walker will see files[1] as
    // already-deleted by rewriteA and reject.
    DataFile mergedB = makeFakeDataFile("merged_b.parquet", 200);
    RewriteFiles rewriteB =
        table
            .newRewrite()
            .validateFromSnapshot(startingSnapshotId)
            .deleteFile(files.get(1))
            .deleteFile(files.get(2))
            .addFile(mergedB);

    assertThatThrownBy(rewriteB::commit)
        .isInstanceOf(ValidationException.class)
        .satisfiesAnyOf(
            e -> assertThat(e.getMessage()).containsIgnoringCase("deleted"),
            e -> assertThat(e.getMessage()).containsIgnoringCase("missing"),
            e -> assertThat(e.getMessage()).containsIgnoringCase("conflict"));

    // No silent data loss: the table still contains rewriteA's output plus the untouched files.
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId())
        .as("Second rewrite must not have advanced the table")
        .isNotEqualTo(rewriteB);
    // Live data files: mergedA + files[2] + files[3] = 3.
    assertThat(currentDataFileCount(table)).isEqualTo(3);
  }

  @Test
  void disjointCompactionsBothSucceed() throws IOException {
    HadoopCatalog catalog = newCatalog("disjoint");
    Table table = createTable(catalog, "disjoint");
    List<DataFile> files = appendInitialDataFiles(table, 4);
    long startingSnapshotId = table.currentSnapshot().snapshotId();

    // First rewrite: files[0] + files[1] → mergedA.
    DataFile mergedA = makeFakeDataFile("merged_a.parquet", 200);
    table
        .newRewrite()
        .validateFromSnapshot(startingSnapshotId)
        .deleteFile(files.get(0))
        .deleteFile(files.get(1))
        .addFile(mergedA)
        .commit();
    table.refresh();

    // Second rewrite: files[2] + files[3] → mergedB. Planned from the same starting snapshot,
    // but operating on a disjoint subset, so the validator finds no conflict.
    DataFile mergedB = makeFakeDataFile("merged_b.parquet", 200);
    table
        .newRewrite()
        .validateFromSnapshot(startingSnapshotId)
        .deleteFile(files.get(2))
        .deleteFile(files.get(3))
        .addFile(mergedB)
        .commit();
    table.refresh();

    // Both rewrites landed: the live state is exactly {mergedA, mergedB}.
    assertThat(currentDataFileCount(table)).isEqualTo(2);
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private HadoopCatalog newCatalog(String dir) {
    File root = new File(tempDir, dir);
    return new HadoopCatalog(new Configuration(), root.toURI().toString());
  }

  private static Table createTable(HadoopCatalog catalog, String name) {
    return catalog.createTable(
        TableIdentifier.of("db", name),
        WorkloadGenerator.SCHEMA,
        PartitionSpec.unpartitioned(),
        null,
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
  }

  /**
   * Materialize {@code numFiles} small Parquet files on disk via {@link WorkloadCommitter} and
   * commit them as one append snapshot. Returns the committed {@link DataFile}s.
   */
  private static List<DataFile> appendInitialDataFiles(Table table, int numFiles)
      throws IOException {
    OutputFileFactory factory = WorkloadCommitter.parquetFileFactory(table, 0, 0L);
    List<DataFile> files =
        WorkloadCommitter.writeDataFiles(table, factory, /* seed */ 7L, numFiles * 100L, 100);
    assertThat(files).hasSize(numFiles);

    AppendFiles append = table.newAppend();
    files.forEach(append::appendFile);
    append.commit();
    table.refresh();
    return files;
  }

  /**
   * Build a synthetic {@link DataFile} with the schema's spec but a fake path. The path doesn't
   * need to point at a real file — RewriteFiles validation only inspects metadata, not contents.
   */
  private DataFile makeFakeDataFile(String name, long recordCount) {
    File fake = new File(tempDir, name);
    return org.apache.iceberg.DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(fake.toURI().toString())
        .withFormat(org.apache.iceberg.FileFormat.PARQUET)
        .withRecordCount(recordCount)
        .withFileSizeInBytes(1024L)
        .build();
  }

  private static int currentDataFileCount(Table table) {
    List<DataFile> live = Lists.newArrayList();
    table.currentSnapshot().addedDataFiles(table.io()).forEach(live::add);
    // addedDataFiles only includes added-this-snapshot; sum running total via the summary.
    String total = table.currentSnapshot().summary().get("total-data-files");
    return total == null ? live.size() : Integer.parseInt(total);
  }
}
