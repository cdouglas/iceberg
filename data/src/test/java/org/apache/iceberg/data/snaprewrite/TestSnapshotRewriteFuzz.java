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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.GenericSnapshotRewriteIO;
import org.apache.iceberg.data.WorkloadGenerator;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.snaprewrite.RewriteRefusedException;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Random histories checked by the full oracle.
 *
 * <p>Rows and delete positions come from {@link WorkloadGenerator}, the same generator the compaction
 * baseline benchmark uses, so the two harnesses explore the same data. Its clustered deletes matter
 * more here than the row shape: deleting contiguous runs rather than scattered singletons produces
 * long runs in the compaction map and contiguous stretches in the resurrection files, which is the
 * shape real "right to be forgotten" workloads have and the one a hand-rolled uniform sample never
 * generates.
 *
 * <p>The op sequence is this suite's own. {@code FuzzScenario} builds one compaction plus concurrent
 * late transactions over ~100k rows, which is a different question at a scale no unit test can carry;
 * a rewrite needs a window of interstitial commits between two compactions. Its weighted op mix --
 * position delete, append, row replacement -- is what is mirrored here.
 */
public class TestSnapshotRewriteFuzz extends SnapshotRewriteTestBase {

  @ParameterizedTest
  @ValueSource(
      longs = {
        1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L, 55L, 89L, 144L, 233L, 377L, 610L, 987L, 1597L, 2584L,
        4181L, 6765L, 10946L
      })
  public void randomHistory(long seed) throws IOException {
    // The generator's schema, and a fresh location per seed so a failure leaves the table behind.
    useSchema(WorkloadGenerator.SCHEMA, PartitionSpec.unpartitioned());

    Random random = new Random(seed);
    long rowSeed = seed;

    append(WorkloadGenerator.generateRows(rowSeed++, 8 + random.nextInt(8)));
    compactMaybeRolling(random);

    int operations = 3 + random.nextInt(5);
    for (int i = 0; i < operations; i += 1) {
      switch (random.nextInt(6)) {
        case 0:
          append(WorkloadGenerator.generateRows(rowSeed++, 1 + random.nextInt(4)));
          break;

        case 1:
          deleteClustered(random, rowSeed++);
          break;

        case 2:
          rowSeed = replaceRows(random, rowSeed);
          break;

        case 3:
          removeRandomFile(random);
          break;

        case 4:
          // Two delete commits in a row will often overlap on positions the first already removed.
          // That is legal, and the induction has to compute liveness by difference rather than
          // trusting a commit's delete set.
          deleteClustered(random, rowSeed++);
          deleteClustered(random, rowSeed++);
          break;

        default:
          table.newAppend().commit();
          break;
      }
    }

    if (liveFiles().isEmpty()) {
      append(WorkloadGenerator.generateRows(rowSeed++, 3));
    }

    compactMaybeRolling(random);

    try {
      SnapshotRewriteResult result = rewrite();
      assertLossless(result);
      assertIdentityPreserved(result);

      // Each row that died in the window is recovered exactly once, so recovered rows can never
      // exceed the rows that ever existed.
      assertThat(result.plan().resurrectedRows()).isLessThanOrEqualTo(totalRowsEverWritten());
    } catch (RewriteRefusedException e) {
      // A refusal is a valid outcome for a generated history; it must never be a silent wrong answer.
      assertThat(e.refusal()).isNotNull();
    }
  }

  // ------------------------------------------------------------------ workload

  /**
   * Deletes a clustered run of live positions from one file.
   *
   * <p>{@link WorkloadGenerator#generateClusteredPositions} produces contiguous runs, so the
   * surviving rows on either side stay contiguous too and the compaction map records few, long runs
   * rather than one run per row.
   */
  private void deleteClustered(Random random, long seed) throws IOException {
    List<FileScanTask> tasks = liveTasks();
    if (tasks.isEmpty()) {
      return;
    }

    FileScanTask task = tasks.get(random.nextInt(tasks.size()));
    List<Pair<CharSequence, Long>> targets =
        livePositions(task, clusteredPositions(random, seed, task, 1 + random.nextInt(4)));
    if (!targets.isEmpty()) {
      delete(targets);
    }
  }

  /** A row replacement: delete some rows and insert others in one commit. */
  private long replaceRows(Random random, long rowSeed) throws IOException {
    List<FileScanTask> tasks = liveTasks();
    if (tasks.isEmpty()) {
      append(WorkloadGenerator.generateRows(rowSeed, 2));
      return rowSeed + 1;
    }

    FileScanTask task = tasks.get(random.nextInt(tasks.size()));
    List<Pair<CharSequence, Long>> targets =
        livePositions(task, clusteredPositions(random, rowSeed, task, 1 + random.nextInt(2)));
    if (targets.isEmpty()) {
      append(WorkloadGenerator.generateRows(rowSeed, 2));
    } else {
      appendAndDelete(WorkloadGenerator.generateRows(rowSeed, 1 + random.nextInt(3)), targets);
    }

    return rowSeed + 1;
  }

  /**
   * Clustered positions inside one file, clamped to what the file can supply.
   *
   * <p>A rolled compaction leaves short trailing files -- sometimes a single row -- and the generator
   * rejects a request for more deletes than the file has positions.
   */
  private long[] clusteredPositions(Random random, long seed, FileScanTask task, int wanted) {
    long recordCount = task.file().recordCount();
    int capped = (int) Math.min(recordCount, wanted);
    if (capped <= 0) {
      return new long[0];
    }

    return WorkloadGenerator.generateClusteredPositions(
        seed, recordCount, capped, 1 + random.nextInt(3));
  }

  private void removeRandomFile(Random random) {
    List<DataFile> files = liveFiles();
    if (files.size() > 1) {
      removeFile(files.get(random.nextInt(files.size())));
    }
  }

  /**
   * Compacts, sometimes with a row cap so the output rolls.
   *
   * <p>Rolling makes one source file's rows land in several targets, which is the shape real
   * compactions produce at a target size and the one where per-run target files matter.
   */
  private void compactMaybeRolling(Random random) {
    if (random.nextBoolean()) {
      LocalCompactor.compact(table, true, file -> true, 2 + random.nextInt(4));
    } else {
      compact();
    }
  }

  // ------------------------------------------------------------------ table inspection

  /** Keeps only the generated positions that are still live, so deletes reference real rows. */
  private List<Pair<CharSequence, Long>> livePositions(FileScanTask task, long[] positions) {
    PositionDeleteIndex deleted =
        task.deletes().isEmpty()
            ? null
            : new GenericSnapshotRewriteIO(table)
                .loadPositionDeletes(task.deletes(), task.file().location());

    List<Pair<CharSequence, Long>> live = Lists.newArrayList();
    for (long position : positions) {
      if (position < task.file().recordCount() && (deleted == null || !deleted.isDeleted(position))) {
        live.add(Pair.of(task.file().location(), position));
      }
    }

    return live;
  }

  private List<FileScanTask> liveTasks() {
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
      for (FileScanTask task : planned) {
        tasks.add(task);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    tasks.sort(java.util.Comparator.comparing(task -> task.file().location()));
    return tasks;
  }

  private List<DataFile> liveFiles() {
    List<DataFile> files = Lists.newArrayList();
    for (FileScanTask task : liveTasks()) {
      files.add(task.file());
    }

    return files;
  }

  private long totalRowsEverWritten() {
    long total = 0;
    for (org.apache.iceberg.Snapshot snapshot : table.snapshots()) {
      for (DataFile file : snapshot.addedDataFiles(table.io())) {
        total += file.recordCount();
      }
    }

    return total;
  }
}
