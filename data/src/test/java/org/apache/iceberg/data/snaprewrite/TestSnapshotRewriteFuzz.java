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
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.snaprewrite.RewriteRefusedException;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Random histories over the same operation mix, checked by the full oracle.
 *
 * <p>The hand-written cases each pin down one situation the design has to handle. This looks for
 * the combinations nobody thought to write down: a delete landing on rows another commit already
 * removed, a file emptied and then removed, an insert whose rows die across two different
 * transactions. Seeds are fixed so a failure reproduces.
 */
public class TestSnapshotRewriteFuzz extends SnapshotRewriteTestBase {

  @ParameterizedTest
  @ValueSource(
      longs = {
        1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L, 55L, 89L, 144L, 233L, 377L, 610L, 987L, 1597L, 2584L,
        4181L, 6765L, 10946L
      })
  public void randomHistory(long seed) throws IOException {
    // A fresh location per seed, so a failure leaves the offending table behind for inspection.
    usePartitionSpec(PartitionSpec.unpartitioned());

    Random random = new Random(seed);
    int nextId = 1;

    append(records(nextId, 6 + random.nextInt(6), "base"));
    nextId += 100;
    compact();

    int operations = 3 + random.nextInt(5);
    for (int i = 0; i < operations; i += 1) {
      switch (random.nextInt(6)) {
        case 0:
          append(records(nextId, 1 + random.nextInt(4), "ins" + i));
          nextId += 100;
          break;

        case 1:
          deleteSome(random, 1 + random.nextInt(3));
          break;

        case 2:
          {
            List<Pair<CharSequence, Long>> targets = liveSample(random, 1 + random.nextInt(2));
            if (targets.isEmpty()) {
              append(records(nextId, 2, "ins" + i));
              nextId += 100;
            } else {
              appendAndDelete(records(nextId, 1 + random.nextInt(3), "mix" + i), targets);
              nextId += 100;
            }
          }

          break;

        case 3:
          removeRandomFile(random);
          break;

        case 4:
          // Re-delete positions that may already be dead: legal, and the induction has to compute
          // liveness by difference rather than trusting the commit's delete set.
          deleteSome(random, 1 + random.nextInt(3));
          deleteSome(random, 1 + random.nextInt(3));
          break;

        default:
          table.newAppend().commit();
          break;
      }
    }

    if (table.currentSnapshot() == null || liveFileCount() == 0) {
      append(records(nextId, 3, "tail"));
    }

    compact();

    try {
      SnapshotRewriteResult result = rewrite();
      assertLossless(result);
      assertIdentityPreserved(result);

      // Each row that died in the window is recovered exactly once, so recovered rows can never
      // exceed the rows that ever existed.
      assertThat(result.plan().resurrectedRows()).isLessThanOrEqualTo(totalRowsEverWritten());
    } catch (RewriteRefusedException e) {
      // A refusal is a valid outcome for a generated history; it must never be a silent wrong
      // answer.
      assertThat(e.refusal()).isNotNull();
    }
  }

  // ------------------------------------------------------------------ workload helpers

  private void deleteSome(Random random, int count) throws IOException {
    List<Pair<CharSequence, Long>> targets = liveSample(random, count);
    if (!targets.isEmpty()) {
      delete(targets);
    }
  }

  private void removeRandomFile(Random random) {
    List<DataFile> files = liveFiles();
    if (files.size() > 1) {
      removeFile(files.get(random.nextInt(files.size())));
    }
  }

  /** Samples live positions, so generated deletes reference rows that actually exist. */
  private List<Pair<CharSequence, Long>> liveSample(Random random, int count) {
    List<Pair<CharSequence, Long>> candidates = Lists.newArrayList();
    GenericSnapshotRewriteIO io = new GenericSnapshotRewriteIO(table);

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        PositionDeleteIndex deleted =
            task.deletes().isEmpty()
                ? null
                : io.loadPositionDeletes(task.deletes(), task.file().location());
        for (long position = 0; position < task.file().recordCount(); position += 1) {
          if (deleted == null || !deleted.isDeleted(position)) {
            candidates.add(Pair.of(task.file().location(), position));
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    List<Pair<CharSequence, Long>> chosen = Lists.newArrayList();
    for (int i = 0; i < count && !candidates.isEmpty(); i += 1) {
      chosen.add(candidates.remove(random.nextInt(candidates.size())));
    }

    return chosen;
  }

  private List<DataFile> liveFiles() {
    List<DataFile> files = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        files.add(task.file());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    files.sort(java.util.Comparator.comparing(DataFile::location));
    return files;
  }

  private int liveFileCount() {
    return liveFiles().size();
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
