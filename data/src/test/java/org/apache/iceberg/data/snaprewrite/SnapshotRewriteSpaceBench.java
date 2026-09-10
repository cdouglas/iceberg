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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.WorkloadGenerator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.snaprewrite.SnapshotRewriteReport;
import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;

/**
 * Sweeps the two workload parameters that decide whether a snapshot rewrite pays, and writes the
 * byte accounting to CSV.
 *
 * <p>Every figure this emits comes from {@link SnapshotRewriteReport}, whose numbers are read off
 * the files the rewrite actually wrote. Nothing here simulates a cost or a latency. What it adds
 * over {@code TestSnapshotRewriteScale} and {@code TestSnapshotRewriteDeleteCost} is scale and a
 * sweep: those pin the accounting at 20-80k rows in a single data file and assert bounds, this runs
 * the same code over a parameter range at up to 500k rows across a multi-file compaction and
 * reports a curve.
 *
 * <p>Two sweeps, each isolating one term of the cost model:
 *
 * <ul>
 *   <li><b>mix</b> — a window of fixed size whose row-changes shift from all inserts to mostly
 *       deletes. Isolates the recovery term: the rewrite must materialize one copy of every row
 *       that died in the window.
 *   <li><b>depth</b> — an insert-only window of growing depth, across both format versions and both
 *       schema widths. Nothing is recovered, so the delete vectors and the per-snapshot metadata
 *       are the entire cost.
 * </ul>
 *
 * <p>This is not a test. It asserts the one invariant the accounting rests on -- each row that died
 * is recovered exactly once -- and leaves the losslessness oracle to the test suite, which can
 * afford to read every snapshot of every table.
 */
public class SnapshotRewriteSpaceBench extends SnapshotRewriteTestBase {

  /**
   * Clustered runs of this length: the "right to be forgotten" purge shape, not a uniform sample.
   */
  private static final int DELETE_RUN_LENGTH = 100;

  private static final String[] COLUMNS = {
    "figure",
    "preset",
    "seed",
    "format_version",
    "schema_cols",
    "base_rows",
    "base_files",
    "window_commits",
    "window_inserts",
    "window_deletes",
    "delete_share",
    "target_files",
    "target_rows",
    "resurrected_rows",
    "resurrected_bytes",
    "delete_positions",
    "delete_bytes",
    "metadata_bytes",
    "reclaimable_bytes",
    "source_compaction_bytes",
    "interstitial_bytes",
    "target_bytes",
    "added_bytes",
    "saved_bytes",
    "dead_ratio",
    "setup_millis",
    "rewrite_millis",
    "est_delete_bytes",
    "est_metadata_bytes",
    "est_saved_bytes"
  };

  private int formatVersion = 3;

  /** Bytes of {@code C_A}'s output: the layer the rewrite detaches. */
  private long sourceCompactionBytes = 0;

  // ------------------------------------------------------------------ entry point

  public static void main(String[] args) throws IOException {
    BenchArgs parsed = BenchArgs.parse(args);
    Files.createDirectories(Paths.get(parsed.outDir));

    if (parsed.figure.equals("mix") || parsed.figure.equals("all")) {
      runMix(parsed);
    }

    if (parsed.figure.equals("depth") || parsed.figure.equals("all")) {
      runDepth(parsed);
    }
  }

  // ------------------------------------------------------------------ figure 1: the mix

  /**
   * A window of fixed depth whose row-changes shift from all inserts to mostly deletes.
   *
   * <p>Total changes are held equal to the base row count, so the only thing moving is the split.
   * Live rows at {@code C_B} are {@code 2B(1-d)} and rows recovered are {@code Bd}: the saving and
   * its price move in opposite directions along one axis. The default {@code maxDeadRatio} guard
   * lands at {@code d = 0.5}, where {@code recovered/live = 0.5d/(1-d)} is exactly 0.5.
   */
  private static void runMix(BenchArgs args) throws IOException {
    double[] shares = {0.0, 0.1, 0.25, 0.4, 0.5, 0.6, 0.75, 0.9};
    int base = args.full ? 500_000 : 50_000;
    int chunk = args.full ? 100_000 : 10_000;
    int commits = 8;

    Csv csv = new Csv(Paths.get(args.outDir, "mix.csv"));
    try {
      for (double share : shares) {
        SnapshotRewriteSpaceBench bench = new SnapshotRewriteSpaceBench();
        File dir = bench.newScratch(args);
        try {
          csv.write(bench.mixPoint(args, base, chunk, commits, share));
        } finally {
          deleteRecursively(dir);
        }
      }
    } finally {
      csv.close();
    }
  }

  private Map<String, Object> mixPoint(
      BenchArgs args, int baseRows, int chunkRows, int commits, double deleteShare)
      throws IOException {
    this.formatVersion = 3;
    useSchema(WorkloadGenerator.SCHEMA, PartitionSpec.unpartitioned(), 3);

    int totalInserts = (int) Math.round(baseRows * (1.0 - deleteShare));
    int totalDeletes = (int) Math.round(baseRows * deleteShare);
    Preconditions.checkArgument(
        totalDeletes <= baseRows, "Cannot delete %s of %s base rows", totalDeletes, baseRows);

    Random rng = new Random(args.seed);
    long seed = args.seed;
    long setupStart = System.nanoTime();

    // Base written in chunks so no single generate/read/compact step holds the whole table.
    for (int written = 0; written < baseRows; written += chunkRows) {
      append(WorkloadGenerator.generateRows(seed++, Math.min(chunkRows, baseRows - written)));
    }

    // C_A, rolled so the window's deletes hit one 100k-row file rather than the whole base: the
    // recovery read scans a whole source file per request, so file size sets that cost.
    LocalCompactor.compact(table, true, file -> true, chunkRows);
    List<LiveRuns> pools = Lists.newArrayList();
    this.sourceCompactionBytes = 0;
    for (DataFile file : dataFiles(table.currentSnapshot())) {
      pools.add(new LiveRuns(file.location(), (int) file.recordCount()));
      this.sourceCompactionBytes += file.fileSizeInBytes();
    }

    Map<String, DeleteFile> currentDeletes = Maps.newHashMap();
    int insertsDone = 0;
    int deletesDone = 0;
    for (int commit = 0; commit < commits; commit += 1) {
      int inserts = share(totalInserts, insertsDone, commit, commits);
      int deletes = share(totalDeletes, deletesDone, commit, commits);
      insertsDone += inserts;
      deletesDone += deletes;

      DataFile added =
          inserts == 0 ? null : writeData(WorkloadGenerator.generateRows(seed++, inserts));
      commitChange(rng, pools, currentDeletes, commit, added, deletes);
    }

    Preconditions.checkState(
        insertsDone == totalInserts && deletesDone == totalDeletes,
        "Window applied %s/%s inserts and %s/%s deletes",
        insertsDone,
        totalInserts,
        deletesDone,
        totalDeletes);

    LocalCompactor.compact(table, true, file -> true, chunkRows);
    long setupMillis = (System.nanoTime() - setupStart) / 1_000_000;

    return measure(
        args,
        "mix",
        baseRows,
        baseRows / chunkRows,
        commits,
        totalInserts,
        totalDeletes,
        deleteShare,
        setupMillis);
  }

  /**
   * Commits one transaction of the window: an append and a set of deletes, together.
   *
   * <p>This is the MERGE shape -- {@code I_k} and {@code D_k} both non-empty in the same step --
   * rather than separate append and delete commits.
   */
  private void commitChange(
      Random rng,
      List<LiveRuns> pools,
      Map<String, DeleteFile> currentDeletes,
      int commit,
      DataFile added,
      int deletes)
      throws IOException {
    RowDelta delta = table.newRowDelta().validateFromSnapshot(table.currentSnapshot().snapshotId());
    if (added != null) {
      delta.addRows(added);
    }

    // Round-robin from a different file each commit, spilling forward when one empties. Keeping a
    // transaction's deaths inside as few files as possible is what bounds the recovery read.
    int remaining = deletes;
    for (int i = 0; i < pools.size() && remaining > 0; i += 1) {
      LiveRuns pool = pools.get((commit + i) % pools.size());
      int take = Math.min(remaining, pool.remaining());
      if (take == 0) {
        continue;
      }

      long[] taken = pool.takeClustered(rng, take, DELETE_RUN_LENGTH);
      remaining -= take;

      // Under v3 a data file carries at most one deletion vector per snapshot, so a new delete
      // means rewriting the whole vector and superseding the old one. Under v2 the files simply
      // accumulate. Either way the resulting table state is what a real writer would produce.
      DeleteFile written =
          writeDeletes(formatVersion >= 3 ? pool.deadSoFar() : pool.asTargets(taken));
      delta.addDeletes(written);
      DeleteFile superseded = currentDeletes.put(pool.path(), written);
      if (formatVersion >= 3 && superseded != null) {
        delta.removeDeletes(superseded);
      }
    }

    Preconditions.checkState(
        remaining == 0, "Ran out of live positions with %s deletes left", remaining);
    delta.commit();
  }

  // ------------------------------------------------------------------ figure 2: window depth

  /**
   * An insert-only window of growing depth, across both format versions and both schema widths.
   *
   * <p>Insert-only means nothing is recovered, so the delete vectors and the per-snapshot metadata
   * are the whole cost -- the isolation {@code TestSnapshotRewriteDeleteCost} uses, swept over
   * depth rather than sampled at three points. Per-commit volume is held fixed, so masked positions
   * grow as {@code r * m^2 / 2}: quadratic in bytes under v2, and nearly free under v3, where an
   * order-preserving compaction lays each snapshot's masked rows out consecutively and Roaring
   * stores the range as a run.
   */
  private static void runDepth(BenchArgs args) throws IOException {
    int[] depths = args.full ? new int[] {1, 2, 4, 8, 16, 24, 32} : new int[] {1, 2, 4, 8, 16};
    int base = args.full ? 100_000 : 50_000;
    int perCommit = 2_000;

    Csv csv = new Csv(Paths.get(args.outDir, "depth.csv"));
    try {
      for (int version : new int[] {3, 2}) {
        for (boolean wide : new boolean[] {true, false}) {
          for (int depth : depths) {
            SnapshotRewriteSpaceBench bench = new SnapshotRewriteSpaceBench();
            File dir = bench.newScratch(args);
            try {
              csv.write(bench.depthPoint(args, version, wide, base, perCommit, depth));
            } finally {
              deleteRecursively(dir);
            }
          }
        }
      }
    } finally {
      csv.close();
    }
  }

  private Map<String, Object> depthPoint(
      BenchArgs args, int version, boolean wide, int baseRows, int perCommit, int commits)
      throws IOException {
    this.formatVersion = version;
    Schema schema = wide ? WorkloadGenerator.SCHEMA : SCHEMA;
    useSchema(schema, PartitionSpec.unpartitioned(), version);

    long seed = args.seed + version * 1000L + (wide ? 100 : 0) + commits;
    long setupStart = System.nanoTime();

    append(rows(schema, seed++, baseRows, 0));
    LocalCompactor.compact(table, true, file -> true, baseRows);
    this.sourceCompactionBytes = 0;
    for (DataFile file : dataFiles(table.currentSnapshot())) {
      this.sourceCompactionBytes += file.fileSizeInBytes();
    }

    for (int commit = 0; commit < commits; commit += 1) {
      append(rows(schema, seed++, perCommit, baseRows + commit * perCommit));
    }

    LocalCompactor.compact(table, true, file -> true, baseRows + commits * perCommit);
    long setupMillis = (System.nanoTime() - setupStart) / 1_000_000;

    return measure(args, "depth", baseRows, 1, commits, perCommit * commits, 0, 0.0, setupMillis);
  }

  // ------------------------------------------------------------------ measurement

  /** Runs the rewrite, checks the accounting invariant, and collects one CSV row. */
  private Map<String, Object> measure(
      BenchArgs args,
      String figure,
      int baseRows,
      int baseFiles,
      int commits,
      int windowInserts,
      int windowDeletes,
      double deleteShare,
      long setupMillis) {
    SnapshotRewriteReport estimate = args.withEstimate ? rewriter().estimate() : null;

    long start = System.nanoTime();
    SnapshotRewriteResult result = rewriter().materialize();
    long rewriteMillis = (System.nanoTime() - start) / 1_000_000;
    SnapshotRewriteReport report = result.report();

    // The bound the whole cost argument rests on: each row that died in the window is materialized
    // exactly once. LiveRuns hands out disjoint live positions, so the count is exact by
    // construction and a mismatch means the numbers below describe a rewrite that is wrong.
    Preconditions.checkState(
        report.resurrectedRows() == windowDeletes,
        "%s: recovered %s rows but %s died",
        figure,
        report.resurrectedRows(),
        windowDeletes);

    Map<String, Object> row = Maps.newLinkedHashMap();
    row.put("figure", figure);
    row.put("preset", args.full ? "full" : "quick");
    row.put("seed", args.seed);
    row.put("format_version", formatVersion);
    row.put("schema_cols", table.schema().columns().size());
    row.put("base_rows", baseRows);
    row.put("base_files", baseFiles);
    row.put("window_commits", commits);
    row.put("window_inserts", windowInserts);
    row.put("window_deletes", windowDeletes);
    row.put("delete_share", deleteShare);
    row.put("target_files", targetFileCount());
    row.put("target_rows", targetRows(report));
    row.put("resurrected_rows", report.resurrectedRows());
    row.put("resurrected_bytes", report.resurrectedBytes());
    row.put("delete_positions", report.deletePositions());
    row.put("delete_bytes", report.deleteBytes());
    row.put("metadata_bytes", report.metadataBytes());
    row.put("reclaimable_bytes", report.reclaimableBytes());
    // Reclaimable is C_A's output plus everything the window added on top of it. Splitting
    // it lets a reader see the layers the rewrite detaches rather than one total.
    row.put("source_compaction_bytes", sourceCompactionBytes);
    row.put("interstitial_bytes", report.reclaimableBytes() - sourceCompactionBytes);
    // predictedSavedBytes() is the compacted table's own size, which is all it is used for
    // here, so the column says that rather than carrying the prediction as a concept.
    row.put("target_bytes", report.predictedSavedBytes());
    row.put("added_bytes", report.addedBytes());
    row.put("saved_bytes", report.savedBytes());
    row.put("dead_ratio", report.deadRatio());
    row.put("setup_millis", setupMillis);
    row.put("rewrite_millis", rewriteMillis);
    row.put("est_delete_bytes", estimate == null ? "" : estimate.deleteBytes());
    row.put("est_metadata_bytes", estimate == null ? "" : estimate.metadataBytes());
    row.put("est_saved_bytes", estimate == null ? "" : estimate.savedBytes());

    System.out.printf(
        Locale.ROOT,
        "%-6s v%d %2dcol base=%7d m=%2d ins=%7d del=%7d d=%.2f | reclaim %8.2f MiB "
            + "added %7.2f (res %6.2f dv %6.3f meta %6.3f) net %8.2f | C_A %7.2f "
            + "inter %6.2f C_B %7.2f | %5.1fs%n",
        figure,
        formatVersion,
        table.schema().columns().size(),
        baseRows,
        commits,
        windowInserts,
        windowDeletes,
        deleteShare,
        report.reclaimableBytes() / 1048576.0,
        report.addedBytes() / 1048576.0,
        report.resurrectedBytes() / 1048576.0,
        report.deleteBytes() / 1048576.0,
        report.metadataBytes() / 1048576.0,
        report.savedBytes() / 1048576.0,
        sourceCompactionBytes / 1048576.0,
        (report.reclaimableBytes() - sourceCompactionBytes) / 1048576.0,
        report.predictedSavedBytes() / 1048576.0,
        (setupMillis + rewriteMillis) / 1000.0);

    return row;
  }

  // ------------------------------------------------------------------ helpers

  private long targetRows(SnapshotRewriteReport report) {
    // targetRows is not exposed directly; deadRatio() is resurrectedRows / targetRows.
    return report.deadRatio() == 0.0
        ? currentRowCount()
        : Math.round(report.resurrectedRows() / report.deadRatio());
  }

  private long currentRowCount() {
    long rows = 0;
    for (DataFile file : dataFiles(table.currentSnapshot())) {
      rows += file.recordCount();
    }

    return rows;
  }

  private int targetFileCount() {
    return dataFiles(table.currentSnapshot()).size();
  }

  private List<DataFile> dataFiles(Snapshot snapshot) {
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    List<DataFile> files = Lists.newArrayList();
    for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, table.io(), metadata.specsById())) {
        for (DataFile file : reader) {
          files.add(file.copy());
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    files.sort(Comparator.comparing(DataFile::location));
    return files;
  }

  private static List<Record> rows(Schema schema, long seed, int count, int fromId) {
    return schema.columns().size() > 2
        ? WorkloadGenerator.generateRows(seed, count)
        : records(fromId, count, "w");
  }

  /** Splits a total evenly across commits, giving the remainder to the last one. */
  private static int share(int total, int done, int commit, int commits) {
    return commit == commits - 1 ? total - done : total / commits;
  }

  private File newScratch(BenchArgs args) throws IOException {
    Path dir = Files.createTempDirectory(Paths.get(args.tmpDir), "snaprewrite-bench-");
    this.tempDir = dir.toFile();
    return this.tempDir;
  }

  private static void deleteRecursively(File dir) {
    try (Stream<Path> paths = Files.walk(dir.toPath())) {
      paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    } catch (IOException e) {
      System.err.println("Could not clean " + dir + ": " + e.getMessage());
    }
  }

  // ------------------------------------------------------------------ CSV

  private static class Csv implements AutoCloseable {
    private final Writer writer;

    Csv(Path path) throws IOException {
      this.writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
      writer.write(String.join(",", COLUMNS));
      writer.write("\n");
      System.out.println("writing " + path.toAbsolutePath());
    }

    void write(Map<String, Object> row) throws IOException {
      StringBuilder line = new StringBuilder();
      for (int i = 0; i < COLUMNS.length; i += 1) {
        Object value = row.get(COLUMNS[i]);
        Preconditions.checkState(value != null, "Missing CSV column %s", COLUMNS[i]);
        if (i > 0) {
          line.append(',');
        }

        line.append(value instanceof Double ? String.format(Locale.ROOT, "%.6f", value) : value);
      }

      writer.write(line.append('\n').toString());
      writer.flush();
    }

    @Override
    public void close() throws IOException {
      writer.close();
    }
  }

  // ------------------------------------------------------------------ arguments

  private static class BenchArgs {
    private String figure = "all";
    private boolean full = false;
    private String outDir = "benchmark/snaprewrite-space/results";
    private String tmpDir = System.getProperty("java.io.tmpdir");
    private long seed = 20260909L;
    private boolean withEstimate = false;
    private boolean estimateSet = false;

    static BenchArgs parse(String[] args) {
      BenchArgs parsed = new BenchArgs();
      for (int i = 0; i < args.length; i += 1) {
        switch (args[i]) {
          case "--figure":
            parsed.figure = args[++i];
            break;
          case "--preset":
            parsed.full = "full".equals(args[++i]);
            break;
          case "--out":
            parsed.outDir = args[++i];
            break;
          case "--tmp":
            parsed.tmpDir = args[++i];
            break;
          case "--seed":
            parsed.seed = Long.parseLong(args[++i]);
            break;
          case "--with-estimate":
            parsed.withEstimate = true;
            parsed.estimateSet = true;
            break;
          case "--no-estimate":
            parsed.withEstimate = false;
            parsed.estimateSet = true;
            break;
          default:
            throw new IllegalArgumentException(
                "Unknown argument: "
                    + args[i]
                    + "\nUsage: --figure mix|depth|all --preset quick|full --out DIR "
                    + "--tmp DIR --seed N [--with-estimate|--no-estimate]");
        }
      }

      Preconditions.checkArgument(
          parsed.figure.matches("mix|depth|all"), "--figure must be mix, depth or all");
      if (!parsed.estimateSet) {
        // estimate() re-runs the whole induction, so it doubles the expensive phase. Worth it at
        // the quick preset, where it prices the dry run against the measurement; not at full scale.
        parsed.withEstimate = !parsed.full;
      }

      return parsed;
    }
  }
}
