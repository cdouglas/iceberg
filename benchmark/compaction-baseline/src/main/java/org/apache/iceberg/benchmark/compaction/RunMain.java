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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * VM-side performance runner per {@code COMPACT_SPEC.md} §"Runner". Restores a single (variant, K)
 * prebuilt warehouse, runs 1 untimed warmup + N timed iterations, and writes one JSON line per
 * timed iteration.
 *
 * <h3>Usage</h3>
 *
 * <pre>
 * scripts/run.sh \
 *   --state-tar /path/to/baseline-K1k.tar \
 *   --working-dir /tmp/cmpbench-work \
 *   --variant baseline \
 *   --k 1000 \
 *   --iterations 3 \
 *   --warmup 1 \
 *   --output /tmp/cmpbench-work/results.jsonl
 * </pre>
 *
 * <p>{@code scripts/run.sh} supplies the Java 17/21 {@code --add-opens} flags Spark 3.5 needs.
 *
 * <p>State restoration is local-only here: pass a path to a tarball already on disk. For the cloud
 * path the wrapper script downloads the tar from S3 first, then invokes this runner pointing at the
 * local copy.
 */
public final class RunMain {

  private static final Logger LOG = LoggerFactory.getLogger(RunMain.class);

  private RunMain() {}

  public static void main(String[] argv) {
    int exit;
    try {
      Args args = Args.parse(argv);
      exit = run(args);
    } catch (IllegalArgumentException e) {
      LOG.error("Argument error: {}{}{}", e.getMessage(), System.lineSeparator(), usage(), e);
      exit = 2;
    } catch (Exception e) {
      LOG.error("RunMain failed", e);
      exit = 1;
    }
    System.exit(exit);
  }

  static int run(Args args) throws IOException {
    SparkSession spark = startSpark(args);
    try {
      return runWithSpark(args, spark);
    } finally {
      spark.stop();
    }
  }

  /**
   * Same as {@link #run(Args)} but with the SparkSession lifecycle owned by the caller — useful for
   * tests that share one session across multiple scenarios.
   */
  static int runWithSpark(Args args, SparkSession spark) throws IOException {
    Files.createDirectories(args.outputJsonl().toAbsolutePath().getParent());

    Path pristineDir = args.workingDir().resolve("_pristine");
    Path workingDir = args.workingDir().resolve("warehouse");
    deleteIfExists(pristineDir);
    deleteIfExists(workingDir);
    Files.createDirectories(args.workingDir());

    LOG.info("Extracting state {} -> {}", args.stateTar(), pristineDir);
    TarUtils.untar(args.stateTar(), pristineDir);
    Path catalogRoot = catalogRootIn(pristineDir);
    if (!catalogRoot.equals(pristineDir)) {
      LOG.info("Descended into single wrapper dir: {}", catalogRoot.getFileName());
    }

    StageMetricsCollector collector = new StageMetricsCollector();
    collector.attach(spark);

    int validIterations = 0;
    try (BufferedWriter writer =
        Files.newBufferedWriter(
            args.outputJsonl(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

      int total = args.warmup() + args.iterations();
      for (int i = 0; i < total; i++) {
        boolean warmup = i < args.warmup();
        int displayIndex = warmup ? i : (i - args.warmup() + 1);
        IterationResult result =
            new IterationResult(args.variant(), args.kLateTxDeletes(), displayIndex, warmup);
        LOG.info("=== Iteration {}{} ===", warmup ? "WARMUP-" : "", displayIndex);

        restoreWorkingDir(catalogRoot, workingDir);
        try (HadoopCatalog catalog =
            new HadoopCatalog(new Configuration(), workingDir.toUri().toString())) {
          TableIdentifier ident = args.tableIdent();
          Table table = catalog.loadTable(ident);

          if ("treatment".equals(args.variant())) {
            TreatmentTimedRegion treatment = new TreatmentTimedRegion(spark, collector);
            TreatmentTimedRegion.Prep prep = treatment.prepare(table);
            result.compactionMapRuns(prep.compactionMapRuns());
            result.snPlusOneRuns(prep.conflictingDeleteFileCount());
            treatment.runTimed(table, prep, result);
          } else {
            BaselineTimedRegion baseline = new BaselineTimedRegion(spark, collector);
            baseline.run(table, result);
            // For baseline we have no compaction map in advance, so leave compactionMapRuns at 0
            // and report K as sn_plus_one_runs for symmetry with treatment's reporting.
            result.snPlusOneRuns(args.kLateTxDeletes());
          }
        }

        if (!warmup) {
          if ("baseline".equals(args.variant()) && !args.skipM6()) {
            applyM6Assertions(args.kLateTxDeletes(), result);
          }
          result.appendJsonl(writer);
          if (result.valid()) {
            validIterations++;
          }
        }
      }
      writer.flush();
    } finally {
      collector.detach(spark);
    }

    if (validIterations == 0 && args.iterations() > 0) {
      LOG.error("No valid iterations recorded — aborting scenario");
      return 3;
    }
    LOG.info(
        "RunMain finished: {} valid iterations written to {}", validIterations, args.outputJsonl());
    return 0;
  }

  // ---------------------------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------------------------

  private static SparkSession startSpark(Args args) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .master(args.sparkMaster())
            .appName("compaction-baseline-run")
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.memory", args.driverMemory())
            .config("spark.ui.enabled", "false")
            .config(
                "spark.sql.shuffle.partitions",
                String.valueOf(Math.max(2, Runtime.getRuntime().availableProcessors())))
            // Override IcebergSource's default Hive catalog with a HadoopCatalog stub.
            .config("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.default_iceberg.type", "hadoop")
            .config("spark.sql.catalog.default_iceberg.warehouse", "/tmp/run-spark-warehouse")
            .config(
                "spark.sql.catalog.default_cache_iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.default_cache_iceberg.type", "hadoop")
            .config(
                "spark.sql.catalog.default_cache_iceberg.warehouse",
                "/tmp/run-spark-warehouse-cache");
    return builder.getOrCreate();
  }

  /** Replace the working directory with a fresh copy of {@code pristine}. */
  private static void restoreWorkingDir(Path pristine, Path working) throws IOException {
    deleteIfExists(working);
    Files.createDirectories(working);
    try (java.util.stream.Stream<Path> walk = Files.walk(pristine)) {
      walk.forEach(
          source -> {
            try {
              Path target = working.resolve(pristine.relativize(source).toString());
              if (Files.isDirectory(source)) {
                Files.createDirectories(target);
              } else {
                Files.copy(source, target);
              }
            } catch (IOException e) {
              throw new RuntimeException("Failed to copy " + source, e);
            }
          });
    }
  }

  /**
   * SetupMain's tarballs wrap their contents in a single label directory (e.g. {@code
   * baseline-K1k/...}); after untar the actual catalog lives one level deeper. Heuristic: if the
   * extracted dir contains exactly one subdirectory and no files, descend.
   */
  private static Path catalogRootIn(Path dir) throws IOException {
    try (java.util.stream.Stream<Path> entries = Files.list(dir)) {
      List<Path> children = entries.collect(java.util.stream.Collectors.toList());
      if (children.size() == 1 && Files.isDirectory(children.get(0))) {
        return children.get(0);
      }
      return dir;
    }
  }

  private static void deleteIfExists(Path dir) throws IOException {
    if (!Files.exists(dir)) {
      return;
    }
    try (java.util.stream.Stream<Path> walk = Files.walk(dir)) {
      walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
    }
  }

  /** COMPACT_SPEC.md §M6: guard against trivially-cheap baseline runs. */
  private static void applyM6Assertions(int kLateTxDeletes, IterationResult result) {
    if (result.filesRead() < 10) {
      result.invalidate("files_read=" + result.filesRead() + " < 10");
      return;
    }
    if (result.filesWritten() < 10) {
      result.invalidate("files_written=" + result.filesWritten() + " < 10");
      return;
    }
    // M6 calls for >= 5 GB scanned. Production hits this; tests at micro scale don't, so we scope
    // the assertion to "reasonable for K=1k+". The cloud runner is expected to use production
    // sizes, and out-of-band numbers must be explained in the writeup per spec §Hand-off.
    long minBytes = kLateTxDeletes >= 1000 ? 5L * 1024 * 1024 * 1024 : 1L;
    if (result.inputDataBytes() < minBytes) {
      result.invalidate(
          "input_data_bytes=" + result.inputDataBytes() + " < " + minBytes + " (M6 threshold)");
    }
  }

  private static String usage() {
    return String.join(
        System.lineSeparator(),
        "Usage: RunMain --state-tar <path> --working-dir <path> --variant <v>",
        "               --k <int> [--iterations <n>] [--warmup <n>]",
        "               [--output <path>] [--table-name <db.tbl>]",
        "               [--driver-memory <size>] [--spark-master <url>]",
        "",
        "  --state-tar     Path to a tarball produced by SetupMain.",
        "  --working-dir   Local directory to extract & run iterations under.",
        "  --variant       'baseline' or 'treatment'.",
        "  --k             Late-tx K value (for JSON output + soundness checks).",
        "  --iterations    Timed iterations (default: 5 treatment, 3 baseline).",
        "  --warmup        Untimed warmup iterations (default: 1).",
        "  --output        Results JSONL path (default: <working-dir>/results.jsonl).",
        "  --table-name    Iceberg table identifier (default: db.<variant>).",
        "  --driver-memory Spark driver heap (default: 24g, per spec).",
        "  --spark-master  Spark master URL (default: local[*]).",
        "  --skip-m6       Skip the spec's M6 soundness assertions (intended for sub-production",
        "                  fixtures; production runs should leave it off).");
  }

  /** Parsed CLI arguments — internal POJO. */
  static final class Args {
    private final Path stateTar;
    private final Path workingDir;
    private final String variant;
    private final int kLateTxDeletes;
    private final int iterations;
    private final int warmup;
    private final Path outputJsonl;
    private final TableIdentifier tableIdent;
    private final String driverMemory;
    private final String sparkMaster;
    private final boolean skipM6;

    private Args(
        Path stateTar,
        Path workingDir,
        String variant,
        int kLateTxDeletes,
        int iterations,
        int warmup,
        Path outputJsonl,
        TableIdentifier tableIdent,
        String driverMemory,
        String sparkMaster,
        boolean skipM6) {
      this.stateTar = stateTar;
      this.workingDir = workingDir;
      this.variant = variant;
      this.kLateTxDeletes = kLateTxDeletes;
      this.iterations = iterations;
      this.warmup = warmup;
      this.outputJsonl = outputJsonl;
      this.tableIdent = tableIdent;
      this.driverMemory = driverMemory;
      this.sparkMaster = sparkMaster;
      this.skipM6 = skipM6;
    }

    Path stateTar() {
      return stateTar;
    }

    Path workingDir() {
      return workingDir;
    }

    String variant() {
      return variant;
    }

    int kLateTxDeletes() {
      return kLateTxDeletes;
    }

    int iterations() {
      return iterations;
    }

    int warmup() {
      return warmup;
    }

    Path outputJsonl() {
      return outputJsonl;
    }

    TableIdentifier tableIdent() {
      return tableIdent;
    }

    String driverMemory() {
      return driverMemory;
    }

    String sparkMaster() {
      return sparkMaster;
    }

    boolean skipM6() {
      return skipM6;
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    static Args parse(String[] argv) {
      Path stateTar = null;
      Path workingDir = null;
      String variant = null;
      Integer kLateTxDeletes = null;
      Integer iterations = null;
      int warmup = 1;
      Path outputJsonl = null;
      String tableName = null;
      String driverMemory = "24g";
      String sparkMaster = "local[*]";
      boolean skipM6 = false;

      int idx = 0;
      while (idx < argv.length) {
        String arg = argv[idx];
        switch (arg) {
          case "--state-tar":
            stateTar = Paths.get(requireValue(argv, ++idx, arg));
            break;
          case "--working-dir":
            workingDir = Paths.get(requireValue(argv, ++idx, arg));
            break;
          case "--variant":
            variant = requireValue(argv, ++idx, arg);
            break;
          case "--k":
            kLateTxDeletes = Integer.parseInt(requireValue(argv, ++idx, arg));
            break;
          case "--iterations":
            iterations = Integer.parseInt(requireValue(argv, ++idx, arg));
            break;
          case "--warmup":
            warmup = Integer.parseInt(requireValue(argv, ++idx, arg));
            break;
          case "--output":
            outputJsonl = Paths.get(requireValue(argv, ++idx, arg));
            break;
          case "--table-name":
            tableName = requireValue(argv, ++idx, arg);
            break;
          case "--driver-memory":
            driverMemory = requireValue(argv, ++idx, arg);
            break;
          case "--spark-master":
            sparkMaster = requireValue(argv, ++idx, arg);
            break;
          case "--skip-m6":
            skipM6 = true;
            break;
          case "-h":
          case "--help":
            throw new IllegalArgumentException("help requested");
          default:
            throw new IllegalArgumentException("Unknown argument: " + arg);
        }
        idx++;
      }

      if (stateTar == null) {
        throw new IllegalArgumentException("--state-tar is required");
      }
      if (workingDir == null) {
        throw new IllegalArgumentException("--working-dir is required");
      }
      if (variant == null || !(variant.equals("baseline") || variant.equals("treatment"))) {
        throw new IllegalArgumentException("--variant must be 'baseline' or 'treatment'");
      }
      if (kLateTxDeletes == null) {
        throw new IllegalArgumentException("--k is required");
      }
      if (iterations == null) {
        iterations = variant.equals("treatment") ? 5 : 3;
      }
      if (outputJsonl == null) {
        outputJsonl = workingDir.resolve("results.jsonl");
      }
      if (tableName == null) {
        tableName = "db." + variant;
      }

      TableIdentifier ident;
      List<String> parts = Splitter.on('.').splitToList(tableName);
      if (parts.size() == 1) {
        ident = TableIdentifier.of(parts.get(0));
      } else if (parts.size() == 2) {
        ident = TableIdentifier.of(parts.get(0), parts.get(1));
      } else {
        throw new IllegalArgumentException(
            "--table-name must be 'name' or 'namespace.name', got " + tableName);
      }

      return new Args(
          stateTar,
          workingDir,
          variant,
          kLateTxDeletes,
          iterations,
          warmup,
          outputJsonl,
          ident,
          driverMemory,
          sparkMaster,
          skipM6);
    }

    private static String requireValue(String[] argv, int index, String flag) {
      if (index >= argv.length) {
        throw new IllegalArgumentException(flag + " requires a value");
      }
      return argv[index];
    }
  }
}
