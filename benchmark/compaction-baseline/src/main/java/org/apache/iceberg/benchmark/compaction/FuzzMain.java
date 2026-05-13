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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * M1 fuzz harness CLI per {@code COMPACT_SPEC.md} §M1.
 *
 * <p>Runs {@code --seed-count} consecutive scenarios starting at {@code --seed-start} and writes
 * one JSON file per seed plus an aggregate {@code summary.json}. Failing seeds get a full
 * operation dump and a warehouse tarball so the failure can be replayed without the harness JAR.
 *
 * <pre>
 *   java -jar fuzz.jar --seed-start 0 --seed-count 100 --workers 1 --output ./out
 * </pre>
 *
 * <p>Property tested: {@code hash(compact(state) + remap(tx, map)) == hash(compact(state ∪ tx))}.
 */
public final class FuzzMain {

  private static final Logger LOG = LoggerFactory.getLogger(FuzzMain.class);
  private static final long DEFAULT_TIMEOUT_SECONDS = 60L;

  private FuzzMain() {}

  public static void main(String[] argv) throws Exception {
    Args args = Args.parse(argv);
    SparkSession spark = buildSpark(args.outputDir());
    try {
      runWithSpark(args, spark);
    } finally {
      spark.stop();
    }
  }

  static void runWithSpark(Args args, SparkSession spark) throws IOException {
    Path outputDir = Files.createDirectories(args.outputDir());
    LOG.info(
        "fuzz harness: seedStart={} seedCount={} workers={} timeoutS={} output={}",
        args.seedStart(),
        args.seedCount(),
        args.workers(),
        args.timeoutSeconds(),
        outputDir);

    // The --workers flag controls the executor pool, but Spark itself isn't safely shared by
    // simultaneous heavy operations in this single-process local mode, so we rely on the pool
    // to serialize through whatever Spark contention exists. The pool's primary value is
    // giving us a per-seed Future for clean timeout enforcement.
    ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, args.workers()));
    int failures = 0;
    int timeouts = 0;
    List<Long> failedSeeds = Lists.newArrayList();
    long t0 = System.nanoTime();
    try {
      for (long s = 0; s < args.seedCount(); s++) {
        long seed = args.seedStart() + s;
        SeedOutcome outcome = runOneSeed(spark, seed, args.timeoutSeconds(), outputDir, pool);
        if (outcome.kind() == SeedOutcomeKind.FAILED) {
          failures++;
          failedSeeds.add(seed);
        } else if (outcome.kind() == SeedOutcomeKind.TIMEOUT) {
          timeouts++;
          failedSeeds.add(seed);
        }
      }
    } finally {
      pool.shutdown();
      try {
        if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
          pool.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

    Summary summary = new Summary();
    summary.seedStart = args.seedStart();
    summary.seedsRun = args.seedCount();
    summary.seedsFailed = failures;
    summary.seedsTimedOut = timeouts;
    summary.failedSeedList = failedSeeds;
    summary.totalElapsedMs = elapsedMs;
    writeJson(outputDir.resolve("summary.json"), summary);
    LOG.info(
        "fuzz harness done: failures={} timeouts={} elapsedMs={}", failures, timeouts, elapsedMs);
    if (failures > 0 || timeouts > 0) {
      LOG.warn("failed seeds: {}", failedSeeds);
    }
  }

  private static SeedOutcome runOneSeed(
      SparkSession spark, long seed, long timeoutSeconds, Path outputDir, ExecutorService pool) {
    FuzzScenario scenario = FuzzScenario.forSeed(seed);
    File workspace = outputDir.resolve("workspace-seed-" + seed).toFile();
    if (!workspace.mkdirs() && !workspace.isDirectory()) {
      LOG.error("failed to create workspace for seed {}", seed);
      return new SeedOutcome(SeedOutcomeKind.FAILED, "workspace-create-failed");
    }
    FuzzRunner runner = new FuzzRunner(spark);

    long t0 = System.nanoTime();
    Callable<FuzzRunner.Outcome> task = () -> runner.run(scenario, workspace);
    Future<FuzzRunner.Outcome> future = pool.submit(task);
    try {
      FuzzRunner.Outcome outcome = future.get(timeoutSeconds, TimeUnit.SECONDS);
      long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
      if (outcome.passed()) {
        try {
          writeOk(outputDir, seed, scenario, outcome, elapsedMs);
        } catch (IOException e) {
          LOG.warn("failed to write ok record for seed {}", seed, e);
        }
        deleteRecursive(workspace);
        return new SeedOutcome(SeedOutcomeKind.OK, null);
      } else {
        writeFail(outputDir, seed, scenario, outcome, elapsedMs, null);
        try {
          File tarball = outputDir.resolve("seed-" + seed + ".warehouse.tar").toFile();
          TarUtils.tarDirectory(
              workspace.toPath(), tarball.toPath(), "workspace-seed-" + seed);
          deleteRecursive(workspace);
        } catch (IOException e) {
          LOG.warn("failed to tar workspace for failing seed {}", seed, e);
        }
        return new SeedOutcome(SeedOutcomeKind.FAILED, null);
      }
    } catch (TimeoutException e) {
      future.cancel(true);
      writeFail(outputDir, seed, scenario, null, timeoutSeconds * 1000, "timeout");
      return new SeedOutcome(SeedOutcomeKind.TIMEOUT, "timeout");
    } catch (ExecutionException e) {
      writeFail(outputDir, seed, scenario, null, 0, throwableToString(e.getCause()));
      return new SeedOutcome(SeedOutcomeKind.FAILED, e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return new SeedOutcome(SeedOutcomeKind.FAILED, "interrupted");
    }
  }

  private static String throwableToString(Throwable t) {
    if (t == null) {
      return "<null>";
    }
    StringWriter sw = new StringWriter();
    t.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }

  private static void writeOk(
      Path outputDir, long seed, FuzzScenario scenario, FuzzRunner.Outcome outcome, long elapsedMs)
      throws IOException {
    SeedOkRecord record = new SeedOkRecord();
    record.seed = seed;
    record.opsCount = scenario.opsCount();
    record.referenceHash = outcome.referenceHash();
    record.treatmentHash = outcome.treatmentHash();
    record.referenceRows = outcome.referenceRows();
    record.treatmentRows = outcome.treatmentRows();
    record.elapsedMs = elapsedMs;
    record.describe = scenario.describe();
    writeJson(outputDir.resolve("seed-" + seed + ".ok.json"), record);
  }

  private static void writeFail(
      Path outputDir,
      long seed,
      FuzzScenario scenario,
      FuzzRunner.Outcome outcomeOrNull,
      long elapsedMs,
      String errorOrNull) {
    SeedFailRecord record = new SeedFailRecord();
    record.seed = seed;
    record.opsCount = scenario.opsCount();
    if (outcomeOrNull != null) {
      record.referenceHash = outcomeOrNull.referenceHash();
      record.treatmentHash = outcomeOrNull.treatmentHash();
      record.referenceRows = outcomeOrNull.referenceRows();
      record.treatmentRows = outcomeOrNull.treatmentRows();
    }
    record.elapsedMs = elapsedMs;
    record.describe = scenario.describe();
    record.error = errorOrNull;
    record.warehouseTarball = "seed-" + seed + ".warehouse.tar";
    record.reproduceCommand =
        String.format(Locale.ROOT, "./reproduce.sh seed-%d.fail.json", seed);
    try {
      writeJson(outputDir.resolve("seed-" + seed + ".fail.json"), record);
    } catch (IOException e) {
      LOG.error("failed to write failure record for seed {}", seed, e);
    }
  }

  private static void writeJson(Path path, Object record) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    try (java.io.OutputStream out = Files.newOutputStream(path)) {
      mapper.writeValue(out, record);
    }
  }

  private static void deleteRecursive(File file) {
    if (file == null || !file.exists()) {
      return;
    }
    File[] children = file.listFiles();
    if (children != null) {
      for (File child : children) {
        deleteRecursive(child);
      }
    }
    if (!file.delete()) {
      LOG.warn("unable to delete {}", file);
    }
  }

  private static SparkSession buildSpark(Path workspaceRoot) {
    return SparkSession.builder()
        .master("local[2]")
        .appName("compaction-baseline-fuzz")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.default_iceberg.type", "hadoop")
        .config(
            "spark.sql.catalog.default_iceberg.warehouse",
            workspaceRoot.resolve("default-warehouse").toString())
        .getOrCreate();
  }

  /** CLI argument bundle. */
  public static final class Args {
    private final long seedStart;
    private final int seedCount;
    private final int workers;
    private final long timeoutSeconds;
    private final Path outputDir;

    private Args(long seedStart, int seedCount, int workers, long timeoutSeconds, Path outputDir) {
      this.seedStart = seedStart;
      this.seedCount = seedCount;
      this.workers = workers;
      this.timeoutSeconds = timeoutSeconds;
      this.outputDir = outputDir;
    }

    public long seedStart() {
      return seedStart;
    }

    public int seedCount() {
      return seedCount;
    }

    public int workers() {
      return workers;
    }

    public long timeoutSeconds() {
      return timeoutSeconds;
    }

    public Path outputDir() {
      return outputDir;
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public static Args parse(String[] argv) {
      long seedStart = 0L;
      int seedCount = 100;
      int workers = 1;
      long timeoutSeconds = DEFAULT_TIMEOUT_SECONDS;
      Path outputDir = Paths.get("fuzz-out");
      for (int i = 0; i < argv.length; i++) {
        String arg = argv[i];
        switch (arg) {
          case "--seed-start":
            seedStart = Long.parseLong(argv[++i]);
            break;
          case "--seed-count":
            seedCount = Integer.parseInt(argv[++i]);
            break;
          case "--workers":
            workers = Integer.parseInt(argv[++i]);
            break;
          case "--timeout-seconds":
            timeoutSeconds = Long.parseLong(argv[++i]);
            break;
          case "--output":
            outputDir = Paths.get(argv[++i]);
            break;
          default:
            throw new IllegalArgumentException("Unknown argument: " + arg);
        }
      }
      return new Args(seedStart, seedCount, workers, timeoutSeconds, outputDir);
    }
  }

  /** JSON record written to {@code seed-N.ok.json}. */
  public static final class SeedOkRecord {
    public long seed;
    public int opsCount;
    public long referenceHash;
    public long treatmentHash;
    public long referenceRows;
    public long treatmentRows;
    public long elapsedMs;
    public String describe;
  }

  /** JSON record written to {@code seed-N.fail.json}. */
  public static final class SeedFailRecord {
    public long seed;
    public int opsCount;
    public Long referenceHash;
    public Long treatmentHash;
    public Long referenceRows;
    public Long treatmentRows;
    public long elapsedMs;
    public String describe;
    public String error;
    public String warehouseTarball;
    public String reproduceCommand;
  }

  /** Aggregate written to {@code summary.json}. */
  public static final class Summary {
    public long seedStart;
    public int seedsRun;
    public int seedsFailed;
    public int seedsTimedOut;
    public List<Long> failedSeedList = new ArrayList<>();
    public long totalElapsedMs;
  }

  private enum SeedOutcomeKind {
    OK,
    FAILED,
    TIMEOUT
  }

  private static final class SeedOutcome {
    private final SeedOutcomeKind kind;
    @SuppressWarnings("unused")
    private final String error;

    SeedOutcome(SeedOutcomeKind kind, String error) {
      this.kind = kind;
      this.error = error;
    }

    SeedOutcomeKind kind() {
      return kind;
    }
  }
}
