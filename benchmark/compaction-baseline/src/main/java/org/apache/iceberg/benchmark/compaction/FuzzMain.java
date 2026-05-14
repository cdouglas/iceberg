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
import java.util.OptionalLong;
import java.util.Random;
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
 * <p>Tests {@code --seed-count} scenarios drawn pseudorandomly from the seed space and writes one
 * JSON file per seed plus an aggregate {@code summary.json}. Failing seeds get a full operation
 * dump and a warehouse tarball so the failure can be replayed without the harness JAR.
 *
 * <p>Seed selection:
 *
 * <ul>
 *   <li>{@code --seed-start <N>} — anchor the pseudorandom sequence at {@code N}. Same anchor →
 *       same seed list, so a sweep is reproducible.
 *   <li>{@code --seeds <N1,N2,...>} — test exactly the listed seeds (used by {@code
 *       reproduce.sh} to replay a single failure).
 *   <li>Neither — pick a fresh anchor via {@link java.security.SecureRandom} and log it in {@code
 *       summary.json} so a follow-up {@code --seed-start <that>} run reproduces this sweep.
 * </ul>
 *
 * <p>Sequential 0..N-1 sweeps are intentionally avoided: neighbouring seeds share most of their
 * entropy and produce highly similar scenario shapes, so increasing {@code --seed-count} mostly
 * burns CPU on near-duplicates rather than widening coverage.
 *
 * <pre>
 *   java -jar fuzz.jar --seed-count 100 --workers 4 --output ./out
 *   java -jar fuzz.jar --seeds 1369,42 --output ./out          # reproduce specific failures
 *   java -jar fuzz.jar --seed-start 12345 --seed-count 100 ... # reproduce a prior sweep
 * </pre>
 *
 * <p>Property tested: {@code hash(compact(state) + remap(tx, map)) == hash(compact(state ∪ tx))}.
 */
public final class FuzzMain {

  private static final Logger LOG = LoggerFactory.getLogger(FuzzMain.class);
  // Default chosen so a single seed running solo (~10-15s wall) has ~20x headroom under the
  // concurrent execution mode added in `Actually run --workers seeds concurrently`. With N
  // workers contending for `local[N]` task slots in a shared SparkSession, per-seed wall time
  // scales roughly with N — a 60s default was tight at workers=4 and reliably tripped at
  // workers=8+ for normal-shape scenarios. Override with --timeout-seconds.
  private static final long DEFAULT_TIMEOUT_SECONDS = 300L;

  private FuzzMain() {}

  public static void main(String[] argv) throws Exception {
    Args args = Args.parse(argv);
    FuzzConfig config = FuzzConfig.load(args.configPath());
    SparkSession spark = buildSpark(args.outputDir(), args.workers());
    try {
      runWithSpark(args, config, spark);
    } finally {
      spark.stop();
    }
  }

  static void runWithSpark(Args args, FuzzConfig config, SparkSession spark) throws IOException {
    Path outputDir = Files.createDirectories(args.outputDir());

    // Build the list of seeds to test. Three precedence rules:
    //   1. --seeds <list>      → test exactly those seed values (used by reproduce.sh).
    //   2. --seed-start <N>    → anchor a pseudorandom sequence at N. Same N → same seeds, so
    //                            a full sweep stays reproducible.
    //   3. neither given       → pick a random anchor (logged to summary so a future run with
    //                            --seed-start <that> reproduces this sweep).
    // Sequential 0..N exploration is no good for a fuzz sweep: neighbouring seeds share most of
    // their entropy and produce highly similar scenario shapes, so cranking --seed-count just
    // wastes CPU on near-duplicates. Pseudorandom sampling spreads coverage across the seed
    // space.
    long anchor;
    long[] testedSeeds;
    String seedSource;
    if (args.seeds() != null) {
      testedSeeds = args.seeds();
      anchor = 0L; // unused when seeds() is explicit
      seedSource = "explicit-list(" + testedSeeds.length + ")";
    } else {
      anchor =
          args.seedStart().isPresent() ? args.seedStart().getAsLong() : pickRandomAnchor();
      Random rng = new Random(anchor);
      testedSeeds = new long[args.seedCount()];
      for (int i = 0; i < testedSeeds.length; i++) {
        testedSeeds[i] = rng.nextLong();
      }
      seedSource =
          args.seedStart().isPresent()
              ? "anchor=" + anchor + " (specified)"
              : "anchor=" + anchor + " (random — reproduce with --seed-start " + anchor + ")";
    }

    LOG.info(
        "fuzz harness: seedSource={} seedCount={} workers={} timeoutS={} configPath={} output={}",
        seedSource,
        testedSeeds.length,
        args.workers(),
        args.timeoutSeconds(),
        args.configPath(),
        outputDir);

    // Run `workers` seeds concurrently. The shared SparkSession is configured with
    // local[workers] (see buildSpark) so the in-JVM executor has enough task slots to overlap
    // them. Per-seed timeout is enforced via future.get(timeout) measured from batch
    // submission: within a batch all `workers` seeds start ~simultaneously (no queueing,
    // since pool size == batch size), so submission-time ≈ start-time.
    int workers = Math.max(1, args.workers());
    ExecutorService pool = Executors.newFixedThreadPool(workers);
    int failures = 0;
    int timeouts = 0;
    List<Long> failedSeeds = Lists.newArrayList();
    long t0 = System.nanoTime();
    try {
      for (int s = 0; s < testedSeeds.length; s += workers) {
        int batchCount = Math.min(workers, testedSeeds.length - s);
        List<SeedJob> jobs = new ArrayList<>(batchCount);
        for (int b = 0; b < batchCount; b++) {
          long seed = testedSeeds[s + b];
          jobs.add(submitSeed(spark, config, seed, outputDir, pool));
        }
        for (SeedJob job : jobs) {
          SeedOutcome outcome = collectSeed(job, args.timeoutSeconds(), outputDir);
          if (outcome.kind() == SeedOutcomeKind.FAILED) {
            failures++;
            failedSeeds.add(job.seed);
          } else if (outcome.kind() == SeedOutcomeKind.TIMEOUT) {
            timeouts++;
            failedSeeds.add(job.seed);
          }
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
    summary.seedAnchor = args.seeds() == null ? anchor : null;
    summary.seedAnchorRandom = args.seeds() == null && !args.seedStart().isPresent();
    summary.seedsRun = testedSeeds.length;
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

  private static SeedJob submitSeed(
      SparkSession spark,
      FuzzConfig config,
      long seed,
      Path outputDir,
      ExecutorService pool) {
    FuzzScenario scenario = FuzzScenario.forSeed(seed, config);
    File workspace = outputDir.resolve("workspace-seed-" + seed).toFile();
    if (!workspace.mkdirs() && !workspace.isDirectory()) {
      LOG.error("failed to create workspace for seed {}", seed);
      return new SeedJob(seed, scenario, workspace, null, System.nanoTime(),
          "workspace-create-failed");
    }
    FuzzRunner runner = new FuzzRunner(spark);
    long t0 = System.nanoTime();
    Callable<FuzzRunner.Outcome> task = () -> runner.run(scenario, workspace);
    Future<FuzzRunner.Outcome> future = pool.submit(task);
    return new SeedJob(seed, scenario, workspace, future, t0, null);
  }

  private static SeedOutcome collectSeed(SeedJob job, long timeoutSeconds, Path outputDir) {
    if (job.preSubmitError != null) {
      return new SeedOutcome(SeedOutcomeKind.FAILED, job.preSubmitError);
    }
    try {
      FuzzRunner.Outcome outcome = job.future.get(timeoutSeconds, TimeUnit.SECONDS);
      long elapsedMs = (System.nanoTime() - job.t0) / 1_000_000L;
      if (outcome.passed()) {
        try {
          writeOk(outputDir, job.seed, job.scenario, outcome, elapsedMs);
        } catch (IOException e) {
          LOG.warn("failed to write ok record for seed {}", job.seed, e);
        }
        deleteRecursive(job.workspace);
        return new SeedOutcome(SeedOutcomeKind.OK, null);
      } else {
        writeFail(outputDir, job.seed, job.scenario, outcome, elapsedMs, null);
        try {
          File tarball = outputDir.resolve("seed-" + job.seed + ".warehouse.tar").toFile();
          TarUtils.tarDirectory(
              job.workspace.toPath(), tarball.toPath(), "workspace-seed-" + job.seed);
          deleteRecursive(job.workspace);
        } catch (IOException e) {
          LOG.warn("failed to tar workspace for failing seed {}", job.seed, e);
        }
        return new SeedOutcome(SeedOutcomeKind.FAILED, null);
      }
    } catch (TimeoutException e) {
      job.future.cancel(true);
      writeFail(outputDir, job.seed, job.scenario, null, timeoutSeconds * 1000, "timeout");
      return new SeedOutcome(SeedOutcomeKind.TIMEOUT, "timeout");
    } catch (ExecutionException e) {
      writeFail(outputDir, job.seed, job.scenario, null, 0, throwableToString(e.getCause()));
      return new SeedOutcome(SeedOutcomeKind.FAILED, e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return new SeedOutcome(SeedOutcomeKind.FAILED, "interrupted");
    }
  }

  private static final class SeedJob {
    final long seed;
    final FuzzScenario scenario;
    final File workspace;
    final Future<FuzzRunner.Outcome> future;
    final long t0;
    final String preSubmitError;

    SeedJob(
        long seed,
        FuzzScenario scenario,
        File workspace,
        Future<FuzzRunner.Outcome> future,
        long t0,
        String preSubmitError) {
      this.seed = seed;
      this.scenario = scenario;
      this.workspace = workspace;
      this.future = future;
      this.t0 = t0;
      this.preSubmitError = preSubmitError;
    }
  }

  /**
   * Pick a fresh anchor for the seed RNG when --seed-start isn't given. Uses {@link
   * java.security.SecureRandom} so successive runs without --seed-start explore independent
   * portions of the seed space (no risk of two unattended sweeps colliding on the same anchor
   * because {@code System.currentTimeMillis()} only just rolled forward).
   */
  private static long pickRandomAnchor() {
    return new java.security.SecureRandom().nextLong();
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

  private static SparkSession buildSpark(Path workspaceRoot, int workers) {
    // One task slot per concurrent seed: each seed runs sequentially internally, so this is
    // the level of parallelism the harness actually exploits. Cross-seed Spark contention on
    // the driver is fine — the seeds are small.
    int slots = Math.max(1, workers);
    return SparkSession.builder()
        .master("local[" + slots + "]")
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
    private final OptionalLong seedStart;
    private final long[] seeds;
    private final int seedCount;
    private final int workers;
    private final long timeoutSeconds;
    private final Path outputDir;
    private final Path configPath;

    private Args(
        OptionalLong seedStart,
        long[] seeds,
        int seedCount,
        int workers,
        long timeoutSeconds,
        Path outputDir,
        Path configPath) {
      this.seedStart = seedStart;
      this.seeds = seeds;
      this.seedCount = seedCount;
      this.workers = workers;
      this.timeoutSeconds = timeoutSeconds;
      this.outputDir = outputDir;
      this.configPath = configPath;
    }

    /** Path to a JSON {@link FuzzConfig}, or {@code null} for defaults. */
    public Path configPath() {
      return configPath;
    }

    /** Anchor for the pseudorandom seed sequence; empty means "pick a random anchor". */
    public OptionalLong seedStart() {
      return seedStart;
    }

    /** Explicit seed list (overrides {@link #seedStart()}/{@link #seedCount()}), or null. */
    public long[] seeds() {
      return seeds;
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
      OptionalLong seedStart = OptionalLong.empty();
      long[] seeds = null;
      int seedCount = 100;
      int workers = 1;
      long timeoutSeconds = DEFAULT_TIMEOUT_SECONDS;
      Path outputDir = Paths.get("fuzz-out");
      Path configPath = null;
      for (int i = 0; i < argv.length; i++) {
        String arg = argv[i];
        switch (arg) {
          case "--seed-start":
            seedStart = OptionalLong.of(Long.parseLong(argv[++i]));
            break;
          case "--seeds":
            seeds = parseSeedList(argv[++i]);
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
          case "--config":
            configPath = Paths.get(argv[++i]);
            break;
          default:
            throw new IllegalArgumentException("Unknown argument: " + arg);
        }
      }
      if (seeds != null && seedStart.isPresent()) {
        throw new IllegalArgumentException(
            "--seeds and --seed-start are mutually exclusive (--seeds takes an explicit list, "
                + "--seed-start anchors the pseudorandom sequence)");
      }
      return new Args(
          seedStart, seeds, seedCount, workers, timeoutSeconds, outputDir, configPath);
    }

    private static long[] parseSeedList(String csv) {
      List<String> parts =
          org.apache.iceberg.relocated.com.google.common.base.Splitter.on(',')
              .trimResults()
              .omitEmptyStrings()
              .splitToList(csv);
      long[] out = new long[parts.size()];
      for (int i = 0; i < parts.size(); i++) {
        out[i] = Long.parseLong(parts.get(i));
      }
      return out;
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
    /**
     * RNG anchor used to draw the seed sequence — null when {@code --seeds} (explicit list) was
     * passed. When {@code seedAnchorRandom} is true the anchor was generated by
     * {@link #pickRandomAnchor()}, so a future {@code --seed-start <seedAnchor>} reproduces this
     * sweep exactly.
     */
    public Long seedAnchor;

    public boolean seedAnchorRandom;
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
