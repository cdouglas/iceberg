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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Local builder for the eight prebuilt warehouses described in {@code COMPACT_SPEC.md} §"Two Frozen
 * Starting States". For each {@code K ∈ {1000, 10000, 100000, 1000000}} this produces a Baseline-K
 * and a Treatment-K warehouse, tars each, optionally uploads to S3, then writes {@code
 * setup_manifest.json}.
 *
 * <h3>Usage</h3>
 *
 * <pre>
 * java -cp compaction-baseline-*-all.jar \
 *   org.apache.iceberg.benchmark.compaction.SetupMain \
 *   --output /tmp/compaction-baseline-out \
 *   --bucket my-bench-bucket \
 *   [--only-k 1000,10000] \
 *   [--skip-k 1000000] \
 *   [--skip-upload] \
 *   [--seed 12345]
 * </pre>
 *
 * <p>Setup is heavy (~21M rows × 8 cells, so multiple hours on a workstation). Use {@code --only-k
 * 1000} during development and graduate to the full sweep when the plumbing is solid.
 */
public final class SetupMain {

  private static final Logger LOG = LoggerFactory.getLogger(SetupMain.class);

  private static final List<Integer> ALL_K = Arrays.asList(1_000, 10_000, 100_000, 1_000_000);
  private static final long DEFAULT_SEED = 0x5ee_dL;

  private SetupMain() {}

  public static void main(String[] args) {
    int exit;
    try {
      Args parsed = Args.parse(args);
      exit = run(parsed);
    } catch (IllegalArgumentException e) {
      LOG.error("Argument error: {}{}{}", e.getMessage(), System.lineSeparator(), usage(), e);
      exit = 2;
    } catch (Exception e) {
      LOG.error("SetupMain failed", e);
      exit = 1;
    }
    System.exit(exit);
  }

  private static int run(Args args) throws IOException {
    Files.createDirectories(args.output());
    LOG.info(
        "SetupMain: output={} bucket={} skipUpload={} seed={} cells={}",
        args.output(),
        args.bucket(),
        args.skipUpload(),
        args.seed(),
        args.cells());

    SparkSession spark = startSpark();
    SetupManifest manifest = new SetupManifest(args.seed());
    try {
      for (int kValue : args.cells()) {
        BuildConfig config = configFor(args, kValue);
        runCell(args, spark, manifest, "baseline", kValue, config);
        runCell(args, spark, manifest, "treatment", kValue, config);
      }
    } finally {
      spark.stop();
    }

    Path manifestPath = args.output().resolve("setup_manifest.json");
    manifest.writeJson(manifestPath);
    LOG.info("Wrote setup manifest to {}", manifestPath);
    if (!args.skipUpload()) {
      String s3Uri = "s3://" + args.bucket() + "/states/setup_manifest.json";
      S3Uploader.upload(manifestPath, s3Uri);
    }
    return 0;
  }

  private static BuildConfig configFor(Args args, int kValue) {
    if (args.useTestSizes()) {
      // Tiny shape for plumbing checks: the test workload is independent of K but we still want
      // distinct seeds per cell so file content differs.
      return BuildConfig.builder()
          .seed(args.seed() ^ kValue)
          .lateTxDeletes(Math.min(kValue, 100))
          .lateTxRunLength(Math.max(1, Math.min(kValue, 100) / 10))
          .lateTxFileFanout(kValue <= 10_000 ? 2 : 0)
          .build();
    }
    return BuildConfig.productionConfig(args.seed() ^ kValue, kValue);
  }

  private static void runCell(
      Args args,
      SparkSession spark,
      SetupManifest manifest,
      String variant,
      int kValue,
      BuildConfig config)
      throws IOException {
    String label = variant + "-K" + kLabel(kValue);
    LOG.info("=== Building cell {} ===", label);

    Path warehouseDir = args.output().resolve(label);
    if (Files.exists(warehouseDir)) {
      throw new IOException(
          "Output directory for "
              + label
              + " already exists: "
              + warehouseDir
              + " — refusing to overwrite. Delete it manually if you mean to rebuild.");
    }
    Files.createDirectories(warehouseDir);

    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), warehouseDir.toUri().toString());
    TableIdentifier tableIdent = TableIdentifier.of("db", variant);
    WarehouseBuilder builder = new WarehouseBuilder(catalog, tableIdent, config);

    BuildResult result =
        variant.equals("baseline") ? builder.buildBaseline() : builder.buildTreatment(spark);

    Path tarPath = args.output().resolve(label + ".tar");
    LOG.info("Tarring {} -> {}", warehouseDir, tarPath);
    TarUtils.tarDirectory(warehouseDir, tarPath, label);

    String s3Uri = null;
    if (!args.skipUpload()) {
      s3Uri =
          S3Uploader.upload(tarPath, "s3://" + args.bucket() + "/states/" + tarPath.getFileName());
    }

    manifest.addCell(result, tarPath, s3Uri);
    LOG.info(
        "Cell {} complete: snapshots={} dataFiles={} deleteFiles={} mapRunCount={}",
        label,
        result.snapshotIds().size(),
        result.dataFileCount(),
        result.deleteFileCount(),
        result.compactionMapRunCount());
  }

  private static SparkSession startSpark() {
    return SparkSession.builder()
        .master("local[*]")
        .appName("compaction-baseline-setup")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "16g")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.sql.shuffle.partitions",
            String.valueOf(Runtime.getRuntime().availableProcessors()))
        // Override IcebergSource's default Hive catalog with a HadoopCatalog stub. The warehouse
        // path is a placeholder — SparkActions uses the supplied Table's absolute paths.
        .config("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.default_iceberg.type", "hadoop")
        .config("spark.sql.catalog.default_iceberg.warehouse", "/tmp/setup-spark-warehouse")
        .config("spark.sql.catalog.default_cache_iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.default_cache_iceberg.type", "hadoop")
        .config(
            "spark.sql.catalog.default_cache_iceberg.warehouse", "/tmp/setup-spark-warehouse-cache")
        .getOrCreate();
  }

  private static String kLabel(int kValue) {
    if (kValue >= 1_000_000) {
      return (kValue / 1_000_000) + "m";
    }
    if (kValue >= 1_000) {
      return (kValue / 1_000) + "k";
    }
    return Integer.toString(kValue);
  }

  private static String usage() {
    return String.join(
        System.lineSeparator(),
        "Usage: SetupMain --output <dir> [--bucket <name>] [--seed <long>]",
        "                 [--only-k <k1,k2,...>] [--skip-k <k1,...>]",
        "                 [--skip-upload] [--test-sizes]",
        "",
        "  --output       Local directory for tarballs + setup_manifest.json (required).",
        "  --bucket       Destination S3 bucket; required unless --skip-upload is set.",
        "  --only-k       Comma-separated K values; defaults to " + ALL_K + ".",
        "  --skip-k       Comma-separated K values to omit from the default sweep.",
        "  --skip-upload  Build tarballs locally only — do not invoke `aws s3 cp`.",
        "  --seed         Master seed (default " + DEFAULT_SEED + ").",
        "  --test-sizes   Use BuildConfig.Builder defaults (~thousands of rows) instead of",
        "                 the production sizes. For local plumbing checks only.");
  }

  /** Parsed CLI arguments — package-private accessors only since they're internal. */
  static final class Args {
    private final Path output;
    private final String bucket;
    private final boolean skipUpload;
    private final boolean useTestSizes;
    private final long seed;
    private final List<Integer> cells;

    private Args(
        Path output,
        String bucket,
        boolean skipUpload,
        boolean useTestSizes,
        long seed,
        List<Integer> cells) {
      this.output = output;
      this.bucket = bucket;
      this.skipUpload = skipUpload;
      this.useTestSizes = useTestSizes;
      this.seed = seed;
      this.cells = cells;
    }

    Path output() {
      return output;
    }

    String bucket() {
      return bucket;
    }

    boolean skipUpload() {
      return skipUpload;
    }

    boolean useTestSizes() {
      return useTestSizes;
    }

    long seed() {
      return seed;
    }

    List<Integer> cells() {
      return cells;
    }

    static Args parse(String[] argv) {
      Path output = null;
      String bucket = null;
      boolean skipUpload = false;
      boolean useTestSizes = false;
      long seed = DEFAULT_SEED;
      Set<Integer> only = null;
      Set<Integer> skip = Sets.newHashSet();

      int idx = 0;
      while (idx < argv.length) {
        String arg = argv[idx];
        switch (arg) {
          case "--output":
            output = Paths.get(requireValue(argv, ++idx, arg));
            break;
          case "--bucket":
            bucket = requireValue(argv, ++idx, arg);
            break;
          case "--skip-upload":
            skipUpload = true;
            break;
          case "--test-sizes":
            useTestSizes = true;
            break;
          case "--seed":
            seed = Long.parseLong(requireValue(argv, ++idx, arg));
            break;
          case "--only-k":
            only = parseKList(requireValue(argv, ++idx, arg));
            break;
          case "--skip-k":
            skip.addAll(parseKList(requireValue(argv, ++idx, arg)));
            break;
          case "-h":
          case "--help":
            throw new IllegalArgumentException("help requested");
          default:
            throw new IllegalArgumentException("Unknown argument: " + arg);
        }
        idx++;
      }

      if (output == null) {
        throw new IllegalArgumentException("--output is required");
      }
      if (!skipUpload && bucket == null) {
        throw new IllegalArgumentException("--bucket is required unless --skip-upload is set");
      }

      Set<Integer> chosen = Sets.newLinkedHashSet();
      chosen.addAll(only != null ? only : ALL_K);
      chosen.removeAll(skip);
      if (chosen.isEmpty()) {
        throw new IllegalArgumentException(
            "No K cells selected after applying --only-k / --skip-k");
      }
      List<Integer> ordered = Lists.newArrayList(chosen);
      Collections.sort(ordered);

      return new Args(output, bucket, skipUpload, useTestSizes, seed, ordered);
    }

    private static String requireValue(String[] argv, int index, String flag) {
      if (index >= argv.length) {
        throw new IllegalArgumentException(flag + " requires a value");
      }
      return argv[index];
    }

    private static Set<Integer> parseKList(String csv) {
      Set<Integer> out = Sets.newLinkedHashSet();
      for (String token : Splitter.on(',').trimResults().omitEmptyStrings().split(csv)) {
        out.add(Integer.parseInt(token));
      }
      return out;
    }
  }
}
