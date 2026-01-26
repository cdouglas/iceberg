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
package org.apache.iceberg.benchmark.remapping;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.benchmark.remapping.metrics.BenchmarkMetrics;
import org.apache.iceberg.benchmark.remapping.metrics.BenchmarkMetrics.ScenarioSummary;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for the remapping microbenchmark.
 *
 * <p>This benchmark measures the cost of rebasing position deletes on top of a compaction commit.
 * It includes both position delete files (V2/V3) and deletion vectors (V3).
 *
 * <p>Usage:
 *
 * <pre>
 *   # Run with default config
 *   java -jar remapping-microbenchmark.jar
 *
 *   # Run with custom config
 *   java -jar remapping-microbenchmark.jar --config my-config.yaml
 *
 *   # Run on specific cloud storage
 *   java -jar remapping-microbenchmark.jar --storage-uri s3://my-bucket/benchmark
 * </pre>
 */
public class RemappingMicrobenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(RemappingMicrobenchmark.class);
  private static final DateTimeFormatter TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss", Locale.ROOT);

  public static void main(String[] args) {
    try {
      BenchmarkConfig config = parseArgs(args);
      run(config);
    } catch (Exception e) {
      LOG.error("Benchmark failed", e);
      System.exit(1);
    }
  }

  public static void run(BenchmarkConfig config) throws IOException {
    LOG.info("=== Remapping Microbenchmark ===");
    LOG.info("Storage URI: {}", config.storageUri());
    LOG.info("Output directory: {}", config.outputDir());
    LOG.info("Warmup iterations: {}", config.warmupIterations());
    LOG.info("Measurement iterations: {}", config.measurementIterations());

    // Create output directory
    String timestamp = LocalDateTime.now(ZoneOffset.UTC).format(TIMESTAMP_FORMAT);
    String outputDir = config.outputDir() + "/run_" + timestamp;
    new File(outputDir).mkdirs();

    // Save config for reproducibility
    config.save(outputDir + "/config.yaml");

    // Create FileIO based on storage URI
    FileIO fileIO = createFileIO(config);
    String baseLocation = config.storageUri() + "/run_" + timestamp;

    // Run benchmark
    RemappingBenchmarkRunner runner = new RemappingBenchmarkRunner(config, fileIO, baseLocation);
    runner.runAll();

    // Save results
    BenchmarkMetrics metrics = runner.getMetrics();
    metrics.saveResults(outputDir + "/results.json");
    metrics.saveSummary(outputDir + "/summary.json");

    // Print summary
    printSummary(metrics);

    LOG.info("Results saved to: {}", outputDir);
  }

  private static BenchmarkConfig parseArgs(String[] args) throws IOException {
    BenchmarkConfig config = BenchmarkConfig.defaults();

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--config":
          if (i + 1 < args.length) {
            config = BenchmarkConfig.load(args[++i]);
          }
          break;
        case "--storage-uri":
          if (i + 1 < args.length) {
            config = config.withStorageUri(args[++i]);
          }
          break;
        case "--output-dir":
          if (i + 1 < args.length) {
            config = config.withOutputDir(args[++i]);
          }
          break;
        case "--warmup":
          if (i + 1 < args.length) {
            config = config.withWarmupIterations(Integer.parseInt(args[++i]));
          }
          break;
        case "--iterations":
          if (i + 1 < args.length) {
            config = config.withMeasurementIterations(Integer.parseInt(args[++i]));
          }
          break;
        case "--help":
          printUsage();
          System.exit(0);
          break;
        default:
          LOG.warn("Unknown argument: {}", args[i]);
      }
    }

    return config;
  }

  @SuppressWarnings("unused")
  private static FileIO createFileIO(BenchmarkConfig config) {
    Configuration hadoopConf = new Configuration();
    // storageUri used for cloud provider configuration (placeholder for future implementation)
    String storageUri = config.storageUri();

    // Configure for cloud providers
    switch (config.cloudProvider()) {
      case AWS_S3:
        // S3 configuration would go here
        // hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        break;
      case GCP_GCS:
        // GCS configuration would go here
        // hadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
        break;
      case AZURE_BLOB:
        // Azure Blob configuration would go here
        break;
      case LOCAL:
      default:
        // Use default Hadoop local filesystem
        break;
    }

    return new HadoopFileIO(hadoopConf);
  }

  private static void printSummary(BenchmarkMetrics metrics) {
    Map<String, ScenarioSummary> summaries = metrics.summarize();

    LOG.info("\n=== Benchmark Summary ===\n");

    // Group by format for cleaner output
    LOG.info("--- Position Delete Files ---");
    for (Map.Entry<String, ScenarioSummary> entry : summaries.entrySet()) {
      if (entry.getKey().startsWith("POSITION_DELETE")) {
        LOG.info("{}", entry.getValue());
      }
    }

    LOG.info("\n--- Deletion Vectors ---");
    for (Map.Entry<String, ScenarioSummary> entry : summaries.entrySet()) {
      if (entry.getKey().startsWith("DELETION_VECTOR")) {
        LOG.info("{}", entry.getValue());
      }
    }

    // Strategy comparison
    LOG.info("\n--- Strategy Comparison (avg latency ms) ---");
    printStrategyComparison(summaries);
  }

  private static void printStrategyComparison(Map<String, ScenarioSummary> summaries) {
    // Find unique scenario keys (without strategy)
    java.util.Set<String> baseKeys = new java.util.TreeSet<>();
    for (String key : summaries.keySet()) {
      // Remove strategy suffix
      String baseKey =
          key.replaceAll(
              "_(LINEAR|BINARY_SEARCH|INTERVAL_TREE|STREAM_JOIN|RANGE_QUERY|SMART)_", "_X_");
      baseKeys.add(baseKey);
    }

    // Print header
    System.out.printf(
        "%-40s %10s %10s %10s %10s %10s %10s%n",
        "Scenario", "LINEAR", "BINARY", "INTERVAL", "STREAM", "RANGE", "SMART");
    System.out.println("-".repeat(100));

    for (String baseKey : baseKeys) {
      String[] parts = baseKey.split("_", -1);
      String format = parts[0] + "_" + parts[1];
      String density = parts[2];
      String deletes = parts[4];
      String runs = parts[5];

      String label = String.format(Locale.ROOT, "%s %s d=%s r=%s", format, density, deletes, runs);

      System.out.printf("%-40s", label);

      for (String strategy :
          new String[] {
            "LINEAR", "BINARY_SEARCH", "INTERVAL_TREE", "STREAM_JOIN", "RANGE_QUERY", "SMART"
          }) {
        String fullKey = baseKey.replace("_X_", "_" + strategy + "_");
        ScenarioSummary summary = summaries.get(fullKey);
        if (summary != null) {
          System.out.printf(" %10.2f", summary.avgTotalLatencyMs());
        } else {
          System.out.printf(" %10s", "-");
        }
      }
      System.out.println();
    }
  }

  private static void printUsage() {
    System.out.println("Usage: java -jar remapping-microbenchmark.jar [options]");
    System.out.println();
    System.out.println("Options:");
    System.out.println("  --config <file>       Load configuration from YAML file");
    System.out.println("  --storage-uri <uri>   Storage location (file://, s3://, gs://)");
    System.out.println("  --output-dir <dir>    Output directory for results");
    System.out.println("  --warmup <n>          Number of warmup iterations");
    System.out.println("  --iterations <n>      Number of measurement iterations");
    System.out.println("  --help                Show this help message");
  }
}
