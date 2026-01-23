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
package org.apache.iceberg.benchmark.cloud;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.benchmark.cloud.config.BenchmarkConfig;
import org.apache.iceberg.benchmark.cloud.metrics.BenchmarkReport;
import org.apache.iceberg.benchmark.cloud.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for the compaction cloud benchmark.
 *
 * <p>This benchmark evaluates compaction map efficacy under realistic concurrent workloads using
 * metadata-only simulated transactions.
 *
 * <p>Usage:
 *
 * <pre>
 *   java -jar compaction-cloud-benchmark.jar [options]
 *
 *   Options:
 *     --config <path>        Path to YAML config file (default: uses built-in defaults)
 *     --table-location <path> Override table location (local path or cloud URI)
 *     --output-dir <path>    Output directory for results (default: benchmark-results)
 *     --seed <long>          Random seed for reproducibility
 *     --iterations <n>       Number of iterations
 *     --no-maps              Disable compaction maps (for comparison)
 *     --help                 Show this help message
 * </pre>
 */
public class CompactionCloudBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionCloudBenchmark.class);

  private final BenchmarkConfig config;
  private final ConflictStatistics stats;
  private final MetricsCollector metrics;
  private SimulatedTable table;
  private TransactionSimulator transactionSimulator;
  private CompactionSimulator compactionSimulator;

  public CompactionCloudBenchmark(BenchmarkConfig config) {
    this.config = config;
    this.stats = new ConflictStatistics();
    this.metrics = new MetricsCollector(stats, config.collectDetailedStats());
  }

  /**
   * Run the benchmark.
   *
   * @return exit code (0 for success)
   */
  public int run() {
    long startTime = System.currentTimeMillis();

    try {
      LOG.info("═══════════════════════════════════════════════════════════════");
      LOG.info("           Compaction Cloud Benchmark");
      LOG.info("═══════════════════════════════════════════════════════════════");
      LOG.info("Configuration:");
      LOG.info("  Table Location:       {}", config.tableLocation());
      LOG.info("  Format Version:       {}", config.formatVersion());
      LOG.info("  Compaction Maps:      {}", config.compactionMapsEnabled());
      LOG.info("  Workload Mode:        {}", config.workloadMode());
      LOG.info("  Iterations:           {}", config.numIterations());
      LOG.info("  Concurrent Writers:   {}", config.concurrentWriters());
      LOG.info("  Conflict Probability: {}", config.conflictProbability());
      LOG.info("═══════════════════════════════════════════════════════════════");

      // Initialize table
      initializeTable();

      // Generate workload
      WorkloadGenerator generator = WorkloadGenerator.create(config);
      List<WorkloadGenerator.WorkloadEvent> events = generator.generate();
      LOG.info("Generated {} workload events", events.size());

      // Execute workload
      executeWorkload(events);

      // Generate report
      long elapsedTime = System.currentTimeMillis() - startTime;
      BenchmarkReport report = new BenchmarkReport(stats, config, elapsedTime);
      report.generate(config.outputDir());
      report.printSummary();

      return 0;

    } catch (Exception e) {
      LOG.error("Benchmark failed", e);
      return 1;
    }
  }

  private void initializeTable() throws IOException {
    LOG.info("Initializing simulated table...");
    table = SimulatedTable.create("benchmark", config);
    transactionSimulator =
        new TransactionSimulator(table, metrics, config, config.randomSeed());
    compactionSimulator =
        new CompactionSimulator(table, metrics, config, config.randomSeed() + 1);
    LOG.info("Table initialized at: {}", config.tableLocation());
  }

  private void executeWorkload(List<WorkloadGenerator.WorkloadEvent> events) throws InterruptedException {
    LOG.info("Executing workload with {} concurrent writers...", config.concurrentWriters());

    ExecutorService executor = Executors.newFixedThreadPool(
        config.concurrentWriters() + config.concurrentCompactors());

    AtomicInteger completedEvents = new AtomicInteger(0);
    int totalEvents = events.size();

    // Group events by timestamp for concurrent execution
    int i = 0;
    while (i < events.size()) {
      long currentTimestamp = events.get(i).timestamp();

      // Find all events with the same timestamp
      int startIdx = i;
      while (i < events.size() && events.get(i).timestamp() == currentTimestamp) {
        i++;
      }

      // Execute concurrent events
      List<WorkloadGenerator.WorkloadEvent> concurrentEvents = events.subList(startIdx, i);
      CountDownLatch latch = new CountDownLatch(concurrentEvents.size());

      for (WorkloadGenerator.WorkloadEvent event : concurrentEvents) {
        executor.submit(() -> {
          try {
            executeEvent(event);
            int completed = completedEvents.incrementAndGet();
            if (completed % 10 == 0) {
              LOG.info("Progress: {}/{} events completed", completed, totalEvents);
            }
          } finally {
            latch.countDown();
          }
        });
      }

      // Wait for all concurrent events to complete
      latch.await();
    }

    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.HOURS);
  }

  private void executeEvent(WorkloadGenerator.WorkloadEvent event) {
    switch (event.type()) {
      case INITIAL_LOAD:
        transactionSimulator.executeInitialLoad(
            event.fileCount() > 0 ? event.fileCount() : config.numFiles(),
            config.avgRowsPerFile());
        break;

      case DELETE_ROWS:
        TransactionSimulator.DeleteOperation deleteOp =
            new TransactionSimulator.DeleteOperation(event.selectivity(), event.pattern());
        transactionSimulator.executeDelete(deleteOp);
        break;

      case COMPACTION:
        CompactionSimulator.CompactionOperation compactOp =
            new CompactionSimulator.CompactionOperation(
                event.fileCount() > 0 ? event.fileCount() : config.numFiles() / 10);
        compactionSimulator.executeCompaction(compactOp);
        break;

      case CONCURRENT_DELETE:
        // Get files being compacted and target them specifically
        Set<String> compactingFiles = compactionSimulator.getCompactingFiles();
        TransactionSimulator.DeleteOperation conflictOp =
            new TransactionSimulator.DeleteOperation(
                event.selectivity(),
                event.pattern(),
                event.target() == WorkloadGenerator.DeleteTarget.COMPACTING
                    ? compactingFiles
                    : new HashSet<>());
        transactionSimulator.executeDelete(conflictOp);
        break;

      case ADD_FILES:
        transactionSimulator.executeAddFiles(
            event.fileCount() > 0 ? event.fileCount() : 10,
            config.avgRowsPerFile());
        break;

      default:
        LOG.warn("Unknown event type: {}", event.type());
    }
  }

  public static void main(String[] args) {
    try {
      BenchmarkConfig config = parseArgs(args);
      CompactionCloudBenchmark benchmark = new CompactionCloudBenchmark(config);
      System.exit(benchmark.run());
    } catch (Exception e) {
      LOG.error("Failed to run benchmark", e);
      System.exit(1);
    }
  }

  private static BenchmarkConfig parseArgs(String[] args) throws IOException {
    BenchmarkConfig config = BenchmarkConfig.defaults();

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--config":
          config = BenchmarkConfig.load(args[++i]);
          break;
        case "--table-location":
          config = config.withTableLocation(args[++i]);
          break;
        case "--output-dir":
          config = config.withOutputDir(args[++i]);
          break;
        case "--seed":
          config = config.withRandomSeed(Long.parseLong(args[++i]));
          break;
        case "--iterations":
          config = config.withNumIterations(Integer.parseInt(args[++i]));
          break;
        case "--no-maps":
          config = config.withCompactionMapsEnabled(false);
          break;
        case "--help":
          printUsage();
          System.exit(0);
          break;
        default:
          if (args[i].startsWith("-")) {
            LOG.warn("Unknown option: {}", args[i]);
          }
      }
    }

    return config;
  }

  private static void printUsage() {
    System.out.println("Compaction Cloud Benchmark");
    System.out.println();
    System.out.println("Usage: java -jar compaction-cloud-benchmark.jar [options]");
    System.out.println();
    System.out.println("Options:");
    System.out.println("  --config <path>         Path to YAML config file");
    System.out.println("  --table-location <path> Override table location");
    System.out.println("  --output-dir <path>     Output directory for results");
    System.out.println("  --seed <long>           Random seed for reproducibility");
    System.out.println("  --iterations <n>        Number of iterations");
    System.out.println("  --no-maps               Disable compaction maps");
    System.out.println("  --help                  Show this help message");
    System.out.println();
    System.out.println("Examples:");
    System.out.println("  # Run with defaults");
    System.out.println("  java -jar compaction-cloud-benchmark.jar");
    System.out.println();
    System.out.println("  # Run with custom config");
    System.out.println("  java -jar compaction-cloud-benchmark.jar --config my-config.yaml");
    System.out.println();
    System.out.println("  # Compare with and without maps");
    System.out.println("  java -jar compaction-cloud-benchmark.jar --output-dir results-with-maps");
    System.out.println("  java -jar compaction-cloud-benchmark.jar --no-maps --output-dir results-no-maps");
  }
}
