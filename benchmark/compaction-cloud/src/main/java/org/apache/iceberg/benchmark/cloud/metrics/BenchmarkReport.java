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
package org.apache.iceberg.benchmark.cloud.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iceberg.benchmark.cloud.ConflictStatistics;
import org.apache.iceberg.benchmark.cloud.config.BenchmarkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates comprehensive benchmark reports in multiple formats.
 *
 * <p>Outputs include:
 *
 * <ul>
 *   <li>JSON statistics for programmatic analysis
 *   <li>Human-readable summary
 *   <li>Comparison data when running A/B tests
 * </ul>
 */
public class BenchmarkReport {

  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkReport.class);
  private static final ObjectMapper JSON_MAPPER =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
  private static final DateTimeFormatter TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

  private final ConflictStatistics stats;
  private final BenchmarkConfig config;
  private final long elapsedTimeMs;

  public BenchmarkReport(ConflictStatistics stats, BenchmarkConfig config, long elapsedTimeMs) {
    this.stats = stats;
    this.config = config;
    this.elapsedTimeMs = elapsedTimeMs;
  }

  /**
   * Generate all report outputs.
   *
   * @param outputDir directory to write reports
   * @throws IOException if writing fails
   */
  public void generate(String outputDir) throws IOException {
    File dir = new File(outputDir);
    dir.mkdirs();

    String timestamp = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
        .withZone(ZoneId.systemDefault())
        .format(Instant.now());

    // Write JSON statistics
    File statsFile = new File(dir, "statistics_" + timestamp + ".json");
    writeJsonStats(statsFile);

    // Write config used
    File configFile = new File(dir, "config_" + timestamp + ".yaml");
    config.save(configFile.getAbsolutePath());

    // Write human-readable report
    File reportFile = new File(dir, "report_" + timestamp + ".txt");
    writeTextReport(reportFile);

    LOG.info("Benchmark report written to: {}", dir.getAbsolutePath());
  }

  private void writeJsonStats(File file) throws IOException {
    Map<String, Object> report = new LinkedHashMap<>();

    // Metadata
    report.put("timestamp", TIMESTAMP_FORMAT.format(Instant.now()));
    report.put("elapsed_time_ms", elapsedTimeMs);
    report.put("compaction_maps_enabled", config.compactionMapsEnabled());
    report.put("format_version", config.formatVersion());

    // Summary statistics
    Map<String, Object> summary = new LinkedHashMap<>();
    summary.put("total_deletes", stats.getTotalDeletes());
    summary.put("successful_deletes", stats.getSuccessfulDeletes());
    summary.put("failed_deletes", stats.getFailedDeletes());
    summary.put("conflict_rate", stats.getConflictRate());
    summary.put("remap_success_rate", stats.getRemapSuccessRate());
    summary.put("total_compactions", stats.getTotalCompactions());
    summary.put("successful_compactions", stats.getSuccessfulCompactions());
    summary.put("compactions_with_maps", stats.getCompactionsWithMaps());
    report.put("summary", summary);

    // Latency statistics
    Map<String, Object> latency = new LinkedHashMap<>();
    latency.put("avg_delete_latency_ms", stats.getAvgDeleteLatencyMs());
    latency.put("p50_delete_latency_ms", stats.getDeleteLatencyPercentile(50));
    latency.put("p95_delete_latency_ms", stats.getDeleteLatencyPercentile(95));
    latency.put("p99_delete_latency_ms", stats.getDeleteLatencyPercentile(99));
    latency.put("avg_remap_latency_ms", stats.getAvgRemapLatencyMs());
    latency.put("p50_remap_latency_ms", stats.getRemapLatencyPercentile(50));
    latency.put("p99_remap_latency_ms", stats.getRemapLatencyPercentile(99));
    latency.put("avg_compaction_latency_ms", stats.getAvgCompactionLatencyMs());
    latency.put("avg_map_build_latency_ms", stats.getAvgMapBuildLatencyMs());
    report.put("latency", latency);

    // Map efficiency
    Map<String, Object> mapEfficiency = new LinkedHashMap<>();
    mapEfficiency.put("avg_map_size_kb", stats.getAvgMapSizeKB());
    mapEfficiency.put("avg_run_count", stats.getAvgRunCount());
    mapEfficiency.put("avg_files_per_compaction", stats.getAvgFilesPerCompaction());
    report.put("map_efficiency", mapEfficiency);

    // Row statistics
    Map<String, Object> rows = new LinkedHashMap<>();
    rows.put("total_rows_loaded", stats.getTotalRowsLoaded());
    rows.put("total_rows_compacted", stats.getTotalRowsCompacted());
    report.put("rows", rows);

    // Strategy metrics (if available)
    if (!stats.getStrategyMetrics().isEmpty()) {
      Map<String, Object> strategies = new LinkedHashMap<>();
      for (Map.Entry<String, ConflictStatistics.StrategyMetrics> entry :
          stats.getStrategyMetrics().entrySet()) {
        Map<String, Object> strategyData = new LinkedHashMap<>();
        strategyData.put("count", entry.getValue().getCount());
        strategyData.put("avg_latency_ms", entry.getValue().getAvgLatencyMs());
        strategies.put(entry.getKey(), strategyData);
      }
      report.put("strategy_metrics", strategies);
    }

    JSON_MAPPER.writeValue(file, report);
  }

  private void writeTextReport(File file) throws IOException {
    try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
      writer.println("═══════════════════════════════════════════════════════════════");
      writer.println("           Compaction Cloud Benchmark Report");
      writer.println("═══════════════════════════════════════════════════════════════");
      writer.println();
      writer.printf("Timestamp: %s%n", TIMESTAMP_FORMAT.format(Instant.now()));
      writer.printf("Elapsed Time: %.2f seconds%n", elapsedTimeMs / 1000.0);
      writer.printf("Compaction Maps: %s%n", config.compactionMapsEnabled() ? "ENABLED" : "DISABLED");
      writer.printf("Format Version: %d%n", config.formatVersion());
      writer.println();

      writer.println("───────────────────────────────────────────────────────────────");
      writer.println("                    Transaction Summary");
      writer.println("───────────────────────────────────────────────────────────────");
      writer.printf("Total Deletes:        %,d%n", stats.getTotalDeletes());
      writer.printf("  Successful:         %,d (%.1f%%)%n",
          stats.getSuccessfulDeletes(),
          stats.getTotalDeletes() > 0 ? 100.0 * stats.getSuccessfulDeletes() / stats.getTotalDeletes() : 0);
      writer.printf("  Failed:             %,d (%.1f%%)%n",
          stats.getFailedDeletes(),
          stats.getTotalDeletes() > 0 ? 100.0 * stats.getFailedDeletes() / stats.getTotalDeletes() : 0);
      writer.printf("  Conflicts:          %,d%n", stats.getConflictedDeletes());
      writer.printf("Conflict Rate:        %.2f%%%n", stats.getConflictRate() * 100);
      writer.printf("Remap Success Rate:   %.2f%%%n", stats.getRemapSuccessRate() * 100);
      writer.println();

      writer.println("───────────────────────────────────────────────────────────────");
      writer.println("                    Compaction Summary");
      writer.println("───────────────────────────────────────────────────────────────");
      writer.printf("Total Compactions:    %,d%n", stats.getTotalCompactions());
      writer.printf("  Successful:         %,d%n", stats.getSuccessfulCompactions());
      writer.printf("  With Maps:          %,d (%.1f%%)%n",
          stats.getCompactionsWithMaps(),
          stats.getTotalCompactions() > 0 ? 100.0 * stats.getCompactionsWithMaps() / stats.getTotalCompactions() : 0);
      writer.printf("Total Rows Compacted: %,d%n", stats.getTotalRowsCompacted());
      writer.printf("Avg Files/Compaction: %.1f%n", stats.getAvgFilesPerCompaction());
      writer.println();

      writer.println("───────────────────────────────────────────────────────────────");
      writer.println("                    Latency Statistics");
      writer.println("───────────────────────────────────────────────────────────────");
      writer.printf("Delete Latency:%n");
      writer.printf("  Average:     %.2f ms%n", stats.getAvgDeleteLatencyMs());
      writer.printf("  P50:         %.2f ms%n", stats.getDeleteLatencyPercentile(50));
      writer.printf("  P95:         %.2f ms%n", stats.getDeleteLatencyPercentile(95));
      writer.printf("  P99:         %.2f ms%n", stats.getDeleteLatencyPercentile(99));
      writer.printf("Remap Latency:%n");
      writer.printf("  Average:     %.2f ms%n", stats.getAvgRemapLatencyMs());
      writer.printf("  P50:         %.2f ms%n", stats.getRemapLatencyPercentile(50));
      writer.printf("  P99:         %.2f ms%n", stats.getRemapLatencyPercentile(99));
      writer.printf("Compaction Latency:%n");
      writer.printf("  Average:     %.2f ms%n", stats.getAvgCompactionLatencyMs());
      writer.printf("Map Build Latency:%n");
      writer.printf("  Average:     %.2f ms%n", stats.getAvgMapBuildLatencyMs());
      writer.println();

      writer.println("───────────────────────────────────────────────────────────────");
      writer.println("                    Map Efficiency");
      writer.println("───────────────────────────────────────────────────────────────");
      writer.printf("Average Map Size:     %.2f KB%n", stats.getAvgMapSizeKB());
      writer.printf("Average Run Count:    %.1f%n", stats.getAvgRunCount());
      writer.println();

      // Strategy breakdown if available
      if (!stats.getStrategyMetrics().isEmpty()) {
        writer.println("───────────────────────────────────────────────────────────────");
        writer.println("                    Strategy Usage");
        writer.println("───────────────────────────────────────────────────────────────");
        for (Map.Entry<String, ConflictStatistics.StrategyMetrics> entry :
            stats.getStrategyMetrics().entrySet()) {
          writer.printf("%-20s: %,d uses, avg %.2f ms%n",
              entry.getKey(),
              entry.getValue().getCount(),
              entry.getValue().getAvgLatencyMs());
        }
        writer.println();
      }

      writer.println("═══════════════════════════════════════════════════════════════");
    }
  }

  /** Print summary to console. */
  public void printSummary() {
    LOG.info("═══════════════════════════════════════════════════════════════");
    LOG.info("                Benchmark Complete");
    LOG.info("═══════════════════════════════════════════════════════════════");
    LOG.info("Elapsed Time:         {:.2f} seconds", elapsedTimeMs / 1000.0);
    LOG.info("Total Deletes:        {}", stats.getTotalDeletes());
    LOG.info("Conflict Rate:        {:.2f}%", stats.getConflictRate() * 100);
    LOG.info("Remap Success Rate:   {:.2f}%", stats.getRemapSuccessRate() * 100);
    LOG.info("Total Compactions:    {}", stats.getTotalCompactions());
    LOG.info("Avg Delete Latency:   {:.2f} ms", stats.getAvgDeleteLatencyMs());
    LOG.info("Avg Remap Latency:    {:.2f} ms", stats.getAvgRemapLatencyMs());
    LOG.info("═══════════════════════════════════════════════════════════════");
  }
}
