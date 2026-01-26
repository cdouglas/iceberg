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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.benchmark.cloud.config.BenchmarkConfig;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;

/**
 * A simulated table wrapper for benchmarking compaction operations.
 *
 * <p>This class wraps a real Iceberg table but provides metadata-only operations. Simulated row
 * counts are stored in snapshot properties rather than actual data files. This allows benchmarking
 * compaction map operations without requiring real data or storage I/O.
 *
 * <p>Key capabilities:
 *
 * <ul>
 *   <li>Create tables in memory or on local/cloud storage
 *   <li>Add simulated data files with specified row counts
 *   <li>Track simulated row counts separately from actual file metadata
 *   <li>Support compaction operations with map generation
 *   <li>Provide statistics for benchmark reporting
 * </ul>
 */
public class SimulatedTable {

  private static final String SIMULATED_ROWS_PREFIX = "benchmark.simulated-rows.";

  private final Table table;
  private final ConcurrentHashMap<String, Long> fileRowCounts;
  private final BenchmarkConfig config;

  private SimulatedTable(Table table, BenchmarkConfig config) {
    this.table = table;
    this.fileRowCounts = new ConcurrentHashMap<>();
    this.config = config;
  }

  /**
   * Create a new simulated table in a temporary directory.
   *
   * @param name table name
   * @param config benchmark configuration
   * @return a new SimulatedTable
   * @throws IOException if table creation fails
   */
  public static SimulatedTable create(String name, BenchmarkConfig config) throws IOException {
    File tableDir = new File(config.tableLocation(), name);
    tableDir.mkdirs();

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "ts", Types.TimestampType.withoutZone()));

    PartitionSpec spec = PartitionSpec.unpartitioned();

    Map<String, String> properties = new HashMap<>();
    if (config.compactionMapsEnabled()) {
      properties.put("write.compaction-map.enabled", "true");
    }

    TableOperations ops = new LocalTableOperations(tableDir.getAbsolutePath());
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema, spec, null, tableDir.getAbsolutePath(), properties, config.formatVersion());
    ops.commit(null, metadata);

    Table table = new BaseTable(ops, name);
    return new SimulatedTable(table, config);
  }

  /**
   * Create a simulated table using an existing Iceberg table.
   *
   * @param table existing table
   * @param config benchmark configuration
   * @return a SimulatedTable wrapper
   */
  public static SimulatedTable wrap(Table table, BenchmarkConfig config) {
    return new SimulatedTable(table, config);
  }

  /** Get the underlying Iceberg table. */
  public Table table() {
    return table;
  }

  /** Get the partition spec for this table. */
  public PartitionSpec spec() {
    return table.spec();
  }

  /**
   * Add simulated data files to the table.
   *
   * @param files data files to add
   */
  public void addFiles(DataFile... files) {
    AppendFiles append = table.newAppend();
    for (DataFile file : files) {
      append.appendFile(file);
      fileRowCounts.put(file.location(), file.recordCount());
    }
    append.commit();
  }

  /**
   * Create and add simulated data files with specified row counts.
   *
   * @param numFiles number of files to create
   * @param avgRowsPerFile average rows per file
   * @param random random source for reproducibility
   */
  public void addSimulatedFiles(int numFiles, long avgRowsPerFile, Random random) {
    DataFile[] files =
        SimulatedDataFile.createBatch(
            spec(), numFiles, avgRowsPerFile, config.rowCountVariance(), random);
    addFiles(files);
  }

  /**
   * Add position deletes targeting specific data files.
   *
   * @param deleteFiles delete files to add
   */
  public void addDeletes(DeleteFile... deleteFiles) {
    RowDelta rowDelta = table.newRowDelta();
    for (DeleteFile deleteFile : deleteFiles) {
      rowDelta.addDeletes(deleteFile);
    }
    rowDelta.commit();
  }

  /** Create a new row delta transaction. */
  public RowDelta newRowDelta() {
    return table.newRowDelta();
  }

  /** Create a new append files transaction. */
  public AppendFiles newAppend() {
    return table.newAppend();
  }

  /** Create a new rewrite files transaction. */
  public RewriteFiles newRewrite() {
    return table.newRewrite();
  }

  /** Refresh the table to pick up new commits. */
  public void refresh() {
    table.refresh();
  }

  /** Get the current snapshot. */
  public Snapshot currentSnapshot() {
    return table.currentSnapshot();
  }

  /**
   * Get the simulated row count for a file.
   *
   * @param filePath file path
   * @return row count, or 0 if not tracked
   */
  public long getRowCount(String filePath) {
    return fileRowCounts.getOrDefault(filePath, 0L);
  }

  /**
   * Get total simulated row count across all tracked files.
   *
   * @return total row count
   */
  public long getTotalRowCount() {
    return fileRowCounts.values().stream().mapToLong(Long::longValue).sum();
  }

  /**
   * Get the number of tracked data files.
   *
   * @return file count
   */
  public int getFileCount() {
    return fileRowCounts.size();
  }

  /**
   * Track a file's row count for simulation purposes.
   *
   * @param filePath file path
   * @param rowCount row count
   */
  public void trackFile(String filePath, long rowCount) {
    fileRowCounts.put(filePath, rowCount);
  }

  /**
   * Remove a file from tracking (e.g., after compaction).
   *
   * @param filePath file path
   */
  public void untrackFile(String filePath) {
    fileRowCounts.remove(filePath);
  }

  /** Get the benchmark configuration. */
  public BenchmarkConfig config() {
    return config;
  }

  /**
   * Get snapshot summary with simulated row counts stored as properties.
   *
   * @return snapshot properties including simulated row counts
   */
  public Map<String, String> getSimulatedProperties() {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (Map.Entry<String, Long> entry : fileRowCounts.entrySet()) {
      builder.put(SIMULATED_ROWS_PREFIX + entry.getKey(), String.valueOf(entry.getValue()));
    }
    return builder.build();
  }

  /** A minimal local-file based TableOperations for testing. */
  private static class LocalTableOperations implements TableOperations {
    private final String location;
    private TableMetadata current;
    private long lastRefresh;
    private int version = 0;

    LocalTableOperations(String location) {
      this.location = location;
    }

    @Override
    public TableMetadata current() {
      return current;
    }

    @Override
    public TableMetadata refresh() {
      return current;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      if (base != current) {
        throw new org.apache.iceberg.exceptions.CommitFailedException(
            "Cannot commit: stale table metadata");
      }
      version++;
      current =
          TableMetadata.buildFrom(metadata)
              .withMetadataLocation(location + "/metadata/v" + version + ".metadata.json")
              .build();
      lastRefresh = System.currentTimeMillis();
    }

    @Override
    public FileIO io() {
      return new LocalFileIO();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return location + "/metadata/" + fileName;
    }

    @Override
    public org.apache.iceberg.io.LocationProvider locationProvider() {
      return path -> location + "/" + path;
    }
  }

  /** A minimal local FileIO for testing. */
  private static class LocalFileIO implements FileIO {
    @Override
    public InputFile newInputFile(String path) {
      return org.apache.iceberg.Files.localInput(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return org.apache.iceberg.Files.localOutput(new File(path));
    }

    @Override
    public void deleteFile(String path) {
      new File(path).delete();
    }
  }
}
