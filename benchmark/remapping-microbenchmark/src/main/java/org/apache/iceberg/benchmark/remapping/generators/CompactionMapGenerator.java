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
package org.apache.iceberg.benchmark.remapping.generators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMapBuilder;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.io.OutputFile;

/**
 * Generates compaction maps for benchmarking.
 *
 * <p>Creates compaction maps with configurable:
 *
 * <ul>
 *   <li>Number of source files (fanout factor)
 *   <li>Number of target files (split factor)
 *   <li>Number of runs per file mapping
 *   <li>Rows per source file
 * </ul>
 *
 * <p>Supports two scenarios:
 *
 * <ul>
 *   <li><b>Fanout</b>: Many source files → one target file (typical bin-pack)
 *   <li><b>Split</b>: One source file → many target files (large file split)
 * </ul>
 */
public class CompactionMapGenerator {

  @SuppressWarnings("unused")
  private final Random random;

  private final long rowsPerFile;

  public CompactionMapGenerator(long seed, long rowsPerFile) {
    this.random = new Random(seed);
    this.rowsPerFile = rowsPerFile;
  }

  public CompactionMapGenerator(long seed) {
    this(seed, 10_000_000L); // 10M rows per file default
  }

  /**
   * Generate a compaction map for a fanout scenario (many sources → one target).
   *
   * <p>This simulates bin-pack compaction where multiple small files are merged into one.
   *
   * @param outputFile where to write the compaction map
   * @param numSourceFiles number of source files being compacted
   * @param runsPerFile number of runs per source file (1 = contiguous, >1 = interleaved)
   * @return metadata about the generated compaction map
   */
  public GeneratedCompactionMap generateFanout(
      OutputFile outputFile, int numSourceFiles, int runsPerFile) throws IOException {

    List<String> sourceFiles = generateSourceFilePaths(numSourceFiles);
    String targetFile = "s3://bucket/data/compacted-00000.parquet";

    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    long targetOffset = 0;
    long totalSourceRows = 0;

    for (String sourceFile : sourceFiles) {
      CompactionMapBuilder.FileMappingBuilder fileMapping =
          builder.addFileMapping(sourceFile, targetFile);

      long sourceRowsRemaining = rowsPerFile;
      long sourceOffset = 0;
      long rowsPerRun = rowsPerFile / runsPerFile;

      for (int run = 0; run < runsPerFile; run++) {
        long runRows = (run == runsPerFile - 1) ? sourceRowsRemaining : rowsPerRun;
        fileMapping.addRun(sourceOffset, targetOffset, runRows);

        sourceOffset += runRows;
        targetOffset += runRows;
        sourceRowsRemaining -= runRows;
      }

      totalSourceRows += rowsPerFile;
    }

    CompactionMap map = builder.build();
    CompactionMaps.write(map, outputFile);

    return new GeneratedCompactionMap(
        outputFile.location(),
        sourceFiles,
        Collections.singletonList(targetFile),
        numSourceFiles * runsPerFile,
        totalSourceRows,
        outputFile.toInputFile().getLength(),
        CompactionScenario.FANOUT);
  }

  /**
   * Generate a compaction map for a split scenario (one source → many targets).
   *
   * <p>This simulates splitting a large file into multiple smaller files.
   *
   * @param outputFile where to write the compaction map
   * @param numTargetFiles number of target files after split
   * @param runsPerTarget number of runs per target file
   * @return metadata about the generated compaction map
   */
  public GeneratedCompactionMap generateSplit(
      OutputFile outputFile, int numTargetFiles, int runsPerTarget) throws IOException {

    String sourceFile = "s3://bucket/data/source-00000.parquet";
    List<String> targetFiles = generateTargetFilePaths(numTargetFiles);

    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    long rowsPerTarget = rowsPerFile / numTargetFiles;
    long sourceOffset = 0;
    int totalRuns = 0;

    for (String targetFile : targetFiles) {
      CompactionMapBuilder.FileMappingBuilder fileMapping =
          builder.addFileMapping(sourceFile, targetFile);

      long targetRowsRemaining = rowsPerTarget;
      long targetOffset = 0;
      long rowsPerRun = rowsPerTarget / runsPerTarget;

      for (int run = 0; run < runsPerTarget; run++) {
        long runRows = (run == runsPerTarget - 1) ? targetRowsRemaining : rowsPerRun;
        fileMapping.addRun(sourceOffset, targetOffset, runRows);

        sourceOffset += runRows;
        targetOffset += runRows;
        targetRowsRemaining -= runRows;
        totalRuns++;
      }
    }

    CompactionMap map = builder.build();
    CompactionMaps.write(map, outputFile);

    return new GeneratedCompactionMap(
        outputFile.location(),
        Collections.singletonList(sourceFile),
        targetFiles,
        totalRuns,
        rowsPerFile,
        outputFile.toInputFile().getLength(),
        CompactionScenario.SPLIT);
  }

  /**
   * Generate a compaction map for a mixed scenario (many sources → many targets).
   *
   * <p>This simulates complex compaction where files are reorganized.
   *
   * @param outputFile where to write the compaction map
   * @param numSourceFiles number of source files
   * @param numTargetFiles number of target files
   * @param runsPerMapping average runs per file mapping
   * @return metadata about the generated compaction map
   */
  public GeneratedCompactionMap generateMixed(
      OutputFile outputFile, int numSourceFiles, int numTargetFiles, int runsPerMapping)
      throws IOException {

    List<String> sourceFiles = generateSourceFilePaths(numSourceFiles);
    List<String> targetFiles = generateTargetFilePaths(numTargetFiles);

    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    long totalRows = numSourceFiles * rowsPerFile;
    long rowsPerTarget = totalRows / numTargetFiles;
    int totalRuns = 0;

    // Distribute source file rows across target files
    long[] targetOffsets = new long[numTargetFiles];
    long sourceGlobalOffset = 0;

    for (int srcIdx = 0; srcIdx < numSourceFiles; srcIdx++) {
      String sourceFile = sourceFiles.get(srcIdx);
      long sourceRowsRemaining = rowsPerFile;
      long sourceLocalOffset = 0;

      // This source file may span multiple targets
      while (sourceRowsRemaining > 0) {
        // Find target file for current position
        int targetIdx = (int) ((sourceGlobalOffset + sourceLocalOffset) / rowsPerTarget);
        targetIdx = Math.min(targetIdx, numTargetFiles - 1);
        String targetFile = targetFiles.get(targetIdx);

        CompactionMapBuilder.FileMappingBuilder fileMapping =
            builder.addFileMapping(sourceFile, targetFile);

        // Calculate rows for this run
        long targetBoundary = (targetIdx + 1) * rowsPerTarget;
        long rowsToTargetBoundary = targetBoundary - (sourceGlobalOffset + sourceLocalOffset);
        long runRows = Math.min(sourceRowsRemaining, rowsToTargetBoundary);

        // Add multiple smaller runs if requested
        long rowsPerRun = runRows / runsPerMapping;
        rowsPerRun = Math.max(1, rowsPerRun);

        long runOffset = sourceLocalOffset;
        long targetOffset = targetOffsets[targetIdx];
        long remaining = runRows;

        while (remaining > 0) {
          long thisRunRows = Math.min(rowsPerRun, remaining);
          fileMapping.addRun(runOffset, targetOffset, thisRunRows);

          runOffset += thisRunRows;
          targetOffset += thisRunRows;
          remaining -= thisRunRows;
          totalRuns++;
        }

        targetOffsets[targetIdx] += runRows;
        sourceLocalOffset += runRows;
        sourceRowsRemaining -= runRows;
      }

      sourceGlobalOffset += rowsPerFile;
    }

    CompactionMap map = builder.build();
    CompactionMaps.write(map, outputFile);

    return new GeneratedCompactionMap(
        outputFile.location(),
        sourceFiles,
        targetFiles,
        totalRuns,
        totalRows,
        outputFile.toInputFile().getLength(),
        CompactionScenario.MIXED);
  }

  private List<String> generateSourceFilePaths(int numFiles) {
    List<String> paths = new ArrayList<>(numFiles);
    for (int i = 0; i < numFiles; i++) {
      paths.add(String.format(Locale.ROOT, "s3://bucket/data/source-%05d.parquet", i));
    }
    return paths;
  }

  private List<String> generateTargetFilePaths(int numFiles) {
    List<String> paths = new ArrayList<>(numFiles);
    for (int i = 0; i < numFiles; i++) {
      paths.add(String.format(Locale.ROOT, "s3://bucket/data/target-%05d.parquet", i));
    }
    return paths;
  }

  public enum CompactionScenario {
    /** Many source files → one target file (bin-pack) */
    FANOUT,
    /** One source file → many target files (split) */
    SPLIT,
    /** Many source files → many target files (reorganization) */
    MIXED
  }

  /** Metadata about a generated compaction map. */
  public static class GeneratedCompactionMap {
    private final String path;
    private final List<String> sourceFiles;
    private final List<String> targetFiles;
    private final int totalRuns;
    private final long totalSourceRows;
    private final long fileSizeBytes;
    private final CompactionScenario scenario;

    public GeneratedCompactionMap(
        String path,
        List<String> sourceFiles,
        List<String> targetFiles,
        int totalRuns,
        long totalSourceRows,
        long fileSizeBytes,
        CompactionScenario scenario) {
      this.path = path;
      this.sourceFiles = Collections.unmodifiableList(new ArrayList<>(sourceFiles));
      this.targetFiles = Collections.unmodifiableList(new ArrayList<>(targetFiles));
      this.totalRuns = totalRuns;
      this.totalSourceRows = totalSourceRows;
      this.fileSizeBytes = fileSizeBytes;
      this.scenario = scenario;
    }

    public String path() {
      return path;
    }

    public List<String> sourceFiles() {
      return sourceFiles;
    }

    public List<String> targetFiles() {
      return targetFiles;
    }

    public int numSourceFiles() {
      return sourceFiles.size();
    }

    public int numTargetFiles() {
      return targetFiles.size();
    }

    public int totalRuns() {
      return totalRuns;
    }

    public long totalSourceRows() {
      return totalSourceRows;
    }

    public long fileSizeBytes() {
      return fileSizeBytes;
    }

    public CompactionScenario scenario() {
      return scenario;
    }

    /** Average runs per source file */
    public double avgRunsPerSource() {
      return (double) totalRuns / sourceFiles.size();
    }

    @Override
    public String toString() {
      return String.format(
          Locale.ROOT,
          "CompactionMap{path=%s, sources=%d, targets=%d, runs=%d, rows=%d, size=%d, scenario=%s}",
          path,
          sourceFiles.size(),
          targetFiles.size(),
          totalRuns,
          totalSourceRows,
          fileSizeBytes,
          scenario);
    }
  }
}
