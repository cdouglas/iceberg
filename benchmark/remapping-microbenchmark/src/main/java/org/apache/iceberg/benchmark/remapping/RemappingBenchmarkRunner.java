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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.PositionDeleteRemapper;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.DeleteFormat;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.Density;
import org.apache.iceberg.benchmark.remapping.BenchmarkConfig.Strategy;
import org.apache.iceberg.benchmark.remapping.generators.CompactionMapGenerator;
import org.apache.iceberg.benchmark.remapping.generators.CompactionMapGenerator.GeneratedCompactionMap;
import org.apache.iceberg.benchmark.remapping.generators.DeletionVectorGenerator;
import org.apache.iceberg.benchmark.remapping.generators.PositionDeleteGenerator;
import org.apache.iceberg.benchmark.remapping.generators.PositionDeleteGenerator.GeneratedDeleteFile;
import org.apache.iceberg.benchmark.remapping.metrics.BenchmarkMetrics;
import org.apache.iceberg.benchmark.remapping.metrics.BenchmarkMetrics.BenchmarkResult;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.PuffinWriter;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes remapping benchmarks with real cloud I/O.
 *
 * <p>The benchmark measures three phases:
 *
 * <ol>
 *   <li><b>Read</b>: Load position deletes (Parquet) or deletion vectors (Puffin) from storage
 *   <li><b>Remap</b>: Apply compaction map to transform positions using PositionDeleteRemapper
 *   <li><b>Write</b>: Write remapped deletes back to storage
 * </ol>
 *
 * <p>This captures the full cost of conflict resolution, including cloud storage latency.
 *
 * <p>Note: Strategy selection is handled internally by PositionDeleteRemapper using the SMART
 * selector. For detailed strategy comparison benchmarks, see the JMH benchmarks in iceberg-core.
 */
public class RemappingBenchmarkRunner {

  private static final Logger LOG = LoggerFactory.getLogger(RemappingBenchmarkRunner.class);
  private static final String DV_BLOB_TYPE = "deletion-vector-v1";

  private final BenchmarkConfig config;
  private final FileIO fileIO;
  private final String baseLocation;
  private final BenchmarkMetrics metrics;

  private final PositionDeleteGenerator posDeleteGenerator;
  private final DeletionVectorGenerator dvGenerator;
  private final CompactionMapGenerator mapGenerator;

  public RemappingBenchmarkRunner(BenchmarkConfig config, FileIO fileIO, String baseLocation) {
    this.config = config;
    this.fileIO = fileIO;
    this.baseLocation = baseLocation;
    this.metrics = new BenchmarkMetrics();

    this.posDeleteGenerator = new PositionDeleteGenerator(config.randomSeed());
    this.dvGenerator = new DeletionVectorGenerator(config.randomSeed());
    this.mapGenerator = new CompactionMapGenerator(config.randomSeed());
  }

  public BenchmarkMetrics getMetrics() {
    return metrics;
  }

  /** Run all configured benchmark scenarios. */
  public void runAll() throws IOException {
    LOG.info(
        "Starting remapping benchmark with {} warmup and {} measurement iterations",
        config.warmupIterations(),
        config.measurementIterations());

    for (DeleteFormat format : config.formats()) {
      for (Density density : config.densities()) {
        for (int numDeletes : config.deleteCounts()) {
          for (int numRuns : config.runCounts()) {
            // For public API benchmarks, we only test SMART strategy
            // For detailed strategy comparisons, use the JMH benchmarks in core
            runScenario(format, density, numDeletes, numRuns, Strategy.SMART);
          }
        }
      }
    }

    LOG.info("Benchmark complete. {} results collected.", metrics.getResults().size());
  }

  /** Run a single scenario with warmup and measurement iterations. */
  public void runScenario(
      DeleteFormat format, Density density, int numDeletes, int numRuns, Strategy strategy)
      throws IOException {

    LOG.info(
        "Running scenario: format={}, density={}, deletes={}, runs={}, strategy={}",
        format,
        density,
        numDeletes,
        numRuns,
        strategy);

    // Generate test data
    TestData testData = generateTestData(format, density, numDeletes, numRuns);

    // Warmup iterations
    for (int i = 0; i < config.warmupIterations(); i++) {
      BenchmarkResult result =
          runSingleIteration(testData, format, density, numDeletes, numRuns, strategy, i, true);
      metrics.record(result);
    }

    // Measurement iterations (offset by warmup count to avoid file path collision)
    int warmupCount = config.warmupIterations();
    for (int i = 0; i < config.measurementIterations(); i++) {
      BenchmarkResult result =
          runSingleIteration(
              testData, format, density, numDeletes, numRuns, strategy, warmupCount + i, false);
      metrics.record(result);
    }

    // Cleanup test data
    cleanupTestData(testData);
  }

  private BenchmarkResult runSingleIteration(
      TestData testData,
      DeleteFormat format,
      Density density,
      int numDeletes,
      int numRuns,
      Strategy strategy,
      int iteration,
      boolean warmup)
      throws IOException {

    long totalStart = System.nanoTime();

    // Phase 1: Read deletes from storage
    long readStart = System.nanoTime();
    Object deletes = readDeletes(testData, format);
    long readEnd = System.nanoTime();

    // Phase 2: Remap positions using PositionDeleteRemapper
    long remapStart = System.nanoTime();
    Object remapped = remapDeletes(deletes, testData.remapper, format);
    long remapEnd = System.nanoTime();

    // Phase 3: Write remapped deletes
    long writeStart = System.nanoTime();
    long outputSize = writeDeletes(testData, remapped, format, iteration);
    long writeEnd = System.nanoTime();

    long totalEnd = System.nanoTime();

    return BenchmarkResult.builder()
        .format(format)
        .density(density)
        .strategy(strategy)
        .numDeletes(numDeletes)
        .numRuns(numRuns)
        .numSourceFiles(testData.generatedMap.numSourceFiles())
        .numTargetFiles(testData.generatedMap.numTargetFiles())
        .readLatencyNs(readEnd - readStart)
        .remapLatencyNs(remapEnd - remapStart)
        .writeLatencyNs(writeEnd - writeStart)
        .totalLatencyNs(totalEnd - totalStart)
        .inputSizeBytes(testData.inputSizeBytes)
        .outputSizeBytes(outputSize)
        .mapSizeBytes(testData.generatedMap.fileSizeBytes())
        .iteration(iteration)
        .warmup(warmup)
        .build();
  }

  private TestData generateTestData(
      DeleteFormat format, Density density, int numDeletes, int numRuns) throws IOException {

    String scenarioDir =
        String.format(
            Locale.ROOT, "%s/%s_%s_d%d_r%d", baseLocation, format, density, numDeletes, numRuns);

    // Generate compaction map with fanout scenario
    int numSourceFiles = Math.max(1, numRuns / 10);
    int runsPerFile = Math.max(1, numRuns / numSourceFiles);
    OutputFile mapOutput = fileIO.newOutputFile(scenarioDir + "/compaction-map.avro");
    GeneratedCompactionMap generatedMap =
        mapGenerator.generateFanout(mapOutput, numSourceFiles, runsPerFile);

    // Read the compaction map
    InputFile mapInput = fileIO.newInputFile(generatedMap.path());
    CompactionMap compactionMap = CompactionMaps.read(mapInput);

    // Create remapper
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(compactionMap);

    // Generate deletes
    long inputSizeBytes;
    String deletePath;

    if (format == DeleteFormat.POSITION_DELETE_FILE) {
      OutputFile deleteOutput = fileIO.newOutputFile(scenarioDir + "/position-deletes.parquet");
      GeneratedDeleteFile generated =
          posDeleteGenerator.generate(deleteOutput, numDeletes, numSourceFiles, density, true);
      inputSizeBytes = generated.fileSizeBytes();
      deletePath = generated.path();
    } else {
      // For DVs, create one per source file
      int deletesPerDV = numDeletes / numSourceFiles;
      OutputFile dvOutput = fileIO.newOutputFile(scenarioDir + "/deletion-vectors.puffin");
      DeletionVectorGenerator.GeneratedDeletionVectorFile generated =
          dvGenerator.generateMultiple(dvOutput, deletesPerDV, generatedMap.sourceFiles(), density);
      inputSizeBytes = generated.fileSizeBytes();
      deletePath = generated.path();
    }

    return new TestData(scenarioDir, deletePath, generatedMap, remapper, inputSizeBytes);
  }

  private Object readDeletes(TestData testData, DeleteFormat format) throws IOException {
    InputFile input = fileIO.newInputFile(testData.deletePath);

    if (format == DeleteFormat.POSITION_DELETE_FILE) {
      return readPositionDeletes(input);
    } else {
      return readDeletionVectors(input);
    }
  }

  @SuppressWarnings("unchecked")
  private List<PositionDelete<Record>> readPositionDeletes(InputFile input) throws IOException {
    List<PositionDelete<Record>> deletes = new ArrayList<>();

    try (CloseableIterable<Record> reader =
        Parquet.read(input)
            .project(PositionDeleteGenerator.DELETE_SCHEMA)
            .createReaderFunc(
                schema ->
                    GenericParquetReaders.buildReader(
                        PositionDeleteGenerator.DELETE_SCHEMA, schema))
            .build()) {

      for (Record record : reader) {
        String filePath = (String) record.getField("file_path");
        Long pos = (Long) record.getField("pos");
        PositionDelete<Record> delete = PositionDelete.create();
        delete.set(filePath, pos, null);
        deletes.add(delete);
      }
    }

    return deletes;
  }

  private List<DVEntry> readDeletionVectors(InputFile input) throws IOException {
    List<DVEntry> entries = new ArrayList<>();

    try (PuffinReader reader = Puffin.read(input).build()) {
      for (BlobMetadata blobMeta : reader.fileMetadata().blobs()) {
        if (DV_BLOB_TYPE.equals(blobMeta.type())) {
          String referencedFile = blobMeta.properties().get("referenced-data-file");

          // Read blob data
          List<ByteBuffer> blobs = new ArrayList<>();
          for (org.apache.iceberg.util.Pair<BlobMetadata, ByteBuffer> pair :
              reader.readAll(Collections.singletonList(blobMeta))) {
            blobs.add(pair.second());
          }

          if (!blobs.isEmpty()) {
            ByteBuffer buffer = blobs.get(0);
            RoaringBitmap bitmap = new RoaringBitmap();
            bitmap.deserialize(buffer);
            entries.add(new DVEntry(referencedFile, bitmap));
          }
        }
      }
    }

    return entries;
  }

  private Object remapDeletes(
      Object deletes, PositionDeleteRemapper remapper, DeleteFormat format) {

    if (format == DeleteFormat.POSITION_DELETE_FILE) {
      @SuppressWarnings("unchecked")
      List<PositionDelete<Record>> posDeletes = (List<PositionDelete<Record>>) deletes;
      return remapPositionDeletes(posDeletes, remapper);
    } else {
      @SuppressWarnings("unchecked")
      List<DVEntry> dvEntries = (List<DVEntry>) deletes;
      return remapDeletionVectors(dvEntries, remapper);
    }
  }

  @SuppressWarnings("unchecked")
  private List<PositionDelete<Record>> remapPositionDeletes(
      List<PositionDelete<Record>> deletes, PositionDeleteRemapper remapper) {

    List<PositionDelete<Record>> remapped = new ArrayList<>();

    for (PositionDelete<Record> delete : deletes) {
      PositionDelete<?> result = remapper.remapDeleteOrNull(delete);
      if (result != null) {
        remapped.add((PositionDelete<Record>) result);
      }
      // null means the position was filtered during merge compaction - skip it
    }

    return remapped;
  }

  private List<DVEntry> remapDeletionVectors(
      List<DVEntry> entries, PositionDeleteRemapper remapper) {

    // This method uses the same core remapping API as production:
    //   Production: remapDVBulk() -> DVPositionReader.readDeletedPositionsPrimitive() -> long[]
    //                             -> remapPositionsBulkPrimitive(sourceFile, long[])
    //   Benchmark:  RoaringBitmap -> long[] -> remapPositionsBulkPrimitive(sourceFile, long[])
    //
    // Both paths converge on remapPositionsBulkPrimitive(), ensuring the benchmark measures
    // the same algorithm used in production. The benchmark extracts positions directly from
    // RoaringBitmap to avoid I/O overhead, which would obscure remapping performance.

    // Group remapped positions by target file using RoaringBitmap directly
    // This avoids HashSet overhead and boxing/unboxing
    Map<String, RoaringBitmap> remappedByTarget = new HashMap<>();

    for (DVEntry entry : entries) {
      String sourceFile = entry.referencedFile;
      RoaringBitmap sourceBitmap = entry.bitmap;

      // Extract positions as primitive array (no boxing)
      long[] positions = new long[sourceBitmap.getCardinality()];
      int idx = 0;
      for (int pos : sourceBitmap) {
        positions[idx++] = pos;
      }

      // Use the optimized primitive bulk remapping API
      Map<String, long[]> remapped = remapper.remapPositionsBulkPrimitive(sourceFile, positions);

      // Merge directly into RoaringBitmaps (no HashSet intermediate)
      for (Map.Entry<String, long[]> e : remapped.entrySet()) {
        RoaringBitmap targetBitmap =
            remappedByTarget.computeIfAbsent(e.getKey(), k -> new RoaringBitmap());
        for (long pos : e.getValue()) {
          targetBitmap.add((int) pos);
        }
      }
    }

    // Convert to DVEntry list
    List<DVEntry> result = new ArrayList<>();
    for (Map.Entry<String, RoaringBitmap> e : remappedByTarget.entrySet()) {
      result.add(new DVEntry(e.getKey(), e.getValue()));
    }

    return result;
  }

  private long writeDeletes(TestData testData, Object remapped, DeleteFormat format, int iteration)
      throws IOException {

    String outputPath = testData.scenarioDir + "/remapped-" + iteration;

    if (format == DeleteFormat.POSITION_DELETE_FILE) {
      @SuppressWarnings("unchecked")
      List<PositionDelete<Record>> posDeletes = (List<PositionDelete<Record>>) remapped;
      return writePositionDeletes(outputPath + ".parquet", posDeletes);
    } else {
      @SuppressWarnings("unchecked")
      List<DVEntry> dvEntries = (List<DVEntry>) remapped;
      return writeDeletionVectors(outputPath + ".puffin", dvEntries);
    }
  }

  private long writePositionDeletes(String path, List<PositionDelete<Record>> deletes)
      throws IOException {
    OutputFile output = fileIO.newOutputFile(path);

    try (FileAppender<Record> appender =
        Parquet.write(output)
            .schema(PositionDeleteGenerator.DELETE_SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .build()) {

      for (PositionDelete<Record> delete : deletes) {
        GenericRecord record = GenericRecord.create(PositionDeleteGenerator.DELETE_SCHEMA);
        record.setField("file_path", delete.path().toString());
        record.setField("pos", delete.pos());
        appender.add(record);
      }
    }

    return safeGetFileLength(output, 0);
  }

  private long writeDeletionVectors(String path, List<DVEntry> entries) throws IOException {
    OutputFile output = fileIO.newOutputFile(path);

    try (PuffinWriter writer = Puffin.write(output).build()) {
      for (DVEntry entry : entries) {
        ByteBuffer buffer = ByteBuffer.allocate(entry.bitmap.serializedSizeInBytes());
        entry.bitmap.serialize(buffer);
        buffer.flip();

        Blob blob =
            new Blob(
                DV_BLOB_TYPE,
                Collections.singletonList(1),
                0,
                0,
                buffer,
                null,
                Collections.singletonMap("referenced-data-file", entry.referencedFile));

        writer.add(blob);
      }
      writer.finish();
    }

    return safeGetFileLength(output, 0);
  }

  @SuppressWarnings("unused")
  private void cleanupTestData(TestData testData) {
    // Optionally clean up generated files
    // For now, leave them for debugging
  }

  /**
   * Safely get file length from an OutputFile, handling cloud storage eventual consistency.
   *
   * <p>Some cloud storage systems may not immediately return the file after writing. This method
   * handles such cases gracefully by estimating the size from the estimated bytes written.
   */
  private static long safeGetFileLength(OutputFile outputFile, long estimatedBytes) {
    try {
      return outputFile.toInputFile().getLength();
    } catch (Exception e) {
      // Cloud storage may have eventual consistency issues, use estimate
      return estimatedBytes;
    }
  }

  /** Holds generated test data for a scenario. */
  private static class TestData {
    final String scenarioDir;
    final String deletePath;
    final GeneratedCompactionMap generatedMap;
    final PositionDeleteRemapper remapper;
    final long inputSizeBytes;

    TestData(
        String scenarioDir,
        String deletePath,
        GeneratedCompactionMap generatedMap,
        PositionDeleteRemapper remapper,
        long inputSizeBytes) {
      this.scenarioDir = scenarioDir;
      this.deletePath = deletePath;
      this.generatedMap = generatedMap;
      this.remapper = remapper;
      this.inputSizeBytes = inputSizeBytes;
    }
  }

  /** Deletion vector entry (referenced file + bitmap). */
  private static class DVEntry {
    final String referencedFile;
    final RoaringBitmap bitmap;

    DVEntry(String referencedFile, RoaringBitmap bitmap) {
      this.referencedFile = referencedFile;
      this.bitmap = bitmap;
    }
  }
}
