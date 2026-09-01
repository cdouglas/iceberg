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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.WorkloadGenerator;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds one prebuilt warehouse for the compaction-baseline benchmark, either the {@code
 * Baseline-K} or {@code Treatment-K} variant. See {@code COMPACT_SPEC.md} §"Two Frozen Starting
 * States" for the on-disk contract each variant must satisfy.
 *
 * <p>This class is not thread-safe; create a fresh instance per build.
 */
public final class WarehouseBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(WarehouseBuilder.class);

  // Salt constants ensure each derived seed is distinct without entangling streams.
  private static final long SALT_INSERT = 0x1111111111111111L;
  private static final long SALT_DELETE = 0x2222222222222222L;
  private static final long SALT_DISTRIBUTION = 0x3333333333333333L;
  private static final long SALT_LATE_TX = 0x4444444444444444L;

  // Each S_i commit gets its own (partitionId, taskId) so output filenames don't collide. We use
  // the snapshot index as the taskId; partitionId stays zero (unpartitioned table).
  private static final int PARTITION_ID = 0;

  private final HadoopCatalog catalog;
  private final TableIdentifier tableIdent;
  private final BuildConfig config;
  private final List<DataFile> preCompactionDataFiles = Lists.newArrayList();
  private final List<Long> snapshotIds = Lists.newArrayList();
  // Tracks the current DV (file-scoped, V3) for each data file path. Updated after every
  // chain-snapshot commit so the next snapshot's BaseDVFileWriter can merge prior positions.
  private final Map<String, DeleteFile> currentDvByDataFile = Maps.newHashMap();

  public WarehouseBuilder(HadoopCatalog catalog, TableIdentifier tableIdent, BuildConfig config) {
    this.catalog = catalog;
    this.tableIdent = tableIdent;
    this.config = config;
  }

  /**
   * Pre-compaction data file list captured by {@link #buildSnapshotChain(Table)}. Exposed for
   * hand-crafted tests that need to drive late transactions outside the standard {@link
   * #commitLateTx(Table)} flow.
   */
  List<DataFile> preCompactionDataFiles() {
    return preCompactionDataFiles;
  }

  /**
   * Live DV index keyed by data-file path. Updated by {@link #buildSnapshotChain(Table)} and
   * cleared by {@link #runCompactionAndCaptureMap(SparkSession, Table)}. Exposed for hand-
   * crafted tests that compose multiple late transactions and need to merge with already-
   * committed DVs.
   */
  Map<String, DeleteFile> currentDvByDataFile() {
    return currentDvByDataFile;
  }

  /**
   * Build the {@code Baseline-K} variant: snapshot chain {@code S_0..S_n} plus the late transaction
   * {@code S_{n+1}}, no compaction performed. The result is a table whose pre- compaction layout is
   * intact and which contains a single conflicting DV at the head of the snapshot chain.
   */
  public BuildResult buildBaseline() throws IOException {
    Table table = createTable(false /* compactionMapEnabled */);
    buildSnapshotChain(table);
    commitLateTx(table);
    return summarize(table, "baseline", null /* compactionMapPath */, 0 /* runCount */);
  }

  /**
   * Build the {@code Treatment-K} variant: snapshot chain {@code S_0..S_n}, then {@code
   * compact(S_0..S_n)} via {@code SparkActions.rewriteDataFiles}, then the late transaction {@code
   * S_{n+1}} committed as a DV against the (now-orphan) pre-compaction file paths. The compaction
   * map file produced during the rewrite is verified non-empty.
   */
  public BuildResult buildTreatment(SparkSession spark) throws IOException {
    Table table = createTable(true /* compactionMapEnabled */);
    buildSnapshotChain(table);
    String mapPath = runCompactionAndCaptureMap(spark, table);
    commitLateTx(table);

    // Re-read the table after the late-tx commit so summarize() reflects the final state.
    table = catalog.loadTable(tableIdent);
    int runCount = countMapRuns(table, mapPath);
    return summarize(table, "treatment", mapPath, runCount);
  }

  // ---------------------------------------------------------------------------------------------
  // Phase steps
  // ---------------------------------------------------------------------------------------------

  Table createTable(boolean compactionMapEnabled) {
    Map<String, String> props = Maps.newHashMap();
    props.put(TableProperties.FORMAT_VERSION, Integer.toString(config.formatVersion()));
    if (compactionMapEnabled) {
      props.put(TableProperties.COMPACTION_MAP_ENABLED, "true");
    }
    Table table =
        catalog.createTable(
            tableIdent, WorkloadGenerator.SCHEMA, PartitionSpec.unpartitioned(), null, props);
    LOG.info(
        "Created table {} (formatVersion={}, compactionMapEnabled={})",
        tableIdent,
        config.formatVersion(),
        compactionMapEnabled);
    return table;
  }

  /**
   * Upgrade {@code table} to format-version 3 when {@link BuildConfig#upgradeAfterChain()} is set
   * and the table is currently below v3. Called by callers (typically the fuzz runner) between
   * {@link #buildSnapshotChain} and any post-chain operations so the chain history contains
   * position-delete files while later writes can commit DVs. No-op otherwise.
   */
  void maybeUpgradeFormat(Table table) {
    if (!config.upgradeAfterChain()) {
      return;
    }
    if (config.formatVersion() >= 3) {
      return;
    }
    LOG.info("Upgrading table {} from v{} to v3", tableIdent, config.formatVersion());
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();
    table.refresh();
  }

  /**
   * Commit S_0 (insert only) followed by S_1..S_n (each insert + scattered deletes against earlier
   * files). Tracks every committed data-file path so the late transaction can target them.
   */
  void buildSnapshotChain(Table table) throws IOException {
    // S_0
    LOG.info("S_0: writing {} rows", config.s0Rows());
    OutputFileFactory s0Factory = WorkloadCommitter.parquetFileFactory(table, PARTITION_ID, 0L);
    List<DataFile> s0Files =
        WorkloadCommitter.writeDataFiles(
            table,
            s0Factory,
            derive(config.seed(), 0, SALT_INSERT),
            config.s0Rows(),
            config.rowsPerFile());
    AppendFiles s0Append = table.newAppend();
    s0Files.forEach(s0Append::appendFile);
    s0Append.commit();
    table.refresh();
    preCompactionDataFiles.addAll(s0Files);
    snapshotIds.add(table.currentSnapshot().snapshotId());

    // S_1..S_n
    for (int i = 1; i <= config.snapshotChainLength(); i++) {
      LOG.info(
          "S_{}: writing {} rows + {} deletes against {} earlier files",
          i,
          config.perSnapshotRows(),
          config.perSnapshotDeletes(),
          preCompactionDataFiles.size());

      OutputFileFactory dataFactory =
          WorkloadCommitter.parquetFileFactory(table, PARTITION_ID, (long) i);

      List<DataFile> dataFiles =
          WorkloadCommitter.writeDataFiles(
              table,
              dataFactory,
              derive(config.seed(), i, SALT_INSERT),
              config.perSnapshotRows(),
              config.rowsPerFile());

      Map<String, long[]> deletePositions =
          distributeScatteredDeletes(
              preCompactionDataFiles,
              config.perSnapshotDeletes(),
              derive(config.seed(), i, SALT_DELETE),
              derive(config.seed(), i, SALT_DISTRIBUTION));

      if (config.formatVersion() >= 3) {
        commitChainSnapshotV3(table, i, dataFiles, deletePositions);
      } else {
        commitChainSnapshotV2(table, i, dataFiles, deletePositions);
      }
      table.refresh();
      preCompactionDataFiles.addAll(dataFiles);
      snapshotIds.add(table.currentSnapshot().snapshotId());
    }
  }

  /**
   * V3 chain commit: writes a Puffin DV for the snapshot's deletes (merging into any pre-existing
   * DV on the same data file) and commits via {@code RowDelta.addRows + addDeletes + removeDeletes}.
   * The {@code removeDeletes} step retires any DV the writer absorbed, since V3 only allows one
   * DV per data file.
   */
  private void commitChainSnapshotV3(
      Table table, int i, List<DataFile> dataFiles, Map<String, long[]> deletePositions)
      throws IOException {
    OutputFileFactory dvFactory =
        WorkloadCommitter.puffinFileFactory(table, PARTITION_ID, (long) i);
    DeleteWriteResult deleteResult =
        WorkloadCommitter.writeDeletionVectors(
            table, dvFactory, deletePositions, mergingDvLoader(table));

    // validateFromSnapshot limits validateAddedDVs to commits AFTER the current snapshot.
    // Without it the validator walks every ancestor manifest, finds the prior S_i DV adds, and
    // rejects this commit even though we're sequential.
    RowDelta delta = table.newRowDelta().validateFromSnapshot(table.currentSnapshot().snapshotId());
    dataFiles.forEach(delta::addRows);
    deleteResult.deleteFiles().forEach(delta::addDeletes);
    deleteResult.rewrittenDeleteFiles().forEach(delta::removeDeletes);
    delta.commit();

    deleteResult
        .rewrittenDeleteFiles()
        .forEach(df -> currentDvByDataFile.remove(df.referencedDataFile()));
    for (DeleteFile newDv : deleteResult.deleteFiles()) {
      currentDvByDataFile.put(newDv.referencedDataFile(), newDv);
    }
  }

  /**
   * V2 chain commit: writes one parquet position-delete file per source data file (FILE-
   * granularity, so {@link org.apache.iceberg.CompactionConflictDetector} can resolve them later)
   * and commits via {@code RowDelta.addRows + addDeletes}. V2 has no DV uniqueness invariant, so
   * there is no rewrite/remove step — multiple PD files referencing the same data file are legal.
   */
  private void commitChainSnapshotV2(
      Table table, int i, List<DataFile> dataFiles, Map<String, long[]> deletePositions)
      throws IOException {
    OutputFileFactory pdFactory =
        WorkloadCommitter.parquetFileFactory(table, PARTITION_ID, (long) i + 1_000_000L);
    DeleteWriteResult deleteResult =
        WorkloadCommitter.writePositionDeleteFiles(table, pdFactory, deletePositions);

    RowDelta delta = table.newRowDelta().validateFromSnapshot(table.currentSnapshot().snapshotId());
    dataFiles.forEach(delta::addRows);
    deleteResult.deleteFiles().forEach(delta::addDeletes);
    delta.commit();
  }

  /**
   * Run {@code rewriteDataFiles} via Spark, which absorbs all S_1..S_n position deletes into
   * compacted output files and produces a compaction map (because {@code COMPACTION_MAP_ENABLED} is
   * set on the table). Returns the path of the produced compaction-map Avro file.
   */
  String runCompactionAndCaptureMap(SparkSession spark, Table table) {
    return runCompactionAndCaptureMap(spark, table, /* targetFileSizeBytes */ -1L);
  }

  /**
   * Variant that accepts an explicit target file size — used by tests that need the compaction to
   * emit multiple output files (so two late-tx DVs can land on different targets and avoid the
   * resolver's "multiple remapped DVs targeting the same compacted file" merge gap). Production
   * builds pass {@code -1} to fall back to the table's {@code write.target-file-size-bytes}.
   */
  String runCompactionAndCaptureMap(SparkSession spark, Table table, long targetFileSizeBytes) {
    LOG.info("compact(S_0..S_n): running SparkActions.rewriteDataFiles");
    org.apache.iceberg.actions.RewriteDataFiles action =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            // Force the rewrite even if the planner would otherwise consider it not worthwhile;
            // setup needs the compaction to actually happen.
            .option(org.apache.iceberg.actions.SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1");
    if (targetFileSizeBytes > 0) {
      action =
          action.option(
              org.apache.iceberg.actions.SizeBasedFileRewritePlanner.TARGET_FILE_SIZE_BYTES,
              Long.toString(targetFileSizeBytes));
    }
    RewriteDataFiles.Result result = action.execute();
    LOG.info(
        "compaction rewrote {} data files into {}",
        result.rewrittenDataFilesCount(),
        result.addedDataFilesCount());

    table.refresh();
    snapshotIds.add(table.currentSnapshot().snapshotId());

    // Repopulate the DV-by-data-file cache from the post-compaction snapshot rather than
    // clearing it unconditionally.
    //
    // The previous implementation cleared the cache on the assumption that compaction absorbs
    // every chain DV. That's not generally true: SizeBasedFileRewritePlanner is a per-file
    // decision — files already at or under TARGET_FILE_SIZE_BYTES get left alone, and any DV
    // attached to them survives compaction in the live snapshot.
    //
    // When the cache disagreed with the snapshot, the late-tx merger (mergingDvLoader) returned
    // null for those still-live DVs, and the late-tx writer emitted a brand-new DV against the
    // same data file — producing two live DVs per file and tripping V3's "one DV per file"
    // invariant at the next planFiles. Fuzz seeds 59 and 101 reproduced this exactly.
    refreshDvCacheFromSnapshot(table);

    String mapPath = locateCompactionMap(table);
    if (mapPath == null) {
      throw new IllegalStateException(
          "Treatment build expected a compaction map after rewrite, but none was produced. Check "
              + "that COMPACTION_MAP_ENABLED is set and that the rewrite produced FilePositionMappings.");
    }
    LOG.info("compaction map produced at {}", mapPath);
    return mapPath;
  }

  /**
   * Commit S_{n+1}: a DV-only snapshot whose deletes target the captured pre-compaction files.
   *
   * <p>For the treatment variant those files are no longer in the current snapshot — the commit
   * still succeeds because we don't call {@code validateFromSnapshot}, so the {@code
   * CompactionMapValidator} that would otherwise flag the conflict is bypassed. The resulting
   * orphan reference is exactly what the runner's timed region will resolve.
   */
  void commitLateTx(Table table) throws IOException {
    if (config.lateTxDeletes() <= 0) {
      LOG.info("Late transaction skipped (lateTxDeletes <= 0)");
      return;
    }
    Map<String, long[]> positions = distributeClusteredDeletes(preCompactionDataFiles, config);
    LOG.info(
        "S_{n+1}: committing DV with {} positions across {} files (run length {})",
        positions.values().stream().mapToInt(p -> p.length).sum(),
        positions.size(),
        config.lateTxRunLength());

    OutputFileFactory dvFactory =
        WorkloadCommitter.puffinFileFactory(
            table, PARTITION_ID, (long) (config.snapshotChainLength() + 1));
    // After compaction the per-snapshot DVs are gone from the current snapshot; for the baseline
    // variant currentDvByDataFile is still populated and represents the live state. Either way,
    // mergingDvLoader reflects the right thing.
    DeleteWriteResult deleteResult =
        WorkloadCommitter.writeDeletionVectors(table, dvFactory, positions, mergingDvLoader(table));

    // validateFromSnapshot scopes both validateAddedDVs and validateNoCompactionConflicts to
    // commits after the current snapshot. There are none, so both validators short-circuit —
    // even on the treatment variant whose late-tx DV references compacted (orphan) files.
    RowDelta delta = table.newRowDelta().validateFromSnapshot(table.currentSnapshot().snapshotId());
    deleteResult.deleteFiles().forEach(delta::addDeletes);
    deleteResult.rewrittenDeleteFiles().forEach(delta::removeDeletes);
    delta.commit();
    table.refresh();
    snapshotIds.add(table.currentSnapshot().snapshotId());
  }

  /**
   * Build a {@code loadPreviousDeletes} function for {@link
   * WorkloadCommitter#writeDeletionVectors}. Reads the cached DV (if any) for each requested
   * data-file path via {@link BaseDeleteLoader#loadPositionDeletes}.
   */
  private Function<String, PositionDeleteIndex> mergingDvLoader(Table table) {
    DeleteLoader loader = new BaseDeleteLoader(table.io()::newInputFile);
    return path -> {
      DeleteFile existing = currentDvByDataFile.get(path);
      if (existing == null) {
        return null;
      }
      return loader.loadPositionDeletes(Lists.newArrayList(existing), path);
    };
  }

  /**
   * Reset {@link #currentDvByDataFile} to mirror the table's current snapshot. Used after
   * compaction, which may absorb some chain DVs (the common case) but leaves others alone when
   * the planner decides the underlying data file is already at target size. Walking the live
   * delete manifests is the only reliable way to know which DVs actually survived.
   */
  private void refreshDvCacheFromSnapshot(Table table) {
    currentDvByDataFile.clear();
    Snapshot snapshot = table.currentSnapshot();
    if (snapshot == null) {
      return;
    }
    for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), null)) {
        // ManifestReader.iterator() is the public read path that yields only LIVE delete files
        // (no need to filter on entry.status() ourselves). The reader reuses one DeleteFile
        // instance across iterations to amortize allocations, so we must copy before retaining.
        for (DeleteFile deleteFile : reader) {
          if (ContentFileUtil.isDV(deleteFile) && deleteFile.referencedDataFile() != null) {
            currentDvByDataFile.put(deleteFile.referencedDataFile(), deleteFile.copy(false));
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(
            "Failed to read delete manifest " + manifest.path(), e);
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Delete distribution
  // ---------------------------------------------------------------------------------------------

  /**
   * Distribute {@code totalDeletes} positions uniformly across {@code files} weighted by record
   * count. Each file receives {@code totalDeletes * weight} deletes (rounded by random sampling),
   * scattered with run length 1.
   */
  static Map<String, long[]> distributeScatteredDeletes(
      List<DataFile> files, int totalDeletes, long positionSeed, long distributionSeed) {
    if (totalDeletes <= 0 || files.isEmpty()) {
      return Maps.newHashMap();
    }
    Map<String, Integer> perFileCounts = weightedAllocation(files, totalDeletes, distributionSeed);
    Map<String, long[]> result = Maps.newHashMap();
    long perFileSeed = positionSeed;
    for (DataFile file : files) {
      int count = perFileCounts.getOrDefault(file.path().toString(), 0);
      if (count == 0) {
        continue;
      }
      long[] positions =
          WorkloadGenerator.generateClusteredPositions(
              perFileSeed++, file.recordCount(), count, 1 /* targetRunLength */);
      if (positions.length > 0) {
        result.put(file.path().toString(), positions);
      }
    }
    return result;
  }

  /**
   * Distribute the late transaction's clustered deletes. If {@code config.lateTxFileFanout() > 0}
   * the deletes are confined to the {@code fanout} largest files (matching spec §Workload's "Small
   * K targets 1–2 pre-compaction source files"); otherwise they spread proportionally across all
   * files.
   */
  static Map<String, long[]> distributeClusteredDeletes(List<DataFile> files, BuildConfig config) {
    List<DataFile> targets = files;
    if (config.lateTxFileFanout() > 0 && config.lateTxFileFanout() < files.size()) {
      targets = Lists.newArrayList(files);
      targets.sort(Comparator.comparingLong(DataFile::recordCount).reversed());
      targets = targets.subList(0, config.lateTxFileFanout());
    }
    Map<String, Integer> perFileCounts =
        weightedAllocation(
            targets,
            config.lateTxDeletes(),
            derive(config.seed(), 0, SALT_DISTRIBUTION ^ SALT_LATE_TX));
    Map<String, long[]> result = Maps.newHashMap();
    long perFileSeed = derive(config.seed(), 0, SALT_LATE_TX);
    for (DataFile file : targets) {
      int count = perFileCounts.getOrDefault(file.path().toString(), 0);
      if (count == 0) {
        continue;
      }
      // Cap the requested deletes to the file's row count; the run-length-100 GDPR shape means a
      // single small file might otherwise be asked to absorb all of K=1M.
      int capped = (int) Math.min(count, file.recordCount());
      long[] positions =
          WorkloadGenerator.generateClusteredPositions(
              perFileSeed++, file.recordCount(), capped, config.lateTxRunLength());
      if (positions.length > 0) {
        result.put(file.path().toString(), positions);
      }
    }
    return result;
  }

  /**
   * Allocate {@code total} units across {@code files} weighted by {@link DataFile#recordCount}.
   * Uses a deterministic largest-remainder rounding so the allocations sum to exactly {@code
   * total}. Returns map of file path → integer count; zero entries are omitted.
   */
  private static Map<String, Integer> weightedAllocation(
      List<DataFile> files, int total, long tieBreakSeed) {
    Map<String, Integer> result = Maps.newHashMap();
    if (total <= 0 || files.isEmpty()) {
      return result;
    }
    long totalRecords = files.stream().mapToLong(DataFile::recordCount).sum();
    if (totalRecords == 0) {
      return result;
    }

    int[] floors = new int[files.size()];
    double[] remainders = new double[files.size()];
    int allocated = 0;
    for (int i = 0; i < files.size(); i++) {
      double share = total * (double) files.get(i).recordCount() / totalRecords;
      floors[i] = (int) Math.floor(share);
      remainders[i] = share - floors[i];
      allocated += floors[i];
    }
    // Distribute the remaining `total - allocated` units to files with the largest remainders;
    // ties broken by a seeded Random for reproducibility.
    int residual = total - allocated;
    Integer[] indices = new Integer[files.size()];
    for (int i = 0; i < indices.length; i++) {
      indices[i] = i;
    }
    Random tieBreak = new Random(tieBreakSeed);
    java.util.Arrays.sort(
        indices,
        (a, b) -> {
          int cmp = Double.compare(remainders[b], remainders[a]);
          if (cmp != 0) {
            return cmp;
          }
          // Stable, deterministic tie-break: pre-shuffle indices using the seeded Random.
          return Integer.compare(tieBreak.nextInt(), tieBreak.nextInt());
        });
    int[] counts = floors.clone();
    for (int k = 0; k < residual; k++) {
      counts[indices[k % indices.length]]++;
    }
    for (int i = 0; i < files.size(); i++) {
      if (counts[i] > 0) {
        result.put(files.get(i).path().toString(), counts[i]);
      }
    }
    return result;
  }

  // ---------------------------------------------------------------------------------------------
  // Compaction-map discovery & summary
  // ---------------------------------------------------------------------------------------------

  /**
   * Walk the current snapshot's manifest list looking for a compaction-map location attached by
   * {@link org.apache.iceberg.ManifestWriter}. Returns the first non-null path found, or {@code
   * null} if none is set.
   */
  private static String locateCompactionMap(Table table) {
    Snapshot current = table.currentSnapshot();
    if (current == null) {
      return null;
    }
    for (ManifestFile manifest : current.allManifests(table.io())) {
      String location = manifest.compactionMapLocation();
      if (location != null) {
        return location;
      }
    }
    return null;
  }

  private static int countMapRuns(Table table, String mapPath) {
    CompactionMap map = CompactionMaps.read(table.io().newInputFile(mapPath));
    int total = 0;
    for (CompactionMap.FileMapping mapping : map.fileMappings()) {
      total += mapping.runs().size();
    }
    return total;
  }

  private BuildResult summarize(
      Table table, String variant, String compactionMapPath, int runCount) {
    long liveRows = 0L;
    int dataFiles = 0;
    int deleteFiles = 0;
    Snapshot current = table.currentSnapshot();
    if (current != null) {
      // Iceberg maintains running totals on every snapshot's summary map; use those instead of
      // walking manifests to avoid an extra I/O pass for what is meant to be cheap reporting.
      Map<String, String> summary = current.summary();
      if (summary != null) {
        if (summary.get("total-records") != null) {
          liveRows = Long.parseLong(summary.get("total-records"));
        }
        if (summary.get("total-data-files") != null) {
          dataFiles = Integer.parseInt(summary.get("total-data-files"));
        }
        if (summary.get("total-delete-files") != null) {
          deleteFiles = Integer.parseInt(summary.get("total-delete-files"));
        }
      }
    }
    return new BuildResult(
        variant,
        config.lateTxDeletes(),
        config.seed(),
        Lists.newArrayList(snapshotIds),
        dataFiles,
        deleteFiles,
        liveRows,
        compactionMapPath,
        runCount);
  }

  // ---------------------------------------------------------------------------------------------
  // Seed derivation
  // ---------------------------------------------------------------------------------------------

  private static long derive(long base, int index, long salt) {
    // Mix index then salt into base via Long.rotateLeft + xor — cheap and avalanche-y enough for
    // independent seed streams.
    long mixed = base ^ (index * 0x9E3779B97F4A7C15L);
    mixed = Long.rotateLeft(mixed, 17) ^ salt;
    return mixed;
  }
}
