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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CompactionConflictDetector;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteConflictInfo;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.spark.actions.SparkCompactionConflictResolver;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes a {@link FuzzScenario} via both reconciliation paths and returns the resulting row
 * hashes. The fuzz harness drives this with one fresh catalog per seed so concurrent workers cannot
 * collide.
 *
 * <p>{@code applyLateTx} dispatches on op kind ({@link PositionDeleteOp}, {@link AppendOp}, {@link
 * RowReplacementOp}, {@link EqualityDeleteOp}). The dispatcher also branches on the table's current
 * format version, so a single scenario can run identically across v2-only, v3-only, and
 * v2-then-upgrade-to-v3 tables: the same {@link PositionDeleteOp} writes a V2 PD file before
 * upgrade and a V3 DV after.
 */
public final class FuzzRunner {

  private static final Logger LOG = LoggerFactory.getLogger(FuzzRunner.class);

  // The equality-delete writer projects long_0 only. Field IDs match WorkloadGenerator.SCHEMA.
  private static final int EQ_DELETE_FIELD_ID = 5;

  private final SparkSession spark;

  public FuzzRunner(SparkSession spark) {
    this.spark = spark;
  }

  /**
   * Run the scenario in two parallel catalogs and return the (reference, treatment) hash pair.
   *
   * @param scenario the deterministic plan
   * @param workspaceRoot a temporary directory to hold both catalogs
   * @return both hashes (and the op count actually executed)
   */
  public Outcome run(FuzzScenario scenario, java.io.File workspaceRoot) throws IOException {
    java.io.File refDir = new java.io.File(workspaceRoot, "reference");
    java.io.File trtDir = new java.io.File(workspaceRoot, "treatment");
    if (!refDir.mkdirs() && !refDir.isDirectory()) {
      throw new IOException("Could not create reference catalog dir: " + refDir);
    }
    if (!trtDir.mkdirs() && !trtDir.isDirectory()) {
      throw new IOException("Could not create treatment catalog dir: " + trtDir);
    }

    long t0 = System.nanoTime();
    long referenceHash = buildReference(scenario, refDir);
    long refRows =
        spark
            .read()
            .format("iceberg")
            .load("file:" + refDir.getAbsolutePath() + "/db/reference")
            .count();
    long t1 = System.nanoTime();
    long treatmentHash = buildTreatment(scenario, trtDir);
    long trtRows =
        spark
            .read()
            .format("iceberg")
            .load("file:" + trtDir.getAbsolutePath() + "/db/treatment")
            .count();
    long t2 = System.nanoTime();
    LOG.info(
        "seed={} refRows={} trtRows={} refHash={} trtHash={} refMs={} trtMs={}",
        scenario.seed(),
        refRows,
        trtRows,
        referenceHash,
        treatmentHash,
        (t1 - t0) / 1_000_000,
        (t2 - t1) / 1_000_000);
    return new Outcome(referenceHash, treatmentHash, scenario.opsCount(), refRows, trtRows);
  }

  private long buildReference(FuzzScenario scenario, java.io.File dir) throws IOException {
    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), dir.toURI().toString());
    WarehouseBuilder builder =
        new WarehouseBuilder(
            catalog, TableIdentifier.of("db", "reference"), scenario.buildConfig());
    Table table = builder.createTable(false /* compactionMapEnabled */);
    builder.buildSnapshotChain(table);
    builder.maybeUpgradeFormat(table);

    Map<String, DeleteFile> liveDvByPath = builder.currentDvByDataFile();
    List<DataFile> sourceFiles = Lists.newArrayList(builder.preCompactionDataFiles());
    for (LateTxOp op : scenario.lateTxOps()) {
      applyLateTx(table, sourceFiles, op, liveDvByPath);
    }

    // Match the treatment's small target-file-size so both paths produce comparable layouts.
    RewriteDataFiles.Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .option(SizeBasedFileRewritePlanner.TARGET_FILE_SIZE_BYTES, "1048576")
            .execute();
    if (result.rewrittenDataFilesCount() == 0) {
      throw new IOException("Reference rewrite produced no output for seed " + scenario.seed());
    }
    table.refresh();
    return CorrectnessCheck.hash(spark, table.location());
  }

  private long buildTreatment(FuzzScenario scenario, java.io.File dir) throws IOException {
    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), dir.toURI().toString());
    WarehouseBuilder builder =
        new WarehouseBuilder(
            catalog, TableIdentifier.of("db", "treatment"), scenario.buildConfig());
    Table table = builder.createTable(true /* compactionMapEnabled */);
    builder.buildSnapshotChain(table);
    builder.maybeUpgradeFormat(table);
    String mapPath = builder.runCompactionAndCaptureMap(spark, table, 1_048_576L);
    if (mapPath == null) {
      throw new IOException(
          "Treatment compaction produced no compaction map for seed " + scenario.seed());
    }

    Map<String, DeleteFile> liveDvByPath = builder.currentDvByDataFile();
    List<DataFile> sourceFiles = Lists.newArrayList(builder.preCompactionDataFiles());
    for (LateTxOp op : scenario.lateTxOps()) {
      applyLateTx(table, sourceFiles, op, liveDvByPath);
    }

    CompactionMap map = CompactionMaps.read(table.io().newInputFile(mapPath));
    long compactSnapshotId = findReplaceSnapshotId(table);
    long startingSnapshotId = table.snapshot(compactSnapshotId).parentId();
    Snapshot currentSnapshot = table.currentSnapshot();
    TableMetadata base = ((HasTableOperations) table).operations().current();

    java.util.Set<String> mapSourceFiles = Sets.newHashSet();
    for (CompactionMap.FileMapping mapping : map.fileMappings()) {
      mapSourceFiles.add(mapping.sourceFile());
    }
    DeleteConflictInfo conflicts =
        new CompactionConflictDetector(table.io(), base, startingSnapshotId, currentSnapshot)
            .detectConflicts(mapSourceFiles);
    // Use hasConflicts() rather than conflictingDeleteFiles().isEmpty(): the latter ignores
    // PARTITION-granularity (multi-file) PD files, which the resolver handles via the
    // multiFilePositionDeletes() bucket. Skipping the resolver when only multi-file conflicts
    // exist drops deletes silently.
    if (!conflicts.hasConflicts()) {
      return CorrectnessCheck.hash(spark, table.location());
    }

    SparkCompactionConflictResolver resolver = new SparkCompactionConflictResolver(spark, table);
    List<DeleteFile> newDeletes = resolver.resolve(map, conflicts);
    List<DeleteFile> commitable = mergePerTargetFile(table, newDeletes);

    RowDelta delta = table.newRowDelta().validateFromSnapshot(table.currentSnapshot().snapshotId());
    commitable.forEach(delta::addDeletes);
    delta.commit();
    return CorrectnessCheck.hash(spark, table.location());
  }

  /**
   * Dispatch a single late-tx op against the table. Concrete writer is selected by the table's
   * current format version: {@link PositionDeleteOp} writes a DV in v3 and a parquet PD file in v2;
   * {@link RowReplacementOp} mixes either delete shape into a RowDelta alongside fresh data rows.
   * {@link AppendOp} and {@link EqualityDeleteOp} are format-invariant in their output shape but
   * {@link EqualityDeleteOp} is still safe to apply on either format.
   */
  private void applyLateTx(
      Table table, List<DataFile> sourceFiles, LateTxOp op, Map<String, DeleteFile> liveDvByPath)
      throws IOException {
    int formatVersion = ((HasTableOperations) table).operations().current().formatVersion();
    if (op instanceof PositionDeleteOp) {
      applyPositionDelete(table, sourceFiles, (PositionDeleteOp) op, liveDvByPath, formatVersion);
    } else if (op instanceof AppendOp) {
      applyAppend(table, sourceFiles, (AppendOp) op);
    } else if (op instanceof RowReplacementOp) {
      applyRowReplacement(table, sourceFiles, (RowReplacementOp) op, liveDvByPath, formatVersion);
    } else if (op instanceof EqualityDeleteOp) {
      applyEqualityDelete(table, (EqualityDeleteOp) op);
    } else {
      throw new IllegalStateException("Unhandled late-tx op kind: " + op);
    }
  }

  private void applyPositionDelete(
      Table table,
      List<DataFile> sourceFiles,
      PositionDeleteOp op,
      Map<String, DeleteFile> liveDvByPath,
      int formatVersion)
      throws IOException {
    Map<String, long[]> positions =
        clusteredPositionsForSlice(
            sourceFiles,
            op.sliceOffsetFraction(),
            op.sliceWidthFraction(),
            op.opSeed(),
            op.deletesPerOp());
    if (positions.isEmpty()) {
      return;
    }
    if (formatVersion >= 3) {
      writeDvAndCommit(table, positions, op.opSeed(), liveDvByPath);
    } else {
      writePdFileAndCommit(table, positions, op.opSeed());
    }
  }

  private void applyAppend(Table table, List<DataFile> sourceFiles, AppendOp op)
      throws IOException {
    if (op.rows() <= 0) {
      return;
    }
    OutputFileFactory factory = WorkloadCommitter.parquetFileFactory(table, 0, op.opSeed());
    List<DataFile> dataFiles =
        WorkloadCommitter.writeDataFiles(table, factory, op.opSeed(), op.rows(), op.rowsPerFile());
    if (dataFiles.isEmpty()) {
      return;
    }
    AppendFiles append = table.newAppend();
    dataFiles.forEach(append::appendFile);
    append.commit();
    table.refresh();
    // Late-tx appended files become available as sources for subsequent ops in the same scenario.
    sourceFiles.addAll(dataFiles);
  }

  private void applyRowReplacement(
      Table table,
      List<DataFile> sourceFiles,
      RowReplacementOp op,
      Map<String, DeleteFile> liveDvByPath,
      int formatVersion)
      throws IOException {
    Map<String, long[]> positions =
        clusteredPositionsForSlice(
            sourceFiles,
            op.sliceOffsetFraction(),
            op.sliceWidthFraction(),
            op.opSeed(),
            op.deletesPerOp());

    OutputFileFactory dataFactory =
        WorkloadCommitter.parquetFileFactory(table, 0, op.opSeed() ^ 0xC0FFEEL);
    List<DataFile> addedRows =
        WorkloadCommitter.writeDataFiles(
            table,
            dataFactory,
            op.opSeed() ^ 0xDEADBEEFL,
            op.replacementRows(),
            25_000 /* rowsPerFile */);

    if (positions.isEmpty() && addedRows.isEmpty()) {
      return;
    }

    DeleteWriteResult deleteResult;
    if (formatVersion >= 3) {
      OutputFileFactory dvFactory =
          WorkloadCommitter.puffinFileFactory(table, 0, op.opSeed() & 0xFFFFL);
      DeleteLoader loader = new BaseDeleteLoader(table.io()::newInputFile);
      deleteResult =
          WorkloadCommitter.writeDeletionVectors(
              table,
              dvFactory,
              positions,
              path -> loadAllExistingPositionDeletes(table, loader, liveDvByPath, path));
    } else {
      OutputFileFactory pdFactory =
          WorkloadCommitter.parquetFileFactory(table, 0, (op.opSeed() & 0xFFFFL) + 2_000_000L);
      deleteResult = WorkloadCommitter.writePositionDeleteFiles(table, pdFactory, positions);
    }

    RowDelta delta = table.newRowDelta().validateFromSnapshot(table.currentSnapshot().snapshotId());
    addedRows.forEach(delta::addRows);
    deleteResult.deleteFiles().forEach(delta::addDeletes);
    if (formatVersion >= 3) {
      deleteResult.rewrittenDeleteFiles().forEach(delta::removeDeletes);
    }
    delta.commit();
    table.refresh();

    sourceFiles.addAll(addedRows);
    if (formatVersion >= 3) {
      deleteResult
          .rewrittenDeleteFiles()
          .forEach(df -> liveDvByPath.remove(df.referencedDataFile()));
      for (DeleteFile newDv : deleteResult.deleteFiles()) {
        liveDvByPath.put(newDv.referencedDataFile(), newDv);
      }
    }
  }

  private void applyEqualityDelete(Table table, EqualityDeleteOp op) throws IOException {
    Schema eqRowSchema = table.schema().select("long_0");
    OutputFileFactory factory =
        WorkloadCommitter.parquetFileFactory(table, 0, (op.opSeed() & 0xFFFFL) + 3_000_000L);
    DeleteFile eqFile =
        WorkloadCommitter.writeEqualityDeleteFile(
            table,
            factory,
            new int[] {EQ_DELETE_FIELD_ID},
            eqRowSchema,
            op.opSeed(),
            op.rowsPerOp());
    RowDelta delta = table.newRowDelta().validateFromSnapshot(table.currentSnapshot().snapshotId());
    delta.addDeletes(eqFile);
    delta.commit();
    table.refresh();
  }

  /**
   * Compute per-source-file position arrays for a slice {@code [offset, offset+width)} of {@code
   * sourceFiles}. Slice bounds are clamped to the available file list; each in-slice file receives
   * {@code min(deletesPerOp, recordCount)} clustered positions seeded from {@code opSeed}.
   */
  private static Map<String, long[]> clusteredPositionsForSlice(
      List<DataFile> sourceFiles,
      double sliceOffsetFraction,
      double sliceWidthFraction,
      long opSeed,
      int deletesPerOp) {
    int total = sourceFiles.size();
    if (total == 0 || deletesPerOp <= 0) {
      return Maps.newLinkedHashMap();
    }
    int sliceOffset = (int) Math.floor(sliceOffsetFraction * total);
    int sliceWidth = Math.max(1, (int) Math.round(sliceWidthFraction * total));
    sliceOffset = Math.min(sliceOffset, total - 1);
    sliceWidth = Math.min(sliceWidth, total - sliceOffset);

    Map<String, long[]> positions = Maps.newLinkedHashMap();
    long perFileSeed = opSeed;
    int end = sliceOffset + sliceWidth;
    for (int i = sliceOffset; i < end; i++) {
      DataFile file = sourceFiles.get(i);
      long[] sel =
          WorkloadGenerator.generateClusteredPositions(
              perFileSeed++,
              file.recordCount(),
              (int) Math.min(deletesPerOp, file.recordCount()),
              1 /* run length 1 */);
      if (sel.length > 0) {
        positions.put(file.path().toString(), sel);
      }
    }
    return positions;
  }

  private void writeDvAndCommit(
      Table table, Map<String, long[]> positions, long opSeed, Map<String, DeleteFile> liveDvByPath)
      throws IOException {
    DeleteLoader loader = new BaseDeleteLoader(table.io()::newInputFile);
    OutputFileFactory factory = WorkloadCommitter.puffinFileFactory(table, 0, opSeed & 0xFFFFL);
    DeleteWriteResult result =
        WorkloadCommitter.writeDeletionVectors(
            table,
            factory,
            positions,
            path -> loadAllExistingPositionDeletes(table, loader, liveDvByPath, path));
    RowDelta delta = table.newRowDelta().validateFromSnapshot(table.currentSnapshot().snapshotId());
    result.deleteFiles().forEach(delta::addDeletes);
    result.rewrittenDeleteFiles().forEach(delta::removeDeletes);
    delta.commit();
    table.refresh();

    result.rewrittenDeleteFiles().forEach(df -> liveDvByPath.remove(df.referencedDataFile()));
    for (DeleteFile newDv : result.deleteFiles()) {
      liveDvByPath.put(newDv.referencedDataFile(), newDv);
    }
  }

  /**
   * Load all position-delete content (both V2 PD files and V3 DVs) currently attached to {@code
   * dataFilePath} in the table's current snapshot, so a newly-written V3 DV can absorb them.
   *
   * <p>V3 planning treats a DV as the sole source of position deletes for its data file; any
   * pre-upgrade V2 PD file for the same data file is silently superseded by the DV (see {@code
   * TestRowDelta.testManifestMergingAfterUpgradeToV3} for the upstream contract). Without merging
   * those positions into the new DV, the chain V2 PD deletes are dropped on the floor. The
   * in-memory {@code liveDvByPath} cache only knows about DVs the harness itself committed, so
   * scenarios that upgrade after a V2 chain (e.g. {@link
   * BuildConfig.FormatMix#V2_THEN_UPGRADE_TO_V3}) need a second pass over the manifest tree.
   */
  private static PositionDeleteIndex loadAllExistingPositionDeletes(
      Table table,
      DeleteLoader loader,
      Map<String, DeleteFile> liveDvByPath,
      CharSequence dataFilePath) {
    String pathStr = dataFilePath.toString();
    List<DeleteFile> existing = Lists.newArrayList();
    DeleteFile dv = liveDvByPath.get(pathStr);
    if (dv != null) {
      existing.add(dv);
    }
    Snapshot current = table.currentSnapshot();
    if (current != null) {
      for (ManifestFile manifest : current.deleteManifests(table.io())) {
        try (ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(manifest, table.io(), null)) {
          // ManifestReader.iterator() iterates LIVE entries (skipping DELETED) and copies each
          // file, so we don't need package-private ManifestEntry access here.
          for (DeleteFile df : reader) {
            if (df.content() != FileContent.POSITION_DELETES) {
              continue;
            }
            if (ContentFileUtil.isDV(df)) {
              continue; // already handled via liveDvByPath above
            }
            String referenced = ContentFileUtil.referencedDataFileLocation(df);
            if (pathStr.equals(referenced)) {
              existing.add(df);
            }
          }
        } catch (IOException e) {
          throw new java.io.UncheckedIOException(e);
        }
      }
    }
    if (existing.isEmpty()) {
      return null;
    }
    return loader.loadPositionDeletes(existing, pathStr);
  }

  private void writePdFileAndCommit(Table table, Map<String, long[]> positions, long opSeed)
      throws IOException {
    OutputFileFactory factory =
        WorkloadCommitter.parquetFileFactory(table, 0, (opSeed & 0xFFFFL) + 4_000_000L);
    DeleteWriteResult result =
        WorkloadCommitter.writePositionDeleteFiles(table, factory, positions);
    if (result.deleteFiles().isEmpty()) {
      return;
    }
    RowDelta delta = table.newRowDelta().validateFromSnapshot(table.currentSnapshot().snapshotId());
    result.deleteFiles().forEach(delta::addDeletes);
    delta.commit();
    table.refresh();
  }

  private List<DeleteFile> mergePerTargetFile(Table table, List<DeleteFile> dvs)
      throws IOException {
    // Only DVs need per-target merging — V3 enforces "one DV per data file" and the harness must
    // collapse multiple remapped DVs targeting the same file before committing. V2 PD files have
    // no uniqueness invariant, can coexist freely against the same data file, and (when written
    // by the resolver's PositionDeletesTable Spark write path) often arrive WITHOUT
    // referenced_data_file populated. Bucketing those by referencedDataFile() collapses them all
    // under a null key and then crashes BaseDeleteLoader.loadPositionDeletes("...file_path=null")
    // when the bucket has size > 1. Skip merging for non-DV inputs entirely.
    List<DeleteFile> merged = Lists.newArrayList();
    Map<String, List<DeleteFile>> groups = new LinkedHashMap<>();
    for (DeleteFile df : dvs) {
      if (ContentFileUtil.isDV(df)) {
        groups.computeIfAbsent(df.referencedDataFile(), k -> Lists.newArrayList()).add(df);
      } else {
        merged.add(df);
      }
    }

    DeleteLoader loader = new BaseDeleteLoader(table.io()::newInputFile);
    for (Map.Entry<String, List<DeleteFile>> entry : groups.entrySet()) {
      if (entry.getValue().size() == 1) {
        merged.add(entry.getValue().get(0));
        continue;
      }
      java.util.NavigableSet<Long> unioned = new java.util.TreeSet<>();
      for (DeleteFile dv : entry.getValue()) {
        PositionDeleteIndex idx =
            loader.loadPositionDeletes(Lists.newArrayList(dv), entry.getKey());
        idx.forEach((java.util.function.LongConsumer) unioned::add);
      }
      if (unioned.isEmpty()) {
        continue;
      }
      long[] positionArray = new long[unioned.size()];
      int i = 0;
      for (long p : unioned) {
        positionArray[i++] = p;
      }
      OutputFileFactory dvFactory = WorkloadCommitter.puffinFileFactory(table, 0, 9100L);
      Map<String, long[]> positions = new LinkedHashMap<>();
      positions.put(entry.getKey(), positionArray);
      DeleteWriteResult result =
          WorkloadCommitter.writeDeletionVectors(table, dvFactory, positions, path -> null);
      merged.addAll(result.deleteFiles());
    }
    return merged;
  }

  private static long findReplaceSnapshotId(Table table) {
    for (Snapshot snapshot : table.snapshots()) {
      if (DataOperations.REPLACE.equals(snapshot.operation())) {
        return snapshot.snapshotId();
      }
    }
    throw new IllegalStateException(
        "Treatment fixture must contain a REPLACE snapshot for seed-driven compaction");
  }

  /** Immutable result tuple from {@link #run}. */
  public static final class Outcome {
    private final long referenceHash;
    private final long treatmentHash;
    private final int opsCount;
    private final long referenceRows;
    private final long treatmentRows;

    Outcome(
        long referenceHash,
        long treatmentHash,
        int opsCount,
        long referenceRows,
        long treatmentRows) {
      this.referenceHash = referenceHash;
      this.treatmentHash = treatmentHash;
      this.opsCount = opsCount;
      this.referenceRows = referenceRows;
      this.treatmentRows = treatmentRows;
    }

    public long referenceHash() {
      return referenceHash;
    }

    public long treatmentHash() {
      return treatmentHash;
    }

    public int opsCount() {
      return opsCount;
    }

    public long referenceRows() {
      return referenceRows;
    }

    public long treatmentRows() {
      return treatmentRows;
    }

    public boolean passed() {
      return referenceHash == treatmentHash;
    }
  }
}
