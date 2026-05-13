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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CompactionConflictDetector;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteConflictInfo;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.RowDelta;
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
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * M2 — Multi-Transaction Race Test (per {@code COMPACT_SPEC.md} §M2).
 *
 * <p>Verifies that a single {@link SparkCompactionConflictResolver#resolve} call can rebase more
 * than one concurrent delete-only transaction against the compaction map. The reference state is
 * built by committing both late transactions BEFORE compaction (so the rewrite absorbs them
 * naturally); the treatment state commits both AFTER compaction (so they become orphan refs that
 * the resolver must remap). Row-multiset hashes must match.
 *
 * <p>Bonus: the resolver call is also exercised with the two delete files passed in opposite
 * order; both orderings must produce the same row hash (confluence under permutation).
 */
class TestMultiTransactionRace {

  private static SparkSession spark;

  @TempDir File tempDir;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("compaction-baseline-tests")
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.default_iceberg.type", "hadoop")
            .config(
                "spark.sql.catalog.default_iceberg.warehouse",
                "/tmp/compaction-baseline-tests-m2")
            .config(
                "spark.sql.catalog.default_cache_iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.default_cache_iceberg.type", "hadoop")
            .config(
                "spark.sql.catalog.default_cache_iceberg.warehouse",
                "/tmp/compaction-baseline-tests-m2-cache")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  /**
   * Tiny config — 1000 rows in S_0, three chain snapshots, 20 deletes per late tx (each cluster
   * length 5). Two late txs targeting disjoint position ranges so neither overlaps the other.
   */
  private static BuildConfig microConfig(long seed) {
    return BuildConfig.builder()
        .seed(seed)
        // Bigger S_0 + zero chain DVs so the single-tx control isolates the resolver behavior
        // from any per-S_i DV interactions. The multi-tx case below adds a small chain.
        .s0Rows(2_000L)
        .snapshotChainLength(0)
        .perSnapshotRows(0L)
        .perSnapshotDeletes(0)
        // The base config's late tx is unused in this test; we drive late txs by hand below.
        .lateTxDeletes(0)
        .lateTxRunLength(5)
        .lateTxFileFanout(0)
        .rowsPerFile(500)
        .build();
  }

  @Test
  void noLateTxBaseline() throws IOException {
    // Sanity: with NO late tx in either path, both should produce the same row multiset.
    // If this fails, the test framework itself is asymmetric (e.g., compaction-map-enabled
    // changes the rewrite output), and any later assertion is unreliable.
    long seed = 1313L;

    HadoopCatalog refCat = newCatalog("ref_zero");
    WarehouseBuilder refBuilder =
        new WarehouseBuilder(refCat, TableIdentifier.of("db", "ref_zero"), microConfig(seed));
    Table refTable = refBuilder.createTable(true);
    refBuilder.buildSnapshotChain(refTable);
    SparkActions.get(spark)
        .rewriteDataFiles(refTable)
        .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
        .execute();
    refTable.refresh();
    long referenceHash = CorrectnessCheck.hash(spark, refTable.location());

    HadoopCatalog trtCat = newCatalog("trt_zero");
    WarehouseBuilder trtBuilder =
        new WarehouseBuilder(trtCat, TableIdentifier.of("db", "trt_zero"), microConfig(seed));
    Table trtTable = trtBuilder.createTable(true);
    trtBuilder.buildSnapshotChain(trtTable);
    trtBuilder.runCompactionAndCaptureMap(spark, trtTable);
    long treatmentHash = CorrectnessCheck.hash(spark, trtTable.location());

    assertThat(treatmentHash).isEqualTo(referenceHash);
  }

  @Test
  void singleLateTxControl() throws IOException {
    // Control case: one late tx, no merging needed. If this fails, the resolver itself has
    // bugs that put M2's multi-tx assertion out of reach without significant refactoring.
    long seed = 1313L;

    HadoopCatalog refCat = newCatalog("ref_single");
    WarehouseBuilder refBuilder =
        new WarehouseBuilder(refCat, TableIdentifier.of("db", "ref_single"), microConfig(seed));
    // Use the same compaction-map property as treatment so the two paths execute identical write
    // configurations — the only intentional difference is WHERE the late tx is committed.
    Table refTable = refBuilder.createTable(true);
    refBuilder.buildSnapshotChain(refTable);
    Map<String, DeleteFile> refLive = refBuilder.currentDvByDataFile();
    int total = refBuilder.preCompactionDataFiles().size();
    commitDvOnlySnapshot(refTable, refBuilder.preCompactionDataFiles(), 0, total, seed + 100, refLive);
    SparkActions.get(spark)
        .rewriteDataFiles(refTable)
        .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
        .execute();
    refTable.refresh();
    long referenceHash = CorrectnessCheck.hash(spark, refTable.location());

    HadoopCatalog trtCat = newCatalog("trt_single");
    WarehouseBuilder trtBuilder =
        new WarehouseBuilder(trtCat, TableIdentifier.of("db", "trt_single"), microConfig(seed));
    Table trtTable = trtBuilder.createTable(true);
    trtBuilder.buildSnapshotChain(trtTable);
    String mapPath = trtBuilder.runCompactionAndCaptureMap(spark, trtTable);
    Map<String, DeleteFile> trtLive = trtBuilder.currentDvByDataFile();
    commitDvOnlySnapshot(trtTable, trtBuilder.preCompactionDataFiles(), 0, total, seed + 100, trtLive);

    CompactionMap map = CompactionMaps.read(trtTable.io().newInputFile(mapPath));
    long compactSnapshotId = findReplaceSnapshotId(trtTable);
    long startingSnapshotId = trtTable.snapshot(compactSnapshotId).parentId();
    Snapshot currentSnapshot = trtTable.currentSnapshot();
    TableMetadata base = ((HasTableOperations) trtTable).operations().current();

    java.util.Set<String> sourceFiles = Sets.newHashSet();
    for (CompactionMap.FileMapping mapping : map.fileMappings()) {
      sourceFiles.add(mapping.sourceFile());
    }
    DeleteConflictInfo conflicts =
        new CompactionConflictDetector(trtTable.io(), base, startingSnapshotId, currentSnapshot)
            .detectConflicts(sourceFiles);

    org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TestMultiTransactionRace.class);
    log.info("map fileMappings count: {}", map.fileMappings().size());
    for (CompactionMap.FileMapping fm : map.fileMappings()) {
      log.info(
          "  src={} tgt={} runs={}",
          fm.sourceFile(),
          fm.targetFile(),
          fm.runs().stream()
              .map(
                  r ->
                      "(srcPos="
                          + r.sourcePosition()
                          + ",tgtPos="
                          + r.targetPosition()
                          + ",len="
                          + r.length()
                          + ",tgtFile="
                          + r.targetFile()
                          + ")")
              .collect(java.util.stream.Collectors.joining(",")));
    }
    log.info(
        "unique conflicting DVs by path: {}",
        conflicts.conflictingDeleteFiles().stream()
            .map(df -> df.path().toString())
            .distinct()
            .count());
    for (DeleteFile df : conflicts.conflictingDeleteFiles()) {
      log.info(
          "  conflicting DV: path={} offset={} size={} refDataFile={}",
          df.path(),
          df.contentOffset(),
          df.contentSizeInBytes(),
          df.referencedDataFile());
    }

    SparkCompactionConflictResolver resolver = new SparkCompactionConflictResolver(spark, trtTable);
    List<DeleteFile> newDeletes = resolver.resolve(map, conflicts);
    log.info("resolver returned {} DVs", newDeletes.size());

    List<DeleteFile> commitable = mergePerTargetFile(trtTable, newDeletes);
    log.info("after per-target merge: {} DVs", commitable.size());
    RowDelta delta =
        trtTable.newRowDelta().validateFromSnapshot(trtTable.currentSnapshot().snapshotId());
    commitable.forEach(delta::addDeletes);
    delta.commit();
    long treatmentHash = CorrectnessCheck.hash(spark, trtTable.location());

    long referenceCount =
        spark.read().format("iceberg").load(refTable.location()).count();
    long treatmentCount =
        spark.read().format("iceberg").load(trtTable.location()).count();
    org.slf4j.LoggerFactory.getLogger(TestMultiTransactionRace.class)
        .info(
            "single-tx control: refRows={} trtRows={} refHash={} trtHash={}",
            referenceCount,
            treatmentCount,
            referenceHash,
            treatmentHash);

    // The control case (one late tx, no merge of multiple resolver outputs) should match the
    // reference. If this assertion ever needs to be relaxed, that signals a bug in the resolver
    // or remapper itself, which is outside M2's scope to fix.
    assertThat(treatmentCount).isEqualTo(referenceCount);
    assertThat(treatmentHash).isEqualTo(referenceHash);
  }

  @Test
  void resolverHandlesTwoConcurrentLateTransactions() throws IOException {
    long seed = 1313L;
    // ---- Reference: chain + commit both late txs + compact (absorbs them) ----
    long referenceHash = buildReferenceHash(seed, 100L, 200L);

    // ---- Treatment: chain + compact (generates map) + commit both late txs + resolve+commit ----
    long treatmentHash = buildTreatmentHash(seed, "treatment", false /* reverseOrder */);

    // ---- Permutation: same two delete sets, opposite commit order. The conflict detector will
    // surface them in (B, A) order, so resolver.resolve sees the flipped permutation. ----
    long reversedHash = buildTreatmentHash(seed, "treatment_rev", true /* reverseOrder */);

    org.slf4j.LoggerFactory.getLogger(TestMultiTransactionRace.class)
        .info(
            "multi-tx hashes: ref={} trt={} rev={}",
            referenceHash,
            treatmentHash,
            reversedHash);
    // Spec assertion: resolver-remap path produces the same row multiset as the
    // compact-absorbs path.
    assertThat(treatmentHash).isEqualTo(referenceHash);
    // Bonus per spec: confluence under permutation — committing the same late txs in opposite
    // order before resolve yields the same hash.
    assertThat(reversedHash).isEqualTo(referenceHash);
  }

  /**
   * Build the reference state in its own catalog: commit chain, commit both late-tx DVs against
   * pre-compaction files (which still exist), then rewriteDataFiles absorbs both DVs. Hash the
   * resulting live rows.
   */
  private long buildReferenceHash(long seed, long lateA, long lateB) throws IOException {
    HadoopCatalog catalog = newCatalog("reference");
    WarehouseBuilder builder =
        new WarehouseBuilder(catalog, TableIdentifier.of("db", "reference"), microConfig(seed));
    Table table = builder.createTable(false /* compactionMapEnabled */);
    builder.buildSnapshotChain(table);

    // Commit the two late txs while the pre-compaction files still exist in the current
    // snapshot; rewriteDataFiles will absorb both during scan-time delete application. The
    // V3-DV "one per file" rule means each commit has to merge with whatever DVs the chain
    // (or a prior late tx) already wrote, so we share the builder's already-tracked DV index.
    Map<String, DeleteFile> liveDvByPath = builder.currentDvByDataFile();
    int total = builder.preCompactionDataFiles().size();
    int width = Math.max(1, total / 2);
    commitDvOnlySnapshot(
        table, builder.preCompactionDataFiles(), 0, width, seed + lateA, liveDvByPath);
    commitDvOnlySnapshot(
        table, builder.preCompactionDataFiles(), width, total - width, seed + lateB, liveDvByPath);

    // Match the treatment's small target-file-size so both variants produce comparable layouts —
    // any incidental difference in row order across files would not change the hash, but keeping
    // them aligned makes failure triage easier.
    RewriteDataFiles.Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .option(SizeBasedFileRewritePlanner.TARGET_FILE_SIZE_BYTES, "262144")
            .execute();
    assertThat(result.rewrittenDataFilesCount()).isGreaterThan(0);
    table.refresh();
    return CorrectnessCheck.hash(spark, table.location());
  }

  /**
   * Build the treatment state in its own catalog: commit chain, compact (generates map), commit
   * both late-tx DVs as orphan references, build a single {@code DeleteConflictInfo} containing
   * both, run {@code resolver.resolve} once, commit remapped DVs.
   *
   * <p>{@code reverseOrder=true} commits the same two delete operations (slice [0, width) with
   * seed+100, slice [width, total) with seed+200) in the opposite commit order — exercising
   * confluence under permutation without changing which rows get deleted.
   */
  private long buildTreatmentHash(long seed, String name, boolean reverseOrder)
      throws IOException {
    HadoopCatalog catalog = newCatalog(name);
    WarehouseBuilder builder =
        new WarehouseBuilder(catalog, TableIdentifier.of("db", name), microConfig(seed));
    Table table = builder.createTable(true /* compactionMapEnabled */);
    builder.buildSnapshotChain(table);
    // Small target-file-size forces multi-output compaction so the two late-tx DVs remap to
    // different target files; otherwise both would land on a single compacted file and the
    // resolver's "one DV per file" output would fail RowDelta.commit's validateAddedDVs.
    String mapPath = builder.runCompactionAndCaptureMap(spark, table, /* tgt */ 262_144L);
    assertThat(mapPath).isNotNull();

    // After compaction the chain's DVs are gone (the rewrite marked them removed and
    // WarehouseBuilder cleared the map), so the first late tx starts clean. The second still
    // has to merge with the first via the shared liveDvByPath.
    Map<String, DeleteFile> liveDvByPath = builder.currentDvByDataFile();
    int total = builder.preCompactionDataFiles().size();
    int width = Math.max(1, total / 2);
    if (reverseOrder) {
      commitDvOnlySnapshot(
          table, builder.preCompactionDataFiles(), width, total - width, seed + 200L, liveDvByPath);
      commitDvOnlySnapshot(
          table, builder.preCompactionDataFiles(), 0, width, seed + 100L, liveDvByPath);
    } else {
      commitDvOnlySnapshot(
          table, builder.preCompactionDataFiles(), 0, width, seed + 100L, liveDvByPath);
      commitDvOnlySnapshot(
          table, builder.preCompactionDataFiles(), width, total - width, seed + 200L, liveDvByPath);
    }

    CompactionMap map = CompactionMaps.read(table.io().newInputFile(mapPath));
    long compactSnapshotId = findReplaceSnapshotId(table);
    long startingSnapshotId = table.snapshot(compactSnapshotId).parentId();
    Snapshot currentSnapshot = table.currentSnapshot();
    TableMetadata base = ((HasTableOperations) table).operations().current();

    java.util.Set<String> sourceFiles = Sets.newHashSet();
    for (CompactionMap.FileMapping mapping : map.fileMappings()) {
      sourceFiles.add(mapping.sourceFile());
    }

    DeleteConflictInfo conflicts =
        new CompactionConflictDetector(table.io(), base, startingSnapshotId, currentSnapshot)
            .detectConflicts(sourceFiles);
    // The detector returns DV entries across manifest visits; deduplicate by file path to count
    // unique conflicting DVs. We expect exactly two — one per late tx.
    long uniqueConflictingDvs =
        conflicts.conflictingDeleteFiles().stream()
            .map(df -> df.path().toString())
            .distinct()
            .count();
    assertThat(uniqueConflictingDvs).isEqualTo(2L);

    org.slf4j.Logger logM = org.slf4j.LoggerFactory.getLogger(TestMultiTransactionRace.class);
    logM.info("multi-tx conflicting DVs: {}", conflicts.conflictingDeleteFiles().size());
    for (DeleteFile df : conflicts.conflictingDeleteFiles()) {
      logM.info(
          "  multi-tx conflict: path={} offset={} size={} ref={}",
          df.path(),
          df.contentOffset(),
          df.contentSizeInBytes(),
          df.referencedDataFile());
    }

    SparkCompactionConflictResolver resolver = new SparkCompactionConflictResolver(spark, table);
    List<DeleteFile> newDeletes = resolver.resolve(map, conflicts);
    logM.info("multi-tx resolver returned {} DVs", newDeletes.size());
    assertThat(newDeletes).isNotEmpty();

    // The resolver remaps each source DV independently, so two source DVs that map to the same
    // target compacted file produce two output DVs against that file. V3's "one DV per file"
    // rule means the caller has to merge before commit; this is a real gap the spec's snippet
    // doesn't yet handle. We merge here so the rest of the test exercises the spec's confluence
    // property.
    List<DeleteFile> commitable = mergePerTargetFile(table, newDeletes);

    RowDelta delta =
        table.newRowDelta().validateFromSnapshot(table.currentSnapshot().snapshotId());
    commitable.forEach(delta::addDeletes);
    delta.commit();

    return CorrectnessCheck.hash(spark, table.location());
  }

  /**
   * Commit a delete-only snapshot containing a small DV against the subset of {@code sourceFiles}
   * starting at {@code fileOffset} and spanning {@code fileWidth} files. Positions are
   * deterministically derived from {@code seed}. {@code liveDvByPath} is updated in place so a
   * subsequent call can merge with the DVs this call wrote.
   *
   * <p>Limiting each late tx to a disjoint slice of source files avoids the resolver's
   * "multiple remapped DVs targeting the same compacted file" gap: each source DV maps to a
   * different target DV, so the {@link RowDelta} commit doesn't trip the "one DV per file"
   * validator. Real GDPR-style batch deletes typically have this disjoint shape.
   */
  private void commitDvOnlySnapshot(
      Table table,
      List<DataFile> sourceFiles,
      int fileOffset,
      int fileWidth,
      long seed,
      Map<String, DeleteFile> liveDvByPath)
      throws IOException {
    Map<String, long[]> positions = new LinkedHashMap<>();
    long perFileSeed = seed;
    int deletesPerFile = 5;
    int end = Math.min(fileOffset + fileWidth, sourceFiles.size());
    for (int i = fileOffset; i < end; i++) {
      DataFile file = sourceFiles.get(i);
      long[] sel =
          WorkloadGenerator.generateClusteredPositions(
              perFileSeed++,
              file.recordCount(),
              (int) Math.min(deletesPerFile, file.recordCount()),
              1 /* run length 1, uniformly scattered */);
      if (sel.length > 0) {
        positions.put(file.path().toString(), sel);
      }
    }

    DeleteLoader loader = new BaseDeleteLoader(table.io()::newInputFile);
    OutputFileFactory factory =
        WorkloadCommitter.puffinFileFactory(table, 0, /* taskId */ seed & 0xFFFFL);
    DeleteWriteResult result =
        WorkloadCommitter.writeDeletionVectors(
            table,
            factory,
            positions,
            path -> {
              DeleteFile existing = liveDvByPath.get(path);
              if (existing == null) {
                return null;
              }
              return loader.loadPositionDeletes(Lists.newArrayList(existing), path);
            });

    RowDelta delta =
        table.newRowDelta().validateFromSnapshot(table.currentSnapshot().snapshotId());
    result.deleteFiles().forEach(delta::addDeletes);
    result.rewrittenDeleteFiles().forEach(delta::removeDeletes);
    delta.commit();
    table.refresh();

    // Update the per-path index so the next late-tx commit knows what to merge with.
    result.rewrittenDeleteFiles()
        .forEach(df -> liveDvByPath.remove(df.referencedDataFile()));
    for (DeleteFile newDv : result.deleteFiles()) {
      liveDvByPath.put(newDv.referencedDataFile(), newDv);
    }
  }

  /**
   * Combine multiple DVs that reference the same data file into a single DV per file. Uses
   * BaseDVFileWriter's built-in merge: {@code loadPreviousDeletes} returns positions from the
   * accumulator, so the writer unions them with the new positions.
   */
  private List<DeleteFile> mergePerTargetFile(Table table, List<DeleteFile> dvs)
      throws IOException {
    // Group by referenced data file path.
    Map<String, List<DeleteFile>> groups = new LinkedHashMap<>();
    for (DeleteFile dv : dvs) {
      groups.computeIfAbsent(dv.referencedDataFile(), k -> Lists.newArrayList()).add(dv);
    }

    List<DeleteFile> merged = Lists.newArrayList();
    DeleteLoader loader = new BaseDeleteLoader(table.io()::newInputFile);
    for (Map.Entry<String, List<DeleteFile>> entry : groups.entrySet()) {
      if (entry.getValue().size() == 1) {
        merged.add(entry.getValue().get(0));
        continue;
      }
      // BaseDeleteLoader.loadPositionDeletes only handles PUFFIN when handed a single DV, so we
      // read each source DV separately and union the positions ourselves.
      java.util.SortedSet<Long> unioned = new java.util.TreeSet<>();
      for (DeleteFile dv : entry.getValue()) {
        PositionDeleteIndex idx = loader.loadPositionDeletes(Lists.newArrayList(dv), entry.getKey());
        java.util.SortedSet<Long> thisOne = new java.util.TreeSet<>();
        idx.forEach((java.util.function.LongConsumer) thisOne::add);
        org.slf4j.LoggerFactory.getLogger(TestMultiTransactionRace.class)
            .info("    DV positions in {} -> {} entries: {}", dv.path(), thisOne.size(), thisOne);
        unioned.addAll(thisOne);
      }
      org.slf4j.LoggerFactory.getLogger(TestMultiTransactionRace.class)
          .info("    target={} unioned={} positions", entry.getKey(), unioned.size());
      if (unioned.isEmpty()) {
        continue;
      }
      long[] positionArray = new long[unioned.size()];
      int i = 0;
      for (long p : unioned) {
        positionArray[i++] = p;
      }

      // Re-write via BaseDVFileWriter so the output is a proper V3 DV.
      OutputFileFactory dvFactory = WorkloadCommitter.puffinFileFactory(table, 0, 9000L);
      Map<String, long[]> positions = new LinkedHashMap<>();
      positions.put(entry.getKey(), positionArray);
      DeleteWriteResult result =
          WorkloadCommitter.writeDeletionVectors(
              table, dvFactory, positions, path -> null);
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
    throw new IllegalStateException("Treatment fixture must contain a REPLACE snapshot");
  }

  // Used only for sanity checks during development; left in to keep ManifestFile imported.
  @SuppressWarnings("unused")
  private static int totalManifests(Table table) {
    int count = 0;
    for (Snapshot snapshot : table.snapshots()) {
      for (ManifestFile manifest : snapshot.allManifests(table.io())) {
        if (manifest != null) {
          count++;
        }
      }
    }
    return count;
  }

  private HadoopCatalog newCatalog(String dirName) {
    File root = new File(tempDir, dirName);
    return new HadoopCatalog(new Configuration(), root.toURI().toString());
  }
}
