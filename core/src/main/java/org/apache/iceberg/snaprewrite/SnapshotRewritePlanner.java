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
package org.apache.iceberg.snaprewrite;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMapChain;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PositionDeleteRemapper;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRewriteUnsafe;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.StructLikeMap;

/**
 * Runs the backward induction that expresses a window of snapshots against a later compaction.
 *
 * <p>Walking backward from the compaction, each step inverts one transaction. The rows it inserted
 * are live at the following state, so the compaction map already knows where they are and they
 * become position deletes. The rows it deleted are live at the preceding state but absent from
 * everything downstream, so they must be copied out of the original layout into a resurrection
 * file. The locator carries the correspondence backward.
 *
 * <p>Nothing here writes. Row locations are fully resolved, so materialization only fills in file
 * metrics.
 */
public class SnapshotRewritePlanner {
  private final TableMetadata base;
  private final FileIO io;
  private final SnapshotRewriteIO rewriteIO;
  private final long minAgeMs;
  private final double maxDeadRatio;

  public SnapshotRewritePlanner(
      TableMetadata base,
      FileIO io,
      SnapshotRewriteIO rewriteIO,
      long minAgeMs,
      double maxDeadRatio) {
    this.base = base;
    this.io = io;
    this.rewriteIO = rewriteIO;
    this.minAgeMs = minAgeMs;
    this.maxDeadRatio = maxDeadRatio;
  }

  /**
   * Plans the rewrite of every snapshot from {@code floorSnapshotId} up to the compaction's parent.
   *
   * @param compactionSnapshotId the compaction to rewrite onto; must carry a compaction map
   * @param floorSnapshotId the oldest snapshot to rewrite, or null for the previous compaction
   */
  public SnapshotRewritePlan plan(long compactionSnapshotId, Long floorSnapshotId) {
    Snapshot compaction = base.snapshot(compactionSnapshotId);
    if (compaction == null) {
      throw new RewriteRefusedException(
          RewriteRefusal.NO_COMPACTION, "unknown snapshot " + compactionSnapshotId);
    }

    List<Snapshot> window = resolveWindow(compaction, floorSnapshotId);
    checkStaticPreconditions(compaction, window);

    PositionDeleteRemapper remapper = loadRemapper(compaction, window);
    RowLocator locator = new RowLocator(remapper);

    PartitionSpec spec = base.spec();
    Map<Long, SnapshotState> states = Maps.newHashMap();
    for (Snapshot snapshot : window) {
      states.put(snapshot.snapshotId(), buildState(snapshot));
    }

    SnapshotState targetState = buildState(compaction);
    List<DataFile> targetFiles = ImmutableList.copyOf(targetState.files.values());
    Map<String, DataFile> newFilesByPath = Maps.newHashMap();
    for (DataFile file : targetFiles) {
      newFilesByPath.put(file.location(), file);
    }

    // The newest snapshot in the window has the same state as the compaction, so its rewritten form
    // is the compaction's files with nothing deleted and nothing resurrected.
    Map<Long, SnapshotRewritePlan.RewrittenSnapshot> rewritten = Maps.newHashMap();
    Snapshot newest = window.get(window.size() - 1);
    rewritten.put(
        newest.snapshotId(),
        new SnapshotRewritePlan.RewrittenSnapshot(newest, ImmutableList.of(), ImmutableList.of()));

    Map<String, PositionSet> deletes = Maps.newHashMap();
    List<String> presentResurrections = Lists.newArrayList();
    List<ResurrectionRequest> resurrections = Lists.newArrayList();
    Map<String, StructLike> partitionByPath = Maps.newHashMap();
    for (DataFile file : targetFiles) {
      partitionByPath.put(file.location(), file.partition());
    }

    long resurrectedRows = 0;
    for (int k = window.size() - 1; k >= 1; k -= 1) {
      Snapshot current = window.get(k);
      Snapshot previous = window.get(k - 1);
      SnapshotState currentState = states.get(current.snapshotId());
      SnapshotState previousState = states.get(previous.snapshotId());

      RowDelta delta = diff(previousState, currentState);

      invertInserts(delta.inserted, current, locator, deletes);
      resurrectedRows +=
          invertDeletes(
              delta.deleted,
              current,
              previousState,
              spec,
              locator,
              resurrections,
              presentResurrections,
              partitionByPath);

      rewritten.put(
          previous.snapshotId(),
          new SnapshotRewritePlan.RewrittenSnapshot(
              previous,
              ImmutableList.copyOf(presentResurrections),
              deleteRequests(previous, spec, deletes, partitionByPath)));
    }

    checkSourceFilesExist(resurrections);

    long targetRows = 0;
    for (DataFile file : targetFiles) {
      targetRows += file.recordCount();
    }

    checkDeadRatio(resurrectedRows, targetRows);

    return new SnapshotRewritePlan(
        base,
        compaction,
        window,
        targetFiles,
        rewritten,
        resurrections,
        detachedFiles(window),
        resurrectedRows,
        targetRows);
  }

  /**
   * Turns the rows a transaction inserted into deletes against the rewritten layout.
   *
   * <p>Every one of these rows is live at the state following the transaction, so the locator has
   * an answer for it: either the compaction map, if it survived to the compaction, or a
   * resurrection file created by inverting a later transaction that deleted it.
   */
  private void invertInserts(
      List<RowRef> inserted,
      Snapshot current,
      RowLocator locator,
      Map<String, PositionSet> deletes) {
    for (RowRef row : inserted) {
      RowRef location = locator.locate(row.path(), row.position());
      if (location == null) {
        throw new RewriteRefusedException(
            RewriteRefusal.UNLOCATABLE_ROW,
            String.format(
                "row %s inserted by snapshot %s is not in the compaction map",
                row, current.snapshotId()));
      }

      deletes
          .computeIfAbsent(location.path(), ignored -> new PositionSet())
          .add(location.position());
    }
  }

  /**
   * Copies the rows a transaction deleted into resurrection files, and records where they went.
   *
   * <p>These rows are live at the preceding state but present in neither the compaction nor any
   * other resurrection file, because each row is killed by exactly one transaction and so recovered
   * by exactly one inverse. That is what bounds a rewrite to one copy of the rows that died inside
   * the window.
   */
  private long invertDeletes(
      List<RowRef> deleted,
      Snapshot current,
      SnapshotState previousState,
      PartitionSpec spec,
      RowLocator locator,
      List<ResurrectionRequest> resurrections,
      List<String> presentResurrections,
      Map<String, StructLike> partitionByPath) {
    if (deleted.isEmpty()) {
      return 0;
    }

    StructLikeMap<List<RowRef>> byPartition = StructLikeMap.create(spec.partitionType());
    for (RowRef dead : deleted) {
      DataFile source = previousState.files.get(dead.path());
      byPartition.computeIfAbsent(source.partition(), ignored -> Lists.newArrayList()).add(dead);
    }

    long count = 0;
    for (Map.Entry<StructLike, List<RowRef>> entry : byPartition.entrySet()) {
      List<RowRef> sources = entry.getValue();
      sources.sort(Comparator.comparing(RowRef::path).thenComparingLong(RowRef::position));

      String path = newDataPath(current.snapshotId());
      resurrections.add(
          new ResurrectionRequest(
              current.snapshotId(), spec, entry.getKey(), base.schema(), sources, path));
      partitionByPath.put(path, entry.getKey());
      presentResurrections.add(path);
      count += sources.size();

      for (int i = 0; i < sources.size(); i += 1) {
        RowRef dead = sources.get(i);
        locator.put(dead.path(), dead.position(), new RowRef(path, i));
      }
    }

    return count;
  }

  // ---------------------------------------------------------------- window and preconditions

  private List<Snapshot> resolveWindow(Snapshot compaction, Long floorSnapshotId) {
    List<Snapshot> ancestors = Lists.newArrayList();
    for (Snapshot ancestor : SnapshotUtil.ancestorsOf(compaction.snapshotId(), base::snapshot)) {
      if (ancestor.snapshotId() == compaction.snapshotId()) {
        continue;
      }

      ancestors.add(ancestor);
      if (floorSnapshotId != null && ancestor.snapshotId() == floorSnapshotId) {
        break;
      }

      if (floorSnapshotId == null && isCompaction(ancestor)) {
        break;
      }
    }

    if (ancestors.isEmpty()) {
      throw new RewriteRefusedException(
          RewriteRefusal.NO_COMPACTION, "no snapshots between the floor and the compaction");
    }

    // ancestorsOf walks newest to oldest; the induction runs the other way.
    return ImmutableList.copyOf(Lists.reverse(ancestors));
  }

  private boolean isCompaction(Snapshot snapshot) {
    return DataOperations.REPLACE.equals(snapshot.operation());
  }

  private void checkStaticPreconditions(Snapshot compaction, List<Snapshot> window) {
    checkFormatAndAge(compaction);
    checkSchema(compaction, window);

    List<Snapshot> all = Lists.newArrayList(window);
    all.add(compaction);
    checkNoEqualityDeletes(all);
    checkSingleSpec(all);
  }

  private void checkFormatAndAge(Snapshot compaction) {
    if (base.formatVersion() != 2) {
      throw new RewriteRefusedException(
          RewriteRefusal.FORMAT_VERSION, "format version " + base.formatVersion());
    }

    if (System.currentTimeMillis() - compaction.timestampMillis() < minAgeMs) {
      throw new RewriteRefusedException(
          RewriteRefusal.TOO_RECENT,
          String.format(
              "compaction %s is %sms old, minimum is %sms",
              compaction.snapshotId(),
              System.currentTimeMillis() - compaction.timestampMillis(),
              minAgeMs));
    }
  }

  private void checkSchema(Snapshot compaction, List<Snapshot> window) {
    Integer schemaId = compaction.schemaId();
    for (Snapshot snapshot : window) {
      if (schemaId != null && !schemaId.equals(snapshot.schemaId())) {
        throw new RewriteRefusedException(
            RewriteRefusal.SCHEMA_CHANGED,
            String.format(
                "snapshot %s uses schema %s, compaction uses %s",
                snapshot.snapshotId(), snapshot.schemaId(), schemaId));
      }
    }
  }

  private void checkNoEqualityDeletes(List<Snapshot> all) {
    for (Snapshot snapshot : all) {
      for (ManifestFile manifest : snapshot.deleteManifests(io)) {
        try (ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(manifest, io, base.specsById())) {
          for (DeleteFile file : reader) {
            if (file.content() == FileContent.EQUALITY_DELETES) {
              throw new RewriteRefusedException(
                  RewriteRefusal.EQUALITY_DELETES,
                  String.format(
                      "snapshot %s references %s", snapshot.snapshotId(), file.location()));
            }
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
  }

  private void checkSingleSpec(List<Snapshot> all) {
    Set<Integer> specIds = Sets.newHashSet();
    for (Snapshot snapshot : all) {
      for (ManifestFile manifest : snapshot.dataManifests(io)) {
        specIds.add(manifest.partitionSpecId());
      }
    }

    if (specIds.size() > 1) {
      throw new RewriteRefusedException(
          RewriteRefusal.SPEC_CHANGED, "partition spec ids in window: " + specIds);
    }
  }

  private void checkSourceFilesExist(List<ResurrectionRequest> requests) {
    Set<String> paths = Sets.newHashSet();
    for (ResurrectionRequest request : requests) {
      for (RowRef source : request.sources()) {
        paths.add(source.path());
      }
    }

    for (String path : paths) {
      if (!io.newInputFile(path).exists()) {
        throw new RewriteRefusedException(RewriteRefusal.MISSING_SOURCE_FILE, path);
      }
    }
  }

  private void checkDeadRatio(long resurrectedRows, long targetRows) {
    if (targetRows > 0) {
      double ratio = (double) resurrectedRows / targetRows;
      if (ratio > maxDeadRatio) {
        throw new RewriteRefusedException(
            RewriteRefusal.DEAD_RATIO,
            String.format(
                Locale.ROOT,
                "%s rows to resurrect against %s live rows (ratio %.3f, max %.3f)",
                resurrectedRows,
                targetRows,
                ratio,
                maxDeadRatio));
      }
    }
  }

  // ---------------------------------------------------------------- compaction maps

  private PositionDeleteRemapper loadRemapper(Snapshot compaction, List<Snapshot> window) {
    List<CompactionMap> maps = Lists.newArrayList();

    // Compactions inside the window relocate rows before the final compaction does, so their maps
    // compose with it. The window's oldest snapshot is the previous compaction; its own map
    // describes a layout change before the window and is not needed here.
    for (int i = 1; i < window.size(); i += 1) {
      Snapshot snapshot = window.get(i);
      if (isCompaction(snapshot)) {
        maps.add(requireMap(snapshot));
      }
    }

    maps.add(requireMap(compaction));

    if (maps.size() == 1) {
      return new PositionDeleteRemapper(maps.get(0));
    }

    return new PositionDeleteRemapper(CompactionMapChain.build(maps));
  }

  private CompactionMap requireMap(Snapshot snapshot) {
    CompactionMap map = new CompactionMapLookup(io).forSnapshot(snapshot);
    if (map == null) {
      throw new RewriteRefusedException(
          RewriteRefusal.MISSING_COMPACTION_MAP, "snapshot " + snapshot.snapshotId());
    }

    return map;
  }

  // ---------------------------------------------------------------- snapshot state and diffs

  private SnapshotState buildState(Snapshot snapshot) {
    Map<String, DataFile> files = Maps.newHashMap();
    for (ManifestFile manifest : snapshot.dataManifests(io)) {
      try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, io, base.specsById())) {
        for (DataFile file : reader) {
          files.put(file.location(), file);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    SnapshotRewriteUnsafe.DeleteIndex index =
        SnapshotRewriteUnsafe.deleteIndex(io, snapshot.deleteManifests(io), base.specsById());

    Map<String, PositionSet> deleted = Maps.newHashMap();
    for (DataFile file : files.values()) {
      DeleteFile[] applicable = index.forDataFile(file);
      PositionDeleteIndex positions =
          applicable.length == 0
              ? null
              : rewriteIO.loadPositionDeletes(Arrays.asList(applicable), file.location());
      deleted.put(file.location(), PositionSet.deleted(file.recordCount(), positions));
    }

    return new SnapshotState(files, deleted);
  }

  /**
   * Computes the rows that changed liveness between two adjacent states.
   *
   * <p>This is a set difference over live rows, not the commit's delta. Delete files are idempotent
   * and a transaction may re-delete a position that was already dead; taking the delta would
   * resurrect a row that was not live in the preceding state and insert it into a state it never
   * belonged to.
   */
  private RowDelta diff(SnapshotState previous, SnapshotState current) {
    RowDelta delta = new RowDelta();
    Set<String> paths = Sets.newHashSet(previous.files.keySet());
    paths.addAll(current.files.keySet());

    for (String path : paths) {
      DataFile previousFile = previous.files.get(path);
      DataFile currentFile = current.files.get(path);

      if (previousFile == null) {
        // File added by this transaction: every row live in it is new.
        addLive(delta.inserted, path, currentFile.recordCount(), current.deleted.get(path));
      } else if (currentFile == null) {
        // File removed outright: every row still live in it must be recovered.
        addLive(delta.deleted, path, previousFile.recordCount(), previous.deleted.get(path));
      } else {
        PositionSet previousDeletes = previous.deleted.get(path);
        PositionSet currentDeletes = current.deleted.get(path);
        add(delta.deleted, path, currentDeletes.minus(previousDeletes));
        add(delta.inserted, path, previousDeletes.minus(currentDeletes));
      }
    }

    return delta;
  }

  private void addLive(List<RowRef> target, String path, long recordCount, PositionSet deleted) {
    for (long pos = 0; pos < recordCount; pos += 1) {
      if (deleted == null || !deleted.contains(pos)) {
        target.add(new RowRef(path, pos));
      }
    }
  }

  private void add(List<RowRef> target, String path, PositionSet positions) {
    positions.forEach(pos -> target.add(new RowRef(path, pos)));
  }

  // ---------------------------------------------------------------- output shaping

  private List<PositionDeleteRequest> deleteRequests(
      Snapshot snapshot,
      PartitionSpec spec,
      Map<String, PositionSet> deletes,
      Map<String, StructLike> partitionByPath) {
    if (deletes.isEmpty()) {
      return ImmutableList.of();
    }

    StructLikeMap<Map<String, PositionSet>> byPartition =
        StructLikeMap.create(spec.partitionType());
    for (Map.Entry<String, PositionSet> entry : deletes.entrySet()) {
      byPartition
          .computeIfAbsent(partitionByPath.get(entry.getKey()), ignored -> Maps.newHashMap())
          .put(entry.getKey(), entry.getValue().copy());
    }

    List<PositionDeleteRequest> requests = Lists.newArrayList();
    for (Map.Entry<StructLike, Map<String, PositionSet>> entry : byPartition.entrySet()) {
      requests.add(
          new PositionDeleteRequest(
              snapshot.snapshotId(),
              spec,
              entry.getKey(),
              entry.getValue(),
              newDeletePath(snapshot.snapshotId())));
    }

    return requests;
  }

  /**
   * Returns files reachable from the window before the rewrite and from nothing after it, with
   * their sizes.
   *
   * <p>Sizes come from manifest metadata wherever possible so that planning stays cheap; only
   * manifest lists, which nothing records the length of, are stat'ed.
   */
  private Map<String, Long> detachedFiles(List<Snapshot> window) {
    Set<Long> windowIds = Sets.newHashSet();
    for (Snapshot snapshot : window) {
      windowIds.add(snapshot.snapshotId());
    }

    Map<String, Long> inWindow = Maps.newHashMap();
    for (Snapshot snapshot : window) {
      collectFiles(snapshot, inWindow);
    }

    for (Snapshot snapshot : base.snapshots()) {
      if (!windowIds.contains(snapshot.snapshotId())) {
        Map<String, Long> elsewhere = Maps.newHashMap();
        collectFiles(snapshot, elsewhere);
        inWindow.keySet().removeAll(elsewhere.keySet());
      }
    }

    return inWindow;
  }

  private void collectFiles(Snapshot snapshot, Map<String, Long> sizes) {
    sizes.put(
        snapshot.manifestListLocation(),
        io.newInputFile(snapshot.manifestListLocation()).getLength());
    for (ManifestFile manifest : snapshot.allManifests(io)) {
      sizes.put(manifest.path(), manifest.length());
    }

    for (DataFile file : snapshot.addedDataFiles(io)) {
      sizes.put(file.location(), file.fileSizeInBytes());
    }

    for (DeleteFile file : snapshot.addedDeleteFiles(io)) {
      sizes.put(file.location(), file.fileSizeInBytes());
    }
  }

  private String newDataPath(long snapshotId) {
    return dataLocation()
        + "/"
        + fileFormat().addExtension("snaprewrite-" + snapshotId + "-" + UUID.randomUUID());
  }

  private String newDeletePath(long snapshotId) {
    return dataLocation()
        + "/"
        + fileFormat().addExtension("snaprewrite-deletes-" + snapshotId + "-" + UUID.randomUUID());
  }

  private String dataLocation() {
    String configured = base.property(TableProperties.WRITE_DATA_LOCATION, null);
    return configured != null ? configured : base.location() + "/data";
  }

  private FileFormat fileFormat() {
    return FileFormat.fromString(
        base.property(
            TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT));
  }

  private static class SnapshotState {
    private final Map<String, DataFile> files;
    private final Map<String, PositionSet> deleted;

    SnapshotState(Map<String, DataFile> files, Map<String, PositionSet> deleted) {
      this.files = files;
      this.deleted = deleted;
    }
  }

  private static class RowDelta {
    private final List<RowRef> inserted = Lists.newArrayList();
    private final List<RowRef> deleted = Lists.newArrayList();
  }
}
