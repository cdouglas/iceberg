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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMapBuilder;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRewriteUnsafe;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;

/**
 * Rebuilds the layout a rewrite replaced, from the rewritten layout and the rewrite's ledger.
 *
 * <p>{@link SnapshotRewriteRestore} undoes a rewrite by pointing back at the original manifest
 * lists, and so stops working at reclaim. This does not need the old files. Every row a window
 * snapshot can read is still in the rewritten layout with its {@code _row_id}, and the ledger says
 * which original file held which ids in which order (SNAPREWRITE_SPEC.md 14). So each original file
 * can be written again, and each snapshot pointed at those files as forward deltas: a data file
 * {@code ADDED} by the commit that wrote it, a deletion vector by the commit that deleted the rows,
 * every sequence number its original.
 *
 * <p>Files are rebuilt one of three ways:
 *
 * <ul>
 *   <li>An original that still exists is referenced as it is.
 *   <li>A file without holes is rebuilt at its original path. It holds the same rows at the same
 *       positions with the same ids, so the compaction maps that name it stay correct.
 *   <li>A file with holes -- rows no window snapshot could read, which the rewritten layout never
 *       held -- is rebuilt without them at a new path, since its later rows moved up. The map of
 *       the compaction that removed it is written again to name the new path and positions.
 * </ul>
 *
 * <p>Each snapshot lists as removed whatever its parent held that it does not, with the parent as
 * it will be once this is committed. That includes the compaction each window ended at, which the
 * rewrite never touched: it keeps its files, but what it removed is now its parent's rebuilt files
 * and vectors, so it is rebuilt too.
 *
 * <p>Nothing here commits. {@link #materialize()} writes files and builds metadata, like a rewrite.
 */
public class SnapshotRewriteReconstruct {
  /** The manifest list a reconstructed snapshot replaced: the rewritten one. */
  public static final String RESTORED_FROM = "snapshot-restored-from";

  /** Prefix under which a reconstructed snapshot keeps the summary of the snapshot it replaced. */
  public static final String RESTORED_PREFIX = "snapshot-restored-from.";

  private final String name;
  private final TableMetadata base;
  private final FileIO io;
  private final SnapshotRewriteIO rewriteIO;

  private SnapshotRewriteReconstruct(Table table, SnapshotRewriteIO rewriteIO) {
    this.name = table.name();
    this.base = ((HasTableOperations) table).operations().current();
    this.io = table.io();
    this.rewriteIO = rewriteIO;
  }

  public static SnapshotRewriteReconstruct forTable(Table table, SnapshotRewriteIO rewriteIO) {
    return new SnapshotRewriteReconstruct(table, rewriteIO);
  }

  /**
   * The snapshots a reconstruction would rebuild, by the ledger each is rebuilt from.
   *
   * <p>A snapshot rewritten twice names two ledgers: the second pass's, and, carried down a level
   * with the rest of the summary it replaced, the first's. The deepest one reaches the original,
   * and since row ids survive every rewrite it can be used directly against the current layout --
   * there is no need to undo the passes one at a time.
   */
  public Map<String, List<Long>> ledgers() {
    Map<String, List<Long>> grouped = Maps.newLinkedHashMap();
    for (Snapshot snapshot : base.snapshots()) {
      LedgerRef ref = deepestLedger(snapshot);
      if (ref != null) {
        grouped
            .computeIfAbsent(ref.location, location -> Lists.newArrayList())
            .add(snapshot.snapshotId());
      }
    }

    return grouped;
  }

  public SnapshotReconstructResult materialize() {
    if (base.formatVersion() < 3) {
      throw new RewriteRefusedException(
          RewriteRefusal.FORMAT_VERSION, "reconstruction keys rows by _row_id, which needs v3");
    }

    Map<String, List<Long>> grouped = ledgers();
    if (grouped.isEmpty()) {
      throw new RewriteRefusedException(
          RewriteRefusal.NO_LEDGER, "no snapshot names a ledger to reconstruct it from");
    }

    // Oldest compaction first. Where two ledgers name the same file, the first to rebuild it
    // decides
    // its layout, and the oldest ledger is the one that describes the original.
    List<Map.Entry<String, SnapshotRewriteLedger>> ledgers = Lists.newArrayList();
    for (String location : grouped.keySet()) {
      ledgers.add(
          Maps.immutableEntry(
              location, SnapshotRewriteLedger.read(io.newInputFile(location), base.specsById())));
    }

    ledgers.sort(Comparator.comparingLong(entry -> compactionOrder(entry.getValue())));

    Set<Long> restored = Sets.newHashSet();
    for (List<Long> snapshotIds : grouped.values()) {
      restored.addAll(snapshotIds);
    }

    Build build = new Build(restored);
    for (Map.Entry<String, SnapshotRewriteLedger> ledger : ledgers) {
      build.reconstruct(
          ledger.getKey(), ledger.getValue(), Sets.newHashSet(grouped.get(ledger.getKey())));
    }

    build.writeSnapshots();
    return build.result();
  }

  /**
   * Returns metadata with every reconstructed snapshot pointed back at the rewritten form it
   * replaced. Uncommitted.
   *
   * <p>The same pointer undo a rewrite has, and with the same limit: it lasts until the rewritten
   * layout is reclaimed.
   *
   * @throws IllegalStateException if no snapshot was reconstructed
   */
  public static TableMetadata undo(TableMetadata metadata) {
    List<Snapshot> snapshots = Lists.newArrayList();
    int count = 0;
    for (Snapshot snapshot : metadata.snapshots()) {
      Map<String, String> summary = snapshot.summary();
      if (summary == null || !summary.containsKey(RESTORED_FROM)) {
        snapshots.add(snapshot);
        continue;
      }

      Map<String, String> replaced = Maps.newHashMap();
      for (Map.Entry<String, String> entry : summary.entrySet()) {
        if (entry.getKey().startsWith(RESTORED_PREFIX)) {
          replaced.put(entry.getKey().substring(RESTORED_PREFIX.length()), entry.getValue());
        }
      }

      snapshots.add(
          SnapshotRewriteUnsafe.newSnapshot(
              snapshot.sequenceNumber(),
              snapshot.snapshotId(),
              snapshot.parentId(),
              snapshot.timestampMillis(),
              snapshot.operation(),
              replaced,
              snapshot.schemaId(),
              summary.get(RESTORED_FROM),
              snapshot.firstRowId(),
              snapshot.addedRows()));
      count += 1;
    }

    Preconditions.checkState(count > 0, "No reconstructed snapshots to undo");
    return SnapshotRewriteUnsafe.replaceSnapshots(
        metadata, snapshots, metadata.statisticsFiles(), metadata.partitionStatisticsFiles());
  }

  private long compactionOrder(SnapshotRewriteLedger ledger) {
    Snapshot compaction = base.snapshot(ledger.compactionSnapshotId());
    return compaction != null ? compaction.sequenceNumber() : Long.MAX_VALUE;
  }

  /**
   * The ledger that reaches furthest back for a snapshot, and how many rewrites deep it is.
   *
   * <p>A rewrite names its ledger at the top level of a snapshot's summary and copies the summary
   * it replaced under {@code snapshot-rewritten-from.}, so a snapshot rewritten twice carries the
   * first ledger one prefix down. A reconstructed snapshot keeps the summary it replaced under a
   * different prefix, so its old ledgers are not mistaken for live ones.
   */
  private static LedgerRef deepestLedger(Snapshot snapshot) {
    Map<String, String> summary = snapshot.summary();
    if (summary == null) {
      return null;
    }

    LedgerRef deepest = null;
    for (int depth = 0; ; depth += 1) {
      String prefix = SnapshotRewriteRestore.ORIGINAL_PREFIX.repeat(depth);
      String location = summary.get(prefix + SnapshotRewriteLedger.SUMMARY_KEY);
      if (location != null) {
        deepest = new LedgerRef(location, depth);
      }

      String deeper = prefix + SnapshotRewriteRestore.ORIGINAL_PREFIX;
      if (summary.keySet().stream().noneMatch(key -> key.startsWith(deeper))) {
        return deepest;
      }
    }
  }

  /**
   * Whether a snapshot names a ledger at any depth -- as the rewrite that wrote it, or as one a
   * later rewrite carried down. Keys a reconstruction kept under its own prefix do not count.
   */
  private static boolean namesLedger(Snapshot snapshot, String location) {
    Map<String, String> summary = snapshot.summary();
    if (summary == null) {
      return false;
    }

    for (Map.Entry<String, String> entry : summary.entrySet()) {
      String key = entry.getKey();
      while (key.startsWith(SnapshotRewriteRestore.ORIGINAL_PREFIX)) {
        key = key.substring(SnapshotRewriteRestore.ORIGINAL_PREFIX.length());
      }

      if (key.equals(SnapshotRewriteLedger.SUMMARY_KEY) && location.equals(entry.getValue())) {
        return true;
      }
    }

    return false;
  }

  private static class LedgerRef {
    private final String location;
    private final int depth;

    LedgerRef(String location, int depth) {
      this.location = location;
      this.depth = depth;
    }
  }

  /** How one original file is represented in the reconstructed layout. */
  private static class Physical {
    private final DataFile file;
    private final long[] positions;
    private final Kind kind;

    private Physical(DataFile file, long[] positions, Kind kind) {
      this.file = file;
      this.positions = positions;
      this.kind = kind;
    }

    /** Where an original position lives in the physical file, or -1 if it was left out. */
    long position(int original) {
      return positions == null ? original : positions[original];
    }

    boolean isSkipped() {
      return kind == Kind.SKIPPED;
    }
  }

  private enum Kind {
    REUSED,
    REBUILT_IN_PLACE,
    REBUILT_MOVED,
    SKIPPED
  }

  /**
   * A file as a snapshot lists it: the snapshot that added it, and its sequence numbers there. For
   * a deletion vector, the snapshot that added it is the one that deleted the rows it masks.
   */
  private static class Listed<F extends ContentFile<F>> {
    private final F file;
    private final long snapshotId;
    private final long dataSequenceNumber;
    private final long fileSequenceNumber;

    Listed(F file, long snapshotId, long dataSequenceNumber, long fileSequenceNumber) {
      this.file = file;
      this.snapshotId = snapshotId;
      this.dataSequenceNumber = dataSequenceNumber;
      this.fileSequenceNumber = fileSequenceNumber;
    }
  }

  /** The files one snapshot holds, by path. */
  private static class Holding {
    private final Map<String, Listed<DataFile>> data = Maps.newLinkedHashMap();
    private final Map<String, Listed<DeleteFile>> deletes = Maps.newLinkedHashMap();
  }

  /** What a ledger's scan has to gather: where rows are born, and which rows rebuilt files need. */
  private static class Needs {
    private final List<SnapshotRewriteLedger.Entry> toRebuild = Lists.newArrayList();
    private final Set<Integer> births = Sets.newHashSet();
    private final Map<Integer, Set<Long>> wanted = Maps.newHashMap();

    void add(SnapshotRewriteLedger.Entry entry, boolean rebuilding) {
      long[] ids = entry.rowIds();
      int[] born = entry.births();
      for (int pos = 0; pos < born.length; pos += 1) {
        if (born[pos] >= 0) {
          births.add(born[pos]);
          if (rebuilding) {
            wanted.computeIfAbsent(born[pos], index -> Sets.newHashSet()).add(ids[pos]);
          }
        }
      }
    }
  }

  /**
   * The rows of one rebuilt file, and the lineage each has to carry explicitly.
   *
   * <p>Ids and sequence numbers are written only where they cannot be derived: a row whose id is
   * the file's {@code first_row_id} plus its position, and whose sequence number is the file's,
   * reads back the same without them. A file where every row derives is written without the lineage
   * columns at all, as an append would have written it.
   */
  private static class Rows {
    private final SnapshotRewriteLedger.Entry entry;
    private final long[] ids;
    private final long firstRowId;
    private final List<LiveRow> rows = Lists.newArrayList();
    private final List<Long> writtenIds = Lists.newArrayList();
    private final List<Long> writtenSequences = Lists.newArrayList();
    private boolean lineage = false;

    Rows(SnapshotRewriteLedger.Entry entry) {
      this.entry = entry;
      this.ids = entry.rowIds();
      this.firstRowId = entry.file().firstRowId();
    }

    /** Appends the row at an original position, and returns its position in the rebuilt file. */
    long add(int pos, LiveRow row) {
      long at = rows.size();
      Long id = ids[pos] == firstRowId + at ? null : ids[pos];
      Long sequence = derivesSequence(row) ? null : row.lastUpdatedSequenceNumber();
      lineage |= id != null || sequence != null;
      rows.add(row);
      writtenIds.add(id);
      writtenSequences.add(sequence);
      return at;
    }

    /**
     * A null sequence number reads as the file's, so it is left out only where the file has one
     * sequence number wherever it is referenced.
     */
    private boolean derivesSequence(LiveRow row) {
      return entry.hasOneSequence()
          && Objects.equals(
              row.lastUpdatedSequenceNumber(), entry.fileSequenceNumber(entry.first()));
    }

    Schema schema(Schema tableSchema) {
      return lineage ? MetadataColumns.schemaWithRowLineage(tableSchema) : tableSchema;
    }

    List<Record> records(Schema tableSchema, Schema writeSchema) {
      List<Record> records = Lists.newArrayList();
      for (int i = 0; i < rows.size(); i += 1) {
        Record record = GenericRecord.create(writeSchema);
        for (Types.NestedField field : tableSchema.columns()) {
          record.setField(field.name(), rows.get(i).values().getField(field.name()));
        }

        if (lineage) {
          record.setField(MetadataColumns.ROW_ID.name(), writtenIds.get(i));
          record.setField(
              MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), writtenSequences.get(i));
        }

        records.add(record);
      }

      return records;
    }
  }

  /** The state of one reconstruction, which may span several ledgers. */
  private class Build {
    private final Table view =
        new BaseTable(new ShadowTableOperations(base, io), name + "#rewritten");
    private final Set<Long> restored;
    private final Set<Long> parentsOfRestored = Sets.newHashSet();
    private final Map<String, Physical> physical = Maps.newHashMap();
    private final Map<String, DeleteFile> vectorFiles = Maps.newHashMap();
    private final Map<Long, Holding> holdings = Maps.newLinkedHashMap();
    private final Map<Long, String> mapLocations = Maps.newHashMap();
    private final Map<Long, Holding> outside = Maps.newHashMap();
    private final Map<String, PositionSet> retainedPositions = Maps.newHashMap();
    private Map<String, List<Listed<DeleteFile>>> retained = null;
    private final Map<Long, Snapshot> replacements = Maps.newHashMap();
    private final List<String> written = Lists.newArrayList();
    private final List<String> metadataPaths = Lists.newArrayList();
    private long holes = 0;
    private long rebuiltRows = 0;
    private long rebuiltBytes = 0;
    private long predictedBytes = 0;
    private int compactions = 0;
    private int maps = 0;

    Build(Set<Long> restored) {
      this.restored = restored;
      for (long snapshotId : restored) {
        Long parentId = base.snapshot(snapshotId).parentId();
        if (parentId != null) {
          parentsOfRestored.add(parentId);
        }
      }
    }

    void reconstruct(String location, SnapshotRewriteLedger ledger, Set<Long> restore) {
      validate(location, ledger);
      List<SnapshotRewriteLedger.WindowSnapshot> window = ledger.window();
      List<SnapshotRewriteLedger.Entry> entries = restoredEntries(ledger, restore);
      Needs needs = decide(entries);
      Scan scan = new Scan(needs);
      for (int index = 0; index < window.size(); index += 1) {
        scan.read(index, window.get(index).snapshotId());
      }

      Map<SnapshotRewriteLedger.Entry, RowRef[]> assigned = assign(entries, window, scan);
      for (SnapshotRewriteLedger.Entry entry : needs.toRebuild) {
        physical.put(
            entry.file().location(),
            rebuild(entry, assigned.get(entry), scan.values, ledger.specId()));
      }

      Vectors vectors = new Vectors(window, scan.spans, assigned);
      for (int index = 0; index < window.size(); index += 1) {
        long snapshotId = window.get(index).snapshotId();
        if (restore.contains(snapshotId)) {
          holdings.put(snapshotId, holdingAt(ledger, index, vectors));
          mapLocations.put(snapshotId, window.get(index).compactionMapLocation());
        }
      }
    }

    /** The files a reconstructed snapshot holds: those the ledger lists, and their vectors. */
    private Holding holdingAt(SnapshotRewriteLedger ledger, int index, Vectors vectors) {
      Holding holding = new Holding();
      for (SnapshotRewriteLedger.Entry entry : referencedAt(ledger, index)) {
        DataFile file = physical.get(entry.file().location()).file;
        holding.data.put(
            file.location(),
            new Listed<>(
                file,
                entry.addedSnapshotId(),
                entry.dataSequenceNumber(index),
                entry.fileSequenceNumber(index)));
        Listed<DeleteFile> vector = vectors.at(entry, index);
        if (vector != null) {
          holding.deletes.put(vector.file.location(), vector);
        }
      }

      return holding;
    }

    /**
     * Only files some snapshot being restored references. A ledger written over a layout that was
     * itself rewritten also describes that layout's recovery files, which only snapshots restored
     * from an older ledger referenced.
     */
    private List<SnapshotRewriteLedger.Entry> restoredEntries(
        SnapshotRewriteLedger ledger, Set<Long> restore) {
      Set<Integer> restoring = Sets.newHashSet();
      for (int index = 0; index < ledger.window().size(); index += 1) {
        if (restore.contains(ledger.window().get(index).snapshotId())) {
          restoring.add(index);
        }
      }

      List<SnapshotRewriteLedger.Entry> entries = Lists.newArrayList();
      for (SnapshotRewriteLedger.Entry entry : ledger.entries()) {
        for (int index = entry.first(); index <= entry.last(); index += 1) {
          if (restoring.contains(index)) {
            entries.add(entry);
            break;
          }
        }
      }

      return entries;
    }

    /**
     * Decides what can be decided before reading anything: reuse what still exists, skip what has
     * nothing readable, and note what must be rebuilt and which rows that will need.
     */
    private Needs decide(List<SnapshotRewriteLedger.Entry> entries) {
      Needs needs = new Needs();
      for (SnapshotRewriteLedger.Entry entry : entries) {
        String path = entry.file().location();
        boolean rebuilding = false;
        if (!physical.containsKey(path)) {
          PositionSet entryHoles = entry.holes();
          if (entryHoles.size() == entry.file().recordCount()) {
            physical.put(path, new Physical(null, null, Kind.SKIPPED));
            holes += entryHoles.size();
          } else if (io.newInputFile(path).exists()) {
            physical.put(path, new Physical(entry.file(), null, Kind.REUSED));
          } else {
            needs.toRebuild.add(entry);
            rebuilding = true;
          }
        }

        if (!isSkipped(entry)) {
          needs.add(entry, rebuilding);
        }
      }

      return needs;
    }

    private boolean isSkipped(SnapshotRewriteLedger.Entry entry) {
      Physical file = physical.get(entry.file().location());
      return file != null && file.isSkipped();
    }

    /**
     * Assigns each readable position of each file to the physical row holding it.
     *
     * <p>Ids are unique among the rows live at one snapshot, so the row with this id live where the
     * position is born is this position's row -- even if another version of it is live elsewhere at
     * another time.
     */
    private Map<SnapshotRewriteLedger.Entry, RowRef[]> assign(
        List<SnapshotRewriteLedger.Entry> entries,
        List<SnapshotRewriteLedger.WindowSnapshot> window,
        Scan scan) {
      Map<SnapshotRewriteLedger.Entry, RowRef[]> assigned = Maps.newHashMap();
      for (SnapshotRewriteLedger.Entry entry : entries) {
        if (isSkipped(entry)) {
          continue;
        }

        long[] ids = entry.rowIds();
        int[] born = entry.births();
        RowRef[] keys = new RowRef[ids.length];
        for (int pos = 0; pos < ids.length; pos += 1) {
          if (born[pos] >= 0) {
            keys[pos] = scan.idsAt.get(born[pos]).get(ids[pos]);
            if (keys[pos] == null) {
              throw new RewriteRefusedException(
                  RewriteRefusal.UNLOCATABLE_ROW,
                  String.format(
                      "row id %s at %s:%s is not live at snapshot %s",
                      ids[pos], entry.file().location(), pos, window.get(born[pos]).snapshotId()));
            }
          }
        }

        assigned.put(entry, keys);
      }

      return assigned;
    }

    /**
     * Refuses a ledger the table no longer matches.
     *
     * <p>Every window snapshot must still exist and still be expressed against one layout -- the
     * layout of the rewrite that rewrote them last. A window part of which was rewritten again by a
     * later pass would show one row at two locations across the window, and the spans below would
     * be meaningless.
     */
    private void validate(String location, SnapshotRewriteLedger ledger) {
      if (ledger.schemaId() != base.currentSchemaId()) {
        throw new RewriteRefusedException(
            RewriteRefusal.SCHEMA_CHANGED,
            String.format(
                "ledger %s was written at schema %s, the table is now at %s",
                location, ledger.schemaId(), base.currentSchemaId()));
      }

      if (ledger.specId() != base.defaultSpecId()) {
        throw new RewriteRefusedException(
            RewriteRefusal.SPEC_CHANGED,
            String.format(
                "ledger %s was written at spec %s, the table is now at %s",
                location, ledger.specId(), base.defaultSpecId()));
      }

      String layout = null;
      for (SnapshotRewriteLedger.WindowSnapshot windowSnapshot : ledger.window()) {
        Snapshot snapshot = base.snapshot(windowSnapshot.snapshotId());
        if (snapshot == null) {
          throw new RewriteRefusedException(
              RewriteRefusal.LEDGER_MISMATCH,
              String.format(
                  "snapshot %s of ledger %s no longer exists",
                  windowSnapshot.snapshotId(), location));
        }

        if (!namesLedger(snapshot, location)) {
          throw new RewriteRefusedException(
              RewriteRefusal.LEDGER_MISMATCH,
              String.format(
                  "snapshot %s is no longer the one ledger %s rewrote",
                  snapshot.snapshotId(), location));
        }

        String current = snapshot.summary().get(SnapshotRewriteLedger.SUMMARY_KEY);
        if (layout == null) {
          layout = current;
        } else if (!layout.equals(current)) {
          throw new RewriteRefusedException(
              RewriteRefusal.LEDGER_MISMATCH,
              String.format(
                  "the window of ledger %s is expressed against more than one layout", location));
        }
      }
    }

    /**
     * Writes an original file again, from the rows the rewritten layout holds.
     *
     * <p>Ids and sequence numbers are written only where they cannot be derived: a row whose id is
     * the file's {@code first_row_id} plus its position, and whose sequence number is the file's,
     * reads back the same without them. A file where every row derives is written without the
     * lineage columns at all, as an append would have written it.
     */
    private Physical rebuild(
        SnapshotRewriteLedger.Entry entry, RowRef[] keys, Map<RowRef, LiveRow> values, int specId) {
      DataFile original = entry.file();
      PositionSet entryHoles = entry.holes();
      boolean inPlace = entryHoles.isEmpty();
      long[] positions = inPlace ? null : new long[keys.length];
      Rows rows = new Rows(entry);
      for (int pos = 0; pos < keys.length; pos += 1) {
        if (keys[pos] == null) {
          // A hole. Only a file with holes is rebuilt anywhere but in place, so positions exists.
          positions[pos] = -1;
        } else {
          LiveRow row = values.get(keys[pos]);
          Preconditions.checkState(row != null, "No values read for row %s", keys[pos]);
          long at = rows.add(pos, row);
          if (positions != null) {
            positions[pos] = at;
          }
        }
      }

      PartitionSpec spec = base.specsById().get(specId);
      String path = inPlace ? original.location() : newDataPath(entry, spec);
      Schema writeSchema = rows.schema(base.schema());
      DataFile rebuilt =
          rewriteIO.writeRebuilt(
              new RebuildRequest(
                  spec,
                  original.partition(),
                  writeSchema,
                  path,
                  original.firstRowId(),
                  rows.records(base.schema(), writeSchema)));
      written.add(rebuilt.location());
      holes += entryHoles.size();
      rebuiltRows += rebuilt.recordCount();
      rebuiltBytes += rebuilt.fileSizeInBytes();
      predictedBytes += original.fileSizeInBytes();
      return new Physical(rebuilt, positions, inPlace ? Kind.REBUILT_IN_PLACE : Kind.REBUILT_MOVED);
    }

    private String newDataPath(SnapshotRewriteLedger.Entry entry, PartitionSpec spec) {
      FileFormat format = FileFormat.fromFileName(entry.file().location());
      String filename =
          (format != null ? format : FileFormat.PARQUET)
              .addExtension("reconstructed-" + entry.addedSnapshotId() + "-" + UUID.randomUUID());
      LocationProvider locations =
          LocationProviders.locationsFor(base.location(), base.properties());
      return spec.isPartitioned()
          ? locations.newDataLocation(spec, entry.file().partition(), filename)
          : locations.newDataLocation(filename);
    }

    /**
     * Writes every reconstructed snapshot, then every snapshot outside the windows whose parent is
     * one of them: the compaction a window ended at.
     *
     * <p>Last, once every ledger has been read, because each snapshot lists as removed whatever its
     * parent held that it does not, and the parent may have been reconstructed from another ledger.
     * The compaction keeps its files; it is rebuilt because what it removed is now the parent's
     * rebuilt files and vectors, and its map may name a file that moved.
     */
    void writeSnapshots() {
      for (Map.Entry<Long, Holding> entry : holdings.entrySet()) {
        Snapshot rewritten = base.snapshot(entry.getKey());
        replacements.put(
            entry.getKey(),
            write(
                rewritten,
                entry.getValue(),
                mapFor(rewritten, mapLocations.get(entry.getKey())),
                summary(rewritten)));
      }

      CompactionMapLookup lookup = new CompactionMapLookup(io);
      for (Snapshot snapshot : base.snapshots()) {
        Long parentId = snapshot.parentId();
        if (!restored.contains(snapshot.snapshotId())
            && parentId != null
            && restored.contains(parentId)) {
          replacements.put(
              snapshot.snapshotId(),
              write(
                  snapshot,
                  held(snapshot),
                  mapFor(snapshot, lookup.locationFor(snapshot)),
                  keptSummary(snapshot)));
          compactions += 1;
        }
      }
    }

    /**
     * One snapshot as forward deltas against its parent: the files it holds, added or carried, and
     * the files its parent held that it does not, removed.
     */
    private Snapshot write(
        Snapshot current, Holding holding, String mapLocation, Map<String, String> summary) {
      long snapshotId = current.snapshotId();
      Holding parent = parentOf(current);
      List<ManifestFile> manifests = Lists.newArrayList();
      manifests.addAll(
          writeManifests(
              snapshotId,
              holding.data,
              parent.data,
              "reconstruct-data-",
              mapLocation,
              (spec, file) -> ManifestFiles.write(base.formatVersion(), spec, file, snapshotId)));
      manifests.addAll(
          writeManifests(
              snapshotId,
              holding.deletes,
              parent.deletes,
              "reconstruct-deletes-",
              null,
              (spec, file) ->
                  ManifestFiles.writeDeleteManifest(base.formatVersion(), spec, file, snapshotId)));

      String listPath = newMetadataPath("reconstruct-list-" + snapshotId, "avro");
      try {
        SnapshotRewriteUnsafe.writeManifestList(
            base.formatVersion(),
            io.newOutputFile(listPath),
            snapshotId,
            current.parentId(),
            current.sequenceNumber(),
            current.firstRowId(),
            manifests);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      metadataPaths.add(listPath);

      return SnapshotRewriteUnsafe.newSnapshot(
          current.sequenceNumber(),
          snapshotId,
          current.parentId(),
          current.timestampMillis(),
          current.operation(),
          summary,
          current.schemaId(),
          listPath,
          current.firstRowId(),
          current.addedRows());
    }

    /**
     * What a snapshot's parent holds once this is committed: as reconstructed, or, if the
     * reconstruction leaves it alone, as it is. Nothing if it has expired, so a snapshot whose
     * parent has expired lists nothing as removed.
     */
    private Holding parentOf(Snapshot snapshot) {
      Long parentId = snapshot.parentId();
      if (parentId == null) {
        return new Holding();
      }

      Holding reconstructed = holdings.get(parentId);
      if (reconstructed != null) {
        return reconstructed;
      }

      Snapshot parent = base.snapshot(parentId);
      return parent != null ? held(parent) : new Holding();
    }

    /** The files a snapshot the reconstruction does not restore holds, read from its manifests. */
    private Holding held(Snapshot snapshot) {
      return outside.computeIfAbsent(
          snapshot.snapshotId(),
          snapshotId -> {
            Holding holding = new Holding();
            SnapshotRewriteUnsafe.liveEntries(
                snapshot,
                io,
                base.specsById(),
                (file, addedBy) ->
                    holding.data.put(
                        file.location(),
                        new Listed<>(
                            file, addedBy, file.dataSequenceNumber(), file.fileSequenceNumber())),
                (file, addedBy) ->
                    holding.deletes.put(
                        file.location(),
                        new Listed<>(
                            file, addedBy, file.dataSequenceNumber(), file.fileSequenceNumber())));
            return holding;
          });
    }

    /**
     * A vector a snapshot next to a window still holds, for a file reused as it is, that masks
     * exactly these positions and already applied at this sequence number. Null if there is none.
     *
     * <p>Such a vector was carried into the window from before its floor, or out of it into the
     * compaction it ended at, so it is the one the original snapshots carried. Writing an equal one
     * instead would give the floor a vector it never added, and leave the compaction carrying a
     * vector its parent never held.
     */
    private Listed<DeleteFile> retainedVector(
        Physical file, PositionSet mask, long sequenceNumber) {
      if (file.kind != Kind.REUSED) {
        return null;
      }

      Listed<DeleteFile> found = null;
      for (Listed<DeleteFile> vector :
          retainedVectors().getOrDefault(file.file.location(), List.of())) {
        if (vector.dataSequenceNumber <= sequenceNumber
            && (found == null || vector.dataSequenceNumber > found.dataSequenceNumber)
            && positionsOf(vector.file, file.file).equals(mask)) {
          found = vector;
        }
      }

      return found;
    }

    /** Vectors held by the snapshots just outside the windows, by the data file each applies to. */
    private Map<String, List<Listed<DeleteFile>>> retainedVectors() {
      if (retained == null) {
        retained = Maps.newHashMap();
        Set<String> seen = Sets.newHashSet();
        for (Snapshot snapshot : base.snapshots()) {
          if (!bordersAWindow(snapshot)) {
            continue;
          }

          for (Listed<DeleteFile> vector : held(snapshot).deletes.values()) {
            String dataFile = vector.file.referencedDataFile();
            if (dataFile != null && seen.add(vector.file.location())) {
              retained.computeIfAbsent(dataFile, path -> Lists.newArrayList()).add(vector);
            }
          }
        }
      }

      return retained;
    }

    /** Whether a snapshot outside every window is the parent or a child of one inside one. */
    private boolean bordersAWindow(Snapshot snapshot) {
      if (restored.contains(snapshot.snapshotId())) {
        return false;
      }

      Long parentId = snapshot.parentId();
      return (parentId != null && restored.contains(parentId))
          || parentsOfRestored.contains(snapshot.snapshotId());
    }

    private PositionSet positionsOf(DeleteFile vector, DataFile dataFile) {
      return retainedPositions.computeIfAbsent(
          vector.location(),
          path ->
              PositionSet.deleted(
                  dataFile.recordCount(),
                  rewriteIO.loadPositionDeletes(List.of(vector), dataFile.location())));
    }

    /**
     * The compaction map a snapshot should carry: the one it had, or, if a file it names as a
     * source moved, that map with the file's new path and positions.
     *
     * <p>A moved file lost only its holes, and no compaction maps a hole: a row the compaction
     * copied was live in its parent, so some window snapshot read it. Each run therefore still
     * covers consecutive positions of the moved file, and only its start changes. A compaction's
     * own output never has holes, so a target never moves.
     */
    private String mapFor(Snapshot snapshot, String location) {
      if (location == null) {
        return null;
      }

      CompactionMap map = CompactionMaps.read(io.newInputFile(location));
      CompactionMapBuilder builder =
          new CompactionMapBuilder(map.sourceSnapshotId(), map.targetSnapshotId());
      boolean changed = false;
      for (CompactionMap.FileMapping mapping : map.fileMappings()) {
        Preconditions.checkState(
            !moved(mapping.targetFile()), "A compaction's output moved: %s", mapping.targetFile());
        Physical source = physical.get(mapping.sourceFile());
        boolean sourceMoved = moved(mapping.sourceFile());
        CompactionMapBuilder.FileMappingBuilder files =
            builder.addFileMapping(
                sourceMoved ? source.file.location() : mapping.sourceFile(), mapping.targetFile());
        for (CompactionMap.Run run : mapping.runs()) {
          Preconditions.checkState(
              run.targetFile() == null || !moved(run.targetFile()),
              "A compaction's output moved: %s",
              run.targetFile());
          long start = run.sourcePosition();
          if (sourceMoved) {
            start = source.position((int) run.sourcePosition());
            long end = source.position((int) (run.sourcePosition() + run.length() - 1));
            Preconditions.checkState(
                start >= 0 && end == start + run.length() - 1,
                "Snapshot %s maps a hole of %s",
                snapshot.snapshotId(),
                mapping.sourceFile());
          }

          files.addRun(start, run.targetPosition(), run.length(), run.targetFile());
        }

        changed |= sourceMoved;
      }

      if (!changed) {
        return location;
      }

      String path = newMetadataPath("compaction-map-" + snapshot.snapshotId(), "avro");
      try {
        CompactionMaps.write(builder.build(), io.newOutputFile(path));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      metadataPaths.add(path);
      maps += 1;
      return path;
    }

    private boolean moved(String path) {
      Physical file = physical.get(path);
      return file != null && file.kind == Kind.REBUILT_MOVED;
    }

    /**
     * The summary of a snapshot rebuilt outside the windows: its own, plus what is needed to point
     * back at the form it replaced.
     */
    private Map<String, String> keptSummary(Snapshot snapshot) {
      Map<String, String> original = snapshot.summary() != null ? snapshot.summary() : Map.of();
      Map<String, String> summary = Maps.newHashMap();
      for (Map.Entry<String, String> entry : original.entrySet()) {
        if (!entry.getKey().startsWith(RESTORED_FROM)) {
          summary.put(entry.getKey(), entry.getValue());
        }
      }

      summary.put(RESTORED_FROM, snapshot.manifestListLocation());
      for (Map.Entry<String, String> entry : original.entrySet()) {
        summary.put(RESTORED_PREFIX + entry.getKey(), entry.getValue());
      }

      return summary;
    }

    /**
     * The summary the snapshot had before the rewrite reached by its deepest ledger, plus what is
     * needed to point back at the rewritten form.
     */
    private Map<String, String> summary(Snapshot rewritten) {
      int depth = deepestLedger(rewritten).depth;
      String prefix = SnapshotRewriteRestore.ORIGINAL_PREFIX.repeat(depth + 1);
      Map<String, String> summary = Maps.newHashMap();
      for (Map.Entry<String, String> entry : rewritten.summary().entrySet()) {
        if (entry.getKey().startsWith(prefix)) {
          summary.put(entry.getKey().substring(prefix.length()), entry.getValue());
        }
      }

      summary.put(RESTORED_FROM, rewritten.manifestListLocation());
      for (Map.Entry<String, String> entry : rewritten.summary().entrySet()) {
        summary.put(RESTORED_PREFIX + entry.getKey(), entry.getValue());
      }

      return summary;
    }

    SnapshotReconstructResult result() {
      List<Snapshot> snapshots = Lists.newArrayList();
      for (Snapshot snapshot : base.snapshots()) {
        Snapshot replacement = replacements.get(snapshot.snapshotId());
        snapshots.add(replacement != null ? replacement : snapshot);
      }

      TableMetadata reconstructed =
          SnapshotRewriteUnsafe.replaceSnapshots(
              base, snapshots, base.statisticsFiles(), base.partitionStatisticsFiles());

      // What the replaced snapshots referenced that nothing in the result does.
      Map<String, Long> replacedSizes = Maps.newHashMap();
      for (long snapshotId : replacements.keySet()) {
        SnapshotFiles.collect(base.snapshot(snapshotId), io, base.specsById(), replacedSizes);
      }

      Map<String, Long> stillReferenced = Maps.newHashMap();
      for (Snapshot snapshot : reconstructed.snapshots()) {
        SnapshotFiles.collect(snapshot, io, reconstructed.specsById(), stillReferenced);
      }

      replacedSizes.keySet().removeAll(stillReferenced.keySet());
      long reclaimable = replacedSizes.values().stream().mapToLong(Long::longValue).sum();

      long metadataBytes = 0;
      for (String path : metadataPaths) {
        metadataBytes += io.newInputFile(path).getLength();
      }

      long vectorBytes = 0;
      for (DeleteFile file : vectorFiles.values()) {
        vectorBytes += file.fileSizeInBytes();
      }

      int reused = 0;
      int inPlace = 0;
      int moved = 0;
      int skipped = 0;
      for (Physical file : physical.values()) {
        switch (file.kind) {
          case REUSED:
            reused += 1;
            break;
          case REBUILT_IN_PLACE:
            inPlace += 1;
            break;
          case REBUILT_MOVED:
            moved += 1;
            break;
          case SKIPPED:
            skipped += 1;
            break;
        }
      }

      SnapshotReconstructReport report =
          new SnapshotReconstructReport(
              holdings.size(),
              compactions,
              maps,
              reused,
              inPlace,
              moved,
              skipped,
              holes,
              rebuiltRows,
              rebuiltBytes,
              predictedBytes,
              vectorFiles.size(),
              vectorBytes,
              metadataBytes,
              reclaimable);

      List<String> allWritten = Lists.newArrayList(written);
      for (DeleteFile file : vectorFiles.values()) {
        allWritten.add(file.location());
      }

      allWritten.addAll(metadataPaths);
      return new SnapshotReconstructResult(
          name, base, reconstructed, io, report, replacedSizes.keySet(), allWritten);
    }

    /**
     * One read of every window snapshot: when each physical row is live, which row holds each id
     * where rows are born, and the values of the rows a rebuilt file needs.
     */
    private class Scan {
      private final Needs needs;
      private final Map<RowRef, int[]> spans = Maps.newHashMap();
      private final Map<Integer, Map<Long, RowRef>> idsAt = Maps.newHashMap();
      private final Map<RowRef, LiveRow> values = Maps.newHashMap();

      Scan(Needs needs) {
        this.needs = needs;
      }

      void read(int index, long snapshotId) {
        Map<Long, RowRef> ids = needs.births.contains(index) ? Maps.newHashMap() : null;
        Set<Long> want = needs.wanted.getOrDefault(index, Set.of());
        rewriteIO.readLiveRows(
            view, snapshotId, !want.isEmpty(), row -> visit(row, index, snapshotId, ids, want));
        if (ids != null) {
          idsAt.put(index, ids);
        }
      }

      private void visit(
          LiveRow row, int index, long snapshotId, Map<Long, RowRef> ids, Set<Long> want) {
        RowRef key = new RowRef(row.path(), row.position());
        extend(key, index);
        if (row.rowId() == null) {
          return;
        }

        if (ids != null) {
          RowRef prior = ids.put(row.rowId(), key);
          if (prior != null) {
            throw new RewriteRefusedException(
                RewriteRefusal.LEDGER_MISMATCH,
                String.format(
                    "row id %s is live twice in snapshot %s, at %s and %s",
                    row.rowId(), snapshotId, prior, key));
          }
        }

        if (want.contains(row.rowId())) {
          values.putIfAbsent(key, row);
        }
      }

      /** A physical row's span is the run of window snapshots it is live in, and has no gaps. */
      private void extend(RowRef key, int index) {
        int[] span = spans.get(key);
        if (span == null) {
          spans.put(key, new int[] {index, index});
        } else if (span[1] == index - 1) {
          span[1] = index;
        } else {
          throw new RewriteRefusedException(
              RewriteRefusal.LEDGER_MISMATCH,
              String.format("row %s is live, then not, then live again", key));
        }
      }
    }

    /**
     * The deletion vector each file carries at each snapshot, written once per distinct vector.
     *
     * <p>A vector applies from the snapshot that wrote it until one replaces it, so a run of
     * snapshots masking the same positions shares one file, owned by the first of them -- which is
     * the commit that deleted those rows.
     */
    private class Vectors {
      private final List<SnapshotRewriteLedger.WindowSnapshot> window;
      private final Map<RowRef, int[]> spans;
      private final Map<SnapshotRewriteLedger.Entry, RowRef[]> assigned;
      private final Map<SnapshotRewriteLedger.Entry, PositionSet[]> masks = Maps.newHashMap();

      Vectors(
          List<SnapshotRewriteLedger.WindowSnapshot> window,
          Map<RowRef, int[]> spans,
          Map<SnapshotRewriteLedger.Entry, RowRef[]> assigned) {
        this.window = window;
        this.spans = spans;
        this.assigned = assigned;
      }

      Listed<DeleteFile> at(SnapshotRewriteLedger.Entry entry, int index) {
        PositionSet[] entryMasks = masks.computeIfAbsent(entry, this::masksOf);
        PositionSet mask = entryMasks[index - entry.first()];
        if (mask.isEmpty()) {
          return null;
        }

        int owner = index;
        while (owner > entry.first() && entryMasks[owner - 1 - entry.first()].equals(mask)) {
          owner -= 1;
        }

        Physical file = physical.get(entry.file().location());
        long sequenceNumber = base.snapshot(window.get(index).snapshotId()).sequenceNumber();
        Listed<DeleteFile> kept = retainedVector(file, mask, sequenceNumber);
        if (kept != null) {
          return kept;
        }

        long ownerId = window.get(owner).snapshotId();
        long ownerSequence = base.snapshot(ownerId).sequenceNumber();
        Preconditions.checkState(
            ownerSequence >= entry.dataSequenceNumber(owner),
            "A deletion vector for %s would sort below it: %s < %s",
            file.file.location(),
            ownerSequence,
            entry.dataSequenceNumber(owner));

        int ownerIndex = owner;
        DeleteFile vector =
            vectorFiles.computeIfAbsent(
                file.file.location() + "@" + ownerId,
                ignored ->
                    rewriteIO.writePositionDeletes(
                        new PositionDeleteRequest(
                            ownerId,
                            base.specsById().get(file.file.specId()),
                            file.file.partition(),
                            Map.of(file.file.location(), mask),
                            newMetadataPath("unused-" + ownerIndex, "puffin"))));
        return new Listed<>(vector, ownerId, ownerSequence, ownerSequence);
      }

      /** The positions of the physical file masked at each snapshot the entry spans. */
      private PositionSet[] masksOf(SnapshotRewriteLedger.Entry entry) {
        Physical file = physical.get(entry.file().location());
        RowRef[] keys = assigned.get(entry);
        PositionSet[] result = new PositionSet[entry.last() - entry.first() + 1];
        for (int index = entry.first(); index <= entry.last(); index += 1) {
          PositionSet mask = new PositionSet();
          for (int pos = 0; pos < keys.length; pos += 1) {
            long at = file.position(pos);
            if (keys[pos] == null) {
              // A hole still in the file -- only an original reused as it is has any -- is dead at
              // every snapshot.
              if (at >= 0) {
                mask.add(at);
              }

              continue;
            }

            Preconditions.checkState(
                at >= 0,
                "%s was rebuilt without position %s, which ledger window %s reads",
                file.file.location(),
                pos,
                window.get(entry.first()).snapshotId());
            int[] span = spans.get(keys[pos]);
            if (index < span[0] || index > span[1]) {
              mask.add(at);
            }
          }

          result[index - entry.first()] = mask;
        }

        return result;
      }
    }

    /** Entries a window snapshot references and the reconstruction keeps. */
    private List<SnapshotRewriteLedger.Entry> referencedAt(
        SnapshotRewriteLedger ledger, int index) {
      List<SnapshotRewriteLedger.Entry> referenced = Lists.newArrayList();
      for (SnapshotRewriteLedger.Entry entry : ledger.entries()) {
        // Unset for a file no snapshot being restored references, which reconstruct() passed over.
        Physical file = physical.get(entry.file().location());
        if (file != null && !file.isSkipped() && entry.first() <= index && index <= entry.last()) {
          referenced.add(entry);
        }
      }

      return referenced;
    }

    /**
     * A snapshot's manifests of one content: the files it holds, added if it is the snapshot that
     * added them and carried otherwise, then the files its parent held that it does not, removed.
     * Each at the sequence numbers it was listed with, one manifest per partition spec.
     */
    private <F extends ContentFile<F>> List<ManifestFile> writeManifests(
        long snapshotId,
        Map<String, Listed<F>> held,
        Map<String, Listed<F>> before,
        String prefix,
        String mapLocation,
        BiFunction<PartitionSpec, OutputFile, ManifestWriter<F>> newWriter) {
      Map<Integer, List<Listed<F>>> kept = Maps.newTreeMap();
      for (Listed<F> file : held.values()) {
        kept.computeIfAbsent(file.file.specId(), id -> Lists.newArrayList()).add(file);
      }

      Map<Integer, List<Listed<F>>> removed = Maps.newTreeMap();
      for (Listed<F> file : before.values()) {
        if (!held.containsKey(file.file.location())) {
          removed.computeIfAbsent(file.file.specId(), id -> Lists.newArrayList()).add(file);
        }
      }

      List<ManifestFile> manifests = Lists.newArrayList();
      for (int specId : Sets.union(kept.keySet(), removed.keySet())) {
        String path = newMetadataPath(prefix + snapshotId, "avro");
        ManifestWriter<F> writer =
            newWriter.apply(base.specsById().get(specId), io.newOutputFile(path));
        if (mapLocation != null) {
          writer.setCompactionMapLocation(mapLocation);
        }

        int entries = 0;
        try {
          for (Listed<F> file : kept.getOrDefault(specId, List.of())) {
            if (file.snapshotId == snapshotId) {
              writer.add(file.file, file.dataSequenceNumber);
            } else {
              writer.existing(
                  file.file, file.snapshotId, file.dataSequenceNumber, file.fileSequenceNumber);
            }

            entries += 1;
          }

          for (Listed<F> file : removed.getOrDefault(specId, List.of())) {
            writer.delete(file.file, file.dataSequenceNumber, file.fileSequenceNumber);
            entries += 1;
          }
        } finally {
          close(writer);
        }

        ManifestFile manifest = keepIfAny(writer, path, entries);
        if (manifest != null) {
          manifests.add(manifest);
        }
      }

      return manifests;
    }

    private ManifestFile keepIfAny(ManifestWriter<?> writer, String path, int entries) {
      if (entries == 0) {
        io.deleteFile(path);
        return null;
      }

      metadataPaths.add(path);
      return writer.toManifestFile();
    }

    private String newMetadataPath(String prefix, String extension) {
      String configured = base.property(TableProperties.WRITE_METADATA_LOCATION, null);
      String directory = configured != null ? configured : base.location() + "/metadata";
      return directory + "/" + prefix + "-" + UUID.randomUUID() + "." + extension;
    }

    private void close(ManifestWriter<?> writer) {
      try {
        writer.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
