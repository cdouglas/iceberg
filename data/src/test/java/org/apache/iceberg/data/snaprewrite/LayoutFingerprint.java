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
package org.apache.iceberg.data.snaprewrite;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.snaprewrite.SnapshotRewriteReconstruct;
import org.apache.iceberg.types.Types;

/**
 * What every snapshot's data files physically hold, row by row, for comparing a layout before a
 * rewrite with its reconstruction (SNAPREWRITE_SPEC.md 14.5).
 *
 * <p>Read from the files themselves rather than through a scan: a scan returns the same rows from
 * any layout that holds them, which is the property a rewrite keeps and the one this is not about.
 * What it records per file is each position's {@code _row_id}, {@code
 * _last_updated_sequence_number} and values, which positions the snapshot reads, and whether the
 * snapshot added the file. Per snapshot it also records the data files it removed and the deletion
 * vectors it carried, added and removed, each with its sequence numbers.
 */
class LayoutFingerprint {
  private final Map<Long, SnapshotLayout> bySnapshot;

  private LayoutFingerprint(Map<Long, SnapshotLayout> bySnapshot) {
    this.bySnapshot = bySnapshot;
  }

  static LayoutFingerprint of(Table table) {
    List<Long> ids = Lists.newArrayList();
    for (Snapshot snapshot : table.snapshots()) {
      ids.add(snapshot.snapshotId());
    }

    return of(table, ids);
  }

  static LayoutFingerprint of(Table table, Collection<Long> snapshotIds) {
    Map<Long, SnapshotLayout> bySnapshot = Maps.newLinkedHashMap();
    Map<String, List<Row>> contents = Maps.newHashMap();
    for (long id : snapshotIds) {
      bySnapshot.put(id, SnapshotLayout.of(table, table.snapshot(id), contents));
    }

    return new LayoutFingerprint(bySnapshot);
  }

  Collection<Long> snapshotIds() {
    return bySnapshot.keySet();
  }

  /**
   * Asserts that {@code after} holds this layout in every snapshot this recorded.
   *
   * <p>A file still at its original path must match exactly: every position, id, sequence number,
   * and value, the same positions read, and the same sequence numbers on its manifest entry. A file
   * that moved is one that had holes -- positions no snapshot reads -- and must hold the same rows
   * minus the holes, in order, with the same ones read. A file with nothing but holes may be left
   * out. Nothing else may appear.
   *
   * <p>Each snapshot must also remove the same data files, at their new paths if they moved, and
   * carry, add and remove the same deletion vectors, by the data file each applies to and its
   * sequence number. A moved file's vectors are left out of that comparison, since they masked its
   * holes. Removals are compared only where the parent still exists: without it nothing says what a
   * snapshot removed.
   *
   * <p>Two things are checked of {@code after} alone. Every snapshot must be its parent less what
   * it removed plus what it added, by path, so that no entry names a file its parent did not hold.
   * And every compaction map must pair rows with the same {@code _row_id}, from a file the parent
   * held to one the compaction added.
   */
  Comparison assertReproducedBy(Table after) {
    LayoutFingerprint reproduced = of(after, bySnapshot.keySet());

    // Positions read anywhere in the recorded history, by file: everything else is a hole.
    Map<String, Set<Long>> everRead = Maps.newHashMap();
    for (SnapshotLayout snapshot : bySnapshot.values()) {
      for (FileLayout file : snapshot.files.values()) {
        everRead.computeIfAbsent(file.path, path -> Sets.newHashSet()).addAll(file.live);
      }
    }

    Set<String> exact = Sets.newHashSet();
    Map<String, String> moves = Maps.newHashMap();
    Set<String> leftOut = Sets.newHashSet();
    for (Map.Entry<Long, SnapshotLayout> snapshot : bySnapshot.entrySet()) {
      long id = snapshot.getKey();
      SnapshotLayout now = reproduced.bySnapshot.get(id);
      Set<String> matched = Sets.newHashSet();
      for (FileLayout original : snapshot.getValue().files.values()) {
        FileLayout same = now.files.get(original.path);
        if (same != null) {
          assertSameFile(id, original, same);
          matched.add(original.path);
          exact.add(original.path);
          continue;
        }

        FileLayout match = withoutHoles(id, original, everRead.get(original.path), now, matched);
        if (match == null) {
          leftOut.add(original.path);
        } else {
          matched.add(match.path);
          moves.put(original.path, match.path);
        }
      }

      assertThat(now.files.keySet()).as("files of snapshot %s", id).isEqualTo(matched);
      assertThat(withoutRestoredFrom(now.summary))
          .as("summary of snapshot %s", id)
          .isEqualTo(withoutRestoredFrom(snapshot.getValue().summary));
    }

    assertChangesReproduced(after, reproduced, moves, leftOut);
    assertForwardDeltas(after, bySnapshot.keySet());
    assertMapsHold(after, bySnapshot.keySet());
    return new Comparison(exact.size(), moves.size());
  }

  private static void assertSameFile(long id, FileLayout original, FileLayout same) {
    assertThat(same.rows).as("%s in snapshot %s", original.path, id).isEqualTo(original.rows);
    assertThat(same.live)
        .as("positions of %s read by snapshot %s", original.path, id)
        .isEqualTo(original.live);
    assertThat(same.added)
        .as("whether snapshot %s added %s", id, original.path)
        .isEqualTo(original.added);
    assertThat(same.sequenceNumbers())
        .as("sequence numbers of %s in snapshot %s", original.path, id)
        .isEqualTo(original.sequenceNumbers());
  }

  /**
   * The file a snapshot now holds in place of one that moved: the same rows minus those nothing
   * read, in order, with the same ones read. Null if nothing in the original was ever read, which
   * leaves nothing to hold.
   */
  private static FileLayout withoutHoles(
      long id, FileLayout original, Set<Long> read, SnapshotLayout now, Set<String> matched) {
    List<Row> expected = Lists.newArrayList();
    for (Row row : original.rows) {
      if (read.contains(row.pos)) {
        expected.add(row.unplaced());
      }
    }

    if (expected.isEmpty()) {
      return null;
    }

    FileLayout match = null;
    for (FileLayout candidate : now.files.values()) {
      if (!matched.contains(candidate.path) && candidate.unplacedRows().equals(expected)) {
        match = candidate;
        break;
      }
    }

    assertThat(match).as("snapshot %s holds %s without its holes", id, original.path).isNotNull();
    assertThat(match.liveIds())
        .as("rows of %s read by snapshot %s", original.path, id)
        .isEqualTo(original.liveIds());
    assertThat(match.added).isEqualTo(original.added);
    assertThat(match.sequenceNumbers()).isEqualTo(original.sequenceNumbers());
    return match;
  }

  /**
   * Each snapshot removes the same data files, at their new paths if they moved, and carries, adds
   * and removes the same vectors, leaving out those of files that moved or were left out.
   */
  private void assertChangesReproduced(
      Table after, LayoutFingerprint reproduced, Map<String, String> moves, Set<String> leftOut) {
    Set<String> vectorsBefore = Sets.union(moves.keySet(), leftOut);
    Set<String> vectorsAfter = Sets.newHashSet(moves.values());
    for (Map.Entry<Long, SnapshotLayout> snapshot : bySnapshot.entrySet()) {
      long id = snapshot.getKey();
      SnapshotLayout original = snapshot.getValue();
      SnapshotLayout now = reproduced.bySnapshot.get(id);
      assertThat(now.vectors(vectorsAfter))
          .as("deletion vectors of snapshot %s", id)
          .isEqualTo(original.vectors(vectorsBefore));

      if (original.parentId == null || after.snapshot(original.parentId) == null) {
        continue;
      }

      Map<String, List<Long>> removed = Maps.newHashMap();
      for (Map.Entry<String, List<Long>> file : original.removed.entrySet()) {
        if (!leftOut.contains(file.getKey())) {
          removed.put(moves.getOrDefault(file.getKey(), file.getKey()), file.getValue());
        }
      }

      assertThat(now.removed).as("data files snapshot %s removed", id).isEqualTo(removed);
      assertThat(now.removedVectors(vectorsAfter))
          .as("deletion vectors snapshot %s removed", id)
          .isEqualTo(original.removedVectors(vectorsBefore));
    }
  }

  /**
   * A summary without what a reconstruction adds to point back at the form it replaced, on either
   * side: the layout being reproduced may itself be a reconstruction.
   */
  private static Map<String, String> withoutRestoredFrom(Map<String, String> summary) {
    Map<String, String> kept = Maps.newHashMap(summary);
    kept.keySet().removeIf(key -> key.startsWith(SnapshotRewriteReconstruct.RESTORED_FROM));
    return kept;
  }

  /** Each snapshot is its parent, less the files it removed, plus the files it added. */
  private static void assertForwardDeltas(Table table, Collection<Long> snapshotIds) {
    for (long id : snapshotIds) {
      Snapshot snapshot = table.snapshot(id);
      Snapshot parent = snapshot.parentId() != null ? table.snapshot(snapshot.parentId()) : null;
      if (parent == null) {
        continue;
      }

      assertDelta(
          "data files",
          id,
          paths(dataFiles(table, parent)),
          paths(dataFiles(table, snapshot)),
          paths(snapshot.addedDataFiles(table.io())),
          paths(snapshot.removedDataFiles(table.io())));
      assertDelta(
          "delete files",
          id,
          paths(deleteFiles(table, parent)),
          paths(deleteFiles(table, snapshot)),
          paths(snapshot.addedDeleteFiles(table.io())),
          paths(snapshot.removedDeleteFiles(table.io())));
    }
  }

  private static void assertDelta(
      String what,
      long id,
      Set<String> before,
      Set<String> now,
      Set<String> added,
      Set<String> removed) {
    assertThat(before)
        .as("%s snapshot %s removed are ones its parent held", what, id)
        .containsAll(removed);
    assertThat(Sets.intersection(before, added))
        .as("%s snapshot %s added that its parent already held", what, id)
        .isEmpty();
    Set<String> expected = Sets.newHashSet(before);
    expected.removeAll(removed);
    expected.addAll(added);
    assertThat(now)
        .as("%s of snapshot %s: its parent's, less what it removed, plus what it added", what, id)
        .isEqualTo(expected);
  }

  /**
   * Every run of each compaction's own map pairs a row of a file its parent held with a row of a
   * file it added, and the two have the same {@code _row_id}.
   */
  private static void assertMapsHold(Table table, Collection<Long> snapshotIds) {
    Map<String, List<Row>> contents = Maps.newHashMap();
    for (long id : snapshotIds) {
      Snapshot snapshot = table.snapshot(id);
      Snapshot parent = snapshot.parentId() != null ? table.snapshot(snapshot.parentId()) : null;
      Map<String, DataFile> added = byPath(snapshot.addedDataFiles(table.io()));
      CompactionMap map = ownMap(table, snapshot, added.keySet());
      if (map == null || parent == null) {
        continue;
      }

      Map<String, DataFile> held = byPath(dataFiles(table, parent));
      for (CompactionMap.FileMapping mapping : map.fileMappings()) {
        DataFile source = held.get(mapping.sourceFile());
        assertThat(source)
            .as(
                "source %s of snapshot %s's map is a file its parent held",
                mapping.sourceFile(), id)
            .isNotNull();
        List<Row> sourceRows =
            contents.computeIfAbsent(source.location(), path -> rowsOf(table, source));
        for (CompactionMap.Run run : mapping.runs()) {
          String targetPath = run.targetFile() != null ? run.targetFile() : mapping.targetFile();
          DataFile target = added.get(targetPath);
          assertThat(target)
              .as("target %s of snapshot %s's map is a file it added", targetPath, id)
              .isNotNull();
          List<Row> targetRows =
              contents.computeIfAbsent(target.location(), path -> rowsOf(table, target));
          assertRunHolds(id, run, source, sourceRows, target, targetRows);
        }
      }
    }
  }

  private static void assertRunHolds(
      long id,
      CompactionMap.Run run,
      DataFile source,
      List<Row> sourceRows,
      DataFile target,
      List<Row> targetRows) {
    for (int i = 0; i < run.length(); i += 1) {
      int from = (int) run.sourcePosition() + i;
      int to = (int) run.targetPosition() + i;
      assertThat(targetRows.get(to).id)
          .as("snapshot %s maps %s:%s to %s:%s", id, source.location(), from, target.location(), to)
          .isEqualTo(sourceRows.get(from).id);
    }
  }

  /** The map a snapshot wrote: the one whose targets are files the snapshot added. */
  private static CompactionMap ownMap(Table table, Snapshot snapshot, Set<String> added) {
    for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
      String location = manifest.compactionMapLocation();
      if (location == null) {
        continue;
      }

      CompactionMap map = CompactionMaps.read(table.io().newInputFile(location));
      for (CompactionMap.FileMapping mapping : map.fileMappings()) {
        if (added.contains(mapping.targetFile())) {
          return map;
        }
      }
    }

    return null;
  }

  /** How many original files came back at their own path, and how many moved. */
  static class Comparison {
    private final int exact;
    private final int moved;

    Comparison(int exact, int moved) {
      this.exact = exact;
      this.moved = moved;
    }

    int exact() {
      return exact;
    }

    int moved() {
      return moved;
    }
  }

  /** One snapshot: its data files, what it removed, and its deletion vectors. */
  private static class SnapshotLayout {
    private final Long parentId;
    private final Map<String, FileLayout> files;
    private final Map<String, List<Long>> removed;
    private final List<DeleteFile> deletes;
    private final Set<String> addedDeletes;
    private final List<DeleteFile> removedDeletes;
    private final Map<String, String> summary;

    private SnapshotLayout(
        Long parentId,
        Map<String, FileLayout> files,
        Map<String, List<Long>> removed,
        List<DeleteFile> deletes,
        Set<String> addedDeletes,
        List<DeleteFile> removedDeletes,
        Map<String, String> summary) {
      this.parentId = parentId;
      this.files = files;
      this.removed = removed;
      this.deletes = deletes;
      this.addedDeletes = addedDeletes;
      this.removedDeletes = removedDeletes;
      this.summary = summary;
    }

    static SnapshotLayout of(Table table, Snapshot snapshot, Map<String, List<Row>> contents) {
      Set<String> added = paths(snapshot.addedDataFiles(table.io()));
      Map<String, Set<Long>> live = livePositions(table, snapshot.snapshotId());
      Map<String, FileLayout> files = Maps.newHashMap();
      for (DataFile file : dataFiles(table, snapshot)) {
        List<Row> rows = contents.computeIfAbsent(file.location(), path -> rowsOf(table, file));
        files.put(
            file.location(),
            new FileLayout(
                file.location(),
                added.contains(file.location()),
                file.dataSequenceNumber(),
                file.fileSequenceNumber(),
                rows,
                live.getOrDefault(file.location(), Set.of())));
      }

      Map<String, List<Long>> removed = Maps.newHashMap();
      for (DataFile file : snapshot.removedDataFiles(table.io())) {
        removed.put(
            file.location(),
            Lists.newArrayList(file.dataSequenceNumber(), file.fileSequenceNumber()));
      }

      return new SnapshotLayout(
          snapshot.parentId(),
          files,
          removed,
          deleteFiles(table, snapshot),
          paths(snapshot.addedDeleteFiles(table.io())),
          Lists.newArrayList(snapshot.removedDeleteFiles(table.io())),
          snapshot.summary());
    }

    /**
     * The vectors carried, by the data file each applies to: its sequence number, and whether this
     * snapshot added it. Those that apply to {@code excluded} files are left out.
     */
    Map<String, String> vectors(Set<String> excluded) {
      Map<String, String> vectors = Maps.newHashMap();
      for (DeleteFile file : deletes) {
        String target = appliesTo(file);
        if (!excluded.contains(target)) {
          String added = addedDeletes.contains(file.location()) ? " added" : "";
          vectors.put(target, "seq=" + file.dataSequenceNumber() + added);
        }
      }

      return vectors;
    }

    /** The vectors removed, by the data file each applied to, with its sequence number. */
    Map<String, Long> removedVectors(Set<String> excluded) {
      Map<String, Long> vectors = Maps.newHashMap();
      for (DeleteFile file : removedDeletes) {
        String target = appliesTo(file);
        if (!excluded.contains(target)) {
          vectors.put(target, file.dataSequenceNumber());
        }
      }

      return vectors;
    }

    private static String appliesTo(DeleteFile file) {
      return file.referencedDataFile() != null ? file.referencedDataFile() : file.location();
    }
  }

  private static class FileLayout {
    private final String path;
    private final boolean added;
    private final Long dataSequenceNumber;
    private final Long fileSequenceNumber;
    private final List<Row> rows;
    private final Set<Long> live;

    FileLayout(
        String path,
        boolean added,
        Long dataSequenceNumber,
        Long fileSequenceNumber,
        List<Row> rows,
        Set<Long> live) {
      this.path = path;
      this.added = added;
      this.dataSequenceNumber = dataSequenceNumber;
      this.fileSequenceNumber = fileSequenceNumber;
      this.rows = rows;
      this.live = live;
    }

    List<Long> sequenceNumbers() {
      return Lists.newArrayList(dataSequenceNumber, fileSequenceNumber);
    }

    List<Row> unplacedRows() {
      List<Row> unplaced = Lists.newArrayList();
      for (Row row : rows) {
        unplaced.add(row.unplaced());
      }

      return unplaced;
    }

    Set<Long> liveIds() {
      Set<Long> ids = Sets.newHashSet();
      for (Row row : rows) {
        if (live.contains(row.pos)) {
          ids.add(row.id);
        }
      }

      return ids;
    }
  }

  /** One position of a data file. A position of -1 means "wherever it is". */
  private static class Row {
    private final long pos;
    private final Long id;
    private final Long seq;
    private final String values;

    Row(long pos, Long id, Long seq, String values) {
      this.pos = pos;
      this.id = id;
      this.seq = seq;
      this.values = values;
    }

    Row unplaced() {
      return new Row(-1, id, seq, values);
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Row)) {
        return false;
      }

      Row that = (Row) other;
      return pos == that.pos
          && Objects.equals(id, that.id)
          && Objects.equals(seq, that.seq)
          && values.equals(that.values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(pos, id, seq, values);
    }

    @Override
    public String toString() {
      return pos + ": id=" + id + " seq=" + seq + " " + values;
    }
  }

  private static List<Row> rowsOf(Table table, DataFile file) {
    List<Row> rows = Lists.newArrayList();
    long pos = 0;
    for (Record record : RawFiles.readAllWithLineage(table.io(), file, table.schema())) {
      StringBuilder values = new StringBuilder();
      for (Types.NestedField field : table.schema().columns()) {
        values.append(field.name()).append('=').append(record.getField(field.name())).append(' ');
      }

      rows.add(
          new Row(
              pos,
              (Long) record.getField(MetadataColumns.ROW_ID.name()),
              (Long) record.getField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()),
              values.toString()));
      pos += 1;
    }

    return rows;
  }

  static List<DataFile> dataFiles(Table table, Snapshot snapshot) {
    List<DataFile> files = Lists.newArrayList();
    for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, table.io(), table.specs())) {
        for (DataFile file : reader) {
          files.add(file.copy());
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    return files;
  }

  private static List<DeleteFile> deleteFiles(Table table, Snapshot snapshot) {
    List<DeleteFile> files = Lists.newArrayList();
    for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile file : reader) {
          files.add(file.copy());
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    return files;
  }

  private static Set<String> paths(Iterable<? extends ContentFile<?>> files) {
    Set<String> paths = Sets.newHashSet();
    for (ContentFile<?> file : files) {
      paths.add(file.location());
    }

    return paths;
  }

  private static <F extends ContentFile<F>> Map<String, F> byPath(Iterable<F> files) {
    Map<String, F> byPath = Maps.newHashMap();
    for (F file : files) {
      byPath.put(file.location(), file);
    }

    return byPath;
  }

  /** Positions a snapshot reads, by file: what a scan returns, not what the manifests say. */
  static Map<String, Set<Long>> livePositions(Table table, long snapshotId) {
    List<Types.NestedField> columns = Lists.newArrayList(table.schema().columns());
    columns.add(MetadataColumns.FILE_PATH);
    columns.add(MetadataColumns.ROW_POSITION);
    Map<String, Set<Long>> positions = Maps.newHashMap();
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(table).useSnapshot(snapshotId).project(new Schema(columns)).build()) {
      for (Record record : records) {
        positions
            .computeIfAbsent(
                record.getField(MetadataColumns.FILE_PATH.name()).toString(),
                path -> Sets.newHashSet())
            .add((Long) record.getField(MetadataColumns.ROW_POSITION.name()));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return positions;
  }
}
