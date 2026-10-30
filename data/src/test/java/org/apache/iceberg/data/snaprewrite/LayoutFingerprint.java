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
import org.apache.iceberg.DataFile;
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
 * snapshot added the file.
 */
class LayoutFingerprint {
  private final Map<Long, Map<String, FileLayout>> bySnapshot;
  private final Map<Long, Map<String, String>> summaries;

  private LayoutFingerprint(
      Map<Long, Map<String, FileLayout>> bySnapshot, Map<Long, Map<String, String>> summaries) {
    this.bySnapshot = bySnapshot;
    this.summaries = summaries;
  }

  static LayoutFingerprint of(Table table) {
    List<Long> ids = Lists.newArrayList();
    for (Snapshot snapshot : table.snapshots()) {
      ids.add(snapshot.snapshotId());
    }

    return of(table, ids);
  }

  static LayoutFingerprint of(Table table, Collection<Long> snapshotIds) {
    Map<Long, Map<String, FileLayout>> bySnapshot = Maps.newLinkedHashMap();
    Map<Long, Map<String, String>> summaries = Maps.newHashMap();
    Map<String, List<Row>> contents = Maps.newHashMap();
    for (long id : snapshotIds) {
      Snapshot snapshot = table.snapshot(id);
      Set<String> added = Sets.newHashSet();
      for (DataFile file : snapshot.addedDataFiles(table.io())) {
        added.add(file.location());
      }

      Map<String, Set<Long>> live = livePositions(table, id);
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

      bySnapshot.put(id, files);
      summaries.put(id, snapshot.summary());
    }

    return new LayoutFingerprint(bySnapshot, summaries);
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
   */
  Comparison assertReproducedBy(Table after) {
    LayoutFingerprint reproduced = of(after, bySnapshot.keySet());

    // Positions read anywhere in the recorded history, by file: everything else is a hole.
    Map<String, Set<Long>> everRead = Maps.newHashMap();
    for (Map<String, FileLayout> files : bySnapshot.values()) {
      for (FileLayout file : files.values()) {
        everRead.computeIfAbsent(file.path, path -> Sets.newHashSet()).addAll(file.live);
      }
    }

    Set<String> exact = Sets.newHashSet();
    Set<String> moved = Sets.newHashSet();
    for (Map.Entry<Long, Map<String, FileLayout>> snapshot : bySnapshot.entrySet()) {
      long id = snapshot.getKey();
      Map<String, FileLayout> now = reproduced.bySnapshot.get(id);
      Set<String> matched = Sets.newHashSet();
      for (FileLayout original : snapshot.getValue().values()) {
        FileLayout same = now.get(original.path);
        if (same != null) {
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
          matched.add(original.path);
          exact.add(original.path);
          continue;
        }

        Set<Long> read = everRead.get(original.path);
        List<Row> expected = Lists.newArrayList();
        for (Row row : original.rows) {
          if (read.contains(row.pos)) {
            expected.add(row.unplaced());
          }
        }

        if (expected.isEmpty()) {
          continue;
        }

        FileLayout match = null;
        for (FileLayout candidate : now.values()) {
          if (!matched.contains(candidate.path) && candidate.unplacedRows().equals(expected)) {
            match = candidate;
            break;
          }
        }

        assertThat(match)
            .as("snapshot %s holds %s without its holes", id, original.path)
            .isNotNull();
        assertThat(match.liveIds())
            .as("rows of %s read by snapshot %s", original.path, id)
            .isEqualTo(original.liveIds());
        assertThat(match.added).isEqualTo(original.added);
        assertThat(match.sequenceNumbers()).isEqualTo(original.sequenceNumbers());
        matched.add(match.path);
        moved.add(original.path);
      }

      assertThat(now.keySet()).as("files of snapshot %s", id).isEqualTo(matched);

      Map<String, String> summary = Maps.newHashMap(reproduced.summaries.get(id));
      summary.keySet().removeIf(key -> key.startsWith(SnapshotRewriteReconstruct.RESTORED_FROM));
      assertThat(summary).as("summary of snapshot %s", id).isEqualTo(summaries.get(id));
    }

    return new Comparison(exact.size(), moved.size());
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
