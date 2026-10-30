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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;

/**
 * What a rewrite detached, recorded so that the original layout can be rebuilt after reclaim.
 *
 * <p>Undo by pointing back at the original manifest lists stops working at reclaim, because reclaim
 * deletes what they point at. What survives is every readable row, with its {@code _row_id}, in the
 * compaction's files and the recovery files. Row ids say which physical rows are the same row, but
 * not where one original file ended and the next began, nor the order of a file whose ids were
 * written per row. The ledger records exactly that, per original data file: the file's own
 * metadata, when it was referenced, and its row ids in position order (SNAPREWRITE_SPEC.md 14.3).
 *
 * <p>Keyed by {@code _row_id} rather than by where each row went. A later rewrite moves the rows
 * again and leaves their ids alone, so a ledger never goes stale, and a reconstruction checks
 * itself: every id it expects must be found exactly once.
 *
 * <p>Written as JSON next to the table metadata. Every rewritten snapshot names it in its summary.
 */
public class SnapshotRewriteLedger {
  /** The summary key on a rewritten snapshot that names the ledger able to reconstruct it. */
  public static final String SUMMARY_KEY = "snapshot-rewrite-ledger";

  private static final int FORMAT_VERSION = 1;
  private static final String FORMAT_VERSION_FIELD = "format-version";
  private static final String COMPACTION = "compaction-snapshot-id";
  private static final String SCHEMA_ID = "schema-id";
  private static final String SPEC_ID = "spec-id";
  private static final String WINDOW = "window";
  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String COMPACTION_MAP = "compaction-map";
  private static final String FILES = "files";
  private static final String DATA_FILE = "data-file";
  private static final String SEQUENCE_NUMBERS = "sequence-numbers";
  private static final String ADDED_SNAPSHOT_ID = "added-snapshot-id";
  private static final String FIRST = "first";
  private static final String LAST = "last";
  private static final String ROW_IDS = "row-ids";
  private static final String BIRTHS = "births";

  private final long compactionSnapshotId;
  private final int schemaId;
  private final int specId;
  private final List<WindowSnapshot> window;
  private final List<Entry> entries;

  SnapshotRewriteLedger(
      long compactionSnapshotId,
      int schemaId,
      int specId,
      List<WindowSnapshot> window,
      List<Entry> entries) {
    this.compactionSnapshotId = compactionSnapshotId;
    this.schemaId = schemaId;
    this.specId = specId;
    this.window = ImmutableList.copyOf(window);
    this.entries = ImmutableList.copyOf(entries);
  }

  /** The compaction the window was rewritten onto. */
  public long compactionSnapshotId() {
    return compactionSnapshotId;
  }

  public int schemaId() {
    return schemaId;
  }

  public int specId() {
    return specId;
  }

  /** The rewritten snapshots, oldest first. Entries refer to them by index. */
  public List<WindowSnapshot> window() {
    return window;
  }

  /** Every data file a window snapshot referenced before the rewrite. */
  public List<Entry> entries() {
    return entries;
  }

  /** Whether every entry has its row ids, which needs a read for files that write them per row. */
  public boolean isComplete() {
    return entries.stream().noneMatch(Entry::isPending);
  }

  /**
   * Fills in the row ids of every entry that needs a read to know them.
   *
   * @param readRuns reads a data file's row ids in position order, as runs
   */
  SnapshotRewriteLedger withRowIds(Function<DataFile, long[]> readRuns) {
    List<Entry> complete = Lists.newArrayList();
    for (Entry entry : entries) {
      complete.add(entry.isPending() ? entry.withRowIdRuns(readRuns.apply(entry.file())) : entry);
    }

    return new SnapshotRewriteLedger(compactionSnapshotId, schemaId, specId, window, complete);
  }

  /** Data files whose row ids have to be read, because the file may write them per row. */
  List<DataFile> pendingFiles() {
    List<DataFile> pending = Lists.newArrayList();
    for (Entry entry : entries) {
      if (entry.isPending()) {
        pending.add(entry.file());
      }
    }

    return pending;
  }

  /** One rewritten snapshot, and the compaction map it carried if it was a compaction. */
  public static class WindowSnapshot {
    private final long snapshotId;
    private final String compactionMapLocation;

    WindowSnapshot(long snapshotId, String compactionMapLocation) {
      this.snapshotId = snapshotId;
      this.compactionMapLocation = compactionMapLocation;
    }

    public long snapshotId() {
      return snapshotId;
    }

    /**
     * The compaction map this snapshot's manifests named, or null if it was not a compaction.
     *
     * <p>A reconstruction attaches it again. Compaction maps are not reclaimed, and a map stays
     * accurate exactly as long as the files it names come back at their original paths.
     */
    public String compactionMapLocation() {
      return compactionMapLocation;
    }
  }

  /** One original data file. */
  public static class Entry {
    private final DataFile file;
    private final long[] sequenceRuns;
    private final long addedSnapshotId;
    private final int first;
    private final int last;
    private final long[] rowIdRuns;
    private final long[] birthRuns;

    Entry(
        DataFile file,
        long[] sequenceRuns,
        long addedSnapshotId,
        int first,
        int last,
        long[] rowIdRuns,
        long[] birthRuns) {
      Preconditions.checkArgument(
          first <= last, "Entry for %s is referenced from %s to %s", file.location(), first, last);
      Preconditions.checkArgument(
          rowIdRuns == null || runLength(rowIdRuns) == file.recordCount(),
          "Row ids for %s cover %s rows, but the file has %s",
          file.location(),
          rowIdRuns == null ? 0 : runLength(rowIdRuns),
          file.recordCount());
      this.file = file;
      this.sequenceRuns = sequenceRuns;
      this.addedSnapshotId = addedSnapshotId;
      this.first = first;
      this.last = last;
      this.rowIdRuns = rowIdRuns;
      this.birthRuns = birthRuns;
    }

    /** The file as its manifest described it, including its partition and {@code first_row_id}. */
    public DataFile file() {
      return file;
    }

    /** The data sequence number the file carried in the window snapshot at {@code index}. */
    public long dataSequenceNumber(int index) {
      return sequenceRun(index)[2];
    }

    /** The file sequence number the file carried in the window snapshot at {@code index}. */
    public long fileSequenceNumber(int index) {
      return sequenceRun(index)[3];
    }

    /**
     * Whether the file carried one pair of sequence numbers throughout.
     *
     * <p>Always, in an original history. Not necessarily in a rewritten one: a compaction's file
     * carries the window's shared base number in every rewritten snapshot and its own after them
     * (errata 2), so a ledger written over a rewritten layout records both.
     */
    public boolean hasOneSequence() {
      return sequenceRuns.length == 4;
    }

    private long[] sequenceRun(int index) {
      for (int i = 0; i < sequenceRuns.length; i += 4) {
        if (sequenceRuns[i] <= index && index <= sequenceRuns[i + 1]) {
          return new long[] {
            sequenceRuns[i], sequenceRuns[i + 1], sequenceRuns[i + 2], sequenceRuns[i + 3]
          };
        }
      }

      throw new IllegalArgumentException(
          String.format("%s is not referenced at window index %s", file.location(), index));
    }

    /** The commit that added this file, which may predate the window. */
    public long addedSnapshotId() {
      return addedSnapshotId;
    }

    /** Index into the window of the oldest snapshot referencing this file. */
    public int first() {
      return first;
    }

    /** Index into the window of the newest snapshot referencing this file. */
    public int last() {
      return last;
    }

    boolean isPending() {
      return rowIdRuns == null;
    }

    /** The {@code _row_id} at every position of the file, in order. */
    public long[] rowIds() {
      Preconditions.checkState(rowIdRuns != null, "Row ids of %s were never read", file.location());
      return expand(rowIdRuns, file.recordCount());
    }

    /**
     * For every position, the window index of the first snapshot that reads it, or -1 if none does.
     *
     * <p>A reconstruction finds each row where it is first read: row ids are unique among the rows
     * one snapshot reads, so the row with this position's id read at its birth is this position's
     * row, even if another version of it is read elsewhere later. Births vary within a file only
     * when the layout recorded is itself a rewritten one, whose early snapshots mask rows that
     * later ones read; in an original history every readable row is born where its file begins.
     */
    public int[] births() {
      int[] births = new int[(int) file.recordCount()];
      for (int i = 0; i < birthRuns.length; i += 3) {
        for (long pos = birthRuns[i]; pos < birthRuns[i] + birthRuns[i + 1]; pos += 1) {
          births[(int) pos] = (int) birthRuns[i + 2];
        }
      }

      return births;
    }

    /**
     * Positions no window snapshot can read: inserted and deleted by one commit, or already dead at
     * the oldest snapshot that references the file.
     *
     * <p>The rewritten layout never held these rows, so nothing can rebuild them from it.
     */
    public PositionSet holes() {
      PositionSet holes = new PositionSet();
      int[] births = births();
      for (int pos = 0; pos < births.length; pos += 1) {
        if (births[pos] < 0) {
          holes.add(pos);
        }
      }

      return holes;
    }

    /** How many runs the row ids take: one for a file whose ids derive from its range. */
    public int rowIdRunCount() {
      return rowIdRuns == null ? 0 : rowIdRuns.length / 2;
    }

    Entry withRowIdRuns(long[] runs) {
      return new Entry(file, sequenceRuns, addedSnapshotId, first, last, runs, birthRuns);
    }
  }

  // ------------------------------------------------------------------ runs

  /** Compresses consecutive values into (start, length) pairs. */
  public static long[] runsOf(long[] values) {
    List<Long> runs = Lists.newArrayList();
    int index = 0;
    while (index < values.length) {
      long start = values[index];
      int end = index + 1;
      while (end < values.length && values[end] == values[end - 1] + 1) {
        end += 1;
      }

      runs.add(start);
      runs.add((long) (end - index));
      index = end;
    }

    return runs.stream().mapToLong(Long::longValue).toArray();
  }

  /** Compresses per-position births into (first position, length, birth) triples. */
  static long[] birthRunsOf(int[] births) {
    List<Long> runs = Lists.newArrayList();
    int index = 0;
    while (index < births.length) {
      int end = index + 1;
      while (end < births.length && births[end] == births[index]) {
        end += 1;
      }

      runs.add((long) index);
      runs.add((long) (end - index));
      runs.add((long) births[index]);
      index = end;
    }

    return runs.stream().mapToLong(Long::longValue).toArray();
  }

  /** The single run a file whose ids derive from its {@code first_row_id} occupies. */
  static long[] derivedRuns(DataFile file) {
    return file.recordCount() == 0
        ? new long[0]
        : new long[] {file.firstRowId(), file.recordCount()};
  }

  private static long runLength(long[] runs) {
    long length = 0;
    for (int i = 1; i < runs.length; i += 2) {
      length += runs[i];
    }

    return length;
  }

  static long[] expand(long[] runs, long count) {
    Preconditions.checkArgument(count <= Integer.MAX_VALUE, "Too many rows to expand: %s", count);
    long[] values = new long[(int) count];
    int index = 0;
    for (int i = 0; i < runs.length; i += 2) {
      for (long offset = 0; offset < runs[i + 1]; offset += 1) {
        values[index] = runs[i] + offset;
        index += 1;
      }
    }

    return values;
  }

  // ------------------------------------------------------------------ JSON

  void write(OutputFile out, Map<Integer, PartitionSpec> specsById) {
    Preconditions.checkState(isComplete(), "Cannot write a ledger with unread row ids");
    String json = JsonUtil.generate(generator -> toJson(generator, specsById), false);
    try (OutputStream stream = out.createOrOverwrite()) {
      stream.write(json.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static SnapshotRewriteLedger read(InputFile in, Map<Integer, PartitionSpec> specsById) {
    try (InputStream stream = in.newStream()) {
      return fromJson(JsonUtil.mapper().readTree(stream), specsById);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void toJson(JsonGenerator generator, Map<Integer, PartitionSpec> specsById)
      throws IOException {
    generator.writeStartObject();
    generator.writeNumberField(FORMAT_VERSION_FIELD, FORMAT_VERSION);
    generator.writeNumberField(COMPACTION, compactionSnapshotId);
    generator.writeNumberField(SCHEMA_ID, schemaId);
    generator.writeNumberField(SPEC_ID, specId);

    generator.writeArrayFieldStart(WINDOW);
    for (WindowSnapshot snapshot : window) {
      generator.writeStartObject();
      generator.writeNumberField(SNAPSHOT_ID, snapshot.snapshotId());
      JsonUtil.writeStringFieldIfPresent(
          COMPACTION_MAP, snapshot.compactionMapLocation(), generator);
      generator.writeEndObject();
    }
    generator.writeEndArray();

    generator.writeArrayFieldStart(FILES);
    for (Entry entry : entries) {
      generator.writeStartObject();
      generator.writeFieldName(DATA_FILE);
      ContentFileParser.toJson(entry.file(), specsById.get(entry.file().specId()), generator);
      writeLongs(SEQUENCE_NUMBERS, entry.sequenceRuns, generator);
      generator.writeNumberField(ADDED_SNAPSHOT_ID, entry.addedSnapshotId());
      generator.writeNumberField(FIRST, entry.first());
      generator.writeNumberField(LAST, entry.last());
      writeLongs(ROW_IDS, entry.rowIdRuns, generator);
      writeLongs(BIRTHS, entry.birthRuns, generator);
      generator.writeEndObject();
    }
    generator.writeEndArray();

    generator.writeEndObject();
  }

  private static SnapshotRewriteLedger fromJson(
      JsonNode node, Map<Integer, PartitionSpec> specsById) {
    int version = JsonUtil.getInt(FORMAT_VERSION_FIELD, node);
    Preconditions.checkArgument(
        version == FORMAT_VERSION, "Unsupported ledger format version: %s", version);

    List<WindowSnapshot> window = Lists.newArrayList();
    for (JsonNode snapshot : node.get(WINDOW)) {
      window.add(
          new WindowSnapshot(
              JsonUtil.getLong(SNAPSHOT_ID, snapshot),
              JsonUtil.getStringOrNull(COMPACTION_MAP, snapshot)));
    }

    List<Entry> entries = Lists.newArrayList();
    for (JsonNode entry : node.get(FILES)) {
      entries.add(
          new Entry(
              (DataFile) ContentFileParser.fromJson(entry.get(DATA_FILE), specsById),
              readLongs(SEQUENCE_NUMBERS, entry),
              JsonUtil.getLong(ADDED_SNAPSHOT_ID, entry),
              JsonUtil.getInt(FIRST, entry),
              JsonUtil.getInt(LAST, entry),
              readLongs(ROW_IDS, entry),
              readLongs(BIRTHS, entry)));
    }

    return new SnapshotRewriteLedger(
        JsonUtil.getLong(COMPACTION, node),
        JsonUtil.getInt(SCHEMA_ID, node),
        JsonUtil.getInt(SPEC_ID, node),
        window,
        entries);
  }

  private static void writeLongs(String field, long[] values, JsonGenerator generator)
      throws IOException {
    generator.writeArrayFieldStart(field);
    for (long value : values) {
      generator.writeNumber(value);
    }
    generator.writeEndArray();
  }

  private static long[] readLongs(String field, JsonNode node) {
    JsonNode array = node.get(field);
    long[] values = new long[array.size()];
    for (int i = 0; i < values.length; i += 1) {
      values[i] = array.get(i).asLong();
    }

    return values;
  }
}
