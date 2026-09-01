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
package org.apache.iceberg.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.snaprewrite.PositionSet;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.orc.OrcRowReader;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.snaprewrite.PositionDeleteRequest;
import org.apache.iceberg.snaprewrite.ResurrectionRequest;
import org.apache.iceberg.snaprewrite.RowRef;
import org.apache.iceberg.snaprewrite.SnapshotRewriteIO;
import org.apache.orc.TypeDescription;
import org.apache.parquet.schema.MessageType;

/**
 * Reads and writes the data files a snapshot rewrite needs, using generic records.
 *
 * <p>{@code iceberg-core} has no record reader, so the rewrite delegates the three operations that
 * touch data: loading position deletes to compute live sets, copying dead rows forward into
 * resurrection files, and writing the position deletes of the rewritten snapshots.
 *
 * <p>Rows are read straight out of their source files with no delete filtering. That is the point:
 * every row a resurrection file recovers is dead in the current snapshot, and a filtered read would
 * return nothing.
 */
public class GenericSnapshotRewriteIO implements SnapshotRewriteIO {
  private final Table table;
  private final FileIO io;
  private final DeleteLoader deleteLoader;

  public GenericSnapshotRewriteIO(Table table) {
    this.table = table;
    this.io = table.io();
    this.deleteLoader = new BaseDeleteLoader(file -> io.newInputFile(file.location()));
  }

  @Override
  public boolean preservesRowLineage() {
    return true;
  }

  @Override
  public PositionDeleteIndex loadPositionDeletes(
      Iterable<DeleteFile> deleteFiles, CharSequence dataFilePath) {
    return deleteLoader.loadPositionDeletes(deleteFiles, dataFilePath);
  }

  @Override
  public DataFile resurrect(ResurrectionRequest request) {
    boolean lineage = GenericRowLineage.tracked(table);
    Schema readSchema =
        lineage ? GenericRowLineage.writeSchema(request.schema()) : request.schema();
    Map<String, Map<Long, Record>> loaded = readSources(request, readSchema);

    OutputFile output = io.newOutputFile(request.outputPath());
    FileFormat format = FileFormat.fromFileName(request.outputPath());
    GenericAppenderFactory factory =
        new GenericAppenderFactory(readSchema, request.spec()).setAll(table.properties());

    long recordCount = 0;
    FileAppender<Record> appender = factory.newAppender(output, format);
    try {
      for (RowRef source : request.sources()) {
        Record record = loaded.get(source.path()).get(source.position());
        if (record == null) {
          throw new IllegalStateException(
              String.format("Row %s is missing from its source file", source));
        }

        if (lineage) {
          // The recovered row keeps the identity it had. Deriving one from this file's range would
          // give it a new identity in a snapshot where it is supposed to be the same row -- the one
          // thing about a row that is meant to survive a change of layout.
          record =
              GenericRowLineage.withRowId(readSchema, record, rowIdOf(record, source, request));
        }

        appender.add(record);
        recordCount += 1;
      }
    } finally {
      close(appender);
    }

    DataFiles.Builder builder =
        DataFiles.builder(request.spec())
            .withPath(request.outputPath())
            .withFormat(format)
            .withFileSizeInBytes(appender.length())
            .withMetrics(appender.metrics())
            .withRecordCount(recordCount);
    if (request.spec().isPartitioned()) {
      builder.withPartition(request.partition());
    }

    if (lineage) {
      // A materialized id is only read when the file also carries a first_row_id, so one is needed
      // even though nothing derives from it. Reusing a source range keeps this file from laying
      // claim to id space the table has not handed out.
      builder.withFirstRowId(anySourceRange(request));
    }

    return builder.build();
  }

  /** The identity of a recovered row: written into its source file, or derived from that file's range. */
  private Long rowIdOf(Record record, RowRef source, ResurrectionRequest request) {
    Object materialized = record.getField(MetadataColumns.ROW_ID.name());
    if (materialized != null) {
      return (Long) materialized;
    }

    Long first = request.sourceFirstRowIds().get(source.path());
    return first == null ? null : first + source.position();
  }

  private long anySourceRange(ResurrectionRequest request) {
    long lowest = Long.MAX_VALUE;
    for (Long first : request.sourceFirstRowIds().values()) {
      lowest = Math.min(lowest, first);
    }

    return lowest == Long.MAX_VALUE ? 0L : lowest;
  }

  @Override
  public DeleteFile writePositionDeletes(PositionDeleteRequest request) {
    if (TableUtil.formatVersion(table) >= 3) {
      return writeDeletionVector(request);
    }

    FileFormat format =
        FileFormat.fromString(
            table
                .properties()
                .getOrDefault(
                    TableProperties.DELETE_DEFAULT_FILE_FORMAT,
                    table
                        .properties()
                        .getOrDefault(
                            TableProperties.DEFAULT_FILE_FORMAT,
                            TableProperties.DEFAULT_FILE_FORMAT_DEFAULT)));

    GenericAppenderFactory factory =
        new GenericAppenderFactory(table.schema(), request.spec()).setAll(table.properties());
    PositionDeleteWriter<Record> writer =
        factory.newPosDeleteWriter(
            EncryptedFiles.plainAsEncryptedOutput(io.newOutputFile(request.outputPath())),
            format,
            request.partition());

    List<String> paths = Lists.newArrayList(request.deletes().keySet());
    paths.sort(String::compareTo);

    PositionDelete<Record> delete = PositionDelete.create();
    try {
      for (String path : paths) {
        PositionSet positions = request.deletes().get(path);
        positions.forEach(position -> writer.write(delete.set(path, position, null)));
      }
    } finally {
      close(writer);
    }

    return writer.toDeleteFile();
  }

  /**
   * Writes one deletion vector.
   *
   * <p>A deletion vector references exactly one data file, so the planner emits one request per file
   * rather than per partition, and this asserts that shape rather than silently writing the first
   * entry. The Puffin writer allocates the output location itself, so the request's path is an
   * identifier and the returned file's location is the real one.
   */
  private DeleteFile writeDeletionVector(PositionDeleteRequest request) {
    Preconditions.checkArgument(
        request.deletes().size() == 1,
        "A deletion vector references one data file, got %s",
        request.deletes().size());

    Map.Entry<String, PositionSet> entry =
        request.deletes().entrySet().iterator().next();
    OutputFileFactory files =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();

    DVFileWriter writer = new BaseDVFileWriter(files, path -> null);
    try (DVFileWriter closeable = writer) {
      entry
          .getValue()
          .forEach(
              position ->
                  closeable.delete(entry.getKey(), position, request.spec(), request.partition()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return Iterables.getOnlyElement(writer.result().deleteFiles());
  }

  /**
   * Loads every row a request needs, one pass per source file.
   *
   * <p>Positions are file offsets, so the reader counts rows as it goes. Container reuse is off:
   * these records outlive the iteration.
   */
  private Map<String, Map<Long, Record>> readSources(
      ResurrectionRequest request, Schema readSchema) {
    Map<String, PositionSet> wanted = Maps.newHashMap();
    for (RowRef source : request.sources()) {
      wanted.computeIfAbsent(source.path(), ignored -> new PositionSet()).add(source.position());
    }

    Map<String, Map<Long, Record>> loaded = Maps.newHashMap();
    for (Map.Entry<String, PositionSet> entry : wanted.entrySet()) {
      Map<Long, Record> rows = Maps.newHashMap();
      loaded.put(entry.getKey(), rows);

      try (CloseableIterable<Record> reader =
          open(entry.getKey(), readSchema, request.sourceFirstRowIds().get(entry.getKey()))) {
        long position = 0;
        for (Record record : reader) {
          if (entry.getValue().contains(position)) {
            rows.put(position, record);
          }

          position += 1;
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    return loaded;
  }

  /**
   * Opens a data file for a positional read.
   *
   * <p>{@code firstRowId} has to be passed through as a constant for {@code _row_id} to be readable
   * at all: without it the reader returns nulls and discards the column even when the file has one.
   */
  private CloseableIterable<Record> open(String path, Schema projection, Long firstRowId) {
    Map<Integer, Object> constants =
        firstRowId == null
            ? ImmutableMap.of()
            : ImmutableMap.of(MetadataColumns.ROW_ID.fieldId(), firstRowId);
    InputFile input = io.newInputFile(path);
    FileFormat format = FileFormat.fromFileName(path);
    if (format == null) {
      throw new UnsupportedOperationException("Cannot determine file format of " + path);
    }

    switch (format) {
      case AVRO:
        return Avro.read(input)
            .project(projection)
            .createResolvingReader(PlannedDataReader::create)
            .build();

      case PARQUET:
        return Parquet.read(input)
            .project(projection)
            .createReaderFunc(parquetReader(projection, constants))
            .build();

      case ORC:
        return ORC.read(input).project(projection).createReaderFunc(orcReader(projection)).build();

      default:
        throw new UnsupportedOperationException(
            String.format("Cannot read %s, unsupported format: %s", path, format));
    }
  }

  private Function<MessageType, ParquetValueReader<?>> parquetReader(
      Schema projection, Map<Integer, Object> constants) {
    return fileSchema -> GenericParquetReaders.buildReader(projection, fileSchema, constants);
  }

  private Function<TypeDescription, OrcRowReader<?>> orcReader(Schema projection) {
    return fileSchema -> GenericOrcReader.buildReader(projection, fileSchema);
  }

  private void close(java.io.Closeable closeable) {
    try {
      closeable.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
