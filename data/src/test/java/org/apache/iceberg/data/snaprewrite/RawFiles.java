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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRowLineage;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Reads a data file by position, ignoring any deletes that apply to it.
 *
 * <p>Both the local compactor and the rewrite need rows at file offsets rather than rows a scan
 * would return: the compactor needs to know which offset a surviving row came from, and a rewrite
 * needs rows that are dead in the current snapshot.
 *
 * <p>Reading {@code _row_id} needs the file's {@code first_row_id} passed in as a constant. Without
 * it {@code ParquetValueReaders.rowIds} returns nulls and discards the column even when the file has
 * one, so a raw read would report no identities at all. With it, the reader behaves as it does in a
 * scan: a materialized value wins, and otherwise the id is derived from the constant and the offset.
 */
class RawFiles {
  private RawFiles() {}

  static List<Record> readAll(FileIO io, String path, Schema schema) {
    return read(io, path, schema);
  }

  /** Reads with the lineage columns projected, so each row's {@code _row_id} comes through. */
  static List<Record> readAllWithLineage(FileIO io, DataFile file, Schema tableSchema) {
    Schema schema = GenericRowLineage.writeSchema(tableSchema);
    Map<Integer, Object> constants =
        file.firstRowId() == null
            ? ImmutableMap.of()
            : ImmutableMap.of(MetadataColumns.ROW_ID.fieldId(), file.firstRowId());
    return read(io, file.location(), schema, constants);
  }

  private static List<Record> read(FileIO io, String path, Schema schema) {
    return read(io, path, schema, ImmutableMap.of());
  }

  private static List<Record> read(
      FileIO io, String path, Schema schema, Map<Integer, Object> constants) {
    List<Record> rows = Lists.newArrayList();
    try (CloseableIterable<Record> reader =
        Parquet.read(io.newInputFile(path))
            .project(schema)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema, constants))
            .build()) {
      for (Record record : reader) {
        rows.add(record);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to read " + path, e);
    }

    return rows;
  }
}
