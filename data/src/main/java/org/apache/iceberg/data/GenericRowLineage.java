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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.types.Types;

/**
 * Writing data files whose rows carry their own {@code _row_id}.
 *
 * <p>Under v3 a row's id is normally derived from its file's {@code first_row_id} plus its offset,
 * which only works while a file's rows are one contiguous range. Any operation that gathers rows from
 * several files -- a compaction, or recovering rows into a resurrection file -- produces a file whose
 * rows came from unrelated ranges, and the only way to keep their identities is to write the ids out
 * per row.
 *
 * <p>The read side already prefers a materialized value over the derived one ({@code
 * ParquetValueReaders.RowIdReader}), but only when the file also carries a {@code first_row_id}: with
 * none assigned the reader returns null rather than reading the column. So a file written this way
 * still needs {@code withFirstRowId}, even though nothing derives from it.
 *
 * <p>This is not specific to snapshot rewriting. Without it no generic-writer operation can preserve
 * row lineage, which is why Iceberg's Spark rewrite carries its own equivalent.
 */
public class GenericRowLineage {

  private GenericRowLineage() {}

  /** Whether this table tracks row lineage at all. */
  public static boolean tracked(Table table) {
    return TableUtil.formatVersion(table) >= 3;
  }

  /** The schema to write when rows carry explicit ids: the table's columns plus the lineage ones. */
  public static Schema writeSchema(Schema tableSchema) {
    return MetadataColumns.schemaWithRowLineage(tableSchema);
  }

  /**
   * The id of a row read out of {@code sourceFile} at {@code sourcePosition}.
   *
   * <p>Mirrors the read side: a value written into the file wins, and otherwise the id is derived.
   * Reading a file that has no {@code _row_id} column yields null for it, so both cases arrive here
   * the same way.
   *
   * @return the row's id, or null if the source file has no assigned range to derive one from
   */
  public static Long resolveRowId(Record source, DataFile sourceFile, long sourcePosition) {
    Object materialized = fieldOrNull(source, MetadataColumns.ROW_ID.name());
    if (materialized != null) {
      return (Long) materialized;
    }

    Long first = sourceFile.firstRowId();
    return first == null ? null : first + sourcePosition;
  }

  /**
   * Copies a row into {@code writeSchema}, carrying an explicit id.
   *
   * <p>{@code _last_updated_sequence_number} is left null so the reader falls back to the file's own
   * sequence number, which is what an operation that relocates a row without changing it should
   * report.
   */
  public static Record withRowId(Schema writeSchema, Record source, Long rowId) {
    Record row = GenericRecord.create(writeSchema);
    for (Types.NestedField field : writeSchema.columns()) {
      if (field.fieldId() == MetadataColumns.ROW_ID.fieldId()) {
        row.setField(field.name(), rowId);
      } else if (field.fieldId() == MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId()) {
        row.setField(field.name(), null);
      } else {
        row.setField(field.name(), fieldOrNull(source, field.name()));
      }
    }

    return row;
  }

  private static Object fieldOrNull(Record record, String name) {
    try {
      return record.getField(name);
    } catch (IllegalArgumentException e) {
      // The source was read without lineage columns, or lacks a column the write schema has.
      return null;
    }
  }
}
