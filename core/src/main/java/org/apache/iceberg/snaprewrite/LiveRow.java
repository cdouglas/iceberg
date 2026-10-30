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

import org.apache.iceberg.data.Record;

/**
 * One row a snapshot reads, as a scan returns it: where it physically is, its lineage, and
 * optionally its values.
 *
 * <p>A reconstruction identifies a physical row by its location, and a logical row by its {@code
 * _row_id}. The two differ exactly when a row has more than one version in the layout -- an updated
 * row keeps its id but lives in a new file -- and keeping both is what tells the versions apart.
 */
public class LiveRow {
  private final String path;
  private final long position;
  private final Long rowId;
  private final Long lastUpdatedSequenceNumber;
  private final Record values;

  public LiveRow(
      String path, long position, Long rowId, Long lastUpdatedSequenceNumber, Record values) {
    this.path = path;
    this.position = position;
    this.rowId = rowId;
    this.lastUpdatedSequenceNumber = lastUpdatedSequenceNumber;
    this.values = values;
  }

  /** The data file the row is read from. */
  public String path() {
    return path;
  }

  /** The row's position in that file. */
  public long position() {
    return position;
  }

  public Long rowId() {
    return rowId;
  }

  public Long lastUpdatedSequenceNumber() {
    return lastUpdatedSequenceNumber;
  }

  /** The row's values, or null when they were not asked for. Fields are looked up by name. */
  public Record values() {
    return values;
  }
}
