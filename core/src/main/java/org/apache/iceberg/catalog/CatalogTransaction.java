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
package org.apache.iceberg.catalog;

public interface CatalogTransaction {

  enum IsolationLevel {

    /**
     * All reads that are being made will see the last committed values that existed when the table
     * was loaded first inside the catalog transaction. Subsequent changes to a table that happened
     * outside the catalog transaction after the table was read will not be seen to prevent <b>read
     * skew</b> (reading a table multiple times within the catalog transaction should always return
     * the same results). <br>
     * <br>
     * Will successfully commit only if the values updated by the transaction do not conflict with
     * other concurrent updates. <br>
     * <br>
     *
     * <p>Note that under SNAPSHOT isolation a <b>write skew anomaly</b> is acceptable and
     * permitted. In a <b>write skew anomaly</b>, two transactions (T1 and T2) concurrently read an
     * overlapping data set (e.g. values V1 and V2), concurrently make disjoint updates (e.g. T1
     * updates V1, T2 updates V2), and finally concurrently commit, neither having seen the update
     * performed by the other.
     */
    SNAPSHOT,

    /**
     * As {@link #SNAPSHOT}, and everything the transaction read must read the same where its commit
     * takes effect, which is then a serial order for the whole transaction: so a <b>write skew
     * anomaly</b> is not possible. What a transaction reads is what it scanned (the rows of a
     * branch or tag under the scan's filter, as of the snapshot it named when the transaction first
     * read the table; an incremental scan reads the branch whose current snapshot it ends at and
     * the tags it names, and one between snapshots fixed by their ids reads nothing) and what it
     * declared ({@link CatalogTransaction#declareRead}); a metadata table it loads and a scan of
     * partition statistics read the whole table, as a declared read does. Loading a data table or
     * listing a namespace reads nothing. A read-only transaction's commit checks its reads and
     * writes nothing. See {@link TableRead}.
     */
    SERIALIZABLE;
  }

  /**
   * Performs an atomic commit of all the pending changes across multiple tables. Engine-specific
   * implementations must ensure that all pending changes are applied atomically.
   */
  void commitTransaction();

  /**
   * Returns this catalog transaction as a {@link Catalog} API so that any actions that are called
   * through this API are participating in this catalog transaction.
   *
   * @return This catalog transaction as a {@link Catalog} API. Any actions that are called through
   *     this API are participating in this catalog transaction.
   */
  Catalog asCatalog();

  /**
   * Makes a {@link IsolationLevel#SERIALIZABLE} transaction depend on the whole version of a table
   * as it first read it, as for a decision made on the table's schema, properties or snapshots
   * without scanning it: the commit fails if the table changed at all. Does nothing under {@link
   * IsolationLevel#SNAPSHOT}.
   *
   * @param identifier the table, by its name in this transaction
   * @throws org.apache.iceberg.exceptions.NoSuchTableException if there is no such table
   */
  void declareRead(TableIdentifier identifier);

  /**
   * Returns the current {@link IsolationLevel} for this transaction.
   *
   * @return The {@link IsolationLevel} for this transaction.
   */
  IsolationLevel isolationLevel();
}
