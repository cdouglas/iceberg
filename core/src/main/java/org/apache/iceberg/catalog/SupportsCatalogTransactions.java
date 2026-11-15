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

import org.apache.iceberg.catalog.CatalogTransaction.IsolationLevel;

/**
 * A catalog that commits several changes to tables and namespaces atomically: a catalog
 * transaction.
 */
public interface SupportsCatalogTransactions {

  /**
   * Create a new {@link CatalogTransaction} with the given {@link IsolationLevel}.
   *
   * @param isolationLevel The isolation level to use.
   * @return A new {@link CatalogTransaction}.
   * @throws UnsupportedOperationException if the catalog does not support the isolation level
   */
  CatalogTransaction createTransaction(IsolationLevel isolationLevel);

  /**
   * Commits a catalog transaction: applies everything in {@code commit} where every condition and
   * read holds, or nothing (see {@link CatalogCommit}). A commit with no changes checks its reads
   * against the catalog as it is and writes nothing.
   *
   * @throws org.apache.iceberg.exceptions.CommitFailedException if a condition or read no longer
   *     holds; the caller may run the transaction again
   * @throws org.apache.iceberg.exceptions.ValidationException if a table update's own validation
   *     fails
   * @throws org.apache.iceberg.exceptions.CommitStateUnknownException if the outcome is unknown
   * @throws UnsupportedOperationException if the catalog cannot commit some of it
   */
  void commitTransaction(CatalogCommit commit);
}
