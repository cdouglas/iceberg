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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.view.ViewMetadata;
import org.immutables.value.Value;

/**
 * Everything a catalog transaction commits: its net effect on the catalog as it found it, and, for
 * {@link CatalogTransaction.IsolationLevel#SERIALIZABLE}, what it read.
 *
 * <p>A catalog applies all of it where every condition and read holds, or none of it. The effect
 * is, in order: the namespace changes other than drops, in the order the transaction made them; the
 * table and view changes (drops, then updates, then renames); the tables and views created; then
 * the namespace drops. Tables and views share their namespace's names, so a name a drop or rename
 * frees may be taken by a table or a view. The conditions, checked where the commit takes effect: a
 * namespace created is absent, a namespace changed is present, a namespace dropped then holds no
 * tables, views or namespaces; a table changed is the table the transaction read (by uuid), its
 * updates commit as the catalog commits a table's (at least by their requirements); a view changed
 * is the view at the version the transaction read ({@link ViewChange#base()}); a rename's target is
 * absent (no table or view has its name) and its namespace present; a table or view created is
 * absent, as is any table or view of its name, and its namespace present; and every read is
 * unchanged ({@link TableRead}). A drop that purges deletes the table's files after the commit
 * lands.
 */
@Value.Immutable
public interface CatalogCommit {
  CatalogTransaction.IsolationLevel isolationLevel();

  /** Namespace changes, in the order the transaction made them. */
  List<NamespaceChange> namespaceChanges();

  /** Changes to tables that existed when the transaction started. */
  List<TableChange> tableChanges();

  /** Tables the transaction created, by their final names, with their metadata. */
  Map<TableIdentifier, TableMetadata> createdTables();

  /** Changes to views that existed when the transaction started. */
  List<ViewChange> viewChanges();

  /** Views the transaction created, by their final names, with their metadata. */
  Map<TableIdentifier, ViewMetadata> createdViews();

  /** Whether the commit changes views, which a catalog without views cannot commit. */
  default boolean hasViewChanges() {
    return !viewChanges().isEmpty() || !createdViews().isEmpty();
  }

  /** What a serializable transaction read; empty under SNAPSHOT isolation. */
  List<TableRead> reads();

  /** Whether the commit changes anything (a read-only one only checks its reads). */
  default boolean hasChanges() {
    return !namespaceChanges().isEmpty()
        || !tableChanges().isEmpty()
        || !createdTables().isEmpty()
        || hasViewChanges();
  }
}
