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
package org.apache.iceberg.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.NamespaceChange;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * A catalog transaction's commit ({@link
 * JdbcCatalog#commitTransaction(org.apache.iceberg.catalog.CatalogCommit)}) as the statements of
 * one database transaction.
 *
 * <p>It first reads every table the commit reads or changes, by its name when the catalog
 * transaction started and in the order of the names, and checks that each has the metadata location
 * the catalog transaction read. Where the database supports {@code SELECT ... FOR UPDATE}
 * (PostgreSQL) that read locks the table's row until the database transaction ends, so no other
 * commit changes the table before this one does; elsewhere the database's own isolation holds it
 * (SQLite's is serializable: once a transaction has read the database, no other commits a write
 * until it ends). Then it applies the commit's effects in order, each conditioned on the catalog as
 * the catalog transaction found it: the namespace changes other than drops, in order; the table
 * drops, updates and renames; the tables created; and the namespace drops. A condition that fails
 * ends the run with a {@link Failure}, and the database transaction is rolled back.
 *
 * <p>A table created, or a rename's target, must be absent: the catalog table's primary key, which
 * also covers views, keeps two transactions from both taking a name. A namespace a commit changes,
 * drops, or creates a table in must be present: its property rows are locked where the database
 * supports it, so that a namespace drop committed this way, which locks them before it checks that
 * the namespace is empty, and a create in the namespace are ordered.
 */
final class JdbcCatalogCommit {
  // a renamed table's name between the two steps of its rename, in its namespace
  private static final String TEMPORARY_NAME_PREFIX = "iceberg-catalog-transaction-";
  private static final String AND_AT_LOCATION =
      " AND " + BaseMetastoreTableOperations.METADATA_LOCATION_PROP + " = ?";
  private static final String GET_CHILD_NAMESPACE_PROPERTIES_SQL =
      "SELECT "
          + JdbcUtil.NAMESPACE_NAME
          + " FROM "
          + JdbcUtil.NAMESPACE_PROPERTIES_TABLE_NAME
          + " WHERE "
          + JdbcUtil.CATALOG_NAME
          + " = ? AND "
          + JdbcUtil.NAMESPACE_NAME
          + " LIKE ? ESCAPE '!'";

  /** Why a commit's condition failed, and the table whose version changed, if one did. */
  static final class Failure {
    private final TableIdentifier table;
    private final String message;

    private Failure(TableIdentifier table, String message, Object... args) {
      this.table = table;
      this.message = String.format(Locale.ROOT, message, args);
    }

    /** The table whose version is not the one the transaction read, or null. */
    TableIdentifier table() {
      return table;
    }

    String message() {
      return message;
    }
  }

  private final String catalogName;
  private final JdbcUtil.SchemaVersion schemaVersion;
  // every table the commit reads or changes, by its name at the start, with the metadata location
  // the transaction read
  private final SortedMap<TableIdentifier, String> expected = byName();
  private final List<NamespaceChange> namespaceChanges = Lists.newArrayList();
  // by name at the start: the location read
  private final SortedMap<TableIdentifier, String> drops = byName();
  // by name at the start: the location read and the new one
  private final SortedMap<TableIdentifier, Map.Entry<String, String>> updates = byName();
  // by name at the start: the name at the end
  private final SortedMap<TableIdentifier, TableIdentifier> renames = byName();
  // by name: the new metadata location
  private final SortedMap<TableIdentifier, String> creates = byName();
  // a COMMIT that failed in a way that may have committed
  private SQLException unanswered = null;

  JdbcCatalogCommit(String catalogName, JdbcUtil.SchemaVersion schemaVersion) {
    this.catalogName = catalogName;
    this.schemaVersion = schemaVersion;
  }

  private static <V> SortedMap<TableIdentifier, V> byName() {
    return new TreeMap<>(Comparator.comparing(TableIdentifier::toString));
  }

  /** Makes the commit depend on {@code table} having {@code metadataLocation}. */
  void expect(TableIdentifier table, String metadataLocation) {
    Preconditions.checkArgument(
        metadataLocation != null, "Cannot commit %s: no version of it read", table);
    String other = expected.putIfAbsent(table, metadataLocation);
    Preconditions.checkArgument(
        other == null || other.equals(metadataLocation),
        "Cannot commit %s: read at two versions, %s and %s",
        table,
        other,
        metadataLocation);
  }

  /** Adds a namespace change; a create's properties are its rows, the existence marker included. */
  void change(NamespaceChange change) {
    namespaceChanges.add(change);
  }

  void drop(TableIdentifier table, String metadataLocation) {
    expect(table, metadataLocation);
    drops.put(table, metadataLocation);
  }

  void update(TableIdentifier table, String metadataLocation, String newMetadataLocation) {
    expect(table, metadataLocation);
    updates.put(
        table, new AbstractMap.SimpleImmutableEntry<>(metadataLocation, newMetadataLocation));
  }

  void rename(TableIdentifier from, String metadataLocation, TableIdentifier to) {
    expect(from, metadataLocation);
    renames.put(from, to);
  }

  void create(TableIdentifier table, String metadataLocation) {
    Preconditions.checkArgument(
        creates.putIfAbsent(table, metadataLocation) == null, "Cannot create %s twice", table);
  }

  /** Whether the commit has nothing to check and nothing to change. */
  boolean isEmpty() {
    return expected.isEmpty() && namespaceChanges.isEmpty() && creates.isEmpty();
  }

  private boolean changes() {
    return !namespaceChanges.isEmpty()
        || !drops.isEmpty()
        || !updates.isEmpty()
        || !renames.isEmpty()
        || !creates.isEmpty();
  }

  /**
   * The metadata files only this commit wrote, by the name of the table each is in once the commit
   * lands: if a table names one of them, the commit landed.
   */
  Map<TableIdentifier, String> witnesses() {
    Map<TableIdentifier, String> witnesses = Maps.newLinkedHashMap();
    updates.forEach(
        (table, locations) ->
            witnesses.put(renames.getOrDefault(table, table), locations.getValue()));
    witnesses.putAll(creates);
    return witnesses;
  }

  /**
   * The COMMIT of a run that failed in a way that may have committed it, or null: a later run's
   * outcome then does not say whether the commit landed.
   */
  SQLException unanswered() {
    return unanswered;
  }

  /**
   * Runs the commit in one database transaction on {@code conn}: returns null if it committed, or
   * the {@link Failure} of a condition after rolling back. A commit that changes nothing checks its
   * reads and rolls back. Throws what the database threw, after rolling back; a COMMIT that failed
   * other than by the database rolling it back is recorded first ({@link #unanswered()}).
   */
  Failure run(Connection conn) throws SQLException {
    boolean autoCommit = conn.getAutoCommit();
    boolean ended = false;
    conn.setAutoCommit(false);
    try {
      Failure failure = apply(conn);
      if (failure != null || !changes()) {
        conn.rollback();
        ended = true;
        return failure;
      }

      try {
        conn.commit();
      } catch (SQLException e) {
        if (!JdbcUtil.isRollback(e) && !JdbcUtil.isConstraintViolation(e)) {
          this.unanswered = e;
        }

        throw e;
      }

      ended = true;
      return null;
    } catch (SQLException | RuntimeException e) {
      try {
        conn.rollback();
        ended = true;
      } catch (SQLException rollback) {
        e.addSuppressed(rollback);
      }

      throw e;
    } finally {
      if (ended) {
        // restoring auto-commit commits an open transaction: only restore it once this one ended
        try {
          conn.setAutoCommit(autoCommit);
        } catch (SQLException ignored) {
          // a broken connection: the pool discards it
        }
      }
    }
  }

  private Failure apply(Connection conn) throws SQLException {
    boolean lockRows = conn.getMetaData().supportsSelectForUpdate();
    Failure failure = checkVersions(conn, lockRows);
    for (NamespaceChange change : namespaceChanges) {
      if (failure == null && change.kind() != NamespaceChange.Kind.DROP) {
        failure = changeNamespace(conn, change, lockRows);
      }
    }

    failure = failure != null ? failure : changeTables(conn, lockRows);
    failure = failure != null ? failure : createTables(conn, lockRows);
    for (NamespaceChange change : namespaceChanges) {
      if (failure == null && change.kind() == NamespaceChange.Kind.DROP) {
        failure = dropNamespace(conn, change.namespace(), lockRows);
      }
    }

    return failure;
  }

  /**
   * Checks that every table the commit reads or changes has the metadata location the transaction
   * read, in the order of their names; with {@code lockRows}, locks each table's row.
   */
  private Failure checkVersions(Connection conn, boolean lockRows) throws SQLException {
    for (Map.Entry<TableIdentifier, String> table : expected.entrySet()) {
      String location = metadataLocation(conn, table.getKey(), lockRows);
      if (location == null) {
        return new Failure(
            table.getKey(), "Cannot commit transaction: table %s no longer exists", table.getKey());
      } else if (!location.equals(table.getValue())) {
        return changed(table.getKey());
      }
    }

    return null;
  }

  private Failure createTables(Connection conn, boolean lockRows) throws SQLException {
    for (Map.Entry<TableIdentifier, String> create : creates.entrySet()) {
      TableIdentifier table = create.getKey();
      String absent = whyNotCreate(conn, table, lockRows);
      if (absent != null) {
        return new Failure(null, "Cannot commit transaction: cannot create %s: %s", table, absent);
      }

      String[] row = {
        catalogName,
        JdbcUtil.namespaceToString(table.namespace()),
        table.name(),
        create.getValue(),
        JdbcUtil.TABLE_RECORD_TYPE
      };
      if (schemaVersion != JdbcUtil.SchemaVersion.V1) {
        row = Arrays.copyOf(row, 4);
      }

      if (execute(conn, createSql(), row) != 1) {
        return new Failure(null, "Cannot commit transaction: cannot create %s", table);
      }
    }

    return null;
  }

  /**
   * Drops, updates and renames tables, each conditioned on the metadata location read. A rename
   * moves its table to a temporary name first and then to its target, so that renames among the
   * commit's own tables (a swap) do not collide.
   */
  private Failure changeTables(Connection conn, boolean lockRows) throws SQLException {
    for (Map.Entry<TableIdentifier, String> drop : drops.entrySet()) {
      TableIdentifier table = drop.getKey();
      if (execute(conn, dropSql(), tableArgs(table, drop.getValue())) != 1) {
        return changed(table);
      }
    }

    for (Map.Entry<TableIdentifier, Map.Entry<String, String>> update : updates.entrySet()) {
      TableIdentifier table = update.getKey();
      String location = update.getValue().getKey();
      String newLocation = update.getValue().getValue();
      if (execute(
              conn,
              updateSql(),
              newLocation,
              location,
              catalogName,
              JdbcUtil.namespaceToString(table.namespace()),
              table.name(),
              location)
          != 1) {
        return changed(table);
      }
    }

    Map<TableIdentifier, TableIdentifier> temporary = Maps.newLinkedHashMap();
    for (Map.Entry<TableIdentifier, TableIdentifier> rename : renames.entrySet()) {
      TableIdentifier from = rename.getKey();
      TableIdentifier moved =
          TableIdentifier.of(from.namespace(), TEMPORARY_NAME_PREFIX + UUID.randomUUID());
      Map.Entry<String, String> update = updates.get(from);
      String location = update != null ? update.getValue() : expected.get(from);
      if (execute(conn, renameSql() + AND_AT_LOCATION, renameArgs(from, moved, location)) != 1) {
        return changed(from);
      }

      temporary.put(moved, from);
    }

    for (Map.Entry<TableIdentifier, TableIdentifier> moved : temporary.entrySet()) {
      TableIdentifier from = moved.getValue();
      TableIdentifier to = renames.get(from);
      String absent = whyNotCreate(conn, to, lockRows);
      if (absent != null) {
        return new Failure(
            null, "Cannot commit transaction: cannot rename %s to %s: %s", from, to, absent);
      }

      if (execute(conn, renameSql(), renameArgs(moved.getKey(), to, null)) != 1) {
        return new Failure(null, "Cannot commit transaction: cannot rename %s to %s", from, to);
      }
    }

    return null;
  }

  private Failure changeNamespace(Connection conn, NamespaceChange change, boolean lockRows)
      throws SQLException {
    Namespace namespace = change.namespace();
    String name = JdbcUtil.namespaceToString(namespace);
    switch (change.kind()) {
      case CREATE:
        if (namespaceExists(conn, namespace, false)) {
          return new Failure(
              null, "Cannot commit transaction: namespace %s already exists", namespace);
        }

        insertProperties(conn, name, change.properties());
        return null;

      case SET_PROPERTIES:
        if (!namespaceExists(conn, namespace, lockRows)) {
          return noSuchNamespace(namespace);
        }

        if (!change.properties().isEmpty()) {
          Set<String> existing = properties(conn, name).keySet();
          Map<String, String> inserts = Maps.newHashMap();
          Map<String, String> updated = Maps.newHashMap();
          change
              .properties()
              .forEach(
                  (key, value) -> (existing.contains(key) ? updated : inserts).put(key, value));
          insertProperties(conn, name, inserts);
          if (!updated.isEmpty()) {
            String[] args =
                Stream.concat(
                        updated.entrySet().stream()
                            .flatMap(e -> Stream.of(e.getKey(), e.getValue())),
                        Stream.concat(Stream.of(catalogName, name), updated.keySet().stream()))
                    .toArray(String[]::new);
            if (execute(conn, JdbcUtil.updatePropertiesStatement(updated.size()), args)
                != updated.size()) {
              return new Failure(
                  null, "Cannot commit transaction: namespace %s changed", namespace);
            }
          }
        }

        return null;

      case REMOVE_PROPERTIES:
        if (!namespaceExists(conn, namespace, lockRows)) {
          return noSuchNamespace(namespace);
        }

        if (!change.removals().isEmpty()) {
          execute(
              conn,
              JdbcUtil.deletePropertiesStatement(change.removals()),
              Stream.concat(Stream.of(catalogName, name), change.removals().stream())
                  .toArray(String[]::new));
        }

        return null;

      default:
        throw new IllegalArgumentException("Unknown namespace change: " + change.kind());
    }
  }

  private Failure dropNamespace(Connection conn, Namespace namespace, boolean lockRows)
      throws SQLException {
    // lock its rows before looking for tables in it, so a create in it committed this way waits
    if (!namespaceExists(conn, namespace, lockRows)) {
      return noSuchNamespace(namespace);
    }

    String name = JdbcUtil.namespaceToString(namespace);
    String children = JdbcUtil.childNamespacePattern(namespace);
    if (JdbcUtil.exists(conn, JdbcUtil.GET_NAMESPACE_SQL, catalogName, name, children)
        || JdbcUtil.exists(conn, GET_CHILD_NAMESPACE_PROPERTIES_SQL, catalogName, children)) {
      return new Failure(null, "Cannot commit transaction: namespace %s is not empty", namespace);
    }

    execute(conn, JdbcUtil.DELETE_ALL_NAMESPACE_PROPERTIES_SQL, catalogName, name);
    return null;
  }

  /**
   * Whether {@code namespace} exists on {@code conn}, as JdbcCatalog decides; with {@code
   * lockRows}, its property rows are locked first.
   */
  private boolean namespaceExists(Connection conn, Namespace namespace, boolean lockRows)
      throws SQLException {
    if (namespace.isEmpty()) {
      return true;
    }

    if (lockRows
        && JdbcUtil.exists(
            conn,
            JdbcUtil.GET_ALL_NAMESPACE_PROPERTIES_SQL + "FOR UPDATE",
            catalogName,
            JdbcUtil.namespaceToString(namespace))) {
      return true;
    }

    return JdbcUtil.namespaceExists(conn, catalogName, namespace);
  }

  /** Why {@code table} cannot be created on {@code conn}, or null if it can. */
  private String whyNotCreate(Connection conn, TableIdentifier table, boolean lockRows)
      throws SQLException {
    if (!namespaceExists(conn, table.namespace(), lockRows)) {
      return String.format(Locale.ROOT, "namespace %s does not exist", table.namespace());
    }

    // a table or a view: both are rows of the catalog table, keyed by name
    if (JdbcUtil.exists(
        conn,
        JdbcUtil.V0_GET_TABLE_SQL,
        catalogName,
        JdbcUtil.namespaceToString(table.namespace()),
        table.name())) {
      return String.format(Locale.ROOT, "%s already exists", table);
    }

    return null;
  }

  /** The metadata location of {@code table} on {@code conn}, or null if there is no such table. */
  private String metadataLocation(Connection conn, TableIdentifier table, boolean lockRows)
      throws SQLException {
    String sql =
        (schemaVersion == JdbcUtil.SchemaVersion.V1
                ? JdbcUtil.V1_GET_TABLE_SQL
                : JdbcUtil.V0_GET_TABLE_SQL)
            + (lockRows ? " FOR UPDATE" : "");
    try (PreparedStatement statement = conn.prepareStatement(sql)) {
      statement.setString(1, catalogName);
      statement.setString(2, JdbcUtil.namespaceToString(table.namespace()));
      statement.setString(3, table.name());
      try (ResultSet rs = statement.executeQuery()) {
        return rs.next() ? rs.getString(BaseMetastoreTableOperations.METADATA_LOCATION_PROP) : null;
      }
    }
  }

  private Map<String, String> properties(Connection conn, String namespace) throws SQLException {
    Map<String, String> properties = Maps.newHashMap();
    try (PreparedStatement statement =
        conn.prepareStatement(JdbcUtil.GET_ALL_NAMESPACE_PROPERTIES_SQL)) {
      statement.setString(1, catalogName);
      statement.setString(2, namespace);
      try (ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          properties.put(
              rs.getString(JdbcUtil.NAMESPACE_PROPERTY_KEY),
              rs.getString(JdbcUtil.NAMESPACE_PROPERTY_VALUE));
        }
      }
    }

    return properties;
  }

  private void insertProperties(Connection conn, String namespace, Map<String, String> properties)
      throws SQLException {
    if (!properties.isEmpty()) {
      String[] args =
          properties.entrySet().stream()
              .flatMap(e -> Stream.of(catalogName, namespace, e.getKey(), e.getValue()))
              .toArray(String[]::new);
      execute(conn, JdbcUtil.insertPropertiesStatement(properties.size()), args);
    }
  }

  private String[] tableArgs(TableIdentifier table, String location) {
    return new String[] {
      catalogName, JdbcUtil.namespaceToString(table.namespace()), table.name(), location
    };
  }

  private String[] renameArgs(TableIdentifier from, TableIdentifier to, String location) {
    String[] args = {
      JdbcUtil.namespaceToString(to.namespace()),
      to.name(),
      catalogName,
      JdbcUtil.namespaceToString(from.namespace()),
      from.name(),
      location
    };
    return location != null ? args : Arrays.copyOf(args, 5);
  }

  private String dropSql() {
    return (schemaVersion == JdbcUtil.SchemaVersion.V1
            ? JdbcUtil.V1_DROP_TABLE_SQL
            : JdbcUtil.V0_DROP_TABLE_SQL)
        + AND_AT_LOCATION;
  }

  private String updateSql() {
    return schemaVersion == JdbcUtil.SchemaVersion.V1
        ? JdbcUtil.V1_DO_COMMIT_TABLE_SQL
        : JdbcUtil.V0_DO_COMMIT_SQL;
  }

  private String renameSql() {
    return schemaVersion == JdbcUtil.SchemaVersion.V1
        ? JdbcUtil.V1_RENAME_TABLE_SQL
        : JdbcUtil.V0_RENAME_TABLE_SQL;
  }

  private String createSql() {
    return schemaVersion == JdbcUtil.SchemaVersion.V1
        ? JdbcUtil.V1_DO_COMMIT_CREATE_SQL
        : JdbcUtil.V0_DO_COMMIT_CREATE_SQL;
  }

  private static Failure changed(TableIdentifier table) {
    return new Failure(
        table, "Cannot commit transaction: table %s changed since the transaction read it", table);
  }

  private static Failure noSuchNamespace(Namespace namespace) {
    return new Failure(null, "Cannot commit transaction: namespace %s does not exist", namespace);
  }

  private static int execute(Connection conn, String sql, String... args) throws SQLException {
    try (PreparedStatement statement = conn.prepareStatement(sql)) {
      for (int pos = 0; pos < args.length; pos += 1) {
        statement.setString(pos + 1, args[pos]);
      }

      return statement.executeUpdate();
    }
  }
}
