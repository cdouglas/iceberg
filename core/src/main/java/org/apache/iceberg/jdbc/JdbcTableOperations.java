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

import java.sql.DataTruncation;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLWarning;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JdbcTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcTableOperations.class);
  private final String catalogName;
  private final TableIdentifier tableIdentifier;
  private final FileIO fileIO;
  private final JdbcClientPool connections;
  private final Map<String, String> catalogProperties;
  private final JdbcUtil.SchemaVersion schemaVersion;
  // the table as doCommit committed it, which commit adopts as current once it returns
  private TableMetadata committed = null;

  protected JdbcTableOperations(
      JdbcClientPool dbConnPool,
      FileIO fileIO,
      String catalogName,
      TableIdentifier tableIdentifier,
      Map<String, String> catalogProperties,
      JdbcUtil.SchemaVersion schemaVersion) {
    this.catalogName = catalogName;
    this.tableIdentifier = tableIdentifier;
    this.fileIO = fileIO;
    this.connections = dbConnPool;
    this.catalogProperties = catalogProperties;
    this.schemaVersion = schemaVersion;
  }

  @Override
  public void doRefresh() {
    Map<String, String> table;

    try {
      table = JdbcUtil.loadTable(schemaVersion, connections, catalogName, tableIdentifier);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted during refresh");
    } catch (SQLException e) {
      // SQL exception happened when getting table from catalog
      throw new UncheckedSQLException(
          e, "Failed to get table %s from catalog %s", tableIdentifier, catalogName);
    }

    if (table.isEmpty()) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(
            "Failed to load table %s from catalog %s: dropped by another process",
            tableIdentifier, catalogName);
      } else {
        this.disableRefresh();
        return;
      }
    }

    String newMetadataLocation = table.get(METADATA_LOCATION_PROP);
    Preconditions.checkState(
        newMetadataLocation != null,
        "Invalid table %s: metadata location is null",
        tableIdentifier);
    refreshFromMetadataLocation(newMetadataLocation);
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    this.committed = null;
    super.commit(base, metadata);
    TableMetadata justCommitted = committed;
    this.committed = null;
    if (justCommitted != null) {
      // the table's row now names the metadata file this commit wrote, whose contents are the
      // table as committed: hold them as current instead of reading the file back on the next
      // current(), as REST's table operations hold a commit's response
      try {
        refreshFromMetadataLocation(
            justCommitted.metadataFileLocation(), null, 0, location -> justCommitted);
      } catch (RuntimeException e) {
        // the commit succeeded; the next current() refreshes from the catalog, as it did before
        LOG.warn("Failed to adopt committed metadata for table {}", tableIdentifier, e);
      }
    }
  }

  @Override
  public void doCommit(TableMetadata base, TableMetadata metadata) {
    boolean newTable = base == null;
    String newMetadataLocation = writeNewMetadataIfRequired(newTable, metadata);
    // what TableMetadataParser.read returns for the file just written, built before the row
    // changes so that nothing can fail after it does
    TableMetadata written =
        TableMetadata.buildFrom(metadata)
            .withMetadataLocation(newMetadataLocation)
            .discardChanges()
            .build();
    try {
      Map<String, String> table =
          JdbcUtil.loadTable(schemaVersion, connections, catalogName, tableIdentifier);

      if (base != null) {
        validateMetadataLocation(table, base);
        String oldMetadataLocation = base.metadataFileLocation();
        // Start atomic update
        LOG.debug("Committing existing table: {}", tableName());
        updateTable(newMetadataLocation, oldMetadataLocation, metadata);
      } else {
        // table not exists create it
        LOG.debug("Committing new table: {}", tableName());
        createTable(newMetadataLocation, metadata);
      }

      // the conditional UPDATE changed exactly one row from base's location to this one, the
      // INSERT created the row with it, or a check of the commit's status after an unclear outcome
      // found the table names the file: the table is now, or was, the file just written
      this.committed = written;

    } catch (SQLTimeoutException e) {
      throw new UncheckedSQLException(e, "Database Connection timeout");
    } catch (SQLTransientConnectionException | SQLNonTransientConnectionException e) {
      throw new UncheckedSQLException(e, "Database Connection failed");
    } catch (DataTruncation e) {
      throw new UncheckedSQLException(e, "Database data truncation error");
    } catch (SQLWarning e) {
      throw new UncheckedSQLException(e, "Database warning");
    } catch (SQLException e) {
      if (JdbcUtil.isConstraintViolation(e)) {
        if (currentMetadataLocation() == null) {
          throw new AlreadyExistsException(e, "Table already exists: %s", tableIdentifier);
        } else {
          throw new UncheckedSQLException(e, "Table already exists: %s", tableIdentifier);
        }
      }

      throw new UncheckedSQLException(e, "Unknown failure");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted during commit");
    }
  }

  private void updateTable(
      String newMetadataLocation, String oldMetadataLocation, TableMetadata metadata)
      throws SQLException, InterruptedException {
    int updatedRecords;
    try {
      updatedRecords =
          JdbcUtil.updateTable(
              schemaVersion,
              connections,
              catalogName,
              tableIdentifier,
              newMetadataLocation,
              oldMetadataLocation);
    } catch (SQLException e) {
      if (connections.isConnectionException(e)) {
        // the connection failed while the UPDATE ran, on every attempt the pool made: it may
        // have committed
        checkCommitted(newMetadataLocation, metadata, e);
        return;
      }

      throw e;
    }

    if (updatedRecords == 1) {
      LOG.debug("Successfully committed to existing table: {}", tableIdentifier);
    } else {
      // No row had the base's metadata location: another process committed, or the pool ran the
      // UPDATE again after its connection failed (JdbcClientPool) and the first run had committed,
      // so the table names this commit's metadata file. The UPDATE has finished either way, so a
      // table that does not name the file now never will.
      switch (checkCommitStatusStrict(newMetadataLocation, metadata)) {
        case SUCCESS:
          LOG.info(
              "Committed to existing table {}, whose UPDATE matched no row when run again",
              tableIdentifier);
          return;
        case FAILURE:
          throw new CommitFailedException(
              "Failed to update table %s from catalog %s", tableIdentifier, catalogName);
        default:
          throw new CommitStateUnknownException(
              new CommitFailedException(
                  "Cannot tell whether table %s was updated in catalog %s",
                  tableIdentifier, catalogName));
      }
    }
  }

  /**
   * Returns if the table names {@code newMetadataLocation} after the connection failed while its
   * commit's statement ran ({@code cause}), and throws CommitStateUnknownException if it does not
   * or cannot be read: a statement whose connection failed may still have committed.
   */
  private void checkCommitted(
      String newMetadataLocation, TableMetadata metadata, SQLException cause) {
    if (checkCommitStatus(newMetadataLocation, metadata) != CommitStatus.SUCCESS) {
      throw new CommitStateUnknownException(cause);
    }

    LOG.info(
        "Committed to table {} although the connection failed while committing",
        tableIdentifier,
        cause);
  }

  private void createTable(String newMetadataLocation, TableMetadata metadata)
      throws SQLException, InterruptedException {
    Namespace namespace = tableIdentifier.namespace();
    if (PropertyUtil.propertyAsBoolean(catalogProperties, JdbcUtil.STRICT_MODE_PROPERTY, false)
        && !JdbcUtil.namespaceExists(catalogName, connections, namespace)) {
      throw new NoSuchNamespaceException(
          "Cannot create table %s in catalog %s. Namespace %s does not exist",
          tableIdentifier, catalogName, namespace);
    }

    if (schemaVersion == JdbcUtil.SchemaVersion.V1
        && JdbcUtil.viewExists(catalogName, connections, tableIdentifier)) {
      throw new AlreadyExistsException("View with same name already exists: %s", tableIdentifier);
    }

    if (JdbcUtil.tableExists(schemaVersion, catalogName, connections, tableIdentifier)) {
      throw new AlreadyExistsException("Table already exists: %s", tableIdentifier);
    }

    int insertRecord;
    try {
      insertRecord =
          JdbcUtil.doCommitCreateTable(
              schemaVersion,
              connections,
              catalogName,
              namespace,
              tableIdentifier,
              newMetadataLocation);
    } catch (SQLException e) {
      if (connections.isConnectionException(e)) {
        // the connection failed while the INSERT ran: it may have created the table (the pool runs
        // it again after a failure, which then fails on the table's key and reports the first)
        checkCommitted(newMetadataLocation, metadata, e);
        return;
      }

      throw e;
    }

    if (insertRecord == 1) {
      LOG.debug("Successfully committed to new table: {}", tableIdentifier);
    } else {
      throw new CommitFailedException(
          "Failed to create table %s in catalog %s", tableIdentifier, catalogName);
    }
  }

  private void validateMetadataLocation(Map<String, String> table, TableMetadata base) {
    String catalogMetadataLocation = table.get(METADATA_LOCATION_PROP);
    String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;

    if (!Objects.equals(baseMetadataLocation, catalogMetadataLocation)) {
      throw new CommitFailedException(
          "Cannot commit %s: metadata location %s has changed from %s",
          tableIdentifier, baseMetadataLocation, catalogMetadataLocation);
    }
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected String tableName() {
    return tableIdentifier.toString();
  }
}
