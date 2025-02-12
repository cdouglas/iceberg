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
package org.apache.iceberg.io;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.BaseCatalogTransaction;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsCatalogTransactions;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LocationUtil;

public class FileIOCatalog extends BaseMetastoreCatalog
    implements Configurable, SupportsNamespaces, SupportsCatalogTransactions {
  // TODO audit loadTable in BaseMetastoreCatalog
  // TODO buildTable overridden in BaseMetastoreCatalog?

  private static final String FILE_FORMAT = "fileio.catalog.format";

  private Configuration conf; // TODO: delete
  private String catalogName = "fileio";
  private String catalogLocation;
  private String warehouseLocation;
  private CatalogFormat format;
  private SupportsAtomicOperations<CAS> fileIO;
  private final Map<String, String> catalogProperties;

  @SuppressWarnings("unused") // reflection cstr
  FileIOCatalog() {
    // XXX Isn't using Maps.newHashMap() deprecated after Java7?
    catalogProperties = Maps.newHashMap();
  }

  public FileIOCatalog(
      String catalogName,
      String catalogLocation,
      Configuration conf,
      CatalogFormat format,
      SupportsAtomicOperations<CAS> fileIO,
      Map<String, String> catalogProperties) {
    this.catalogName = catalogName;
    this.catalogLocation = catalogLocation;
    this.conf = conf;
    this.format = format;
    this.fileIO = fileIO;
    this.catalogProperties = catalogProperties;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
    Preconditions.checkNotNull(properties, "Properties are required");
    catalogProperties.putAll(properties);

    if (name != null) {
      catalogName = name;
    }

    warehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
    if (null == catalogLocation) {
      Preconditions.checkArgument(
          !Strings.isNullOrEmpty(warehouseLocation),
          "Cannot initialize FileIOCatalog because warehousePath must not be null or empty");
      catalogLocation = LocationUtil.stripTrailingSlash(warehouseLocation) + "/catalog";
    }
    if (null == format) {
      // TODO configuration
      format = new CASCatalogFormat();
    }
    if (null == fileIO) {
      // TODO check warehouseLocation schema?
      String fileIOImpl =
          properties.getOrDefault(
              CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.gcp.gcs.GCSFileIO");

      // TODO handle this more gracefully; use listings/HadoopCatalog?
      // TODO remove generics
      fileIO =
          (SupportsAtomicOperations<CAS>) CatalogUtil.loadFileIO(fileIOImpl, properties, getConf());
    }
    final InputFile catalogFile = fileIO.newInputFile(catalogLocation);
    if (!catalogFile.exists()) {
      format.empty(catalogFile).commit(fileIO);
    }
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    if (!namespaceExists(namespace) && !namespace.isEmpty()) {
      throw new NoSuchNamespaceException(
          "Cannot list tables for namespace. Namespace does not exist: %s", namespace);
    }
    return getCatalogFile().tables().stream()
        .filter(t -> t.namespace().isEmpty() || t.namespace().equals(namespace))
        .sorted(Comparator.comparing(TableIdentifier::toString))
        .collect(Collectors.toList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    final InputFile catalog = fileIO.newInputFile(catalogLocation);
    final CatalogFile catalogFile = format.read(fileIO, catalog);
    try {
      format.from(catalogFile).dropTable(identifier).commit(fileIO);
      return true;
    } catch (CommitFailedException | NoSuchTableException e) {
      return false;
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    final InputFile catalog = fileIO.newInputFile(catalogLocation);
    final CatalogFile catalogFile = format.read(fileIO, catalog);
    format
        .from(catalogFile)
        .dropTable(from)
        .createTable(to, catalogFile.location(from)) // TODO preserve metadata
        .commit(fileIO);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new FileIOTableOperations(tableIdentifier, catalogLocation, format, fileIO);
  }

  FileIOTableOperations newTableOps(TableIdentifier tableIdentifier, CatalogFile catalogFile) {
    return new FileIOTableOperations(tableIdentifier, catalogLocation, format, fileIO, catalogFile);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return String.join(
        "/", defaultNamespaceLocation(tableIdentifier.namespace()), tableIdentifier.name());
  }

  private String defaultNamespaceLocation(Namespace namespace) {
    if (namespace.isEmpty()) {
      return warehouseLocation;
    }
    return warehouseLocation + "/" + String.join("/", namespace.levels());
  }

  private CatalogFile getCatalogFile() {
    final InputFile catalog = fileIO.newInputFile(catalogLocation);
    return format.read(fileIO, catalog);
  }

  //
  // SupportsNamespaces
  //

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    final InputFile catalog = fileIO.newInputFile(catalogLocation);
    final CatalogFile catalogFile = format.read(fileIO, catalog);
    format.from(catalogFile).createNamespace(namespace, metadata).commit(fileIO);
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    return Lists.newArrayList(getCatalogFile().namespaces().iterator());
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    CatalogFile catalogFile = getCatalogFile();
    if (catalogFile.namespaces().contains(namespace)) {
      return catalogFile.namespaceProperties(namespace);
    } else {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    final InputFile catalog = fileIO.newInputFile(catalogLocation);
    // XXX TODO wait, wtf?
    //     TODO catalog ops must also follow the refresh cycle, or only TableOperations detect
    // concurrent changes?
    final CatalogFile catalogFile = format.read(fileIO, catalog);
    try {
      format.from(catalogFile).dropNamespace(namespace).commit(fileIO);
    } catch (NoSuchNamespaceException e) {
      return false; // sigh.
    }
    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    final CatalogFile catalogFile = getCatalogFile();
    try {
      format.from(catalogFile).updateProperties(namespace, properties).commit(fileIO);
    } catch (CommitFailedException e) {
      return false; // sigh.
    }
    return false;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    return setProperties(
        namespace,
        properties.stream().collect(Maps::newHashMap, (m, k) -> m.put(k, null), Map::putAll));
  }

  static class FileIOTableOperations extends BaseMetastoreTableOperations {
    private final String catalogLocation;
    private final TableIdentifier tableId;
    private final CatalogFormat format;
    private final SupportsAtomicOperations<CAS> fileIO;
    private volatile CatalogFile lastCatalogFile = null;

    FileIOTableOperations(
        TableIdentifier tableId,
        String catalogLocation,
        CatalogFormat format,
        SupportsAtomicOperations<CAS> fileIO) {
      this(tableId, catalogLocation, format, fileIO, null);
    }

    FileIOTableOperations(
        TableIdentifier tableId,
        String catalogLocation,
        CatalogFormat format,
        SupportsAtomicOperations<CAS> fileIO,
        CatalogFile catalogFile) {
      this.fileIO = fileIO;
      this.format = format;
      this.tableId = tableId;
      this.catalogLocation = catalogLocation;
      this.lastCatalogFile = catalogFile;
      if (catalogFile != null) {
        updateVersionAndMetadata(catalogFile.version(tableId), catalogFile.location(tableId));
        disableRefresh();
      }
    }

    @Override
    public SupportsAtomicOperations<CAS> io() {
      return fileIO;
    }

    // version 0 reserved for empty catalog; tables created in subsequent commits TODO replace w/
    // metadata embed
    private synchronized void updateVersionAndMetadata(int newVersion, String metadataFile) {
      // update if table exists and version lags newVersion
      if (null == metadataFile) {
        if (currentMetadataLocation() != null) {
          // table used to exist, but not found in CatalogFile
          throw new NoSuchTableException("Table %s was deleted", tableId);
        } else {
          disableRefresh(); // table does not exist, yet; no need to refresh
          return;
        }
      }
      // checks for UUID match
      refreshFromMetadataLocation(metadataFile);
    }

    @Override
    protected String tableName() {
      return tableId.toString();
    }

    @Override
    protected void doRefresh() {
      final CatalogFile updatedCatalogFile =
          format.read(fileIO, io().newInputFile(catalogLocation));
      updateVersionAndMetadata(
          updatedCatalogFile.version(tableId), updatedCatalogFile.location(tableId));
      lastCatalogFile = updatedCatalogFile;
    }

    // visible from commitTransaction
    String writeUpdateMetadata(boolean isCreate, TableMetadata metadata) {
      return writeNewMetadataIfRequired(isCreate, metadata);
    }

    @Override
    public void doCommit(TableMetadata base, TableMetadata metadata) {
      final boolean isCreate = null == base;
      final String newMetadataLocation = writeUpdateMetadata(isCreate, metadata);
      try {
        if (null == base) {
          format.from(lastCatalogFile).createTable(tableId, newMetadataLocation).commit(io());
        } else {
          format.from(lastCatalogFile).updateTable(tableId, newMetadataLocation).commit(io());
        }
      } catch (SupportsAtomicOperations.CASException e) {
        throw new CommitFailedException(e, "Failed to commit metadata for table %s", tableId);
      }
    }
  }

  // SupportsCatalogTransaction

  @Override
  public void commitTransaction(List<TableCommit> commits) {
    // TableCommit validations check the table UUID and snapshot ref for each table
    // if all validations pass for the current CatalogFile, then attempt atomic replace
    final CatalogFile current = getCatalogFile();
    final CatalogFile.Mut newCatalog = format.from(current);
    for (TableCommit commit : commits) {
      final TableIdentifier tableId = commit.identifier();
      // use fixed catalog snapshot for validation
      FileIOTableOperations ops = newTableOps(tableId, current);
      final TableMetadata currentMetadata = ops.current();
      commit.requirements().forEach(req -> req.validate(currentMetadata));

      TableMetadata.Builder newMetadataBuilder = TableMetadata.buildFrom(currentMetadata);
      commit.updates().forEach(update -> update.applyTo(newMetadataBuilder));
      final TableMetadata newMetadata = newMetadataBuilder.build();
      if (newMetadata.changes().isEmpty()) {
        continue;
      }
      // !#! HERE
      final String newLocation = ops.writeUpdateMetadata(false, newMetadata);
      newCatalog.updateTable(tableId, newLocation);
    }
    try {
      newCatalog.commit(fileIO);
    } catch (SupportsAtomicOperations.CASException e) {
      throw new CommitFailedException(e, "Failed to commit metadata for multi-table commit");
    }
  }

  @Override
  public CatalogTransaction createTransaction(CatalogTransaction.IsolationLevel isolationLevel) {
    return new BaseCatalogTransaction(this, isolationLevel);
  }
}
