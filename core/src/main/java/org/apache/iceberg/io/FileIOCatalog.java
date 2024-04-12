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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
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
    implements Configurable, SupportsNamespaces {
  // TODO audit loadTable in BaseMetastoreCatalog
  // TODO buildTable overridden in BaseMetastoreCatalog?

  private Configuration conf; // TODO: delete
  private String catalogName = "fileio";
  private String catalogLocation;
  private String warehouseLocation;
  private SupportsAtomicOperations fileIO;
  private Map<String, String> catalogProperties;

  public FileIOCatalog() {
    // XXX Isn't using Maps.newHashMap() deprecated after Java7?
    catalogProperties = Maps.newHashMap();
  }

  public FileIOCatalog(
      String catalogName,
      String catalogLocation,
      Configuration conf,
      SupportsAtomicOperations fileIO,
      Map<String, String> catalogProperties) {
    this.catalogName = catalogName;
    this.catalogLocation = catalogLocation;
    this.conf = conf;
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
    String uri = properties.get(CatalogProperties.URI);

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
    if (null == fileIO) {
      String fileIOImpl =
          properties.getOrDefault(
              CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.gcp.gcs.GCSFileIO");

      // TODO handle this more gracefully; use listings/HadoopCatalog
      fileIO = (SupportsAtomicOperations) CatalogUtil.loadFileIO(fileIOImpl, properties, getConf());
    }
    // TODO: create empty catalog if not exists
    try (SupportsAtomicOperations io = fileIO) {
      if (!io.newInputFile(catalogLocation).exists()) {
        try (OutputStream out = io.newOutputFile(catalogLocation).create()) {
          CatalogFile.empty().commit(out);
        } catch (IOException e) {
          // ignore
        }
      }
    }
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    Preconditions.checkArgument(Namespace.empty().equals(namespace), "Namespaces not supported");
    return getCatalogFile().tables();
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    final InputFile catalog = fileIO.newInputFile(catalogLocation);
    final CatalogFile catalogFile = getCatalogFile(catalog);
    try {
      CatalogFile.from(catalogFile).dropTable(identifier).commit(fileIO.newOutputFile(catalog));
      return true;
    } catch (CommitFailedException e) {
      return false;
    } catch (NoSuchNamespaceException e) {
      return false;
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    final InputFile catalog = fileIO.newInputFile(catalogLocation);
    final CatalogFile catalogFile = getCatalogFile(catalog);
    CatalogFile.from(catalogFile)
        .dropTable(from)
        .createTable(to, catalogFile.location(from)) // TODO preserve metadata
        .commit(fileIO.newOutputFile(catalog));
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new FileIOTableOperations(tableIdentifier, catalogLocation, fileIO);
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
    return getCatalogFile(catalog);
  }

  //
  // SupportsNamespaces
  //

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    final InputFile catalog = fileIO.newInputFile(catalogLocation);
    final CatalogFile catalogFile = getCatalogFile(catalog);
    CatalogFile.from(catalogFile)
        .createNamespace(namespace, metadata)
        .commit(fileIO.newOutputFile(catalog));
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
      return getCatalogFile().namespaceProperties(namespace);
    } else {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    final InputFile catalog = fileIO.newInputFile(catalogLocation);
    final CatalogFile catalogFile = getCatalogFile(catalog);
    try {
      CatalogFile.from(catalogFile).dropNamespace(namespace).commit(fileIO.newOutputFile(catalog));
    } catch (NoSuchNamespaceException e) {
      // sigh.
      return false;
    }
    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    final InputFile catalog = fileIO.newInputFile(catalogLocation);
    final CatalogFile catalogFile = getCatalogFile(catalog);
    try {
      CatalogFile.from(catalogFile)
          .updateProperties(namespace, properties)
          .commit(fileIO.newOutputFile(catalog));
    } catch (CommitFailedException e) {
      return false; // sigh.
    }
    return false;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    CatalogFile catalogFile = getCatalogFile();
    if (catalogFile.namespaces().contains(namespace)) {
      return setProperties(
          namespace,
          properties.stream().collect(Maps::newHashMap, (m, k) -> m.put(k, null), Map::putAll));
    } else {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
  }

  private static CatalogFile getCatalogFile(InputFile catalogLocation) {
    try (InputStream in = catalogLocation.newStream()) {
      return CatalogFile.read(in);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static class FileIOTableOperations extends BaseMetastoreTableOperations {
    private final String catalogLocation;
    private final TableIdentifier tableId;
    private final SupportsAtomicOperations fileIO;

    private volatile int catalogVersion = -1;

    FileIOTableOperations(
        TableIdentifier tableId, String catalogLocation, SupportsAtomicOperations fileIO) {
      this.fileIO = fileIO;
      this.tableId = tableId;
      this.catalogLocation = catalogLocation;
    }

    @Override
    public SupportsAtomicOperations io() {
      return fileIO;
    }

    // version 0 reserved for empty catalog; tables created in subsequent commits
    protected synchronized void updateVersionAndMetadata(int newVersion, String metadataFile) {
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
      catalogVersion = newVersion;
      refreshFromMetadataLocation(metadataFile);
    }

    @Override
    protected String tableName() {
      return tableId.toString();
    }

    @Override
    protected void doRefresh() {
      try (FileIO io = io()) {
        CatalogFile catalogFile = getCatalogFile(io.newInputFile(catalogLocation));
        updateVersionAndMetadata(catalogFile.version(tableId), catalogFile.location(tableId));
      }
    }

    @Override
    public void doCommit(TableMetadata base, TableMetadata metadata) {
      // TODO: if base == null, table is being relocated? created?
      final boolean isCreate = null == base;
      final String newMetadataLocation = writeNewMetadataIfRequired(isCreate, metadata);
      try (SupportsAtomicOperations io = io()) {
        final InputFile catalog = io.newInputFile(catalogLocation);
        final CatalogFile catalogFile = getCatalogFile(catalog);
        if (null == base) {
          CatalogFile.from(catalogFile)
              .createTable(tableId, newMetadataLocation)
              .commit(fileIO.newOutputFile(catalog));
        } else {
          CatalogFile.from(catalogFile)
              .updateTable(tableId, newMetadataLocation)
              .commit(fileIO.newOutputFile(catalog));
        }
      }
    }
  }
}
