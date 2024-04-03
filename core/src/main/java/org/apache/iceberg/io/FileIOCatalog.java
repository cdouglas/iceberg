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
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LocationUtil;

public class FileIOCatalog extends BaseMetastoreCatalog
    implements Configurable, SupportsNamespaces {
  // TODO SupportsNamespaces
  // TODO audit loadTable in BaseMetastoreCatalog
  // TODO buildTable overridden in BaseMetastoreCatalog?

  private String catalogName = "fileio";
  private String location;
  private Configuration conf;
  private SupportsAtomicOperations fileIO;
  private Map<String, String> catalogProperties;

  public FileIOCatalog() {
    // XXX Isn't using Maps.newHashMap() deprecated after Java7?
    catalogProperties = Maps.newHashMap();
  }

  public FileIOCatalog(
      String catalogName,
      String location,
      Configuration conf,
      SupportsAtomicOperations fileIO,
      Map<String, String> catalogProperties) {
    this.catalogName = catalogName;
    this.location = location;
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
    this.catalogProperties.putAll(properties);
    String uri = properties.get(CatalogProperties.URI);

    if (name != null) {
      catalogName = name;
    }

    String inputWarehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(inputWarehouseLocation),
        "Cannot initialize FileIOCatalog because warehousePath must not be null or empty");
    this.location = LocationUtil.stripTrailingSlash(inputWarehouseLocation);
    String fileIOImpl =
        properties.getOrDefault(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.hadoop.HadoopFileIO");

    // TODO handle this more gracefully
    this.fileIO =
        (SupportsAtomicOperations) CatalogUtil.loadFileIO(fileIOImpl, properties, getConf());
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    Preconditions.checkArgument(Namespace.empty().equals(namespace), "Namespaces not supported");
    return readCatalogFileFromStorage().tables();
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    final InputFile catalog = fileIO.newInputFile(location);
    final CatalogFile catalogFile = getCatalogFile(catalog);
    // TODO retry in a loop
    final String tableLocation = catalogFile.drop(identifier);
    if (null == tableLocation) {
      return false;
    }
    try (OutputStream out = fileIO.newOutputFile(catalog).createOrOverwrite()) {
      catalogFile.write(out);
      return true;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    final InputFile catalog = fileIO.newInputFile(location);
    final CatalogFile catalogFile = getCatalogFile(catalog);
    final String tableLocation = catalogFile.drop(from);
    if (null == tableLocation) {
      throw new NoSuchTableException("Table not found: %s", from);
    }
    if (!catalogFile.add(to, tableLocation)) {
      throw new AlreadyExistsException("Table already exists: %s", to);
    }
    // TODO retry in a loop
    try (OutputStream out = fileIO.newOutputFile(catalog).createOrOverwrite()) {
      catalogFile.write(out);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new FileIOTableOperations(tableIdentifier, location, fileIO);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    // TODO undocumented semantics, but seems to be joining on a namespace
    throw new UnsupportedOperationException("Unsupported operation: defaultWarehouseLocation");
  }

  private CatalogFile readCatalogFileFromStorage() {
    final InputFile catalog = fileIO.newInputFile(location);
    return getCatalogFile(catalog);
  }

  //
  // SupportsNamespaces
  //

  // TODO: add namespaces (including empty tables)
  // TODO: add namespace metadata (namespace -> Map<String,String>)

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {}

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    return null;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    return null;
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    return false;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    return false;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    return false;
  }

  private static CatalogFile getCatalogFile(InputFile catalogLocation) {
    final CatalogFile catalogFile = new CatalogFile();
    try (InputStream in = catalogLocation.newStream()) {
      catalogFile.read(in);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return catalogFile;
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
        try (OutputStream out = io.newOutputFile(catalog).createOrOverwrite()) {
          boolean exists = catalogFile.add(tableId, newMetadataLocation);
          if (isCreate && exists) {
            throw new AlreadyExistsException("Table already exists: %s", tableId);
          } else if (!isCreate && !exists) {
            throw new NoSuchTableException("Table not found: %s", tableId);
          }
          // additional validation?
          catalogFile.write(out);
        } catch (IOException e) {
          throw new RuntimeIOException(e);
        }
      }
    }
  }
}
