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
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class FileIOCatalog extends BaseMetastoreCatalog implements Configurable, SupportsNamespaces {
  // TODO SupportsNamespaces
  // TODO audit loadTable in BaseMetastoreCatalog
  // TODO buildTable overridden in BaseMetastoreCatalog?

  private Configuration conf;
  private SupportsAtomicOperations fileIO;
  private String location;

  public FileIOCatalog() {
    this(false);
  }

  public FileIOCatalog(boolean createCatalog) {
    if (createCatalog) {
      throw new UnsupportedOperationException("TODO");
    }
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
    String fileIOImpl =
        properties.getOrDefault(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.hadoop.HadoopFileIO");

    // TODO handle this more gracefully
    this.fileIO = (SupportsAtomicOperations) CatalogUtil.loadFileIO(fileIOImpl, properties, conf);
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
    // TODO extend HadoopTableOperations, as most should be shared
    return null;
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

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {

  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    return null;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    return null;
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    return false;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws NoSuchNamespaceException {
    return false;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
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

  static class FileIOTableOperations extends HadoopTableOperations {
    private final TableIdentifier tableId;
    private final String tblLocation;

    FileIOTableOperations(TableIdentifier tableId, String tblLocation, FileIO fileIO, Configuration conf) {
      super(null, fileIO, conf, new NullLockManager());
      this.tblLocation = tblLocation;
      this.tableId = tableId;
    }

    @Override
    public TableMetadata refresh() {
      try (FileIO io = io()) {
        CatalogFile catalogFile = getCatalogFile(io.newInputFile(tblLocation));
        updateVersionAndMetadata(catalogFile.version(tableId), catalogFile.location(tableId));
      }
      return current();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      try (FileIO io = io()) {
      }
    }
  }

  private static class NullLockManager implements LockManager {
    @Override
    public boolean acquire(String entityId, String ownerId) {
      return true;
    }

    @Override
    public boolean release(String entityId, String ownerId) {
      return true;
    }

    @Override
    public void initialize(Map<String, String> properties) {
      // ignore
    }

    @Override
    public void close() throws Exception {
      // ignore
    }
  }

}
