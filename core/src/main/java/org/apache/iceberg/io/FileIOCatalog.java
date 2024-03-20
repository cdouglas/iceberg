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

import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

public class FileIOCatalog extends BaseMetastoreCatalog implements Configurable {
  // TODO SupportsNamespaces
  // TODO audit loadTable in BaseMetastoreCatalog
  // TODO

  private Configuration conf;
  private FileIO fileIO;

  FileIOCatalog() {
    super();
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

    this.fileIO = CatalogUtil.loadFileIO(fileIOImpl, properties, conf);
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    // TODO read CatalogFile through FileIO
    // TODO filter by namespace... evnetually
    return null;
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    // TODO read CatalogFile through FileIO
    // TODO replace CatalogFile with tombstoned table
    return false;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    // TODO change tableName property; use internal UUID table identifier for transactions?
    throw new UnsupportedOperationException("Cannot rename tables");
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
}
