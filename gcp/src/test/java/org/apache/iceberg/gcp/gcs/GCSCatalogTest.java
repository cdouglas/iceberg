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
package org.apache.iceberg.gcp.gcs;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIOCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class GCSCatalogTest extends CatalogTests<FileIOCatalog> {
  private static final String TEST_BUCKET = "TEST_BUCKET";

  private final Storage storage = LocalStorageHelper.getOptions().getService();
  private FileIOCatalog catalog;

  @BeforeEach
  public void before(TestInfo info) {
    // XXX don't call io.initialize(), as it will overwrite this config
    GCSFileIO io = new GCSFileIO(() -> storage, new GCPProperties());

    final String testName = info.getTestMethod().orElseThrow(RuntimeException::new).getName();
    final String warehouseLocation = "gs://" + TEST_BUCKET + "/" + testName;
    final Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
    final String location = warehouseLocation + "/catalog";
    catalog = new FileIOCatalog("test", location, null, io, Maps.newHashMap());
    catalog.initialize(testName, properties);
    this.tableLocation = "gs://bucket/blob";
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    // TODO fix
    return true;
  }

  @Override
  protected FileIOCatalog catalog() {
    return catalog;
  }

  @Test
  public void testBasicFunctionality() {
    FileIOCatalog underTest = catalog();
    underTest.createNamespace(Namespace.of("ns1"));
  }

  @Override
  @Disabled
  public void testListTables() {}

  // Tests to Ignore: testConcurrentReplaceTransactionSchemaConflict

}
