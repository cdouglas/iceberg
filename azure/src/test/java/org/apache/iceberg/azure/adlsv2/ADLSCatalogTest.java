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
package org.apache.iceberg.azure.adlsv2;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.io.FileIOCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(ADLSCatalogTest.SuccessCleanupExtension.class)
public class ADLSCatalogTest extends CatalogTests<FileIOCatalog> {
  private static final String TEST_BUCKET = "lst-consistency/TEST_BUCKET";
  private static final Logger LOG = LoggerFactory.getLogger(ADLSCatalogTest.class);
  protected static final AzuriteContainer AZURITE_CONTAINER = new AzuriteContainer();

  private FileIOCatalog catalog;
  private static String warehouseLocation;
  private static String uniqTestRun;

  // copied from BaseAzuriteTest (needs to extend CatalogTests)
  @BeforeAll
  public static void beforeAll() {
    AZURITE_CONTAINER.start();
    uniqTestRun = UUID.randomUUID().toString();
    LOG.info("TEST RUN: {}", uniqTestRun);
    // show ridiculous stack traces
    // Assertions.setMaxStackTraceElementsDisplayed(Integer.MAX_VALUE);
  }

  @AfterAll
  public static void afterAll() {
    AZURITE_CONTAINER.stop();
  }

  @AfterEach
  public void baseAfter() {
    AZURITE_CONTAINER.deleteStorageContainer();
  }

  protected ADLSFileIO createFileIO() {
    final AzureProperties azureProps;
    if (true) {
      azureProps = spy(new AzureProperties());
      doAnswer(
              invoke -> {
                DataLakeFileSystemClientBuilder clientBuilder = invoke.getArgument(1);
                clientBuilder.endpoint(AZURITE_CONTAINER.endpoint());
                clientBuilder.credential(AZURITE_CONTAINER.credential());
                return null;
              })
          .when(azureProps)
          .applyClientConfiguration(any(), any());
    }
    return new ADLSFileIO(azureProps);
  }

  // Don't keep artifacts from successful tests
  static class SuccessCleanupExtension implements TestWatcher {
    @Override
    public void testSuccessful(ExtensionContext ctxt) {
      cleanupWarehouseLocation();
    }
  }

  static void cleanupWarehouseLocation() {
    // TODO: remove test data if test passed
  }

  @Test
  public void catalogFileTest() throws IOException {
    catalog.createNamespace(Namespace.of("dingos"), new HashMap<>());
  }

  @BeforeEach
  public void before(TestInfo info) throws IOException {
    AZURITE_CONTAINER.createStorageContainer();
    ADLSFileIO io = createFileIO();

    final String testName = info.getTestMethod().orElseThrow(RuntimeException::new).getName();
    warehouseLocation =
        AZURITE_CONTAINER.location(TEST_BUCKET + "/" + uniqTestRun + "/" + testName);
    cleanupWarehouseLocation();

    final Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
    final String location = warehouseLocation + "/catalog";
    catalog = new FileIOCatalog("test", location, null, io, Maps.newHashMap());
    catalog.initialize(testName, properties);
  }

  @Override
  protected String cannedTableLocation() {
    return warehouseLocation + "/tmp/ns/table";
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsConcurrentCreate() {
    return false;
  }

  @Override
  protected FileIOCatalog catalog() {
    return catalog;
  }
}
