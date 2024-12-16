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

import static org.assertj.core.api.Assertions.setMaxStackTraceElementsDisplayed;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.catalog.CatalogTransactionTests;
import org.apache.iceberg.io.FileIOCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(ADLSCatalogTest.SuccessCleanupExtension.class)
public class ADLSFileIOCatalogTransactionTests extends CatalogTransactionTests<FileIOCatalog> {
  private static final String TEST_BUCKET = "lst-consistency/TEST_BUCKET";
  private static final Logger LOG = LoggerFactory.getLogger(ADLSCatalogTest.class);
  protected static AzuriteContainer azuriteContainer = null;

  private static Map<String, String> azureProperties;
  private FileIOCatalog catalog;
  private static String warehouseLocation;
  private static String uniqTestRun;
  private static LocationResolver az;

  @BeforeAll
  public static void initStorage() throws IOException {
    uniqTestRun = UUID.randomUUID().toString();
    LOG.info("TEST RUN: {}", uniqTestRun);
    AzureSAS creds =
        AzureSAS.readCreds(new File("/home/chris/work/.cloud/azure/lstnsgym-20250228.json"));
    if (creds != null) {
      azureProperties = Maps.newHashMap();
      azureProperties.put(
          AzureProperties.ADLS_SAS_TOKEN_PREFIX + "lstnsgym.dfs.core.windows.net", creds.sasToken);
      az = new AzureSAS.SasResolver(creds);
      LOG.info("Using remote storage");
    } else {
      azuriteContainer = new AzuriteContainer();
      azuriteContainer.start();
      az = azuriteContainer;
      LOG.info("Using local storage");
    }
    // show ridiculous stack traces
    setMaxStackTraceElementsDisplayed(Integer.MAX_VALUE);
  }

  @BeforeEach
  public void before(TestInfo info) {
    final ADLSFileIO io;
    if (null == azureProperties) {
      azuriteContainer.createStorageContainer();
      final AzureProperties azureProps = spy(new AzureProperties());
      doAnswer(
              invoke -> {
                DataLakeFileSystemClientBuilder clientBuilder = invoke.getArgument(1);
                clientBuilder.endpoint(azuriteContainer.endpoint());
                clientBuilder.credential(azuriteContainer.credential());
                return null;
              })
          .when(azureProps)
          .applyClientConfiguration(any(), any());
      io = new ADLSFileIO(azureProps);
    } else {
      io = new ADLSFileIO(azureProperties);
    }

    final String testName = info.getTestMethod().orElseThrow(RuntimeException::new).getName();
    warehouseLocation =
        az.location(TEST_BUCKET + "/" + uniqTestRun + "/" + testName + "_" + info.getDisplayName());
    cleanupWarehouseLocation();

    final Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
    final String location = warehouseLocation + "/catalog";
    catalog = new FileIOCatalog("test", location, null, io, Maps.newHashMap());
    catalog.initialize(testName, properties);
  }

  @AfterAll
  public static void afterAll() {
    if (azuriteContainer != null) {
      azuriteContainer.stop();
    }
  }

  @AfterEach
  public void baseAfter() {
    if (azuriteContainer != null) {
      azuriteContainer.deleteStorageContainer();
    }
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

  @Override
  protected FileIOCatalog catalog() {
    return catalog;
  }
}
