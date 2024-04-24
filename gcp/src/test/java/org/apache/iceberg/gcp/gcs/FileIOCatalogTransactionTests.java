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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.CatalogTransactionTests;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIOCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileIOCatalogTransactionTests extends CatalogTransactionTests<FileIOCatalog> {
  private static final String TEST_BUCKET = "lst-consistency/TEST_BUCKET";
  private static final Logger LOG = LoggerFactory.getLogger(GCSCatalogTest.class);

  // private final Storage storage =
  private static Storage storage;
  private FileIOCatalog catalog;
  private static String warehouseLocation;
  private static String uniqTestRun;

  // Don't keep artifacts from successful tests
  static class SuccessCleanupExtension implements TestWatcher {
    @Override
    public void testSuccessful(ExtensionContext ctxt) {
      cleanupWarehouseLocation();
    }
  }

  static void cleanupWarehouseLocation() {
    try (GCSFileIO io = new GCSFileIO(() -> storage, new GCPProperties())) {
      if (io.listPrefix(warehouseLocation).iterator().hasNext()) {
        io.deletePrefix(warehouseLocation);
      }
    }
  }

  @BeforeAll
  public static void initStorage() throws IOException {
    uniqTestRun = RandomStringUtils.randomAlphabetic(8);
    LOG.info("TEST RUN: " + uniqTestRun);
    // TODO get from env
    final File credFile =
        new File("/IdeaProjects/iceberg/.secret/lst-consistency-8dd2dfbea73a.json");
    if (credFile.exists()) {
      try (FileInputStream creds = new FileInputStream(credFile)) {
        storage = RemoteStorageHelper.create("lst-consistency", creds).getOptions().getService();
        LOG.info("Using remote storage");
      }
    } else {
      storage = spy(LocalStorageHelper.getOptions().getService());
      doAnswer(
              invoke -> {
                Iterable<BlobId> iter = invoke.getArgument(0);
                List<Boolean> answer = Lists.newArrayList();
                iter.forEach(
                    blobId -> {
                      answer.add(storage.delete(blobId));
                    });
                return answer;
              })
          .when(storage)
          .delete(any(Iterable.class));
      LOG.info("Using local storage");
    }
    // show ridiculous stack traces
    Assertions.setMaxStackTraceElementsDisplayed(Integer.MAX_VALUE);
  }

  @BeforeEach
  public void before(TestInfo info) {
    // XXX don't call io.initialize(), as it will overwrite this config
    GCSFileIO io = new GCSFileIO(() -> storage, new GCPProperties());

    final String testName = info.getTestMethod().orElseThrow(RuntimeException::new).getName();
    warehouseLocation = "gs://" + TEST_BUCKET + "/" + uniqTestRun + "/" + testName;
    cleanupWarehouseLocation();
    final Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
    final String location = warehouseLocation + "/catalog";
    catalog = new FileIOCatalog("test", location, null, io, Maps.newHashMap());
    catalog.initialize(testName, properties);
  }

  @Override
  protected FileIOCatalog catalog() {
    return catalog;
  }
}
