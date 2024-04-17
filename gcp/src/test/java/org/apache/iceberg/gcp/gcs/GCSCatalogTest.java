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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.List;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIOCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCSCatalogTest extends CatalogTests<FileIOCatalog> {
  private static final String TEST_BUCKET = "lst-consistency/TEST_BUCKET";
  private static final Logger LOG = LoggerFactory.getLogger(GCSCatalogTest.class);

  // private final Storage storage =
  private static Storage storage;
  private FileIOCatalog catalog;
  private String warehouseLocation;

  @BeforeAll
  public static void initStorage() throws IOException {
    // TODO get from env
    final File credFile =
        new File("/IdeaProjects/iceberg/.secret/lst-consistency-8dd2dfbea73a.json_local");
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
  }

  @BeforeEach
  public void before(TestInfo info) {
    // XXX don't call io.initialize(), as it will overwrite this config
    GCSFileIO io = new GCSFileIO(() -> storage, new GCPProperties());

    final String testName = info.getTestMethod().orElseThrow(RuntimeException::new).getName();
    warehouseLocation = "gs://" + TEST_BUCKET + "/" + testName;
    final Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
    final String location = warehouseLocation + "/catalog";
    catalog = new FileIOCatalog("test", location, null, io, Maps.newHashMap());
    catalog.initialize(testName, properties);
    if (io.newInputFile(warehouseLocation).exists()) {
      io.deletePrefix(warehouseLocation);
    }
  }

  @Override
  protected String cannedTableLocation() {
    return warehouseLocation + "/tmp/ns/table";
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return false;
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
  public void testListTables() {
    // XXX why is this test disabled?
  }

  @Override
  @Disabled
  public void testRenameTable() {
    // XXX why is this test disabled?
  }

  @Override
  protected boolean supportsNamespaceProperties() {
    return false;
  }
  // Tests to Ignore: testConcurrentReplaceTransactionSchemaConflict

}
