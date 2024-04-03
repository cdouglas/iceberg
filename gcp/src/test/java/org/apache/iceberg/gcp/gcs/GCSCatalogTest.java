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

import static org.mockito.Mockito.spy;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.util.Random;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIOCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;

public class GCSCatalogTest extends CatalogTests<FileIOCatalog> {
  private static final String TEST_BUCKET = "TEST_BUCKET";
  private final Random random = new Random(1);

  private final Storage storage = spy(LocalStorageHelper.getOptions().getService());
  private GCSFileIO io;
  private FileIOCatalog catalog;

  @BeforeEach
  public void before() {
    io = new GCSFileIO(() -> storage, new GCPProperties());
    // testname
    final String location = String.format("gs://%s/path/to/file.txt", TEST_BUCKET);
    catalog = new FileIOCatalog("test", location, null, io, Maps.newHashMap());
  }

  @Override
  protected FileIOCatalog catalog() {
    return catalog;
  }

  @Override
  protected boolean supportsNamespaceProperties() {
    return false;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return false;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return false;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return false;
  }

  @Override
  protected boolean overridesRequestedLocation() {
    return false;
  }

  @Override
  protected boolean supportsNamesWithSlashes() {
    return true;
  }

  @Override
  protected boolean supportsNamesWithDot() {
    return true;
  }
}
