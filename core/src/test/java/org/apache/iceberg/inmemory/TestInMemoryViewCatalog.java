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
package org.apache.iceberg.inmemory;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewCatalogTests;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestInMemoryViewCatalog extends ViewCatalogTests<InMemoryCatalog> {
  private InMemoryCatalog catalog;

  @BeforeEach
  public void before() {
    this.catalog = new InMemoryCatalog();
    this.catalog.initialize(
        "in-memory-catalog",
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.VIEW_DEFAULT_PREFIX + "key1", "catalog-default-key1")
            .put(CatalogProperties.VIEW_DEFAULT_PREFIX + "key2", "catalog-default-key2")
            .put(CatalogProperties.VIEW_DEFAULT_PREFIX + "key3", "catalog-default-key3")
            .put(CatalogProperties.VIEW_OVERRIDE_PREFIX + "key3", "catalog-override-key3")
            .put(CatalogProperties.VIEW_OVERRIDE_PREFIX + "key4", "catalog-override-key4")
            .build());
  }

  @Override
  protected InMemoryCatalog catalog() {
    return catalog;
  }

  @Override
  protected Catalog tableCatalog() {
    return catalog;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsEmptyNamespace() {
    return true;
  }

  @Test
  public void updateViewLocationRetryWithConcurrentModification() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");
    catalog.createNamespace(identifier.namespace());
    catalog
        .buildView(identifier)
        .withSchema(new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())))
        .withDefaultNamespace(identifier.namespace())
        .withQuery("spark", "select * from ns.tbl")
        .create();

    // On the first commit attempt, another writer modifies the view
    ViewOperations ops = ((BaseView) catalog.loadView(identifier)).operations();
    ViewOperations concurrentOps =
        new ViewOperations() {
          private boolean firstAttempt = true;

          @Override
          public ViewMetadata current() {
            return ops.current();
          }

          @Override
          public ViewMetadata refresh() {
            return ops.refresh();
          }

          @Override
          public void commit(ViewMetadata base, ViewMetadata metadata) {
            if (firstAttempt) {
              firstAttempt = false;
              catalog.loadView(identifier).updateProperties().set("key", "value").commit();
            }

            ops.commit(base, metadata);
          }
        };

    String location = "file:///tmp/new-view-location";
    new BaseView(concurrentOps, identifier.name()).updateLocation().setLocation(location).commit();

    View view = catalog.loadView(identifier);
    assertThat(view.location()).isEqualTo(location);
    assertThat(view.properties()).containsEntry("key", "value");
  }
}
