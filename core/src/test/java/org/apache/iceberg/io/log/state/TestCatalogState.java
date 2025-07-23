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
package org.apache.iceberg.io.log.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;

public class TestCatalogState {

  @Test
  public void testEmpty() {
    CatalogState state = CatalogState.empty();

    assertNull(state.catalogUuid());
    assertEquals(1, state.nextNsid());
    assertEquals(1, state.nextTblid());
    assertFalse(state.sealed());
    assertTrue(state.namespaces().contains(Namespace.empty()));
    assertTrue(state.tableRegistry().tables().isEmpty());
  }

  @Test
  public void testBuilder() {
    UUID catalogUuid = UUID.randomUUID();
    CatalogState.Builder builder = new CatalogState.Builder();

    builder.setGlobals(catalogUuid, 5, 10);
    builder.setSealed();

    UUID txnId = UUID.randomUUID();
    builder.addCommittedTransaction(txnId);

    CatalogState state = builder.build();

    assertEquals(catalogUuid, state.catalogUuid());
    assertEquals(5, state.nextNsid());
    assertEquals(10, state.nextTblid());
    assertTrue(state.sealed());
    assertTrue(state.containsTransaction(txnId));
  }

  @Test
  public void testBuilderFromExisting() {
    UUID catalogUuid = UUID.randomUUID();
    UUID txnId = UUID.randomUUID();

    CatalogState.Builder originalBuilder = new CatalogState.Builder();
    originalBuilder.setGlobals(catalogUuid, 5, 10);
    originalBuilder.addCommittedTransaction(txnId);
    CatalogState original = originalBuilder.build();

    CatalogState.Builder newBuilder = new CatalogState.Builder(original);
    UUID newTxnId = UUID.randomUUID();
    newBuilder.addCommittedTransaction(newTxnId);

    CatalogState updated = newBuilder.build();

    assertEquals(catalogUuid, updated.catalogUuid());
    assertEquals(5, updated.nextNsid());
    assertEquals(10, updated.nextTblid());
    assertTrue(updated.containsTransaction(txnId));
    assertTrue(updated.containsTransaction(newTxnId));
  }

  @Test
  public void testRemap() {
    CatalogState.Builder builder = new CatalogState.Builder();

    // Test remapping virtual namespace IDs
    int virtualId1 = -1;
    int virtualId2 = -2;

    int actualId1 = builder.remap(virtualId1);
    int actualId2 = builder.remap(virtualId2);

    assertTrue(actualId1 > 0);
    assertTrue(actualId2 > 0);
    assertTrue(actualId1 != actualId2);

    // Test getting remapped values
    assertEquals(actualId1, builder.getRemapped(virtualId1));
    assertEquals(actualId2, builder.getRemapped(virtualId2));

    // Test getting non-virtual IDs (should return as-is)
    assertEquals(5, builder.getRemapped(5));
  }

  @Test
  public void testNamespaceOperations() {
    CatalogState.Builder builder = new CatalogState.Builder();

    // Add a namespace
    builder.namespaceBuilder().addNamespace("test", 0, 1, 1);

    CatalogState state = builder.build();

    Namespace testNs = Namespace.of("test");
    assertTrue(state.containsNamespace(testNs));
    assertThat(state.namespaceProperties(testNs)).isEmpty();
    assertTrue(state.namespaces().contains(testNs));
  }

  @Test
  public void testTableOperations() {
    CatalogState.Builder builder = new CatalogState.Builder();

    // Add a namespace first
    builder.namespaceBuilder().addNamespace("test", 0, 1, 1);

    // Add a table
    TableIdentifier tableId = TableIdentifier.of("test", "table1");
    builder.tableBuilder().addTable(1, tableId, 1, "s3://bucket/table1");

    CatalogState state = builder.build();

    assertEquals("s3://bucket/table1", state.tableLocation(tableId));
    assertTrue(state.tableRegistry().tables().contains(tableId));
  }

  @Test
  public void testVersionOperations() {
    CatalogState.Builder builder = new CatalogState.Builder();

    // Add namespace and table
    builder.namespaceBuilder().addNamespace("test", 0, 1, 1);
    TableIdentifier tableId = TableIdentifier.of("test", "table1");
    builder.tableBuilder().addTable(1, tableId, 5, "s3://bucket/table1");

    CatalogState state = builder.build();

    assertEquals(Integer.valueOf(1), state.namespaceVersion(1));
    assertEquals(Integer.valueOf(5), state.tableVersion(1));
  }
}
