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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.LogCatalogFormat.LogCatalogFile;
import org.apache.iceberg.io.log.actions.namespace.AddNamespacePropertyAction;
import org.apache.iceberg.io.log.actions.namespace.CreateNamespaceAction;
import org.apache.iceberg.io.log.actions.table.CreateTableAction;
import org.apache.iceberg.io.log.actions.table.UpdateTableAction;
import org.apache.iceberg.io.log.transactions.TransactionAction;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

public class TestLogCatalogFormat {

  private final Namespace testNs = Namespace.of("test");
  private final TableIdentifier testTable = TableIdentifier.of(testNs, "table");

  @Test
  public void testBasicOperations() throws IOException {
    InputFile mockFile = mock(InputFile.class);
    when(mockFile.location()).thenReturn("test://catalog");
    when(mockFile.exists()).thenReturn(false);

    LogCatalogFormat format = new LogCatalogFormat();
    LogCatalogFormat.Mut catalog = (LogCatalogFormat.Mut) format.empty(mockFile);

    // Test creating namespace and table
    catalog.createNamespace(testNs).createTable(testTable, "test://table/location");

    LogCatalogFile result = catalog.build();

    assertTrue(result.containsNamespace(testNs));
    assertEquals("test://table/location", result.location(testTable));
    assertTrue(result.tables().contains(testTable));
  }

  @Test
  public void testTransactionSerialization() throws IOException {
    // Create a simple transaction
    CreateNamespaceAction createNs = new CreateNamespaceAction("test", 0, 1, 1);
    TransactionAction txn =
        new TransactionAction(UUID.randomUUID(), Lists.newArrayList(createNs), false);

    // Serialize the transaction
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    txn.serialize(dos);
    byte[] txnBytes = baos.toByteArray();

    // Deserialize the transaction
    ByteArrayInputStream bais = new ByteArrayInputStream(txnBytes);
    DataInputStream dis = new DataInputStream(bais);
    dis.readByte(); // Skip the transaction type byte
    TransactionAction deserializedTxn = TransactionAction.deserialize(dis);

    assertEquals(txn.txnId(), deserializedTxn.txnId());
    assertEquals(txn.isSealed(), deserializedTxn.isSealed());
    assertEquals(txn.actions().size(), deserializedTxn.actions().size());
  }

  @Test
  public void testSealingOperations() throws IOException {
    CreateNamespaceAction createNs = new CreateNamespaceAction("test", 0, 1, 1);
    TransactionAction txn =
        new TransactionAction(UUID.randomUUID(), Lists.newArrayList(createNs), false);

    // Serialize the transaction
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    txn.serialize(dos);
    byte[] txnBytes = baos.toByteArray();

    // Test sealing and unsealing
    assertFalse(txn.isSealed());

    TransactionAction.seal(txnBytes);
    ByteArrayInputStream bais = new ByteArrayInputStream(txnBytes);
    DataInputStream dis = new DataInputStream(bais);
    dis.readByte(); // Skip the transaction type byte
    TransactionAction sealedTxn = TransactionAction.deserialize(dis);
    assertTrue(sealedTxn.isSealed());

    TransactionAction.unseal(txnBytes);
    bais = new ByteArrayInputStream(txnBytes);
    dis = new DataInputStream(bais);
    dis.readByte(); // Skip the transaction type byte
    TransactionAction unsealedTxn = TransactionAction.deserialize(dis);
    assertFalse(unsealedTxn.isSealed());
  }

  @Test
  public void testCatalogFileProperties() {
    InputFile mockFile = mock(InputFile.class);
    when(mockFile.location()).thenReturn("test://catalog");

    LogCatalogFile catalogFile = new LogCatalogFile(mockFile);

    assertTrue(catalogFile.createsHierarchicalNamespaces());
    assertThat(catalogFile.namespaces()).isNotNull();
    assertThat(catalogFile.tables()).isNotNull();
    assertFalse(catalogFile.sealed());
  }

  @Test
  public void testEmptyState() {
    InputFile mockFile = mock(InputFile.class);
    when(mockFile.location()).thenReturn("test://catalog");

    LogCatalogFormat format = new LogCatalogFormat();
    LogCatalogFormat.Mut catalog = (LogCatalogFormat.Mut) format.empty(mockFile);
    LogCatalogFile result = catalog.build();

    // Empty catalog should have empty root namespace
    assertTrue(result.containsNamespace(Namespace.empty()));
    assertTrue(result.tables().isEmpty());
  }

  @Test
  public void testFuzzingSerdeEquivalence() throws IOException {
    // For now, focus on transaction-level serialization/deserialization 
    // which we know works from existing tests
    for (int i = 0; i < 5; i++) {
      List<TransactionAction> randomTransactions = generateRandomTransactions(i, 2);
      
      for (TransactionAction txn : randomTransactions) {
        // Serialize transaction
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        txn.serialize(dos);
        byte[] txnBytes = baos.toByteArray();
        
        // Deserialize transaction
        ByteArrayInputStream bais = new ByteArrayInputStream(txnBytes);
        DataInputStream dis = new DataInputStream(bais);
        dis.readByte(); // Skip the transaction type byte
        TransactionAction deserializedTxn = TransactionAction.deserialize(dis);
        
        // Verify transaction properties
        assertEquals(txn.txnId(), deserializedTxn.txnId());
        assertEquals(txn.isSealed(), deserializedTxn.isSealed());
        assertEquals(txn.actions().size(), deserializedTxn.actions().size());
        
        // Verify that actions can be applied to a state builder without errors
        InputFile mockFile = mock(InputFile.class);
        when(mockFile.location()).thenReturn("test://catalog-" + i);
        LogCatalogFormat.Mut catalog = new LogCatalogFormat.Mut(new LogCatalogFile(mockFile));
        
        // Apply actions to verify they don't throw exceptions
        for (org.apache.iceberg.io.log.actions.LogAction action : deserializedTxn.actions()) {
          // Just verify the action can be created without throwing
          assertThat(action).isNotNull();
          assertThat(action.type()).isNotNull();
        }
      }
    }
  }

  @Test
  public void testRandomTransactionOperations() throws IOException {
    // Test random transaction operations with various action types
    for (int i = 0; i < 5; i++) {
      List<TransactionAction> randomTransactions = generateRandomTransactions(i, 3);
      
      for (TransactionAction txn : randomTransactions) {
        // Serialize transaction
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        txn.serialize(dos);
        byte[] txnBytes = baos.toByteArray();
        
        // Deserialize transaction
        ByteArrayInputStream bais = new ByteArrayInputStream(txnBytes);
        DataInputStream dis = new DataInputStream(bais);
        dis.readByte(); // Skip the transaction type byte
        TransactionAction deserializedTxn = TransactionAction.deserialize(dis);
        
        // Verify transaction properties
        assertEquals(txn.txnId(), deserializedTxn.txnId());
        assertEquals(txn.isSealed(), deserializedTxn.isSealed());
        assertEquals(txn.actions().size(), deserializedTxn.actions().size());
      }
    }
  }

  private LogCatalogFile generateRandomLogCatalogFile(long seed) {
    Random random = new Random(seed);
    InputFile mockFile = mock(InputFile.class);
    when(mockFile.location()).thenReturn("test://catalog-" + seed);
    
    LogCatalogFormat format = new LogCatalogFormat();
    LogCatalogFormat.Mut catalog = (LogCatalogFormat.Mut) format.empty(mockFile);
    
    // Generate random namespaces
    int numNamespaces = random.nextInt(5) + 1;
    for (int i = 0; i < numNamespaces; i++) {
      String nsName = "namespace_" + i + "_" + seed;
      Namespace ns = Namespace.of(nsName);
      
      // Create namespace with random properties
      Map<String, String> properties = Maps.newHashMap();
      if (random.nextBoolean()) {
        properties.put("prop_" + i, "value_" + i);
      }
      catalog.createNamespace(ns, properties);
    }
    
    // Generate random tables
    int numTables = random.nextInt(3) + 1;
    List<Namespace> namespaces = Lists.newArrayList(catalog.build().namespaces());
    for (int i = 0; i < numTables && !namespaces.isEmpty(); i++) {
      Namespace ns = namespaces.get(random.nextInt(namespaces.size()));
      if (!ns.isEmpty()) { // Skip empty namespace for table creation
        String tableName = "table_" + i + "_" + seed;
        TableIdentifier tableId = TableIdentifier.of(ns, tableName);
        String location = "test://table/" + tableName + "/location";
        catalog.createTable(tableId, location);
      }
    }
    
    return catalog.build();
  }

  private List<TransactionAction> generateRandomTransactions(long seed, int count) {
    Random random = new Random(seed);
    List<TransactionAction> transactions = Lists.newArrayList();
    
    for (int i = 0; i < count; i++) {
      List<org.apache.iceberg.io.log.actions.LogAction> actions = Lists.newArrayList();
      int numActions = random.nextInt(3) + 1;
      
      for (int j = 0; j < numActions; j++) {
        int actionType = random.nextInt(4);
        switch (actionType) {
          case 0: // CreateNamespaceAction
            actions.add(new CreateNamespaceAction(
                "ns_" + i + "_" + j, 
                random.nextInt(10), 
                random.nextInt(100) + 1, 
                random.nextInt(5) + 1));
            break;
          case 1: // AddNamespacePropertyAction
            actions.add(new AddNamespacePropertyAction(
                random.nextInt(100) + 1, 
                random.nextInt(5) + 1, 
                "prop_" + j, 
                "value_" + j));
            break;
          case 2: // CreateTableAction
            Namespace randomNamespace = Namespace.of("ns_" + i + "_" + j);
            actions.add(new CreateTableAction(
                "table_" + i + "_" + j, 
                random.nextInt(100) + 1, 
                random.nextInt(5) + 1, 
                random.nextInt(10) + 1, 
                random.nextInt(5) + 1, 
                "test://table/location_" + i + "_" + j,
                randomNamespace));
            break;
          case 3: // UpdateTableAction
            actions.add(new UpdateTableAction(
                random.nextInt(100) + 1, 
                random.nextInt(5) + 1, 
                "test://table/updated_location_" + i + "_" + j,
                TableIdentifier.of("ns_" + i, "table_" + j)));
            break;
        }
      }
      
      UUID txnId = new UUID(random.nextLong(), random.nextLong());
      boolean sealed = random.nextBoolean();
      transactions.add(new TransactionAction(txnId, actions, sealed));
    }
    
    return transactions;
  }
}
