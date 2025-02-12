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

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class TestLogCatalogFormat {

  @Test
  public void testLogStream() throws IOException {
    // Create a sample transaction to write to the stream
    UUID txnId = UUID.randomUUID();
    LogCatalogFormat.LogAction.CreateNamespace createNamespace =
        new LogCatalogFormat.LogAction.CreateNamespace("testNamespace", 0, 1);
    final List<LogCatalogFormat.LogAction> actions = new ArrayList<>();
    actions.add(createNamespace);
    LogCatalogFormat.LogAction.Transaction transaction =
        new LogCatalogFormat.LogAction.Transaction(txnId, actions, true);

    // Write the transaction to a byte array
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    transaction.write(dos);
    byte[] data = baos.toByteArray();

    // Read the transaction from the byte array using LogStream
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    DataInputStream dis = new DataInputStream(bais);
    LogCatalogFormat.LogAction.LogStream logStream = new LogCatalogFormat.LogAction.LogStream(dis);

    // Verify that the LogStream correctly reads the transaction
    assertTrue(logStream.hasNext());
    LogCatalogFormat.LogAction.Transaction readTransaction = logStream.next();
    assertEquals(txnId, readTransaction.txnId);
    assertTrue(readTransaction.sealed());
    assertEquals(1, readTransaction.actions.size());
    assertTrue(
        readTransaction.actions.get(0) instanceof LogCatalogFormat.LogAction.CreateNamespace);
    assertFalse(logStream.hasNext());
  }
}
