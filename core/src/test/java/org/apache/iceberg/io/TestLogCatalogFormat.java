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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.LogCatalogRegionFormat.LogCatalogFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

public class TestLogCatalogFormat {

  private final Random random = new Random();

  @BeforeEach
    public void before(TestInfo info) {
        final String testName = info.getTestMethod().orElseThrow(RuntimeException::new).getName();
        random.setSeed(System.currentTimeMillis());
        System.out.println(testName + " seed: " + random.nextLong());
    }

  @Test
  public void testRegionFormat() {
    final InputFile mockFile = mock(InputFile.class);
    LogCatalogRegionFormat.LogCatalogFile catalog = generateRandomLogCatalogFile(random.nextLong());
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      byte[] catabytes = toBytes(catalog);
      //LogCatalogFile readCatalog = LogCatalogRegionFormat.readCatalogFile(catabytes, regionFormat);
    } catch (IOException e) {
      fail("Failed to write/read catalog file", e);
    }
  }

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
    assertInstanceOf(
        LogCatalogFormat.LogAction.CreateNamespace.class, readTransaction.actions.get(0));
    assertFalse(logStream.hasNext());
  }

  @Test
  public void testRandomFile() throws IOException {
    final long seed = random.nextLong();
    LogCatalogFile a = generateRandomLogCatalogFile(seed);
    LogCatalogFile b = generateRandomLogCatalogFile(seed);
    assertEquals(a, b);

    byte[] aBytes = toBytes(a);
    byte[] bBytes = toBytes(b);
    assertThat(aBytes.length).isGreaterThan(0);
    assertArrayEquals(aBytes, bBytes);

    InputFile mockFile = mock(InputFile.class);
    LogCatalogFormat.LogCatalogFileMut catalog = new LogCatalogFormat.LogCatalogFileMut(mockFile);
    try (ByteArrayInputStream bis = new ByteArrayInputStream(aBytes)) {
      LogCatalogRegionFormat.readCheckpoint(catalog, bis);
      LogCatalogFile c = catalog.merge();
      assertEquals(a, c);
    }
  }

  private LogCatalogFile generateRandomLogCatalogFile(long seed) {
    final Random rand = new Random(seed);
    rand.setSeed(seed);
    InputFile location = mock(InputFile.class);
    final long msb = rand.nextLong();
    final long lsb = rand.nextLong();
    final UUID uuid = new UUID(msb, lsb);
    final int nextNsid = rand.nextInt(20) + 10;
    final int nextTblid = rand.nextInt(100) + 10;

    final Map<Namespace, Integer> nsids = new HashMap<>();
    final Map<Integer, Integer> nsVersion = new HashMap<>();
    final Map<Integer, Map<String, String>> nsProperties = new HashMap<>();
    final Map<TableIdentifier, Integer> tblIds = new HashMap<>();
    final Map<Integer, Integer> tblVersion = new HashMap<>();
    final Map<Integer, String> tblLocations = new HashMap<>();

    final Map<Integer, Namespace> nsLookup = new HashMap<>();

    nsids.put(Namespace.empty(), 0);
    nsLookup.put(0, Namespace.empty());
    nsVersion.put(0, rand.nextInt(1) + 1);
    // ns version \geq #children + #prop
    for (int nsid = 1; nsid < nextNsid; nsid += rand.nextInt(5) + 1) {
      int parentId = rand.nextInt(nsid) + 1;
      final Namespace ns;
      final Namespace parentNs = nsLookup.get(parentId);
      if (null == parentNs) { // root ns
        parentId = 0;
        ns = Namespace.of("ns" + nsid);
      } else {
        String[] levels = Arrays.copyOf(parentNs.levels(), parentNs.levels().length + 1);
        levels[levels.length - 1] = "ns" + nsid;
        ns = Namespace.of(levels);
      }
      final int parentVersion = nsVersion.get(parentId) + rand.nextInt(2) + 1;
      nsVersion.put(nsid, rand.nextInt(parentVersion - 1) + 1);
      nsVersion.put(parentId, parentVersion);

      nsids.put(ns, nsid);
      nsLookup.put(nsid, ns);
      Map<String, String> properties = new HashMap<>();
      for (int j = 0; j < rand.nextInt(5); j++) {
        properties.put("key" + j, nsid + "value" + j);
      }
      if (!properties.isEmpty()) {
        nsProperties.put(nsid, properties);
      }
      nsVersion.put(nsid, nsVersion.get(nsid) + rand.nextInt(2) + 1);
    }

    List<Integer> namespaces = new ArrayList<>(nsids.values());
    for (int i = 0; i < nextTblid; i++) {
      final int nsid = namespaces.get(rand.nextInt(namespaces.size()));
      final Namespace namespace = nsLookup.get(nsid);
      TableIdentifier tblId = TableIdentifier.of(namespace, "tbl" + i);
      tblIds.put(tblId, i);
      tblVersion.put(i, rand.nextInt(10));
      tblLocations.put(i, "location" + i);
      nsVersion.put(nsid, nsVersion.get(nsid) + rand.nextInt(2) + 1);
    }

    return new LogCatalogRegionFormat.LogCatalogFile(location, uuid, nextNsid, nextTblid, nsids, nsVersion, nsProperties, tblIds, tblVersion, tblLocations);
  }

  static byte[] toBytes(LogCatalogFile catalog) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      EnumMap<LogCatalogRegionFormat.RegionType, LogCatalogRegionFormat.Format> regionFormat =
              new EnumMap<>(LogCatalogRegionFormat.RegionType.class);
      regionFormat.put(LogCatalogRegionFormat.RegionType.NS, LogCatalogRegionFormat.Format.LENGTH);
      regionFormat.put(LogCatalogRegionFormat.RegionType.NS_PROP, LogCatalogRegionFormat.Format.LENGTH);
      regionFormat.put(LogCatalogRegionFormat.RegionType.TABLE, LogCatalogRegionFormat.Format.LENGTH);
      IOUtils.copy(LogCatalogRegionFormat.writeCatalogFile(catalog, regionFormat).get(), bos);
      return bos.toByteArray();
    } catch (IOException e) {
      fail("Failed to write/read catalog file", e);
      throw new IllegalStateException("Failed to write/read catalog file", e);
    }
  }

}
