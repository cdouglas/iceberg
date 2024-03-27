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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class CatalogFile {
  // TODO use serialization idioms from the project, handle evolution, etc.

  private int lastCommit;   // or retain deleted TableIdentifiers unless/until not the max
  private long tblChecksum; // checksum of the table identifiers
  private int[] versions;   // versions of the catalog file
  // TODO handle empty namespaces
  private final Map<TableIdentifier, TableInfo> fqti; // fully qualified table identifiers

  static class TableInfo {
    private final int version;
    private final String location;

    TableInfo(int version, String location) {
      this.version = version;
      this.location = location;
    }
  }

  public CatalogFile() {
    // consistent iteration order
    this(new LinkedHashMap<>());
  }

  @VisibleForTesting
  CatalogFile(Map<TableIdentifier, TableInfo> fqti) {
    this.fqti = fqti;
  }

  public String location(TableIdentifier table) {
    final TableInfo info = fqti.get(table);
    if (info != null) {
      return info.location;
    }
    return null;
  }

  public int version(TableIdentifier table) {
    final TableInfo info = fqti.get(table);
    if (info != null) {
      return info.version;
    }
    return -1;
  }

  public List<TableIdentifier> tables() {
    return Lists.newArrayList(fqti.keySet().iterator());
  }

  public boolean add(TableIdentifier table, String location) {
    // Bad API. Create a Builder?
    return null == fqti.putIfAbsent(table, location);
  }

  /**
   * Remove the table from the catalog file.
   *
   * @return the location of the table, or null if the table is not found
   */
  public String drop(TableIdentifier table) {
    return fqti.remove(table);
  }

  @VisibleForTesting
  Map<TableIdentifier, String> fqti() {
    return fqti;
  }

  // table CRC
  // n tables
  //   table version
  //   table namespace
  //   table name
  //   location

  // TODO replace this w/ static init and immutable CatalogFile instances
  public void read(InputStream in) {
    fqti.clear();
    try (DataInputStream din = new DataInputStream(in)) {
      tblChecksum = din.readLong();
      int size = din.readInt();
      versions = new int[size];
      for (int i = 0; i < size; i++) {
        int nlen = din.readInt();
        String[] levels = new String[nlen];
        versions[i] = din.readInt();
        for (int j = 0; j < nlen; j++) {
          levels[j] = din.readUTF();
        }
        Namespace namespace = Namespace.of(levels);
        TableIdentifier tid = TableIdentifier.of(namespace, din.readUTF());
        fqti.put(tid, new TableInfo(versions[i], in.readUTF());
      }
      lastCommit = din.readInt();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public int write(OutputStream out) {
    // versions
    try (DataOutputStream dos = new DataOutputStream(out)) {
      final long chksum = tableChecksum(fqti.keySet());
      dos.writeLong(chksum);
      dos.writeInt(fqti.size());
      int ordinal = 0;
      for (Map.Entry<TableIdentifier, String> e : fqti.entrySet()) {
        dos.writeInt(versions[ordinal++]);
        TableIdentifier tid = e.getKey();
        Namespace namespace = tid.namespace();
        dos.writeInt(namespace.length());
        for (String n : namespace.levels()) {
          dos.writeUTF(n);
        }
        dos.writeUTF(tid.name());
        dos.writeUTF(e.getValue()); // location
      }
      dos.writeInt(lastCommit);
      return dos.size();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    CatalogFile that = (CatalogFile) other;
    return fqti.equals(that.fqti());
  }

  @Override
  public int hashCode() {
    return fqti.hashCode();
  }

  private static long tableChecksum(Iterable<TableIdentifier> fqti) {
    final CRC32 chksum = new CRC32();
    for (TableIdentifier tid : fqti) {
      for (String n : tid.namespace().levels()) {
        final byte[] nsbytes = n.getBytes();
        chksum.update(nsbytes, 0, nsbytes.length);
      }
      final byte[] nameBytes = tid.name().getBytes();
      chksum.update(nameBytes, 0, nameBytes.length);
    }
    return chksum.getValue();
  }

}
