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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class CatalogFile {
  // TODO use serialization idioms from the project, handle evolution, etc.

  private int nextCommit; // or retain deleted TableIdentifiers unless/until not the max
  // TODO handle empty namespaces
  private final Map<TableIdentifier, TableInfo> fqti; // fully qualified table identifiers

  static class TableInfo {
    private final int version;
    private final String location;
    private final Map<String, String> metadata;

    TableInfo(int version, String location) {
      this.version = version;
      this.location = location;
      this.metadata = new LinkedHashMap<>();
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      TableInfo that = (TableInfo) other;
      return version == that.version
          && location.equals(that.location)
          && metadata.equals(that.metadata);
    }

    @Override
    public int hashCode() {
      return 31 * version + location.hashCode();
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
    return info != null ? info.location : null;
  }

  public int version(TableIdentifier table) {
    final TableInfo info = fqti.get(table);
    return info != null ? info.version : -1;
  }

  public String metadata(TableIdentifier table, String key) {
    final TableInfo info = fqti.get(table);
    return info != null ? info.metadata.get(key) : null;
  }

  public List<TableIdentifier> tables() {
    return Lists.newArrayList(fqti.keySet().iterator());
  }

  public boolean add(TableIdentifier table, String location) {
    // Bad API. Create a Builder?
    return null == fqti.putIfAbsent(table, new TableInfo(nextCommit, location));
  }

  public int nextCommit() {
    return nextCommit;
  }

  /**
   * Remove the table from the catalog file.
   *
   * @return the location of the table, or null if the table is not found
   */
  public String drop(TableIdentifier table) {
    TableInfo info = fqti.get(table);
    return null == info ? null : info.location;
  }

  Map<TableIdentifier, TableInfo> fqti() {
    return fqti;
  }

  // TODO replace this w/ static init, tracking changes within the instance
  // TODO tracking changes should preserve which tables need to be updated with nextCommit, req
  // update on conflict
  public void read(InputStream in) {
    fqti.clear();
    try (DataInputStream din = new DataInputStream(in)) {
      int size = din.readInt();
      for (int i = 0; i < size; i++) {
        int version = din.readInt();
        int nlen = din.readInt();
        String[] levels = new String[nlen];
        for (int j = 0; j < nlen; j++) {
          levels[j] = din.readUTF();
        }
        Namespace namespace = Namespace.of(levels);
        TableIdentifier tid = TableIdentifier.of(namespace, din.readUTF());
        fqti.put(tid, new TableInfo(version, din.readUTF()));
      }
      nextCommit = din.readInt() + 1;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public int write(OutputStream out) {
    // TODO: delete this method
    return write(out, nextCommit);
  }

  public int write(OutputStream out, int commit) {
    try (DataOutputStream dos = new DataOutputStream(out)) {
      dos.writeInt(fqti.size());
      for (Map.Entry<TableIdentifier, TableInfo> e : fqti.entrySet()) {
        TableInfo info = e.getValue();
        dos.writeInt(info.version);
        TableIdentifier tid = e.getKey();
        Namespace namespace = tid.namespace();
        dos.writeInt(namespace.length());
        for (String n : namespace.levels()) {
          dos.writeUTF(n);
        }
        dos.writeUTF(tid.name());
        dos.writeUTF(info.location);
      }
      dos.writeInt(commit);
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
}
