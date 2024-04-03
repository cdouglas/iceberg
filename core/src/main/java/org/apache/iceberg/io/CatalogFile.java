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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class CatalogFile {
  // TODO use serialization idioms from the project, handle evolution, etc.

  private final int nextCommit; // or retain deleted TableIdentifiers unless/until not the max
  private final UUID uuid;
  private final Map<TableIdentifier, TableInfo> fqti; // fully qualified table identifiers
  private final Map<Namespace, Map<String, String>> namespaces;

  static class TableInfo {
    private final int version;
    private final String location;

    TableInfo(int version, String location) {
      this.version = version;
      this.location = location;
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
      return version == that.version && location.equals(that.location);
    }

    @Override
    public int hashCode() {
      return 31 * version + location.hashCode();
    }
  }

  public static MutCatalogFile empty() {
    return new MutCatalogFile();
  }

  public static MutCatalogFile from(CatalogFile file) {
    return new MutCatalogFile(file);
  }

  static class MutCatalogFile {
    private static final Map<String, String> DELETED = Collections.emptyMap();

    private final CatalogFile original;
    private final Map<TableIdentifier, String> tables;
    private final Map<Namespace, Map<String, String>> namespaces;

    MutCatalogFile() {
      this(new CatalogFile());
    }

    MutCatalogFile(CatalogFile original) {
      this.original = original;
      this.tables = Maps.newHashMap();
      this.namespaces = Maps.newHashMap();
      namespaces.put(Namespace.empty(), Collections.emptyMap());
    }

    public MutCatalogFile addNamespace(Namespace namespace) {
      return addNamespace(namespace, Collections.emptyMap());
    }

    public MutCatalogFile addNamespace(Namespace namespace, Map<String, String> properties) {
      Preconditions.checkNotNull(namespace, "Namespace cannot be null");
      Preconditions.checkNotNull(properties, "Properties cannot be null");
      if (original.namespaceProperties(namespace) != null
          || namespaces.put(namespace, properties) != null) {
        throw new AlreadyExistsException(
            "Cannot create namespace %s. Namespace already exists", namespace);
      }
      return this;
    }

    public MutCatalogFile updateProperties(Namespace namespace, Map<String, String> properties) {
      // note: merges w/ existing
      // TODO: legal to update properties of empty/root namespace?
      final Map<String, String> mutProp = namespaces.get(namespace);
      if (DELETED == mutProp) { // could just ignore
        throw new NoSuchNamespaceException("Namespace marked for deletion: %s", namespace);
      }
      final Map<String, String> originalProp = original.namespaceProperties(namespace);
      if (null == originalProp && null == mutProp) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }
      namespaces.compute(
          namespace,
          (ignored, old) -> {
            Map<String, String> merged = Maps.newHashMap();
            if (old != null) {
              merged.putAll(old);
            }
            merged.putAll(properties);
            return merged;
          });
      return this;
    }

    public MutCatalogFile dropNamespace(Namespace namespace) {
      // TODO check for tables, refuse if not empty
      if (null == namespaces.computeIfPresent(namespace, (ns, props) -> DELETED)) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }
      return this;
    }

    public MutCatalogFile createTable(TableIdentifier table, String location) {
      // TODO: fix for swap (a -> b; b -> a)
      if (original.location(table) != null || tables.get(table) != null) {
        throw new AlreadyExistsException("Table already exists: %s", table);
      }
      tables.put(table, location);
      return this;
    }

    public MutCatalogFile renameTable(TableIdentifier oldTable, TableIdentifier newTable) {
      if (null == original.location(oldTable)) {
        throw new NoSuchNamespaceException("Table does not exist: %s", oldTable);
      }
      return this;
    }

    public MutCatalogFile updateTable(TableIdentifier table, String location) {
      if (null == original.location(table)) {
        throw new NoSuchNamespaceException("Table does not exist: %s", table);
      }
      tables.put(table, location);
      return this;
    }

    public MutCatalogFile dropTable(TableIdentifier tableId) {
      if (null == original.location(tableId)) {
        throw new NoSuchNamespaceException("Table does not exist: %s", tableId);
      }
      tables.put(tableId, null);
      return this;
    }

    public CatalogFile commit(OutputStream out) {
      final Map<Namespace, Map<String, String>> newNamespaces = Maps.newHashMap();
      for (Map.Entry<Namespace, Map<String, String>> entry : namespaces.entrySet()) {
        final Namespace namespace = entry.getKey();
        final Map<String, String> properties = entry.getValue();
        if (DELETED == properties) {
          continue;
        }
        newNamespaces.put(namespace, properties);
      }
      final Map<TableIdentifier, TableInfo> newFqti = Maps.newHashMap();
      for (Map.Entry<TableIdentifier, String> entry : tables.entrySet()) {
        final TableIdentifier table = entry.getKey();
        final String location = entry.getValue();
        if (null == location) {
          continue;
        }
        final TableInfo info = new TableInfo(original.nextCommit, location);
        newFqti.put(table, info);
      }

      CatalogFile catalog =
          new CatalogFile(original.uuid, original.nextCommit, newNamespaces, newFqti);
      try {
        catalog.write(out);
      } catch (IOException e) {
        throw new CommitFailedException(e, "Failed to commit catalog file");
      }
      return catalog;
    }
  }

  CatalogFile() {
    // consistent iteration order
    this(UUID.randomUUID(), 0, Maps.newHashMap(), Maps.newHashMap());
  }

  CatalogFile(
      UUID uuid,
      int seqno,
      Map<Namespace, Map<String, String>> namespaces,
      Map<TableIdentifier, TableInfo> fqti) {
    this.uuid = uuid;
    this.nextCommit = seqno;
    this.fqti = fqti;
    this.namespaces = namespaces;
  }

  public String location(TableIdentifier table) {
    final TableInfo info = fqti.get(table);
    return info != null ? info.location : null;
  }

  public int version(TableIdentifier table) {
    final TableInfo info = fqti.get(table);
    return info != null ? info.version : -1;
  }

  public Set<Namespace> namespaces() {
    return Collections.unmodifiableSet(namespaces.keySet());
  }

  public Map<String, String> namespaceProperties(Namespace namespace) {
    Map<String, String> props = namespaces.get(namespace);
    return props != null
        ? Collections.unmodifiableMap(namespaces.get(namespace))
        : null;
  }

  public List<TableIdentifier> tables() {
    return Lists.newArrayList(fqti.keySet().iterator());
  }

  Map<TableIdentifier, TableInfo> fqti() {
    return fqti;
  }

  static CatalogFile read(InputStream in) {
    final Map<TableIdentifier, TableInfo> fqti = Maps.newHashMap();
    final Map<Namespace, Map<String, String>> namespaces = Maps.newHashMap();
    try (DataInputStream din = new DataInputStream(in)) {
      int nNamespaces = din.readInt();
      for (int i = 0; i < nNamespaces; ++i) {
        int nlen = din.readInt();
        String[] levels = new String[nlen];
        for (int j = 0; j < nlen; j++) {
          levels[j] = din.readUTF();
        }
        Namespace namespace = Namespace.of(levels);
        Map<String, String> props = Maps.newHashMap();
        int nprops = din.readInt();
        for (int j = 0; j < nprops; j++) {
          props.put(din.readUTF(), din.readUTF());
        }
        namespaces.put(namespace, props);
      }
      int nTables = din.readInt();
      for (int i = 0; i < nTables; i++) {
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
      int seqno = din.readInt();
      long msb = din.readLong();
      long lsb = din.readLong();
      return new CatalogFile(new UUID(msb, lsb), seqno, namespaces, fqti);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public int write(OutputStream out) throws IOException {
    try (DataOutputStream dos = new DataOutputStream(out)) {
      dos.writeInt(namespaces.size());
      for (Map.Entry<Namespace, Map<String,String>> e : namespaces.entrySet()) {
        Namespace namespace = e.getKey();
        dos.writeInt(namespace.length());
        for (String n : namespace.levels()) {
          dos.writeUTF(n);
        }
        Map<String, String> props = e.getValue();
        dos.writeInt(props.size());
        for (Map.Entry<String, String> p : props.entrySet()) {
          dos.writeUTF(p.getKey());
          dos.writeUTF(p.getValue());
        }
      }
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
      dos.writeInt(nextCommit);
      dos.writeLong(uuid.getMostSignificantBits());
      dos.writeLong(uuid.getLeastSignificantBits());
      return dos.size();
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
    return nextCommit == that.nextCommit && uuid.equals(that.uuid) && fqti.equals(that.fqti()) && namespaces.equals(that.namespaces);
  }

  @Override
  public int hashCode() {
    return fqti.hashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("CatalogFile{");
    sb.append("uuid=\"").append(uuid).append("\",");
    sb.append("nextCommit=").append(nextCommit).append(",");
    sb.append("fqti=").append(fqti).append(",");
    sb.append("namespaces=").append(namespaces);
    return sb.toString();
  }
}
