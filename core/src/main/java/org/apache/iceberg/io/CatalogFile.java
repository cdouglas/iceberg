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
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class CatalogFile {
  // TODO use serialization idioms from the project, handle evolution, etc.

  private final int seqno; // or retain deleted TableIdentifiers unless/until not the max
  private final UUID uuid;
  private final Map<TableIdentifier, TableInfo> fqti; // fully qualified table identifiers
  private final Map<Namespace, Map<String, String>> namespaces;
  private byte[] serBytes;
  private InputFile fromFile;

  private void setFromFile(InputFile fromFile) {
    this.fromFile = fromFile;
  }

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

  public static class MutCatalogFile {

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

    public MutCatalogFile createNamespace(Namespace namespace) {
      return createNamespace(namespace, Collections.emptyMap());
    }

    public MutCatalogFile createNamespace(Namespace namespace, Map<String, String> properties) {
      Preconditions.checkNotNull(namespace, "Namespace cannot be null");
      Preconditions.checkNotNull(properties, "Properties cannot be null");
      if (original.containsNamespace(namespace) || namespaces.containsKey(namespace)) {
        throw new AlreadyExistsException(
            "Cannot create namespace %s. Namespace already exists", namespace);
      }
      namespaces.put(namespace, properties);
      return this;
    }

    public MutCatalogFile updateProperties(Namespace namespace, Map<String, String> properties) {
      Preconditions.checkNotNull(namespace, "Namespace cannot be null");
      Preconditions.checkNotNull(properties, "Properties cannot be null");
      // TODO: legal to update properties of empty/root namespace?
      if (namespaces.containsKey(namespace)) {
        final Map<String, String> mutProp = namespaces.get(namespace);
        if (null == mutProp) { // could just ignore
          throw new NoSuchNamespaceException("Namespace marked for deletion: %s", namespace);
        }
      } else {
        if (!original.containsNamespace(namespace)) {
          throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }
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
      checkNamespaceExists(namespace);
      namespaces.put(namespace, null);
      return this;
    }

    public MutCatalogFile createTable(TableIdentifier table, String location) {
      // TODO: fix for swap (a -> b; b -> a)
      checkNamespaceExists(table.namespace());
      if (original.location(table) != null || tables.get(table) != null) {
        throw new AlreadyExistsException("Table already exists: %s", table);
      }
      tables.put(table, location);
      return this;
    }

    private void checkNamespaceExists(Namespace namespace) {
      if (!original.containsNamespace(namespace) && !namespaces.containsKey(namespace)) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }
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
        throw new NoSuchTableException("Table does not exist: %s", tableId);
      }
      tables.put(tableId, null);
      return this;
    }

    public CatalogFile commit(SupportsAtomicOperations<CAS> fileIO) {
      try {
        final AtomicOutputFile<CAS> outputFile = fileIO.newOutputFile(original.fromFile);
        final Map<Namespace, Map<String, String>> newNamespaces =
            Maps.newHashMap(original.namespaces);
        merge(
            newNamespaces,
            namespaces,
            (orig, next) -> {
              Map<String, String> nsProps =
                  null == orig ? Maps.newHashMap() : Maps.newHashMap(orig);
              merge(nsProps, next, (x, y) -> y);
              return nsProps;
            });

        final Map<TableIdentifier, TableInfo> newFqti = Maps.newHashMap(original.fqti);
        merge(newFqti, tables, (x, location) -> new TableInfo(original.seqno, location));

        // TODO need to merge namespace properties?
        // TODO not using table versions... remove

        try {
          CatalogFile catalog =
              new CatalogFile(original.uuid, original.seqno, newNamespaces, newFqti);
          CAS token = outputFile.prepare(catalog::asBytes, AtomicOutputFile.Strategy.CAS);
          InputFile newCatalog = outputFile.writeAtomic(token, catalog::asBytes);
          catalog.setFromFile(newCatalog);
          return catalog;
        } catch (IOException e) {
          throw new CommitFailedException(e, "Failed to commit catalog file");
        }
      } catch (SupportsAtomicOperations.CASException e) {
        throw new CommitFailedException(e, "Cannot commit");
      }
    }

    // XXX TODO remove this, define commit(FileIO) for empty()
    public CatalogFile commit(OutputStream out) {
      final Map<Namespace, Map<String, String>> newNamespaces =
          Maps.newHashMap(original.namespaces);
      merge(newNamespaces, namespaces, (x, y) -> y);

      final Map<TableIdentifier, TableInfo> newFqti = Maps.newHashMap(original.fqti);
      merge(newFqti, tables, (x, location) -> new TableInfo(original.seqno, location));

      final CatalogFile catalog =
          new CatalogFile(original.uuid, original.seqno, newNamespaces, newFqti);
      try {
        catalog.write(out);
      } catch (IOException e) {
        throw new CommitFailedException(e, "Failed to commit catalog file");
      }
      return catalog;
    }

    private static <K, V, U> void merge(
        Map<K, V> original, Map<K, U> update, BiFunction<V, U, V> valueMapper) {
      for (Map.Entry<K, U> entry : update.entrySet()) {
        final K key = entry.getKey();
        final U value = entry.getValue();
        if (null == value) {
          original.remove(key);
        } else {
          original.put(key, valueMapper.apply(original.get(key), value));
        }
      }
    }
  }

  CatalogFile() {
    // consistent iteration order
    this(UUID.randomUUID(), 0, Maps.newHashMap(), Maps.newHashMap(), null);
  }

  CatalogFile(
      UUID uuid,
      int seqno,
      Map<Namespace, Map<String, String>> namespaces,
      Map<TableIdentifier, TableInfo> fqti) {
    this(uuid, seqno, namespaces, fqti, null);
  }

  CatalogFile(
      UUID uuid,
      int seqno,
      Map<Namespace, Map<String, String>> namespaces,
      Map<TableIdentifier, TableInfo> fqti,
      InputFile fromFile) {
    this.uuid = uuid;
    this.seqno = seqno;
    this.fqti = fqti;
    this.fromFile = fromFile;
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

  public boolean containsNamespace(Namespace namespace) {
    return namespaces.containsKey(namespace);
  }

  public Map<String, String> namespaceProperties(Namespace namespace) {
    return Collections.unmodifiableMap(namespaces.get(namespace));
  }

  public List<TableIdentifier> tables() {
    return Lists.newArrayList(fqti.keySet().iterator());
  }

  private InputStream asBytes() {
    if (null == serBytes) {
      // TODO unnecessary buffer copy; DataInput/DataOutputStream avail?
      try (ByteArrayOutputStream bytes = new ByteArrayOutputStream(2048)) {
        write(bytes);
        serBytes = bytes.toByteArray();
      } catch (IOException e) {
        throw new CommitFailedException(e, "Failed to commit catalog file");
      }
    }
    return new ByteArrayInputStream(serBytes);
  }

  int write(OutputStream out) throws IOException {
    try (DataOutputStream dos = new DataOutputStream(out)) {
      dos.writeInt(namespaces.size());
      for (Map.Entry<Namespace, Map<String, String>> e : namespaces.entrySet()) {
        CASCatalogFormat.writeNamespace(dos, e.getKey());
        CASCatalogFormat.writeProperties(dos, e.getValue());
      }
      dos.writeInt(fqti.size());
      for (Map.Entry<TableIdentifier, TableInfo> e : fqti.entrySet()) {
        TableInfo info = e.getValue();
        dos.writeInt(info.version);
        TableIdentifier tid = e.getKey();
        CASCatalogFormat.writeNamespace(dos, tid.namespace());
        dos.writeUTF(tid.name());
        dos.writeUTF(info.location);
      }
      dos.writeInt(seqno);
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
    return seqno == that.seqno
        && uuid.equals(that.uuid)
        && fqti.equals(that.fqti)
        && namespaces.equals(that.namespaces);
  }

  @Override
  public int hashCode() {
    // TODO replace with CRC during deserialization?
    return Objects.hash(uuid, fqti.keySet(), namespaces.keySet());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"uuid\" : \"").append(uuid).append("\",");
    sb.append("\"seqno\" : ").append(seqno).append(",");
    sb.append("\"tables\" : [");
    sb.append(fqti.keySet().stream().map(id -> "\"" + id + "\"").collect(Collectors.joining(",")))
        .append("],");
    sb.append("\"namespaces\" : [");
    sb.append(
        namespaces.keySet().stream().map(id -> "\"" + id + "\"").collect(Collectors.joining(",")));
    sb.append("]").append("}");
    return sb.toString();
  }
}
