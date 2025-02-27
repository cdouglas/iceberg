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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/** Snapshot of Catalog state used in FileIOCatalog. */
public abstract class CatalogFile {

  private final UUID uuid;
  private final InputFile location;

  /** Constructor for empty CatalogFile instances. Assigns a random UUID. */
  CatalogFile(InputFile location) {
    // consistent iteration order; UUIDv7
    this(UUID.randomUUID(), location);
  }

  /** Construct a CatalogFile instance from an existing Catalog. */
  CatalogFile(UUID uuid, InputFile fromFile) {
    this.uuid = uuid;
    this.location = fromFile;
  }

  public UUID uuid() {
    return uuid;
  }

  public InputFile location() {
    return location;
  }

  public abstract String location(TableIdentifier table);

  public abstract Set<Namespace> namespaces();

  public abstract boolean containsNamespace(Namespace namespace);

  public abstract Map<String, String> namespaceProperties(Namespace namespace);

  public abstract List<TableIdentifier> tables();

  abstract Map<Namespace, Map<String, String>> namespaceProperties();

  abstract Map<TableIdentifier, String> locations();

  public abstract static class Mut<T extends CatalogFile> {

    protected final T original;
    protected final Map<TableIdentifier, String> tables;
    protected final Map<Namespace, Map<String, String>> namespaces;

    protected abstract T empty(InputFile location);

    protected Mut(InputFile location) {
      this.original = empty(location);
      this.tables = Maps.newHashMap();
      this.namespaces = Maps.newHashMap();
    }

    protected Mut(T original) {
      this.original = original;
      this.tables = Maps.newHashMap();
      this.namespaces = Maps.newHashMap();
      namespaces.put(Namespace.empty(), Collections.emptyMap());
    }

    public Mut<T> createNamespace(Namespace namespace) {
      return createNamespace(namespace, Collections.emptyMap());
    }

    public Mut<T> createNamespace(Namespace namespace, Map<String, String> properties) {
      Preconditions.checkNotNull(namespace, "Namespace cannot be null");
      Preconditions.checkNotNull(properties, "Properties cannot be null");
      if (original.containsNamespace(namespace) || namespaces.containsKey(namespace)) {
        throw new AlreadyExistsException(
            "Cannot create namespace %s. Namespace already exists", namespace);
      }
      namespaces.put(namespace, properties);
      return this;
    }

    public Mut<T> updateProperties(Namespace namespace, Map<String, String> properties) {
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

    public Mut<T> dropNamespace(Namespace namespace) {
      // TODO check for tables/child namespaces, refuse if not empty
      checkNamespaceExists(namespace);
      namespaces.put(namespace, null);
      return this;
    }

    public Mut<T> createTable(TableIdentifier table, String location) {
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

    public Mut<T> updateTable(TableIdentifier table, String location) {
      if (null == original.location(table)) {
        throw new NoSuchNamespaceException("Table does not exist: %s", table);
      }
      tables.put(table, location);
      return this;
    }

    public Mut<T> dropTable(TableIdentifier tableId) {
      if (null == original.location(tableId)) {
        throw new NoSuchTableException("Table does not exist: %s", tableId);
      }
      tables.put(tableId, null);
      return this;
    }

    public abstract T commit(SupportsAtomicOperations<CAS> fileIO);
  }
}
