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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.compress.utils.Lists;
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

  public abstract boolean createsHierarchicalNamespaces();

  public abstract String location(TableIdentifier table);

  public abstract Set<Namespace> namespaces();

  public abstract boolean containsNamespace(Namespace namespace);

  public abstract Map<String, String> namespaceProperties(Namespace namespace);

  public abstract List<TableIdentifier> tables();

  abstract Map<Namespace, Map<String, String>> namespaceProperties();

  abstract Map<TableIdentifier, String> locations();

  public abstract static class Mut {

    protected final CatalogFile original;
    protected final Map<TableIdentifier, String> tables;
    protected final Map<Namespace, Boolean> namespaces;
    protected final Map<Namespace, Map<String, String>> namespaceProperties;

    protected Mut(CatalogFile original) {
      this.original = original;
      this.tables = Maps.newHashMap();
      this.namespaces = Maps.newHashMap();
      this.namespaceProperties = Maps.newHashMap();
    }

    public Mut createNamespace(Namespace namespace) {
      return createNamespace(namespace, Collections.emptyMap());
    }

    public Mut createNamespace(Namespace namespace, Map<String, String> properties) {
      Preconditions.checkNotNull(namespace, "Namespace cannot be null");
      Preconditions.checkNotNull(properties, "Properties cannot be null");
      Preconditions.checkArgument(!namespace.equals(Namespace.empty()), "Cannot create empty namespace");
      if (original.containsNamespace(namespace) || (namespaces.containsKey(namespace) && !namespaces.get(namespace))) {
        throw new AlreadyExistsException(
            "Cannot create namespace %s. Namespace already exists", namespace);
      }
      for (Namespace ancestor = parentOf(namespace); !original.containsNamespace(ancestor); ancestor = parentOf(ancestor)) {
        if (namespaces.containsKey(ancestor)) {
            if (!namespaces.get(ancestor)) {
                throw new IllegalStateException(String.format("Cannot create namespace %s. Parent namespace %s is marked for deletion", namespace, ancestor));
            }
            break;
        }
        namespaces.put(ancestor, true);
      }
      namespaces.put(namespace, true);
      namespaceProperties.put(namespace, properties);
      return this;
    }

    public Mut updateProperties(Namespace namespace, Map<String, String> properties) {
      Preconditions.checkNotNull(namespace, "Namespace cannot be null");
      Preconditions.checkNotNull(properties, "Properties cannot be null");
      if (checkNamespaceExists(namespace)) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }
      namespaceProperties.compute(
          namespace,
          (ignored, old) -> {
            if (old != null) {
              old.putAll(properties);
              return old;
            }
            return  Maps.newHashMap(properties);
          });
      return this;
    }

    static String nameOf(Namespace ns) {
      final int levels = ns.length();
      return levels > 0 ? ns.levels()[levels - 1] : Namespace.empty().toString();
    }

    static Namespace parentOf(Namespace ns) {
      final int levels = ns.length();
      return levels > 1
                      ? Namespace.of(Arrays.copyOfRange(ns.levels(), 0, levels - 1))
                      : Namespace.empty();
    }

    public Mut dropNamespace(Namespace namespace) {
      // TODO check for tables/child namespaces, refuse if not empty
      Preconditions.checkArgument(!Namespace.empty().equals(namespace), "Cannot drop empty namespace");
      if (checkNamespaceExists(namespace)) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }
      final boolean nsChildren = original.namespaces().stream().noneMatch(ns -> parentOf(ns).equals(namespace)) &&
                                 namespaces.entrySet().stream().noneMatch(e -> e.getValue() && parentOf(e.getKey()).equals(namespace));
      final boolean tblChildren = original.tables().stream().noneMatch(table -> table.namespace().equals(namespace)) &&
                                  tables.keySet().stream().noneMatch(table -> table.namespace().equals(namespace));
      if (!nsChildren && !tblChildren) {
        throw new IllegalStateException("Cannot drop non-empty namespace: " + namespace);
      }
      namespaces.put(namespace, false);
      namespaceProperties.remove(namespace);
      return this;
    }

    public Mut createTable(TableIdentifier table, String location) {
      // TODO: fix for swap (a -> b; b -> a)
      if (checkNamespaceExists(table.namespace())) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", table.namespace());
      }
      if (original.location(table) != null || tables.get(table) != null) {
        throw new AlreadyExistsException("Table already exists: %s", table);
      }
      tables.put(table, location);
      return this;
    }

    private boolean checkNamespaceExists(Namespace namespace) {
      return !Namespace.empty().equals(namespace) && !original.containsNamespace(namespace) && !namespaces.getOrDefault(namespace, false);
    }

    public Mut updateTable(TableIdentifier table, String location) {
      if (null == original.location(table)) {
        throw new NoSuchNamespaceException("Table does not exist: %s", table);
      }
      tables.put(table, location);
      return this;
    }

    public Mut dropTable(TableIdentifier tableId) {
      if (null == original.location(tableId)) {
        throw new NoSuchTableException("Table does not exist: %s", tableId);
      }
      tables.put(tableId, null);
      return this;
    }

    public abstract CatalogFile commit(SupportsAtomicOperations<CAS> fileIO);
  }
}
