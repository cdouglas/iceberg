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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class CatalogState {
  private final UUID catalogUuid;
  private final int nextNsid;
  private final int nextTblid;
  private final boolean sealed;
  private final Set<UUID> committedTransactions;
  private final NamespaceRegistry namespaceRegistry;
  private final TableRegistry tableRegistry;

  public CatalogState(
      UUID catalogUuid,
      int nextNsid,
      int nextTblid,
      boolean sealed,
      Set<UUID> committedTransactions,
      NamespaceRegistry namespaceRegistry,
      TableRegistry tableRegistry) {
    this.catalogUuid = catalogUuid;
    this.nextNsid = nextNsid;
    this.nextTblid = nextTblid;
    this.sealed = sealed;
    this.committedTransactions = Sets.newHashSet(committedTransactions);
    this.namespaceRegistry = namespaceRegistry;
    this.tableRegistry = tableRegistry;
  }

  public static CatalogState empty() {
    NamespaceRegistry nsRegistry = NamespaceRegistry.withRoot();
    TableRegistry tblRegistry = TableRegistry.empty();
    return new CatalogState(null, 1, 1, false, Collections.emptySet(), nsRegistry, tblRegistry);
  }

  public UUID catalogUuid() {
    return catalogUuid;
  }

  public int nextNsid() {
    return nextNsid;
  }

  public int nextTblid() {
    return nextTblid;
  }

  public boolean sealed() {
    return sealed;
  }

  public boolean containsTransaction(UUID txnId) {
    return committedTransactions.contains(txnId);
  }

  public Set<UUID> committedTransactions() {
    return committedTransactions;
  }

  public NamespaceRegistry namespaceRegistry() {
    return namespaceRegistry;
  }

  public TableRegistry tableRegistry() {
    return tableRegistry;
  }

  public Set<Namespace> namespaces() {
    return namespaceRegistry.namespaces();
  }

  public boolean containsNamespace(Namespace namespace) {
    return namespaceRegistry.contains(namespace);
  }

  public Map<String, String> namespaceProperties(Namespace namespace) {
    return namespaceRegistry.properties(namespace);
  }

  public String tableLocation(TableIdentifier tableId) {
    return tableRegistry.location(tableId);
  }

  public Integer namespaceVersion(int nsid) {
    return namespaceRegistry.version(nsid);
  }

  public Integer tableVersion(int tblId) {
    return tableRegistry.version(tblId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CatalogState that = (CatalogState) o;
    return nextNsid == that.nextNsid
        && nextTblid == that.nextTblid
        && sealed == that.sealed
        && Objects.equals(catalogUuid, that.catalogUuid)
        && Objects.equals(committedTransactions, that.committedTransactions)
        && Objects.equals(namespaceRegistry, that.namespaceRegistry)
        && Objects.equals(tableRegistry, that.tableRegistry);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        catalogUuid,
        nextNsid,
        nextTblid,
        sealed,
        committedTransactions,
        namespaceRegistry,
        tableRegistry);
  }

  public static class Builder {
    private UUID catalogUuid;
    private int nextNsid;
    private int nextTblid;
    private boolean sealed;
    private final Set<UUID> committedTransactions;
    private final NamespaceRegistry.Builder namespaceBuilder;
    private final TableRegistry.Builder tableBuilder;
    private final Map<Integer, Integer> nsRemap;

    public Builder() {
      this.nextNsid = 1;
      this.nextTblid = 1;
      this.sealed = false;
      this.committedTransactions = Sets.newHashSet();
      this.namespaceBuilder = NamespaceRegistry.builder();
      this.tableBuilder = TableRegistry.builder();
      this.nsRemap = Maps.newHashMap();
    }

    public Builder(CatalogState original) {
      this.catalogUuid = original.catalogUuid;
      this.nextNsid = original.nextNsid;
      this.nextTblid = original.nextTblid;
      this.sealed = original.sealed;
      this.committedTransactions = Sets.newHashSet(original.committedTransactions);
      this.namespaceBuilder = original.namespaceRegistry.toBuilder();
      this.tableBuilder = original.tableRegistry.toBuilder();
      this.nsRemap = Maps.newHashMap();
    }

    public Builder setGlobals(UUID catalogUuid, int nextNsid, int nextTblid) {
      this.catalogUuid = catalogUuid;
      this.nextNsid = nextNsid;
      this.nextTblid = nextTblid;
      return this;
    }

    public Builder setSealed() {
      this.sealed = true;
      return this;
    }

    public Builder addCommittedTransaction(UUID txnId) {
      this.committedTransactions.add(txnId);
      return this;
    }

    public int remap(int nsid) {
      if (nsid >= 0) {
        throw new IllegalArgumentException("Attempting to remap non-virtual namespace: " + nsid);
      }
      final int assignedNsid = nextNsid++;
      nsRemap.put(nsid, assignedNsid);
      return assignedNsid;
    }

    public void incrementNextTblid() {
      nextTblid++;
    }

    public int getRemapped(int nsid) {
      return nsRemap.getOrDefault(nsid, nsid);
    }

    public NamespaceRegistry.Builder namespaceBuilder() {
      return namespaceBuilder;
    }

    public TableRegistry.Builder tableBuilder() {
      return tableBuilder;
    }

    public void clear() {
      this.catalogUuid = null;
      this.nextNsid = 1;
      this.nextTblid = 1;
      this.sealed = false;
      this.committedTransactions.clear();
      this.nsRemap.clear();
      // Note: namespaceBuilder and tableBuilder need to be recreated or cleared
    }

    public void copyFrom(CatalogState other) {
      this.catalogUuid = other.catalogUuid();
      this.nextNsid = other.nextNsid();
      this.nextTblid = other.nextTblid();
      this.sealed = other.sealed();
      this.committedTransactions.clear();
      this.committedTransactions.addAll(other.committedTransactions());
      this.nsRemap.clear();
      // Note: Would need to copy namespace and table registries - simplified for now
    }

    public CatalogState build() {
      return new CatalogState(
          catalogUuid,
          nextNsid,
          nextTblid,
          sealed,
          committedTransactions,
          namespaceBuilder.build(),
          tableBuilder.build());
    }
  }
}
