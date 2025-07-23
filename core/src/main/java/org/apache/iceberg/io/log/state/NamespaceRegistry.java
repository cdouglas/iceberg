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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class NamespaceRegistry {
  private final Map<Namespace, Integer> namespaceIds;
  private final Map<Integer, Integer> namespaceVersions;
  private final Map<Integer, Namespace> namespaceLookup;
  private final Map<Integer, Map<String, String>> namespaceProperties;

  public NamespaceRegistry(
      Map<Namespace, Integer> namespaceIds,
      Map<Integer, Integer> namespaceVersions,
      Map<Integer, Namespace> namespaceLookup,
      Map<Integer, Map<String, String>> namespaceProperties) {
    this.namespaceIds = Collections.unmodifiableMap(namespaceIds);
    this.namespaceVersions = Collections.unmodifiableMap(namespaceVersions);
    this.namespaceLookup = Collections.unmodifiableMap(namespaceLookup);
    this.namespaceProperties = Collections.unmodifiableMap(namespaceProperties);
  }

  public static NamespaceRegistry withRoot() {
    Map<Namespace, Integer> nsids = Maps.newHashMap();
    Map<Integer, Integer> nsVersions = Maps.newHashMap();
    Map<Integer, Namespace> nsLookup = Maps.newHashMap();
    Map<Integer, Map<String, String>> nsProps = Maps.newHashMap();

    nsids.put(Namespace.empty(), 0);
    nsVersions.put(0, 1);
    nsLookup.put(0, Namespace.empty());

    return new NamespaceRegistry(nsids, nsVersions, nsLookup, nsProps);
  }

  public Set<Namespace> namespaces() {
    return namespaceIds.keySet();
  }

  public boolean contains(Namespace namespace) {
    return namespaceIds.containsKey(namespace);
  }

  public Integer namespaceId(Namespace namespace) {
    return namespaceIds.get(namespace);
  }

  public Namespace namespaceById(int nsid) {
    return namespaceLookup.get(nsid);
  }

  public Integer version(int nsid) {
    return namespaceVersions.get(nsid);
  }

  public Map<String, String> properties(Namespace namespace) {
    Integer nsid = namespaceIds.get(namespace);
    if (nsid == null) {
      return null;
    }
    return namespaceProperties.getOrDefault(nsid, Collections.emptyMap());
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    NamespaceRegistry that = (NamespaceRegistry) o;
    return Objects.equals(namespaceIds, that.namespaceIds)
        && Objects.equals(namespaceVersions, that.namespaceVersions)
        && Objects.equals(namespaceLookup, that.namespaceLookup)
        && Objects.equals(namespaceProperties, that.namespaceProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceIds, namespaceVersions, namespaceLookup, namespaceProperties);
  }

  public static class Builder {
    private final Map<Namespace, Integer> namespaceIds;
    private final Map<Integer, Integer> namespaceVersions;
    private final Map<Integer, Namespace> namespaceLookup;
    private final Map<Integer, Map<String, String>> namespaceProperties;

    public Builder() {
      this.namespaceIds = Maps.newHashMap();
      this.namespaceVersions = Maps.newHashMap();
      this.namespaceLookup = Maps.newHashMap();
      this.namespaceProperties = Maps.newHashMap();

      // Initialize with root namespace
      namespaceIds.put(Namespace.empty(), 0);
      namespaceVersions.put(0, 1);
      namespaceLookup.put(0, Namespace.empty());
    }

    public Builder(NamespaceRegistry original) {
      this.namespaceIds = Maps.newHashMap(original.namespaceIds);
      this.namespaceVersions = Maps.newHashMap(original.namespaceVersions);
      this.namespaceLookup = Maps.newHashMap(original.namespaceLookup);
      this.namespaceProperties = Maps.newHashMap();

      // Deep copy properties
      for (Map.Entry<Integer, Map<String, String>> entry :
          original.namespaceProperties.entrySet()) {
        this.namespaceProperties.put(entry.getKey(), Maps.newHashMap(entry.getValue()));
      }
    }

    public Builder addNamespace(String name, int parentId, int nsid, int version) {
      Preconditions.checkNotNull(name, "Namespace name cannot be null");

      final Namespace ns;
      if (nsid == 0) {
        Preconditions.checkArgument(namespaceIds.size() == 1, "Root namespace already exists");
        Preconditions.checkArgument(
            parentId == 0, "Invalid parent id for root namespace: %d", parentId);
        Preconditions.checkArgument(name.isEmpty(), "Invalid name for root namespace: %s", name);
        ns = Namespace.empty();
      } else {
        Namespace parent = namespaceLookup.get(parentId);
        Preconditions.checkNotNull(parent, "Invalid parent namespace: %d", parentId);

        String[] levels = Arrays.copyOf(parent.levels(), parent.levels().length + 1);
        levels[levels.length - 1] = name;
        ns = Namespace.of(levels);
      }

      if (namespaceIds.put(ns, nsid) != null) {
        throw new IllegalStateException("Duplicate namespace: " + ns);
      }
      namespaceLookup.put(nsid, ns);
      namespaceVersions.put(nsid, version);
      return this;
    }

    public Builder addProperty(int nsid, String key, String value) {
      Preconditions.checkArgument(namespaceLookup.containsKey(nsid), "Invalid namespace: %s", nsid);
      Map<String, String> props = namespaceProperties.computeIfAbsent(nsid, k -> Maps.newHashMap());
      props.put(key, value);
      return this;
    }

    public Builder removeProperty(int nsid, String key) {
      Preconditions.checkArgument(namespaceLookup.containsKey(nsid));
      Map<String, String> props = namespaceProperties.get(nsid);
      if (props != null) {
        props.remove(key);
      }
      return this;
    }

    public Builder removeNamespace(int nsid) {
      Namespace ns = namespaceLookup.remove(nsid);
      if (ns == null) {
        throw new IllegalStateException("Invalid namespace: " + nsid);
      }
      namespaceIds.remove(ns);
      namespaceVersions.remove(nsid);
      namespaceProperties.remove(nsid);
      return this;
    }

    public Builder updateVersion(int nsid, int version) {
      namespaceVersions.put(nsid, version);
      return this;
    }

    public Integer getVersion(int nsid) {
      return namespaceVersions.get(nsid);
    }

    public boolean containsNamespace(int nsid) {
      return namespaceLookup.containsKey(nsid);
    }

    public NamespaceRegistry build() {
      return new NamespaceRegistry(
          namespaceIds, namespaceVersions, namespaceLookup, namespaceProperties);
    }
  }
}
