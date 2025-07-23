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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class TableRegistry {
  private final Map<TableIdentifier, Integer> tableIds;
  private final Map<Integer, Integer> tableVersions;
  private final Map<Integer, String> tableLocations;

  public TableRegistry(
      Map<TableIdentifier, Integer> tableIds,
      Map<Integer, Integer> tableVersions,
      Map<Integer, String> tableLocations) {
    this.tableIds = Collections.unmodifiableMap(tableIds);
    this.tableVersions = Collections.unmodifiableMap(tableVersions);
    this.tableLocations = Collections.unmodifiableMap(tableLocations);
  }

  public static TableRegistry empty() {
    return new TableRegistry(Maps.newHashMap(), Maps.newHashMap(), Maps.newHashMap());
  }

  public String location(TableIdentifier tableId) {
    Integer tblId = tableIds.get(tableId);
    if (tblId == null) {
      return null;
    }
    return tableLocations.get(tblId);
  }

  public Integer tableId(TableIdentifier tableId) {
    return tableIds.get(tableId);
  }

  public Integer version(int tblId) {
    return tableVersions.get(tblId);
  }

  public List<TableIdentifier> tables() {
    return Lists.newArrayList(tableIds.keySet());
  }

  public Map<TableIdentifier, String> locations() {
    Map<TableIdentifier, String> result = Maps.newHashMap();
    for (Map.Entry<TableIdentifier, Integer> entry : tableIds.entrySet()) {
      result.put(entry.getKey(), tableLocations.get(entry.getValue()));
    }
    return result;
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
    TableRegistry that = (TableRegistry) o;
    return Objects.equals(tableIds, that.tableIds)
        && Objects.equals(tableVersions, that.tableVersions)
        && Objects.equals(tableLocations, that.tableLocations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableIds, tableVersions, tableLocations);
  }

  public static class Builder {
    private final Map<TableIdentifier, Integer> tableIds;
    private final Map<Integer, Integer> tableVersions;
    private final Map<Integer, String> tableLocations;

    public Builder() {
      this.tableIds = Maps.newHashMap();
      this.tableVersions = Maps.newHashMap();
      this.tableLocations = Maps.newHashMap();
    }

    public Builder(TableRegistry original) {
      this.tableIds = Maps.newHashMap(original.tableIds);
      this.tableVersions = Maps.newHashMap(original.tableVersions);
      this.tableLocations = Maps.newHashMap(original.tableLocations);
    }

    public Builder addTable(int tblId, TableIdentifier tableId, int version, String location) {
      if (tableIds.put(tableId, tblId) != null) {
        throw new IllegalStateException("Duplicate table: " + tableId);
      }
      tableLocations.put(tblId, location);
      tableVersions.put(tblId, version);
      return this;
    }

    public Builder removeTable(int tblId) {
      tableLocations.remove(tblId);
      tableVersions.remove(tblId);
      tableIds.values().removeIf(id -> id.equals(tblId));
      return this;
    }

    public Builder updateTable(int tblId, int version, String location) {
      tableLocations.put(tblId, location);
      tableVersions.put(tblId, version);
      return this;
    }

    public Integer getVersion(int tblId) {
      return tableVersions.get(tblId);
    }

    public boolean containsTable(int tblId) {
      return tableVersions.containsKey(tblId);
    }

    public TableRegistry build() {
      return new TableRegistry(tableIds, tableVersions, tableLocations);
    }
  }
}
