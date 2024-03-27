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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class CatalogFile {
  // TODO use serialization idioms from the project

  private final Map<TableIdentifier, String> fqti; // fully qualified table identifiers

  public CatalogFile() {
    // consistent iteration order
    this(new LinkedHashMap<>());
  }

  CatalogFile(Map<TableIdentifier, String> fqti) {
    this.fqti = fqti;
  }

  public List<TableIdentifier> tables() {
    return Lists.newArrayList(fqti.keySet().iterator());
  }

  public boolean add(TableIdentifier table, String location) {
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

  Map<TableIdentifier, String> fqti() {
    return fqti;
  }

  public int write(OutputStream out) {
    try (DataOutputStream dos = new DataOutputStream(out)) {
      dos.writeInt(fqti.size());
      for (Map.Entry<TableIdentifier, String> e : fqti.entrySet()) {
        TableIdentifier tid = e.getKey();
        Namespace namespace = tid.namespace();
        dos.writeInt(namespace.length());
        for (String n : namespace.levels()) {
          dos.writeUTF(n);
        }
        dos.writeUTF(tid.name());
        dos.writeUTF(e.getValue()); // location
      }
      return dos.size();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void read(InputStream in) {
    fqti.clear();
    try (DataInputStream din = new DataInputStream(in)) {
      int size = din.readInt();
      for (int i = 0; i < size; i++) {
        int nlen = din.readInt();
        String[] levels = new String[nlen];
        for (int j = 0; j < nlen; j++) {
          levels[j] = din.readUTF();
        }
        Namespace namespace = Namespace.of(levels);
        TableIdentifier tid = TableIdentifier.of(namespace, din.readUTF());
        fqti.put(tid, din.readUTF());
      }
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
