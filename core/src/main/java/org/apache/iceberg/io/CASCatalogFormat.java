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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class CASCatalogFormat extends CatalogFormat {

  @Override
  public CatalogFile.Mut empty(InputFile location) {
    return new CASMutCatalogFile(location);
  }

  @Override
  public CatalogFile.Mut from(CatalogFile other) {
    return new CASMutCatalogFile(other);
  }

  static class CASMutCatalogFile extends CatalogFile.Mut {
    CASMutCatalogFile(InputFile location) {
      super(location);
    }
    CASMutCatalogFile(CatalogFile original) {
      super(original);
    }
    @Override
    public CatalogFile commit(SupportsAtomicOperations<CAS> fileIO) {
      try {
        CatalogFile catalog = merge();
        final AtomicOutputFile<CAS> outputFile = fileIO.newOutputFile(original.location());
        try {
          byte[] ffs = asBytes(catalog);
          try (ByteArrayInputStream serBytes = new ByteArrayInputStream(asBytes(catalog))) {
            serBytes.mark(ffs.length); // readAheadLimit ignored, but whatever
            CAS token = outputFile.prepare(() -> serBytes, AtomicOutputFile.Strategy.CAS);
            serBytes.reset();
            InputFile newCatalog = outputFile.writeAtomic(token, () -> serBytes);
            return new CatalogFile(catalog.uuid(), catalog.seqno(), catalog.namespaceProperties(), catalog.tableMetadata(), newCatalog);
          }
        } catch (IOException e) {
          throw new CommitFailedException(e, "Failed to commit catalog file");
        }
      } catch (SupportsAtomicOperations.CASException e) {
        throw new CommitFailedException(e, "Cannot commit");
      }
    }
  }

  @Override
  public CatalogFile read(InputFile catalogLocation) {
    final Map<TableIdentifier, CatalogFile.TableInfo> fqti = Maps.newHashMap();
    final Map<Namespace, Map<String, String>> namespaces = Maps.newHashMap();
    try (InputStream in = catalogLocation.newStream();
        DataInputStream din = new DataInputStream(in)) {
      int nNamespaces = din.readInt();
      for (int i = 0; i < nNamespaces; ++i) {
        Namespace namespace = readNamespace(din);
        Map<String, String> props = readProperties(din);
        namespaces.put(namespace, props);
      }
      int nTables = din.readInt();
      for (int i = 0; i < nTables; i++) {
        int tableVersion = din.readInt();
        Namespace namespace = readNamespace(din);
        TableIdentifier tid = TableIdentifier.of(namespace, din.readUTF());
        fqti.put(tid, new CatalogFile.TableInfo(tableVersion, din.readUTF()));
      }
      int seqno = din.readInt();
      long msb = din.readLong();
      long lsb = din.readLong();
      return new CatalogFile(new UUID(msb, lsb), seqno, namespaces, fqti, catalogLocation);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static byte[] asBytes(CatalogFile file) {
    // TODO unnecessary buffer copy; DataInput/DataOutputStream avail?
    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream(2048)) {
      write(file, bytes);
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new CommitFailedException(e, "Failed to commit catalog file");
    }
  }

  static int write(CatalogFile file, OutputStream out) throws IOException {
    try (DataOutputStream dos = new DataOutputStream(out)) {
      // namespaces
      Map<Namespace,Map<String,String>> namespaces = file.namespaceProperties();
      dos.writeInt(namespaces.size());
      for (Map.Entry<Namespace, Map<String, String>> e : namespaces.entrySet()) {
        CASCatalogFormat.writeNamespace(dos, e.getKey());
        CASCatalogFormat.writeProperties(dos, e.getValue());
      }
      // tableinfo TODO store as bytes
      Map<TableIdentifier, CatalogFile.TableInfo> fqti = file.tableMetadata();
      dos.writeInt(fqti.size());
      for (Map.Entry<TableIdentifier, CatalogFile.TableInfo> e : fqti.entrySet()) {
        CatalogFile.TableInfo info = e.getValue();
        dos.writeInt(info.version);
        TableIdentifier tid = e.getKey();
        CASCatalogFormat.writeNamespace(dos, tid.namespace());
        dos.writeUTF(tid.name());
        dos.writeUTF(info.location);
      }
      dos.writeInt(file.seqno());
      // table uuid
      UUID uuid = file.uuid();
      dos.writeLong(uuid.getMostSignificantBits());
      dos.writeLong(uuid.getLeastSignificantBits());
      return dos.size();
    }
  }

  private static Map<String, String> readProperties(DataInputStream in) throws IOException {
    int nprops = in.readInt();
    Map<String, String> props = nprops > 0 ? Maps.newHashMap() : Collections.emptyMap();
    for (int j = 0; j < nprops; j++) {
      props.put(in.readUTF(), in.readUTF());
    }
    return props;
  }

  private static Namespace readNamespace(DataInputStream in) throws IOException {
    int nlen = in.readInt();
    String[] levels = new String[nlen];
    for (int j = 0; j < nlen; j++) {
      levels[j] = in.readUTF();
    }
    return Namespace.of(levels);
  }

  static void writeNamespace(DataOutputStream out, Namespace namespace) throws IOException {
    out.writeInt(namespace.length());
    for (String n : namespace.levels()) {
      out.writeUTF(n);
    }
  }

  static void writeProperties(DataOutputStream out, Map<String, String> props) throws IOException {
    Map<String, String> writeProps =
        props.entrySet().stream()
            .filter(e -> e.getValue() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    out.writeInt(writeProps.size());
    for (Map.Entry<String, String> p : writeProps.entrySet()) {
      out.writeUTF(p.getKey());
      out.writeUTF(p.getValue());
    }
  }
}
