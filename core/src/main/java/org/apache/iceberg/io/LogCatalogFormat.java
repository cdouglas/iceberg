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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class LogCatalogFormat extends CatalogFormat {
  // UUID generation
  private static final Random random = new Random();

  @Override
  public CatalogFile.Mut empty(InputFile input) {
    return new LogCatalogFileMut(input);
  }

  @Override
  public CatalogFile read(SupportsAtomicOperations fileIO, InputFile catalogLocation) {
    // intentionally drop metadata cached on InputFile
    InputFile refresh = fileIO.newInputFile(catalogLocation.location());
    final long fileLength = refresh.getLength();
    final LogCatalogFileMut catalog = new LogCatalogFileMut(refresh);
    try (SeekableInputStream in = refresh.newStream()) {
      LogCatalogRegionFormat.readCheckpoint(catalog, in);
      final long logLength = fileLength - in.getPos();
      // process log
      // LogCatalogFormat.readLog(catalog, din, logLength);
      return catalog.merge();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public CatalogFile.Mut from(CatalogFile other) {
    throw new UnsupportedOperationException("TODO");
  }

  // Log format
  // <len><op><payload>
  abstract static class LogAction {
    enum Type {
      CREATE_TABLE(1), // <nsid> <ns_ver> <name> <location>
      UPDATE_TABLE(2), // <tbl_id> <tbl_ver> <location>
      DROP_TABLE(3), // <tbl_id> <tbl_ver>
      CREATE_NAMESPACE(4), // <parent_id> <parent_version> <name>
      UPDATE_NAMESPACE(5), // <nsid> <version> <key> <value>
      DROP_NAMESPACE(6), // <nsid> <version>
      TRANSACTION(7); // <txid> <sealed> <n_actions> <action>*

      private final int opcode;

      Type(int opcode) {
        this.opcode = opcode;
      }

      static Type from(int opcode) {
        for (Type t : Type.values()) {
          if (t.opcode == opcode) {
            return t;
          }
        }
        throw new IllegalArgumentException("Unknown opcode: " + opcode);
      }
    }

    abstract boolean verify(LogCatalogFileMut catalog);

    abstract void apply(LogCatalogFileMut catalog);

    abstract void write(DataOutputStream dos) throws IOException;

    static class CreateNamespace extends LogAction {
      final String name;
      final int parentId;
      final int parentVersion;

      CreateNamespace(String name, int parentId, int parentVersion) {
        this.name = name;
        this.parentId = parentId;
        this.parentVersion = parentVersion;
      }

      @Override
      boolean verify(LogCatalogFileMut catalog) {
        // concurrent creates are conflicts, but can be retried
        Integer version = catalog.nsVersion.get(parentId);
        return version != null && version == parentVersion;
      }

      @Override
      void apply(LogCatalogFileMut catalog) {
        // increment parent version, assign uniq nsid
        catalog.nsVersion.put(parentId, parentVersion + 1);
        catalog.addNamespaceInternal(name, parentId, catalog.nextNsid++, 1);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.CREATE_NAMESPACE.opcode);
        dos.writeUTF(name);
        dos.writeInt(parentId);
        dos.writeInt(parentVersion);
      }

      static CreateNamespace read(DataInputStream dis) throws IOException {
        String name = dis.readUTF();
        int parentId = dis.readInt();
        int parentVersion = dis.readInt();
        return new CreateNamespace(name, parentId, parentVersion);
      }
    }

    static class DropNamespace extends LogAction {
      final int nsid;
      final int version;

      DropNamespace(int nsid, int version) {
        this.nsid = nsid;
        this.version = version;
      }

      @Override
      boolean verify(LogCatalogFileMut catalog) {
        Integer version = catalog.nsVersion.get(nsid);
        return version != null && version == this.version;
      }

      @Override
      void apply(LogCatalogFileMut catalog) {
        catalog.dropNamespaceInternal(nsid);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.DROP_NAMESPACE.opcode);
        dos.writeInt(nsid);
        dos.writeInt(version);
      }

      static DropNamespace read(DataInputStream dis) throws IOException {
        int nsid = dis.readInt();
        int version = dis.readInt();
        return new DropNamespace(nsid, version);
      }
    }

    static class UpdateNamespace extends LogAction {
      final int nsid;
      final int version;
      final String key;
      final String value;

      UpdateNamespace(int nsid, int version, String key, String value) {
        this.nsid = nsid;
        this.version = version;
        this.key = key;
        this.value = value;
      }

      @Override
      boolean verify(LogCatalogFileMut catalog) {
        Integer version = catalog.nsVersion.get(nsid);
        return version != null && version == this.version;
      }

      @Override
      void apply(LogCatalogFileMut catalog) {
        catalog.nsVersion.put(nsid, version + 1);
        catalog.addNamespacePropertyInternal(nsid, key, value);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.UPDATE_NAMESPACE.opcode);
        dos.writeInt(nsid);
        dos.writeInt(version);
        dos.writeUTF(key);
        dos.writeUTF(value);
      }

      static UpdateNamespace read(DataInputStream dis) throws IOException {
        int nsid = dis.readInt();
        int version = dis.readInt();
        String key = dis.readUTF();
        String value = dis.readUTF();
        return new UpdateNamespace(nsid, version, key, value);
      }
    }

    static class CreateTable extends LogAction {
      final String name;
      final int nsid;
      final int nsVersion;
      final String location;

      CreateTable(String name, int nsid, int nsVersion, String location) {
        this.name = name;
        this.nsid = nsid;
        this.nsVersion = nsVersion;
        this.location = location;
      }

      @Override
      boolean verify(LogCatalogFileMut catalog) {
        Integer version = catalog.nsVersion.get(nsid);
        return version != null && version == nsVersion;
      }

      @Override
      void apply(LogCatalogFileMut catalog) {
        catalog.addTableInternal(catalog.nextTblid++, nsid, 1, name, location);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.CREATE_TABLE.opcode);
        dos.writeUTF(name);
        dos.writeInt(nsid);
        dos.writeInt(nsVersion);
        dos.writeUTF(location);
      }

      static CreateTable read(DataInputStream dis) throws IOException {
        String name = dis.readUTF();
        int nsid = dis.readInt();
        int nsVersion = dis.readInt();
        String location = dis.readUTF();
        return new CreateTable(name, nsid, nsVersion, location);
      }
    }

    static class DropTable extends LogAction {
      final int tblId;
      final int version;

      DropTable(int tblId, int version) {
        this.tblId = tblId;
        this.version = version;
      }

      @Override
      boolean verify(LogCatalogFileMut catalog) {
        Integer version = catalog.tblVersion.get(tblId);
        return version != null && version == this.version;
      }

      @Override
      void apply(LogCatalogFileMut catalog) {
        catalog.dropTableInternal(tblId);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.DROP_TABLE.opcode);
        dos.writeInt(tblId);
        dos.writeInt(version);
      }

      static DropTable read(DataInputStream dis) throws IOException {
        int tblId = dis.readInt();
        int version = dis.readInt();
        return new DropTable(tblId, version);
      }
    }

    static class UpdateTable extends LogAction {
      final int tblId;
      final int version;
      final String location;

      UpdateTable(int tblId, int version, String location) {
        this.tblId = tblId;
        this.version = version;
        this.location = location;
      }

      @Override
      boolean verify(LogCatalogFileMut catalog) {
        Integer version = catalog.tblVersion.get(tblId);
        return version != null && version == this.version;
      }

      @Override
      void apply(LogCatalogFileMut catalog) {
        catalog.updateTableInternal(tblId, version + 1, location);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.UPDATE_TABLE.opcode);
        dos.writeInt(tblId);
        dos.writeInt(version);
        dos.writeUTF(location);
      }

      static UpdateTable read(DataInputStream dis) throws IOException {
        int tblId = dis.readInt();
        int version = dis.readInt();
        String location = dis.readUTF();
        return new UpdateTable(tblId, version, location);
      }
    }

    static class Transaction extends LogAction {
      boolean sealed;
      final UUID txnId;
      final List<LogAction> actions;

      Transaction(List<LogAction> actions) {
        this(generate(), actions, false);
      }

      Transaction(UUID txnId, List<LogAction> actions, boolean sealed) {
        this.txnId = txnId;
        this.actions = actions;
        this.sealed = sealed;
      }

      boolean sealed() {
        return sealed;
      }

      void seal() {
        this.sealed = true;
      }

      @Override
      boolean verify(LogCatalogFileMut catalog) {
        return actions.stream().map(a -> a.verify(catalog)).reduce(true, (a, b) -> a && b);
      }

      @Override
      void apply(LogCatalogFileMut catalog) {
        actions.forEach(a -> a.apply(catalog));
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.TRANSACTION.opcode);
        dos.writeLong(txnId.getMostSignificantBits());
        dos.writeLong(txnId.getLeastSignificantBits());
        dos.writeBoolean(sealed);
        dos.writeInt(actions.size());
        for (LogAction action : actions) {
          action.write(dos);
        }
      }

      static Transaction read(DataInputStream dis) throws IOException {
        long msb = dis.readLong();
        long lsb = dis.readLong();
        final UUID uuid = new UUID(msb, lsb);
        boolean sealed = dis.readBoolean();
        final int nActions = dis.readInt();
        List<LogAction> actions = new ArrayList<>(nActions);
        for (int i = 0; i < nActions; ++i) {
          Type type = Type.from(dis.readByte());
          switch (type) {
            case CREATE_TABLE:
              actions.add(CreateTable.read(dis));
              break;
            case UPDATE_TABLE:
              actions.add(UpdateTable.read(dis));
              break;
            case DROP_TABLE:
              actions.add(DropTable.read(dis));
              break;
            case CREATE_NAMESPACE:
              actions.add(CreateNamespace.read(dis));
              break;
            case UPDATE_NAMESPACE:
              actions.add(UpdateNamespace.read(dis));
              break;
            case DROP_NAMESPACE:
              actions.add(DropNamespace.read(dis));
              break;
            case TRANSACTION:
              throw new IllegalStateException("Nested transactions are not supported");
            default:
              throw new IllegalArgumentException("Unknown action type: " + type);
          }
        }
        return new Transaction(uuid, actions, sealed);
      }
    }

    static Iterable<Transaction> logIterator(final DataInputStream dis) throws IOException {
      return () -> new LogStream(dis);
    }

    static class LogStream implements Iterator<Transaction> {
      DataInputStream dis;

      LogStream(DataInputStream dis) {
        this.dis = dis;
      }

      @Override
      public boolean hasNext() {
        try {
          return dis.available() > 0;
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      @Override
      public Transaction next() {
        try {
          Type type = Type.from(dis.readByte());
          switch (type) {
            case TRANSACTION:
              return Transaction.read(dis);
            case CREATE_TABLE:
            case UPDATE_TABLE:
            case DROP_TABLE:
            case CREATE_NAMESPACE:
            case UPDATE_NAMESPACE:
            case DROP_NAMESPACE:
              throw new IllegalStateException("Action not in transaction");
            default:
              throw new IllegalArgumentException("Unknown action type: " + type);
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
  }

  static class LogCatalogFileMut extends CatalogFile.Mut {
    // namespace IDs are internal to the catalog format

    private final InputFile location;
    private UUID uuid = null;
    private int nextNsid = 1;
    private int nextTblid = 1;

    // CREATE TABLE namespaces (
    //   nsid INT,
    //   version INT,
    //   parentId INT,
    //   name VARCHAR(255) NOT NULL,
    //   PRIMARY KEY (nsid, version),
    //   FOREIGN KEY (parentId) REFERENCES namespaces(nsid)
    // );
    private final Map<Namespace, Integer> nsids = Maps.newHashMap();
    private final Map<Integer, Integer> nsVersion = Maps.newHashMap();
    private final Map<Integer, Namespace> nsLookup = Maps.newHashMap();

    // CREATE TABLE ns_prop (
    //   nsid INT,
    //   key VARCHAR(255) NOT NULL,
    //   value VARCHAR(1024) NOT NULL
    // );
    private final Map<Integer, Map<String, String>> nsProperties = Maps.newHashMap();

    // CREATE TABLE tables (
    //   tbl_id INT,
    //   version INT,
    //   nsid INT,
    //   name VARCHAR(255) NOT NULL,
    //   location VARCHAR(1024 NOT NULL),
    //   PRIMARY KEY (tbl_id, version),
    //   FOREIGN KEY (nsid) REFERENCES namespaces(nsid)
    // );
    private final Map<TableIdentifier, Integer> tblIds = Maps.newHashMap();
    private final Map<Integer, Integer> tblVersion = Maps.newHashMap();
    private final Map<Integer, String> tblLocations = Maps.newHashMap();

    LogCatalogFileMut(InputFile input) {
      super(input); // TODO dismantle this
      this.location = input;
    }

    LogCatalogFileMut(LogCatalogRegionFormat.LogCatalogFile other) {
      super(other);
      throw new UnsupportedOperationException();
    }

    @Override
    public LogCatalogRegionFormat.LogCatalogFile merge() {
      // TODO w.r.t. original
      return new LogCatalogRegionFormat.LogCatalogFile(location,
          uuid, nextNsid, nextTblid, nsids, nsVersion, nsProperties, tblIds, tblVersion, tblLocations);
    }

    void setGlobals(UUID uuid, int nextNsid, int nextTblid) {
      Preconditions.checkArgument(this.uuid == null, "UUID already set");
      this.uuid = uuid;
      this.nextNsid = nextNsid;
      this.nextTblid = nextTblid;
    }

    void addNamespaceInternal(String name, int parentId, int nsid, int version) {
      Preconditions.checkNotNull(name, "Namespace name cannot be null");
      final Namespace ns;
      if (nsid == 0) {
        Preconditions.checkArgument(nsids.isEmpty(), "Root namespace already exists");
        Preconditions.checkArgument(
            parentId == 0, "Invalid parent id for root namespace: %d", parentId);
        Preconditions.checkArgument(name.isEmpty(), "Invalid name for root namespace: %s", name);
        ns = Namespace.empty();
      } else {
        Namespace parent = nsLookup.get(parentId);
        if (null == parent) {
          throw new IllegalStateException("Invalid parent namespace: " + parentId);
        }
        String[] levels = Arrays.copyOf(parent.levels(), parent.levels().length + 1);
        levels[levels.length - 1] = name;
        ns = Namespace.of(levels);
      }
      if (nsids.put(ns, nsid) != null) {
        throw new IllegalStateException("Duplicate namespace: " + ns);
      }
      nsLookup.put(nsid, ns);
      nsVersion.put(nsid, version);
    }

    // add property to namespace without incrementing version
    void addNamespacePropertyInternal(int nsid, String key, String value) {
      Preconditions.checkArgument(nsLookup.containsKey(nsid), "Invalid namespace: %s", nsid);
      Map<String, String> props = nsProperties.computeIfAbsent(nsid, k -> Maps.newHashMap());
      props.put(key, value);
    }

    void dropNamespaceInternal(int nsid) {
      // TODO Precondition: ensure no tables exist in this namespace (should be checked on insert)
      Namespace ns = nsLookup.remove(nsid);
      if (null == ns) {
        throw new IllegalStateException("Invalid namespace: " + nsid);
      }
      nsids.remove(ns);
      nsVersion.remove(nsid);
    }

    void addTableInternal(int tblId, int nsid, int version, String name, String location) {
      Namespace ns = nsLookup.get(nsid);
      if (null == ns) {
        throw new IllegalStateException("Invalid namespace: " + nsid);
      }
      TableIdentifier ti = TableIdentifier.of(ns, name);
      if (tblIds.put(ti, tblId) != null) {
        throw new IllegalStateException("Duplicate table: " + ti);
      }
      tblLocations.put(tblId, location);
      tblVersion.put(tblId, version);
    }

    void dropTableInternal(int tblId) {
      // TODO Precondition: ensure no tables exist in this namespace (should be checked on insert)
      tblLocations.remove(tblId);
      tblVersion.remove(tblId);
    }

    void updateTableInternal(int tblId, int version, String location) {
      tblLocations.put(tblId, location);
      tblVersion.put(tblId, version);
    }

    void logAction(LogAction.Type action) {
      switch (action) {
        case CREATE_TABLE:
        case UPDATE_TABLE:
        case DROP_TABLE:
          throw new UnsupportedOperationException();
        case CREATE_NAMESPACE:
        case UPDATE_NAMESPACE:
          // nsVersion matches action version
        case DROP_NAMESPACE:
        default:
          throw new UnsupportedOperationException();
      }
    }

    @Override
    public CatalogFile commit(SupportsAtomicOperations fileIO) {
      try {
        // TODO
        // CatalogFile catalog = merge();
        // final AtomicOutputFile<CAS> outputFile = fileIO.newOutputFile(original.location());
        // try {
        //     byte[] ffs = asBytes(catalog);
        //     try (ByteArrayInputStream serBytes = new ByteArrayInputStream(asBytes(catalog))) {
        //         serBytes.mark(ffs.length); // readAheadLimit ignored, but whatever
        //         CAS token = outputFile.prepare(() -> serBytes, AtomicOutputFile.Strategy.APPEND);
        //         serBytes.reset();
        //         InputFile newCatalog = outputFile.writeAtomic(token, () -> serBytes);
        //         return new CatalogFile(
        //                 catalog.uuid(),
        //                 catalog.seqno(),
        //                 catalog.namespaceProperties(),
        //                 catalog.tableMetadata(),
        //                 newCatalog);
        //     }
        // } catch (IOException e) {
        //     throw new CommitFailedException(e, "Failed to commit catalog file");
        // }
      } catch (SupportsAtomicOperations.CASException e) {
        throw new CommitFailedException(e, "Cannot commit");
      }
      return null;
    }
  }

  // UUID v7 generator ; useful for transaction IDs
  static UUID generate() {
    long timestamp = System.currentTimeMillis();
    long unixTsMs = timestamp & 0xFFFFFFFFFFFFL; // 48 bits for timestamp

    // Randomness: 12 bits for unique sequencing within the millisecond
    long randA = random.nextInt(0x1000) & 0x0FFF; // 12 bits

    // Construct the most significant 64 bits
    long msb = (unixTsMs << 16) | (0x7L << 12) | randA; // Version 7 (0111)

    // 62 bits of randomness + UUID variant
    long randB = random.nextLong() & 0x3FFFFFFFFFFFFFFFL; // 62 bits
    long lsb = (0x2L << 62) | randB; // Variant bits: 10x (RFC 4122)

    return new UUID(msb, lsb);
  }
}
