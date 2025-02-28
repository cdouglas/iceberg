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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

@SuppressWarnings("checkstyle:VisibilityModifier")
public class LogCatalogFormat extends CatalogFormat {
  // UUID generation
  private static final Random random = new Random();

  @Override
  public CatalogFile.Mut empty(InputFile input) {
    return new Mut(input);
  }

  @Override
  public CatalogFile.Mut from(CatalogFile other) {
    if (!(other instanceof LogCatalogFile)) {
      throw new IllegalArgumentException("Cannot convert to LogCatalogFile: " + other);
    }
    return new Mut((LogCatalogFile) other);
  }

  @Override
  public CatalogFile read(SupportsAtomicOperations fileIO, InputFile catalogLocation) {
    // intentionally drop metadata cached on InputFile
    InputFile refresh = fileIO.newInputFile(catalogLocation.location());
    Mut catalog = new Mut(refresh);
    try (SeekableInputStream in = refresh.newStream()) {
      return readInternal(catalog, in);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @VisibleForTesting
  LogCatalogFile readInternal(Mut catalog, InputStream in) throws IOException {
    try (DataInputStream din = new DataInputStream(in)) {
      if (din.readByte() != LogAction.Type.CHECKPOINT.opcode) {
        throw new IllegalStateException("Invalid magic bits");
      }
      LogAction.Checkpoint chk = LogAction.Checkpoint.read(din);
      chk.apply(catalog);
      // TODO bound stream to iterator
      for (LogAction action : LogAction.chkIterator(din)) {
        // no validation necessary
        action.apply(catalog);
      }
      // TODO embed table regio
      if (chk.chkEnd != chk.tblEmbedEnd) {
        throw new IllegalStateException("No.");
      }
      // TODO buffer committedTxn, in case it's relevant
      // in.seek(chk.committedTxnEnd);
      for (LogAction.Transaction txn : LogAction.logIterator(din)) {
        if (txn.verify(catalog)) {
          txn.apply(catalog);
        }
        if (txn.sealed) {
          catalog.setSealed();
          break;
        }
      }
      return catalog.merge();
    }
  }

  // Log format
  // <len><op><payload>
  abstract static class LogAction {
    enum Type {
      CHECKPOINT(
          0), // <UUID> <next_nsid> <next_tblid> <chk_end> <tbl_embed_end> <committed_txn_end>
      CREATE_TABLE(1), // <nsid> <ns_ver> <name> <location>
      UPDATE_TABLE(2), // <tbl_id> <tbl_ver> <location>
      DROP_TABLE(3), // <tbl_id> <tbl_ver>
      CREATE_NAMESPACE(4), // <parent_id> <parent_version> <name>
      DROP_NAMESPACE(5), // <nsid> <version>
      ADD_NAMESPACE_PROPERTY(6), // <nsid> <version> <key> <value>
      DROP_NAMESPACE_PROPERTY(7), // <nsid> <version> <key>
      TRANSACTION(8); // <txid> <sealed> <n_actions> <action>*

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

    abstract boolean verify(Mut catalog);

    abstract void apply(Mut catalog);

    abstract void write(DataOutputStream dos) throws IOException;

    static class Checkpoint extends LogAction {
      final UUID catalogUUID;
      final int nextNsid;
      final int nextTblid;
      final long chkEnd;
      final long tblEmbedEnd;
      final long committedTxnEnd;

      Checkpoint(
          UUID catalogUUID,
          int nextNsid,
          int nextTblid,
          long chkEnd,
          long tblEmbedEnd,
          long committedTxnEnd) {
        this.catalogUUID = catalogUUID;
        this.nextNsid = nextNsid;
        this.nextTblid = nextTblid;
        this.chkEnd = chkEnd;
        this.tblEmbedEnd = tblEmbedEnd;
        this.committedTxnEnd = committedTxnEnd;
      }

      @Override
      boolean verify(Mut catalog) {
        return true;
      }

      @Override
      void apply(Mut catalog) {
        catalog.setGlobals(catalogUUID, nextNsid, nextTblid);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.CHECKPOINT.opcode);
        // TODO write additional, magic bits + version
        dos.writeLong(catalogUUID.getMostSignificantBits());
        dos.writeLong(catalogUUID.getLeastSignificantBits());
        dos.writeInt(nextNsid);
        dos.writeInt(nextTblid);
        dos.writeLong(chkEnd);
        dos.writeLong(tblEmbedEnd);
        dos.writeLong(committedTxnEnd);
      }

      static Checkpoint read(DataInputStream dis) throws IOException {
        long msb = dis.readLong();
        long lsb = dis.readLong();
        UUID catalogUUID = new UUID(msb, lsb);
        int nextNsid = dis.readInt();
        int nextTblid = dis.readInt();
        long chkEnd = dis.readLong();
        long tblEmbedEnd = dis.readLong();
        long committedTxnEnd = dis.readLong();
        return new Checkpoint(
            catalogUUID, nextNsid, nextTblid, chkEnd, tblEmbedEnd, committedTxnEnd);
      }
    }

    static class CreateNamespace extends LogAction {
      private static final int LATE_BIND = -1;
      final String name;
      final int logNsid;
      final int version;
      final int parentId;
      final int parentVersion;

      CreateNamespace(String name, int parentId, int parentVersion) {
        this(name, LATE_BIND, LATE_BIND, parentId, parentVersion);
      }

      CreateNamespace(String name, int logNsid, int version, int parentId, int parentVersion) {
        this.name = name;
        this.logNsid = logNsid;
        this.version = version;
        this.parentId = parentId;
        this.parentVersion = parentVersion;
      }

      @Override
      boolean verify(Mut catalog) {
        // concurrent creates are conflicts, but can be retried
        Integer version = catalog.nsVersion.get(parentId);
        return version != null && version == parentVersion;
      }

      @Override
      void apply(Mut catalog) {
        // increment parent version, assign uniq nsid
        final int nsid, version;
        if (logNsid == LATE_BIND) {
          nsid = catalog.nextNsid++;
          catalog.nsVersion.put(parentId, parentVersion + 1);
          version = 1;
        } else {
          nsid = logNsid;
          version = this.version;
        }
        catalog.addNamespaceInternal(name, parentId, nsid, version);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.CREATE_NAMESPACE.opcode);
        dos.writeUTF(name);
        dos.writeInt(logNsid);
        dos.writeInt(version);
        dos.writeInt(parentId);
        dos.writeInt(parentVersion);
      }

      static CreateNamespace read(DataInputStream dis) throws IOException {
        String name = dis.readUTF();
        int nsid = dis.readInt();
        int version = dis.readInt();
        int parentId = dis.readInt();
        int parentVersion = dis.readInt();
        return new CreateNamespace(name, nsid, version, parentId, parentVersion);
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
      boolean verify(Mut catalog) {
        Integer version = catalog.nsVersion.get(nsid);
        return version != null && version == this.version;
      }

      @Override
      void apply(Mut catalog) {
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

    static class AddNamespaceProperty extends LogAction {
      private static final int LATE_BIND = -1;
      final int nsid;
      final int version;
      final String key;
      final String value;

      AddNamespaceProperty(int nsid, String key, String value) {
        this(nsid, LATE_BIND, key, value);
      }

      AddNamespaceProperty(int nsid, int version, String key, String value) {
        this.nsid = nsid;
        this.version = version;
        this.key = key;
        this.value = value;
      }

      @Override
      boolean verify(Mut catalog) {
        Integer version = catalog.nsVersion.get(nsid);
        return version != null && version == this.version;
      }

      @Override
      void apply(Mut catalog) {
        if (version != LATE_BIND) {
          // from the log; increment the namespace version
          // TODO when building a transaction, make subsequent actions LATE_BIND
          catalog.nsVersion.put(nsid, version + 1);
        }
        catalog.addNamespacePropertyInternal(nsid, key, value);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.ADD_NAMESPACE_PROPERTY.opcode);
        dos.writeInt(nsid);
        dos.writeInt(version);
        dos.writeUTF(key);
        dos.writeUTF(value);
      }

      static AddNamespaceProperty read(DataInputStream dis) throws IOException {
        int nsid = dis.readInt();
        int version = dis.readInt();
        String key = dis.readUTF();
        String value = dis.readUTF();
        return new AddNamespaceProperty(nsid, version, key, value);
      }
    }

    static class DropNamespaceProperty extends LogAction {
      final int nsid;
      final int version;
      final String key;

      DropNamespaceProperty(int nsid, int version, String key) {
        this.nsid = nsid;
        this.version = version;
        this.key = key;
      }

      @Override
      boolean verify(Mut catalog) {
        Integer version = catalog.nsVersion.get(nsid);
        return version != null && version == this.version;
      }

      @Override
      void apply(Mut catalog) {
        catalog.nsVersion.put(nsid, version + 1);
        catalog.dropNamespacePropertyInternal(nsid, key);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.DROP_NAMESPACE_PROPERTY.opcode);
        dos.writeInt(nsid);
        dos.writeInt(version);
        dos.writeUTF(key);
      }

      static DropNamespaceProperty read(DataInputStream dis) throws IOException {
        int nsid = dis.readInt();
        int version = dis.readInt();
        String key = dis.readUTF();
        return new DropNamespaceProperty(nsid, version, key);
      }
    }

    static class CreateTable extends LogAction {
      private static final int LATE_BIND = -1;
      final String name;
      final int logTblId;
      final int tblVersion;
      final int nsid;
      final int nsVersion;
      final String location;

      CreateTable(String name, int nsid, int nsVersion, String location) {
        this(name, LATE_BIND, 1, nsid, nsVersion, location);
      }

      CreateTable(
          String name, int logTblId, int tblVersion, int nsid, int nsVersion, String location) {
        this.name = name;
        this.logTblId = logTblId;
        this.tblVersion = tblVersion;
        this.nsid = nsid;
        this.nsVersion = nsVersion;
        this.location = location;
      }

      @Override
      boolean verify(Mut catalog) {
        Integer version = catalog.nsVersion.get(nsid);
        return version != null && version == nsVersion;
      }

      @Override
      void apply(Mut catalog) {
        final int tblId = this.logTblId == LATE_BIND ? catalog.nextTblid++ : this.logTblId;
        catalog.addTableInternal(tblId, nsid, tblVersion, name, location);
      }

      @Override
      void write(DataOutputStream dos) throws IOException {
        dos.writeByte(Type.CREATE_TABLE.opcode);
        dos.writeUTF(name);
        dos.writeInt(logTblId);
        dos.writeInt(tblVersion);
        dos.writeInt(nsid);
        dos.writeInt(nsVersion);
        dos.writeUTF(location);
      }

      static CreateTable read(DataInputStream dis) throws IOException {
        String name = dis.readUTF();
        int tblId = dis.readInt();
        int tblVersion = dis.readInt();
        int nsid = dis.readInt();
        int nsVersion = dis.readInt();
        String location = dis.readUTF();
        return new CreateTable(name, tblId, tblVersion, nsid, nsVersion, location);
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
      boolean verify(Mut catalog) {
        Integer version = catalog.tblVersion.get(tblId);
        return version != null && version == this.version;
      }

      @Override
      void apply(Mut catalog) {
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
      boolean verify(Mut catalog) {
        Integer version = catalog.tblVersion.get(tblId);
        return version != null && version == this.version;
      }

      @Override
      void apply(Mut catalog) {
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
      boolean verify(Mut catalog) {
        return actions.stream().map(a -> a.verify(catalog)).reduce(true, (a, b) -> a && b);
      }

      @Override
      void apply(Mut catalog) {
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
            case ADD_NAMESPACE_PROPERTY:
              actions.add(AddNamespaceProperty.read(dis));
              break;
            case DROP_NAMESPACE_PROPERTY:
              actions.add(DropNamespaceProperty.read(dis));
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

    static Iterable<LogAction> chkIterator(final DataInputStream dis) throws IOException {
      return () -> new ChkStream(dis);
    }

    static Iterable<Transaction> logIterator(final DataInputStream dis) throws IOException {
      return () -> new LogStream(dis);
    }

    static class ChkStream implements Iterator<LogAction> {
      private final DataInputStream dis;

      ChkStream(DataInputStream dis) {
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
      public LogAction next() {
        try {
          Type type = Type.from(dis.readByte());
          switch (type) {
            case CHECKPOINT:
              return Checkpoint.read(dis);
            case CREATE_NAMESPACE:
              return CreateNamespace.read(dis);
            case ADD_NAMESPACE_PROPERTY:
              return AddNamespaceProperty.read(dis);
            case CREATE_TABLE:
              return CreateTable.read(dis);
            default:
              throw new IllegalArgumentException("Unknown action type: " + type);
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }

    static class LogStream implements Iterator<Transaction> {
      private final DataInputStream dis;

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
            case CHECKPOINT:
            case CREATE_TABLE:
            case UPDATE_TABLE:
            case DROP_TABLE:
            case CREATE_NAMESPACE:
            case ADD_NAMESPACE_PROPERTY:
            case DROP_NAMESPACE_PROPERTY:
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

  static class Mut extends CatalogFile.Mut {
    // namespace IDs are internal to the catalog format

    private UUID uuid = null;
    private int nextNsid = 1;
    private int nextTblid = 1;
    private boolean sealed = false;

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

    Mut(InputFile input) {
      this(new LogCatalogFile(input));
    }

    Mut(LogCatalogFile other) {
      super(other);
    }

    void setSealed() {
      this.sealed = true;
    }

    void setGlobals(UUID uuid, int nextNsid, int nextTblid) {
      Preconditions.checkArgument(this.uuid == null, "UUID already set");
      this.uuid = uuid;
      this.nextNsid = nextNsid;
      this.nextTblid = nextTblid;
      // TODO add compaction parameters so clients have the same criteria
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

    void dropNamespacePropertyInternal(int nsid, String key) {
      Preconditions.checkArgument(nsLookup.containsKey(nsid));
      Map<String, String> props = nsProperties.get(nsid);
      props.remove(key);
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

    LogCatalogFile merge() {
      // TODO compute diff with original
      return new LogCatalogFile(
          original.location(),
          uuid,
          nextNsid,
          nextTblid,
          sealed,
          Maps.newHashMap(nsids),
          Maps.newHashMap(nsVersion),
          Maps.newHashMap(nsProperties),
          Maps.newHashMap(tblIds),
          Maps.newHashMap(tblVersion),
          Maps.newHashMap(tblLocations));
    }

    List<LogAction> diff() {
      // TODO
      throw new UnsupportedOperationException("TODO");
    }

    @Override
    public LogCatalogFile commit(SupportsAtomicOperations fileIO) {
      try {
        // TODO
        throw new UnsupportedOperationException("TODO");
      } catch (SupportsAtomicOperations.CASException e) {
        throw new CommitFailedException(e, "Cannot commit");
      }
    }
  }

  // UUIDv7 generator ; useful for transaction IDs
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

  static class LogCatalogFile extends CatalogFile {
    final int nextNsid;
    final int nextTblid;
    final boolean sealed;

    private final Map<Namespace, Integer> nsids;
    private final Map<Integer, Integer> nsVersion;
    private final Map<Integer, Namespace> nsLookup;

    private final Map<Integer, Map<String, String>> nsProperties;

    private final Map<TableIdentifier, Integer> tblIds;
    private final Map<Integer, Integer> tblVersion;
    private final Map<Integer, String> tblLocations;

    // empty LogCatalogFile
    LogCatalogFile(InputFile location) {
      super(location);
      this.nextNsid = 1;
      this.nextTblid = 1;
      this.sealed = false;
      this.nsids = Maps.newHashMap();
      this.nsVersion = Maps.newHashMap();
      this.nsProperties = Maps.newHashMap();
      this.tblIds = Maps.newHashMap();
      this.tblVersion = Maps.newHashMap();
      this.tblLocations = Maps.newHashMap();
      this.nsLookup = Maps.newHashMap();
    }

    LogCatalogFile(
        InputFile location,
        UUID catalogUUID,
        int nextNsid,
        int nextTblid,
        boolean sealed,
        Map<Namespace, Integer> nsids,
        Map<Integer, Integer> nsVersion,
        Map<Integer, Map<String, String>> nsProperties,
        Map<TableIdentifier, Integer> tblIds,
        Map<Integer, Integer> tblVersion,
        Map<Integer, String> tblLocations) {
      super(catalogUUID, location);
      this.sealed = sealed;
      this.nextNsid = nextNsid;
      this.nextTblid = nextTblid;
      this.nsids = nsids;
      this.nsVersion = nsVersion;
      this.nsProperties = nsProperties;
      this.tblIds = tblIds;
      this.tblVersion = tblVersion;
      this.tblLocations = tblLocations;
      this.nsLookup =
          nsids.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    @Override
    public String location(TableIdentifier table) {
      final Integer tblId = tblIds.get(table);
      if (tblId == null) {
        return null;
      }
      return tblLocations.get(tblId);
    }

    @Override
    public Set<Namespace> namespaces() {
      return Collections.unmodifiableSet(nsids.keySet());
    }

    @Override
    public boolean containsNamespace(Namespace namespace) {
      return nsids.containsKey(namespace);
    }

    @Override
    public Map<String, String> namespaceProperties(Namespace namespace) {
      final Integer nsid = nsids.get(namespace);
      if (nsid == null) {
        return null;
      }
      return Collections.unmodifiableMap(nsProperties.get(nsid));
    }

    @Override
    public List<TableIdentifier> tables() {
      return Lists.newArrayList(tblIds.keySet().iterator());
    }

    @Override
    Map<Namespace, Map<String, String>> namespaceProperties() {
      return nsids.entrySet().stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey,
                  e -> nsProperties.getOrDefault(e.getValue(), Collections.emptyMap())));
    }

    @Override
    Map<TableIdentifier, String> locations() {
      return tblIds.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> tblLocations.get(e.getValue())));
    }

    List<LogAction> checkpointStream() {
      List<LogAction> actions = Lists.newArrayList();
      // TODO regions
      // TODO ensure properties of deleted namespaces are removed
      actions.add(new LogAction.Checkpoint(uuid(), nextNsid, nextTblid, -1, -1, -1));
      // sort by nsid; sufficient for parentId, since namespaces never move, are created in order
      for (Map.Entry<Namespace, Integer> e :
          nsids.entrySet().stream()
              .sorted(Comparator.comparingInt(Map.Entry::getValue))
              .collect(Collectors.toList())) {
        final Namespace ns = e.getKey();
        final int nsid = e.getValue();
        final int version = nsVersion.get(nsid);
        final int levels = ns.length();
        final Namespace parent =
            levels > 1
                ? Namespace.of(Arrays.copyOfRange(ns.levels(), 0, levels - 1))
                : Namespace.empty();
        final int parentId = nsids.get(parent);
        actions.add(
            new LogAction.CreateNamespace(
                0 == levels ? "" : ns.level(levels - 1),
                nsid,
                version,
                parentId,
                nsVersion.get(parentId)));
      }
      for (Map.Entry<Integer, Map<String, String>> e : nsProperties.entrySet()) {
        final int nsid = e.getKey();
        for (Map.Entry<String, String> prop : e.getValue().entrySet()) {
          actions.add(new LogAction.AddNamespaceProperty(nsid, prop.getKey(), prop.getValue()));
        }
      }
      for (Map.Entry<TableIdentifier, Integer> e : tblIds.entrySet()) {
        final TableIdentifier ti = e.getKey();
        final int tblId = e.getValue();
        final int version = tblVersion.get(tblId);
        final int nsid = nsids.get(ti.namespace());
        actions.add(
            new LogAction.CreateTable(
                ti.name(), tblId, version, nsid, nsVersion.get(nsid), tblLocations.get(tblId)));
      }
      return actions;
    }

    void write(OutputStream out) throws IOException {
      try (DataOutputStream dos = new DataOutputStream(out)) {
        for (LogAction action : checkpointStream()) {
          action.write(dos);
        }
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LogCatalogFile that = (LogCatalogFile) o;
      return nextNsid == that.nextNsid
          && nextTblid == that.nextTblid
          && Objects.equals(uuid(), that.uuid())
          && Objects.equals(nsids, that.nsids)
          && Objects.equals(nsVersion, that.nsVersion)
          && Objects.equals(nsProperties, that.nsProperties)
          && Objects.equals(tblIds, that.tblIds)
          && Objects.equals(tblVersion, that.tblVersion)
          && Objects.equals(tblLocations, that.tblLocations);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          uuid(),
          nextNsid,
          nextTblid,
          nsids,
          nsVersion,
          nsProperties,
          tblIds,
          tblVersion,
          tblLocations);
    }

    @Override
    public String toString() {
      return "LogCatalogFile{"
          + "catalogUUID="
          + uuid()
          + ", nextNsid="
          + nextNsid
          + ", nextTblid="
          + nextTblid
          + ", nsids="
          + nsids
          + ", nsVersion="
          + nsVersion
          + ", nsProperties="
          + nsProperties
          + ", tblIds="
          + tblIds
          + ", tblVersion="
          + tblVersion
          + ", tblLocations="
          + tblLocations
          + '}';
    }
  }
}
