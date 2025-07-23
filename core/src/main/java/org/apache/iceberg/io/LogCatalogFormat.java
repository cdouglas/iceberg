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
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.util.LimitInputStream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.log.actions.CheckpointAction;
import org.apache.iceberg.io.log.actions.LogAction;
import org.apache.iceberg.io.log.actions.namespace.AddNamespacePropertyAction;
import org.apache.iceberg.io.log.actions.namespace.CreateNamespaceAction;
import org.apache.iceberg.io.log.actions.namespace.DropNamespaceAction;
import org.apache.iceberg.io.log.actions.namespace.DropNamespacePropertyAction;
import org.apache.iceberg.io.log.actions.table.CreateTableAction;
import org.apache.iceberg.io.log.actions.table.DropTableAction;
import org.apache.iceberg.io.log.actions.table.ReadTableAction;
import org.apache.iceberg.io.log.actions.table.UpdateTableAction;
import org.apache.iceberg.io.log.serialization.LogActionSerializer;
import org.apache.iceberg.io.log.state.CatalogState;
import org.apache.iceberg.io.log.transactions.TransactionAction;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

@SuppressWarnings("checkstyle:VisibilityModifier")
public class LogCatalogFormat
    implements CatalogFormat<LogCatalogFormat.LogCatalogFile, LogCatalogFormat.Mut> {

  public LogCatalogFormat() {
    this(Collections.emptyMap());
  }

  public LogCatalogFormat(Map<String, String> properties) {}

  @Override
  public CatalogFile.Mut<LogCatalogFile, Mut> empty(InputFile input) {
    return new Mut(input);
  }

  @Override
  public CatalogFile.Mut<LogCatalogFile, Mut> from(CatalogFile other) {
    if (!(other instanceof LogCatalogFile)) {
      throw new IllegalArgumentException("Cannot convert to LogCatalogFile: " + other);
    }
    return new Mut((LogCatalogFile) other);
  }

  @Override
  public LogCatalogFile read(SupportsAtomicOperations fileIO, InputFile catalogLocation) {
    // intentionally drop metadata cached on InputFile
    InputFile refresh = fileIO.newInputFile(catalogLocation.location());
    Mut catalog = new Mut(refresh);
    try (SeekableInputStream in = refresh.newStream()) {
      return readInternal(catalog, in, (int) refresh.getLength());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static LogCatalogFile readInternal(Mut catalog, InputStream in) throws IOException {
    return readInternal(catalog, in, Integer.MAX_VALUE);
  }

  @VisibleForTesting
  static LogCatalogFile readInternal(Mut catalog, InputStream in, int catalogLen)
      throws IOException {
    try (DataInputStream din = new DataInputStream(in)) {
      if (din.readByte() != LogAction.Type.CHECKPOINT.opcode) {
        throw new IllegalStateException("Invalid magic bits");
      }

      CheckpointAction chk = CheckpointAction.deserialize(din);
      chk.apply(catalog.stateBuilder);

      byte[] chkBytes = new byte[(int) chk.chkLen()];
      IOUtil.readFully(in, chkBytes, 0, chkBytes.length);
      InputStream chkStream = new DataInputStream(new ByteArrayInputStream(chkBytes));

      try (DataInputStream chkDis = new DataInputStream(chkStream)) {
        while (chkDis.available() > 0) {
          LogAction action = LogActionSerializer.deserialize(chkDis);
          action.apply(catalog.stateBuilder);
        }
      }

      if (chk.tblEmbedEnd() != 0) {
        // TODO embed table region
        throw new IllegalStateException("TODO");
      }

      byte[] txnBytes = new byte[(int) chk.committedTxnLen()];
      IOUtil.readFully(in, txnBytes, 0, txnBytes.length);
      readCommittedTransactions(catalog.stateBuilder, txnBytes);

      final InputStream logStream;
      if (catalogLen == Integer.MAX_VALUE) {
        // TODO HACK to get around current callers in tests backed by byte arrays
        logStream = new LimitInputStream(in, Integer.MAX_VALUE);
      } else {
        byte[] logBytes =
            new byte
                [catalogLen
                    - chk.length()
                    - chk.chkLen()
                    - chk.tblEmbedEnd()
                    - chk.committedTxnLen()];
        IOUtil.readFully(in, logBytes, 0, logBytes.length);
        logStream = new ByteArrayInputStream(logBytes);
      }

      try (DataInputStream logDis = new DataInputStream(logStream)) {
        while (logDis.available() > 0) {
          LogAction logAction = LogActionSerializer.deserialize(logDis);
          TransactionAction txn = (TransactionAction) logAction;
          // Build state once for verification to avoid multiple build() calls
          CatalogState verificationState = catalog.stateBuilder.build();
          if (txn.verify(verificationState)) {
            for (LogAction action : txn.actions()) {
              action.apply(catalog.stateBuilder);
            }
            catalog.stateBuilder.addCommittedTransaction(txn.txnId());
          }
          if (txn.isSealed()) {
            catalog.stateBuilder.setSealed();
            break;
          }
        }
      }

      return catalog.build();
    }
  }

  private static void readCommittedTransactions(CatalogState.Builder builder, byte[] txnBytes) {
    if (txnBytes.length == 0) {
      return;
    }

    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(txnBytes))) {
      while (dis.available() > 0) {
        UUID txnId = new UUID(dis.readLong(), dis.readLong());
        builder.addCommittedTransaction(txnId);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read committed transactions", e);
    }
  }

  public static class LogCatalogFile extends CatalogFile {
    private final CatalogState catalogState;

    // Create empty catalog file
    LogCatalogFile(InputFile location) {
      super(location);
      // Initialize CatalogState with the UUID from the base class
      CatalogState.Builder builder = new CatalogState.Builder();
      builder.setGlobals(uuid(), 1, 1);
      // NamespaceRegistry.builder() already includes the root namespace
      this.catalogState = builder.build();
    }

    LogCatalogFile(InputFile location, CatalogState catalogState) {
      super(catalogState.catalogUuid(), location);
      this.catalogState = catalogState;
    }

    LogCatalogFile(
        InputFile location,
        UUID catalogUuid,
        int nextNsid,
        int nextTblid,
        boolean sealed,
        Map<Namespace, Integer> nsids,
        Map<Integer, Integer> nsVersion,
        Map<Integer, Map<String, String>> nsProperties,
        Map<TableIdentifier, Integer> tblIds,
        Map<Integer, Integer> tblVersion,
        Map<Integer, String> tblLocations,
        Set<UUID> committedTxn) {

      super(location);

      // Build CatalogState from legacy parameters
      CatalogState.Builder builder = new CatalogState.Builder();
      builder.setGlobals(catalogUuid, nextNsid, nextTblid);
      if (sealed) {
        builder.setSealed();
      }

      // Add namespaces
      for (Map.Entry<Namespace, Integer> entry : nsids.entrySet()) {
        Namespace ns = entry.getKey();
        Integer nsid = entry.getValue();
        Integer version = nsVersion.get(nsid);

        if (ns.isEmpty()) {
          builder.namespaceBuilder().addNamespace("", 0, nsid, version != null ? version : 1);
        } else {
          Namespace parent =
              ns.length() > 1
                  ? Namespace.of(Arrays.copyOf(ns.levels(), ns.length() - 1))
                  : Namespace.empty();
          Integer parentId = nsids.get(parent);
          String name = ns.levels()[ns.length() - 1];
          builder
              .namespaceBuilder()
              .addNamespace(
                  name, parentId != null ? parentId : 0, nsid, version != null ? version : 1);
        }

        // Add namespace properties
        Map<String, String> props = nsProperties.get(nsid);
        if (props != null) {
          for (Map.Entry<String, String> prop : props.entrySet()) {
            builder.namespaceBuilder().addProperty(nsid, prop.getKey(), prop.getValue());
          }
        }
      }

      // Add tables
      for (Map.Entry<TableIdentifier, Integer> entry : tblIds.entrySet()) {
        TableIdentifier tableId = entry.getKey();
        Integer tblId = entry.getValue();
        Integer version = tblVersion.get(tblId);
        String tableLocation = tblLocations.get(tblId);

        if (version != null && tableLocation != null) {
          builder.tableBuilder().addTable(tblId, tableId, version, tableLocation);
        }
      }

      // Add committed transactions
      for (UUID txnId : committedTxn) {
        builder.addCommittedTransaction(txnId);
      }

      this.catalogState = builder.build();
    }

    @Override
    public Set<Namespace> namespaces() {
      return catalogState.namespaces();
    }

    @Override
    public Map<String, String> namespaceProperties(Namespace namespace) {
      return catalogState.namespaceProperties(namespace);
    }

    @Override
    public boolean containsNamespace(Namespace namespace) {
      return catalogState.containsNamespace(namespace);
    }

    @Override
    public List<TableIdentifier> tables() {
      return Lists.newArrayList(catalogState.tableRegistry().tables());
    }

    @Override
    public String location(TableIdentifier table) {
      return catalogState.tableLocation(table);
    }

    @Override
    public boolean createsHierarchicalNamespaces() {
      return true;
    }

    @Override
    Map<Namespace, Map<String, String>> namespaceProperties() {
      Map<Namespace, Map<String, String>> result = Maps.newHashMap();
      for (Namespace ns : catalogState.namespaces()) {
        result.put(ns, catalogState.namespaceProperties(ns));
      }
      return result;
    }

    @Override
    Map<TableIdentifier, String> locations() {
      Map<TableIdentifier, String> result = Maps.newHashMap();
      for (TableIdentifier tableId : catalogState.tableRegistry().tables()) {
        String location = catalogState.tableLocation(tableId);
        if (location != null) {
          result.put(tableId, location);
        }
      }
      return result;
    }

    public boolean containsTransaction(UUID txnId) {
      return catalogState.containsTransaction(txnId);
    }

    public boolean sealed() {
      return catalogState.sealed();
    }

    public UUID catalogUuid() {
      return catalogState.catalogUuid();
    }

    public int nextNsid() {
      return catalogState.nextNsid();
    }

    public int nextTblid() {
      return catalogState.nextTblid();
    }

    CatalogState catalogState() {
      return catalogState;
    }

    void writeCheckpoint(ByteArrayOutputStream baos) throws IOException {
      try (DataOutputStream dos = new DataOutputStream(baos)) {
        // Calculate sizes for checkpoint data and committed transactions
        ByteArrayOutputStream chkStream = new ByteArrayOutputStream();
        writeCheckpointData(chkStream);
        byte[] chkBytes = chkStream.toByteArray();

        ByteArrayOutputStream txnStream = new ByteArrayOutputStream();
        writeCommittedTransactions(txnStream);
        byte[] txnBytes = txnStream.toByteArray();

        // Write checkpoint action (this includes the opcode)
        Integer rootVersion = catalogState.namespaceVersion(0);
        CheckpointAction checkpoint =
            new CheckpointAction(
                catalogState.catalogUuid(),
                catalogState.nextNsid(),
                catalogState.nextTblid(),
                chkBytes.length,
                0, // tblEmbedEnd
                txnBytes.length,
                rootVersion != null ? rootVersion : 1);
        checkpoint.serialize(dos);

        // Write checkpoint data
        dos.write(chkBytes);

        // Write committed transactions
        dos.write(txnBytes);
      }
    }

    private void writeCheckpointData(ByteArrayOutputStream baos) throws IOException {
      try (DataOutputStream dos = new DataOutputStream(baos)) {
        // Write namespace actions (skip empty namespace as it's implicit)
        for (Namespace ns : catalogState.namespaces()) {
          if (ns.isEmpty()) {
            continue; // Skip the root namespace as it's implicitly created
          }
          
          Integer nsid = catalogState.namespaceRegistry().namespaceId(ns);
          Integer version = catalogState.namespaceVersion(nsid);

          {
            Namespace parent =
                ns.length() > 1
                    ? Namespace.of(Arrays.copyOf(ns.levels(), ns.length() - 1))
                    : Namespace.empty();
            Integer parentId = catalogState.namespaceRegistry().namespaceId(parent);
            Integer parentVersion = catalogState.namespaceVersion(parentId);
            String name = ns.levels()[ns.length() - 1];
            CreateNamespaceAction action = new CreateNamespaceAction(name, nsid, version, parentId, parentVersion);
            action.serialize(dos);
          }

          // Write namespace properties
          Map<String, String> props = catalogState.namespaceProperties(ns);
          for (Map.Entry<String, String> prop : props.entrySet()) {
            AddNamespacePropertyAction propAction =
                new AddNamespacePropertyAction(nsid, version, prop.getKey(), prop.getValue());
            propAction.serialize(dos);
          }
        }

        // Write table actions
        for (TableIdentifier tableId : catalogState.tableRegistry().tables()) {
          Integer tblId = catalogState.tableRegistry().tableId(tableId);
          Integer version = catalogState.tableVersion(tblId);
          String location = catalogState.tableLocation(tableId);
          Integer nsid = catalogState.namespaceRegistry().namespaceId(tableId.namespace());

          if (tblId != null && version != null && location != null && nsid != null) {
            Integer nsVersion = catalogState.namespaceVersion(nsid);
            CreateTableAction action =
                new CreateTableAction(tableId.name(), tblId, version, nsid, nsVersion, location, tableId.namespace());
            action.serialize(dos);
          }
        }
      }
    }

    private void writeCommittedTransactions(ByteArrayOutputStream baos) throws IOException {
      try (DataOutputStream dos = new DataOutputStream(baos)) {
        for (UUID txnId : catalogState.committedTransactions()) {
          dos.writeLong(txnId.getMostSignificantBits());
          dos.writeLong(txnId.getLeastSignificantBits());
        }
      }
    }
  }

  public static class Mut extends CatalogFile.Mut<LogCatalogFile, Mut> {
    final CatalogState.Builder stateBuilder;

    // empty
    Mut(InputFile input) {
      this(new LogCatalogFile(input));
    }

    // changes to be applied to this catalog
    Mut(LogCatalogFile other) {
      super(other);
      this.stateBuilder = new CatalogState.Builder(other.catalogState());
    }

    LogCatalogFile build() {
      // If no mutations were made through Mut methods, use stateBuilder directly
      // This handles the case where readInternal has populated stateBuilder
      if (namespaces.isEmpty() && tables.isEmpty() && readTables.isEmpty() && namespaceProperties.isEmpty()) {
        return new LogCatalogFile(original.location(), stateBuilder.build());
      }
      
      // Otherwise, build current state from changes applied through Mut methods
      return new LogCatalogFile(original.location(), buildCurrentState());
    }

    private CatalogState buildCurrentState() {
      // Create actions and apply them with proper ordering using the existing stateBuilder
      // This preserves remapping information that was set up during createOrderedActions()
      List<LogAction> actions = createOrderedActions();
      for (LogAction action : actions) {
        action.apply(stateBuilder);
      }
      
      return stateBuilder.build();
    }
    
    private List<LogAction> createOrderedActions() {
      List<LogAction> actions = Lists.newArrayList();
      CatalogState originalState = stateBuilder.build();
      int virtualId = -1;
      Map<Namespace, Integer> newNsIds = Maps.newHashMap();
      Map<Namespace, Integer> virtualNsIds = Maps.newHashMap(); // Track virtual IDs before remapping
      
      // Create namespaces in dependency order (parents before children)
      List<Namespace> namespacesToCreate = Lists.newArrayList();
      for (Namespace ns : namespaces.keySet()) {
        Boolean isCreate = namespaces.get(ns);
        if (isCreate && !ns.isEmpty()) { // Don't create the root namespace as it already exists
          namespacesToCreate.add(ns);
        }
      }
      
      // Sort namespaces by depth to ensure parents are created before children
      namespacesToCreate.sort((a, b) -> Integer.compare(a.length(), b.length()));
      
      for (Namespace ns : namespacesToCreate) {
        // Skip if already exists in original state
        if (originalState.containsNamespace(ns)) {
          continue;
        }
        
        // Create namespace action
        Integer vId = virtualId--;
        virtualNsIds.put(ns, vId); // Store virtual ID before remapping
        Integer nsid = stateBuilder.remap(vId);
        newNsIds.put(ns, nsid);
        
        Namespace parent =
            ns.length() > 1
                ? Namespace.of(Arrays.copyOf(ns.levels(), ns.length() - 1))
                : Namespace.empty();
        Integer parentId = originalState.namespaceRegistry().namespaceId(parent);
        if (parentId == null) {
          parentId = newNsIds.get(parent);
        }
        if (parentId == null) {
          parentId = 0; // Default to root
        }
        
        String name = ns.levels()[ns.length() - 1];
        actions.add(new CreateNamespaceAction(name, nsid, parentId, 1));
        
        // Add namespace properties
        Map<String, String> props = namespaceProperties.get(ns);
        if (props != null) {
          for (Map.Entry<String, String> prop : props.entrySet()) {
            actions.add(new AddNamespacePropertyAction(nsid, 1, prop.getKey(), prop.getValue()));
          }
        }
      }
      
      // Create table actions
      for (Map.Entry<TableIdentifier, String> entry : tables.entrySet()) {
        TableIdentifier tableId = entry.getKey();
        String location = entry.getValue();
        
        if (location != null) {
          Integer nsid = originalState.namespaceRegistry().namespaceId(tableId.namespace());
          if (nsid == null) {
            // Table in a namespace being created in this transaction - use late-bound constructor
            Integer virtualNsId = virtualNsIds.get(tableId.namespace());
            if (virtualNsId != null) {
              actions.add(new CreateTableAction(tableId.name(), virtualNsId, location, tableId.namespace()));
            } else {
              // This should not happen - table references non-existent namespace
              throw new IllegalStateException("Table " + tableId + " references non-existent namespace: " + tableId.namespace());
            }
          } else {
            // Table in existing namespace - use normal constructor
            Integer nsVersion = originalState.namespaceVersion(nsid);
            actions.add(new CreateTableAction(tableId.name(), nsid, nsVersion != null ? nsVersion : 1, location, tableId.namespace()));
          }
        }
      }
      
      return actions;
    }

    /**
     * Compute a transaction that represents the difference between the original catalog state and
     * the changes applied subsequently.
     */
    TransactionAction diff() {
      return diffFrom((LogCatalogFile) original);
    }

    /**
     * Compute a transaction that represents the difference between the given base catalog state and
     * the changes applied subsequently.
     */
    TransactionAction diffFrom(LogCatalogFile baseCatalog) {
      List<LogAction> actions = Lists.newArrayList();

      CatalogState originalState = baseCatalog.catalogState();
      CatalogState currentState = stateBuilder.build();
      
      int virtualId = -1;
      Map<Namespace, Integer> newNsIds = Maps.newHashMap();
      Map<Namespace, Integer> virtualNsIds = Maps.newHashMap(); // Track virtual IDs before remapping

      // Create/delete namespaces in dependency order (parents before children)
      List<Namespace> namespacesToCreate = Lists.newArrayList();
      for (Namespace ns : namespaces.keySet()) {
        Boolean isCreate = namespaces.get(ns);
        if (isCreate && !ns.isEmpty()) { // Don't create the root namespace as it already exists
          namespacesToCreate.add(ns);
        }
      }
      
      // Sort namespaces by depth to ensure parents are created before children
      namespacesToCreate.sort((a, b) -> Integer.compare(a.length(), b.length()));
      
      for (Namespace ns : namespacesToCreate) {
        // Create namespace
        Integer vId = virtualId--;
        virtualNsIds.put(ns, vId); // Store virtual ID before remapping
        Integer nsid = stateBuilder.remap(vId); // Assign new namespace ID with unique virtual ID
        newNsIds.put(ns, nsid);
        Integer parentId = 0;
        
        Namespace parent =
            ns.length() > 1
                ? Namespace.of(Arrays.copyOf(ns.levels(), ns.length() - 1))
                : Namespace.empty();
        parentId = originalState.namespaceRegistry().namespaceId(parent);
        if (parentId == null) {
          // Check if parent was created in this transaction
          parentId = newNsIds.get(parent);
        }
        if (parentId == null) {
          // Parent doesn't exist - this should not happen because CatalogFile.Mut
          // automatically creates parent namespaces, but default to root
          parentId = 0;
        }
        
        String name = ns.levels()[ns.length() - 1];
        Integer parentVersion = originalState.namespaceVersion(parentId);
        if (parentVersion == null) {
          parentVersion = 1; // Default version for new namespaces
        }
        actions.add(new CreateNamespaceAction(name, vId, parentId, parentVersion));
      }
      
      // Handle namespace deletions
      for (Namespace ns : namespaces.keySet()) {
        Boolean isCreate = namespaces.get(ns);
        if (!isCreate) {
          // Drop namespace
          Integer nsid = originalState.namespaceRegistry().namespaceId(ns);
          Integer version = originalState.namespaceVersion(nsid);
          if (nsid != null && version != null) {
            actions.add(new DropNamespaceAction(nsid, version));
          }
        }
      }

      // Namespace property changes
      for (Map.Entry<Namespace, Map<String, String>> entry : namespaceProperties.entrySet()) {
        Namespace ns = entry.getKey();
        Map<String, String> newProps = entry.getValue();
        Integer nsid = originalState.namespaceRegistry().namespaceId(ns);
        if (nsid == null) {
          nsid = newNsIds.get(ns); // New namespace created in this transaction
        }
        Integer version = originalState.namespaceVersion(nsid);
        if (version == null) {
          version = 1;
        }

        Map<String, String> oldProps = originalState.namespaceProperties(ns);
        if (oldProps == null) {
          oldProps = Collections.emptyMap();
        }

        // Add new properties
        for (Map.Entry<String, String> prop : newProps.entrySet()) {
          if (!oldProps.containsKey(prop.getKey())
              || !oldProps.get(prop.getKey()).equals(prop.getValue())) {
            actions.add(
                new AddNamespacePropertyAction(nsid, version, prop.getKey(), prop.getValue()));
          }
        }

        // Remove deleted properties
        for (String key : oldProps.keySet()) {
          if (!newProps.containsKey(key)) {
            actions.add(new DropNamespacePropertyAction(nsid, version, key));
          }
        }
      }

      // Table operations
      for (Map.Entry<TableIdentifier, String> entry : tables.entrySet()) {
        TableIdentifier tableId = entry.getKey();
        String location = entry.getValue();

        if (location != null) {
          // Create/update table
          Integer tblId = originalState.tableRegistry().tableId(tableId);
          if (tblId == null) {
            // Create table
            Integer nsid = originalState.namespaceRegistry().namespaceId(tableId.namespace());
            if (nsid == null) {
              // Table in a namespace being created in this transaction - use late-bound constructor
              Integer virtualNsId = virtualNsIds.get(tableId.namespace());
              if (virtualNsId != null) {
                actions.add(new CreateTableAction(tableId.name(), virtualNsId, location, tableId.namespace()));
              } else {
                // This should not happen - table references non-existent namespace
                throw new IllegalStateException("Table " + tableId + " references non-existent namespace: " + tableId.namespace());
              }
            } else {
              // Table in existing namespace - use normal constructor
              Integer nsVersion = originalState.namespaceVersion(nsid);
              actions.add(new CreateTableAction(tableId.name(), nsid, nsVersion != null ? nsVersion : 1, location, tableId.namespace()));
            }
          } else {
            // Update table
            Integer version = originalState.tableVersion(tblId);
            actions.add(new UpdateTableAction(tblId, version, location, tableId));
          }
        } else {
          // Drop table
          Integer tblId = originalState.tableRegistry().tableId(tableId);
          Integer version = originalState.tableVersion(tblId);
          if (tblId != null && version != null) {
            actions.add(new DropTableAction(tblId, version, tableId));
          }
        }
      }

      // Read operations
      for (TableIdentifier tableId : readTables) {
        Integer tblId = originalState.tableRegistry().tableId(tableId);
        Integer version = originalState.tableVersion(tblId);
        if (tblId != null && version != null) {
          actions.add(new ReadTableAction(tblId, version, tableId));
        }
      }

      return new TransactionAction(UUID.randomUUID(), actions, false);
    }

    @Override
    public LogCatalogFile commit(SupportsAtomicOperations fileIO) {
      final long IO_ATTEMPTS = 10;
      final long MAX_CATALOG_SIZE = 16L * 1024 * 1024; // TODO from config/global
      LogCatalogFile baseCatalog = (LogCatalogFile) original;
      InputFile current = original.location();
      TransactionAction txn = diff();
      byte[] txnBytes = toBytes(txn);

      // case 0: initial commit of the catalog
      if (!current.exists()) {
        return tryCAS(current, txnBytes, fileIO)
            .orElseThrow(() -> new CommitFailedException("Cannot commit: catalog creation failed"));
      }
      
      // If there are no changes to commit to an existing catalog, return the original catalog
      if (txn.actions().isEmpty()) {
        return baseCatalog;
      }

      for (int attempts = 0; attempts < IO_ATTEMPTS; ++attempts) {
        // case 1: original LogCatalogFile is sealed
        if (baseCatalog.sealed()) {
          TransactionAction.unseal(txnBytes);
          Optional<LogCatalogFile> rslt = tryCAS(current, txnBytes, fileIO);
          if (rslt.isPresent()) {
            LogCatalogFile newCatalog = rslt.get();
            if (!newCatalog.containsTransaction(txn.txnId())) {
              throw new CommitFailedException("Cannot commit: conflicting, concurrent transaction");
            }
            return newCatalog;
          }
        } else {
          if (current.getLength() + txnBytes.length > MAX_CATALOG_SIZE) {
            TransactionAction.seal(txnBytes);
          }
          Optional<LogCatalogFile> rslt = tryAppend(current, txn, txnBytes, fileIO);
          if (rslt.isPresent()) {
            LogCatalogFile newCatalog = rslt.get();
            if (!newCatalog.containsTransaction(txn.txnId())) {
              throw new CommitFailedException("Cannot commit: conflicting, concurrent transaction");
            }
            return newCatalog;
          }
        }
        long oldLength = current.getLength();
        current = fileIO.newInputFile(current.location());
        if (current.getLength() < oldLength) {
          throw new CommitFailedException("Cannot commit: catalog shrunk during retry");
        }
        LogCatalogFormat format = new LogCatalogFormat();
        baseCatalog = format.read(fileIO, current);
        // Update stateBuilder with new base state
        this.stateBuilder.clear();
        this.stateBuilder.copyFrom(baseCatalog.catalogState());
        // Recompute transaction since state has changed, using updated base catalog
        txn = diffFrom(baseCatalog);
        
        // If there are no changes to commit after recomputation, return the base catalog
        if (txn.actions().isEmpty()) {
          return baseCatalog;
        }
        
        txnBytes = toBytes(txn);
      }

      throw new CommitFailedException("Cannot commit: exceeded maximum retry attempts");
    }

    private Optional<LogCatalogFile> tryCAS(
        InputFile current, byte[] txnBytes, SupportsAtomicOperations fileIO) {
      try {
        Preconditions.checkArgument(current.location().equals(original.location().location()));
        AtomicOutputFile outputFile = fileIO.newOutputFile(current);
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
          ((LogCatalogFile) original).writeCheckpoint(baos);
          baos.write(txnBytes);
          final byte[] checkpointBytes = baos.toByteArray();
          try (ByteArrayInputStream serBytes = new ByteArrayInputStream(checkpointBytes)) {
            serBytes.mark(checkpointBytes.length);
            CAS token = outputFile.prepare(() -> serBytes, AtomicOutputFile.Strategy.CAS);
            serBytes.reset();
            InputFile newCatalog = outputFile.writeAtomic(token, () -> serBytes);
            final Mut merged = new Mut(newCatalog);
            try (SeekableInputStream in = newCatalog.newStream()) {
              return Optional.of(readInternal(merged, in, (int) newCatalog.getLength()));
            }
          }
        }
      } catch (SupportsAtomicOperations.CASException e) {
        return Optional.empty();
      } catch (IOException e) {
        throw new CommitFailedException(e, "Cannot commit: %s", e.getMessage());
      }
    }

    private Optional<LogCatalogFile> tryAppend(
        InputFile current,
        TransactionAction txn,
        byte[] txnBytes,
        SupportsAtomicOperations fileIO) {
      try {
        AtomicOutputFile outputFile = fileIO.newOutputFile(current);
        try (ByteArrayInputStream serBytes = new ByteArrayInputStream(txnBytes)) {
          serBytes.mark(txnBytes.length);
          CAS token = outputFile.prepare(() -> serBytes, AtomicOutputFile.Strategy.APPEND);
          serBytes.reset();
          InputFile newCatalog = outputFile.writeAtomic(token, () -> serBytes);
          
          // Instead of re-reading the entire file, apply the transaction to current state
          final Mut merged = new Mut((LogCatalogFile) original);
          txn.apply(merged.stateBuilder);
          return Optional.of(new LogCatalogFile(newCatalog, merged.stateBuilder.build()));
        }
      } catch (SupportsAtomicOperations.AppendException e) {
        return Optional.empty();
      } catch (IOException e) {
        throw new UncheckedIOException("Cannot commit: " + e.getMessage(), e);
      }
    }

    static byte[] toBytes(TransactionAction txn) {
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
          DataOutputStream dos = new DataOutputStream(bos)) {
        txn.serialize(dos);
        return bos.toByteArray();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to write transaction", e);
      }
    }
  }
}
