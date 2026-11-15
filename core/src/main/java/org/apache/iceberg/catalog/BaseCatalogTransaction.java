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
package org.apache.iceberg.catalog;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.DataTableScan;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.TableScanContext;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdatePartitionStatistics;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.UpdateStatistics;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * A catalog transaction staged on the client: {@link #asCatalog()} stages every change to tables
 * and namespaces, and {@link #commitTransaction()} commits them in one {@link
 * SupportsCatalogTransactions#commitTransaction(CatalogCommit)}.
 *
 * <p>The transaction sees its own changes: a table it dropped no longer loads, one it renamed loads
 * under its new name only, one it created loads and takes updates, and listings include them. Each
 * table it loads is held as it first loaded it, and its updates are staged on that version. Under
 * {@link IsolationLevel#SERIALIZABLE} it records what it reads ({@link TableRead}): each scan's
 * planning, of a branch or tag, and each {@link #declareRead}. Replace transactions, {@code
 * registerTable} and views are not part of a catalog transaction and throw.
 */
public class BaseCatalogTransaction implements CatalogTransaction {
  private final Catalog origin;
  private final IsolationLevel isolationLevel;
  // Every table the transaction touched, in the order it did; byName holds those that resolve by
  // their names in this transaction, gone the names that no longer resolve to the origin's table
  private final List<Entry> entries = Lists.newArrayList();
  private final Map<TableIdentifier, Entry> byName = Maps.newHashMap();
  private final Set<TableIdentifier> gone = Sets.newHashSet();
  private final List<TableRead> reads = Lists.newArrayList();
  private final List<NamespaceChange> namespaceChanges = Lists.newArrayList();
  private final Map<Namespace, Map<String, String>> createdNamespaces = Maps.newLinkedHashMap();
  private final Set<Namespace> droppedNamespaces = Sets.newHashSet();
  private final Map<Namespace, Map<String, String>> setProperties = Maps.newHashMap();
  private final Map<Namespace, Set<String>> removedProperties = Maps.newHashMap();
  private boolean hasCommitted = false;

  public BaseCatalogTransaction(Catalog origin, IsolationLevel isolationLevel) {
    Preconditions.checkArgument(null != origin, "Invalid origin catalog: null");
    Preconditions.checkArgument(null != isolationLevel, "Invalid isolation level: null");
    Preconditions.checkArgument(
        origin instanceof SupportsCatalogTransactions,
        "Origin catalog does not support catalog transactions");
    this.origin = origin;
    this.isolationLevel = isolationLevel;
  }

  /** A table the transaction touched. */
  private static final class Entry {
    // its name when the transaction started, and its version as the transaction first read it;
    // both null for a table the transaction created
    private final TableIdentifier startName;
    private final TableMetadata base;
    private final Table loaded;
    private TableIdentifier name;
    // its staged updates: on base, or the create of a table the transaction created
    private Transaction tx;
    private boolean dropped;
    private boolean purge;

    private Entry(TableIdentifier name, TableMetadata base, Table loaded, Transaction tx) {
      this.startName = tx == null ? name : null;
      this.base = base;
      this.loaded = loaded;
      this.name = name;
      this.tx = tx;
    }

    private boolean created() {
      return startName == null;
    }

    private Table table() {
      return tx != null ? tx.table() : loaded;
    }
  }

  @Override
  public void commitTransaction() {
    Preconditions.checkState(!hasCommitted, "Transaction has already committed changes");

    try {
      CatalogCommit commit = toCommit();
      if (commit.hasChanges() || !commit.reads().isEmpty()) {
        ((SupportsCatalogTransactions) origin).commitTransaction(commit);
      }

      hasCommitted = true;
    } catch (CommitStateUnknownException e) {
      throw e;
    } catch (RuntimeException e) {
      rollback();
      throw e;
    }
  }

  /** The transaction's net effect on the catalog as it found it, and what it read. */
  private CatalogCommit toCommit() {
    ImmutableCatalogCommit.Builder commit =
        ImmutableCatalogCommit.builder()
            .isolationLevel(isolationLevel)
            .namespaceChanges(namespaceChanges);
    for (Entry entry : entries) {
      if (entry.created()) {
        if (!entry.dropped) {
          commit.putCreatedTables(entry.name, ((BaseTransaction) entry.tx).currentMetadata());
        }
      } else {
        TableChange change = tableChange(entry);
        if (change != null) {
          commit.addTableChanges(change);
        }
      }
    }

    if (IsolationLevel.SERIALIZABLE == isolationLevel) {
      commit.addAllReads(reads);
    }

    return commit.build();
  }

  /** What the transaction did to a table that existed when it started, or null if nothing. */
  private static TableChange tableChange(Entry entry) {
    TableCommit updates = null;
    if (!entry.dropped && entry.tx != null) {
      TableMetadata updated = ((BaseTransaction) entry.tx).currentMetadata();
      if (!updated.changes().isEmpty()) {
        updates = TableCommit.create(entry.startName, entry.base, updated);
      }
    }

    boolean renamed = !entry.dropped && !entry.name.equals(entry.startName);
    if (updates == null && !renamed && !entry.dropped) {
      return null;
    }

    return ImmutableTableChange.builder()
        .identifier(entry.startName)
        .base(entry.base)
        .commit(updates)
        .renamedTo(renamed ? entry.name : null)
        .dropped(entry.dropped)
        .purge(entry.purge)
        .build();
  }

  private void rollback() {
    entries.clear();
    byName.clear();
    gone.clear();
    reads.clear();
    namespaceChanges.clear();
    createdNamespaces.clear();
    droppedNamespaces.clear();
    setProperties.clear();
    removedProperties.clear();
  }

  @Override
  public Catalog asCatalog() {
    return new AsTransactionalCatalog();
  }

  @Override
  public IsolationLevel isolationLevel() {
    return isolationLevel;
  }

  @Override
  public void declareRead(TableIdentifier identifier) {
    Entry entry = entry(identifier);
    if (entry == null) {
      throw new NoSuchTableException("Table does not exist: %s", identifier);
    }

    if (IsolationLevel.SERIALIZABLE == isolationLevel && !entry.created()) {
      reads.add(TableRead.whole(entry.startName, entry.base));
    }
  }

  /**
   * The table {@code name} names in this transaction, loaded from the origin the first time; null
   * if there is none.
   */
  private Entry entry(TableIdentifier name) {
    Entry entry = byName.get(name);
    if (entry != null || gone.contains(name)) {
      return entry;
    }

    Table table;
    try {
      table = origin.loadTable(name);
    } catch (NoSuchTableException e) {
      return null;
    }

    if (!(table instanceof HasTableOperations)) {
      // a metadata table, which a transaction reads as the origin does and cannot change
      return null;
    }

    entry = new Entry(name, opsFromTable(table).current(), table, null);
    entries.add(entry);
    byName.put(name, entry);
    return entry;
  }

  /**
   * The updates staged on a table, on the version the transaction first read: a change since then
   * is judged by the catalog where the commit takes effect.
   */
  private Transaction txForTable(Entry entry) {
    if (entry.tx == null) {
      TableOperations ops = opsFromTable(entry.loaded);
      entry.tx = Transactions.newTransaction(entry.loaded.name(), ops, entry.base);
    }

    return entry.tx;
  }

  /**
   * A transaction of its own on a table of this one, staged on what this one staged, whose commit
   * stages its updates here: they commit with the catalog transaction, or not at all.
   */
  private Transaction nestedTransaction(Entry entry) {
    Transaction tx = txForTable(entry);
    TableOperations staging = opsFromTable(tx.table());
    return Transactions.newTransaction(tx.table().name(), staging, staging.current());
  }

  private static TableOperations opsFromTable(Table table) {
    return ((HasTableOperations) table).operations();
  }

  private SupportsNamespaces namespaces() {
    if (origin instanceof SupportsNamespaces) {
      return (SupportsNamespaces) origin;
    }

    throw new UnsupportedOperationException("Catalog does not support namespaces: " + origin);
  }

  private boolean namespaceExists(Namespace namespace) {
    if (namespace.isEmpty() || !(origin instanceof SupportsNamespaces)) {
      return true;
    }

    if (createdNamespaces.containsKey(namespace)) {
      return true;
    }

    return !droppedNamespaces.contains(namespace) && namespaces().namespaceExists(namespace);
  }

  private void checkNamespaceExists(Namespace namespace) {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
  }

  public class AsTransactionalCatalog implements Catalog, SupportsNamespaces {
    @Override
    public String name() {
      return origin.name();
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
      Entry entry = entry(identifier);
      if (entry == null) {
        if (!gone.contains(identifier)) {
          // a metadata table, or a NoSuchTableException
          return origin.loadTable(identifier);
        }

        throw new NoSuchTableException("Table does not exist: %s", identifier);
      }

      return new TransactionalTable(entry);
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      checkNamespaceExists(namespace);
      Set<TableIdentifier> tables = Sets.newLinkedHashSet();
      if (!createdNamespaces.containsKey(namespace)) {
        for (TableIdentifier table : origin.listTables(namespace)) {
          if (!gone.contains(table)) {
            tables.add(table);
          }
        }
      }

      for (Entry entry : byName.values()) {
        if (entry.name.namespace().equals(namespace)) {
          tables.add(entry.name);
        }
      }

      return Lists.newArrayList(tables);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      Entry entry = entry(identifier);
      if (entry == null) {
        return false;
      }

      entry.dropped = true;
      entry.purge = purge;
      byName.remove(identifier);
      gone.add(identifier);
      return true;
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
      Entry entry = entry(from);
      if (entry == null) {
        throw new NoSuchTableException("Cannot rename %s to %s. Table does not exist", from, to);
      }

      if (entry(to) != null) {
        throw new AlreadyExistsException("Cannot rename %s to %s. Table already exists", from, to);
      }

      checkNamespaceExists(to.namespace());
      byName.remove(from);
      gone.add(from);
      byName.put(to, entry);
      entry.name = to;
    }

    @Override
    public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
      return new StagedTableBuilder(identifier, schema);
    }

    @Override
    public Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
      throw new UnsupportedOperationException("Cannot register a table in a catalog transaction");
    }

    @Override
    public void invalidateTable(TableIdentifier identifier) {}

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> metadata) {
      namespaces();
      if (namespaceExists(namespace)) {
        throw new AlreadyExistsException("Namespace already exists: %s", namespace);
      }

      if (droppedNamespaces.contains(namespace)) {
        throw new UnsupportedOperationException(
            "Cannot create a namespace this catalog transaction dropped: " + namespace);
      }

      createdNamespaces.put(namespace, Maps.newHashMap(metadata));
      namespaceChanges.add(NamespaceChange.create(namespace, ImmutableMap.copyOf(metadata)));
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) {
      checkNamespaceExists(namespace);
      Set<Namespace> children = Sets.newLinkedHashSet();
      if (!createdNamespaces.containsKey(namespace)) {
        for (Namespace child : namespaces().listNamespaces(namespace)) {
          if (!droppedNamespaces.contains(child)) {
            children.add(child);
          }
        }
      }

      for (Namespace created : createdNamespaces.keySet()) {
        if (created.length() == namespace.length() + 1 && isPrefix(namespace, created)) {
          children.add(created);
        }
      }

      return Lists.newArrayList(children);
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
      checkNamespaceExists(namespace);
      Map<String, String> properties =
          createdNamespaces.containsKey(namespace)
              ? Maps.newHashMap(createdNamespaces.get(namespace))
              : Maps.newHashMap(namespaces().loadNamespaceMetadata(namespace));
      properties.putAll(setProperties.getOrDefault(namespace, ImmutableMap.of()));
      removedProperties.getOrDefault(namespace, ImmutableSet.of()).forEach(properties::remove);
      return properties;
    }

    @Override
    public boolean dropNamespace(Namespace namespace) {
      if (!namespaceExists(namespace)) {
        return false;
      }

      if (!listTables(namespace).isEmpty() || !listNamespaces(namespace).isEmpty()) {
        throw new NamespaceNotEmptyException("Namespace %s is not empty", namespace);
      }

      if (createdNamespaces.remove(namespace) != null) {
        // created and dropped by this transaction: neither happens
        namespaceChanges.removeIf(change -> change.namespace().equals(namespace));
      } else {
        droppedNamespaces.add(namespace);
        namespaceChanges.add(NamespaceChange.drop(namespace));
      }

      setProperties.remove(namespace);
      removedProperties.remove(namespace);
      return true;
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> properties) {
      checkNamespaceExists(namespace);
      setProperties.computeIfAbsent(namespace, ns -> Maps.newHashMap()).putAll(properties);
      Set<String> removed = removedProperties.get(namespace);
      if (removed != null) {
        removed.removeAll(properties.keySet());
      }

      namespaceChanges.add(
          NamespaceChange.setProperties(namespace, ImmutableMap.copyOf(properties)));
      return true;
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> properties) {
      checkNamespaceExists(namespace);
      removedProperties.computeIfAbsent(namespace, ns -> Sets.newHashSet()).addAll(properties);
      Map<String, String> set = setProperties.get(namespace);
      if (set != null) {
        properties.forEach(set::remove);
      }

      namespaceChanges.add(
          NamespaceChange.removeProperties(namespace, ImmutableSet.copyOf(properties)));
      return true;
    }

    @Override
    public boolean namespaceExists(Namespace namespace) {
      namespaces();
      return BaseCatalogTransaction.this.namespaceExists(namespace);
    }
  }

  private static boolean isPrefix(Namespace prefix, Namespace namespace) {
    for (int i = 0; i < prefix.length(); i += 1) {
      if (!prefix.level(i).equals(namespace.level(i))) {
        return false;
      }
    }

    return true;
  }

  /** A table this transaction creates, staged when the builder creates it. */
  private class StagedTableBuilder implements Catalog.TableBuilder {
    private final TableIdentifier identifier;
    private final Catalog.TableBuilder delegate;

    private StagedTableBuilder(TableIdentifier identifier, Schema schema) {
      this.identifier = identifier;
      this.delegate = origin.buildTable(identifier, schema);
    }

    @Override
    public Catalog.TableBuilder withPartitionSpec(PartitionSpec spec) {
      delegate.withPartitionSpec(spec);
      return this;
    }

    @Override
    public Catalog.TableBuilder withSortOrder(SortOrder sortOrder) {
      delegate.withSortOrder(sortOrder);
      return this;
    }

    @Override
    public Catalog.TableBuilder withLocation(String location) {
      delegate.withLocation(location);
      return this;
    }

    @Override
    public Catalog.TableBuilder withProperties(Map<String, String> properties) {
      delegate.withProperties(properties);
      return this;
    }

    @Override
    public Catalog.TableBuilder withProperty(String key, String value) {
      delegate.withProperty(key, value);
      return this;
    }

    @Override
    public Table create() {
      return new TransactionalTable(stage());
    }

    @Override
    public Transaction createTransaction() {
      return nestedTransaction(stage());
    }

    @Override
    public Transaction replaceTransaction() {
      throw new UnsupportedOperationException("Cannot replace a table in a catalog transaction");
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      throw new UnsupportedOperationException("Cannot replace a table in a catalog transaction");
    }

    private Entry stage() {
      if (entry(identifier) != null) {
        throw new AlreadyExistsException("Table already exists: %s", identifier);
      }

      checkNamespaceExists(identifier.namespace());
      // the origin's create, which commits only if asked to: this transaction takes its metadata
      Entry entry = new Entry(identifier, null, null, delegate.createTransaction());
      entries.add(entry);
      byName.put(identifier, entry);
      gone.remove(identifier);
      return entry;
    }
  }

  /** A table of this transaction: its updates are staged in the transaction. */
  private class TransactionalTable extends BaseTable {
    private final Entry entry;

    private TransactionalTable(Entry entry) {
      super(opsFromTable(entry.table()), entry.table().name());
      this.entry = entry;
    }

    @Override
    public TableScan newScan() {
      TableScan tableScan = super.newScan();
      if (tableScan instanceof DataTableScan) {
        DataTableScan scan = (DataTableScan) tableScan;
        return new TransactionalTableScan(entry, null, this, scan.schema(), scan.context());
      }

      return tableScan;
    }

    @Override
    public Transaction newTransaction() {
      return nestedTransaction(entry);
    }

    @Override
    public UpdateSchema updateSchema() {
      return txForTable(entry).updateSchema();
    }

    @Override
    public UpdatePartitionSpec updateSpec() {
      return txForTable(entry).updateSpec();
    }

    @Override
    public UpdateProperties updateProperties() {
      return txForTable(entry).updateProperties();
    }

    @Override
    public ReplaceSortOrder replaceSortOrder() {
      return txForTable(entry).replaceSortOrder();
    }

    @Override
    public UpdateLocation updateLocation() {
      return txForTable(entry).updateLocation();
    }

    @Override
    public AppendFiles newAppend() {
      return txForTable(entry).newAppend();
    }

    @Override
    public AppendFiles newFastAppend() {
      return txForTable(entry).newFastAppend();
    }

    @Override
    public RewriteFiles newRewrite() {
      return txForTable(entry).newRewrite();
    }

    @Override
    public RewriteManifests rewriteManifests() {
      return txForTable(entry).rewriteManifests();
    }

    @Override
    public OverwriteFiles newOverwrite() {
      return txForTable(entry).newOverwrite();
    }

    @Override
    public RowDelta newRowDelta() {
      return txForTable(entry).newRowDelta();
    }

    @Override
    public ReplacePartitions newReplacePartitions() {
      return txForTable(entry).newReplacePartitions();
    }

    @Override
    public DeleteFiles newDelete() {
      return txForTable(entry).newDelete();
    }

    @Override
    public UpdateStatistics updateStatistics() {
      return txForTable(entry).updateStatistics();
    }

    @Override
    public UpdatePartitionStatistics updatePartitionStatistics() {
      return txForTable(entry).updatePartitionStatistics();
    }

    @Override
    public ExpireSnapshots expireSnapshots() {
      return txForTable(entry).expireSnapshots();
    }

    @Override
    public ManageSnapshots manageSnapshots() {
      return txForTable(entry).manageSnapshots();
    }
  }

  /**
   * A scan of a table of this transaction: under SERIALIZABLE, its planning records what it read.
   */
  private class TransactionalTableScan extends DataTableScan {
    private final Entry entry;
    // the branch or tag it reads, if it named one
    private final String ref;

    private TransactionalTableScan(
        Entry entry, String ref, Table table, Schema schema, TableScanContext context) {
      super(table, schema, context);
      this.entry = entry;
      this.ref = ref;
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
      return new TransactionalTableScan(entry, ref, table, schema, context);
    }

    @Override
    public TableScan useRef(String name) {
      TransactionalTableScan scan = (TransactionalTableScan) super.useRef(name);
      return new TransactionalTableScan(entry, name, scan.table(), scan.schema(), scan.context());
    }

    @Override
    public CloseableIterable<FileScanTask> planFiles() {
      // before planning: a scan of a branch with no snapshot plans nothing, and read that it had
      // none
      recordRead();
      return super.planFiles();
    }

    /**
     * Records the rows this scan reads: of its ref as of the snapshot the ref named when the
     * transaction first read the table, plus the transaction's own changes, which only the
     * transaction can make. A scan of a fixed snapshot reads rows that cannot change, and a scan of
     * a table the transaction created reads only its own.
     */
    private void recordRead() {
      if (IsolationLevel.SERIALIZABLE != isolationLevel || entry.created()) {
        return;
      }

      if (ref == null && context().snapshotId() != null) {
        return;
      }

      TableRead read =
          TableRead.scan(
              entry.startName,
              entry.base,
              ref != null ? ref : SnapshotRef.MAIN_BRANCH,
              filter(),
              isCaseSensitive());
      if (!reads.contains(read)) {
        reads.add(read);
      }
    }
  }
}
