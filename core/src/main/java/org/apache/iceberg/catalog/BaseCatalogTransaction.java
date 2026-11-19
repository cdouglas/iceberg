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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BasePartitionStatisticsScan;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataTableScan;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.IncrementalScan;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatistics;
import org.apache.iceberg.PartitionStatisticsScan;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
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
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.view.BaseMetastoreViewCatalog;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.apache.iceberg.view.ViewUtil;

/**
 * A catalog transaction staged on the client: {@link #asCatalog()} stages every change to tables
 * and namespaces, and {@link #commitTransaction()} commits them in one {@link
 * SupportsCatalogTransactions#commitTransaction(CatalogCommit)}.
 *
 * <p>The transaction sees its own changes: a table it dropped no longer loads, one it renamed loads
 * under its new name only, one it created loads and takes updates, and listings include them. Each
 * table it loads is held as it first loaded it, and its updates are staged on that version. Under
 * {@link IsolationLevel#SERIALIZABLE} it records what it reads ({@link TableRead}): each scan's
 * planning, of a branch or tag (an incremental scan's, of the branch whose current snapshot it ends
 * at and of the tags it names), and each {@link #declareRead}; and, as reads of the whole table,
 * each metadata table it loads of a table it holds and each partition statistics scan's read. Views
 * stage as tables do, where the catalog has them ({@link #asViewCatalog()}; {@link #asCatalog()} is
 * then a {@link ViewCatalog} too): a view's create, replace (or property or location change),
 * rename and drop, each change staged on the view's version as the transaction first loaded it.
 * Tables and views share names: a staged table or view keeps a name from the other, and dropping a
 * namespace counts its views. Replace transactions, {@code registerTable} and {@code registerView}
 * are not part of a catalog transaction and throw.
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
  // Every view the transaction touched, in the order it did; viewsByName holds those that resolve
  // by their names in this transaction, viewsGone the names that no longer resolve to the origin's
  private final List<ViewEntry> viewEntries = Lists.newArrayList();
  private final Map<TableIdentifier, ViewEntry> viewsByName = Maps.newHashMap();
  private final Set<TableIdentifier> viewsGone = Sets.newHashSet();
  private boolean hasCommitted = false;
  private boolean hasFailed = false;

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
    // transactions of its own on it, nested in this one: their commits stage here
    private final List<Transaction> nested = Lists.newArrayList();
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

  /** A view the transaction touched. */
  private static final class ViewEntry {
    // its name when the transaction started, and its version as the transaction first read it;
    // both null for a view the transaction created
    private final TableIdentifier startName;
    private final ViewMetadata base;
    private TableIdentifier name;
    // its metadata in the transaction: base, or what the transaction staged
    private ViewMetadata current;
    private boolean updated;
    private boolean dropped;

    private ViewEntry(TableIdentifier name, ViewMetadata base, ViewMetadata current) {
      this.startName = base != null ? name : null;
      this.base = base;
      this.name = name;
      this.current = current;
    }

    private boolean created() {
      return startName == null;
    }
  }

  @Override
  public void commitTransaction() {
    Preconditions.checkState(!hasCommitted, "Transaction has already committed changes");
    Preconditions.checkState(
        !hasFailed, "Transaction failed to commit, or its outcome is unknown: start another");

    try {
      CatalogCommit commit = toCommit();
      if (commit.hasChanges() || !commit.reads().isEmpty()) {
        ((SupportsCatalogTransactions) origin).commitTransaction(commit);
      }

      hasCommitted = true;
    } catch (CommitStateUnknownException e) {
      hasFailed = true;
      throw e;
    } catch (RuntimeException e) {
      hasFailed = true;
      rollback();
      throw e;
    }

    cleanUpExpiredSnapshots();
  }

  /**
   * Deletes the files of the snapshots the transaction's expiries removed, now that the catalog has
   * committed their removal: an expiry staged on a table of the transaction, or in a transaction
   * nested in it, only stages (a nested transaction's commit stages here), so its cleanup waits for
   * this commit, and runs not at all if this one fails or is abandoned. Not for a table the
   * transaction dropped, whose purge, if any, is the catalog's. A cleanup that cannot run is logged
   * and leaves files behind: the commit stands.
   */
  private void cleanUpExpiredSnapshots() {
    for (Entry entry : entries) {
      if (entry.dropped) {
        continue;
      }
      for (Transaction nested : entry.nested) {
        ((BaseTransaction) nested).cleanUpExpiredSnapshotsOnceCommitted();
      }
      if (entry.tx != null) {
        ((BaseTransaction) entry.tx).cleanUpExpiredSnapshotsOnceCommitted();
      }
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

    for (ViewEntry entry : viewEntries) {
      if (entry.created()) {
        if (!entry.dropped) {
          commit.putCreatedViews(entry.name, entry.current);
        }
      } else {
        ViewChange change = viewChange(entry);
        if (change != null) {
          commit.addViewChanges(change);
        }
      }
    }

    if (IsolationLevel.SERIALIZABLE == isolationLevel) {
      commit.addAllReads(reads);
    }

    return commit.build();
  }

  /** What the transaction did to a view that existed when it started, or null if nothing. */
  private static ViewChange viewChange(ViewEntry entry) {
    ViewMetadata updated = !entry.dropped && entry.updated ? entry.current : null;
    boolean renamed = !entry.dropped && !entry.name.equals(entry.startName);
    if (updated == null && !renamed && !entry.dropped) {
      return null;
    }

    return ImmutableViewChange.builder()
        .identifier(entry.startName)
        .base(entry.base)
        .updated(updated)
        .renamedTo(renamed ? entry.name : null)
        .dropped(entry.dropped)
        .build();
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
    viewEntries.clear();
    viewsByName.clear();
    viewsGone.clear();
  }

  @Override
  public Catalog asCatalog() {
    return origin instanceof ViewCatalog
        ? new AsTransactionalViewCatalog()
        : new AsTransactionalCatalog();
  }

  @Override
  public ViewCatalog asViewCatalog() {
    if (!(origin instanceof ViewCatalog)) {
      throw new UnsupportedOperationException("Catalog does not support views: " + origin);
    }

    return new AsTransactionalViewCatalog();
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

    recordWholeRead(entry);
  }

  /**
   * Records, under SERIALIZABLE, a read of a table's whole version as the transaction first read
   * it: what {@link #declareRead} records, for what reads the table other than by a scan whose ref
   * and filter the transaction sees. A table the transaction created is read only as it made it.
   */
  private void recordWholeRead(Entry entry) {
    if (!entry.created()) {
      addRead(TableRead.whole(entry.startName, entry.base));
    }
  }

  private void addRead(TableRead read) {
    if (IsolationLevel.SERIALIZABLE == isolationLevel && !reads.contains(read)) {
      reads.add(read);
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
   * The view {@code name} names in this transaction, loaded from the origin the first time; null if
   * there is none.
   */
  private ViewEntry viewEntry(TableIdentifier name) {
    ViewEntry entry = viewsByName.get(name);
    if (entry != null || viewsGone.contains(name)) {
      return entry;
    }

    View view;
    try {
      view = views().loadView(name);
    } catch (NoSuchViewException e) {
      return null;
    }

    Preconditions.checkState(
        view instanceof BaseView, "Cannot stage changes to view %s: no operations", name);
    ViewMetadata base = ((BaseView) view).operations().current();
    entry = new ViewEntry(name, base, base);
    viewEntries.add(entry);
    viewsByName.put(name, entry);
    return entry;
  }

  /**
   * Whether a view staged in this transaction has {@code name}: the transaction keeps a name a view
   * holds from a table, as the catalog does; one of the origin's views it has not loaded is checked
   * where the commit takes effect.
   */
  private boolean stagedViewHas(TableIdentifier name) {
    return viewsByName.containsKey(name);
  }

  private ViewCatalog views() {
    if (origin instanceof ViewCatalog) {
      return (ViewCatalog) origin;
    }

    throw new UnsupportedOperationException("Catalog does not support views: " + origin);
  }

  /**
   * Whether {@code namespace} holds a view in this transaction: one it staged there, or one of the
   * origin's it has not dropped or renamed away. A catalog that cannot list views has none.
   */
  private boolean holdsViews(Namespace namespace) {
    for (ViewEntry entry : viewsByName.values()) {
      if (entry.name.namespace().equals(namespace)) {
        return true;
      }
    }

    if (!(origin instanceof ViewCatalog) || createdNamespaces.containsKey(namespace)) {
      return false;
    }

    try {
      return ((ViewCatalog) origin)
          .listViews(namespace).stream().anyMatch(view -> !viewsGone.contains(view));
    } catch (UnsupportedOperationException e) {
      // a catalog without views (JdbcCatalog on its V0 schema)
      return false;
    }
  }

  /**
   * The operations of view {@code name} in this transaction: what it is here, and a commit that
   * stages a create (where no table or view has its name and its namespace exists) or a change on
   * the version it read. A view loaded through the transaction keeps its entry across a rename.
   */
  private class StagedViewOperations implements ViewOperations {
    private final TableIdentifier name;
    private ViewEntry entry;

    private StagedViewOperations(TableIdentifier name, ViewEntry entry) {
      this.name = name;
      this.entry = entry;
    }

    private ViewEntry resolve() {
      if (entry != null) {
        return entry.dropped ? null : entry;
      }

      return viewEntry(name);
    }

    @Override
    public ViewMetadata current() {
      ViewEntry resolved = resolve();
      return resolved != null ? resolved.current : null;
    }

    @Override
    public ViewMetadata refresh() {
      return current();
    }

    @Override
    public void commit(ViewMetadata base, ViewMetadata metadata) {
      ViewEntry resolved = resolve();
      if (resolved == null) {
        if (base != null) {
          throw new CommitFailedException(
              "Cannot commit %s: the transaction dropped or renamed it", name);
        }

        stageCreate(metadata);
        return;
      }

      if (!Objects.equals(base, resolved.current)) {
        throw new CommitFailedException(
            "Cannot commit %s: its metadata changed in the transaction", resolved.name);
      }

      resolved.current = metadata;
      resolved.updated = true;
    }

    private void stageCreate(ViewMetadata metadata) {
      checkNamespaceExists(name.namespace());
      if (entry(name) != null) {
        throw new AlreadyExistsException("Table with same name already exists: %s", name);
      }

      ViewEntry created = new ViewEntry(name, null, metadata);
      viewEntries.add(created);
      viewsByName.put(name, created);
      viewsGone.remove(name);
      this.entry = created;
    }
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
    // its expiries' files wait for this transaction's commit, which may fail or never come
    Transaction nested =
        Transactions.newNestedTransaction(tx.table().name(), staging, staging.current());
    entry.nested.add(nested);
    return nested;
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
      if (entry != null) {
        return new TransactionalTable(entry);
      }

      if (gone.contains(identifier)) {
        throw new NoSuchTableException("Table does not exist: %s", identifier);
      }

      if (MetadataTableUtils.hasMetadataTableName(identifier)
          && !identifier.namespace().isEmpty()) {
        return metadataTable(identifier);
      }

      // a NoSuchTableException
      return origin.loadTable(identifier);
    }

    /**
     * A metadata table, which the transaction reads as the origin does: of a table the transaction
     * holds, a read of the whole table, recorded as it is handed out, since its scans read the
     * table's snapshots, refs, manifests and files without a scan of the table this transaction
     * sees. The table is held first, so the metadata table reads its version as the transaction
     * first read it or a later one, which fails the commit.
     */
    private Table metadataTable(TableIdentifier identifier) {
      TableIdentifier tableName = TableIdentifier.of(identifier.namespace().levels());
      Entry table = entry(tableName);
      if (table == null && gone.contains(tableName)) {
        throw new NoSuchTableException("Table does not exist: %s", identifier);
      }

      Table metadataTable = origin.loadTable(identifier);
      if (table != null) {
        recordWholeRead(table);
      }

      return metadataTable;
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

      if (stagedViewHas(to)) {
        throw new AlreadyExistsException("Cannot rename %s to %s. View already exists", from, to);
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

      if (!listTables(namespace).isEmpty()
          || !listNamespaces(namespace).isEmpty()
          || holdsViews(namespace)) {
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

  /** The transaction's catalog where the origin has views: its views stage too. */
  public class AsTransactionalViewCatalog extends AsTransactionalCatalog implements ViewCatalog {
    @Override
    public String name() {
      return origin.name();
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {}

    @Override
    public List<TableIdentifier> listViews(Namespace namespace) {
      ViewCatalog views = views();
      checkNamespaceExists(namespace);
      Set<TableIdentifier> listed = Sets.newLinkedHashSet();
      if (!createdNamespaces.containsKey(namespace)) {
        for (TableIdentifier view : views.listViews(namespace)) {
          if (!viewsGone.contains(view)) {
            listed.add(view);
          }
        }
      }

      for (ViewEntry entry : viewsByName.values()) {
        if (entry.name.namespace().equals(namespace)) {
          listed.add(entry.name);
        }
      }

      return Lists.newArrayList(listed);
    }

    @Override
    public View loadView(TableIdentifier identifier) {
      ViewEntry entry = viewEntry(identifier);
      if (entry == null) {
        throw new NoSuchViewException("View does not exist: %s", identifier);
      }

      return new BaseView(
          new StagedViewOperations(identifier, entry), ViewUtil.fullViewName(name(), identifier));
    }

    @Override
    public boolean viewExists(TableIdentifier identifier) {
      return viewEntry(identifier) != null;
    }

    @Override
    public ViewBuilder buildView(TableIdentifier identifier) {
      ViewCatalog views = views();
      if (!(views instanceof BaseMetastoreViewCatalog)) {
        throw new UnsupportedOperationException(
            "Cannot create or replace a view in a catalog transaction of " + origin);
      }

      return ((BaseMetastoreViewCatalog) views)
          .buildView(identifier, new StagedViewOperations(identifier, null));
    }

    @Override
    public boolean dropView(TableIdentifier identifier) {
      ViewEntry entry = viewEntry(identifier);
      if (entry == null) {
        return false;
      }

      entry.dropped = true;
      viewsByName.remove(identifier);
      viewsGone.add(identifier);
      return true;
    }

    @Override
    public void renameView(TableIdentifier from, TableIdentifier to) {
      ViewEntry entry = viewEntry(from);
      if (entry == null) {
        throw new NoSuchViewException("Cannot rename %s to %s. View does not exist", from, to);
      }

      if (entry(to) != null) {
        throw new AlreadyExistsException("Cannot rename %s to %s. Table already exists", from, to);
      }

      if (viewEntry(to) != null) {
        throw new AlreadyExistsException("Cannot rename %s to %s. View already exists", from, to);
      }

      checkNamespaceExists(to.namespace());
      viewsByName.remove(from);
      viewsGone.add(from);
      viewsByName.put(to, entry);
      entry.name = to;
    }

    @Override
    public View registerView(TableIdentifier identifier, String metadataFileLocation) {
      throw new UnsupportedOperationException("Cannot register a view in a catalog transaction");
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

      if (stagedViewHas(identifier)) {
        throw new AlreadyExistsException("View with same name already exists: %s", identifier);
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

    /**
     * Partition statistics are the table's metadata, which another writer may set or remove even
     * for a snapshot fixed in advance: a scan of them reads the whole table.
     */
    @Override
    public PartitionStatisticsScan newPartitionStatisticsScan() {
      return new BasePartitionStatisticsScan(this) {
        @Override
        public CloseableIterable<PartitionStatistics> scan() {
          recordWholeRead(entry);
          return super.scan();
        }
      };
    }

    /**
     * An engine's distributed planner scans the table without {@link #newScan} or {@link
     * #newBatchScan}, so the transaction would not see what it read: under SERIALIZABLE, plan
     * locally.
     */
    @Override
    public boolean allowDistributedPlanning() {
      return IsolationLevel.SERIALIZABLE != isolationLevel && super.allowDistributedPlanning();
    }

    @Override
    public IncrementalAppendScan newIncrementalAppendScan() {
      return new TransactionalIncrementalAppendScan(
          entry, super.newIncrementalAppendScan(), IncrementalRange.CURRENT);
    }

    @Override
    public IncrementalChangelogScan newIncrementalChangelogScan() {
      return new TransactionalIncrementalChangelogScan(
          entry, super.newIncrementalChangelogScan(), IncrementalRange.CURRENT);
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

    /**
     * The appends after a snapshot up to main's current snapshot, which it resolves now into a
     * range of fixed snapshots: recorded now, as an incremental scan to main's current snapshot is
     * when it plans.
     */
    @Override
    @SuppressWarnings("deprecation")
    public TableScan appendsAfter(long fromSnapshotId) {
      TableScan scan = super.appendsAfter(fromSnapshotId);
      recordIncrementalRead(entry, IncrementalRange.CURRENT.refs(), filter(), isCaseSensitive());
      return scan;
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

      addRead(
          TableRead.scan(
              entry.startName,
              entry.base,
              ref != null ? ref : SnapshotRef.MAIN_BRANCH,
              filter(),
              isCaseSensitive()));
    }
  }

  /**
   * Records, under SERIALIZABLE, an incremental scan's read of each ref its range resolved ({@link
   * IncrementalRange#refs}) as a scan read of the ref, as a table scan of the ref records: a range
   * that ends at a ref's current snapshot is judged as a scan of that ref is, and a range between
   * snapshots fixed by their ids reads rows that cannot change, and records nothing. A scan of a
   * table the transaction created reads only its own.
   */
  private void recordIncrementalRead(
      Entry entry, Set<String> refs, Expression filter, boolean caseSensitive) {
    if (entry.created()) {
      return;
    }

    for (String ref : refs) {
      addRead(TableRead.scan(entry.startName, entry.base, ref, filter, caseSensitive));
    }
  }

  /**
   * Where an incremental scan's range starts and ends, as far as a ref names it: the tag it starts
   * at, if any; and the tag it ends at, else a snapshot fixed by its id, else the current snapshot
   * of a branch (main if it named none).
   */
  private static final class IncrementalRange {
    private static final IncrementalRange CURRENT = new IncrementalRange(null, null, false, null);

    private final String fromTag;
    private final String toTag;
    private final boolean toFixed;
    private final String branch;

    private IncrementalRange(String fromTag, String toTag, boolean toFixed, String branch) {
      this.fromTag = fromTag;
      this.toTag = toTag;
      this.toFixed = toFixed;
      this.branch = branch;
    }

    private IncrementalRange from(String tag) {
      return new IncrementalRange(tag, toTag, toFixed, branch);
    }

    private IncrementalRange to(String tag) {
      return new IncrementalRange(fromTag, tag, tag == null, branch);
    }

    private IncrementalRange onBranch(String name) {
      return new IncrementalRange(fromTag, toTag, toFixed, name);
    }

    /**
     * The refs the range resolved, whose snapshots another writer may move: the branch whose
     * current snapshot it ends at, unless it ends at a tag or a fixed snapshot; and a tag it starts
     * or ends at, which it resolves as a scan of the tag does.
     */
    private Set<String> refs() {
      Set<String> refs = Sets.newLinkedHashSet();
      if (fromTag != null) {
        refs.add(fromTag);
      }

      if (toTag != null) {
        refs.add(toTag);
      } else if (!toFixed) {
        refs.add(branch != null ? branch : SnapshotRef.MAIN_BRANCH);
      }

      return refs;
    }
  }

  /**
   * An incremental scan of a table of this transaction, around the scan the table made: under
   * SERIALIZABLE, its planning records the refs its range resolved.
   */
  private abstract class TransactionalIncrementalScan<
          ThisT extends IncrementalScan<ThisT, T, G>,
          T extends ScanTask,
          G extends ScanTaskGroup<T>>
      implements IncrementalScan<ThisT, T, G> {
    private final Entry entry;
    private final ThisT scan;
    private final IncrementalRange range;

    private TransactionalIncrementalScan(Entry entry, ThisT scan, IncrementalRange range) {
      this.entry = entry;
      this.scan = scan;
      this.range = range;
    }

    /**
     * A scan of this transaction around {@code tableScan}, a scan of the table over {@code
     * ofRange}.
     */
    protected abstract ThisT wrap(Entry ofEntry, ThisT tableScan, IncrementalRange ofRange);

    private ThisT wrap(ThisT tableScan, IncrementalRange ofRange) {
      return wrap(entry, tableScan, ofRange);
    }

    private ThisT wrap(ThisT tableScan) {
      return wrap(entry, tableScan, range);
    }

    private void recordRead() {
      recordIncrementalRead(entry, range.refs(), scan.filter(), scan.isCaseSensitive());
    }

    @Override
    public ThisT fromSnapshotInclusive(long fromSnapshotId) {
      return wrap(scan.fromSnapshotInclusive(fromSnapshotId), range.from(null));
    }

    @Override
    public ThisT fromSnapshotInclusive(String ref) {
      return wrap(scan.fromSnapshotInclusive(ref), range.from(ref));
    }

    @Override
    public ThisT fromSnapshotExclusive(long fromSnapshotId) {
      return wrap(scan.fromSnapshotExclusive(fromSnapshotId), range.from(null));
    }

    @Override
    public ThisT fromSnapshotExclusive(String ref) {
      return wrap(scan.fromSnapshotExclusive(ref), range.from(ref));
    }

    @Override
    public ThisT toSnapshot(long toSnapshotId) {
      return wrap(scan.toSnapshot(toSnapshotId), range.to(null));
    }

    @Override
    public ThisT toSnapshot(String ref) {
      return wrap(scan.toSnapshot(ref), range.to(ref));
    }

    @Override
    public ThisT useBranch(String branch) {
      return wrap(scan.useBranch(branch), range.onBranch(branch));
    }

    @Override
    public ThisT option(String property, String value) {
      return wrap(scan.option(property, value));
    }

    @Override
    public ThisT project(Schema schema) {
      return wrap(scan.project(schema));
    }

    @Override
    public ThisT caseSensitive(boolean caseSensitive) {
      return wrap(scan.caseSensitive(caseSensitive));
    }

    @Override
    public boolean isCaseSensitive() {
      return scan.isCaseSensitive();
    }

    @Override
    public ThisT includeColumnStats() {
      return wrap(scan.includeColumnStats());
    }

    @Override
    public ThisT includeColumnStats(Collection<String> requestedColumns) {
      return wrap(scan.includeColumnStats(requestedColumns));
    }

    @Override
    public ThisT select(Collection<String> columns) {
      return wrap(scan.select(columns));
    }

    @Override
    public ThisT filter(Expression expr) {
      return wrap(scan.filter(expr));
    }

    @Override
    public Expression filter() {
      return scan.filter();
    }

    @Override
    public ThisT ignoreResiduals() {
      return wrap(scan.ignoreResiduals());
    }

    @Override
    public ThisT planWith(ExecutorService executorService) {
      return wrap(scan.planWith(executorService));
    }

    @Override
    public Schema schema() {
      return scan.schema();
    }

    @Override
    public CloseableIterable<T> planFiles() {
      recordRead();
      return scan.planFiles();
    }

    @Override
    public CloseableIterable<G> planTasks() {
      recordRead();
      return scan.planTasks();
    }

    @Override
    public long targetSplitSize() {
      return scan.targetSplitSize();
    }

    @Override
    public int splitLookback() {
      return scan.splitLookback();
    }

    @Override
    public long splitOpenFileCost() {
      return scan.splitOpenFileCost();
    }

    @Override
    public ThisT metricsReporter(MetricsReporter reporter) {
      return wrap(scan.metricsReporter(reporter));
    }

    @Override
    public ThisT minRowsRequested(long numRows) {
      return wrap(scan.minRowsRequested(numRows));
    }

    @Override
    public Supplier<FileIO> fileIO() {
      return scan.fileIO();
    }
  }

  private class TransactionalIncrementalAppendScan
      extends TransactionalIncrementalScan<IncrementalAppendScan, FileScanTask, CombinedScanTask>
      implements IncrementalAppendScan {
    private TransactionalIncrementalAppendScan(
        Entry entry, IncrementalAppendScan scan, IncrementalRange range) {
      super(entry, scan, range);
    }

    @Override
    protected IncrementalAppendScan wrap(
        Entry ofEntry, IncrementalAppendScan tableScan, IncrementalRange ofRange) {
      return new TransactionalIncrementalAppendScan(ofEntry, tableScan, ofRange);
    }
  }

  private class TransactionalIncrementalChangelogScan
      extends TransactionalIncrementalScan<
          IncrementalChangelogScan, ChangelogScanTask, ScanTaskGroup<ChangelogScanTask>>
      implements IncrementalChangelogScan {
    private TransactionalIncrementalChangelogScan(
        Entry entry, IncrementalChangelogScan scan, IncrementalRange range) {
      super(entry, scan, range);
    }

    @Override
    protected IncrementalChangelogScan wrap(
        Entry ofEntry, IncrementalChangelogScan tableScan, IncrementalRange ofRange) {
      return new TransactionalIncrementalChangelogScan(ofEntry, tableScan, ofRange);
    }
  }
}
