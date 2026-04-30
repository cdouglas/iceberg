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
package org.apache.iceberg;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.metrics.LoggingMetricsReporter;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseTransaction implements Transaction {
  private static final Logger LOG = LoggerFactory.getLogger(BaseTransaction.class);

  enum TransactionType {
    CREATE_TABLE,
    REPLACE_TABLE,
    CREATE_OR_REPLACE_TABLE,
    SIMPLE
  }

  private final String tableName;
  private final TableOperations ops;
  private final TransactionTable transactionTable;
  private final TableOperations transactionOps;
  private final List<PendingUpdate> updates;
  private final Set<String> deletedFiles =
      Sets.newHashSet(); // keep track of files deleted in the most recent commit
  private final Consumer<String> enqueueDelete = deletedFiles::add;
  private final TransactionType type;
  // rebuilds a replace transaction's replacement on the table as it is when it commits; null to
  // commit the replacement as the transaction began with it
  private final UnaryOperator<TableMetadata> replacement;
  // the replacement as the transaction began with it, which a rebuild must match
  private final TableMetadata startingReplacement;
  private TableMetadata base;
  private TableMetadata current;
  private boolean hasLastOpCommitted;
  private final MetricsReporter reporter;

  BaseTransaction(
      String tableName, TableOperations ops, TransactionType type, TableMetadata start) {
    this(tableName, ops, type, start, LoggingMetricsReporter.instance());
  }

  BaseTransaction(
      String tableName,
      TableOperations ops,
      TransactionType type,
      TableMetadata start,
      MetricsReporter reporter) {
    this(tableName, ops, type, start, null, reporter);
  }

  BaseTransaction(
      String tableName,
      TableOperations ops,
      TransactionType type,
      TableMetadata start,
      UnaryOperator<TableMetadata> replacement,
      MetricsReporter reporter) {
    this.tableName = tableName;
    this.ops = ops;
    this.transactionTable = new TransactionTable();
    this.current = start;
    // Capability-transparent wrapper: if the underlying ops is a ManifestListSink,
    // expose that capability through the wrapper so SnapshotProducer.apply() takes
    // the inline path (no transient snap-*.avro files written and orphaned on a
    // crash before the catalog commit). The temp(...) view path is intentionally
    // sink-less — temp constructs an in-memory TableOperations from a hypothetical
    // metadata snapshot that must not pollute the catalog with stale deltas.
    this.transactionOps =
        (ops instanceof ManifestListSink)
            ? new TransactionTableOperationsWithSink()
            : new TransactionTableOperations();
    this.updates = Lists.newArrayList();
    this.base = ops.current();
    this.type = type;
    this.replacement = replacement;
    this.startingReplacement = start;
    this.hasLastOpCommitted = true;
    this.reporter = reporter;
  }

  @Override
  public Table table() {
    return transactionTable;
  }

  public String tableName() {
    return tableName;
  }

  public TableMetadata startMetadata() {
    return base;
  }

  public TableMetadata currentMetadata() {
    return current;
  }

  public TableOperations underlyingOps() {
    return ops;
  }

  protected final <T extends PendingUpdate> T appendUpdate(T update) {
    checkLastOperationCommitted(update.getClass());

    if (update instanceof SnapshotUpdate) {
      ((SnapshotUpdate) update).deleteWith(enqueueDelete);
    }

    if (update instanceof SnapshotProducer) {
      ((SnapshotProducer) update).reportWith(reporter);
    }

    this.updates.add(update);
    return update;
  }

  private void checkLastOperationCommitted(Class<? extends PendingUpdate> clazz) {
    String operation =
        clazz.getInterfaces().length > 0
            ? clazz.getInterfaces()[0].getSimpleName()
            : clazz.getSimpleName();
    Preconditions.checkState(
        hasLastOpCommitted, "Cannot create new %s: last operation has not committed", operation);

    // SnapshotManager handles its own commits internally
    if (SnapshotManager.class != clazz) {
      this.hasLastOpCommitted = false;
    }
  }

  @Override
  public UpdateSchema updateSchema() {
    return appendUpdate(new SchemaUpdate(transactionOps));
  }

  @Override
  public UpdatePartitionSpec updateSpec() {
    return appendUpdate(new BaseUpdatePartitionSpec(transactionOps));
  }

  @Override
  public UpdateProperties updateProperties() {
    return appendUpdate(new PropertiesUpdate(transactionOps));
  }

  @Override
  public ReplaceSortOrder replaceSortOrder() {
    return appendUpdate(new BaseReplaceSortOrder(transactionOps));
  }

  @Override
  public UpdateLocation updateLocation() {
    return appendUpdate(new SetLocation(transactionOps));
  }

  @Override
  public AppendFiles newAppend() {
    return appendUpdate(new MergeAppend(tableName, transactionOps));
  }

  @Override
  public AppendFiles newFastAppend() {
    return appendUpdate(new FastAppend(tableName, transactionOps));
  }

  @Override
  public RewriteFiles newRewrite() {
    return appendUpdate(new BaseRewriteFiles(tableName, transactionOps));
  }

  @Override
  public RewriteManifests rewriteManifests() {
    return appendUpdate(new BaseRewriteManifests(tableName, transactionOps));
  }

  @Override
  public OverwriteFiles newOverwrite() {
    return appendUpdate(new BaseOverwriteFiles(tableName, transactionOps));
  }

  @Override
  public RowDelta newRowDelta() {
    return appendUpdate(new BaseRowDelta(tableName, transactionOps));
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    return appendUpdate(new BaseReplacePartitions(tableName, transactionOps));
  }

  @Override
  public DeleteFiles newDelete() {
    return appendUpdate(new StreamingDelete(tableName, transactionOps));
  }

  @Override
  public UpdateStatistics updateStatistics() {
    return appendUpdate(new SetStatistics(transactionOps));
  }

  @Override
  public UpdatePartitionStatistics updatePartitionStatistics() {
    return appendUpdate(new SetPartitionStatistics(transactionOps));
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    // its files are cleaned up once the transaction commits, from what it committed
    return appendUpdate(new RemoveSnapshots(transactionOps).cleanUpOnceTransactionCommits());
  }

  @Override
  public ManageSnapshots manageSnapshots() {
    return appendUpdate(new SnapshotManager(this));
  }

  CherryPickOperation cherryPick() {
    return appendUpdate(new CherryPickOperation(tableName, transactionOps));
  }

  SetSnapshotOperation setBranchSnapshot() {
    return appendUpdate(new SetSnapshotOperation(transactionOps));
  }

  UpdateSnapshotReferencesOperation updateSnapshotReferencesOperation() {
    return appendUpdate(new UpdateSnapshotReferencesOperation(transactionOps));
  }

  @Override
  public void commitTransaction() {
    Preconditions.checkState(
        hasLastOpCommitted, "Cannot commit transaction: last operation has not committed");

    switch (type) {
      case CREATE_TABLE:
        commitCreateTransaction();
        break;

      case REPLACE_TABLE:
        commitReplaceTransaction(false);
        break;

      case CREATE_OR_REPLACE_TABLE:
        commitReplaceTransaction(true);
        break;

      case SIMPLE:
        commitSimpleTransaction();
        break;
    }
  }

  private void commitCreateTransaction() {
    // this operation creates the table. if the commit fails, this cannot retry because another
    // process has created the same table.
    try {
      ops.commit(null, current);
      cleanUpExpiredSnapshots();

    } catch (CommitStateUnknownException e) {
      throw e;

    } catch (RuntimeException e) {
      // the commit failed and no files were committed. clean up each update
      if (!ops.requireStrictCleanup() || e instanceof CleanableFailure) {
        cleanAllUpdates();
      }

      throw e;
    } finally {
      // create table never needs to retry because the table has no previous state. because retries
      // are not a
      // concern, it is safe to delete all the deleted files from individual operations
      deleteUncommittedFiles(deletedFiles);
    }
  }

  private void commitReplaceTransaction(boolean orCreate) {
    Map<String, String> props = base != null ? base.properties() : current.properties();
    // the replacement keeps the snapshots that the table had when the transaction began
    Set<Long> startingSnapshotIds = snapshotIds(base);
    // without a table when it began, the transaction built its replacement as a new table
    boolean replacesNoTable = base == null;

    try {
      Tasks.foreach(ops)
          .retry(PropertyUtil.propertyAsInt(props, COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
          .exponentialBackoff(
              PropertyUtil.propertyAsInt(
                  props, COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
              PropertyUtil.propertyAsInt(
                  props, COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
              PropertyUtil.propertyAsInt(
                  props, COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
              2.0 /* exponential */)
          .onlyRetryOn(CommitFailedException.class)
          .run(
              underlyingOps -> {
                refreshReplacement(underlyingOps, orCreate, replacesNoTable, startingSnapshotIds);
                underlyingOps.commit(base, current);
              });

    } catch (CommitStateUnknownException e) {
      throw e;

    } catch (PendingUpdateFailedException e) {
      cleanUp();
      throw e.wrapped();

    } catch (RuntimeException e) {
      // the commit failed and no files were committed. clean up each update.
      if (!ops.requireStrictCleanup() || e instanceof CleanableFailure) {
        cleanUp();
      }

      throw e;
    }

    // the commit succeeded. a rebuild may have reused files that an earlier attempt's commit
    // replaced, so delete only those that the committed snapshots don't reference
    cleanUpAfterCommit(snapshotIds(base));
    cleanUpExpiredSnapshots();
  }

  /**
   * Refreshes the table the replace commits over and, if another writer changed it since the
   * transaction began, the replacement too.
   */
  private void refreshReplacement(
      TableOperations underlyingOps,
      boolean orCreate,
      boolean replacesNoTable,
      Set<Long> startingSnapshotIds) {
    try {
      underlyingOps.refresh();
    } catch (NoSuchTableException e) {
      if (!orCreate) {
        throw e;
      }
    }

    // committing that new table over one another writer created since would replace
    // that table with a different one, dropping its history: fail as a create does
    TableMetadata refreshed = underlyingOps.current();
    if (replacesNoTable && refreshed != null) {
      throw new AlreadyExistsException("Table already exists: %s", tableName);
    }

    // a replace replaces the table's data, schema and partitioning, whatever changed
    // since it began, but keeps the table's history: its replacement keeps the
    // snapshots, refs and properties of the table as the transaction began with it
    if (base != refreshed) {
      if (replacement != null && !sameVersion(base, refreshed)) {
        rebuildReplacement(refreshed);
      } else {
        this.base = refreshed;

        // without a way to rebuild it, at least not those the table no longer has,
        // such as one expired since
        Set<Long> removed = Sets.difference(startingSnapshotIds, snapshotIds(base));
        if (!removed.isEmpty()) {
          this.current = TableMetadata.buildFrom(current).removeSnapshots(removed).build();
        }
      }
    }
  }

  /**
   * Rebuilds the replacement on {@code refreshed}, the table as other writers left it since the
   * transaction began, and applies the transaction's updates to it again, so that the replace keeps
   * what they committed, as it would had it begun after them. Fails the commit when that cannot be
   * done exactly: the files the transaction wrote depend on its replacement's schema, partition
   * spec, sort order and format version, and an update may not apply again.
   */
  private void rebuildReplacement(TableMetadata refreshed) {
    if (refreshed == null) {
      throw cannotRebuild("was dropped", null);
    }

    TableMetadata rebuilt;
    try {
      rebuilt = replacement.apply(refreshed);
    } catch (RuntimeException e) {
      throw cannotRebuild("cannot be replaced as the transaction began", e);
    }

    if (!rebuilt.schema().sameSchema(startingReplacement.schema())
        || rebuilt.currentSchemaId() != startingReplacement.currentSchemaId()
        || !rebuilt.spec().equals(startingReplacement.spec())
        || !rebuilt.sortOrder().equals(startingReplacement.sortOrder())
        || rebuilt.formatVersion() != startingReplacement.formatVersion()) {
      throw cannotRebuild(
          "its replacement would have another schema, partition spec, sort order or format", null);
    }

    this.base = refreshed;
    this.current = rebuilt;
    try {
      for (PendingUpdate update : updates) {
        update.commit();
      }
    } catch (RuntimeException e) {
      throw cannotRebuild("the transaction's updates cannot apply to it again", e);
    }
  }

  private PendingUpdateFailedException cannotRebuild(String reason, Throwable cause) {
    return new PendingUpdateFailedException(
        new CommitFailedException(
            cause,
            "Cannot commit replace transaction: table %s changed since it began and %s",
            tableName,
            reason));
  }

  // whether the table read again is the version read before: the same metadata file
  private static boolean sameVersion(TableMetadata before, TableMetadata now) {
    if (before == now) {
      return true;
    } else if (before == null || now == null) {
      return false;
    }

    return before.metadataFileLocation() != null
        && before.metadataFileLocation().equals(now.metadataFileLocation());
  }

  private static Set<Long> snapshotIds(TableMetadata metadata) {
    if (metadata == null) {
      return Set.of();
    }

    return metadata.snapshots().stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
  }

  private void commitSimpleTransaction() {
    // if there were no changes, don't try to commit
    if (base == current) {
      return;
    }

    Set<Long> startingSnapshots =
        base.snapshots().stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
    try {
      Tasks.foreach(ops)
          .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
          .exponentialBackoff(
              base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
              base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
              base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
              2.0 /* exponential */)
          .onlyRetryOn(CommitFailedException.class)
          .run(
              underlyingOps -> {
                applyUpdates(underlyingOps);

                underlyingOps.commit(base, current);
              });

    } catch (CommitStateUnknownException e) {
      throw e;

    } catch (PendingUpdateFailedException e) {
      cleanUp();
      throw e.wrapped();
    } catch (RuntimeException e) {
      if (!ops.requireStrictCleanup() || e instanceof CleanableFailure) {
        cleanUp();
      }

      throw e;
    }

    // the commit succeeded
    cleanUpAfterCommit(startingSnapshots);
    cleanUpExpiredSnapshots();
  }

  private void cleanUpAfterCommit(Set<Long> startingSnapshots) {
    try {
      // clean up the data files that were deleted by each operation. first, get the list of
      // committed manifests to ensure that no committed manifest is deleted.
      // A manifest could be deleted in one successful operation commit, but reused in another
      // successful commit of that operation if the whole transaction is retried.
      Set<Snapshot> newSnapshots = Sets.newHashSet();
      for (Snapshot snapshot : current.snapshots()) {
        if (!startingSnapshots.contains(snapshot.snapshotId())) {
          newSnapshots.add(snapshot);
        }
      }

      Set<String> committedFiles = committedFiles(ops.io(), newSnapshots);
      // delete all the files that were deleted in the most recent set of operation commits
      Set<String> uncommittedFiles =
          deletedFiles.stream()
              .filter(f -> !committedFiles.contains(f))
              .collect(Collectors.toSet());
      deleteUncommittedFiles(uncommittedFiles);

    } catch (RuntimeException e) {
      LOG.warn("Failed to load committed metadata, skipping clean-up", e);
    }
  }

  // the files of the snapshots that the transaction's expiries removed, now that it has committed
  // their removal
  private void cleanUpExpiredSnapshots() {
    for (PendingUpdate update : updates) {
      if (update instanceof RemoveSnapshots) {
        try {
          ((RemoveSnapshots) update).cleanUpAfterTransaction();
        } catch (RuntimeException e) {
          LOG.warn("Failed to clean up the files of expired snapshots", e);
        }
      }
    }
  }

  protected void cleanUp() {
    // the commit failed and no files were committed. clean up each update.
    cleanAllUpdates();

    // delete all the uncommitted files
    deleteUncommittedFiles(deletedFiles);
  }

  private void cleanAllUpdates() {
    Tasks.foreach(updates)
        .suppressFailureWhenFinished()
        .run(
            update -> {
              if (update instanceof SnapshotProducer) {
                ((SnapshotProducer) update).cleanAll();
              }
            });
  }

  private void deleteUncommittedFiles(Iterable<String> paths) {
    CatalogUtil.deleteFiles(ops.io(), paths, "uncommitted");
  }

  private void applyUpdates(TableOperations underlyingOps) {
    if (base != underlyingOps.refresh()) {
      // use refreshed the metadata
      this.base = underlyingOps.current();
      this.current = underlyingOps.current();
      for (PendingUpdate update : updates) {
        // re-commit each update in the chain to apply it and update current
        try {
          update.commit();
        } catch (CommitFailedException e) {
          // Cannot pass even with retry due to conflicting metadata changes. So, break the
          // retry-loop.
          throw new PendingUpdateFailedException(e);
        }
      }
    }
  }

  // returns the manifest lists and manifests referenced by the given committed snapshots
  private static Set<String> committedFiles(FileIO io, Set<Snapshot> snapshots) {
    Set<String> committedFiles = Sets.newHashSet();

    for (Snapshot snap : snapshots) {
      committedFiles.add(snap.manifestListLocation());
      snap.allManifests(io).forEach(manifest -> committedFiles.add(manifest.path()));
    }

    return committedFiles;
  }

  public class TransactionTableOperations implements TableOperations {
    private TableOperations tempOps = ops.temp(current);

    @Override
    public TableMetadata current() {
      return current;
    }

    @Override
    public TableMetadata refresh() {
      return current;
    }

    @Override
    @SuppressWarnings("ConsistentOverrides")
    public void commit(TableMetadata underlyingBase, TableMetadata metadata) {
      if (underlyingBase != current) {
        // trigger a refresh and retry
        throw new CommitFailedException("Table metadata refresh is required");
      }

      BaseTransaction.this.current = metadata;

      this.tempOps = ops.temp(metadata);

      BaseTransaction.this.hasLastOpCommitted = true;
    }

    @Override
    public FileIO io() {
      return tempOps.io();
    }

    @Override
    public EncryptionManager encryption() {
      return tempOps.encryption();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return tempOps.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
      return tempOps.locationProvider();
    }

    @Override
    public long newSnapshotId() {
      return tempOps.newSnapshotId();
    }
  }

  /**
   * Capability-transparent variant of {@link TransactionTableOperations} that forwards {@link
   * ManifestListSink} calls to the underlying ops. Selected at construction when the underlying ops
   * implements the sink, so {@link SnapshotProducer#apply()}'s {@code instanceof ManifestListSink}
   * check sees the capability through the wrapper.
   */
  class TransactionTableOperationsWithSink extends TransactionTableOperations
      implements ManifestListSink {
    @Override
    public void stageManifestListDelta(
        long sequenceNumber,
        long snapshotId,
        Long parentSnapshotId,
        Long nextRowId,
        ManifestListSink.ManifestListDelta delta,
        Long nextRowIdAfter) {
      ((ManifestListSink) ops)
          .stageManifestListDelta(
              sequenceNumber, snapshotId, parentSnapshotId, nextRowId, delta, nextRowIdAfter);
    }
  }

  public class TransactionTable implements Table, HasTableOperations, Serializable {

    @Override
    public TableOperations operations() {
      return transactionOps;
    }

    @Override
    public String name() {
      return tableName;
    }

    @Override
    public void refresh() {}

    @Override
    public TableScan newScan() {
      throw new UnsupportedOperationException("Transaction tables do not support scans");
    }

    @Override
    public Schema schema() {
      return current.schema();
    }

    @Override
    public Map<Integer, Schema> schemas() {
      return current.schemasById();
    }

    @Override
    public PartitionSpec spec() {
      return current.spec();
    }

    @Override
    public Map<Integer, PartitionSpec> specs() {
      return current.specsById();
    }

    @Override
    public SortOrder sortOrder() {
      return current.sortOrder();
    }

    @Override
    public Map<Integer, SortOrder> sortOrders() {
      return current.sortOrdersById();
    }

    @Override
    public Map<String, String> properties() {
      return current.properties();
    }

    @Override
    public String location() {
      return current.location();
    }

    @Override
    public Snapshot currentSnapshot() {
      return current.currentSnapshot();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
      return current.snapshot(snapshotId);
    }

    @Override
    public Iterable<Snapshot> snapshots() {
      return current.snapshots();
    }

    @Override
    public List<HistoryEntry> history() {
      return current.snapshotLog();
    }

    @Override
    public UpdateSchema updateSchema() {
      return BaseTransaction.this.updateSchema();
    }

    @Override
    public UpdatePartitionSpec updateSpec() {
      return BaseTransaction.this.updateSpec();
    }

    @Override
    public UpdateProperties updateProperties() {
      return BaseTransaction.this.updateProperties();
    }

    @Override
    public ReplaceSortOrder replaceSortOrder() {
      return BaseTransaction.this.replaceSortOrder();
    }

    @Override
    public UpdateLocation updateLocation() {
      return BaseTransaction.this.updateLocation();
    }

    @Override
    public AppendFiles newAppend() {
      return BaseTransaction.this.newAppend();
    }

    @Override
    public AppendFiles newFastAppend() {
      return BaseTransaction.this.newFastAppend();
    }

    @Override
    public RewriteFiles newRewrite() {
      return BaseTransaction.this.newRewrite();
    }

    @Override
    public RewriteManifests rewriteManifests() {
      return BaseTransaction.this.rewriteManifests();
    }

    @Override
    public OverwriteFiles newOverwrite() {
      return BaseTransaction.this.newOverwrite();
    }

    @Override
    public RowDelta newRowDelta() {
      return BaseTransaction.this.newRowDelta();
    }

    @Override
    public ReplacePartitions newReplacePartitions() {
      return BaseTransaction.this.newReplacePartitions();
    }

    @Override
    public DeleteFiles newDelete() {
      return BaseTransaction.this.newDelete();
    }

    @Override
    public UpdateStatistics updateStatistics() {
      return BaseTransaction.this.updateStatistics();
    }

    @Override
    public UpdatePartitionStatistics updatePartitionStatistics() {
      return BaseTransaction.this.updatePartitionStatistics();
    }

    @Override
    public ExpireSnapshots expireSnapshots() {
      return BaseTransaction.this.expireSnapshots();
    }

    @Override
    public ManageSnapshots manageSnapshots() {
      throw new UnsupportedOperationException(
          "Transaction tables do not support managing snapshots");
    }

    @Override
    public Transaction newTransaction() {
      throw new UnsupportedOperationException("Cannot create a transaction within a transaction");
    }

    @Override
    public FileIO io() {
      return transactionOps.io();
    }

    @Override
    public EncryptionManager encryption() {
      return transactionOps.encryption();
    }

    @Override
    public LocationProvider locationProvider() {
      return transactionOps.locationProvider();
    }

    @Override
    public List<StatisticsFile> statisticsFiles() {
      return current.statisticsFiles();
    }

    @Override
    public List<PartitionStatisticsFile> partitionStatisticsFiles() {
      return current.partitionStatisticsFiles();
    }

    @Override
    public Map<String, SnapshotRef> refs() {
      return current.refs();
    }

    @Override
    public UUID uuid() {
      return UUID.fromString(current.uuid());
    }

    @Override
    public String toString() {
      return name();
    }

    Object writeReplace() {
      return SerializableTable.copyOf(this);
    }
  }

  @VisibleForTesting
  TableOperations ops() {
    return ops;
  }

  @VisibleForTesting
  Set<String> deletedFiles() {
    return deletedFiles;
  }

  /**
   * Exception used to avoid retrying {@link PendingUpdate} when it is failed with {@link
   * CommitFailedException}.
   */
  private static class PendingUpdateFailedException extends RuntimeException {
    private final CommitFailedException wrapped;

    private PendingUpdateFailedException(CommitFailedException cause) {
      super(cause);
      this.wrapped = cause;
    }

    public CommitFailedException wrapped() {
      return wrapped;
    }
  }
}
