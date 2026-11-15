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
package org.apache.iceberg.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.CatalogTransactionTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Catalog transactions on a JdbcCatalog, on a SQLite file. */
public class TestJdbcCatalogTransaction extends CatalogTransactionTests<JdbcCatalog> {
  private static final Namespace NS = Namespace.of("ns");
  private static final TableIdentifier TABLE_A = TableIdentifier.of(NS, "a");
  private static final TableIdentifier TABLE_B = TableIdentifier.of(NS, "b");

  @TempDir private Path dir;
  // runs once, just before the next database transaction of the catalog under test ends
  private final AtomicReference<Runnable> beforeEnd = new AtomicReference<>();
  // the next COMMIT of the catalog under test commits, then fails as a lost connection
  private final AtomicBoolean loseCommitAnswer = new AtomicBoolean(false);
  private final List<JdbcCatalog> catalogs = Lists.newArrayList();
  private JdbcCatalog catalog;

  @BeforeEach
  public void before() {
    this.catalog = newCatalog(true);
  }

  @AfterEach
  public void after() {
    catalogs.forEach(JdbcCatalog::close);
  }

  @Override
  protected JdbcCatalog catalog() {
    return catalog;
  }

  @Override
  protected boolean conflictsOnTableVersions() {
    return true;
  }

  @Override
  protected boolean supportsNamesWithDot() {
    // a namespace level with a dot is stored as two levels, as TestJdbcCatalog says
    return false;
  }

  /**
   * A JdbcCatalog on this test's database file, as another process would open it; with {@code
   * hooked}, its connections run {@link #beforeEnd} and {@link #loseCommitAnswer}.
   */
  private JdbcCatalog newCatalog(boolean hooked) {
    Map<String, String> properties =
        ImmutableMap.of(
            CatalogProperties.URI,
            "jdbc:sqlite:" + dir.resolve("catalog.db"),
            CatalogProperties.WAREHOUSE_LOCATION,
            dir.resolve("warehouse").toString());
    JdbcCatalog jdbcCatalog =
        new JdbcCatalog(
            null,
            hooked ? props -> new HookedClientPool(props.get(CatalogProperties.URI), props) : null,
            true);
    jdbcCatalog.setConf(new Configuration());
    jdbcCatalog.initialize("jdbc_txn", properties);
    catalogs.add(jdbcCatalog);
    return jdbcCatalog;
  }

  /** Tables ns.a and ns.b, each with one file. */
  private void createTables() {
    catalog.createNamespace(NS);
    catalog.createTable(TABLE_A, SCHEMA, SPEC).newFastAppend().appendFile(FILE_A).commit();
    catalog.createTable(TABLE_B, SCHEMA, SPEC).newFastAppend().appendFile(FILE_B).commit();
  }

  private TableMetadata current(TableIdentifier identifier) {
    return ((BaseTable) catalog.loadTable(identifier)).operations().current();
  }

  private int snapshots(TableIdentifier identifier) {
    return Lists.newArrayList(catalog.loadTable(identifier).snapshots()).size();
  }

  private Set<Path> metadataFiles() throws IOException {
    try (Stream<Path> files = Files.walk(dir.resolve("warehouse"))) {
      return files
          .filter(file -> file.getFileName().toString().endsWith(".metadata.json"))
          .collect(Collectors.toSet());
    }
  }

  /** A drop with purge deletes the table's files once the database transaction has committed. */
  @Test
  public void aPurgeDeletesTheDroppedTablesFilesAfterItsCommit() {
    createTables();
    FileIO io = catalog.loadTable(TABLE_B).io();
    TableMetadata dropped = current(TABLE_B);
    List<String> files = Lists.newArrayList(dropped.metadataFileLocation());
    dropped.previousFiles().forEach(file -> files.add(file.file()));
    files.add(dropped.currentSnapshot().manifestListLocation());
    assertThat(files).allMatch(file -> io.newInputFile(file).exists());

    CatalogTransaction catalogTx =
        catalog.createTransaction(CatalogTransaction.IsolationLevel.SNAPSHOT);
    assertThat(catalogTx.asCatalog().dropTable(TABLE_B, true)).isTrue();
    AtomicBoolean presentAtCommit = new AtomicBoolean(false);
    beforeEnd.set(
        () -> presentAtCommit.set(files.stream().allMatch(file -> io.newInputFile(file).exists())));
    catalogTx.commitTransaction();

    assertThat(presentAtCommit).as("the files are there while the drop commits").isTrue();
    assertThat(catalog.tableExists(TABLE_B)).isFalse();
    assertThat(files).noneMatch(file -> io.newInputFile(file).exists());
  }

  /**
   * A commit whose last condition fails, a namespace it drops that is no longer empty, rolls back
   * every change it made before, and deletes the metadata files it wrote; a table it dropped with
   * purge keeps its files.
   */
  @Test
  public void aFailedCommitChangesNoRowAndLeavesNoMetadataFile() throws IOException {
    createTables();
    TableIdentifier purged = TableIdentifier.of(NS, "c");
    catalog.createTable(purged, SCHEMA, SPEC).newFastAppend().appendFile(FILE_C).commit();
    Namespace empty = Namespace.of("empty");
    catalog.createNamespace(empty);
    Namespace made = Namespace.of("made");
    TableIdentifier created = TableIdentifier.of(made, "t");
    TableIdentifier renamed = TableIdentifier.of(NS, "d");

    CatalogTransaction catalogTx =
        catalog.createTransaction(CatalogTransaction.IsolationLevel.SNAPSHOT);
    Catalog txCatalog = catalogTx.asCatalog();
    SupportsNamespaces txNamespaces = (SupportsNamespaces) txCatalog;
    txNamespaces.setProperties(NS, ImmutableMap.of("k", "v"));
    txNamespaces.createNamespace(made, ImmutableMap.of("owner", "tx"));
    txCatalog.createTable(created, SCHEMA, SPEC).newFastAppend().appendFile(FILE_D).commit();
    txCatalog.loadTable(TABLE_A).newFastAppend().appendFile(FILE_D).commit();
    txCatalog.renameTable(TABLE_B, renamed);
    assertThat(txCatalog.dropTable(purged, true)).isTrue();
    assertThat(txNamespaces.dropNamespace(empty)).isTrue();

    // a table appears in the namespace the transaction drops, its last effect
    catalog.createTable(TableIdentifier.of(empty, "t"), SCHEMA);
    Map<TableIdentifier, String> locations =
        Stream.of(TABLE_A, TABLE_B, purged)
            .collect(Collectors.toMap(table -> table, t -> current(t).metadataFileLocation()));
    Set<Path> metadataFiles = metadataFiles();

    assertThatThrownBy(catalogTx::commitTransaction)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Cannot commit transaction: namespace empty is not empty");

    locations.forEach(
        (table, location) ->
            assertThat(current(table).metadataFileLocation()).as("%s", table).isEqualTo(location));
    assertThat(catalog.tableExists(renamed)).isFalse();
    assertThat(catalog.namespaceExists(made)).isFalse();
    assertThat(catalog.tableExists(created)).isFalse();
    assertThat(catalog.loadNamespaceMetadata(NS)).doesNotContainKey("k");
    assertThat(catalog.namespaceExists(empty)).isTrue();
    assertThat(metadataFiles()).isEqualTo(metadataFiles);
    assertThat(catalog.loadTable(purged).newScan().planFiles()).hasSize(1);
  }

  /**
   * A table commit from another process, racing a serializable transaction that read the table,
   * lands only after the transaction's database transaction ended, whether the transaction only
   * read or also wrote: the read held when the transaction committed.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void aTableCommitWaitsForATransactionThatReadTheTable(boolean writes) throws Exception {
    JdbcCatalog other = newCatalog(false);
    createTables();
    CatalogTransaction catalogTx =
        catalog.createTransaction(CatalogTransaction.IsolationLevel.SERIALIZABLE);
    Catalog txCatalog = catalogTx.asCatalog();
    assertThat(txCatalog.loadTable(TABLE_B).newScan().planFiles()).hasSize(1);
    if (writes) {
      txCatalog.loadTable(TABLE_A).newFastAppend().appendFile(FILE_C).commit();
    }

    ExecutorService executor = Executors.newSingleThreadExecutor();
    AtomicReference<Future<?>> racing = new AtomicReference<>();
    AtomicBoolean doneWhileOpen = new AtomicBoolean(false);
    beforeEnd.set(
        () -> {
          racing.set(
              executor.submit(
                  () -> other.loadTable(TABLE_B).newFastAppend().appendFile(FILE_D).commit()));
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }

          doneWhileOpen.set(racing.get().isDone());
        });
    try {
      catalogTx.commitTransaction();
      racing.get().get(30, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }

    assertThat(doneWhileOpen).as("the racing commit landed in the transaction").isFalse();
    assertThat(snapshots(TABLE_A)).isEqualTo(writes ? 2 : 1);
    assertThat(snapshots(TABLE_B)).isEqualTo(2);
  }

  /** A transaction swaps two tables' names: each rename's target is the other's source. */
  @Test
  public void aTransactionSwapsTwoTables() {
    createTables();
    String aUuid = catalog.loadTable(TABLE_A).uuid().toString();
    String bUuid = catalog.loadTable(TABLE_B).uuid().toString();
    TableIdentifier aside = TableIdentifier.of(NS, "aside");

    CatalogTransaction catalogTx =
        catalog.createTransaction(CatalogTransaction.IsolationLevel.SNAPSHOT);
    Catalog txCatalog = catalogTx.asCatalog();
    txCatalog.renameTable(TABLE_A, aside);
    txCatalog.renameTable(TABLE_B, TABLE_A);
    txCatalog.loadTable(TABLE_A).newFastAppend().appendFile(FILE_C).commit();
    txCatalog.renameTable(aside, TABLE_B);
    catalogTx.commitTransaction();

    assertThat(catalog.loadTable(TABLE_A).uuid().toString()).isEqualTo(bUuid);
    assertThat(catalog.loadTable(TABLE_B).uuid().toString()).isEqualTo(aUuid);
    assertThat(catalog.loadTable(TABLE_A).newScan().planFiles()).hasSize(2);
    assertThat(catalog.listTables(NS)).containsExactlyInAnyOrder(TABLE_A, TABLE_B);
  }

  /**
   * A COMMIT whose answer is lost: the pool runs the commit again, whose conditions fail because
   * the first run committed. A table names the metadata file the commit wrote, so it landed.
   */
  @Test
  public void aCommitWhoseAnswerIsLostLandedIfATableNamesItsFile() {
    createTables();
    CatalogTransaction catalogTx =
        catalog.createTransaction(CatalogTransaction.IsolationLevel.SNAPSHOT);
    catalogTx.asCatalog().loadTable(TABLE_A).newFastAppend().appendFile(FILE_C).commit();
    catalogTx.asCatalog().dropTable(TABLE_B, false);

    loseCommitAnswer.set(true);
    catalogTx.commitTransaction();

    assertThat(loseCommitAnswer).isFalse();
    assertThat(snapshots(TABLE_A)).isEqualTo(2);
    assertThat(catalog.tableExists(TABLE_B)).isFalse();
  }

  /** A COMMIT whose answer is lost, of a commit that wrote no metadata file, is unknown. */
  @Test
  public void aCommitWhoseAnswerIsLostIsUnknownWithoutAFileOfItsOwn() {
    createTables();
    CatalogTransaction catalogTx =
        catalog.createTransaction(CatalogTransaction.IsolationLevel.SNAPSHOT);
    catalogTx.asCatalog().dropTable(TABLE_B, false);

    loseCommitAnswer.set(true);
    assertThatThrownBy(catalogTx::commitTransaction)
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageContaining("Connection failed after COMMIT (test)");
    assertThat(catalog.tableExists(TABLE_B)).isFalse();
  }

  /** A client pool whose connections run the test's hooks when a database transaction ends. */
  private class HookedClientPool extends JdbcClientPool {
    HookedClientPool(String dbUrl, Map<String, String> properties) {
      super(dbUrl, properties);
    }

    @Override
    protected Connection newClient() {
      Connection connection = super.newClient();
      return (Connection)
          Proxy.newProxyInstance(
              Connection.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, args) -> {
                boolean commit = args == null && method.getName().equals("commit");
                if (commit || (args == null && method.getName().equals("rollback"))) {
                  Runnable hook = beforeEnd.getAndSet(null);
                  if (hook != null) {
                    hook.run();
                  }
                }

                Object result = invoke(connection, method, args);
                if (commit && loseCommitAnswer.getAndSet(false)) {
                  throw new SQLException("Connection failed after COMMIT (test)", "08006");
                }

                return result;
              });
    }

    private Object invoke(Object target, Method method, Object[] args) throws Exception {
      try {
        return method.invoke(target, args);
      } catch (InvocationTargetException e) {
        if (e.getCause() instanceof Exception) {
          throw (Exception) e.getCause();
        }

        throw (Error) e.getCause();
      }
    }
  }
}
