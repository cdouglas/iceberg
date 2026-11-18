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
package org.apache.iceberg.rest;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * An expiry's file cleanup deletes the files of the snapshots it removed, not those of snapshots
 * another writer removed while it ran: on a REST catalog, whose server applies the removal to the
 * table as it is then (a {@code remove-snapshots} update carries no requirement), and on any
 * catalog when a removal lands between the expiry's commit and its cleanup's refresh. Its cleanup
 * compares the table after its commit with the metadata it computed its removal on, less the
 * snapshots others removed, and cleans up incrementally only when there are none.
 *
 * <p>History: S1 appends a, S2 appends b, S3 deletes b. Writer Y's expiry reads all three.
 */
class TestExpiryCleanupStaleBase {
  private static final Namespace NS = Namespace.of("ns");
  private static final TableIdentifier ID = TableIdentifier.of(NS, "t");
  private static final Schema SCHEMA = new Schema(required(1, "id", Types.LongType.get()));

  private final InMemoryFileIO io = new InMemoryFileIO();
  private String warehouse;
  private InMemoryCatalog backend;
  private RESTCatalog rest;

  /** Run on the server, once, just before it applies the next table update. */
  private final AtomicReference<Runnable> beforeUpdate = new AtomicReference<>();

  /** How Y picks S1, which picks the cleanup strategy. */
  enum Pick {
    /** By id: {@code ReachableFileCleanup}. */
    BY_ID,
    /** By age: {@code IncrementalFileCleanup}, the table's history being linear. */
    BY_AGE;

    ExpireSnapshots expire(Table table, History history) {
      ExpireSnapshots expiry = table.expireSnapshots();
      return this == BY_ID
          ? expiry.expireSnapshotId(history.s1)
          : expiry.expireOlderThan(table.snapshot(history.s2).timestampMillis());
    }
  }

  /** Snapshot ids and S2's own files: its manifest list and the manifests S3 does not keep. */
  private static class History {
    private long s1;
    private long s2;
    private long s3;
    private String s1List;
    private Set<String> s2Metadata;
    private String fileB;
  }

  @BeforeEach
  void before() {
    warehouse = "mem://wh-" + UUID.randomUUID();
    backend = new InMemoryCatalog();
    backend.initialize("backend", Map.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse));
    backend.createNamespace(NS);
    RESTCatalogAdapter adapter =
        new RESTCatalogAdapter(backend) {
          @Override
          public <T extends RESTResponse> T handleRequest(
              Route route,
              Map<String, String> vars,
              HTTPRequest httpRequest,
              Class<T> responseType,
              Consumer<Map<String, String>> responseHeaders) {
            if (route == Route.UPDATE_TABLE) {
              Runnable race = beforeUpdate.getAndSet(null);
              if (race != null) {
                race.run();
              }
            }
            return super.handleRequest(route, vars, httpRequest, responseType, responseHeaders);
          }
        };
    rest = new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), config -> adapter);
    rest.initialize(
        "rest",
        Map.of(
            CatalogProperties.URI,
            "http://localhost:8181",
            CatalogProperties.FILE_IO_IMPL,
            InMemoryFileIO.class.getName()));
  }

  @AfterEach
  void after() throws IOException {
    rest.close();
    backend.close();
  }

  private DataFile dataFile(String name) {
    String path = warehouse + "/data/" + name + ".parquet";
    try (OutputStream out = io.newOutputFile(path).createOrOverwrite()) {
      out.write(new byte[] {1, 2, 3});
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(path)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(3)
        .withRecordCount(1)
        .build();
  }

  private static void nextMillisecond(Table table) {
    long last = table.currentSnapshot().timestampMillis();
    while (System.currentTimeMillis() <= last) {
      Thread.onSpinWait();
    }
  }

  private History history(Catalog catalog) {
    Table table = catalog.createTable(ID, SCHEMA, PartitionSpec.unpartitioned());
    DataFile fileA = dataFile("a");
    DataFile fileB = dataFile("b");
    History history = new History();
    table.newFastAppend().appendFile(fileA).commit();
    history.s1 = table.currentSnapshot().snapshotId();
    nextMillisecond(table);
    table.newFastAppend().appendFile(fileB).commit();
    history.s2 = table.currentSnapshot().snapshotId();
    nextMillisecond(table);
    table.newDelete().deleteFile(fileB).commit();
    history.s3 = table.currentSnapshot().snapshotId();

    Set<String> s3Manifests = manifests(table, history.s3);
    history.s1List = table.snapshot(history.s1).manifestListLocation();
    history.s2Metadata = Sets.newHashSet(manifests(table, history.s2));
    history.s2Metadata.removeAll(s3Manifests);
    history.s2Metadata.add(table.snapshot(history.s2).manifestListLocation());
    history.fileB = fileB.location();
    assertThat(history.s2Metadata).hasSize(2);
    return history;
  }

  private static Set<String> manifests(Table table, long snapshotId) {
    return table.snapshot(snapshotId).allManifests(table.io()).stream()
        .map(ManifestFile::path)
        .collect(Collectors.toSet());
  }

  private boolean exists(String path) {
    return io.fileExists(path);
  }

  /** X expires S2 and keeps its files. */
  private Runnable expireS2KeepingFiles(History history) {
    return () ->
        backend
            .loadTable(ID)
            .expireSnapshots()
            .expireSnapshotId(history.s2)
            .cleanExpiredFiles(false)
            .commit();
  }

  /**
   * X rolls main back from S3, which deleted b, to S2, and expires S3, keeping its files (as
   * Spark's expire action commits before it deletes).
   */
  private Runnable rollBackAndExpireS3(History history) {
    return () -> {
      Table table = backend.loadTable(ID);
      table.manageSnapshots().rollbackTo(history.s2).commit();
      table.expireSnapshots().expireSnapshotId(history.s3).cleanExpiredFiles(false).commit();
    };
  }

  private void assertKeepsS2(History history) {
    Table now = backend.loadTable(ID);
    assertThat(now.snapshots()).extracting(Snapshot::snapshotId).containsExactly(history.s3);
    assertThat(exists(history.s1List)).as("Y cleaned up S1").isFalse();
    SoftAssertions.assertSoftly(
        softly -> {
          for (String path : history.s2Metadata) {
            softly
                .assertThat(exists(path))
                .as("S2's %s, which X kept", path.contains("/snap-") ? "manifest list" : "manifest")
                .isTrue();
          }
          softly
              .assertThat(exists(history.fileB))
              .as("b, which only S2 has, which X kept")
              .isTrue();
        });
  }

  private void assertKeepsLiveB(History history) {
    Table now = backend.loadTable(ID);
    assertThat(now.currentSnapshot().snapshotId()).isEqualTo(history.s2);
    Set<String> live = Sets.newHashSet();
    try (CloseableIterable<FileScanTask> tasks = now.newScan().planFiles()) {
      tasks.forEach(task -> live.add(task.file().location()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    assertThat(live).as("b is live in the table").contains(history.fileB);
    assertThat(exists(history.fileB)).as("b, live in the table, still exists").isTrue();
  }

  // REST: the server applies Y's removal of S1 to the table as it is when Y's request arrives

  @ParameterizedTest
  @EnumSource(Pick.class)
  void restExpiryKeepsTheFilesOfASnapshotAnotherWriterExpired(Pick pick) {
    History history = history(rest);
    beforeUpdate.set(expireS2KeepingFiles(history));

    pick.expire(rest.loadTable(ID), history).commit();

    assertThat(beforeUpdate.get()).as("X raced Y").isNull();
    assertKeepsS2(history);
  }

  @Test
  void restExpiryKeepsALiveFileAfterARollback() {
    History history = history(rest);
    beforeUpdate.set(rollBackAndExpireS3(history));

    Pick.BY_AGE.expire(rest.loadTable(ID), history).commit();

    assertThat(beforeUpdate.get()).as("X raced Y").isNull();
    assertKeepsLiveB(history);
  }

  // A catalog that swaps metadata: X lands before Y's commit, which fails; Y retries on a fresh
  // read

  @ParameterizedTest
  @EnumSource(Pick.class)
  void swappingExpiryRacedBeforeItsCommitKeepsTheFilesOfASnapshotAnotherWriterExpired(Pick pick) {
    History history = history(backend);
    Table expiring = racing(expireS2KeepingFiles(history), true);

    pick.expire(expiring, history).commit();

    assertKeepsS2(history);
  }

  @Test
  void swappingExpiryRacedBeforeItsCommitKeepsALiveFileAfterARollback() {
    History history = history(backend);
    Table expiring = racing(rollBackAndExpireS3(history), true);

    Pick.BY_AGE.expire(expiring, history).commit();

    assertKeepsLiveB(history);
  }

  // A catalog that swaps metadata: X lands after Y's commit, before Y's cleanup refreshes

  @ParameterizedTest
  @EnumSource(Pick.class)
  void swappingExpiryRacedAfterItsCommitKeepsTheFilesOfASnapshotAnotherWriterExpired(Pick pick) {
    History history = history(backend);
    Table expiring = racing(expireS2KeepingFiles(history), false);

    pick.expire(expiring, history).commit();

    assertKeepsS2(history);
  }

  @Test
  void swappingExpiryRacedAfterItsCommitKeepsALiveFileAfterARollback() {
    History history = history(backend);
    Table expiring = racing(rollBackAndExpireS3(history), false);

    Pick.BY_AGE.expire(expiring, history).commit();

    assertKeepsLiveB(history);
  }

  /** Y's handle on the backend, running {@code race} once just before or just after its commit. */
  private Table racing(Runnable race, boolean beforeCommit) {
    TableOperations ops = ((HasTableOperations) backend.loadTable(ID)).operations();
    AtomicReference<Runnable> once = new AtomicReference<>(race);
    TableOperations racing =
        new TableOperations() {
          @Override
          public TableMetadata current() {
            return ops.current();
          }

          @Override
          public TableMetadata refresh() {
            return ops.refresh();
          }

          @Override
          public void commit(TableMetadata base, TableMetadata metadata) {
            if (beforeCommit) {
              runOnce();
            }
            ops.commit(base, metadata);
            if (!beforeCommit) {
              runOnce();
            }
          }

          private void runOnce() {
            Runnable run = once.getAndSet(null);
            if (run != null) {
              run.run();
            }
          }

          @Override
          public FileIO io() {
            return ops.io();
          }

          @Override
          public String metadataFileLocation(String fileName) {
            return ops.metadataFileLocation(fileName);
          }

          @Override
          public LocationProvider locationProvider() {
            return ops.locationProvider();
          }
        };
    return new BaseTable(racing, ID.toString());
  }
}
