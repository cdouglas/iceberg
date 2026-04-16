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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

/**
 * A {@link Snapshot} implementation that holds its manifest list in memory instead of reading it
 * from a separate Avro file. Used by {@link SnapshotProducer#apply()} when {@link TableOperations}
 * implements {@link ManifestListSink}.
 *
 * <p>{@link #manifestListLocation()} returns {@code null} — the contract documented on the
 * interface allows this ("null if it is not separate"). Consumers of this snapshot read manifests
 * via the {@link #allManifests(FileIO)} / {@link #dataManifests(FileIO)} / {@link
 * #deleteManifests(FileIO)} methods, which return the inline list directly without touching the
 * provided {@code FileIO}.
 *
 * <p>The file-diff methods ({@link #addedDataFiles}, {@link #removedDataFiles}, {@link
 * #addedDeleteFiles}, {@link #removedDeleteFiles}) still need a real {@link FileIO} because they
 * read individual manifest files (not manifest lists) to identify which files were added/removed in
 * this snapshot. The inline manifest list is only the list-level state; manifest files themselves
 * remain external.
 *
 * <p><b>Identity:</b> {@link #equals(Object)} and {@link #hashCode()} compare only scalar identity
 * fields ({@code snapshotId}, {@code parentId}, {@code sequenceNumber}, {@code timestampMillis},
 * {@code schemaId}), <em>not</em> the manifest list contents. This matches {@link BaseSnapshot}'s
 * contract and supports {@link java.util.Set Set}-based membership checks across mixed
 * {@code BaseSnapshot} / {@code InlineSnapshot} instances (e.g. in {@code ReachableFileCleanup}).
 * Two {@code InlineSnapshot} instances with the same scalar identity and different manifest lists
 * are {@code equal()} — callers that need to detect manifest-pool divergence must compare
 * {@link #allManifests(FileIO)} explicitly rather than relying on {@code equals}.
 */
public class InlineSnapshot implements Snapshot {

  private static final long serialVersionUID = 1L;

  private final long snapshotId;
  private final Long parentId;
  private final long sequenceNumber;
  private final long timestampMillis;
  private final String operation;
  private final Map<String, String> summary;
  private final Integer schemaId;
  private final Long firstRowId;
  private final Long addedRows;
  private final String keyId;
  private final List<ManifestFile> manifests;

  // lazy-computed partitions of the inline list
  private transient List<ManifestFile> dataManifestsView = null;
  private transient List<ManifestFile> deleteManifestsView = null;

  // lazy-computed per-snapshot diffs (require reading manifest files)
  private transient List<DataFile> addedDataFiles = null;
  private transient List<DataFile> removedDataFiles = null;
  private transient List<DeleteFile> addedDeleteFiles = null;
  private transient List<DeleteFile> removedDeleteFiles = null;

  public InlineSnapshot(
      long sequenceNumber,
      long snapshotId,
      Long parentId,
      long timestampMillis,
      String operation,
      Map<String, String> summary,
      Integer schemaId,
      Long firstRowId,
      Long addedRows,
      String keyId,
      List<ManifestFile> manifests) {
    this.sequenceNumber = sequenceNumber;
    this.snapshotId = snapshotId;
    this.parentId = parentId;
    this.timestampMillis = timestampMillis;
    this.operation = operation;
    this.summary = summary;
    this.schemaId = schemaId;
    this.firstRowId = firstRowId;
    this.addedRows = firstRowId != null ? addedRows : null;
    this.keyId = keyId;
    this.manifests = ImmutableList.copyOf(manifests);
  }

  @Override
  public long sequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public long snapshotId() {
    return snapshotId;
  }

  @Override
  public Long parentId() {
    return parentId;
  }

  @Override
  public long timestampMillis() {
    return timestampMillis;
  }

  @Override
  public String operation() {
    return operation;
  }

  @Override
  public Map<String, String> summary() {
    return summary;
  }

  @Override
  public Integer schemaId() {
    return schemaId;
  }

  @Override
  public Long firstRowId() {
    return firstRowId;
  }

  @Override
  public Long addedRows() {
    return addedRows;
  }

  @Override
  public String keyId() {
    return keyId;
  }

  @Override
  public String manifestListLocation() {
    return null;
  }

  @Override
  public List<ManifestFile> allManifests(FileIO io) {
    return manifests;
  }

  @Override
  public List<ManifestFile> dataManifests(FileIO io) {
    if (dataManifestsView == null) {
      this.dataManifestsView =
          ImmutableList.copyOf(
              Iterables.filter(manifests, m -> m.content() == ManifestContent.DATA));
    }
    return dataManifestsView;
  }

  @Override
  public List<ManifestFile> deleteManifests(FileIO io) {
    if (deleteManifestsView == null) {
      this.deleteManifestsView =
          ImmutableList.copyOf(
              Iterables.filter(manifests, m -> m.content() == ManifestContent.DELETES));
    }
    return deleteManifestsView;
  }

  @Override
  public Iterable<DataFile> addedDataFiles(FileIO io) {
    if (addedDataFiles == null) {
      cacheDataFileChanges(io);
    }
    return addedDataFiles;
  }

  @Override
  public Iterable<DataFile> removedDataFiles(FileIO io) {
    if (removedDataFiles == null) {
      cacheDataFileChanges(io);
    }
    return removedDataFiles;
  }

  @Override
  public Iterable<DeleteFile> addedDeleteFiles(FileIO io) {
    if (addedDeleteFiles == null) {
      cacheDeleteFileChanges(io);
    }
    return addedDeleteFiles;
  }

  @Override
  public Iterable<DeleteFile> removedDeleteFiles(FileIO io) {
    if (removedDeleteFiles == null) {
      cacheDeleteFileChanges(io);
    }
    return removedDeleteFiles;
  }

  private void cacheDataFileChanges(FileIO io) {
    ImmutableList.Builder<DataFile> adds = ImmutableList.builder();
    ImmutableList.Builder<DataFile> deletes = ImmutableList.builder();

    // read only manifests that were created by this snapshot
    Iterable<ManifestFile> changedManifests =
        Iterables.filter(
            dataManifests(io), manifest -> Objects.equal(manifest.snapshotId(), snapshotId));
    try (CloseableIterable<ManifestEntry<DataFile>> entries =
        new ManifestGroup(io, changedManifests).ignoreExisting().entries()) {
      for (ManifestEntry<DataFile> entry : entries) {
        switch (entry.status()) {
          case ADDED:
            adds.add(entry.file().copy());
            break;
          case DELETED:
            deletes.add(entry.file().copyWithoutStats());
            break;
          default:
            throw new IllegalStateException(
                "Unexpected entry status, not added or deleted: " + entry);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close entries while caching changes", e);
    }

    this.addedDataFiles = adds.build();
    this.removedDataFiles = deletes.build();
  }

  private void cacheDeleteFileChanges(FileIO io) {
    ImmutableList.Builder<DeleteFile> adds = ImmutableList.builder();
    ImmutableList.Builder<DeleteFile> deletes = ImmutableList.builder();

    Iterable<ManifestFile> changedManifests =
        Iterables.filter(
            deleteManifests(io), manifest -> Objects.equal(manifest.snapshotId(), snapshotId));

    for (ManifestFile manifest : changedManifests) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, io, null)) {
        for (ManifestEntry<DeleteFile> entry : reader.entries()) {
          switch (entry.status()) {
            case ADDED:
              adds.add(entry.file().copy());
              break;
            case DELETED:
              deletes.add(entry.file().copyWithoutStats());
              break;
            default:
              // ignore existing
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close manifest reader", e);
      }
    }

    this.addedDeleteFiles = adds.build();
    this.removedDeleteFiles = deletes.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof Snapshot) {
      Snapshot other = (Snapshot) o;
      return this.snapshotId == other.snapshotId()
          && Objects.equal(this.parentId, other.parentId())
          && this.sequenceNumber == other.sequenceNumber()
          && this.timestampMillis == other.timestampMillis()
          && Objects.equal(this.schemaId, other.schemaId());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        this.snapshotId, this.parentId, this.sequenceNumber, this.timestampMillis, this.schemaId);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", snapshotId)
        .add("timestamp_ms", timestampMillis)
        .add("operation", operation)
        .add("summary", summary)
        .add("manifest-count", manifests.size())
        .add("schema-id", schemaId)
        .toString();
  }
}
