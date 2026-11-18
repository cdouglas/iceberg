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

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.immutables.value.Value;

/**
 * What a serializable catalog transaction read of a table, which must read the same where its
 * commit takes effect.
 *
 * <p>A <em>scan read</em> ({@link #ref()} set) is the rows of a branch or tag under a row filter,
 * as of the snapshot the ref named when the transaction first read the table ({@link
 * #snapshotId()}, null if it named none): what a table scan's planning read. It is unchanged if no
 * data or delete file matching the filter was added to the ref since that snapshot, no data file
 * matching it was removed except by a rewrite that keeps its rows, the ref was not reset to a
 * snapshot that does not descend from it, and the schema the scan reads with is the same: a
 * branch's (or a missing ref's) is the table's current schema, so a column renamed, dropped and
 * added again, or promoted changes what the scan read without moving the ref; a tag's is its
 * snapshot's. A <em>declared read</em> ({@link #ref()} null) is the table's whole version, for a
 * transaction that decided on its schema, properties or snapshots without scanning ({@link
 * CatalogTransaction#declareRead}).
 *
 * <p>A catalog may judge a read more coarsely than that and fail more commits, but must never
 * commit one whose read changed: {@link #unchangedIn} is the coarsest judgment, by the table's
 * version.
 */
@Value.Immutable
public interface TableRead {
  /** The table, by the name it had when the transaction started. */
  TableIdentifier identifier();

  /** The table's version as the transaction first read it. */
  TableMetadata metadata();

  /** The branch or tag a scan read, or null for a declared read of the whole version. */
  @Nullable
  String ref();

  /** The snapshot {@link #ref()} named in {@link #metadata()}, or null if it named none. */
  @Nullable
  Long snapshotId();

  /** A scan read's row filter. */
  @Value.Default
  default Expression filter() {
    return Expressions.alwaysTrue();
  }

  /** Whether a scan read's filter binds column names case-sensitively. */
  @Value.Default
  default boolean caseSensitive() {
    return true;
  }

  default boolean isScan() {
    return ref() != null;
  }

  /**
   * Whether {@code current}, the table where the commit takes effect (null if there is none), reads
   * as this read did, judged by the table's version: a declared read by the whole version, a scan
   * read by the snapshot its ref names and the schema the scan reads with. True only where the read
   * is unchanged; false also where only rows outside the read changed.
   */
  default boolean unchangedIn(TableMetadata current) {
    if (current == null || !Objects.equals(current.uuid(), metadata().uuid())) {
      return false;
    }
    if (!isScan()) {
      return Objects.equals(current.metadataFileLocation(), metadata().metadataFileLocation());
    }
    SnapshotRef now = current.ref(ref());
    return Objects.equals(now != null ? now.snapshotId() : null, snapshotId())
        && scanSchema(current, ref()).asStruct().equals(scanSchema(metadata(), ref()).asStruct());
  }

  /**
   * The schema a scan of {@code ref} reads with in {@code metadata}: a tag's snapshot's, else (a
   * branch, or a ref {@code metadata} lacks) the table's current schema.
   */
  private static Schema scanSchema(TableMetadata metadata, String ref) {
    SnapshotRef named = metadata.ref(ref);
    if (named == null || named.isBranch()) {
      return metadata.schema();
    }

    Snapshot snapshot = metadata.snapshot(named.snapshotId());
    Schema schema =
        snapshot != null && snapshot.schemaId() != null
            ? metadata.schemasById().get(snapshot.schemaId())
            : null;
    return schema != null ? schema : metadata.schema();
  }

  /** A scan read of {@code ref} of the table as the transaction first read it. */
  static TableRead scan(
      TableIdentifier identifier,
      TableMetadata metadata,
      String ref,
      Expression filter,
      boolean caseSensitive) {
    SnapshotRef named = metadata.ref(ref);
    return ImmutableTableRead.builder()
        .identifier(identifier)
        .metadata(metadata)
        .ref(ref)
        .snapshotId(named != null ? named.snapshotId() : null)
        .filter(filter)
        .caseSensitive(caseSensitive)
        .build();
  }

  /** A declared read of the table's whole version, as the transaction first read it. */
  static TableRead whole(TableIdentifier identifier, TableMetadata metadata) {
    return ImmutableTableRead.builder().identifier(identifier).metadata(metadata).build();
  }
}
