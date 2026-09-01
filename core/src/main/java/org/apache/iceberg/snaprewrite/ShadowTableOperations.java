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
package org.apache.iceberg.snaprewrite;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

/**
 * Serves rewritten metadata to a read-only table and refuses to commit it.
 *
 * <p>The rewritten snapshots are read through unmodified Iceberg scan planning, so the shadow table
 * exercises the same delete-file matching, manifest filtering, and sequence-number comparison a
 * committed table would. That matters because the sequence-number stamping fails silently in v2: a
 * mis-stamped delete is dropped during planning here exactly as it would be in production, and the
 * rows it should have hidden reappear where a test can see them.
 */
class ShadowTableOperations implements TableOperations {
  private final TableMetadata rewritten;
  private final FileIO io;

  ShadowTableOperations(TableMetadata rewritten, FileIO io) {
    this.rewritten = rewritten;
    this.io = io;
  }

  @Override
  public TableMetadata current() {
    return rewritten;
  }

  @Override
  public TableMetadata refresh() {
    return rewritten;
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    throw new UnsupportedOperationException(
        "Shadow table is read-only; use SnapshotRewriteResult.commit() to commit a rewrite");
  }

  @Override
  public FileIO io() {
    return io;
  }

  @Override
  public String metadataFileLocation(String fileName) {
    return rewritten.location() + "/metadata/" + fileName;
  }

  @Override
  public LocationProvider locationProvider() {
    return LocationProviders.locationsFor(rewritten.location(), rewritten.properties());
  }

  @Override
  public long newSnapshotId() {
    return ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
  }
}
