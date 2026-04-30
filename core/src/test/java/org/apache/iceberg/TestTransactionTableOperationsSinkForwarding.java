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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies R2: {@link BaseTransaction.TransactionTableOperations} forwards the {@link
 * ManifestListSink} capability when the underlying ops implements it.
 *
 * <p>Before R2, the wrapper was minimal — it implemented only {@link TableOperations}, masking the
 * sink capability. {@link SnapshotProducer#apply()}'s {@code ops instanceof ManifestListSink} check
 * therefore returned false inside a transaction, and the producer fell back to writing a transient
 * {@code snap-*.avro}. A crash between the Avro write and the catalog commit would orphan the file.
 * After R2 the wrapper subclass is sink-transparent: the capability check sees the sink and the
 * inline path is taken.
 */
public class TestTransactionTableOperationsSinkForwarding {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).build();

  private static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(100)
          .build();

  private static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(20)
          .withRecordCount(200)
          .build();

  @TempDir Path tableDir;

  @Test
  public void transactionForwardsSinkAndWritesNoAvroManifestList() throws Exception {
    File dir = tableDir.resolve("sink-tx").toFile();
    assertThat(dir.mkdir()).isTrue();
    TestTables.TestTable raw = TestTables.create(dir, "sink-tx", SCHEMA, SPEC, 2);

    try {
      TestManifestListSink.CapturingOps capturing =
          new TestManifestListSink.CapturingOps(raw.operations());
      // Drive commits through a Transaction wrapper. Pre-R2, the wrapper masked
      // ManifestListSink and SnapshotProducer.apply() would call writeManifestList.
      // Post-R2, the wrapper forwards the sink and apply() takes the inline path.
      Transaction txn = Transactions.newTransaction("sink-tx", capturing);

      txn.newFastAppend().appendFile(FILE_A).commit();
      txn.newFastAppend().appendFile(FILE_B).commit();
      txn.commitTransaction();

      // Sink received both deltas — capability traversed the wrapper.
      assertThat(capturing.deltasBySnapshot).hasSize(2);
      capturing
          .deltasBySnapshot
          .values()
          .forEach(
              d -> {
                assertThat(d).isNotNull();
                assertThat(d.added()).isNotEmpty();
              });

      // Snapshot.manifestListLocation() is null for sink-staged snapshots.
      for (Snapshot s : raw.ops().current().snapshots()) {
        assertThat(s.manifestListLocation())
            .as(
                "snapshot %d manifestListLocation must be null when sink path is taken",
                s.snapshotId())
            .isNull();
      }

      // No transient snap-*.avro files were written by SnapshotProducer.
      File metadataDir = new File(dir, "metadata");
      File[] files = metadataDir.listFiles();
      assertThat(files).isNotNull();
      long avroSnapCount =
          java.util.Arrays.stream(files)
              .filter(f -> f.getName().startsWith("snap-") && f.getName().endsWith(".avro"))
              .count();
      assertThat(avroSnapCount)
          .as("snap-*.avro must not be written when the wrapper forwards the sink")
          .isZero();
    } finally {
      TestTables.clearTables();
    }
  }

  @Test
  public void transactionWithoutSinkUsesPlainWrapper() throws Exception {
    // Sanity: when the underlying ops does NOT implement ManifestListSink, the wrapper
    // is the plain TransactionTableOperations and the Avro path is taken.
    File dir = tableDir.resolve("no-sink-tx").toFile();
    assertThat(dir.mkdir()).isTrue();
    TestTables.TestTable raw = TestTables.create(dir, "no-sink-tx", SCHEMA, SPEC, 2);

    try {
      Transaction txn = Transactions.newTransaction("no-sink-tx", raw.operations());
      txn.newFastAppend().appendFile(FILE_A).commit();
      txn.commitTransaction();

      Snapshot snap = raw.ops().current().currentSnapshot();
      assertThat(snap.manifestListLocation())
          .as("plain wrapper must produce a real manifest-list location")
          .isNotNull()
          .endsWith(".avro");
    } finally {
      TestTables.clearTables();
    }
  }
}
