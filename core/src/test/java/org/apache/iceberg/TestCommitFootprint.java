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
import java.util.function.Consumer;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** The footprint each producer's inline snapshot carries ({@link CommitFootprint}). */
public class TestCommitFootprint {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).build();

  @TempDir Path dir;

  private int nextFile = 0;

  private DataFile newFile() {
    return DataFiles.builder(SPEC)
        .withPath("/path/to/data-" + nextFile++ + ".parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  /** The footprint of the snapshot {@code commit} commits to a table that inlines its lists. */
  private CommitFootprint footprint(int formatVersion, Consumer<Table> commit) {
    File tableDir = dir.resolve("table-" + nextFile++).toFile();
    assertThat(tableDir.mkdir()).isTrue();
    TestTables.TestTable table = TestTables.create(tableDir, "t", SCHEMA, SPEC, formatVersion);
    try {
      Table inline = new BaseTable(new TestManifestListSink.CapturingOps(table.operations()), "t");
      inline.newFastAppend().appendFile(newFile()).commit();
      commit.accept(inline);
      Snapshot snapshot = inline.currentSnapshot();
      assertThat(snapshot).isInstanceOf(InlineSnapshot.class);
      return ((InlineSnapshot) snapshot).footprint();
    } finally {
      TestTables.clearTables();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3})
  public void appendsThatValidateNothingAreBlind(int formatVersion) {
    assertThat(footprint(formatVersion, t -> t.newFastAppend().appendFile(newFile()).commit()))
        .isEqualTo(CommitFootprint.BLIND);
    assertThat(footprint(formatVersion, t -> t.newAppend().appendFile(newFile()).commit()))
        .isEqualTo(CommitFootprint.BLIND);
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3})
  public void anAppendWithAnAncestryValidatorIsOpaque(int formatVersion) {
    SnapshotAncestryValidator validator =
        new SnapshotAncestryValidator() {
          @Override
          public boolean validate(Iterable<Snapshot> ancestors) {
            return true;
          }

          @Override
          public String errorMessage() {
            return "never";
          }
        };
    assertThat(
            footprint(
                formatVersion,
                t -> t.newFastAppend().appendFile(newFile()).validateWith(validator).commit()))
        .isEqualTo(CommitFootprint.OPAQUE);
    assertThat(
            footprint(
                formatVersion,
                t -> t.newAppend().appendFile(newFile()).validateWith(validator).commit()))
        .isEqualTo(CommitFootprint.OPAQUE);
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3})
  public void otherProducersAreOpaque(int formatVersion) {
    assertThat(
            footprint(
                formatVersion,
                t -> t.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit()))
        .isEqualTo(CommitFootprint.OPAQUE);
    assertThat(
            footprint(
                formatVersion,
                t ->
                    t.newOverwrite()
                        .overwriteByRowFilter(Expressions.alwaysTrue())
                        .addFile(newFile())
                        .commit()))
        .isEqualTo(CommitFootprint.OPAQUE);
    assertThat(footprint(formatVersion, t -> t.newRowDelta().addRows(newFile()).commit()))
        .isEqualTo(CommitFootprint.OPAQUE);
  }
}
