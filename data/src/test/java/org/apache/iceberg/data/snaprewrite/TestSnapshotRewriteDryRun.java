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
package org.apache.iceberg.data.snaprewrite;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.data.SnapshotRewriteDryRun;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

/**
 * The dry-run entry point, exercised the way the documentation says to invoke it.
 *
 * <p>Worth a test rather than a manual check: the documented command is the first thing anyone
 * runs, and the tool's whole promise is that it cannot modify a table. Both halves are asserted
 * here.
 */
public class TestSnapshotRewriteDryRun extends SnapshotRewriteTestBase {

  @Test
  public void reportsWithoutWritingAnything() throws IOException {
    Snapshot compaction = buildHistory();

    List<String> before = filesUnder(table.location());

    assertThatCode(
            () ->
                SnapshotRewriteDryRun.main(new String[] {"--dry-run", "--table", table.location()}))
        .doesNotThrowAnyException();

    assertThatCode(
            () ->
                SnapshotRewriteDryRun.main(
                    new String[] {
                      "--dry-run",
                      "--table",
                      table.location(),
                      "--reach",
                      String.valueOf(compaction.snapshotId())
                    }))
        .doesNotThrowAnyException();

    org.assertj.core.api.Assertions.assertThat(filesUnder(table.location()))
        .as("a dry run must not write")
        .isEqualTo(before);
  }

  /** A table with no compaction map has nothing to price, and says so rather than failing. */
  @Test
  public void handlesATableWithNoCompaction() throws IOException {
    append(records(1, 4, "base"));
    append(records(10, 2, "alpha"));

    assertThatCode(
            () ->
                SnapshotRewriteDryRun.main(new String[] {"--dry-run", "--table", table.location()}))
        .doesNotThrowAnyException();
  }

  /** {@code --dry-run} is required, so nothing runs by accident. */
  @Test
  public void requiresTheDryRunFlag() throws IOException {
    buildHistory();

    assertThatThrownBy(() -> SnapshotRewriteDryRun.main(new String[] {"--table", table.location()}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--dry-run is required");
  }

  @Test
  public void rejectsBadArguments() throws IOException {
    buildHistory();

    assertThatThrownBy(() -> SnapshotRewriteDryRun.main(new String[] {"--dry-run"}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--table is required");

    assertThatThrownBy(
            () ->
                SnapshotRewriteDryRun.main(
                    new String[] {"--dry-run", "--table", table.location(), "--nope"}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown argument");

    assertThatThrownBy(() -> SnapshotRewriteDryRun.main(new String[] {"--dry-run", "--table"}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires a value");
  }

  private Snapshot buildHistory() throws IOException {
    append(records(1, 8, "base"));
    compact();
    DataFile compacted = onlyDataFile();

    DataFile alpha = append(records(100, 4, "alpha"));
    delete(ImmutableList.of(at(compacted, 0), at(alpha, 1)));
    compact();

    append(records(200, 3, "beta"));
    return compact();
  }

  private DataFile onlyDataFile() throws IOException {
    List<DataFile> files = Lists.newArrayList();
    for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(
              manifest,
              table.io(),
              ((HasTableOperations) table).operations().current().specsById())) {
        for (DataFile file : reader) {
          files.add(file);
        }
      }
    }

    return files.get(0);
  }

  private static List<String> filesUnder(String location) {
    List<String> paths = Lists.newArrayList();
    collect(new File(location.replaceFirst("^file:", "")), paths);
    paths.sort(String::compareTo);
    return paths;
  }

  private static void collect(File directory, List<String> paths) {
    File[] children = directory.listFiles();
    if (children == null) {
      return;
    }

    for (File child : children) {
      if (child.isDirectory()) {
        collect(child, paths);
      } else {
        paths.add(child.getAbsolutePath());
      }
    }
  }
}
