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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Snapshot expiry keeps a compaction map while a manifest of a retained snapshot names it, and
 * deletes it once none does. Both cleanup strategies are covered.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestCompactionMapCleanup {
  private static final Schema SCHEMA = new Schema(required(1, "id", Types.IntegerType.get()));

  @Parameters(name = "incrementalCleanup = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(true, false);
  }

  @Parameter private boolean incrementalCleanup;

  @TempDir private File tableDir;

  private Table table;
  private int fileCount = 0;

  @BeforeEach
  public void createTable() {
    table = TestTables.create(tableDir, "test", SCHEMA, PartitionSpec.unpartitioned(), 2);
    table.updateProperties().set(TableProperties.COMPACTION_MAP_ENABLED, "true").commit();
  }

  @AfterEach
  public void clearTables() {
    TestTables.clearTables();
  }

  @TestTemplate
  public void reachableFilesIncludeTheMapsManifestsName() {
    DataFile first = dataFile(100);
    DataFile second = dataFile(100);
    table.newAppend().appendFile(first).appendFile(second).commit();
    String map = compact(dataFile(200), first, second);

    assertThat(ReachableFileUtil.compactionMapLocations(table)).containsExactly(map);
  }

  @TestTemplate
  public void expiryKeepsAMapThatARetainedManifestNames() {
    DataFile first = dataFile(100);
    DataFile second = dataFile(100);
    table.newAppend().appendFile(first).appendFile(second).commit();
    String map = compact(dataFile(200), first, second);
    table.newAppend().appendFile(dataFile(10)).commit();

    Set<String> deleted = expireAllButCurrent();

    assertThat(deleted).doesNotContain(map);
    assertThat(table.io().newInputFile(map).exists()).isTrue();
  }

  @TestTemplate
  public void expiryDeletesAMapThatNoRetainedManifestNames() {
    DataFile first = dataFile(100);
    DataFile second = dataFile(100);
    table.newAppend().appendFile(first).appendFile(second).commit();
    DataFile compacted = dataFile(200);
    String firstMap = compact(compacted, first, second);
    String secondMap = compact(dataFile(200), compacted);

    // The next commit drops the manifest that marked the first compaction's output deleted
    table.newAppend().appendFile(dataFile(10)).commit();
    assertThat(mapsNamedBy(table.currentSnapshot())).containsExactly(secondMap);

    Set<String> deleted = expireAllButCurrent();

    assertThat(deleted).contains(firstMap).doesNotContain(secondMap);
  }

  @TestTemplate
  public void aMergedManifestKeepsItsMap() {
    table
        .updateProperties()
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "2")
        .set(TableProperties.MANIFEST_MERGE_ENABLED, "true")
        .commit();
    DataFile first = dataFile(100);
    DataFile second = dataFile(100);
    table.newAppend().appendFile(first).appendFile(second).commit();
    String map = compact(dataFile(200), first, second);

    // The append merges the compaction's manifest into a new one, which carries the location
    table.newAppend().appendFile(dataFile(10)).commit();
    Snapshot current = table.currentSnapshot();
    assertThat(current.dataManifests(table.io()))
        .as("the compaction's manifest was merged away")
        .allMatch(manifest -> manifest.snapshotId() == current.snapshotId());
    assertThat(mapsNamedBy(current)).containsExactly(map);

    Set<String> deleted = expireAllButCurrent();

    assertThat(deleted).doesNotContain(map);
  }

  private Set<String> expireAllButCurrent() {
    long now = waitUntilAfter(table.currentSnapshot().timestampMillis());
    Set<String> deleted = ConcurrentHashMap.newKeySet();
    ((RemoveSnapshots) table.expireSnapshots())
        .withIncrementalCleanup(incrementalCleanup)
        .expireOlderThan(now)
        .deleteWith(deleted::add)
        .commit();
    return deleted;
  }

  /** Compacts {@code inputs} into {@code output}, and returns the location of the map it wrote. */
  private String compact(DataFile output, DataFile... inputs) {
    RewriteFiles rewrite = table.newRewrite();
    for (DataFile input : inputs) {
      rewrite.deleteFile(input);
    }

    rewrite.addFile(output);
    ConcatenationMaps.concatenate(table, rewrite, Arrays.asList(inputs), output).commit();

    Snapshot compaction = table.currentSnapshot();
    List<String> written =
        compaction.dataManifests(table.io()).stream()
            .filter(manifest -> manifest.snapshotId() == compaction.snapshotId())
            .filter(ManifestFile::hasAddedFiles)
            .map(ManifestFile::compactionMapLocation)
            .filter(Objects::nonNull)
            .distinct()
            .collect(Collectors.toList());
    assertThat(written).as("the compaction wrote one map").hasSize(1);
    return written.get(0);
  }

  private Set<String> mapsNamedBy(Snapshot snapshot) {
    return snapshot.dataManifests(table.io()).stream()
        .map(ManifestFile::compactionMapLocation)
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
  }

  private DataFile dataFile(long records) {
    fileCount += 1;
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("/data/file-" + fileCount + ".parquet")
        .withFileSizeInBytes(1024)
        .withRecordCount(records)
        .build();
  }

  private static long waitUntilAfter(long timestampMillis) {
    long current = System.currentTimeMillis();
    while (current <= timestampMillis) {
      current = System.currentTimeMillis();
    }

    return current;
  }
}
