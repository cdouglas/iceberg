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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestRewriteTablePathUtil {

  @Test
  public void testStagingPathPreservesDirectoryStructure() {
    String sourcePrefix = "/source/table";
    String stagingDir = "/staging/";

    // Two files with same name but different paths
    String file1 = "/source/table/hash1/delete_0_0_0.parquet";
    String file2 = "/source/table/hash2/delete_0_0_0.parquet";

    String stagingPath1 = RewriteTablePathUtil.stagingPath(file1, sourcePrefix, stagingDir);
    String stagingPath2 = RewriteTablePathUtil.stagingPath(file2, sourcePrefix, stagingDir);

    // Should preserve directory structure to avoid conflicts
    assertThat(stagingPath1)
        .startsWith(stagingDir)
        .isEqualTo("/staging/hash1/delete_0_0_0.parquet")
        .isNotEqualTo(stagingPath2);
    assertThat(stagingPath2)
        .startsWith(stagingDir)
        .isEqualTo("/staging/hash2/delete_0_0_0.parquet");
  }

  @Test
  public void testStagingPathBackwardCompatibility() {
    // Test that the deprecated method still works
    String originalPath = "/some/path/file.parquet";
    String stagingDir = "/staging/";

    String result = RewriteTablePathUtil.stagingPath(originalPath, stagingDir);

    assertThat(result).isEqualTo("/staging/file.parquet");
  }

  @Test
  public void testStagingPathWithComplexPaths() {
    String sourcePrefix = "/warehouse/db/table";
    String stagingDir = "/tmp/staging/";

    String filePath = "/warehouse/db/table/data/year=2023/month=01/part-00001.parquet";
    String result = RewriteTablePathUtil.stagingPath(filePath, sourcePrefix, stagingDir);

    assertThat(result).isEqualTo("/tmp/staging/data/year=2023/month=01/part-00001.parquet");
  }

  @Test
  public void testStagingPathWithNoMiddlePart() {
    // Test case where file is directly under source prefix (no middle directory structure)
    String sourcePrefix = "/source/table";
    String stagingDir = "/staging/";
    String fileDirectlyUnderPrefix = "/source/table/file.parquet";

    // Test new method
    String newMethodResult =
        RewriteTablePathUtil.stagingPath(fileDirectlyUnderPrefix, sourcePrefix, stagingDir);

    // Test old deprecated method
    String oldMethodResult = RewriteTablePathUtil.stagingPath(fileDirectlyUnderPrefix, stagingDir);

    // Both methods should behave the same when there's no middle part
    assertThat(newMethodResult).isEqualTo("/staging/file.parquet");
    assertThat(oldMethodResult).isEqualTo("/staging/file.parquet");
    assertThat(newMethodResult).isEqualTo(oldMethodResult);
  }

  // -----------------------------------------------------------------------------------------
  // Inline-ML guard (R3 / errata D4)
  //
  // The path-rewrite migration utility writes a fresh manifest list per snapshot at the target
  // prefix. Inline-ML snapshots have no separate manifest list file (manifestListLocation()
  // returns null by contract); they cannot be rewritten by this utility. Both entry points that
  // touch manifestListLocation() must fail fast with a descriptive IllegalStateException rather
  // than NPE'ing inside ManifestLists.read.
  // -----------------------------------------------------------------------------------------

  @Test
  public void testRewriteManifestListRejectsInlineSnapshot() {
    Snapshot inline =
        new InlineSnapshot(
            1L,
            42L,
            null,
            0L,
            "append",
            ImmutableMap.of(),
            0,
            null,
            null,
            null,
            ImmutableList.of());
    FileIO io = Mockito.mock(FileIO.class);
    TableMetadata md = Mockito.mock(TableMetadata.class);

    assertThatThrownBy(
            () ->
                RewriteTablePathUtil.rewriteManifestList(
                    inline, io, md, Collections.emptySet(), "/src", "/dst", "/staging", "/out"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("inline-ML snapshot 42")
        .hasMessageContaining("evict to pointer mode");

    // The guard fires before any FileIO interaction.
    Mockito.verifyNoInteractions(io);
  }

  @Test
  public void testReplacePathsRejectsInlineSnapshot() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    TableMetadata empty =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), "s3://bucket/loc", ImmutableMap.of());

    Snapshot inline =
        new InlineSnapshot(
            1L,
            42L,
            null,
            0L,
            "append",
            ImmutableMap.of(),
            0,
            null,
            null,
            null,
            ImmutableList.of());
    TableMetadata withInline = TableMetadata.buildFrom(empty).addSnapshot(inline).build();

    assertThatThrownBy(
            () -> RewriteTablePathUtil.replacePaths(withInline, "s3://bucket/", "s3://other/"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("inline-ML snapshot 42")
        .hasMessageContaining("evict to pointer mode");
  }
}
