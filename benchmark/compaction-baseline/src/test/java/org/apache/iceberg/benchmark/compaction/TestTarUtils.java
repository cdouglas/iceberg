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
package org.apache.iceberg.benchmark.compaction;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Round-trip test for {@link TarUtils}: tar a small directory, untar it, compare contents. */
class TestTarUtils {

  @TempDir Path tempDir;

  @Test
  void tarUntarRoundTripsAllContents() throws IOException {
    Path source = tempDir.resolve("warehouse");
    Files.createDirectories(source.resolve("metadata"));
    Files.createDirectories(source.resolve("data/sub"));
    Files.writeString(source.resolve("metadata/v1.json"), "metadata content");
    Files.writeString(source.resolve("data/file1.parquet"), "parquet bytes one");
    Files.writeString(source.resolve("data/sub/file2.parquet"), "parquet bytes two");

    Path tar = tempDir.resolve("warehouse.tar");
    TarUtils.tarDirectory(source, tar, "warehouse");

    assertThat(tar).exists();
    assertThat(Files.size(tar)).isGreaterThan(0L);

    Path extracted = tempDir.resolve("extracted");
    TarUtils.untar(tar, extracted);

    assertThat(extracted.resolve("warehouse/metadata/v1.json")).exists();
    assertThat(Files.readString(extracted.resolve("warehouse/metadata/v1.json")))
        .isEqualTo("metadata content");
    assertThat(extracted.resolve("warehouse/data/file1.parquet")).exists();
    assertThat(
            Files.readString(
                extracted.resolve("warehouse/data/file1.parquet"), StandardCharsets.UTF_8))
        .isEqualTo("parquet bytes one");
    assertThat(extracted.resolve("warehouse/data/sub/file2.parquet")).exists();
  }

  @Test
  void untarRejectsPathTraversalEntries() throws IOException {
    // Build a tar that contains an entry with "../escape.txt" — TarUtils should refuse to
    // extract it. We construct it via TarArchiveOutputStream directly so the entry name is the
    // exact string we want.
    Path tar = tempDir.resolve("malicious.tar");
    try (java.io.OutputStream out = Files.newOutputStream(tar);
        org.apache.commons.compress.archivers.tar.TarArchiveOutputStream stream =
            new org.apache.commons.compress.archivers.tar.TarArchiveOutputStream(out)) {
      org.apache.commons.compress.archivers.tar.TarArchiveEntry entry =
          new org.apache.commons.compress.archivers.tar.TarArchiveEntry("../escape.txt");
      entry.setSize(4);
      stream.putArchiveEntry(entry);
      stream.write("oops".getBytes(StandardCharsets.UTF_8));
      stream.closeArchiveEntry();
      stream.finish();
    }

    Path target = tempDir.resolve("safe");
    assertThat(catchThrowableIO(() -> TarUtils.untar(tar, target)))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("outside target dir");
  }

  private interface IoBlock {
    void run() throws IOException;
  }

  private static Throwable catchThrowableIO(IoBlock block) {
    try {
      block.run();
      return null;
    } catch (IOException e) {
      return e;
    }
  }
}
