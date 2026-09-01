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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.io.FileIO;

/**
 * Enumerates every file a snapshot references, with its size.
 *
 * <p>Deliberately not {@code addedDataFiles}: a snapshot references files earlier snapshots added,
 * and a partial compaction leaves files in place that it did not write. Counting only added files
 * would report a file as unreachable while a later snapshot still holds it -- which, on the reclaim
 * path, means deleting data the table still needs.
 */
class SnapshotFiles {
  private SnapshotFiles() {}

  static void collect(
      Snapshot snapshot,
      FileIO io,
      Map<Integer, PartitionSpec> specsById,
      Map<String, Long> sizes) {
    sizes.put(
        snapshot.manifestListLocation(),
        io.newInputFile(snapshot.manifestListLocation()).getLength());

    for (ManifestFile manifest : snapshot.dataManifests(io)) {
      sizes.put(manifest.path(), manifest.length());
      try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, io, specsById)) {
        for (DataFile file : reader) {
          sizes.put(file.location(), file.fileSizeInBytes());
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    for (ManifestFile manifest : snapshot.deleteManifests(io)) {
      sizes.put(manifest.path(), manifest.length());
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, io, specsById)) {
        for (DeleteFile file : reader) {
          sizes.put(file.location(), file.fileSizeInBytes());
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
