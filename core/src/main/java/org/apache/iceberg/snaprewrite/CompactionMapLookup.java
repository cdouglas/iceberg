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

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.CompactionMaps;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * Finds the compaction map a snapshot wrote.
 *
 * <p>Reading the location off any manifest the snapshot holds is not enough. A manifest carries its
 * compaction map location through later rewrites, and the rewritten copy is stamped with the
 * rewriting snapshot's id, so a snapshot can appear to own a map describing a layout change it had
 * nothing to do with. A compaction with no map would then look like a compaction with the wrong
 * one, and the induction would follow rows into files that snapshot never wrote.
 *
 * <p>The reliable test is semantic: a snapshot's own compaction map targets the data files that
 * snapshot added.
 */
class CompactionMapLookup {
  private final FileIO io;
  private final Map<String, CompactionMap> cache = Maps.newHashMap();

  CompactionMapLookup(FileIO io) {
    this.io = io;
  }

  /** Returns the compaction map this snapshot wrote, or null if it wrote none. */
  CompactionMap forSnapshot(Snapshot snapshot) {
    Set<String> added = Sets.newHashSet();
    for (DataFile file : snapshot.addedDataFiles(io)) {
      added.add(file.location());
    }

    if (added.isEmpty()) {
      return null;
    }

    for (ManifestFile manifest : snapshot.allManifests(io)) {
      String location = manifest.compactionMapLocation();
      if (location == null) {
        continue;
      }

      CompactionMap map =
          cache.computeIfAbsent(location, path -> CompactionMaps.read(io.newInputFile(path)));
      if (targetsAnyOf(map, added)) {
        return map;
      }
    }

    return null;
  }

  private boolean targetsAnyOf(CompactionMap map, Set<String> added) {
    for (CompactionMap.FileMapping mapping : map.fileMappings()) {
      if (added.contains(mapping.targetFile())) {
        return true;
      }

      for (CompactionMap.Run run : mapping.runs()) {
        if (run.targetFile() != null && added.contains(run.targetFile())) {
          return true;
        }
      }
    }

    return false;
  }
}
