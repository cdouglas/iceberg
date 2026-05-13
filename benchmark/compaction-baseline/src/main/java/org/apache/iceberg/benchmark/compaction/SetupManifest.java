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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Aggregator for {@code setup_manifest.json}, the per-(variant, K) inventory the runner uses to
 * locate each prebuilt warehouse and the implementer uses to verify that setup actually produced
 * what the spec calls for.
 *
 * <p>Each cell entry is a flat map serialized with Jackson — no nested PoJO classes, since the
 * manifest is a one-shot artifact whose only consumer is {@code analyze.py} and humans.
 */
public final class SetupManifest {

  private final List<Map<String, Object>> cells = Lists.newArrayList();
  private final long masterSeed;

  public SetupManifest(long masterSeed) {
    this.masterSeed = masterSeed;
  }

  /**
   * Append one (variant, K) cell. {@code tarPath} is the local tarball; {@code s3Uri} may be {@code
   * null} if upload was skipped.
   */
  public void addCell(BuildResult result, Path tarPath, String s3Uri) {
    Map<String, Object> cell = new LinkedHashMap<>();
    cell.put("variant", result.variant());
    cell.put("k_late_tx_deletes", result.kLateTxDeletes());
    cell.put("seed", result.seed());
    cell.put("snapshot_ids", result.snapshotIds());
    cell.put("data_file_count", result.dataFileCount());
    cell.put("delete_file_count", result.deleteFileCount());
    cell.put("total_live_rows", result.totalLiveRows());
    cell.put("compaction_map_path", result.compactionMapPath());
    cell.put("compaction_map_run_count", result.compactionMapRunCount());
    cell.put("tar_path", tarPath.toAbsolutePath().toString());
    cell.put("tar_size_bytes", tarSizeBytesOrZero(tarPath));
    cell.put("s3_uri", s3Uri);
    cells.add(cell);
  }

  /** Write the manifest to {@code outputPath} as pretty-printed JSON. Overwrites existing file. */
  public void writeJson(Path outputPath) throws IOException {
    Map<String, Object> root = new LinkedHashMap<>();
    root.put("generated_at", Instant.now().toString());
    root.put("master_seed", masterSeed);
    root.put("cells", cells);

    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    Files.createDirectories(outputPath.toAbsolutePath().getParent());
    mapper.writeValue(outputPath.toFile(), root);
  }

  private static long tarSizeBytesOrZero(Path tarPath) {
    try {
      return tarPath != null && Files.exists(tarPath) ? Files.size(tarPath) : 0L;
    } catch (IOException e) {
      return 0L;
    }
  }
}
