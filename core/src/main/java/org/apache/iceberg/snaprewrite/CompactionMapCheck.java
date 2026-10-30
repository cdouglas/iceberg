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

import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.DataFile;

/**
 * Checks, under v3, that every compaction map a rewrite follows pairs rows with the same {@code
 * _row_id}.
 *
 * <p>The rewrite places rows through the maps and otherwise trusts what they say. The planner
 * refuses a row a map cannot place, or places outside the compaction's files, but a map that is
 * wrong and stays inside them passes: a run off by one, two sources sent to one row, a row the
 * compaction dropped. The rewrite would then mask or expose the wrong rows. Under v3 a compaction
 * keeps each row's id, so a map is right exactly when every run joins equal ids. That also refuses
 * a compaction that gave its rows new ids, which no rewrite onto it could hide.
 *
 * <p>Only runs whose source a window snapshot holds are checked: no other run can place a row the
 * rewrite asks about. A source without a {@code first_row_id}, written before the table moved to
 * v3, has no ids to compare and is passed over.
 */
class CompactionMapCheck {
  private CompactionMapCheck() {}

  /**
   * @param maps the maps followed, by the snapshot that wrote each
   * @param layout every data file the window and the compaction hold, by path
   * @param ids each file's {@code _row_id} by position
   * @throws RewriteRefusedException with {@link RewriteRefusal#MAP_MISMATCH} at the first run that
   *     joins different ids or reaches past the end of a file
   */
  static void verify(
      Map<Long, CompactionMap> maps, Map<String, DataFile> layout, Function<DataFile, long[]> ids) {
    for (Map.Entry<Long, CompactionMap> followed : maps.entrySet()) {
      for (CompactionMap.FileMapping mapping : followed.getValue().fileMappings()) {
        DataFile source = layout.get(mapping.sourceFile());
        if (source == null || source.firstRowId() == null) {
          continue;
        }

        long[] from = ids.apply(source);
        for (CompactionMap.Run run : mapping.runs()) {
          String targetPath = run.targetFile() != null ? run.targetFile() : mapping.targetFile();
          DataFile target = layout.get(targetPath);
          if (target != null) {
            verifyRun(followed.getKey(), run, source, from, target, ids.apply(target));
          }
        }
      }
    }
  }

  private static void verifyRun(
      long snapshotId,
      CompactionMap.Run run,
      DataFile source,
      long[] from,
      DataFile target,
      long[] to) {
    if (run.sourcePosition() + run.length() > from.length
        || run.targetPosition() + run.length() > to.length) {
      throw new RewriteRefusedException(
          RewriteRefusal.MAP_MISMATCH,
          String.format(
              Locale.ROOT,
              "snapshot %s maps %s:%s+%s to %s:%s, past the end of a file",
              snapshotId,
              source.location(),
              run.sourcePosition(),
              run.length(),
              target.location(),
              run.targetPosition()));
    }

    for (int i = 0; i < run.length(); i += 1) {
      int fromPos = (int) run.sourcePosition() + i;
      int toPos = (int) run.targetPosition() + i;
      if (from[fromPos] != to[toPos]) {
        throw new RewriteRefusedException(
            RewriteRefusal.MAP_MISMATCH,
            String.format(
                Locale.ROOT,
                "snapshot %s maps %s:%s (row id %s) to %s:%s (row id %s)",
                snapshotId,
                source.location(),
                fromPos,
                from[fromPos],
                target.location(),
                toPos,
                to[toPos]));
      }
    }
  }
}
