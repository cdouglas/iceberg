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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.PositionDeleteRemapper;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Locates a row of the original layout in the rewritten layout.
 *
 * <p>This is {@code loc_k} from the design: defined for exactly the rows live in snapshot {@code
 * S_k}, and rebuilt as the rewrite walks backward through the window.
 *
 * <p>Two layers, because rows reach the rewritten layout by two different routes. Rows that
 * survived to the compaction are located by the compaction map, which stores runs rather than
 * individual positions and so costs almost nothing. Rows that died inside the window are absent
 * from the compaction and are located by an explicit overlay entry pointing into the resurrection
 * file that recovered them.
 *
 * <p>The two layers are disjoint by construction: a row is in the compaction map only if it was
 * live at the compaction, and in the overlay only if it was not.
 *
 * <p>Entries are never removed. A row inserted by {@code T_k} cannot be inserted again by an
 * earlier transaction, so an entry that has gone stale is never queried again.
 *
 * <p>Several compactions in one window are handled by applying their maps in order rather than by
 * composing them. {@code CompactionMaps.compose} requires the first map's target snapshot to be the
 * second's source, which holds only for back-to-back compactions; a window normally has
 * transactions between them. Applying in sequence needs no such agreement, since each map either
 * relocates a reference or passes it through untouched.
 */
public class RowLocator {
  private final List<PositionDeleteRemapper> compactionMaps;
  private final Map<String, Map<Long, RowRef>> overlay = Maps.newHashMap();

  RowLocator(List<PositionDeleteRemapper> compactionMaps) {
    this.compactionMaps = ImmutableList.copyOf(compactionMaps);
  }

  /**
   * Returns where a row of the original layout now lives, or null if it cannot be located.
   *
   * <p>A null result means the row neither survived the compaction nor was resurrected, which for a
   * row that must be live is a bug in the induction rather than an expected outcome. Callers treat
   * it as a reason to refuse the rewrite.
   */
  public RowRef locate(String path, long position) {
    Map<Long, RowRef> byPosition = overlay.get(path);
    if (byPosition != null) {
      RowRef resurrected = byPosition.get(position);
      if (resurrected != null) {
        return resurrected;
      }
    }

    PositionDelete<?> current = PositionDelete.create().set(path, position);
    for (PositionDeleteRemapper remapper : compactionMaps) {
      // Null means the map covers this file but not this position: the row was already dead when
      // that compaction ran and no later map can place it.
      current = remapper.remapDeleteOrNull(current);
      if (current == null) {
        return null;
      }
    }

    return new RowRef(current.path().toString(), current.pos());
  }

  /** Records that a row of the original layout was recovered into a resurrection file. */
  void put(String path, long position, RowRef location) {
    overlay.computeIfAbsent(path, ignored -> Maps.newHashMap()).put(position, location);
  }

  int resurrectedCount() {
    return overlay.values().stream().mapToInt(Map::size).sum();
  }
}
