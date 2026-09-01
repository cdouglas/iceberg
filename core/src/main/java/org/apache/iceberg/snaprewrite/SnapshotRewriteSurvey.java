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
import java.util.Locale;
import java.util.Set;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;

/**
 * Finds and prices the windows a table could rewrite.
 *
 * <p>A recurring pass needs to choose how far back to reach, and that choice is a trade rather than a
 * rule: reaching further releases older layouts, but every rewritten snapshot deletes every row
 * inserted after it, so a longer window costs more than proportionally. There is no reach that is
 * right for every table, so this prices each one and leaves the choice to the caller.
 *
 * <p>Candidates all target the newest compaction, because that is the only thing they can target: a
 * window rewritten onto an older compaction leaves its snapshots pinning that compaction, and
 * releasing it means one window with an older floor rather than a second window starting there. So
 * the candidates are a family of progressively longer reaches, cheapest first.
 *
 * <p>Pricing runs the full induction per candidate, which reads delete files but writes nothing.
 */
public class SnapshotRewriteSurvey {

  private SnapshotRewriteSurvey() {}

  /**
   * Prices every reach available from the newest compaction, shortest first.
   *
   * @return an empty list if the table has no compaction carrying a map
   */
  public static List<Candidate> survey(Table table, SnapshotRewriteIO rewriteIO) {
    TableMetadata base = ((HasTableOperations) table).operations().current();
    FileIO io = table.io();
    CompactionMapLookup lookup = new CompactionMapLookup(io);

    List<Snapshot> ancestry = ancestry(base);
    Snapshot target = null;
    List<Snapshot> compactions = Lists.newArrayList();
    for (Snapshot snapshot : ancestry) {
      if (lookup.forSnapshot(snapshot) != null) {
        if (target == null) {
          target = snapshot;
        } else {
          compactions.add(snapshot);
        }
      }
    }

    if (target == null) {
      return ImmutableList.of();
    }

    CompactionMap targetMap = lookup.forSnapshot(target);
    Set<String> superseded = sourceFiles(targetMap);

    List<Candidate> candidates = Lists.newArrayList();
    for (Snapshot floor : compactions) {
      candidates.add(price(table, rewriteIO, base, io, target, floor, superseded));
    }

    return candidates;
  }

  private static Candidate price(
      Table table,
      SnapshotRewriteIO rewriteIO,
      TableMetadata base,
      FileIO io,
      Snapshot target,
      Snapshot floor,
      Set<String> superseded) {
    SnapshotRewrite rewrite =
        SnapshotRewrite.forTable(table, rewriteIO)
            .onCompaction(target.snapshotId())
            .floor(floor.snapshotId())
            .maxDeadRatio(Double.MAX_VALUE);

    try {
      SnapshotRewritePlan plan = rewrite.plan();
      return new Candidate(
          target,
          floor,
          plan.window().size(),
          rewrite.estimate(),
          null,
          !referencesAnyOf(plan.window(), io, base, superseded));
    } catch (RewriteRefusedException e) {
      return new Candidate(target, floor, 0, null, e.refusal(), false);
    }
  }

  /**
   * Whether any snapshot in the window still references a file the target compaction replaced.
   *
   * <p>This is the idempotence condition a recurring pass needs, and it is not "has this been
   * rewritten". A snapshot moved onto an older compaction has been rewritten and still needs moving;
   * what matters is whether it is already expressed against the newest one.
   */
  private static boolean referencesAnyOf(
      List<Snapshot> window, FileIO io, TableMetadata base, Set<String> superseded) {
    for (Snapshot snapshot : window) {
      java.util.Map<String, Long> files = Maps.newHashMap();
      SnapshotFiles.collect(snapshot, io, base.specsById(), files);
      for (String path : files.keySet()) {
        if (superseded.contains(path)) {
          return true;
        }
      }
    }

    return false;
  }

  private static Set<String> sourceFiles(CompactionMap map) {
    Set<String> sources = Sets.newHashSet();
    for (CompactionMap.FileMapping mapping : map.fileMappings()) {
      sources.add(mapping.sourceFile());
    }

    return sources;
  }

  private static List<Snapshot> ancestry(TableMetadata base) {
    List<Snapshot> ancestors = Lists.newArrayList();
    if (base.currentSnapshot() == null) {
      return ancestors;
    }

    for (Snapshot snapshot :
        SnapshotUtil.ancestorsOf(base.currentSnapshot().snapshotId(), base::snapshot)) {
      ancestors.add(snapshot);
    }

    return ancestors;
  }

  /** One available reach: how far back it goes, what it would cost, or why it cannot be done. */
  public static class Candidate {
    private final Snapshot compaction;
    private final Snapshot floor;
    private final int snapshotCount;
    private final SnapshotRewriteReport estimate;
    private final RewriteRefusal refusal;
    private final boolean alreadyCurrent;

    Candidate(
        Snapshot compaction,
        Snapshot floor,
        int snapshotCount,
        SnapshotRewriteReport estimate,
        RewriteRefusal refusal,
        boolean alreadyCurrent) {
      this.compaction = compaction;
      this.floor = floor;
      this.snapshotCount = snapshotCount;
      this.estimate = estimate;
      this.refusal = refusal;
      this.alreadyCurrent = alreadyCurrent;
    }

    /** The compaction this window would be rewritten onto. */
    public Snapshot compaction() {
      return compaction;
    }

    /** The oldest snapshot the window would cover. */
    public Snapshot floor() {
      return floor;
    }

    public int snapshotCount() {
      return snapshotCount;
    }

    /** What this reach would cost, or null if it cannot be planned. */
    public SnapshotRewriteReport estimate() {
      return estimate;
    }

    /** Why this reach cannot be planned, or null if it can. */
    public RewriteRefusal refusal() {
      return refusal;
    }

    public boolean plannable() {
      return refusal == null;
    }

    /** True when nothing in this window still references a file the compaction replaced. */
    public boolean alreadyCurrent() {
      return alreadyCurrent;
    }

    @Override
    public String toString() {
      if (!plannable()) {
        return String.format(
            Locale.ROOT, "reach to %s: refused (%s)", floor.snapshotId(), refusal.description());
      }

      return String.format(
          Locale.ROOT,
          "reach to %s: %d snapshots, %s%n%s",
          floor.snapshotId(),
          snapshotCount,
          alreadyCurrent ? "already current" : "movable",
          estimate);
    }
  }
}
