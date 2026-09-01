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
package org.apache.iceberg.data;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.snaprewrite.SnapshotRewriteSurvey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prices the snapshot rewrites available on a table, without writing anything.
 *
 * <p>Reaching further back releases older layouts and costs more than proportionally, and which
 * trade is right depends on the table: how wide its rows are, how much its interstitial
 * transactions insert, how much of it dies between compactions. So this reports what each available
 * reach would cost and leaves the decision to a person.
 *
 * <p>The tool has no mode that mutates a table. {@code --dry-run} is required rather than implied
 * so that the intent is on the command line, and so a future mode that does commit has to be asked
 * for explicitly.
 *
 * <pre>
 * java -cp iceberg-data.jar:iceberg-core.jar:&lt;hadoop&gt; \
 *     org.apache.iceberg.data.SnapshotRewriteDryRun \
 *     --dry-run --table /warehouse/db/table [--reach &lt;snapshotId&gt;]
 * </pre>
 *
 * <p>The report goes through SLF4J at INFO, so a run with logging misconfigured prints nothing
 * rather than looking like a table with no candidates.
 */
public class SnapshotRewriteDryRun {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotRewriteDryRun.class);

  private static final String USAGE =
      "Usage: SnapshotRewriteDryRun --dry-run --table <location> [--reach <snapshotId>]";

  private SnapshotRewriteDryRun() {}

  public static void main(String[] args) {
    String location = null;
    Long reach = null;
    boolean dryRun = false;

    for (int i = 0; i < args.length; i += 1) {
      switch (args[i]) {
        case "--dry-run":
          dryRun = true;
          break;

        case "--table":
          location = value(args, ++i, "--table");
          break;

        case "--reach":
          reach = Long.parseLong(value(args, ++i, "--reach"));
          break;

        default:
          throw new IllegalArgumentException("Unknown argument: " + args[i] + "\n" + USAGE);
      }
    }

    if (!dryRun) {
      throw new IllegalArgumentException(
          "--dry-run is required; this tool does not modify tables\n" + USAGE);
    }

    if (location == null) {
      throw new IllegalArgumentException("--table is required\n" + USAGE);
    }

    Table table = new HadoopTables(new Configuration()).load(location);
    LOG.info("table {}", location);

    List<SnapshotRewriteSurvey.Candidate> candidates =
        SnapshotRewriteSurvey.survey(table, new GenericSnapshotRewriteIO(table));

    if (candidates.isEmpty()) {
      LOG.info("No rewrite available: the table has no compaction carrying a compaction map.");
      return;
    }

    int reported = 0;
    for (SnapshotRewriteSurvey.Candidate candidate : candidates) {
      if (reach != null && candidate.floor().snapshotId() != reach) {
        continue;
      }

      LOG.info("{}{}", System.lineSeparator(), candidate);
      reported += 1;
    }

    if (reported == 0) {
      LOG.info("No candidate reaches back to snapshot {}", reach);
    }
  }

  private static String value(String[] args, int index, String flag) {
    if (index >= args.length) {
      throw new IllegalArgumentException(flag + " requires a value\n" + USAGE);
    }

    return args[index];
  }
}
