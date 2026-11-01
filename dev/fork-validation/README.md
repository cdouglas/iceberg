<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Fork Validation

These tools check this fork against vanilla Apache Iceberg 1.12.0: run the same test suites on both,
then compare them class by class. The fork passes when no test class does worse than it does on
vanilla Iceberg.

| File | What it is |
|------|------------|
| `run-suite.sh` | Runs the suites in a checkout and keeps the JUnit XML the run wrote |
| `junit_summary.py` | Summarizes a run, compares two runs per class, or exports a run's per-class counts as CSV |
| `baseline-vanilla-1.12.0.csv` | Per-class counts of vanilla Iceberg 1.12.0 |
| `fork-test-classes.txt` | The test classes this fork adds, which the baseline cannot have |

## Running the Gate

```bash
dev/fork-validation/run-suite.sh "$PWD" /tmp/gate 3.5,4.0,4.2 "" spark,lineage
python3 -I dev/fork-validation/junit_summary.py dev/fork-validation/baseline-vanilla-1.12.0.csv /tmp/gate
```

`run-suite.sh` takes a checkout, an output directory, the Spark and Flink versions, and the parts to
run: `core` (api, core, data), `spark` (each Spark module, plus the rewrite, expiry, orphan-file and
position-delete procedure suites in the extensions), `lineage` (the extensions' row-lineage suites),
`flink` (Flink maintenance) and `bench` (the fork's benchmark modules). Spark 4.x builds with Scala
2.13 and Flink with 2.12, so Flink runs in a second Gradle invocation. The Spark gate takes about an
hour.

The comparison prints four lists:

- **Classes that got worse**: a class that passed fewer cases, failed more, or skipped more than in
  the base. This is the gate. A fork class that gained cases with version assumptions shows here
  for its new skips; read those before counting them.
- **Classes that passed in base and are missing from new**: classes in modules the run did not
  cover, or that no longer run.
- **Classes only in new**: the fork's own test classes, as in `fork-test-classes.txt`.
- **Failing test cases in new**, by class, method, parameters and message.

Upstream's timing-sensitive tests fail under load, in particular `TestExpireSnapshotsAction` and
`TestRemoveOrphanFilesAction` in the Spark actions. Run the gate with nothing else running, and rerun
a class that failed on its own, three times, before calling it a regression.

## The Baseline

`baseline-vanilla-1.12.0.csv` comes from running `run-suite.sh` on tag `apache-iceberg-1.12.0`
(`212aa894e9`) on 2026-10-31, with Java 17, Spark 3.5, 4.0 and 4.2, Flink 1.20, 2.1, 2.2 and 2.3, and
the parts `core`, `spark`, `lineage` and `flink`. The JUnit XML came to 1.4 GB, so only the per-class
counts are kept:

```bash
python3 -I dev/fork-validation/junit_summary.py --export <vanilla-run-dir> baseline-vanilla-1.12.0.csv
```

Either side of a comparison can be a run directory or such a CSV.

`fork-test-classes.txt` lists the concrete test classes this fork adds to the source tree, so it
includes classes a run skips, such as the cloud-storage integration tests that need credentials. It
was generated from `git diff --name-only --diff-filter=A apache-iceberg-1.12.0 -- '*/src/test/java/*.java'`.

## Last Results

On 2026-11-01, `cmpmap` ran Spark 3.5, 4.0 and 4.2 with `spark,lineage`, and no class did worse than
the baseline. The one failure, in `TestExpireSnapshotsAction`, passed 99 of 99 cases in each of
three reruns on its own.

The `-1.10` branches are on Iceberg 1.10.1 and have no baseline here.
