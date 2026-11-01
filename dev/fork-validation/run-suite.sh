#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Runs the fork's regression suite in one checkout and keeps the JUnit XML it wrote.
# usage: run-suite.sh <checkout> <outdir> <sparkVersions> <flinkVersions> [parts]
#   parts: comma list of core,spark,lineage,flink,bench (default: all but bench, which only
#   the fork has)
#   e.g. run-suite.sh ~/work/iceberg-1.12.0-base out/vanilla-1.12.0 3.5,4.0,4.2 1.20,2.1,2.2,2.3
# Spark 4.x needs Scala 2.13 and Flink needs Scala 2.12, so Flink runs in its own Gradle invocation.
# Compare runs with junit_summary.py; see README.md.
set -u
wt=$1 out=$2 spark=$3 flink=$4 parts=${5:-core,spark,lineage,flink}
mkdir -p "$out"
out=$(cd "$out" && pwd)
cd "$wt" || exit 2
has() { [[ ",$parts," == *",$1,"* ]]; }

ext_tests=(
  --tests '*.TestRewriteDataFilesProcedure'
  --tests '*.TestRewriteDataFilesProcedureWithCompactionMaps'
  --tests '*.TestExpireSnapshotsProcedure'
  --tests '*.TestRemoveOrphanFilesProcedure'
  --tests '*.TestRewritePositionDeleteFilesProcedure')
lineage_tests=(
  --tests '*.TestCopyOnWriteWithLineage'
  --tests '*.TestMergeOnReadWithLineage')

scala213=()
has core && scala213+=(:iceberg-api:test :iceberg-core:test :iceberg-data:test)
has bench && scala213+=(:benchmark:remapping-microbenchmark:test :benchmark:compaction-baseline:test)
IFS=, read -ra sv <<< "$spark"
for v in "${sv[@]}"; do
  has spark && scala213+=(":iceberg-spark:iceberg-spark-${v}_2.13:test")
  tests=()
  has spark && tests+=("${ext_tests[@]}")
  has lineage && tests+=("${lineage_tests[@]}")
  ((${#tests[@]})) && scala213+=(":iceberg-spark:iceberg-spark-extensions-${v}_2.13:test" "${tests[@]}")
done
scala212=()
IFS=, read -ra fv <<< "$flink"
for v in "${fv[@]}"; do
  has flink && scala212+=(":iceberg-flink:iceberg-flink-${v}:test" --tests 'org.apache.iceberg.flink.maintenance.*')
done

{
  echo "worktree: $wt"
  echo "commit:   $(git rev-parse HEAD) $(git describe --tags --always)"
  echo "spark:    $spark"
  echo "flink:    $flink"
  echo "parts:    $parts"
  echo "started:  $(date -Is)"
} >> "$out/run.txt"
# Only XML this run writes is kept; a module it doesn't run may hold results from an earlier one
touch "$out/.started"

rc=0
if ((${#scala213[@]})); then
  ./gradlew --continue -DscalaVersion=2.13 -DsparkVersions="$spark" -DflinkVersions="$flink" \
    "${scala213[@]}" >> "$out/gradle.log" 2>&1 || rc=$?
fi
if ((${#scala212[@]})); then
  ./gradlew --continue -DscalaVersion=2.12 -DsparkVersions=3.5 -DflinkVersions="$flink" \
    "${scala212[@]}" >> "$out/gradle-flink.log" 2>&1 || rc=$?
fi
echo "finished: $(date -Is) exit=$rc" >> "$out/run.txt"

# Keep every module's XML under its module path.
find . -path '*/build/test-results/test/*.xml' -newer "$out/.started" -print0 2>/dev/null \
  | xargs -0 -r -I{} cp --parents {} "$out/"
exit $rc
