#!/usr/bin/env bash
# Runs the M7 spec-required preflight test suite and captures output to preflight.log.
#
# Per COMPACT_SPEC.md §M7, the bar is "zero failures" — the underlying implementation
# must be in a known-good state before this benchmark module's tests are trusted.
# Flaky tests should be reported in the writeup, not retried-until-green.
#
# Usage:
#   scripts/preflight.sh [output_file]
#
# Default output: ./preflight.log (in the current working directory).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
OUTPUT="${1:-preflight.log}"

cd "$REPO_ROOT"

# Each command runs independently; we want the full picture even if an earlier
# command fails, so we don't bail on first error. The aggregated exit code is
# 1 if any command failed, 0 otherwise.
overall=0
{
  echo "# M7 Pre-flight test suite"
  echo "# Repo: $REPO_ROOT"
  echo "# Run started: $(date --iso-8601=seconds)"
  echo
} > "$OUTPUT"

run_step() {
  local title="$1"
  shift
  echo "============================================================" | tee -a "$OUTPUT"
  echo "## $title" | tee -a "$OUTPUT"
  echo "## cmd: $*" | tee -a "$OUTPUT"
  echo "============================================================" | tee -a "$OUTPUT"
  if "$@" >> "$OUTPUT" 2>&1; then
    echo "## RESULT: PASS" | tee -a "$OUTPUT"
  else
    echo "## RESULT: FAIL (see above)" | tee -a "$OUTPUT"
    overall=1
  fi
  echo | tee -a "$OUTPUT"
}

run_step "iceberg-core: *Compaction*" \
  ./gradlew :iceberg-core:test --tests "*Compaction*"

run_step "iceberg-core: *Remapping*" \
  ./gradlew :iceberg-core:test --tests "*Remapping*"

run_step "iceberg-spark-3.5: *Compaction*" \
  ./gradlew ":iceberg-spark:iceberg-spark-3.5_2.12:test" --tests "*Compaction*"

run_step "iceberg-spark-4.0: *Compaction*" \
  ./gradlew ":iceberg-spark:iceberg-spark-4.0_2.13:test" --tests "*Compaction*"

{
  echo "============================================================"
  if [[ "$overall" -eq 0 ]]; then
    echo "## OVERALL: PASS (zero failures across all four commands)"
  else
    echo "## OVERALL: FAIL (at least one command had failures)"
  fi
  echo "# Run finished: $(date --iso-8601=seconds)"
} | tee -a "$OUTPUT"

exit "$overall"
