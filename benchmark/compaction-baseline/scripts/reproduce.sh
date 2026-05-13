#!/usr/bin/env bash
# Replays a single fuzz failure dump from the FuzzMain harness.
#
# Inputs:
#   $1 - path to seed-N.fail.json
#
# What it does:
#   1. Reads the failure record to extract the seed.
#   2. Re-runs FuzzMain with --seed-start <N> --seed-count 1 in a fresh output dir.
#   3. Asserts the same failure (hashes diverge again, or same error).
#
# The warehouse tarball alongside the .fail.json is preserved for post-mortem manual inspection
# (`tar -xf seed-N.warehouse.tar` recreates the state at the moment of divergence). Because
# FuzzScenario is purely seed-driven and the harness is deterministic, the re-run on the same
# seed reproduces the original divergence without consulting the tarball.
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <seed-N.fail.json>" >&2
  exit 1
fi

FAIL_JSON="$1"
if [[ ! -f "$FAIL_JSON" ]]; then
  echo "Not a file: $FAIL_JSON" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Extract the seed from the failure record. Avoid jq dependency by using a small awk regex over
# JSON output that's known to be flat (single-line keys).
SEED="$(awk -F: '/"seed"/ { gsub(/[", ]/, "", $2); print $2; exit }' "$FAIL_JSON")"
if [[ -z "$SEED" ]]; then
  echo "Could not extract 'seed' from $FAIL_JSON" >&2
  exit 1
fi

OUT_DIR="$(mktemp -d -t reproduce-seed-XXXX)"
echo "Reproducing seed $SEED into $OUT_DIR"
"$SCRIPT_DIR/fuzz.sh" --seed-start "$SEED" --seed-count 1 --workers 1 \
  --output "$OUT_DIR" --timeout-seconds 120

if [[ -f "$OUT_DIR/seed-${SEED}.fail.json" ]]; then
  echo "Reproduced: seed $SEED diverged again."
  echo "Original: $FAIL_JSON"
  echo "Replay:   $OUT_DIR/seed-${SEED}.fail.json"
  exit 0
elif [[ -f "$OUT_DIR/seed-${SEED}.ok.json" ]]; then
  echo "Original failure did NOT reproduce: seed $SEED passed on replay." >&2
  echo "Either (a) a code change since the failure was captured now satisfies the property for" >&2
  echo "this seed (expected when a bug fix lands), or (b) the harness has an unseeded source" >&2
  echo "of nondeterminism the determinism guard missed. Check git log between the failure" >&2
  echo "timestamp and now to disambiguate." >&2
  exit 2
else
  echo "Replay produced neither ok.json nor fail.json for seed $SEED — see $OUT_DIR" >&2
  exit 3
fi
