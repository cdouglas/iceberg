#!/bin/bash
#
# Run JMH benchmarks in parallel across multiple containers
#
# Partitions by strategy (8 containers), then merges results.
# Includes NoPushdown variants for predicate pushdown ablation study.
# On a large machine, reduces benchmark time significantly.
#
# REENTRANT: Safe to disconnect and reconnect. Running again while
# containers are in progress will wait for the existing run.
#
# Usage:
#   ./run_parallel_benchmark.sh              # Default iterations
#   ./run_parallel_benchmark.sh quick        # Quick test
#   ./run_parallel_benchmark.sh full         # Full suite
#   ./run_parallel_benchmark.sh status       # Check running containers
#   ./run_parallel_benchmark.sh cancel       # Stop running containers
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_NAME="iceberg-jmh-benchmark"
RESULTS_DIR="${SCRIPT_DIR}/results"
RUN_FILE="${SCRIPT_DIR}/.benchmark_run"

# Strategies to benchmark (one container each)
# Includes NoPushdown variants for predicate pushdown ablation study
STRATEGIES=(
    "linearSearch"
    "binarySearch"
    "intervalTree"
    "streamJoin"
    "rangeQuery"
    "smartSelector"
    "streamJoinNoPushdown"
    "rangeQueryNoPushdown"
)

# Container name prefix
CONTAINER_PREFIX="jmh-bench"

get_running_containers() {
    docker ps --filter "name=${CONTAINER_PREFIX}" --format "{{.Names}}" 2>/dev/null | sort
}

get_all_containers() {
    docker ps -a --filter "name=${CONTAINER_PREFIX}" --format "{{.Names}}" 2>/dev/null | sort
}

# Monitor progress using Python script (better terminal handling)
monitor_progress() {
    local timestamp="$1"
    python3 "${SCRIPT_DIR}/progress_monitor.py" "$timestamp"
}

# Quick status check using Python script
show_status() {
    local timestamp="$1"
    python3 "${SCRIPT_DIR}/progress_monitor.py" "$timestamp" --once
}

# Check status
if [ "${1:-}" = "status" ]; then
    running=$(get_running_containers)
    if [ -n "$running" ]; then
        # Get timestamp from run file or extract from container name
        if [ -f "$RUN_FILE" ]; then
            ts=$(cat "$RUN_FILE")
        else
            # Extract from first container name
            first_container=$(echo "$running" | head -1)
            ts="${first_container##*-}"
        fi

        echo "Benchmark in progress (Run ID: $ts)"
        echo ""
        show_status "$ts"
        echo ""
        echo "Follow logs with:"
        echo "  docker logs -f <container_name>"
    else
        echo "No benchmark containers running."
        if [ -f "$RUN_FILE" ]; then
            echo "Last run ID: $(cat "$RUN_FILE")"
        fi
    fi
    exit 0
fi

# Cancel running benchmarks
if [ "${1:-}" = "cancel" ]; then
    running=$(get_running_containers)
    if [ -n "$running" ]; then
        echo "Stopping containers:"
        echo "$running" | while read -r name; do
            echo "  Stopping $name..."
            docker stop "$name" >/dev/null 2>&1 || true
            docker rm "$name" >/dev/null 2>&1 || true
        done
        rm -f "$RUN_FILE"
        echo "Cancelled."
    else
        echo "No benchmark containers running."
    fi
    exit 0
fi

# Check for existing run in progress
running=$(get_running_containers)
if [ -n "$running" ]; then
    echo "=============================================="
    echo "Benchmark already in progress"
    echo "=============================================="
    echo ""
    echo "Running containers:"
    echo "$running" | sed 's/^/  /'
    echo ""

    if [ -f "$RUN_FILE" ]; then
        TIMESTAMP=$(cat "$RUN_FILE")
        echo "Run ID: $TIMESTAMP"
    fi

    echo ""
    echo "Monitoring progress... (Ctrl+C safe - containers keep running)"
    echo ""

    # Monitor progress until completion
    monitor_progress "$TIMESTAMP" || true

    # Continue to merge step with existing timestamp
    if [ -f "$RUN_FILE" ]; then
        TIMESTAMP=$(cat "$RUN_FILE")
    else
        echo "No run file found, cannot merge results."
        exit 1
    fi
else
    # Start new run
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "$TIMESTAMP" > "$RUN_FILE"

    # Parse preset
    case "${1:-default}" in
        quick)
            WI=1; I=2; F=1
            JAVA_OPTS="-Xms2g -Xmx2g -XX:+UseG1GC"
            ;;
        full)
            WI=5; I=10; F=3
            JAVA_OPTS="-Xms4g -Xmx4g -XX:+UseG1GC"
            ;;
        *)
            WI=3; I=5; F=2
            JAVA_OPTS="-Xms2g -Xmx2g -XX:+UseG1GC"
            ;;
    esac

    echo "=============================================="
    echo "Parallel JMH Benchmark Runner"
    echo "=============================================="
    echo "Run ID:       $TIMESTAMP"
    echo "Strategies:   ${#STRATEGIES[@]} containers"
    echo "JMH params:   -wi $WI -i $I -f $F"
    echo "JAVA_OPTS:    $JAVA_OPTS"
    echo "Results dir:  $RESULTS_DIR"
    echo ""

    # Create results directory
    mkdir -p "$RESULTS_DIR"

    # Build image if needed
    if ! docker image inspect "$IMAGE_NAME" &>/dev/null; then
        echo "Building image $IMAGE_NAME..."
        docker build -t "$IMAGE_NAME" -f "$SCRIPT_DIR/Dockerfile" "$SCRIPT_DIR/../.."
    fi

    # Launch containers in parallel
    echo "Launching ${#STRATEGIES[@]} containers..."
    for strategy in "${STRATEGIES[@]}"; do
        container_name="${CONTAINER_PREFIX}-${strategy}-${TIMESTAMP}"
        result_file="/benchmark/results/results-${strategy}-${TIMESTAMP}.json"

        echo "  Starting: $strategy"

        docker run -d \
            --name "$container_name" \
            -v "$RESULTS_DIR:/benchmark/results" \
            -e JAVA_OPTS="$JAVA_OPTS" \
            "$IMAGE_NAME" \
            "RemappingAlgorithmBenchmark.${strategy}" \
            -wi "$WI" -i "$I" -f "$F" \
            -rf json -rff "$result_file" \
            >/dev/null
    done

    echo ""
    echo "Containers launched. Safe to disconnect (Ctrl+C or close terminal)."
    echo "Re-run this script to check status or wait for completion."
    echo ""
    echo "Monitoring progress..."
    echo ""

    # Monitor progress until completion
    monitor_progress "$TIMESTAMP" || true
fi

echo ""

# Wait for all containers to complete (handles Ctrl+C during monitoring)
echo "Waiting for all containers to complete..."
FAILED_CONTAINERS=""
for strategy in "${STRATEGIES[@]}"; do
    container_name="${CONTAINER_PREFIX}-${strategy}-${TIMESTAMP}"
    if docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
        echo "  Waiting for $strategy..."
        exit_code=$(docker wait "$container_name" 2>/dev/null || echo "1")
        if [ "$exit_code" != "0" ]; then
            FAILED_CONTAINERS="$FAILED_CONTAINERS $strategy"
        fi
    fi
done

if [ -n "$FAILED_CONTAINERS" ]; then
    echo "WARNING: Some containers failed:$FAILED_CONTAINERS"
    echo "Check logs with: docker logs jmh-bench-<strategy>-${TIMESTAMP}"
fi

# Merge results
MERGED_FILE="$RESULTS_DIR/results-merged-${TIMESTAMP}.json"
echo ""
echo "Merging results to $MERGED_FILE..."

# Combine JSON arrays using Python for correctness
python3 - "$RESULTS_DIR" "$TIMESTAMP" "$MERGED_FILE" "${STRATEGIES[@]}" << 'PYTHON_MERGE'
import json
import sys
import os

results_dir = sys.argv[1]
timestamp = sys.argv[2]
output_file = sys.argv[3]
strategies = sys.argv[4:]

merged = []
missing = []
for strategy in strategies:
    result_file = os.path.join(results_dir, f"results-{strategy}-{timestamp}.json")
    if os.path.exists(result_file) and os.path.getsize(result_file) > 0:
        try:
            with open(result_file) as f:
                data = json.load(f)
                if isinstance(data, list):
                    merged.extend(data)
                else:
                    merged.append(data)
                print(f"  {strategy}: {len(data) if isinstance(data, list) else 1} results")
        except json.JSONDecodeError as e:
            print(f"  WARNING: Invalid JSON in {result_file}: {e}")
            missing.append(strategy)
    else:
        print(f"  WARNING: Missing or empty {result_file}")
        missing.append(strategy)

if missing:
    print(f"\n  WARNING: {len(missing)} strategy results missing: {', '.join(missing)}")
    print("  (containers may have failed - check docker logs)")

if not merged:
    print("\n  ERROR: No results to merge!")
    sys.exit(1)

# Write merged JSON
with open(output_file, 'w') as f:
    json.dump(merged, f, indent=2)

# Verify the output is valid JSON
try:
    with open(output_file) as f:
        verify = json.load(f)
    if len(verify) != len(merged):
        print(f"  ERROR: Verification failed - expected {len(merged)}, got {len(verify)}")
        sys.exit(1)
    print(f"  Merged {len(merged)} benchmark results (verified)")
except json.JSONDecodeError as e:
    print(f"  ERROR: Output file is not valid JSON: {e}")
    sys.exit(1)
PYTHON_MERGE

if [ $? -ne 0 ]; then
    echo "ERROR: Merge failed. Results may be incomplete."
    echo "Individual result files are preserved in $RESULTS_DIR"
    exit 1
fi

# Cleanup containers
echo "Cleaning up containers..."
for strategy in "${STRATEGIES[@]}"; do
    container_name="${CONTAINER_PREFIX}-${strategy}-${TIMESTAMP}"
    docker rm "$container_name" >/dev/null 2>&1 || true
done

# Clear run file
rm -f "$RUN_FILE"

echo ""
echo "=============================================="
echo "Running Analysis"
echo "=============================================="

# Convert JSON to text format for analyze_results.py (expects JMH text output)
# The merged JSON can be analyzed directly if we update the script, but for now
# generate a CSV from JSON using python

CSV_FILE="$RESULTS_DIR/results-merged-${TIMESTAMP}.csv"
echo "Generating CSV from JSON..."

python3 - "$MERGED_FILE" "$CSV_FILE" << 'PYTHON_SCRIPT'
import json
import sys
import csv

with open(sys.argv[1]) as f:
    data = json.load(f)

with open(sys.argv[2], 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['strategy', 'gap_ratio', 'num_positions', 'num_runs', 'sorted', 'position_coverage', 'avg_time_us', 'error_us'])

    for result in data:
        benchmark = result['benchmark'].split('.')[-1]  # e.g., "linearSearch"
        params = result.get('params', {})
        score = result['primaryMetric']['score']
        error = result['primaryMetric']['scoreError']

        writer.writerow([
            benchmark,
            params.get('gapRatio', ''),
            params.get('numPositions', ''),
            params.get('numRuns', ''),
            params.get('sorted', ''),
            params.get('positionCoverage', '1.0'),
            f"{score:.3f}",
            f"{error:.3f}"
        ])

print(f"Wrote {len(data)} results to {sys.argv[2]}")
PYTHON_SCRIPT

echo ""
echo "Running analysis..."
if [ -f "$SCRIPT_DIR/analyze_results.py" ]; then
    python3 "$SCRIPT_DIR/analyze_results.py" "$MERGED_FILE" 2>/dev/null || echo "  Analysis script failed (non-fatal)"
fi

echo ""
echo "Generating visualizations..."
if [ -f "$SCRIPT_DIR/visualize_results.py" ] && [ -f "$CSV_FILE" ]; then
    cd "$SCRIPT_DIR"
    python3 visualize_results.py "$CSV_FILE" 2>/dev/null || echo "  Visualization failed (non-fatal)"
    cd - >/dev/null
fi

echo ""
echo "=============================================="
echo "Benchmark Complete"
echo "=============================================="
echo ""
echo "Results:"
ls -lh "$RESULTS_DIR"/results-*-${TIMESTAMP}.* 2>/dev/null | sed 's/^/  /'
echo ""
echo "Charts:"
ls -lh "$SCRIPT_DIR"/*.png 2>/dev/null | sed 's/^/  /' || echo "  (none generated)"
