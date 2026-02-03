#!/bin/bash
#
# Run JMH benchmarks with hyper-parallelization for large machines
#
# Partitions each strategy by JMH parameters to maximize CPU utilization.
# Designed for machines with 100+ cores and abundant RAM.
#
# Default partitioning:
#   - streamJoin, rangeQuery: 6 partitions each (heavy strategies)
#   - Other strategies: 4 partitions each
#   - Total: 2×6 + 6×4 = 36 containers
#
# REENTRANT: Safe to disconnect and reconnect.
#
# Usage:
#   ./run_hyperparallel_benchmark.sh              # Default iterations
#   ./run_hyperparallel_benchmark.sh quick        # Quick test
#   ./run_hyperparallel_benchmark.sh full         # Full suite
#   ./run_hyperparallel_benchmark.sh status       # Check running containers
#   ./run_hyperparallel_benchmark.sh cancel       # Stop running containers
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_NAME="iceberg-jmh-benchmark"
RESULTS_DIR="${SCRIPT_DIR}/results"
RUN_FILE="${SCRIPT_DIR}/.benchmark_run_hyper"

# Container name prefix
CONTAINER_PREFIX="jmh-hyper"

# Heavy strategies get 6 partitions (split by sorted × numPositions)
HEAVY_STRATEGIES=("streamJoin" "rangeQuery")

# Light strategies get 4 partitions (split by sorted × numPositions grouped)
LIGHT_STRATEGIES=(
    "linearSearch"
    "binarySearch"
    "intervalTree"
    "smartSelector"
    "streamJoinNoPushdown"
    "rangeQueryNoPushdown"
)

# Parameter partitions for 6-way split (heavy strategies)
# Each partition: "-p sorted=X -p numPositions=Y"
HEAVY_PARTITIONS=(
    "-p sorted=true -p numPositions=1000"
    "-p sorted=true -p numPositions=10000"
    "-p sorted=true -p numPositions=100000"
    "-p sorted=false -p numPositions=1000"
    "-p sorted=false -p numPositions=10000"
    "-p sorted=false -p numPositions=100000"
)

# Parameter partitions for 4-way split (light strategies)
# Groups small/medium numPositions together
LIGHT_PARTITIONS=(
    "-p sorted=true -p numPositions=1000,10000"
    "-p sorted=true -p numPositions=100000"
    "-p sorted=false -p numPositions=1000,10000"
    "-p sorted=false -p numPositions=100000"
)

get_running_containers() {
    docker ps --filter "name=${CONTAINER_PREFIX}" --format "{{.Names}}" 2>/dev/null | sort
}

get_all_containers() {
    docker ps -a --filter "name=${CONTAINER_PREFIX}" --format "{{.Names}}" 2>/dev/null | sort
}

count_total_containers() {
    local count=0
    count=$((count + ${#HEAVY_STRATEGIES[@]} * ${#HEAVY_PARTITIONS[@]}))
    count=$((count + ${#LIGHT_STRATEGIES[@]} * ${#LIGHT_PARTITIONS[@]}))
    echo $count
}

# Check status
if [ "${1:-}" = "status" ]; then
    running=$(get_running_containers)
    if [ -n "$running" ]; then
        if [ -f "$RUN_FILE" ]; then
            ts=$(cat "$RUN_FILE")
        else
            first_container=$(echo "$running" | head -1)
            ts="${first_container##*-}"
        fi

        # Use the progress monitor for detailed status
        python3 "${SCRIPT_DIR}/progress_monitor_hyper.py" "$ts" --once
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
    containers=$(get_all_containers)
    if [ -n "$containers" ]; then
        echo "Stopping and removing containers..."
        echo "$containers" | xargs -r docker rm -f 2>/dev/null || true
        rm -f "$RUN_FILE"
        echo "Cancelled."
    else
        echo "No benchmark containers found."
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

    if [ -f "$RUN_FILE" ]; then
        TIMESTAMP=$(cat "$RUN_FILE")
        echo "Run ID: $TIMESTAMP"
    else
        first_container=$(echo "$running" | head -1)
        TIMESTAMP="${first_container##*-}"
    fi

    echo ""
    echo "Monitoring progress... (Ctrl+C safe)"
    echo ""

    # Monitor with the hyper progress monitor
    python3 "${SCRIPT_DIR}/progress_monitor_hyper.py" "$TIMESTAMP" || true

    if [ -f "$RUN_FILE" ]; then
        TIMESTAMP=$(cat "$RUN_FILE")
    else
        echo "No run file found."
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

    total=$(count_total_containers)

    echo "=============================================="
    echo "Hyper-Parallel JMH Benchmark Runner"
    echo "=============================================="
    echo "Run ID:       $TIMESTAMP"
    echo "Total:        $total containers"
    echo "  Heavy (6):  ${HEAVY_STRATEGIES[*]}"
    echo "  Light (4):  ${LIGHT_STRATEGIES[*]}"
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

    # Launch heavy strategy containers (6 partitions each)
    echo "Launching heavy strategy containers (6 each)..."
    for strategy in "${HEAVY_STRATEGIES[@]}"; do
        for i in "${!HEAVY_PARTITIONS[@]}"; do
            partition="${HEAVY_PARTITIONS[$i]}"
            container_name="${CONTAINER_PREFIX}-${strategy}-p${i}-${TIMESTAMP}"
            result_file="/benchmark/results/results-${strategy}-p${i}-${TIMESTAMP}.json"

            echo "  Starting: ${strategy} partition $i"

            # shellcheck disable=SC2086
            docker run -d \
                --name "$container_name" \
                -v "$RESULTS_DIR:/benchmark/results" \
                -e JAVA_OPTS="$JAVA_OPTS" \
                "$IMAGE_NAME" \
                "RemappingAlgorithmBenchmark.${strategy}" \
                $partition \
                -wi "$WI" -i "$I" -f "$F" \
                -rf json -rff "$result_file" \
                >/dev/null
        done
    done

    # Launch light strategy containers (4 partitions each)
    echo "Launching light strategy containers (4 each)..."
    for strategy in "${LIGHT_STRATEGIES[@]}"; do
        for i in "${!LIGHT_PARTITIONS[@]}"; do
            partition="${LIGHT_PARTITIONS[$i]}"
            container_name="${CONTAINER_PREFIX}-${strategy}-p${i}-${TIMESTAMP}"
            result_file="/benchmark/results/results-${strategy}-p${i}-${TIMESTAMP}.json"

            echo "  Starting: ${strategy} partition $i"

            # shellcheck disable=SC2086
            docker run -d \
                --name "$container_name" \
                -v "$RESULTS_DIR:/benchmark/results" \
                -e JAVA_OPTS="$JAVA_OPTS" \
                "$IMAGE_NAME" \
                "RemappingAlgorithmBenchmark.${strategy}" \
                $partition \
                -wi "$WI" -i "$I" -f "$F" \
                -rf json -rff "$result_file" \
                >/dev/null
        done
    done

    echo ""
    echo "$total containers launched. Safe to disconnect (Ctrl+C)."
    echo "Re-run this script to check status or wait for completion."
    echo ""
    echo "Monitoring progress..."
    echo ""

    # Monitor progress with the hyper progress monitor
    python3 "${SCRIPT_DIR}/progress_monitor_hyper.py" "$TIMESTAMP" || true
fi

echo ""
echo "All containers complete. Checking for failures..."

# Check for failures
FAILED=""
for container in $(get_all_containers | grep "$TIMESTAMP"); do
    exit_code=$(docker inspect "$container" --format="{{.State.ExitCode}}" 2>/dev/null || echo "1")
    if [ "$exit_code" != "0" ]; then
        FAILED="$FAILED $container"
    fi
done

if [ -n "$FAILED" ]; then
    echo "WARNING: Some containers failed:"
    echo "$FAILED" | tr ' ' '\n' | grep -v '^$' | sed 's/^/  /'
    echo ""
    echo "Check logs with: docker logs <container_name>"
fi

# Merge results
MERGED_FILE="$RESULTS_DIR/results-merged-${TIMESTAMP}.json"
echo ""
echo "Merging results to $MERGED_FILE..."

python3 - "$RESULTS_DIR" "$TIMESTAMP" "$MERGED_FILE" << 'PYTHON_MERGE'
import json
import sys
import os
import glob

results_dir = sys.argv[1]
timestamp = sys.argv[2]
output_file = sys.argv[3]

# Find all result files for this timestamp
pattern = os.path.join(results_dir, f"results-*-{timestamp}.json")
result_files = glob.glob(pattern)

merged = []
missing = []
strategies_seen = set()

for result_file in sorted(result_files):
    basename = os.path.basename(result_file)
    # Skip the merged file itself
    if 'merged' in basename:
        continue

    if os.path.exists(result_file) and os.path.getsize(result_file) > 0:
        try:
            with open(result_file) as f:
                data = json.load(f)
                if isinstance(data, list):
                    merged.extend(data)
                    count = len(data)
                else:
                    merged.append(data)
                    count = 1

                # Extract strategy name
                strategy = basename.replace(f"-{timestamp}.json", "").replace("results-", "")
                strategies_seen.add(strategy.split("-p")[0])  # Remove partition suffix
                print(f"  {basename}: {count} results")
        except json.JSONDecodeError as e:
            print(f"  WARNING: Invalid JSON in {basename}: {e}")
            missing.append(basename)
    else:
        print(f"  WARNING: Missing or empty {basename}")
        missing.append(basename)

if missing:
    print(f"\n  WARNING: {len(missing)} result files had issues")

if not merged:
    print("\n  ERROR: No results to merge!")
    sys.exit(1)

# Write merged JSON
with open(output_file, 'w') as f:
    json.dump(merged, f, indent=2)

print(f"\n  Merged {len(merged)} benchmark results from {len(strategies_seen)} strategies")
PYTHON_MERGE

if [ $? -ne 0 ]; then
    echo "ERROR: Merge failed."
    exit 1
fi

# Cleanup containers
echo ""
echo "Cleaning up containers..."
get_all_containers | grep "$TIMESTAMP" | xargs -r docker rm 2>/dev/null || true

# Clear run file
rm -f "$RUN_FILE"

# Generate CSV
CSV_FILE="$RESULTS_DIR/results-merged-${TIMESTAMP}.csv"
echo "Generating CSV..."

python3 - "$MERGED_FILE" "$CSV_FILE" << 'PYTHON_CSV'
import json
import sys
import csv

with open(sys.argv[1]) as f:
    data = json.load(f)

with open(sys.argv[2], 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['strategy', 'gap_ratio', 'num_positions', 'num_runs', 'sorted', 'position_coverage', 'avg_time_us', 'error_us'])

    for result in data:
        benchmark = result['benchmark'].split('.')[-1]
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

print(f"  Wrote {len(data)} results to CSV")
PYTHON_CSV

echo ""
echo "Running analysis..."
if [ -f "$SCRIPT_DIR/analyze_results.py" ]; then
    python3 "$SCRIPT_DIR/analyze_results.py" "$MERGED_FILE" 2>/dev/null || echo "  (analysis failed)"
fi

echo ""
echo "=============================================="
echo "Benchmark Complete"
echo "=============================================="
echo ""
echo "Results:"
ls -lh "$RESULTS_DIR"/results-merged-${TIMESTAMP}.* 2>/dev/null | sed 's/^/  /'
echo ""
echo "To generate visualizations:"
echo "  python3 $SCRIPT_DIR/visualize_results.py $CSV_FILE"
