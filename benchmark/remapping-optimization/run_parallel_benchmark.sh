#!/bin/bash
#
# Run JMH benchmarks in parallel across multiple containers
#
# Partitions by strategy (6 containers), then merges results.
# On a large machine, reduces ~11 hours to ~2 hours.
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
STRATEGIES=(
    "linearSearch"
    "binarySearch"
    "intervalTree"
    "streamJoin"
    "rangeQuery"
    "smartSelector"
)

# Container name prefix
CONTAINER_PREFIX="jmh-bench"

get_running_containers() {
    docker ps --filter "name=${CONTAINER_PREFIX}" --format "{{.Names}}" 2>/dev/null | sort
}

get_all_containers() {
    docker ps -a --filter "name=${CONTAINER_PREFIX}" --format "{{.Names}}" 2>/dev/null | sort
}

# Get progress percentage from container logs
# Returns: integer 0-100, or "done" if container exited successfully, or "fail" if failed
get_container_progress() {
    local container_name="$1"

    # Check if container is still running
    if ! docker ps --format "{{.Names}}" | grep -q "^${container_name}$"; then
        # Container stopped - check exit code
        local exit_code
        exit_code=$(docker inspect "$container_name" --format='{{.State.ExitCode}}' 2>/dev/null || echo "255")
        if [ "$exit_code" -eq 0 ]; then
            echo "done"
        else
            echo "fail:$exit_code"
        fi
        return
    fi

    # Extract latest progress from logs
    # JMH outputs: "# Run progress: 50.00% complete, ETA 00:01:23"
    local progress
    progress=$(docker logs --tail 100 "$container_name" 2>&1 | \
        grep -o "Run progress: [0-9.]*%" | \
        tail -1 | \
        grep -o "[0-9.]*" || echo "0")

    # Return integer percentage
    printf "%.0f" "$progress" 2>/dev/null || echo "0"
}

# Draw a progress bar
# Args: current_percent total_width
draw_progress_bar() {
    local percent=$1
    local width=${2:-30}
    local filled=$((percent * width / 100))
    local empty=$((width - filled))

    printf "["
    printf "%${filled}s" | tr ' ' '█'
    printf "%${empty}s" | tr ' ' '░'
    printf "]"
}

# Monitor progress of all containers until completion
# Args: container_names (space-separated)
monitor_progress() {
    local timestamp="$1"
    shift
    local strategies=("$@")
    local total=${#strategies[@]}
    local completed=0
    local failed=0
    declare -A progress_map
    declare -A status_map

    # Initialize
    for strategy in "${strategies[@]}"; do
        progress_map[$strategy]=0
        status_map[$strategy]="running"
    done

    # Hide cursor
    tput civis 2>/dev/null || true

    # Trap to restore cursor on exit
    trap 'tput cnorm 2>/dev/null || true' EXIT

    while [ $completed -lt $total ]; do
        # Update progress for each container
        local total_progress=0
        completed=0
        failed=0

        for strategy in "${strategies[@]}"; do
            local container_name="${CONTAINER_PREFIX}-${strategy}-${timestamp}"
            local prog
            prog=$(get_container_progress "$container_name")

            if [ "$prog" = "done" ]; then
                progress_map[$strategy]=100
                status_map[$strategy]="done"
                ((completed++))
            elif [[ "$prog" == fail:* ]]; then
                progress_map[$strategy]=100
                status_map[$strategy]="$prog"
                ((completed++))
                ((failed++))
            else
                progress_map[$strategy]=$prog
                status_map[$strategy]="running"
            fi

            total_progress=$((total_progress + progress_map[$strategy]))
        done

        # Calculate overall progress
        local overall=$((total_progress / total))

        # Move cursor up to overwrite previous output (7 lines: header + 6 strategies)
        if [ -n "${PROGRESS_DISPLAYED:-}" ]; then
            tput cuu 8 2>/dev/null || printf "\033[8A"
        fi
        PROGRESS_DISPLAYED=1

        # Clear and redraw
        tput el 2>/dev/null || true
        printf "Progress: "
        draw_progress_bar $overall
        printf " %3d%% (%d/%d containers complete)\n" "$overall" "$completed" "$total"

        tput el 2>/dev/null || true
        echo ""

        for strategy in "${strategies[@]}"; do
            tput el 2>/dev/null || true
            local status="${status_map[$strategy]}"
            local prog="${progress_map[$strategy]}"

            if [ "$status" = "done" ]; then
                printf "  %-14s ✓ done\n" "$strategy:"
            elif [[ "$status" == fail:* ]]; then
                local exit_code="${status#fail:}"
                printf "  %-14s ✗ failed (exit %s)\n" "$strategy:" "$exit_code"
            else
                printf "  %-14s %3d%%\n" "$strategy:" "$prog"
            fi
        done

        # Exit if all done
        [ $completed -ge $total ] && break

        # Wait before next poll
        sleep 3
    done

    # Show cursor
    tput cnorm 2>/dev/null || true
    trap - EXIT

    echo ""
    if [ $failed -gt 0 ]; then
        echo "WARNING: $failed container(s) failed"
        for strategy in "${strategies[@]}"; do
            local status="${status_map[$strategy]}"
            if [[ "$status" == fail:* ]]; then
                local container_name="${CONTAINER_PREFIX}-${strategy}-${timestamp}"
                echo "  $strategy last log lines:"
                docker logs --tail 10 "$container_name" 2>&1 | sed 's/^/    /'
            fi
        done
        return 1
    fi
    return 0
}

# Check status
if [ "${1:-}" = "status" ]; then
    running=$(get_running_containers)
    if [ -n "$running" ]; then
        echo "Running benchmark containers:"
        echo ""

        # Get timestamp from run file or extract from container name
        if [ -f "$RUN_FILE" ]; then
            ts=$(cat "$RUN_FILE")
        else
            # Extract from first container name
            first_container=$(echo "$running" | head -1)
            ts="${first_container##*-}"
        fi

        # Show progress for each strategy
        total_progress=0
        count=0
        for strategy in "${STRATEGIES[@]}"; do
            container_name="${CONTAINER_PREFIX}-${strategy}-${ts}"
            if docker ps --format "{{.Names}}" | grep -q "^${container_name}$"; then
                prog=$(get_container_progress "$container_name")
                printf "  %-14s %3d%%\n" "$strategy:" "$prog"
                total_progress=$((total_progress + prog))
                ((count++))
            fi
        done

        if [ $count -gt 0 ]; then
            overall=$((total_progress / count))
            echo ""
            printf "Overall: "
            draw_progress_bar $overall
            printf " %d%%\n" "$overall"
        fi

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
    monitor_progress "$TIMESTAMP" "${STRATEGIES[@]}" || true

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
    monitor_progress "$TIMESTAMP" "${STRATEGIES[@]}" || true
fi

echo ""

# Merge results
MERGED_FILE="$RESULTS_DIR/results-merged-${TIMESTAMP}.json"
echo "Merging results to $MERGED_FILE..."

# Combine JSON arrays
echo "[" > "$MERGED_FILE"
first=true
for strategy in "${STRATEGIES[@]}"; do
    result_file="$RESULTS_DIR/results-${strategy}-${TIMESTAMP}.json"
    if [ -f "$result_file" ] && [ -s "$result_file" ]; then
        if [ "$first" = true ]; then
            first=false
        else
            echo "," >> "$MERGED_FILE"
        fi
        # Strip leading [ and trailing ], append contents
        sed '1s/^\[//; $s/\]$//' "$result_file" >> "$MERGED_FILE"
    else
        echo "  Warning: Missing or empty $result_file"
    fi
done
echo "]" >> "$MERGED_FILE"

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
    writer.writerow(['strategy', 'gap_ratio', 'num_positions', 'num_runs', 'sorted', 'avg_time_us', 'error_us'])

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
