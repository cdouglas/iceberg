#!/bin/bash
#
# Run JMH benchmarks in parallel across multiple containers
#
# Partitions by strategy (6 containers), then merges results.
# On a large machine, reduces ~11 hours to ~2 hours.
#
# Usage:
#   ./run_parallel_benchmark.sh              # Default iterations
#   ./run_parallel_benchmark.sh quick        # Quick test
#   ./run_parallel_benchmark.sh full         # Full suite
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_NAME="iceberg-jmh-benchmark"
RESULTS_DIR="${SCRIPT_DIR}/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Strategies to benchmark (one container each)
STRATEGIES=(
    "linearSearch"
    "binarySearch"
    "intervalTree"
    "streamJoin"
    "rangeQuery"
    "smartSelector"
)

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
echo "Timestamp:    $TIMESTAMP"
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
CONTAINER_IDS=()
for strategy in "${STRATEGIES[@]}"; do
    container_name="jmh-${strategy}-${TIMESTAMP}"
    result_file="/benchmark/results/results-${strategy}-${TIMESTAMP}.json"

    echo "  Starting: $strategy -> $result_file"

    cid=$(docker run -d \
        --name "$container_name" \
        -v "$RESULTS_DIR:/benchmark/results" \
        -e JAVA_OPTS="$JAVA_OPTS" \
        "$IMAGE_NAME" \
        "RemappingAlgorithmBenchmark.${strategy}" \
        -wi "$WI" -i "$I" -f "$F" \
        -rf json -rff "$result_file")

    CONTAINER_IDS+=("$cid:$strategy")
done

echo ""
echo "All containers launched. Waiting for completion..."
echo ""

# Wait for all containers and collect exit codes
FAILED=()
for entry in "${CONTAINER_IDS[@]}"; do
    cid="${entry%%:*}"
    strategy="${entry##*:}"

    echo -n "Waiting for $strategy... "
    exit_code=$(docker wait "$cid")

    if [ "$exit_code" -eq 0 ]; then
        echo "done (success)"
    else
        echo "FAILED (exit code $exit_code)"
        FAILED+=("$strategy")
        echo "  Logs:"
        docker logs --tail 20 "$cid" | sed 's/^/    /'
    fi
done

echo ""

# Merge results
MERGED_FILE="$RESULTS_DIR/results-merged-${TIMESTAMP}.json"
echo "Merging results to $MERGED_FILE..."

# Combine JSON arrays: read each file, strip outer [], join with commas
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
    fi
done
echo "]" >> "$MERGED_FILE"

# Cleanup containers
echo "Cleaning up containers..."
for entry in "${CONTAINER_IDS[@]}"; do
    cid="${entry%%:*}"
    docker rm "$cid" >/dev/null 2>&1 || true
done

echo ""
echo "=============================================="
echo "Benchmark Complete"
echo "=============================================="
echo "Results:"
ls -lh "$RESULTS_DIR"/results-*-${TIMESTAMP}.json
echo ""
echo "Merged results: $MERGED_FILE"

if [ ${#FAILED[@]} -gt 0 ]; then
    echo ""
    echo "WARNING: ${#FAILED[@]} strategies failed: ${FAILED[*]}"
    exit 1
fi
