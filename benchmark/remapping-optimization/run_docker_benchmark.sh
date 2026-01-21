#!/bin/bash
#
# Run JMH remapping benchmarks in Docker
#
# Usage:
#   ./run_docker_benchmark.sh              # Default: moderate benchmark
#   ./run_docker_benchmark.sh quick        # Quick test (~15 min)
#   ./run_docker_benchmark.sh full         # Full suite (~3 hours)
#   ./run_docker_benchmark.sh selector     # Smart selector only
#   ./run_docker_benchmark.sh [JMH args]   # Custom JMH arguments
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_NAME="iceberg-jmh-benchmark"
RESULTS_DIR="${SCRIPT_DIR}/results"

# Defaults
JAVA_OPTS="${JAVA_OPTS:--Xms2g -Xmx2g -XX:+UseG1GC}"

# Create results directory
mkdir -p "$RESULTS_DIR"

# Build image if it doesn't exist
if ! docker image inspect "$IMAGE_NAME" &>/dev/null; then
    echo "Building image $IMAGE_NAME..."
    docker build -t "$IMAGE_NAME" -f "$SCRIPT_DIR/Dockerfile" "$SCRIPT_DIR/../.."
fi

# Parse preset or use custom args
case "${1:-default}" in
    quick)
        JMH_ARGS="RemappingAlgorithmBenchmark -wi 1 -i 2 -f 1 -rf json -rff /benchmark/results/results-quick.json"
        ;;
    full)
        JAVA_OPTS="-Xms4g -Xmx4g -XX:+UseG1GC"
        JMH_ARGS="RemappingAlgorithmBenchmark -wi 5 -i 10 -f 3 -rf json -rff /benchmark/results/results-full.json"
        ;;
    selector)
        JMH_ARGS="RemappingAlgorithmBenchmark.smartSelector -wi 3 -i 5 -f 2 -rf json -rff /benchmark/results/results-selector.json"
        ;;
    default)
        JMH_ARGS="RemappingAlgorithmBenchmark -wi 3 -i 5 -f 2 -rf json -rff /benchmark/results/results.json"
        ;;
    *)
        # Custom args passed through
        JMH_ARGS="$*"
        ;;
esac

echo "Running benchmark with:"
echo "  JAVA_OPTS: $JAVA_OPTS"
echo "  JMH_ARGS:  $JMH_ARGS"
echo "  Results:   $RESULTS_DIR/"
echo ""

# Run benchmark
docker run --rm \
    -v "$RESULTS_DIR:/benchmark/results" \
    -e JAVA_OPTS="$JAVA_OPTS" \
    "$IMAGE_NAME" \
    $JMH_ARGS

echo ""
echo "Results written to: $RESULTS_DIR/"
ls -la "$RESULTS_DIR/"
