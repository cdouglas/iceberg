#!/bin/bash
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

# Run the compaction cloud benchmark locally

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHMARK_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$(dirname "$BENCHMARK_DIR")")"

# Default values
CONFIG_FILE=""
OUTPUT_DIR="benchmark-results/$(date +%Y%m%d_%H%M%S)"
EXTRA_ARGS=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --config <file>      Config file path"
            echo "  --output-dir <dir>   Output directory"
            echo "  --help               Show this help"
            echo ""
            echo "Additional options are passed to the benchmark:"
            echo "  --seed <n>           Random seed"
            echo "  --iterations <n>     Number of iterations"
            echo "  --no-maps            Disable compaction maps"
            exit 0
            ;;
        *)
            EXTRA_ARGS="$EXTRA_ARGS $1"
            shift
            ;;
    esac
done

# Build the project if needed
echo "Building benchmark..."
cd "$PROJECT_ROOT"
./gradlew :iceberg-benchmark-compaction-cloud:shadowJar -q 2>/dev/null || {
    echo "Note: Gradle build not configured. Running from classes..."
}

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Build classpath
CLASSPATH="$BENCHMARK_DIR/build/libs/*:$BENCHMARK_DIR/build/classes/java/main"
CLASSPATH="$CLASSPATH:$PROJECT_ROOT/core/build/libs/*"
CLASSPATH="$CLASSPATH:$PROJECT_ROOT/api/build/libs/*"
CLASSPATH="$CLASSPATH:$PROJECT_ROOT/bundled-guava/build/libs/*"

# Add common dependencies
for dep in avro jackson-core jackson-databind jackson-dataformat-yaml slf4j-api; do
    for jar in ~/.gradle/caches/modules-2/files-2.1/**/$dep-*/*.jar; do
        if [[ -f "$jar" ]]; then
            CLASSPATH="$CLASSPATH:$jar"
        fi
    done
done

# Run the benchmark
echo "Running benchmark..."
echo "Output directory: $OUTPUT_DIR"
echo ""

java -cp "$CLASSPATH" \
    -Xmx4g \
    org.apache.iceberg.benchmark.cloud.CompactionCloudBenchmark \
    --output-dir "$OUTPUT_DIR" \
    ${CONFIG_FILE:+--config "$CONFIG_FILE"} \
    $EXTRA_ARGS

echo ""
echo "Results written to: $OUTPUT_DIR"
