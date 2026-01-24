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

# Run the remapping microbenchmark locally.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
BENCHMARK_DIR="${SCRIPT_DIR}/.."

CONFIG_FILE="${1:-${BENCHMARK_DIR}/configs/default.yaml}"
OUTPUT_DIR="${2:-${BENCHMARK_DIR}/results}"

echo "=== Remapping Microbenchmark ==="
echo "Project root: ${PROJECT_ROOT}"
echo "Config: ${CONFIG_FILE}"
echo "Output: ${OUTPUT_DIR}"
echo ""

# Build the benchmark module
echo "Building benchmark module..."
cd "${PROJECT_ROOT}"
./gradlew :benchmark:remapping-microbenchmark:build -x test

# Create output directory
mkdir -p "${OUTPUT_DIR}"

# Run the benchmark
echo ""
echo "Running benchmark..."
./gradlew :benchmark:remapping-microbenchmark:runBenchmark \
    -PbenchmarkArgs="--config ${CONFIG_FILE} --output-dir ${OUTPUT_DIR}"

echo ""
echo "Benchmark complete. Results in: ${OUTPUT_DIR}"
