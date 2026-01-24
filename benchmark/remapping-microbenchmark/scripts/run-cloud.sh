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

# Run the remapping microbenchmark on cloud storage.
#
# Usage:
#   ./run-cloud.sh aws [config-file]   # Run on AWS S3
#   ./run-cloud.sh gcp [config-file]   # Run on Google Cloud Storage
#   ./run-cloud.sh azure [config-file] # Run on Azure Blob Storage

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
BENCHMARK_DIR="${SCRIPT_DIR}/.."

CLOUD_PROVIDER="${1:-local}"
CONFIG_FILE="${2:-${BENCHMARK_DIR}/configs/${CLOUD_PROVIDER}.yaml}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "=== Remapping Microbenchmark (Cloud) ==="
echo "Cloud Provider: ${CLOUD_PROVIDER}"
echo "Config: ${CONFIG_FILE}"
echo ""

# Determine storage URI based on cloud provider
case "${CLOUD_PROVIDER}" in
  aws|s3)
    if [ -z "${BENCHMARK_S3_BUCKET}" ]; then
      echo "Error: BENCHMARK_S3_BUCKET environment variable not set"
      echo "Usage: BENCHMARK_S3_BUCKET=my-bucket ./run-cloud.sh aws"
      exit 1
    fi
    STORAGE_URI="s3://${BENCHMARK_S3_BUCKET}/remapping-benchmark/${TIMESTAMP}"
    CLOUD_CONFIG="aws"
    ;;
  gcp|gcs)
    if [ -z "${BENCHMARK_GCS_BUCKET}" ]; then
      echo "Error: BENCHMARK_GCS_BUCKET environment variable not set"
      echo "Usage: BENCHMARK_GCS_BUCKET=my-bucket ./run-cloud.sh gcp"
      exit 1
    fi
    STORAGE_URI="gs://${BENCHMARK_GCS_BUCKET}/remapping-benchmark/${TIMESTAMP}"
    CLOUD_CONFIG="gcp"
    ;;
  azure|blob)
    if [ -z "${BENCHMARK_AZURE_CONTAINER}" ]; then
      echo "Error: BENCHMARK_AZURE_CONTAINER environment variable not set"
      echo "Usage: BENCHMARK_AZURE_CONTAINER=my-container ./run-cloud.sh azure"
      exit 1
    fi
    STORAGE_URI="abfs://${BENCHMARK_AZURE_CONTAINER}@${AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/remapping-benchmark/${TIMESTAMP}"
    CLOUD_CONFIG="azure"
    ;;
  local|*)
    STORAGE_URI="file:///tmp/remapping-benchmark/${TIMESTAMP}"
    CLOUD_CONFIG="local"
    ;;
esac

echo "Storage URI: ${STORAGE_URI}"
echo ""

# Build the benchmark module
echo "Building benchmark module..."
cd "${PROJECT_ROOT}"
./gradlew :benchmark:remapping-microbenchmark:fatJar

# Create local output directory
OUTPUT_DIR="${BENCHMARK_DIR}/results/${CLOUD_PROVIDER}_${TIMESTAMP}"
mkdir -p "${OUTPUT_DIR}"

# Run the benchmark
echo ""
echo "Running benchmark..."
java -Xmx8g \
    -jar "${BENCHMARK_DIR}/build/libs/remapping-microbenchmark-*-all.jar" \
    --config "${CONFIG_FILE}" \
    --storage-uri "${STORAGE_URI}" \
    --output-dir "${OUTPUT_DIR}"

echo ""
echo "Benchmark complete."
echo "Results saved to: ${OUTPUT_DIR}"
echo "Cloud data at: ${STORAGE_URI}"
