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

# Run the compaction cloud benchmark on cloud storage (AWS S3 or GCP GCS)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHMARK_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$(dirname "$BENCHMARK_DIR")")"

# Default values
CLOUD_PROVIDER="${1:-aws}"
CONFIG_FILE="${2:-}"
BUCKET_PREFIX="iceberg-benchmark"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"

# Determine cloud settings
case $CLOUD_PROVIDER in
    aws|s3)
        CLOUD_PROVIDER="aws"
        if [[ -z "$AWS_ACCOUNT_ID" ]]; then
            AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "unknown")
        fi
        TABLE_LOCATION="s3://${BUCKET_PREFIX}-${AWS_ACCOUNT_ID}/benchmark-${TIMESTAMP}/"
        echo "Using AWS S3 storage"
        ;;
    gcp|gs|gcs)
        CLOUD_PROVIDER="gcp"
        if [[ -z "$GCP_PROJECT_ID" ]]; then
            GCP_PROJECT_ID=$(gcloud config get-value project 2>/dev/null || echo "unknown")
        fi
        TABLE_LOCATION="gs://${BUCKET_PREFIX}-${GCP_PROJECT_ID}/benchmark-${TIMESTAMP}/"
        echo "Using Google Cloud Storage"
        ;;
    azure|az|abs)
        CLOUD_PROVIDER="azure"
        STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT:-icebergbenchmark}"
        TABLE_LOCATION="abfs://${BUCKET_PREFIX}@${STORAGE_ACCOUNT}.dfs.core.windows.net/benchmark-${TIMESTAMP}/"
        echo "Using Azure Blob Storage"
        ;;
    local)
        CLOUD_PROVIDER="local"
        TABLE_LOCATION="./benchmark-tables-${TIMESTAMP}/"
        echo "Using local storage"
        ;;
    *)
        echo "Unknown cloud provider: $CLOUD_PROVIDER"
        echo "Usage: $0 [aws|gcp|azure|local] [config-file]"
        exit 1
        ;;
esac

OUTPUT_DIR="results/${CLOUD_PROVIDER}/${TIMESTAMP}"

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "           Compaction Cloud Benchmark"
echo "═══════════════════════════════════════════════════════════════"
echo "Cloud Provider:     $CLOUD_PROVIDER"
echo "Table Location:     $TABLE_LOCATION"
echo "Output Directory:   $OUTPUT_DIR"
echo "Config File:        ${CONFIG_FILE:-default}"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Build if needed
echo "Building benchmark..."
cd "$PROJECT_ROOT"
./gradlew :iceberg-benchmark-compaction-cloud:shadowJar -q 2>/dev/null || {
    echo "Note: Using pre-built classes"
}

# Prepare classpath with cloud dependencies
CLASSPATH="$BENCHMARK_DIR/build/libs/*:$BENCHMARK_DIR/build/classes/java/main"
CLASSPATH="$CLASSPATH:$PROJECT_ROOT/core/build/libs/*"
CLASSPATH="$CLASSPATH:$PROJECT_ROOT/api/build/libs/*"

# Add cloud-specific dependencies
case $CLOUD_PROVIDER in
    aws)
        for jar in ~/.gradle/caches/modules-2/files-2.1/**/aws-*/*.jar; do
            [[ -f "$jar" ]] && CLASSPATH="$CLASSPATH:$jar"
        done
        ;;
    gcp)
        for jar in ~/.gradle/caches/modules-2/files-2.1/**/gcs-*/*.jar; do
            [[ -f "$jar" ]] && CLASSPATH="$CLASSPATH:$jar"
        done
        ;;
esac

# Run benchmark
echo "Starting benchmark..."
START_TIME=$(date +%s)

java -cp "$CLASSPATH" \
    -Xmx8g \
    org.apache.iceberg.benchmark.cloud.CompactionCloudBenchmark \
    --table-location "$TABLE_LOCATION" \
    --output-dir "$OUTPUT_DIR" \
    ${CONFIG_FILE:+--config "$CONFIG_FILE"}

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "           Benchmark Complete"
echo "═══════════════════════════════════════════════════════════════"
echo "Duration:        ${DURATION}s"
echo "Results:         $OUTPUT_DIR"
echo "═══════════════════════════════════════════════════════════════"

# Copy results to cloud if requested
if [[ "$CLOUD_PROVIDER" != "local" && -n "$UPLOAD_RESULTS" ]]; then
    echo ""
    echo "Uploading results to cloud..."
    case $CLOUD_PROVIDER in
        aws)
            aws s3 cp --recursive "$OUTPUT_DIR" "s3://${BUCKET_PREFIX}-${AWS_ACCOUNT_ID}/results/${TIMESTAMP}/"
            ;;
        gcp)
            gsutil -m cp -r "$OUTPUT_DIR/*" "gs://${BUCKET_PREFIX}-${GCP_PROJECT_ID}/results/${TIMESTAMP}/"
            ;;
    esac
    echo "Results uploaded."
fi
