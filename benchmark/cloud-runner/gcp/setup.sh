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

# GCP infrastructure setup for Iceberg benchmarks
# Run once to create persistent infrastructure (idempotent)
#
# Creates:
#   - GCS bucket for benchmark data
#   - Service account for benchmark VMs
#   - IAM bindings for storage access
#
# After setup, VMs launched with the service account will
# automatically have GCS access.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

# Configuration with defaults
PROJECT="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${GCP_REGION:-us-west1}"
ZONE="${GCP_ZONE:-${REGION}-a}"
# Bucket name must be globally unique
BUCKET_NAME="${GCP_GCS_BUCKET:-iceberg-benchmark-${REGION}-$(date +%s | tail -c 11)}"
SERVICE_ACCOUNT_NAME="iceberg-benchmark"

usage() {
    cat <<EOF
Usage: $0 [options]

One-time infrastructure setup for GCP benchmark environment.
This script is idempotent - safe to run multiple times.

Options:
  --project PROJECT        GCP project ID (default: current gcloud project)
  --region REGION          GCP region (default: $REGION)
  --zone ZONE              GCP zone (default: $ZONE)
  --bucket NAME            GCS bucket name (default: auto-generated)
  --help                   Show this help

Environment variables (override defaults):
  GCP_PROJECT             GCP project ID
  GCP_REGION              GCP region
  GCP_ZONE                GCP zone
  GCP_GCS_BUCKET          GCS bucket name

After setup, export these for run.sh:
  export GCP_GCS_BUCKET=<bucket-name>
  export GCP_PROJECT=<project-id>
  export GCP_ZONE=<zone>

Example:
  $0 --project my-project --region us-central1
EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --project)
            PROJECT="$2"
            shift 2
            ;;
        --region)
            REGION="$2"
            ZONE="${REGION}-a"
            shift 2
            ;;
        --zone)
            ZONE="$2"
            shift 2
            ;;
        --bucket)
            BUCKET_NAME="$2"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

if [[ -z "$PROJECT" ]]; then
    log_error "GCP project not set. Use --project or set GCP_PROJECT"
    exit 1
fi

SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT}.iam.gserviceaccount.com"

log_info "Setting up GCP benchmark infrastructure..."
log_info "  Project:         $PROJECT"
log_info "  Region:          $REGION"
log_info "  Zone:            $ZONE"
log_info "  GCS Bucket:      $BUCKET_NAME"
log_info "  Service Account: $SERVICE_ACCOUNT_EMAIL"
echo ""

# 1. Enable required APIs
log_info "Enabling required APIs..."
gcloud services enable compute.googleapis.com --project="$PROJECT" --quiet 2>/dev/null || true
gcloud services enable storage.googleapis.com --project="$PROJECT" --quiet 2>/dev/null || true
log_ok "APIs enabled"

# 2. Create GCS bucket
log_info "Creating GCS bucket..."
if gsutil ls -b "gs://$BUCKET_NAME" &>/dev/null; then
    log_ok "Bucket already exists: $BUCKET_NAME"
else
    gsutil mb -p "$PROJECT" -l "$REGION" "gs://$BUCKET_NAME"
    log_ok "Created bucket: $BUCKET_NAME"
fi

# 3. Create service account for benchmark VMs
log_info "Creating service account..."
if gcloud iam service-accounts describe "$SERVICE_ACCOUNT_EMAIL" --project="$PROJECT" &>/dev/null; then
    log_ok "Service account already exists: $SERVICE_ACCOUNT_EMAIL"
else
    gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
        --project="$PROJECT" \
        --display-name="Iceberg Benchmark VM Service Account" \
        --description="Service account for Iceberg benchmark VMs"
    log_ok "Created service account: $SERVICE_ACCOUNT_EMAIL"
fi

# 4. Grant storage access to service account
log_info "Configuring storage IAM bindings..."

# Grant object admin on bucket
if gsutil iam get "gs://$BUCKET_NAME" | grep -q "$SERVICE_ACCOUNT_EMAIL"; then
    log_ok "Storage IAM binding already exists"
else
    gsutil iam ch "serviceAccount:${SERVICE_ACCOUNT_EMAIL}:objectAdmin" "gs://$BUCKET_NAME"
    log_ok "Granted objectAdmin on bucket"
fi

# 5. Grant compute instance admin to allow VM creation
# (The user running run.sh needs this, not the service account)
log_info "Verifying compute permissions..."
# This just checks - the user needs to have these permissions already
if gcloud compute instances list --project="$PROJECT" --limit=1 &>/dev/null; then
    log_ok "Compute access verified"
else
    log_warn "May need compute.instances.create permission to run benchmarks"
fi

# 6. Set up default network if needed
log_info "Checking network configuration..."
if gcloud compute networks describe default --project="$PROJECT" &>/dev/null; then
    log_ok "Default network exists"
    NETWORK="default"
else
    # Check for any VPC network
    NETWORK=$(gcloud compute networks list --project="$PROJECT" --format="value(name)" --limit=1)
    if [[ -n "$NETWORK" ]]; then
        log_ok "Found network: $NETWORK"
    else
        log_warn "No VPC network found. Create one or VMs will fail."
    fi
fi

# 7. Ensure firewall allows SSH
log_info "Checking firewall rules..."
if gcloud compute firewall-rules describe allow-ssh --project="$PROJECT" &>/dev/null 2>&1 || \
   gcloud compute firewall-rules describe default-allow-ssh --project="$PROJECT" &>/dev/null 2>&1; then
    log_ok "SSH firewall rule exists"
else
    log_info "Creating SSH firewall rule..."
    gcloud compute firewall-rules create iceberg-benchmark-ssh \
        --project="$PROJECT" \
        --network="${NETWORK:-default}" \
        --allow=tcp:22 \
        --source-ranges=0.0.0.0/0 \
        --description="Allow SSH for Iceberg benchmarks" \
        2>/dev/null || log_warn "Could not create firewall rule (may need permissions)"
fi

# 8. Save configuration for run.sh
CONFIG_FILE="$SCRIPT_DIR/setup.conf"
cat > "$CONFIG_FILE" <<EOF
# Generated by setup.sh on $(date -Iseconds)
# Source this file or export these variables before running run.sh

export GCP_PROJECT="$PROJECT"
export GCP_REGION="$REGION"
export GCP_ZONE="$ZONE"
export GCP_GCS_BUCKET="$BUCKET_NAME"
export GCP_NETWORK="${NETWORK:-default}"

# Storage URI for benchmark configs
# Use this format: gs://$BUCKET_NAME/benchmark
EOF

log_ok "Configuration saved to: $CONFIG_FILE"

echo ""
echo "=============================================="
log_ok "GCP setup complete!"
echo "=============================================="
echo ""
echo "Before running benchmarks, export these variables:"
echo ""
echo "  source $CONFIG_FILE"
echo ""
echo "Or manually:"
echo ""
echo "  export GCP_GCS_BUCKET=\"$BUCKET_NAME\""
echo "  export GCP_PROJECT=\"$PROJECT\""
echo "  export GCP_ZONE=\"$ZONE\""
echo ""
echo "Storage URI for benchmark configs:"
echo "  gs://$BUCKET_NAME/benchmark"
echo ""
echo "To run benchmarks:"
echo "  cd $SCRIPT_DIR && ./run.sh start"
echo ""
