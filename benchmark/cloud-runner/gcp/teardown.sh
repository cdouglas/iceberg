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

# GCP infrastructure teardown for Iceberg benchmarks
# Removes resources created by setup.sh
#
# WARNING: This deletes the GCS bucket (including all data),
# service account, and firewall rules.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

# Load setup configuration if available
SETUP_CONF="$SCRIPT_DIR/setup.conf"
if [[ -f "$SETUP_CONF" ]]; then
    source "$SETUP_CONF"
fi

PROJECT="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null || true)}"
BUCKET_NAME="${GCP_GCS_BUCKET:-}"
SERVICE_ACCOUNT_NAME="iceberg-benchmark"

usage() {
    cat <<EOF
Usage: $0 [options]

Removes all GCP benchmark infrastructure.

WARNING: This deletes the GCS bucket (including all data),
service account, and firewall rules!

Options:
  --project PROJECT      GCP project ID (default: current project)
  --bucket NAME          GCS bucket name
  --yes                  Skip confirmation prompt
  --help                 Show this help

Example:
  $0 --yes
EOF
}

SKIP_CONFIRM=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --project)
            PROJECT="$2"
            shift 2
            ;;
        --bucket)
            BUCKET_NAME="$2"
            shift 2
            ;;
        --yes|-y)
            SKIP_CONFIRM=true
            shift
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

# Show what will be deleted
log_warn "This will DELETE the following GCP resources:"
echo ""
echo "  Project:         $PROJECT"
[[ -n "$BUCKET_NAME" ]] && echo "  GCS Bucket:      $BUCKET_NAME (and all contents!)"
echo "  Service Account: $SERVICE_ACCOUNT_EMAIL"
echo "  Firewall Rule:   iceberg-benchmark-ssh (if exists)"
echo ""

if [[ "$SKIP_CONFIRM" != "true" ]]; then
    read -p "Are you sure you want to delete all these resources? [y/N] " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Aborted."
        exit 0
    fi
fi

# 1. Delete GCS bucket
if [[ -n "$BUCKET_NAME" ]]; then
    log_info "Deleting GCS bucket: $BUCKET_NAME..."
    if gsutil ls -b "gs://$BUCKET_NAME" &>/dev/null; then
        # Delete all objects first
        gsutil -m rm -r "gs://$BUCKET_NAME/**" 2>/dev/null || true
        gsutil rb "gs://$BUCKET_NAME" 2>/dev/null || true
        log_ok "Deleted GCS bucket"
    else
        log_info "Bucket does not exist"
    fi
fi

# 2. Delete service account
log_info "Deleting service account: $SERVICE_ACCOUNT_EMAIL..."
if gcloud iam service-accounts describe "$SERVICE_ACCOUNT_EMAIL" --project="$PROJECT" &>/dev/null; then
    gcloud iam service-accounts delete "$SERVICE_ACCOUNT_EMAIL" \
        --project="$PROJECT" \
        --quiet 2>/dev/null || true
    log_ok "Deleted service account"
else
    log_info "Service account does not exist"
fi

# 3. Delete firewall rule
log_info "Deleting firewall rule..."
if gcloud compute firewall-rules describe iceberg-benchmark-ssh --project="$PROJECT" &>/dev/null 2>&1; then
    gcloud compute firewall-rules delete iceberg-benchmark-ssh \
        --project="$PROJECT" \
        --quiet 2>/dev/null || true
    log_ok "Deleted firewall rule"
else
    log_info "Firewall rule does not exist"
fi

# Clean up local state
rm -f "$SCRIPT_DIR/state/"* 2>/dev/null || true
rm -f "$SETUP_CONF" 2>/dev/null || true

log_ok "Local state cleaned up"
log_ok "GCP teardown complete"
