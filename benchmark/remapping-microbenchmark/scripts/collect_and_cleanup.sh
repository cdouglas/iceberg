#!/bin/bash
#
# Collect benchmark results from all clouds and clean up VMs
#
# Usage: ./collect_and_cleanup.sh
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLOUD_RUNNER_DIR="$SCRIPT_DIR/../../cloud-runner"
RESULTS_DIR="$SCRIPT_DIR/../results"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()  { echo -e "${BLUE}[INFO]${NC} $*"; }
log_ok()    { echo -e "${GREEN}[OK]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

collect_gcp() {
    log_info "Collecting GCP results..."
    cd "$CLOUD_RUNNER_DIR"

    export GCP_GCS_BUCKET="${GCP_GCS_BUCKET:-iceberg-benchmark-1769731822}"
    export GCP_PROJECT="${GCP_PROJECT:-lst-consistency}"
    export GCP_ZONE="${GCP_ZONE:-us-central1-a}"
    export SSH_KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/iceberg_benchmark_key}"

    if ./gcp/run.sh results 2>&1; then
        log_ok "GCP results collected"
    else
        log_warn "Failed to collect GCP results (VM may be stopped)"
    fi
}

collect_azure() {
    log_info "Collecting Azure results..."
    cd "$CLOUD_RUNNER_DIR"

    export AZURE_STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT:-icebergbench9731830}"
    export AZURE_STORAGE_CONTAINER="${AZURE_STORAGE_CONTAINER:-benchmark}"
    export SSH_KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/iceberg_benchmark_key}"

    if ./azure/run.sh results 2>&1; then
        log_ok "Azure results collected"
    else
        log_warn "Failed to collect Azure results (VM may be stopped)"
    fi
}

collect_aws() {
    log_info "Collecting AWS results..."
    cd "$CLOUD_RUNNER_DIR"

    export AWS_S3_BUCKET="${AWS_S3_BUCKET:-iceberg-benchmark-1769730994}"
    export AWS_SSH_KEY_NAME="${AWS_SSH_KEY_NAME:-iceberg-benchmark}"
    export AWS_REGION="${AWS_REGION:-us-west-2}"
    export SSH_KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/iceberg-benchmark-aws.pem}"

    if ./aws/run.sh results 2>&1; then
        log_ok "AWS results collected"
    else
        log_warn "Failed to collect AWS results (VM may be stopped)"
    fi
}

cleanup_gcp() {
    log_info "Cleaning up GCP VM..."
    cd "$CLOUD_RUNNER_DIR"

    export GCP_GCS_BUCKET="${GCP_GCS_BUCKET:-iceberg-benchmark-1769731822}"
    export GCP_PROJECT="${GCP_PROJECT:-lst-consistency}"
    export GCP_ZONE="${GCP_ZONE:-us-central1-a}"

    if ./gcp/run.sh stop 2>&1; then
        log_ok "GCP VM terminated"
    else
        log_warn "GCP cleanup may have failed"
    fi
}

cleanup_azure() {
    log_info "Cleaning up Azure VM..."
    cd "$CLOUD_RUNNER_DIR"

    export AZURE_STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT:-icebergbench9731830}"
    export AZURE_STORAGE_CONTAINER="${AZURE_STORAGE_CONTAINER:-benchmark}"

    if ./azure/run.sh stop 2>&1; then
        log_ok "Azure VM terminated"
    else
        log_warn "Azure cleanup may have failed"
    fi
}

cleanup_aws() {
    log_info "Cleaning up AWS VM..."
    cd "$CLOUD_RUNNER_DIR"

    export AWS_S3_BUCKET="${AWS_S3_BUCKET:-iceberg-benchmark-1769730994}"
    export AWS_SSH_KEY_NAME="${AWS_SSH_KEY_NAME:-iceberg-benchmark}"
    export AWS_REGION="${AWS_REGION:-us-west-2}"

    if ./aws/run.sh stop 2>&1; then
        log_ok "AWS VM terminated"
    else
        log_warn "AWS cleanup may have failed"
    fi
}

verify_no_vms() {
    log_info "Verifying no VMs are running..."
    local running=0

    # Check AWS
    local aws_instances=$(aws ec2 describe-instances \
        --region "${AWS_REGION:-us-west-2}" \
        --filters "Name=tag:Name,Values=iceberg-benchmark" "Name=instance-state-name,Values=running,pending" \
        --query 'Reservations[*].Instances[*].InstanceId' \
        --output text 2>/dev/null || echo "")

    if [[ -n "$aws_instances" ]]; then
        log_warn "AWS: Found running instances: $aws_instances"
        running=$((running + 1))
    else
        log_ok "AWS: No running instances"
    fi

    # Check GCP
    local gcp_instances=$(gcloud compute instances list \
        --project="${GCP_PROJECT:-lst-consistency}" \
        --filter="name=iceberg-benchmark AND status=RUNNING" \
        --format="value(name)" 2>/dev/null || echo "")

    if [[ -n "$gcp_instances" ]]; then
        log_warn "GCP: Found running instances: $gcp_instances"
        running=$((running + 1))
    else
        log_ok "GCP: No running instances"
    fi

    # Check Azure
    local azure_vms=$(az vm list \
        --resource-group "${AZURE_RESOURCE_GROUP:-iceberg-benchmark-rg}" \
        --query "[?powerState=='VM running'].name" \
        --output tsv 2>/dev/null || echo "")

    if [[ -n "$azure_vms" ]]; then
        log_warn "Azure: Found running VMs: $azure_vms"
        running=$((running + 1))
    else
        log_ok "Azure: No running VMs"
    fi

    if [[ $running -eq 0 ]]; then
        log_ok "All clouds verified: No VMs running"
        return 0
    else
        log_error "$running cloud(s) still have running VMs"
        return 1
    fi
}

main() {
    echo "========================================"
    echo "  Collect Results and Cleanup VMs"
    echo "========================================"
    echo ""

    # Collect results from all clouds
    log_info "Collecting results from all clouds..."
    collect_aws || true
    collect_gcp || true
    collect_azure || true

    echo ""

    # Cleanup VMs
    log_info "Terminating all VMs..."
    cleanup_aws || true
    cleanup_gcp || true
    cleanup_azure || true

    echo ""

    # Verify cleanup
    log_info "Verifying cleanup..."
    sleep 10  # Wait for termination to propagate
    verify_no_vms || true

    echo ""

    # List results
    log_info "Results collected to: $RESULTS_DIR"
    ls -la "$RESULTS_DIR" 2>/dev/null || log_warn "Results directory not found"

    echo ""
    log_ok "Done!"
}

main "$@"
