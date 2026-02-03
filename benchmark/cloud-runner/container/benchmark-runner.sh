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

# =============================================================================
# Iceberg Multi-Cloud Benchmark Runner
# =============================================================================
#
# Orchestrates benchmark runs across AWS, GCP, and Azure from a container.
#
# Usage:
#   ./benchmark-runner.sh check              # Check cloud credentials
#   ./benchmark-runner.sh setup              # Create infrastructure
#   ./benchmark-runner.sh run                # Run benchmarks on all clouds
#   ./benchmark-runner.sh run --clouds aws   # Run on specific cloud(s)
#   ./benchmark-runner.sh monitor            # Monitor running benchmarks
#   ./benchmark-runner.sh collect            # Collect results
#   ./benchmark-runner.sh cleanup            # Tear down infrastructure
#
# =============================================================================

set -euo pipefail

# =============================================================================
# Configuration
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-/output}"
WORKSPACE_DIR="${WORKSPACE_DIR:-/workspace}"
CONFIG_FILE="${CONFIG_FILE:-$OUTPUT_DIR/cloud-config.yaml}"
STATE_DIR="$OUTPUT_DIR/state"
LOG_DIR="$OUTPUT_DIR/logs"
RESULTS_DIR="$OUTPUT_DIR/results"

# Status indicators
OK="✅"
WARN="⚠️"
FAIL="❌"
RUN="🔄"
WAIT="⏳"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Cloud status tracking
declare -A CLOUD_STATUS
declare -A CLOUD_MSG

# =============================================================================
# Logging
# =============================================================================

log_info()  { echo -e "${BLUE}[INFO]${NC} $*"; }
log_ok()    { echo -e "${GREEN}[OK]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
log_header() { echo -e "\n${BOLD}${CYAN}=== $* ===${NC}\n"; }

# =============================================================================
# Credential Initialization
# =============================================================================

init_credentials() {
    # Copy GCP credentials from read-only host mount to writable cache
    if [[ -n "${GCLOUD_HOST_CONFIG:-}" && -d "$GCLOUD_HOST_CONFIG" ]]; then
        local gcloud_config="$HOME/.config/gcloud"
        mkdir -p "$gcloud_config"
        # Copy essential credential files (not logs or cache)
        for f in application_default_credentials.json credentials.db access_tokens.db \
                 properties configurations/config_default active_config; do
            if [[ -e "$GCLOUD_HOST_CONFIG/$f" ]]; then
                mkdir -p "$(dirname "$gcloud_config/$f")"
                cp -f "$GCLOUD_HOST_CONFIG/$f" "$gcloud_config/$f" 2>/dev/null || true
            fi
        done
        # Copy legacy credentials directory if exists
        if [[ -d "$GCLOUD_HOST_CONFIG/legacy_credentials" ]]; then
            cp -rf "$GCLOUD_HOST_CONFIG/legacy_credentials" "$gcloud_config/" 2>/dev/null || true
        fi
    fi

    # Copy Azure credentials from read-only host mount to writable cache
    if [[ -n "${AZURE_HOST_CONFIG:-}" && -d "$AZURE_HOST_CONFIG" ]]; then
        local azure_config="$HOME/.azure"
        mkdir -p "$azure_config"
        # Copy all config files
        cp -rf "$AZURE_HOST_CONFIG"/* "$azure_config/" 2>/dev/null || true
    fi
}

# =============================================================================
# Utility Functions
# =============================================================================

ensure_dirs() {
    mkdir -p "$STATE_DIR" "$LOG_DIR" "$RESULTS_DIR"
}

state_get() {
    local cloud="$1"
    local key="$2"
    local file="$STATE_DIR/${cloud}_${key}"
    [[ -f "$file" ]] && cat "$file" || echo ""
}

state_set() {
    local cloud="$1"
    local key="$2"
    local value="$3"
    echo "$value" > "$STATE_DIR/${cloud}_${key}"
}

state_rm() {
    local cloud="$1"
    local key="$2"
    rm -f "$STATE_DIR/${cloud}_${key}"
}

config_get() {
    local path="$1"
    local default="${2:-}"
    if [[ -f "$CONFIG_FILE" ]]; then
        local val=$(yq e "$path // \"\"" "$CONFIG_FILE" 2>/dev/null || echo "")
        [[ -n "$val" && "$val" != "null" ]] && echo "$val" || echo "$default"
    else
        echo "$default"
    fi
}

config_set() {
    local path="$1"
    local value="$2"
    if [[ ! -f "$CONFIG_FILE" ]]; then
        echo "# Auto-generated cloud configuration" > "$CONFIG_FILE"
    fi
    yq e -i "$path = \"$value\"" "$CONFIG_FILE"
}

timestamp() {
    date +%Y%m%d_%H%M%S
}

# =============================================================================
# Cloud Credential Checks
# =============================================================================

check_aws() {
    local status="$FAIL"
    local msg="Not configured"
    local details=""
    local account=""
    local region=""
    local bucket=""
    local key_name=""

    # Check if AWS CLI can get identity (disable errexit temporarily)
    set +e
    if aws sts get-caller-identity &>/dev/null; then
        account=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "unknown")
        region=$(aws configure get region 2>/dev/null || echo "not set")

        # Check for S3 bucket
        bucket=$(config_get '.aws.s3_bucket' '')
        if [[ -n "$bucket" ]]; then
            if aws s3 ls "s3://$bucket" &>/dev/null; then
                status="$OK"
                msg="Account: $account | Region: $region | Bucket: $bucket"
            else
                status="$WARN"
                msg="Account: $account | Bucket '$bucket' not accessible"
            fi
        else
            status="$WARN"
            msg="Account: $account | No S3 bucket configured"
        fi

        # Check for SSH key
        key_name=$(config_get '.aws.ssh_key_name' '')
        if [[ -n "$key_name" ]]; then
            if aws ec2 describe-key-pairs --key-names "$key_name" --region "${region:-us-west-2}" &>/dev/null; then
                details="SSH key: $key_name"
            else
                if [[ "$status" == "$OK" ]]; then
                    status="$WARN"
                fi
                details="SSH key '$key_name' not found"
            fi
        else
            if [[ "$status" == "$OK" ]]; then
                status="$WARN"
            fi
            details="No SSH key configured"
        fi
    else
        # Check if credentials exist but are invalid
        if [[ -f ~/.aws/credentials ]] || [[ -n "${AWS_ACCESS_KEY_ID:-}" ]]; then
            status="$FAIL"
            msg="Credentials found but invalid or expired"
        fi
    fi
    set -e

    CLOUD_STATUS[aws]="$status"
    CLOUD_MSG[aws]="$msg"
    if [[ -n "$details" ]]; then
        CLOUD_MSG[aws]="$msg | $details"
    fi
}

check_gcp() {
    local status="$FAIL"
    local msg="Not configured"
    local details=""
    local account=""
    local project=""
    local bucket=""

    set +e
    # Check if gcloud is authenticated
    if gcloud auth list --filter="status:ACTIVE" --format="value(account)" 2>/dev/null | grep -q '@'; then
        account=$(gcloud auth list --filter="status:ACTIVE" --format="value(account)" 2>/dev/null | head -1 || echo "")
        project=$(gcloud config get-value project 2>/dev/null || echo "not set")

        if [[ "$project" != "not set" && -n "$project" && "$project" != "(unset)" ]]; then
            # Check for GCS bucket
            bucket=$(config_get '.gcp.gcs_bucket' '')
            if [[ -n "$bucket" ]]; then
                if gsutil ls "gs://$bucket" &>/dev/null; then
                    status="$OK"
                    msg="Project: $project | Bucket: $bucket"
                else
                    status="$WARN"
                    msg="Project: $project | Bucket '$bucket' not accessible"
                fi
            else
                status="$WARN"
                msg="Project: $project | No GCS bucket configured"
            fi
        else
            status="$WARN"
            msg="Account: $account | No project set"
        fi

        # Check compute API
        if gcloud services list --enabled --filter="name:compute.googleapis.com" --format="value(name)" 2>/dev/null | grep -q compute; then
            details="Compute API enabled"
        else
            if [[ "$status" == "$OK" ]]; then
                status="$WARN"
            fi
            details="Compute API not enabled"
        fi
    else
        if [[ -d ~/.config/gcloud ]]; then
            status="$FAIL"
            msg="Config found but not authenticated"
        fi
    fi
    set -e

    CLOUD_STATUS[gcp]="$status"
    CLOUD_MSG[gcp]="$msg"
    if [[ -n "$details" ]]; then
        CLOUD_MSG[gcp]="$msg | $details"
    fi
}

check_azure() {
    local status="$FAIL"
    local msg="Not configured"
    local details=""
    local subscription=""
    local storage_account=""
    local rg=""

    set +e
    # Check if Azure CLI is logged in
    if az account show &>/dev/null; then
        subscription=$(az account show --query name --output tsv 2>/dev/null || echo "unknown")

        # Check for storage account
        storage_account=$(config_get '.azure.storage_account' '')

        if [[ -n "$storage_account" ]]; then
            if az storage account show --name "$storage_account" &>/dev/null; then
                status="$OK"
                msg="Subscription: $subscription | Storage: $storage_account"
            else
                status="$WARN"
                msg="Subscription: $subscription | Storage '$storage_account' not found"
            fi
        else
            status="$WARN"
            msg="Subscription: $subscription | No storage account configured"
        fi

        # Check resource group
        rg=$(config_get '.azure.resource_group' 'iceberg-benchmark-rg')
        if az group show --name "$rg" &>/dev/null; then
            details="Resource group: $rg"
        else
            if [[ "$status" == "$OK" ]]; then
                status="$WARN"
            fi
            details="Resource group '$rg' does not exist"
        fi
    else
        if [[ -d ~/.azure ]]; then
            status="$FAIL"
            msg="Config found but not logged in"
        fi
    fi
    set -e

    CLOUD_STATUS[azure]="$status"
    CLOUD_MSG[azure]="$msg"
    if [[ -n "$details" ]]; then
        CLOUD_MSG[azure]="$msg | $details"
    fi
}

# =============================================================================
# Infrastructure Setup
# =============================================================================

setup_aws() {
    log_header "Setting up AWS Infrastructure"

    local region=$(config_get '.aws.region' 'us-west-2')
    local bucket=$(config_get '.aws.s3_bucket' '')
    local key_name=$(config_get '.aws.ssh_key_name' 'iceberg-benchmark')

    # Create S3 bucket if needed
    if [[ -z "$bucket" ]]; then
        bucket="iceberg-benchmark-$(date +%s)"
        log_info "Creating S3 bucket: $bucket"
        if aws s3 mb "s3://$bucket" --region "$region" 2>&1 | tee -a "$LOG_DIR/aws_setup.log"; then
            config_set '.aws.s3_bucket' "$bucket"
            log_ok "Created bucket: $bucket"
        else
            log_error "Failed to create bucket"
            return 1
        fi
    fi

    # Create SSH key pair if needed
    if ! aws ec2 describe-key-pairs --key-names "$key_name" --region "$region" &>/dev/null; then
        log_info "Creating SSH key pair: $key_name"
        local key_file="$OUTPUT_DIR/ssh/${key_name}.pem"
        mkdir -p "$OUTPUT_DIR/ssh"
        if aws ec2 create-key-pair --key-name "$key_name" --region "$region" \
            --query 'KeyMaterial' --output text > "$key_file" 2>&1; then
            chmod 600 "$key_file"
            config_set '.aws.ssh_key_name' "$key_name"
            config_set '.aws.ssh_key_file' "$key_file"
            log_ok "Created key pair: $key_name (saved to $key_file)"
        else
            log_error "Failed to create key pair"
            return 1
        fi
    fi

    # Create IAM instance profile if needed
    local profile_name="iceberg-benchmark-profile"
    if ! aws iam get-instance-profile --instance-profile-name "$profile_name" &>/dev/null; then
        log_info "Creating IAM instance profile..."

        # Create role
        cat > /tmp/trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "ec2.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF
        aws iam create-role --role-name iceberg-benchmark-role \
            --assume-role-policy-document file:///tmp/trust-policy.json &>/dev/null || true

        aws iam attach-role-policy --role-name iceberg-benchmark-role \
            --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess &>/dev/null || true

        aws iam create-instance-profile --instance-profile-name "$profile_name" &>/dev/null || true

        aws iam add-role-to-instance-profile \
            --instance-profile-name "$profile_name" \
            --role-name iceberg-benchmark-role &>/dev/null || true

        log_ok "Created IAM instance profile"
        sleep 10  # Wait for IAM propagation
    fi

    config_set '.aws.region' "$region"
    config_set '.aws.setup_complete' 'true'
    log_ok "AWS setup complete"
}

setup_gcp() {
    log_header "Setting up GCP Infrastructure"

    set +e  # Don't exit on errors in this function
    local project=$(gcloud config get-value project 2>/dev/null)
    if [[ -z "$project" || "$project" == "(unset)" ]]; then
        log_error "No GCP project set. Run: gcloud config set project YOUR_PROJECT"
        set -e
        return 1
    fi

    local region=$(config_get '.gcp.region' 'us-central1')
    local zone=$(config_get '.gcp.zone' 'us-central1-a')
    local bucket=$(config_get '.gcp.gcs_bucket' '')

    # Enable APIs
    log_info "Enabling required APIs..."
    if ! gcloud services enable compute.googleapis.com storage.googleapis.com \
        --project="$project" 2>&1 | tee -a "$LOG_DIR/gcp_setup.log"; then
        log_error "Failed to enable APIs. Check if your GCP credentials are valid:"
        log_error "  Run: gcloud auth login"
        set -e
        return 1
    fi

    # Create GCS bucket if needed
    if [[ -z "$bucket" ]]; then
        bucket="iceberg-benchmark-$(date +%s)"
        log_info "Creating GCS bucket: $bucket"
        if gsutil mb -l "$region" -p "$project" "gs://$bucket" 2>&1 | tee -a "$LOG_DIR/gcp_setup.log"; then
            config_set '.gcp.gcs_bucket' "$bucket"
            log_ok "Created bucket: $bucket"
        else
            log_error "Failed to create bucket"
            set -e
            return 1
        fi
    fi

    config_set '.gcp.project' "$project"
    config_set '.gcp.region' "$region"
    config_set '.gcp.zone' "$zone"
    config_set '.gcp.setup_complete' 'true'
    set -e
    log_ok "GCP setup complete"
}

setup_azure() {
    log_header "Setting up Azure Infrastructure"

    set +e  # Don't exit on errors in this function
    local location=$(config_get '.azure.location' 'eastus')
    local rg=$(config_get '.azure.resource_group' 'iceberg-benchmark-rg')
    local storage_account=$(config_get '.azure.storage_account' '')
    local container=$(config_get '.azure.storage_container' 'benchmark')

    # Create resource group if needed
    if ! az group show --name "$rg" &>/dev/null; then
        log_info "Creating resource group: $rg"
        if az group create --name "$rg" --location "$location" 2>&1 | tee -a "$LOG_DIR/azure_setup.log"; then
            log_ok "Created resource group: $rg"
        else
            log_error "Failed to create resource group. Check if your Azure credentials are valid:"
            log_error "  Run: az login"
            set -e
            return 1
        fi
    fi

    # Create storage account if needed
    if [[ -z "$storage_account" ]]; then
        storage_account="icebergbench$(date +%s | tail -c 8)"
        log_info "Creating storage account: $storage_account"
        if az storage account create \
            --name "$storage_account" \
            --resource-group "$rg" \
            --location "$location" \
            --sku Standard_LRS \
            --kind StorageV2 \
            --hierarchical-namespace true 2>&1 | tee -a "$LOG_DIR/azure_setup.log"; then
            config_set '.azure.storage_account' "$storage_account"
            log_ok "Created storage account: $storage_account"
        else
            log_error "Failed to create storage account"
            set -e
            return 1
        fi
    fi

    # Create container
    if ! az storage container show --name "$container" --account-name "$storage_account" &>/dev/null; then
        log_info "Creating storage container: $container"
        az storage container create --name "$container" --account-name "$storage_account" \
            2>&1 | tee -a "$LOG_DIR/azure_setup.log" || true
    fi

    config_set '.azure.resource_group' "$rg"
    config_set '.azure.location' "$location"
    config_set '.azure.storage_container' "$container"
    config_set '.azure.setup_complete' 'true'
    set -e
    log_ok "Azure setup complete"
}

# =============================================================================
# Benchmark Execution
# =============================================================================

run_benchmark_aws() {
    local benchmark="$1"
    local config="$2"
    local ts=$(timestamp)

    log_info "Starting AWS benchmark..."
    state_set "aws" "status" "starting"
    state_set "aws" "timestamp" "$ts"

    # Use the existing cloud-runner scripts
    cd "$WORKSPACE_DIR/benchmark/cloud-runner"

    export AWS_S3_BUCKET=$(config_get '.aws.s3_bucket')
    export AWS_SSH_KEY_NAME=$(config_get '.aws.ssh_key_name')
    export AWS_REGION=$(config_get '.aws.region' 'us-west-2')

    # Run in background
    (
        ./aws/run.sh all \
            --benchmark "$benchmark" \
            --config "$config" \
            --keep \
            2>&1 | tee "$LOG_DIR/aws_benchmark_${ts}.log"

        if [[ ${PIPESTATUS[0]} -eq 0 ]]; then
            state_set "aws" "status" "completed"
        else
            state_set "aws" "status" "failed"
        fi
    ) &

    state_set "aws" "pid" "$!"
    log_ok "AWS benchmark started (PID: $!)"
}

run_benchmark_gcp() {
    local benchmark="$1"
    local config="$2"
    local ts=$(timestamp)

    log_info "Starting GCP benchmark..."
    state_set "gcp" "status" "starting"
    state_set "gcp" "timestamp" "$ts"

    cd "$WORKSPACE_DIR/benchmark/cloud-runner"

    export GCP_GCS_BUCKET=$(config_get '.gcp.gcs_bucket')
    export GCP_PROJECT=$(config_get '.gcp.project')
    export GCP_ZONE=$(config_get '.gcp.zone' 'us-central1-a')
    export GCP_NETWORK=$(config_get '.gcp.network' 'default')

    (
        ./gcp/run.sh all \
            --benchmark "$benchmark" \
            --config "$config" \
            --keep \
            2>&1 | tee "$LOG_DIR/gcp_benchmark_${ts}.log"

        if [[ ${PIPESTATUS[0]} -eq 0 ]]; then
            state_set "gcp" "status" "completed"
        else
            state_set "gcp" "status" "failed"
        fi
    ) &

    state_set "gcp" "pid" "$!"
    log_ok "GCP benchmark started (PID: $!)"
}

run_benchmark_azure() {
    local benchmark="$1"
    local config="$2"
    local ts=$(timestamp)

    log_info "Starting Azure benchmark..."
    state_set "azure" "status" "starting"
    state_set "azure" "timestamp" "$ts"

    cd "$WORKSPACE_DIR/benchmark/cloud-runner"

    export AZURE_STORAGE_ACCOUNT=$(config_get '.azure.storage_account')
    export AZURE_STORAGE_CONTAINER=$(config_get '.azure.storage_container')
    export AZURE_RESOURCE_GROUP=$(config_get '.azure.resource_group')
    export AZURE_LOCATION=$(config_get '.azure.location' 'eastus')

    (
        ./azure/run.sh all \
            --benchmark "$benchmark" \
            --config "$config" \
            --keep \
            2>&1 | tee "$LOG_DIR/azure_benchmark_${ts}.log"

        if [[ ${PIPESTATUS[0]} -eq 0 ]]; then
            state_set "azure" "status" "completed"
        else
            state_set "azure" "status" "failed"
        fi
    ) &

    state_set "azure" "pid" "$!"
    log_ok "Azure benchmark started (PID: $!)"
}

# =============================================================================
# Progress Monitoring
# =============================================================================

get_status_icon() {
    local status="$1"
    case "$status" in
        completed) echo "$OK" ;;
        failed)    echo "$FAIL" ;;
        starting|running) echo "$RUN" ;;
        *)         echo "$WAIT" ;;
    esac
}

monitor_progress() {
    local refresh_interval="${1:-10}"

    log_header "Monitoring Benchmark Progress"
    echo "Press Ctrl+C to stop monitoring (benchmarks continue running)"
    echo ""

    while true; do
        clear
        echo -e "${BOLD}Iceberg Multi-Cloud Benchmark Status${NC}"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo ""
        printf "%-10s %-12s %-50s\n" "Cloud" "Status" "Details"
        echo "──────────────────────────────────────────────────────────────────────────────"

        for cloud in aws gcp azure; do
            local status=$(state_get "$cloud" "status")
            local ts=$(state_get "$cloud" "timestamp")
            local icon=$(get_status_icon "$status")

            local details=""
            if [[ -n "$ts" ]]; then
                details="Started: $ts"

                # Get last log line if available
                local logfile="$LOG_DIR/${cloud}_benchmark_${ts}.log"
                if [[ -f "$logfile" ]]; then
                    local last_line=$(tail -1 "$logfile" 2>/dev/null | cut -c1-40)
                    [[ -n "$last_line" ]] && details="$details | $last_line..."
                fi
            fi

            printf "%-10s %s %-10s %-50s\n" "${cloud^^}" "$icon" "${status:-not started}" "$details"
        done

        echo ""
        echo "──────────────────────────────────────────────────────────────────────────────"
        echo "Updated: $(date '+%Y-%m-%d %H:%M:%S')"
        echo ""

        # Check if all completed
        local all_done=true
        local any_running=false
        for cloud in aws gcp azure; do
            local status=$(state_get "$cloud" "status")
            if [[ "$status" == "running" || "$status" == "starting" ]]; then
                any_running=true
                all_done=false
            elif [[ -z "$status" || "$status" == "not_started" ]]; then
                all_done=false
            fi
        done

        if $all_done; then
            echo -e "${GREEN}All benchmarks completed!${NC}"
            break
        fi

        if ! $any_running && ! $all_done; then
            echo -e "${YELLOW}No benchmarks currently running.${NC}"
        fi

        sleep "$refresh_interval"
    done
}

# =============================================================================
# Results Collection
# =============================================================================

collect_results() {
    log_header "Collecting Results"

    for cloud in aws gcp azure; do
        local status=$(state_get "$cloud" "status")
        local ts=$(state_get "$cloud" "timestamp")

        if [[ "$status" == "completed" || "$status" == "failed" ]]; then
            log_info "Collecting $cloud results..."

            local cloud_results="$RESULTS_DIR/${cloud}_${ts}"
            mkdir -p "$cloud_results"

            # Copy logs
            cp "$LOG_DIR/${cloud}_benchmark_${ts}.log" "$cloud_results/" 2>/dev/null || true

            # Get results from workspace if available
            local workspace_results="$WORKSPACE_DIR/benchmark/*/results/${cloud}_*"
            if ls $workspace_results &>/dev/null; then
                cp -r $workspace_results "$cloud_results/" 2>/dev/null || true
            fi

            log_ok "Collected: $cloud_results"
        else
            log_warn "Skipping $cloud (status: ${status:-not started})"
        fi
    done

    # Generate summary
    generate_summary
}

generate_summary() {
    local summary_file="$RESULTS_DIR/summary_$(timestamp).md"

    cat > "$summary_file" << EOF
# Iceberg Multi-Cloud Benchmark Results

Generated: $(date -Iseconds)

## Execution Summary

| Cloud | Status | Started | Log File |
|-------|--------|---------|----------|
EOF

    for cloud in aws gcp azure; do
        local status=$(state_get "$cloud" "status")
        local ts=$(state_get "$cloud" "timestamp")
        local icon=$(get_status_icon "$status")
        echo "| ${cloud^^} | $icon ${status:-not run} | ${ts:-N/A} | ${cloud}_benchmark_${ts}.log |" >> "$summary_file"
    done

    cat >> "$summary_file" << EOF

## Configuration Used

\`\`\`yaml
$(cat "$CONFIG_FILE" 2>/dev/null || echo "No configuration file found")
\`\`\`

## Files Collected

\`\`\`
$(find "$RESULTS_DIR" -type f -name "*.json" -o -name "*.yaml" -o -name "*.log" 2>/dev/null | head -50)
\`\`\`
EOF

    log_ok "Summary written to: $summary_file"
}

# =============================================================================
# Cleanup
# =============================================================================

cleanup_cloud() {
    local cloud="$1"

    log_info "Cleaning up $cloud resources..."

    cd "$WORKSPACE_DIR/benchmark/cloud-runner" 2>/dev/null || return

    case "$cloud" in
        aws)
            export AWS_S3_BUCKET=$(config_get '.aws.s3_bucket' '')
            export AWS_SSH_KEY_NAME=$(config_get '.aws.ssh_key_name' '')
            export AWS_REGION=$(config_get '.aws.region' 'us-west-2')
            if [[ -n "$AWS_S3_BUCKET" ]]; then
                ./aws/run.sh stop 2>&1 | tee -a "$LOG_DIR/aws_cleanup.log" || true
            else
                log_warn "AWS not configured, skipping"
            fi
            ;;
        gcp)
            export GCP_GCS_BUCKET=$(config_get '.gcp.gcs_bucket' '')
            export GCP_PROJECT=$(config_get '.gcp.project' '')
            export GCP_ZONE=$(config_get '.gcp.zone' 'us-central1-a')
            export GCP_NETWORK=$(config_get '.gcp.network' 'default')
            if [[ -n "$GCP_GCS_BUCKET" ]]; then
                ./gcp/run.sh stop 2>&1 | tee -a "$LOG_DIR/gcp_cleanup.log" || true
            else
                log_warn "GCP not configured, skipping"
            fi
            ;;
        azure)
            export AZURE_STORAGE_ACCOUNT=$(config_get '.azure.storage_account' '')
            export AZURE_STORAGE_CONTAINER=$(config_get '.azure.storage_container' 'benchmark')
            export AZURE_RESOURCE_GROUP=$(config_get '.azure.resource_group' 'iceberg-benchmark-rg')
            export AZURE_LOCATION=$(config_get '.azure.location' 'eastus')
            if [[ -n "$AZURE_STORAGE_ACCOUNT" ]]; then
                ./azure/run.sh stop 2>&1 | tee -a "$LOG_DIR/azure_cleanup.log" || true
            else
                log_warn "Azure not configured, skipping"
            fi
            ;;
    esac

    state_rm "$cloud" "status"
    state_rm "$cloud" "pid"
    state_rm "$cloud" "timestamp"

    log_ok "$cloud cleanup complete"
}

# =============================================================================
# Main Commands
# =============================================================================

cmd_check() {
    log_header "Cloud Credential Status"

    # Run checks
    check_aws
    check_gcp
    check_azure

    # Display results
    printf "\n%-10s %-4s %s\n" "Cloud" "Status" "Details"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    for cloud in aws gcp azure; do
        printf "%-10s %s  %s\n" "${cloud^^}" "${CLOUD_STATUS[$cloud]}" "${CLOUD_MSG[$cloud]}"
    done

    echo ""

    # Summary
    local ok_count=0
    local warn_count=0
    local fail_count=0

    for cloud in aws gcp azure; do
        case "${CLOUD_STATUS[$cloud]:-}" in
            "$OK")   ok_count=$((ok_count + 1)) ;;
            "$WARN") warn_count=$((warn_count + 1)) ;;
            "$FAIL") fail_count=$((fail_count + 1)) ;;
        esac
    done

    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Summary: $OK $ok_count ready | $WARN $warn_count warnings | $FAIL $fail_count unable to run"

    if [[ $warn_count -gt 0 || $fail_count -gt 0 ]]; then
        echo ""
        echo "Run './benchmark-runner.sh setup' to create missing infrastructure"
        echo "Or edit $CONFIG_FILE to configure existing resources"
    fi
}

cmd_setup() {
    ensure_dirs

    local clouds="${1:-aws gcp azure}"
    local setup_errors=0

    for cloud in $clouds; do
        case "$cloud" in
            aws)
                check_aws
                if [[ "${CLOUD_STATUS[aws]}" != "$OK" ]]; then
                    setup_aws || { log_error "AWS setup failed"; setup_errors=$((setup_errors + 1)); }
                fi
                ;;
            gcp)
                check_gcp
                if [[ "${CLOUD_STATUS[gcp]}" != "$OK" ]]; then
                    setup_gcp || { log_error "GCP setup failed"; setup_errors=$((setup_errors + 1)); }
                fi
                ;;
            azure)
                check_azure
                if [[ "${CLOUD_STATUS[azure]}" != "$OK" ]]; then
                    setup_azure || { log_error "Azure setup failed"; setup_errors=$((setup_errors + 1)); }
                fi
                ;;
        esac
    done

    log_header "Setup Complete"
    cmd_check

    if [[ $setup_errors -gt 0 ]]; then
        log_warn "$setup_errors cloud(s) had setup failures. Check the errors above."
    fi
}

cmd_run() {
    local benchmark="${BENCHMARK:-remapping-microbenchmark}"
    local config="${BENCHMARK_CONFIG:-}"
    local clouds="${CLOUDS:-}"

    # Resolve config path
    if [[ -z "$config" ]]; then
        config="$WORKSPACE_DIR/benchmark/$benchmark/configs/quick.yaml"
    elif [[ "$config" != /* ]]; then
        # Relative path - resolve relative to benchmark directory
        config="$WORKSPACE_DIR/benchmark/$benchmark/$config"
    fi

    ensure_dirs

    # Verify workspace
    if [[ ! -d "$WORKSPACE_DIR/benchmark/cloud-runner" ]]; then
        log_error "Workspace not mounted. Mount the Iceberg repo to $WORKSPACE_DIR"
        log_error "  docker run -v /path/to/iceberg:/workspace ..."
        return 1
    fi

    # Check credentials first
    check_aws
    check_gcp
    check_azure

    log_header "Starting Benchmarks"
    echo "Benchmark: $benchmark"
    echo "Config: $config"
    echo ""

    # Determine which clouds to run
    if [[ -z "$clouds" ]]; then
        clouds=""
        [[ "${CLOUD_STATUS[aws]}" == "$OK" ]] && clouds="$clouds aws"
        [[ "${CLOUD_STATUS[gcp]}" == "$OK" ]] && clouds="$clouds gcp"
        [[ "${CLOUD_STATUS[azure]}" == "$OK" ]] && clouds="$clouds azure"
    fi

    if [[ -z "$clouds" ]]; then
        log_error "No clouds available. Run 'setup' first or check credentials."
        return 1
    fi

    log_info "Running on clouds: $clouds"

    # Track PIDs for waiting
    local -a pids=()

    for cloud in $clouds; do
        case "$cloud" in
            aws)   run_benchmark_aws "$benchmark" "$config" ;;
            gcp)   run_benchmark_gcp "$benchmark" "$config" ;;
            azure) run_benchmark_azure "$benchmark" "$config" ;;
        esac
        # Capture the PID of the background process
        pids+=("$!")
    done

    echo ""
    log_info "All benchmarks started. Waiting for completion..."
    log_info "(The benchmarks run on cloud VMs - this container orchestrates via SSH)"
    echo ""

    # Wait for all background processes to complete
    local failed=0
    for pid in "${pids[@]}"; do
        if ! wait "$pid" 2>/dev/null; then
            failed=$((failed + 1))
        fi
    done

    echo ""
    if [[ $failed -eq 0 ]]; then
        log_ok "All benchmarks completed successfully!"
    else
        log_warn "$failed benchmark(s) failed. Check logs in $LOG_DIR"
    fi

    # Collect results automatically
    collect_results
}

cmd_monitor() {
    monitor_progress "${1:-10}"
}

cmd_collect() {
    ensure_dirs
    collect_results
}

cmd_cleanup() {
    local clouds="${1:-aws gcp azure}"

    log_header "Cleanup"

    for cloud in $clouds; do
        cleanup_cloud "$cloud"
    done

    log_ok "Cleanup complete"
}

cmd_help() {
    cat << 'EOF'
Iceberg Multi-Cloud Benchmark Runner

USAGE:
    benchmark-runner.sh <command> [options]

COMMANDS:
    check               Check cloud credentials and configuration
    setup [clouds]      Create infrastructure (S3, GCS, ADLS buckets, etc.)
    run [options]       Start benchmarks on available clouds
    monitor [interval]  Monitor running benchmarks (default: 10s refresh)
    collect             Collect results from completed benchmarks
    cleanup [clouds]    Terminate VMs and clean up state

OPTIONS:
    --clouds <list>     Comma-separated clouds: aws,gcp,azure (default: all available)
    --benchmark <name>  Benchmark to run: remapping-microbenchmark
    --config <file>     Benchmark configuration YAML file

ENVIRONMENT:
    OUTPUT_DIR          Directory for logs, state, results (default: /output)
    WORKSPACE_DIR       Iceberg repository mount point (default: /workspace)
    CONFIG_FILE         Cloud configuration file (default: $OUTPUT_DIR/cloud-config.yaml)

EXAMPLES:
    # Check what's configured
    ./benchmark-runner.sh check

    # Set up infrastructure for all clouds
    ./benchmark-runner.sh setup

    # Run benchmarks on AWS and GCP only
    ./benchmark-runner.sh run --clouds aws,gcp --benchmark remapping-microbenchmark

    # Monitor progress
    ./benchmark-runner.sh monitor

    # Collect results when done
    ./benchmark-runner.sh collect

    # Clean up VMs
    ./benchmark-runner.sh cleanup

DOCKER USAGE:
    docker build -t iceberg-benchmark-runner .

    docker run -it --rm \
        -v ~/.aws:/root/.aws:ro \
        -v ~/.config/gcloud:/root/.config/gcloud:ro \
        -v ~/.azure:/root/.azure:ro \
        -v $(pwd):/workspace \
        -v $(pwd)/benchmark-output:/output \
        iceberg-benchmark-runner check

EOF
}

# =============================================================================
# Entry Point
# =============================================================================

main() {
    ensure_dirs
    init_credentials

    local cmd="${1:-help}"
    shift || true

    # Parse global options
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --clouds)
                CLOUDS="${2//,/ }"
                shift 2
                ;;
            --benchmark)
                BENCHMARK="$2"
                shift 2
                ;;
            --config)
                BENCHMARK_CONFIG="$2"
                shift 2
                ;;
            --help|-h)
                cmd_help
                exit 0
                ;;
            *)
                break
                ;;
        esac
    done

    case "$cmd" in
        check)    cmd_check ;;
        setup)    cmd_setup "$*" ;;
        run)      cmd_run ;;
        monitor)  cmd_monitor "${1:-}" ;;
        collect)  cmd_collect ;;
        cleanup)  cmd_cleanup "$*" ;;
        help|--help|-h) cmd_help ;;
        *)
            log_error "Unknown command: $cmd"
            cmd_help
            exit 1
            ;;
    esac
}

main "$@"
