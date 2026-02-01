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
# Iceberg Multi-Cloud Benchmark Runner - Container Wrapper
# =============================================================================
#
# This script builds the container (if needed) and runs the benchmark-runner
# with proper bind mounts for credentials and output.
#
# Usage:
#   ./run.sh check           # Check cloud credentials
#   ./run.sh setup           # Create infrastructure
#   ./run.sh run             # Run benchmarks
#   ./run.sh monitor         # Monitor progress
#   ./run.sh collect         # Collect results
#   ./run.sh cleanup         # Clean up resources
#   ./run.sh shell           # Interactive shell in container
#
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Container configuration
IMAGE_NAME="iceberg-benchmark-runner"
IMAGE_TAG="latest"
FULL_IMAGE="$IMAGE_NAME:$IMAGE_TAG"

# Output directory (relative to this script)
OUTPUT_DIR="${OUTPUT_DIR:-$SCRIPT_DIR/output}"

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

# =============================================================================
# Container Management
# =============================================================================

build_container() {
    local force="${1:-false}"

    # Check if image exists
    if docker image inspect "$FULL_IMAGE" &>/dev/null && [[ "$force" != "true" ]]; then
        log_info "Container image exists: $FULL_IMAGE"
        return 0
    fi

    log_info "Building container image: $FULL_IMAGE"
    docker build -t "$FULL_IMAGE" -f "$SCRIPT_DIR/Dockerfile" "$SCRIPT_DIR"
    log_ok "Container built successfully"
}

ensure_output_dir() {
    mkdir -p "$OUTPUT_DIR"
    mkdir -p "$OUTPUT_DIR/state"
    mkdir -p "$OUTPUT_DIR/logs"
    mkdir -p "$OUTPUT_DIR/results"
    mkdir -p "$OUTPUT_DIR/ssh"
}

# =============================================================================
# Credential Mount Detection
# =============================================================================

log_credential_status() {
    if [[ -d "$HOME/.aws" ]]; then
        log_info "Mounting AWS credentials from ~/.aws"
    else
        log_warn "No AWS credentials found at ~/.aws"
    fi

    if [[ -d "$HOME/.config/gcloud" ]]; then
        log_info "Mounting GCP credentials from ~/.config/gcloud"
    else
        log_warn "No GCP credentials found at ~/.config/gcloud"
    fi

    if [[ -d "$HOME/.azure" ]]; then
        log_info "Mounting Azure credentials from ~/.azure"
    else
        log_warn "No Azure credentials found at ~/.azure"
    fi
}

# =============================================================================
# Run Container
# =============================================================================

run_container() {
    local interactive="${INTERACTIVE:-true}"
    local cmd="${1:-check}"
    shift || true

    ensure_output_dir
    log_credential_status

    # Build docker args array
    local -a docker_args=("run" "--rm")

    # Add TTY if available
    if [[ -t 0 && "$interactive" == "true" ]]; then
        docker_args+=("-it")
    fi

    # Add credential mounts
    # AWS: read-only is fine
    if [[ -d "$HOME/.aws" ]]; then
        docker_args+=("-v" "$HOME/.aws:/root/.aws:ro")
    fi

    # GCP: needs write access for logs/credentials.db, so mount read-write
    # or use a cache directory approach
    if [[ -d "$HOME/.config/gcloud" ]]; then
        # Create a writable gcloud config cache in output dir
        mkdir -p "$OUTPUT_DIR/.gcloud-cache"
        # Mount original as read-only at a different path, and cache as writable
        docker_args+=("-v" "$HOME/.config/gcloud:/root/.config/gcloud-host:ro")
        docker_args+=("-v" "$OUTPUT_DIR/.gcloud-cache:/root/.config/gcloud")
        # Copy credentials on container start via entrypoint wrapper
        docker_args+=("-e" "GCLOUD_HOST_CONFIG=/root/.config/gcloud-host")
    fi

    # Azure: also needs write access for token cache
    if [[ -d "$HOME/.azure" ]]; then
        mkdir -p "$OUTPUT_DIR/.azure-cache"
        docker_args+=("-v" "$HOME/.azure:/root/.azure-host:ro")
        docker_args+=("-v" "$OUTPUT_DIR/.azure-cache:/root/.azure")
        docker_args+=("-e" "AZURE_HOST_CONFIG=/root/.azure-host")
    fi

    # SSH: copy to writable location (scripts may generate keys)
    if [[ -d "$HOME/.ssh" ]]; then
        mkdir -p "$OUTPUT_DIR/.ssh-cache"
        cp -a "$HOME/.ssh"/* "$OUTPUT_DIR/.ssh-cache/" 2>/dev/null || true
        chmod 700 "$OUTPUT_DIR/.ssh-cache"
        chmod 600 "$OUTPUT_DIR/.ssh-cache"/* 2>/dev/null || true
        docker_args+=("-v" "$OUTPUT_DIR/.ssh-cache:/root/.ssh")
    fi

    # Add workspace mounts
    docker_args+=("-v" "$OUTPUT_DIR:/output")
    docker_args+=("-v" "$PROJECT_ROOT:/workspace")

    # Environment variables
    docker_args+=("-e" "OUTPUT_DIR=/output")
    docker_args+=("-e" "WORKSPACE_DIR=/workspace")
    docker_args+=("-e" "USER=${USER:-root}")
    docker_args+=("-e" "HOME=/root")
    docker_args+=("-e" "TERM=${TERM:-xterm}")

    # Pass through any cloud environment variables
    [[ -n "${AWS_ACCESS_KEY_ID:-}" ]] && docker_args+=("-e" "AWS_ACCESS_KEY_ID")
    [[ -n "${AWS_SECRET_ACCESS_KEY:-}" ]] && docker_args+=("-e" "AWS_SECRET_ACCESS_KEY")
    [[ -n "${AWS_SESSION_TOKEN:-}" ]] && docker_args+=("-e" "AWS_SESSION_TOKEN")
    [[ -n "${AWS_REGION:-}" ]] && docker_args+=("-e" "AWS_REGION")
    [[ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]] && docker_args+=("-e" "GOOGLE_APPLICATION_CREDENTIALS")
    [[ -n "${CLOUDSDK_CORE_PROJECT:-}" ]] && docker_args+=("-e" "CLOUDSDK_CORE_PROJECT")
    [[ -n "${AZURE_SUBSCRIPTION_ID:-}" ]] && docker_args+=("-e" "AZURE_SUBSCRIPTION_ID")

    # Add image and command
    docker_args+=("$FULL_IMAGE" "$cmd")
    [[ $# -gt 0 ]] && docker_args+=("$@")

    # Run
    log_info "Running: $cmd $*"
    echo ""
    docker "${docker_args[@]}"
}

run_shell() {
    ensure_output_dir
    log_credential_status

    log_info "Starting interactive shell..."

    # Build docker args array
    local -a docker_args=("run" "-it" "--rm")

    # Add credential mounts (same pattern as run_container)
    if [[ -d "$HOME/.aws" ]]; then
        docker_args+=("-v" "$HOME/.aws:/root/.aws:ro")
    fi

    if [[ -d "$HOME/.config/gcloud" ]]; then
        mkdir -p "$OUTPUT_DIR/.gcloud-cache"
        docker_args+=("-v" "$HOME/.config/gcloud:/root/.config/gcloud-host:ro")
        docker_args+=("-v" "$OUTPUT_DIR/.gcloud-cache:/root/.config/gcloud")
        docker_args+=("-e" "GCLOUD_HOST_CONFIG=/root/.config/gcloud-host")
    fi

    if [[ -d "$HOME/.azure" ]]; then
        mkdir -p "$OUTPUT_DIR/.azure-cache"
        docker_args+=("-v" "$HOME/.azure:/root/.azure-host:ro")
        docker_args+=("-v" "$OUTPUT_DIR/.azure-cache:/root/.azure")
        docker_args+=("-e" "AZURE_HOST_CONFIG=/root/.azure-host")
    fi

    # SSH: copy to writable location
    if [[ -d "$HOME/.ssh" ]]; then
        mkdir -p "$OUTPUT_DIR/.ssh-cache"
        cp -a "$HOME/.ssh"/* "$OUTPUT_DIR/.ssh-cache/" 2>/dev/null || true
        chmod 700 "$OUTPUT_DIR/.ssh-cache"
        chmod 600 "$OUTPUT_DIR/.ssh-cache"/* 2>/dev/null || true
        docker_args+=("-v" "$OUTPUT_DIR/.ssh-cache:/root/.ssh")
    fi

    # Add workspace mounts
    docker_args+=("-v" "$OUTPUT_DIR:/output")
    docker_args+=("-v" "$PROJECT_ROOT:/workspace")
    docker_args+=("-e" "OUTPUT_DIR=/output")
    docker_args+=("-e" "WORKSPACE_DIR=/workspace")
    docker_args+=("-e" "USER=${USER:-root}")
    docker_args+=("-e" "HOME=/root")
    docker_args+=("-e" "TERM=${TERM:-xterm}")
    docker_args+=("--entrypoint" "/bin/bash")
    docker_args+=("$FULL_IMAGE")

    docker "${docker_args[@]}"
}

# =============================================================================
# Usage
# =============================================================================

usage() {
    cat << EOF
Iceberg Multi-Cloud Benchmark Runner

USAGE:
    ./run.sh [options] <command> [args...]

COMMANDS:
    check               Check cloud credentials and configuration status
    setup [clouds]      Create infrastructure (buckets, VMs, etc.)
    run [options]       Start benchmarks on available clouds
    monitor [interval]  Monitor running benchmarks (default: 10s refresh)
    collect             Collect results from completed benchmarks
    cleanup [clouds]    Terminate VMs and clean up state
    shell               Interactive shell inside container
    build               Build/rebuild container image

OPTIONS:
    --rebuild           Force rebuild of container image
    --output-dir DIR    Output directory (default: ./output)
    --help              Show this help message

EXAMPLES:
    # First time setup
    ./run.sh build
    ./run.sh check
    ./run.sh setup

    # Run benchmarks
    ./run.sh run --benchmark remapping-microbenchmark --config configs/quick.yaml

    # Monitor and collect
    ./run.sh monitor
    ./run.sh collect

    # Debug in container
    ./run.sh shell

CREDENTIAL MOUNTS:
    The script automatically detects and mounts credentials from:
    - ~/.aws          (AWS CLI credentials)
    - ~/.config/gcloud (Google Cloud SDK credentials)
    - ~/.azure        (Azure CLI credentials)
    - ~/.ssh          (SSH keys for VM access)

OUTPUT:
    All logs, state, and results are written to: $OUTPUT_DIR

EOF
}

# =============================================================================
# Main
# =============================================================================

main() {
    local rebuild=false
    local cmd=""

    # Parse options
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --rebuild)
                rebuild=true
                shift
                ;;
            --output-dir)
                OUTPUT_DIR="$2"
                shift 2
                ;;
            --help|-h)
                usage
                exit 0
                ;;
            -*)
                # Pass through to container
                break
                ;;
            *)
                cmd="$1"
                shift
                break
                ;;
        esac
    done

    # Handle commands
    case "${cmd:-help}" in
        build)
            build_container "true"
            ;;
        shell)
            build_container "$rebuild"
            run_shell
            ;;
        check|setup|run|monitor|collect|cleanup)
            build_container "$rebuild"
            run_container "$cmd" "$@"
            ;;
        help|--help|-h)
            usage
            ;;
        *)
            log_error "Unknown command: $cmd"
            usage
            exit 1
            ;;
    esac
}

main "$@"
