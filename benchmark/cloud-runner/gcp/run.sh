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

# GCP benchmark runner for Iceberg benchmarks
# Manages GCE lifecycle and benchmark execution

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

init_state_dir "gcp"
init_project_root

# Defaults
DEFAULT_MACHINE_TYPE="n2-standard-4"
DEFAULT_ZONE="${GCP_ZONE:-us-central1-a}"
DEFAULT_PROJECT="${GCP_PROJECT:-}"
INSTANCE_NAME="iceberg-benchmark"
SSH_USER="${USER}"
BENCHMARK="remapping-microbenchmark"
CONFIG_FILE=""
KEEP_VM=false
FORCE=false

# Required environment
: "${GCP_GCS_BUCKET:?Set GCP_GCS_BUCKET to your benchmark bucket}"

usage() {
    echo "Usage: $0 <command> [options]"
    echo ""
    print_common_usage
    echo ""
    echo "GCP-specific options:"
    echo "  --zone ZONE            GCP zone (default: $DEFAULT_ZONE)"
    echo "  --project PROJECT      GCP project"
    echo ""
    echo "Environment variables:"
    echo "  GCP_GCS_BUCKET         GCS bucket for benchmark data (required)"
    echo "  GCP_PROJECT            GCP project ID"
    echo "  GCP_ZONE               GCP zone"
    echo ""
    echo "Examples:"
    echo "  $0 start --project my-project"
    echo "  $0 deploy --config configs/quick.yaml"
    echo "  $0 run"
    echo "  $0 all --config configs/sigmod.yaml --keep"
}

vm_start() {
    local machine_type="${1:-$DEFAULT_MACHINE_TYPE}"
    local zone="$DEFAULT_ZONE"
    local project="$DEFAULT_PROJECT"

    if [[ -z "$project" ]]; then
        project=$(gcloud config get-value project 2>/dev/null)
        if [[ -z "$project" ]]; then
            log_error "GCP project not set. Use --project or set GCP_PROJECT"
            return 1
        fi
    fi

    # Check if VM already exists
    local existing_ip=$(state_get "public-ip")
    if [[ -n "$existing_ip" && "$FORCE" != "true" ]]; then
        local status=$(gcloud compute instances describe "$INSTANCE_NAME" \
            --zone="$zone" \
            --project="$project" \
            --format='value(status)' 2>/dev/null || echo "DELETED")

        if [[ "$status" == "RUNNING" ]]; then
            log_ok "VM already running: $INSTANCE_NAME ($existing_ip)"
            return 0
        elif [[ "$status" == "STAGING" || "$status" == "PROVISIONING" ]]; then
            log_info "VM is starting up..."
            gcloud compute instances wait-until-running "$INSTANCE_NAME" \
                --zone="$zone" --project="$project"
            log_ok "VM running: $INSTANCE_NAME ($existing_ip)"
            return 0
        fi
        # Otherwise it's deleted/stopped, create new one
        state_rm "public-ip"
    fi

    log_info "Creating GCE instance..."

    # Startup script
    local startup_script=$(cat <<'STARTUP'
#!/bin/bash
set -ex

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    
    jq

# Create benchmark directory
mkdir -p /home/${USER}/benchmark
chown ${USER}:${USER} /home/${USER}/benchmark
STARTUP
)

    # Create instance
    gcloud compute instances create "$INSTANCE_NAME" \
        --zone="$zone" \
        --project="$project" \
        --machine-type="$machine_type" \
        --image-family=ubuntu-2204-lts \
        --image-project=ubuntu-os-cloud \
        --boot-disk-size=50GB \
        --scopes=storage-full \
        --metadata=startup-script="$startup_script" \
        --tags=benchmark

    # Get public IP
    local ip=$(gcloud compute instances describe "$INSTANCE_NAME" \
        --zone="$zone" \
        --project="$project" \
        --format='value(networkInterfaces[0].accessConfigs[0].natIP)')

    state_set "public-ip" "$ip"
    state_set "zone" "$zone"
    state_set "project" "$project"

    log_info "Instance created: $INSTANCE_NAME ($ip)"

    # Wait for SSH
    wait_for_ssh "$ip" "$SSH_USER"
    log_info "Waiting for startup script to complete..."
    sleep 30

    log_ok "VM ready"
}

vm_stop() {
    local zone=$(state_get "zone")
    local project=$(state_get "project")

    if [[ -z "$zone" ]]; then
        zone="$DEFAULT_ZONE"
    fi
    if [[ -z "$project" ]]; then
        project=$(gcloud config get-value project 2>/dev/null)
    fi

    log_info "Deleting instance $INSTANCE_NAME..."
    gcloud compute instances delete "$INSTANCE_NAME" \
        --zone="$zone" \
        --project="$project" \
        --quiet || true

    state_rm "public-ip"
    state_rm "zone"
    state_rm "project"

    log_ok "Instance deleted"
}

vm_status() {
    local zone=$(state_get "zone")
    local project=$(state_get "project")
    local ip=$(state_get "public-ip")

    if [[ -z "$zone" ]]; then
        zone="$DEFAULT_ZONE"
    fi
    if [[ -z "$project" ]]; then
        project=$(gcloud config get-value project 2>/dev/null)
    fi

    local status=$(gcloud compute instances describe "$INSTANCE_NAME" \
        --zone="$zone" \
        --project="$project" \
        --format='value(status)' 2>/dev/null || echo "NOT_FOUND")

    echo "Instance: $INSTANCE_NAME"
    echo "Zone: $zone"
    echo "Project: $project"
    echo "State: $status"
    echo "Public IP: $ip"

    if [[ "$status" == "RUNNING" && -n "$ip" ]]; then
        echo ""
        echo "Benchmark status:"
        show_benchmark_progress "$ip" "$SSH_USER" || true
    fi
}

do_deploy() {
    local ip=$(state_get "public-ip")
    if [[ -z "$ip" ]]; then
        log_error "No VM running. Run 'start' first."
        return 1
    fi

    # Build JAR
    local jar=$(build_benchmark_jar "$BENCHMARK")
    local jar_name=$(basename "$jar")

    log_info "Uploading JAR to VM..."
    remote_copy_to "$ip" "$SSH_USER" "$jar" "benchmark/$jar_name"

    # Upload config if specified
    if [[ -n "$CONFIG_FILE" ]]; then
        local config_name=$(basename "$CONFIG_FILE")
        log_info "Uploading config $config_name..."
        remote_copy_to "$ip" "$SSH_USER" "$CONFIG_FILE" "benchmark/$config_name"
        state_set "config-name" "$config_name"
    fi

    state_set "jar-name" "$jar_name"
    log_ok "Deployment complete"
}

do_run() {
    local ip=$(state_get "public-ip")
    if [[ -z "$ip" ]]; then
        log_error "No VM running. Run 'start' first."
        return 1
    fi

    local jar_name=$(state_get "jar-name")
    if [[ -z "$jar_name" ]]; then
        log_error "No JAR deployed. Run 'deploy' first."
        return 1
    fi

    local config_name=$(state_get "config-name")
    if [[ -z "$config_name" ]]; then
        config_name="default.yaml"
    fi

    local timestamp=$(date +%Y%m%d_%H%M%S)
    local storage_uri="gs://${GCP_GCS_BUCKET}/benchmark/${timestamp}"

    state_set "storage-uri" "$storage_uri"
    state_set "run-timestamp" "$timestamp"

    start_benchmark_remote "$ip" "$SSH_USER" "$jar_name" "$config_name" "$storage_uri"
}

do_tail() {
    local ip=$(state_get "public-ip")
    if [[ -z "$ip" ]]; then
        log_error "No VM running"
        return 1
    fi
    tail_benchmark "$ip" "$SSH_USER"
}

do_results() {
    local ip=$(state_get "public-ip")
    if [[ -z "$ip" ]]; then
        log_error "No VM running"
        return 1
    fi

    local timestamp=$(state_get "run-timestamp")
    if [[ -z "$timestamp" ]]; then
        timestamp=$(date +%Y%m%d_%H%M%S)
    fi

    local results_dir="$PROJECT_ROOT/benchmark/$BENCHMARK/results/gcp_${timestamp}"
    collect_results "$ip" "$SSH_USER" "$results_dir"
}

do_all() {
    vm_start "$DEFAULT_MACHINE_TYPE"
    do_deploy
    do_run

    log_info "Waiting for benchmark to complete..."
    local ip=$(state_get "public-ip")

    while true; do
        sleep 30
        local status=$(check_benchmark_status "$ip" "$SSH_USER")
        case "$status" in
            COMPLETED)
                log_ok "Benchmark completed"
                break
                ;;
            FAILED)
                log_error "Benchmark failed"
                do_results || true
                if [[ "$KEEP_VM" != "true" ]]; then
                    vm_stop
                fi
                return 1
                ;;
            *)
                echo -n "."
                ;;
        esac
    done

    do_results

    if [[ "$KEEP_VM" != "true" ]]; then
        vm_stop
    else
        log_info "Keeping VM running (use 'stop' to terminate)"
    fi
}

# Parse arguments
COMMAND=""
MACHINE_TYPE="$DEFAULT_MACHINE_TYPE"

while [[ $# -gt 0 ]]; do
    case "$1" in
        start|stop|status|deploy|run|tail|results|all)
            COMMAND="$1"
            shift
            ;;
        --instance-type|--machine-type)
            MACHINE_TYPE="$2"
            shift 2
            ;;
        --zone)
            DEFAULT_ZONE="$2"
            shift 2
            ;;
        --project)
            DEFAULT_PROJECT="$2"
            shift 2
            ;;
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --benchmark)
            BENCHMARK="$2"
            shift 2
            ;;
        --keep)
            KEEP_VM=true
            shift
            ;;
        --force)
            FORCE=true
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

if [[ -z "$COMMAND" ]]; then
    usage
    exit 1
fi

# Execute command
case "$COMMAND" in
    start)   vm_start "$MACHINE_TYPE" ;;
    stop)    vm_stop ;;
    status)  vm_status ;;
    deploy)  do_deploy ;;
    run)     do_run ;;
    tail)    do_tail ;;
    results) do_results ;;
    all)     do_all ;;
esac
