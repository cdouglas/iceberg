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

# Azure benchmark runner for Iceberg benchmarks
# Manages Azure VM lifecycle and benchmark execution

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

init_state_dir "azure"
init_project_root

# Defaults
DEFAULT_VM_SIZE="Standard_D4s_v3"
DEFAULT_LOCATION="${AZURE_LOCATION:-eastus}"
RESOURCE_GROUP="iceberg-benchmark-rg"
VM_NAME="iceberg-benchmark"
SSH_USER="azureuser"
BENCHMARK="remapping-microbenchmark"
CONFIG_FILE=""
KEEP_VM=false
FORCE=false

# Required environment
: "${AZURE_STORAGE_ACCOUNT:?Set AZURE_STORAGE_ACCOUNT to your storage account}"
: "${AZURE_STORAGE_CONTAINER:?Set AZURE_STORAGE_CONTAINER to your container}"

usage() {
    echo "Usage: $0 <command> [options]"
    echo ""
    print_common_usage
    echo ""
    echo "Azure-specific options:"
    echo "  --location LOCATION    Azure location (default: $DEFAULT_LOCATION)"
    echo "  --resource-group RG    Resource group name (default: $RESOURCE_GROUP)"
    echo ""
    echo "Environment variables:"
    echo "  AZURE_STORAGE_ACCOUNT    Storage account name (required)"
    echo "  AZURE_STORAGE_CONTAINER  Container name (required)"
    echo "  AZURE_LOCATION           Azure location"
    echo ""
    echo "Examples:"
    echo "  $0 start"
    echo "  $0 deploy --config configs/quick.yaml"
    echo "  $0 run"
    echo "  $0 all --config configs/sigmod.yaml --keep"
}

ensure_resource_group() {
    local location="$1"

    if ! az group show --name "$RESOURCE_GROUP" &>/dev/null; then
        log_info "Creating resource group $RESOURCE_GROUP..."
        az group create --name "$RESOURCE_GROUP" --location "$location" --output none
    fi
}

vm_start() {
    local vm_size="${1:-$DEFAULT_VM_SIZE}"
    local location="$DEFAULT_LOCATION"

    # Check if VM already exists
    local existing_ip=$(state_get "public-ip")
    if [[ -n "$existing_ip" && "$FORCE" != "true" ]]; then
        local power_state=$(az vm get-instance-view \
            --resource-group "$RESOURCE_GROUP" \
            --name "$VM_NAME" \
            --query "instanceView.statuses[?starts_with(code, 'PowerState/')].displayStatus" \
            --output tsv 2>/dev/null || echo "Deleted")

        if [[ "$power_state" == "VM running" ]]; then
            log_ok "VM already running: $VM_NAME ($existing_ip)"
            return 0
        elif [[ "$power_state" == "VM starting" ]]; then
            log_info "VM is starting up..."
            az vm wait --resource-group "$RESOURCE_GROUP" --name "$VM_NAME" --created
            log_ok "VM running: $VM_NAME ($existing_ip)"
            return 0
        fi
        # Otherwise it's deleted/deallocated, create new one
        state_rm "public-ip"
    fi

    ensure_resource_group "$location"

    log_info "Creating Azure VM..."

    # Cloud-init script
    local cloud_init=$(cat <<'CLOUDINIT'
#cloud-config
package_update: true
packages:
  - openjdk-17-jdk-headless
  - tmux
  - jq

runcmd:
  - mkdir -p /home/azureuser/benchmark
  - chown azureuser:azureuser /home/azureuser/benchmark
CLOUDINIT
)

    # Ensure SSH key exists
    ensure_ssh_key

    # Create VM with system-assigned managed identity
    az vm create \
        --resource-group "$RESOURCE_GROUP" \
        --name "$VM_NAME" \
        --location "$location" \
        --image Ubuntu2204 \
        --size "$vm_size" \
        --admin-username "$SSH_USER" \
        --ssh-key-value "$(get_ssh_public_key)" \
        --assign-identity \
        --custom-data "$cloud_init" \
        --output none

    # Get public IP
    local ip=$(az vm show \
        --resource-group "$RESOURCE_GROUP" \
        --name "$VM_NAME" \
        --show-details \
        --query publicIps \
        --output tsv)

    state_set "public-ip" "$ip"
    state_set "location" "$location"

    log_info "VM created: $VM_NAME ($ip)"

    # Assign Storage Blob Data Contributor role to VM's managed identity
    log_info "Assigning storage permissions..."
    local identity_id=$(az vm show \
        --resource-group "$RESOURCE_GROUP" \
        --name "$VM_NAME" \
        --query identity.principalId \
        --output tsv)

    local storage_id=$(az storage account show \
        --name "$AZURE_STORAGE_ACCOUNT" \
        --query id \
        --output tsv)

    az role assignment create \
        --assignee-object-id "$identity_id" \
        --assignee-principal-type ServicePrincipal \
        --role "Storage Blob Data Contributor" \
        --scope "$storage_id" \
        --output none 2>/dev/null || log_warn "Role assignment may already exist"

    # Wait for SSH
    wait_for_ssh "$ip" "$SSH_USER"
    log_info "Waiting for cloud-init to complete..."
    sleep 45

    log_ok "VM ready"
}

vm_stop() {
    log_info "Deleting VM $VM_NAME..."
    az vm delete \
        --resource-group "$RESOURCE_GROUP" \
        --name "$VM_NAME" \
        --yes \
        --output none 2>/dev/null || true

    # Clean up associated resources
    az network nic delete \
        --resource-group "$RESOURCE_GROUP" \
        --name "${VM_NAME}VMNic" \
        --output none 2>/dev/null || true

    az network public-ip delete \
        --resource-group "$RESOURCE_GROUP" \
        --name "${VM_NAME}PublicIP" \
        --output none 2>/dev/null || true

    az network nsg delete \
        --resource-group "$RESOURCE_GROUP" \
        --name "${VM_NAME}NSG" \
        --output none 2>/dev/null || true

    az network vnet delete \
        --resource-group "$RESOURCE_GROUP" \
        --name "${VM_NAME}VNET" \
        --output none 2>/dev/null || true

    state_rm "public-ip"
    state_rm "location"

    log_ok "VM and resources deleted"
}

vm_status() {
    local ip=$(state_get "public-ip")
    local location=$(state_get "location")

    local power_state=$(az vm get-instance-view \
        --resource-group "$RESOURCE_GROUP" \
        --name "$VM_NAME" \
        --query "instanceView.statuses[?starts_with(code, 'PowerState/')].displayStatus" \
        --output tsv 2>/dev/null || echo "NOT_FOUND")

    echo "VM: $VM_NAME"
    echo "Resource Group: $RESOURCE_GROUP"
    echo "Location: $location"
    echo "State: $power_state"
    echo "Public IP: $ip"

    if [[ "$power_state" == "VM running" && -n "$ip" ]]; then
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
    local storage_uri="abfss://${AZURE_STORAGE_CONTAINER}@${AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/benchmark/${timestamp}"

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

    local results_dir="$PROJECT_ROOT/benchmark/$BENCHMARK/results/azure_${timestamp}"
    collect_results "$ip" "$SSH_USER" "$results_dir"
}

do_all() {
    vm_start "$DEFAULT_VM_SIZE"
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
VM_SIZE="$DEFAULT_VM_SIZE"

while [[ $# -gt 0 ]]; do
    case "$1" in
        start|stop|status|deploy|run|tail|results|all)
            COMMAND="$1"
            shift
            ;;
        --instance-type|--vm-size)
            VM_SIZE="$2"
            shift 2
            ;;
        --location)
            DEFAULT_LOCATION="$2"
            shift 2
            ;;
        --resource-group)
            RESOURCE_GROUP="$2"
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
    start)   vm_start "$VM_SIZE" ;;
    stop)    vm_stop ;;
    status)  vm_status ;;
    deploy)  do_deploy ;;
    run)     do_run ;;
    tail)    do_tail ;;
    results) do_results ;;
    all)     do_all ;;
esac
