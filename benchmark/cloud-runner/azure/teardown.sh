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

# Azure infrastructure teardown for Iceberg benchmarks
# Removes all resources created by setup.sh
#
# WARNING: This deletes the resource group and ALL resources in it,
# including any benchmark data stored in the storage account.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

# Load setup configuration if available
SETUP_CONF="$SCRIPT_DIR/setup.conf"
if [[ -f "$SETUP_CONF" ]]; then
    source "$SETUP_CONF"
fi

RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-iceberg-benchmark-rg}"

usage() {
    cat <<EOF
Usage: $0 [options]

Removes all Azure benchmark infrastructure.

WARNING: This deletes the resource group and ALL resources in it,
including the storage account and any benchmark data!

Options:
  --resource-group RG    Resource group to delete (default: $RESOURCE_GROUP)
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
        --resource-group)
            RESOURCE_GROUP="$2"
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

# Check if resource group exists
if ! az group show --name "$RESOURCE_GROUP" &>/dev/null; then
    log_info "Resource group does not exist: $RESOURCE_GROUP"
    exit 0
fi

# Show what will be deleted
log_warn "This will DELETE the resource group: $RESOURCE_GROUP"
log_warn "Including ALL resources:"
az resource list --resource-group "$RESOURCE_GROUP" --query "[].{name:name, type:type}" -o table 2>/dev/null || true

echo ""

if [[ "$SKIP_CONFIRM" != "true" ]]; then
    read -p "Are you sure you want to delete all these resources? [y/N] " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Aborted."
        exit 0
    fi
fi

# Delete resource group (deletes all resources inside)
log_info "Deleting resource group $RESOURCE_GROUP..."
az group delete --name "$RESOURCE_GROUP" --yes --no-wait

log_ok "Resource group deletion initiated (async)"
log_info "Use 'az group show -n $RESOURCE_GROUP' to check status"

# Clean up local state
rm -f "$SCRIPT_DIR/state/"* 2>/dev/null || true
rm -f "$SETUP_CONF" 2>/dev/null || true

log_ok "Local state cleaned up"
