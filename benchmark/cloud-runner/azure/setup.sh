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

# Azure infrastructure setup for Iceberg benchmarks
# Run once to create persistent infrastructure (idempotent)
#
# Creates:
#   - Resource group
#   - Storage account with hierarchical namespace (ADLS Gen2)
#   - Container for benchmark data
#   - Custom RBAC role for benchmark VMs
#
# After setup, any VM created in the resource group with managed identity
# will automatically have storage access via RBAC inheritance.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

# Configuration with defaults
LOCATION="${AZURE_LOCATION:-westus2}"
RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-iceberg-benchmark-rg}"
# Storage account name must be globally unique, 3-24 chars, lowercase alphanumeric only
STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT:-icebergbench${RANDOM}}"
STORAGE_CONTAINER="${AZURE_STORAGE_CONTAINER:-benchmark}"

usage() {
    cat <<EOF
Usage: $0 [options]

One-time infrastructure setup for Azure benchmark environment.
This script is idempotent - safe to run multiple times.

Options:
  --location LOCATION       Azure region (default: $LOCATION)
  --resource-group RG       Resource group name (default: $RESOURCE_GROUP)
  --storage-account NAME    Storage account name (default: auto-generated)
  --container NAME          Container name (default: $STORAGE_CONTAINER)
  --help                    Show this help

Environment variables (override defaults):
  AZURE_LOCATION           Azure region
  AZURE_RESOURCE_GROUP     Resource group name
  AZURE_STORAGE_ACCOUNT    Storage account name
  AZURE_STORAGE_CONTAINER  Container name

After setup, export these for run.sh:
  export AZURE_STORAGE_ACCOUNT=<storage-account-name>
  export AZURE_STORAGE_CONTAINER=$STORAGE_CONTAINER

Example:
  $0 --location eastus2 --storage-account mybenchstore
EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --location)
            LOCATION="$2"
            shift 2
            ;;
        --resource-group)
            RESOURCE_GROUP="$2"
            shift 2
            ;;
        --storage-account)
            STORAGE_ACCOUNT="$2"
            shift 2
            ;;
        --container)
            STORAGE_CONTAINER="$2"
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

# Validate storage account name
if [[ ! "$STORAGE_ACCOUNT" =~ ^[a-z0-9]{3,24}$ ]]; then
    log_error "Storage account name must be 3-24 lowercase alphanumeric characters"
    log_error "Got: $STORAGE_ACCOUNT"
    exit 1
fi

log_info "Setting up Azure benchmark infrastructure..."
log_info "  Location:        $LOCATION"
log_info "  Resource Group:  $RESOURCE_GROUP"
log_info "  Storage Account: $STORAGE_ACCOUNT"
log_info "  Container:       $STORAGE_CONTAINER"
echo ""

# 1. Create resource group
log_info "Creating resource group..."
if az group show --name "$RESOURCE_GROUP" &>/dev/null; then
    log_ok "Resource group already exists: $RESOURCE_GROUP"
else
    az group create \
        --name "$RESOURCE_GROUP" \
        --location "$LOCATION" \
        --output none
    log_ok "Created resource group: $RESOURCE_GROUP"
fi

# 2. Create storage account with hierarchical namespace (ADLS Gen2)
log_info "Creating storage account..."
if az storage account show --name "$STORAGE_ACCOUNT" --resource-group "$RESOURCE_GROUP" &>/dev/null; then
    log_ok "Storage account already exists: $STORAGE_ACCOUNT"
else
    az storage account create \
        --name "$STORAGE_ACCOUNT" \
        --resource-group "$RESOURCE_GROUP" \
        --location "$LOCATION" \
        --sku Standard_LRS \
        --kind StorageV2 \
        --hierarchical-namespace true \
        --output none
    log_ok "Created storage account: $STORAGE_ACCOUNT"
fi

# Get storage account ID for RBAC
STORAGE_ID=$(az storage account show \
    --name "$STORAGE_ACCOUNT" \
    --resource-group "$RESOURCE_GROUP" \
    --query id \
    --output tsv)

# 3. Create container
log_info "Creating storage container..."
# Use storage account key for container creation (avoid auth chicken-and-egg)
STORAGE_KEY=$(az storage account keys list \
    --account-name "$STORAGE_ACCOUNT" \
    --resource-group "$RESOURCE_GROUP" \
    --query '[0].value' \
    --output tsv)

if az storage container show \
    --name "$STORAGE_CONTAINER" \
    --account-name "$STORAGE_ACCOUNT" \
    --account-key "$STORAGE_KEY" &>/dev/null; then
    log_ok "Container already exists: $STORAGE_CONTAINER"
else
    az storage container create \
        --name "$STORAGE_CONTAINER" \
        --account-name "$STORAGE_ACCOUNT" \
        --account-key "$STORAGE_KEY" \
        --output none
    log_ok "Created container: $STORAGE_CONTAINER"
fi

# 4. Create custom role definition for benchmark VMs (scoped to resource group)
# This allows any VM with managed identity in the RG to access storage
log_info "Setting up RBAC for benchmark VMs..."

# Create a custom role that allows the current user to assign roles
# (needed for VMs to get storage access via managed identity)
ROLE_NAME="Iceberg Benchmark VM"
ROLE_DEF=$(cat <<EOF
{
    "Name": "$ROLE_NAME",
    "Description": "Allows VMs to access storage for Iceberg benchmarks",
    "Actions": [
        "Microsoft.Storage/storageAccounts/blobServices/containers/read",
        "Microsoft.Storage/storageAccounts/blobServices/containers/write",
        "Microsoft.Storage/storageAccounts/blobServices/generateUserDelegationKey/action"
    ],
    "DataActions": [
        "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/read",
        "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write",
        "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/delete",
        "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/add/action"
    ],
    "AssignableScopes": [
        "$STORAGE_ID"
    ]
}
EOF
)

# Check if custom role exists
if az role definition list --name "$ROLE_NAME" --query '[0].name' -o tsv 2>/dev/null | grep -q .; then
    log_ok "Custom role already exists: $ROLE_NAME"
else
    # Try to create custom role (may fail if not enough permissions, fall back to built-in)
    if echo "$ROLE_DEF" | az role definition create --role-definition @- --output none 2>/dev/null; then
        log_ok "Created custom role: $ROLE_NAME"
    else
        log_warn "Could not create custom role (may need Owner permissions)"
        log_info "Will use built-in 'Storage Blob Data Contributor' role instead"
    fi
fi

# 5. Save configuration for run.sh
CONFIG_FILE="$SCRIPT_DIR/setup.conf"
cat > "$CONFIG_FILE" <<EOF
# Generated by setup.sh on $(date -Iseconds)
# Source this file or export these variables before running run.sh

export AZURE_LOCATION="$LOCATION"
export AZURE_RESOURCE_GROUP="$RESOURCE_GROUP"
export AZURE_STORAGE_ACCOUNT="$STORAGE_ACCOUNT"
export AZURE_STORAGE_CONTAINER="$STORAGE_CONTAINER"

# Storage URI for benchmark configs
# Use this format: abfs://$STORAGE_CONTAINER@$STORAGE_ACCOUNT.dfs.core.windows.net/benchmark
EOF

log_ok "Configuration saved to: $CONFIG_FILE"

echo ""
echo "=============================================="
log_ok "Azure setup complete!"
echo "=============================================="
echo ""
echo "Before running benchmarks, export these variables:"
echo ""
echo "  source $CONFIG_FILE"
echo ""
echo "Or manually:"
echo ""
echo "  export AZURE_STORAGE_ACCOUNT=\"$STORAGE_ACCOUNT\""
echo "  export AZURE_STORAGE_CONTAINER=\"$STORAGE_CONTAINER\""
echo ""
echo "Storage URI for benchmark configs:"
echo "  abfs://$STORAGE_CONTAINER@$STORAGE_ACCOUNT.dfs.core.windows.net/benchmark"
echo ""
echo "To run benchmarks:"
echo "  cd $SCRIPT_DIR && ./run.sh start"
echo ""
