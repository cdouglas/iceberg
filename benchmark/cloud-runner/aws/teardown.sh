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

# AWS infrastructure teardown for Iceberg benchmarks
# Removes resources created by setup.sh
#
# WARNING: This deletes the S3 bucket (including all data),
# IAM role, instance profile, and EC2 key pair.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

# Load setup configuration if available
SETUP_CONF="$SCRIPT_DIR/setup.conf"
if [[ -f "$SETUP_CONF" ]]; then
    source "$SETUP_CONF"
fi

REGION="${AWS_REGION:-us-west-2}"
BUCKET_NAME="${AWS_S3_BUCKET:-}"
ROLE_NAME="iceberg-benchmark-role"
PROFILE_NAME="iceberg-benchmark-profile"
KEY_NAME="${AWS_SSH_KEY_NAME:-iceberg-benchmark}"

usage() {
    cat <<EOF
Usage: $0 [options]

Removes all AWS benchmark infrastructure.

WARNING: This deletes the S3 bucket (including all data),
IAM role, instance profile, and EC2 key pair!

Options:
  --region REGION        AWS region (default: $REGION)
  --bucket NAME          S3 bucket name
  --key-name NAME        EC2 key pair name (default: $KEY_NAME)
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
        --region)
            REGION="$2"
            shift 2
            ;;
        --bucket)
            BUCKET_NAME="$2"
            shift 2
            ;;
        --key-name)
            KEY_NAME="$2"
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

# Show what will be deleted
log_warn "This will DELETE the following AWS resources:"
echo ""
echo "  Region:           $REGION"
[[ -n "$BUCKET_NAME" ]] && echo "  S3 Bucket:        $BUCKET_NAME (and all contents!)"
echo "  IAM Role:         $ROLE_NAME"
echo "  Instance Profile: $PROFILE_NAME"
echo "  EC2 Key Pair:     $KEY_NAME"
echo ""

if [[ "$SKIP_CONFIRM" != "true" ]]; then
    read -p "Are you sure you want to delete all these resources? [y/N] " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Aborted."
        exit 0
    fi
fi

# 1. Delete S3 bucket (must be empty first)
if [[ -n "$BUCKET_NAME" ]]; then
    log_info "Deleting S3 bucket: $BUCKET_NAME..."
    if aws s3api head-bucket --bucket "$BUCKET_NAME" --region "$REGION" 2>/dev/null; then
        # Empty the bucket first
        aws s3 rm "s3://$BUCKET_NAME" --recursive --region "$REGION" 2>/dev/null || true
        aws s3api delete-bucket --bucket "$BUCKET_NAME" --region "$REGION" 2>/dev/null || true
        log_ok "Deleted S3 bucket"
    else
        log_info "Bucket does not exist"
    fi
fi

# 2. Remove role from instance profile and delete profile
log_info "Deleting instance profile: $PROFILE_NAME..."
if aws iam get-instance-profile --instance-profile-name "$PROFILE_NAME" &>/dev/null; then
    aws iam remove-role-from-instance-profile \
        --instance-profile-name "$PROFILE_NAME" \
        --role-name "$ROLE_NAME" 2>/dev/null || true
    aws iam delete-instance-profile \
        --instance-profile-name "$PROFILE_NAME" 2>/dev/null || true
    log_ok "Deleted instance profile"
else
    log_info "Instance profile does not exist"
fi

# 3. Delete IAM role (must remove policies first)
log_info "Deleting IAM role: $ROLE_NAME..."
if aws iam get-role --role-name "$ROLE_NAME" &>/dev/null; then
    # Delete inline policies
    for policy in $(aws iam list-role-policies --role-name "$ROLE_NAME" --query 'PolicyNames[]' --output text 2>/dev/null); do
        aws iam delete-role-policy --role-name "$ROLE_NAME" --policy-name "$policy"
    done
    # Detach managed policies
    for policy_arn in $(aws iam list-attached-role-policies --role-name "$ROLE_NAME" --query 'AttachedPolicies[].PolicyArn' --output text 2>/dev/null); do
        aws iam detach-role-policy --role-name "$ROLE_NAME" --policy-arn "$policy_arn"
    done
    aws iam delete-role --role-name "$ROLE_NAME" 2>/dev/null || true
    log_ok "Deleted IAM role"
else
    log_info "IAM role does not exist"
fi

# 4. Delete EC2 key pair
log_info "Deleting EC2 key pair: $KEY_NAME..."
if aws ec2 describe-key-pairs --key-names "$KEY_NAME" --region "$REGION" &>/dev/null; then
    aws ec2 delete-key-pair --key-name "$KEY_NAME" --region "$REGION" 2>/dev/null || true
    log_ok "Deleted key pair"
else
    log_info "Key pair does not exist"
fi

# 5. Clean up security group (if no instances using it)
log_info "Cleaning up security group..."
SG_ID=$(aws ec2 describe-security-groups \
    --region "$REGION" \
    --filters "Name=group-name,Values=iceberg-benchmark-sg" \
    --query 'SecurityGroups[0].GroupId' \
    --output text 2>/dev/null || echo "None")

if [[ "$SG_ID" != "None" && -n "$SG_ID" ]]; then
    aws ec2 delete-security-group --group-id "$SG_ID" --region "$REGION" 2>/dev/null || \
        log_warn "Could not delete security group (may be in use)"
fi

# Clean up local state
rm -f "$SCRIPT_DIR/state/"* 2>/dev/null || true
rm -f "$SETUP_CONF" 2>/dev/null || true

log_ok "Local state cleaned up"
log_ok "AWS teardown complete"
