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

# AWS infrastructure setup for Iceberg benchmarks
# Run once to create persistent infrastructure (idempotent)
#
# Creates:
#   - S3 bucket for benchmark data
#   - IAM role for EC2 instances
#   - IAM instance profile
#   - EC2 key pair (imports local SSH key)
#
# After setup, VMs launched with the instance profile will
# automatically have S3 access.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

# Configuration with defaults
REGION="${AWS_REGION:-us-west-2}"
# Bucket name must be globally unique
BUCKET_NAME="${AWS_S3_BUCKET:-iceberg-benchmark-$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo $RANDOM)}"
ROLE_NAME="iceberg-benchmark-role"
PROFILE_NAME="iceberg-benchmark-profile"
KEY_NAME="${AWS_SSH_KEY_NAME:-iceberg-benchmark}"

usage() {
    cat <<EOF
Usage: $0 [options]

One-time infrastructure setup for AWS benchmark environment.
This script is idempotent - safe to run multiple times.

Options:
  --region REGION          AWS region (default: $REGION)
  --bucket NAME            S3 bucket name (default: auto-generated)
  --key-name NAME          EC2 key pair name (default: $KEY_NAME)
  --help                   Show this help

Environment variables (override defaults):
  AWS_REGION              AWS region
  AWS_S3_BUCKET           S3 bucket name
  AWS_SSH_KEY_NAME        EC2 key pair name

After setup, export these for run.sh:
  export AWS_S3_BUCKET=<bucket-name>
  export AWS_SSH_KEY_NAME=<key-name>
  export AWS_REGION=<region>

Example:
  $0 --region us-east-1 --bucket my-benchmark-bucket
EOF
}

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

log_info "Setting up AWS benchmark infrastructure..."
log_info "  Region:           $REGION"
log_info "  S3 Bucket:        $BUCKET_NAME"
log_info "  IAM Role:         $ROLE_NAME"
log_info "  Instance Profile: $PROFILE_NAME"
log_info "  Key Pair:         $KEY_NAME"
echo ""

# 1. Create S3 bucket
log_info "Creating S3 bucket..."
if aws s3api head-bucket --bucket "$BUCKET_NAME" --region "$REGION" 2>/dev/null; then
    log_ok "Bucket already exists: $BUCKET_NAME"
else
    # us-east-1 doesn't accept LocationConstraint
    if [[ "$REGION" == "us-east-1" ]]; then
        aws s3api create-bucket \
            --bucket "$BUCKET_NAME" \
            --region "$REGION" \
            --output none
    else
        aws s3api create-bucket \
            --bucket "$BUCKET_NAME" \
            --region "$REGION" \
            --create-bucket-configuration LocationConstraint="$REGION" \
            --output none
    fi
    log_ok "Created bucket: $BUCKET_NAME"
fi

# 2. Create IAM role for EC2 instances
log_info "Creating IAM role..."

ASSUME_ROLE_POLICY=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "ec2.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF
)

if aws iam get-role --role-name "$ROLE_NAME" &>/dev/null; then
    log_ok "IAM role already exists: $ROLE_NAME"
else
    aws iam create-role \
        --role-name "$ROLE_NAME" \
        --assume-role-policy-document "$ASSUME_ROLE_POLICY" \
        --description "Role for Iceberg benchmark EC2 instances" \
        --output none
    log_ok "Created IAM role: $ROLE_NAME"
fi

# 3. Attach S3 access policy to role
log_info "Configuring S3 access policy..."

S3_POLICY=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::$BUCKET_NAME",
                "arn:aws:s3:::$BUCKET_NAME/*"
            ]
        }
    ]
}
EOF
)

POLICY_NAME="iceberg-benchmark-s3-access"

# Check if policy exists on role
if aws iam get-role-policy --role-name "$ROLE_NAME" --policy-name "$POLICY_NAME" &>/dev/null; then
    log_ok "S3 policy already attached"
else
    aws iam put-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-name "$POLICY_NAME" \
        --policy-document "$S3_POLICY"
    log_ok "Attached S3 access policy"
fi

# 4. Create instance profile
log_info "Creating instance profile..."

if aws iam get-instance-profile --instance-profile-name "$PROFILE_NAME" &>/dev/null; then
    log_ok "Instance profile already exists: $PROFILE_NAME"
else
    aws iam create-instance-profile \
        --instance-profile-name "$PROFILE_NAME" \
        --output none
    log_ok "Created instance profile: $PROFILE_NAME"

    # Add role to profile
    aws iam add-role-to-instance-profile \
        --instance-profile-name "$PROFILE_NAME" \
        --role-name "$ROLE_NAME"
    log_ok "Added role to instance profile"

    # Wait for propagation
    log_info "Waiting for IAM propagation..."
    sleep 10
fi

# 5. Create or import EC2 key pair
log_info "Setting up EC2 key pair..."

# Ensure local SSH key exists
ensure_ssh_key

if aws ec2 describe-key-pairs --key-names "$KEY_NAME" --region "$REGION" &>/dev/null; then
    log_ok "Key pair already exists: $KEY_NAME"
else
    # Import local public key
    local_pub_key=$(get_ssh_public_key)
    aws ec2 import-key-pair \
        --key-name "$KEY_NAME" \
        --public-key-material "$local_pub_key" \
        --region "$REGION" \
        --output none
    log_ok "Imported key pair: $KEY_NAME"
fi

# 6. Save configuration for run.sh
CONFIG_FILE="$SCRIPT_DIR/setup.conf"
cat > "$CONFIG_FILE" <<EOF
# Generated by setup.sh on $(date -Iseconds)
# Source this file or export these variables before running run.sh

export AWS_REGION="$REGION"
export AWS_S3_BUCKET="$BUCKET_NAME"
export AWS_SSH_KEY_NAME="$KEY_NAME"

# Storage URI for benchmark configs
# Use this format: s3://$BUCKET_NAME/benchmark
EOF

log_ok "Configuration saved to: $CONFIG_FILE"

echo ""
echo "=============================================="
log_ok "AWS setup complete!"
echo "=============================================="
echo ""
echo "Before running benchmarks, export these variables:"
echo ""
echo "  source $CONFIG_FILE"
echo ""
echo "Or manually:"
echo ""
echo "  export AWS_S3_BUCKET=\"$BUCKET_NAME\""
echo "  export AWS_SSH_KEY_NAME=\"$KEY_NAME\""
echo "  export AWS_REGION=\"$REGION\""
echo ""
echo "Storage URI for benchmark configs:"
echo "  s3://$BUCKET_NAME/benchmark"
echo ""
echo "To run benchmarks:"
echo "  cd $SCRIPT_DIR && ./run.sh start"
echo ""
