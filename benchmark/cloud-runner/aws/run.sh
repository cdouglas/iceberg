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

# AWS benchmark runner for Iceberg benchmarks
# Manages EC2 lifecycle and benchmark execution

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

init_state_dir "aws"
init_project_root

# Defaults
DEFAULT_INSTANCE_TYPE="m5.xlarge"
DEFAULT_REGION="${AWS_REGION:-us-west-2}"
DEFAULT_AMI=""  # Will be looked up
SSH_USER="ubuntu"
SSH_KEY_NAME="${AWS_SSH_KEY_NAME:-}"
# Look for SSH key in common locations
SSH_KEY_FILE="${AWS_SSH_KEY_FILE:-}"
if [[ -z "$SSH_KEY_FILE" ]]; then
    for keypath in "/output/ssh/${SSH_KEY_NAME}.pem" "${HOME}/.ssh/${SSH_KEY_NAME}.pem" "${HOME}/.ssh/id_rsa"; do
        if [[ -f "$keypath" ]]; then
            SSH_KEY_FILE="$keypath"
            break
        fi
    done
fi
BENCHMARK="remapping-microbenchmark"
CONFIG_FILE=""
KEEP_VM=false
FORCE=false

# Required environment
: "${AWS_S3_BUCKET:?Set AWS_S3_BUCKET to your benchmark bucket}"

usage() {
    echo "Usage: $0 <command> [options]"
    echo ""
    print_common_usage
    echo ""
    echo "AWS-specific options:"
    echo "  --region REGION        AWS region (default: $DEFAULT_REGION)"
    echo "  --key-name NAME        EC2 SSH key pair name (required)"
    echo ""
    echo "Environment variables:"
    echo "  AWS_S3_BUCKET          S3 bucket for benchmark data (required)"
    echo "  AWS_SSH_KEY_NAME       EC2 key pair name"
    echo "  AWS_REGION             AWS region"
    echo ""
    echo "Examples:"
    echo "  $0 start --key-name my-key"
    echo "  $0 deploy --config configs/quick.yaml"
    echo "  $0 run"
    echo "  $0 all --config configs/sigmod.yaml --keep"
}

# Get latest Ubuntu 22.04 AMI
get_ubuntu_ami() {
    local region="$1"
    aws ec2 describe-images \
        --region "$region" \
        --owners 099720109477 \
        --filters "Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*" \
        --query 'sort_by(Images, &CreationDate)[-1].ImageId' \
        --output text
}

# Get default VPC and subnet
get_default_subnet() {
    local region="$1"
    aws ec2 describe-subnets \
        --region "$region" \
        --filters "Name=default-for-az,Values=true" \
        --query 'Subnets[0].SubnetId' \
        --output text
}

create_security_group() {
    local region="$1"
    local vpc_id="$2"
    local sg_name="iceberg-benchmark-sg"

    # Check if exists
    local sg_id=$(aws ec2 describe-security-groups \
        --region "$region" \
        --filters "Name=group-name,Values=$sg_name" "Name=vpc-id,Values=$vpc_id" \
        --query 'SecurityGroups[0].GroupId' \
        --output text 2>/dev/null || echo "None")

    if [[ "$sg_id" != "None" && "$sg_id" != "" ]]; then
        echo "$sg_id"
        return
    fi

    # Create new
    sg_id=$(aws ec2 create-security-group \
        --region "$region" \
        --group-name "$sg_name" \
        --description "Iceberg benchmark SSH access" \
        --vpc-id "$vpc_id" \
        --query 'GroupId' \
        --output text)

    aws ec2 authorize-security-group-ingress \
        --region "$region" \
        --group-id "$sg_id" \
        --protocol tcp \
        --port 22 \
        --cidr 0.0.0.0/0

    echo "$sg_id"
}

vm_start() {
    local instance_type="${1:-$DEFAULT_INSTANCE_TYPE}"
    local region="$DEFAULT_REGION"

    # Check if VM already exists
    local instance_id=$(state_get "instance-id")
    if [[ -n "$instance_id" && "$FORCE" != "true" ]]; then
        # Verify it still exists
        local state=$(aws ec2 describe-instances \
            --region "$region" \
            --instance-ids "$instance_id" \
            --query 'Reservations[0].Instances[0].State.Name' \
            --output text 2>/dev/null || echo "terminated")

        if [[ "$state" == "running" ]]; then
            local ip=$(state_get "public-ip")
            log_ok "VM already running: $instance_id ($ip)"
            return 0
        elif [[ "$state" == "pending" || "$state" == "starting" ]]; then
            log_info "VM is starting up..."
            aws ec2 wait instance-running --region "$region" --instance-ids "$instance_id"
            ip=$(aws ec2 describe-instances \
                --region "$region" \
                --instance-ids "$instance_id" \
                --query 'Reservations[0].Instances[0].PublicIpAddress' \
                --output text)
            state_set "public-ip" "$ip"
            log_ok "VM running: $instance_id ($ip)"
            return 0
        fi
        # Otherwise it's terminated/stopped, create new one
        state_rm "instance-id"
        state_rm "public-ip"
    fi

    if [[ -z "$SSH_KEY_NAME" ]]; then
        log_error "SSH key name required. Set AWS_SSH_KEY_NAME or use --key-name"
        return 1
    fi

    log_info "Creating EC2 instance..."

    # Get AMI
    local ami=$(get_ubuntu_ami "$region")
    log_info "Using AMI: $ami"

    # Get subnet and VPC
    local subnet_id=$(get_default_subnet "$region")
    local vpc_id=$(aws ec2 describe-subnets \
        --region "$region" \
        --subnet-ids "$subnet_id" \
        --query 'Subnets[0].VpcId' \
        --output text)

    # Get or create security group
    local sg_id=$(create_security_group "$region" "$vpc_id")

    # User data script for VM setup
    local user_data=$(cat <<'USERDATA'
#!/bin/bash
set -ex

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    awscli \
    jq

# Create benchmark directory
mkdir -p /home/ubuntu/benchmark
chown ubuntu:ubuntu /home/ubuntu/benchmark
USERDATA
)

    # Launch instance
    instance_id=$(aws ec2 run-instances \
        --region "$region" \
        --image-id "$ami" \
        --instance-type "$instance_type" \
        --key-name "$SSH_KEY_NAME" \
        --subnet-id "$subnet_id" \
        --security-group-ids "$sg_id" \
        --associate-public-ip-address \
        --iam-instance-profile Name=iceberg-benchmark-profile \
        --user-data "$user_data" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=iceberg-benchmark}]" \
        --query 'Instances[0].InstanceId' \
        --output text 2>/dev/null || true)

    # If IAM profile doesn't exist, try without it
    if [[ -z "$instance_id" ]]; then
        log_warn "IAM instance profile not found, launching without it (S3 access may require credentials)"
        instance_id=$(aws ec2 run-instances \
            --region "$region" \
            --image-id "$ami" \
            --instance-type "$instance_type" \
            --key-name "$SSH_KEY_NAME" \
            --subnet-id "$subnet_id" \
            --security-group-ids "$sg_id" \
            --associate-public-ip-address \
            --user-data "$user_data" \
            --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=iceberg-benchmark}]" \
            --query 'Instances[0].InstanceId' \
            --output text)
    fi

    state_set "instance-id" "$instance_id"
    log_info "Instance created: $instance_id"

    # Wait for running
    log_info "Waiting for instance to be running..."
    aws ec2 wait instance-running --region "$region" --instance-ids "$instance_id"

    # Get public IP
    local ip=$(aws ec2 describe-instances \
        --region "$region" \
        --instance-ids "$instance_id" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' \
        --output text)
    state_set "public-ip" "$ip"

    log_ok "Instance running: $instance_id ($ip)"

    # Wait for SSH and user-data to complete
    wait_for_ssh "$ip" "$SSH_USER"
    log_info "Waiting for user-data script to complete..."
    sleep 30  # Give user-data time to finish

    log_ok "VM ready"
}

vm_stop() {
    local instance_id=$(state_get "instance-id")
    if [[ -z "$instance_id" ]]; then
        log_warn "No VM to stop"
        return 0
    fi

    log_info "Terminating instance $instance_id..."
    aws ec2 terminate-instances \
        --region "$DEFAULT_REGION" \
        --instance-ids "$instance_id" >/dev/null

    state_rm "instance-id"
    state_rm "public-ip"

    log_ok "Instance terminated"
}

vm_status() {
    local instance_id=$(state_get "instance-id")
    if [[ -z "$instance_id" ]]; then
        log_info "No VM tracked in state"
        return 0
    fi

    local state=$(aws ec2 describe-instances \
        --region "$DEFAULT_REGION" \
        --instance-ids "$instance_id" \
        --query 'Reservations[0].Instances[0].State.Name' \
        --output text 2>/dev/null || echo "not-found")

    local ip=$(state_get "public-ip")

    echo "Instance ID: $instance_id"
    echo "State: $state"
    echo "Public IP: $ip"

    if [[ "$state" == "running" ]]; then
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
    local storage_uri="s3://${AWS_S3_BUCKET}/benchmark/${timestamp}"

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

    local results_dir="$PROJECT_ROOT/benchmark/$BENCHMARK/results/aws_${timestamp}"
    collect_results "$ip" "$SSH_USER" "$results_dir"
}

do_all() {
    vm_start "$DEFAULT_INSTANCE_TYPE"
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
INSTANCE_TYPE="$DEFAULT_INSTANCE_TYPE"

while [[ $# -gt 0 ]]; do
    case "$1" in
        start|stop|status|deploy|run|tail|results|all)
            COMMAND="$1"
            shift
            ;;
        --instance-type)
            INSTANCE_TYPE="$2"
            shift 2
            ;;
        --region)
            DEFAULT_REGION="$2"
            shift 2
            ;;
        --key-name)
            SSH_KEY_NAME="$2"
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
    start)   vm_start "$INSTANCE_TYPE" ;;
    stop)    vm_stop ;;
    status)  vm_status ;;
    deploy)  do_deploy ;;
    run)     do_run ;;
    tail)    do_tail ;;
    results) do_results ;;
    all)     do_all ;;
esac
