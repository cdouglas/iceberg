#!/bin/bash
#
# Quick status check for all cloud benchmarks
# Outputs minimal information to conserve tokens when used by AI assistants
#
# Usage: ./check-status.sh [aws|gcp|azure]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

check_cloud() {
    local cloud="$1"
    local ip_file="$SCRIPT_DIR/$cloud/state/public-ip"
    local user

    case "$cloud" in
        aws) user="ubuntu" ;;
        gcp) user="${USER}" ;;
        azure) user="azureuser" ;;
    esac

    if [[ ! -f "$ip_file" ]]; then
        echo "$cloud: NO_VM"
        return
    fi

    local ip=$(cat "$ip_file")
    local ssh_opts="-o StrictHostKeyChecking=no -o ConnectTimeout=5 -o LogLevel=ERROR"

    # Find SSH key
    local key=""
    for k in ~/.ssh/iceberg_benchmark_key ~/.ssh/iceberg-benchmark.pem ~/.ssh/id_rsa; do
        [[ -f "$k" ]] && key="-i $k" && break
    done

    # Quick status check
    local status=$(ssh $ssh_opts $key "$user@$ip" "cat ~/benchmark/status.txt 2>/dev/null || echo 'NOT_STARTED'" 2>/dev/null || echo "SSH_FAILED")

    if [[ "$status" == "STARTED" ]]; then
        local scenario=$(ssh $ssh_opts $key "$user@$ip" "grep 'Running scenario' ~/benchmark/benchmark.log 2>/dev/null | tail -1 | sed 's/.*scenario: //' | cut -c1-60" 2>/dev/null || echo "unknown")
        echo "$cloud: RUNNING - $scenario"
    elif [[ "$status" == "COMPLETED" ]]; then
        local results=$(ssh $ssh_opts $key "$user@$ip" "ls ~/benchmark/benchmark-results/ 2>/dev/null | tail -1" 2>/dev/null || echo "")
        echo "$cloud: COMPLETED - results in $results"
    else
        echo "$cloud: $status ($ip)"
    fi
}

# Check specific cloud or all
if [[ $# -gt 0 ]]; then
    check_cloud "$1"
else
    for cloud in aws gcp azure; do
        check_cloud "$cloud"
    done
fi
