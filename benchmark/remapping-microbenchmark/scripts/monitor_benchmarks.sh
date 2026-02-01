#!/bin/bash
#
# Monitor running benchmarks across all three clouds
#
# Usage: ./monitor_benchmarks.sh [interval_seconds]
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLOUD_RUNNER_DIR="$SCRIPT_DIR/../../cloud-runner"

INTERVAL="${1:-30}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

get_status_icon() {
    case "$1" in
        COMPLETED) echo -e "${GREEN}✓${NC}" ;;
        FAILED)    echo -e "${RED}✗${NC}" ;;
        STARTED|running) echo -e "${YELLOW}●${NC}" ;;
        *)         echo -e "${BLUE}○${NC}" ;;
    esac
}

check_gcp() {
    local ip=$(cat "$CLOUD_RUNNER_DIR/gcp/state/public-ip" 2>/dev/null || echo "")
    if [[ -z "$ip" ]]; then
        echo "NOT_RUNNING"
        return
    fi

    local status=$(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 \
        -i ~/.ssh/iceberg_benchmark_key claude@"$ip" \
        "cat ~/benchmark/status 2>/dev/null || echo 'NOT_STARTED'" 2>/dev/null || echo "UNREACHABLE")
    echo "$status"
}

check_azure() {
    local ip=$(cat "$CLOUD_RUNNER_DIR/azure/state/public-ip" 2>/dev/null || echo "")
    if [[ -z "$ip" ]]; then
        echo "NOT_RUNNING"
        return
    fi

    local status=$(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 \
        -i ~/.ssh/iceberg_benchmark_key azureuser@"$ip" \
        "cat ~/benchmark/status 2>/dev/null || echo 'NOT_STARTED'" 2>/dev/null || echo "UNREACHABLE")
    echo "$status"
}

check_aws() {
    local ip=$(cat "$CLOUD_RUNNER_DIR/aws/state/public-ip" 2>/dev/null || echo "")
    if [[ -z "$ip" ]]; then
        echo "NOT_RUNNING"
        return
    fi

    local status=$(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 \
        -i ~/.ssh/iceberg-benchmark-aws.pem ubuntu@"$ip" \
        "cat ~/benchmark/status 2>/dev/null || echo 'NOT_STARTED'" 2>/dev/null || echo "UNREACHABLE")
    echo "$status"
}

get_last_log_line() {
    local cloud="$1"
    local ip=""
    local user=""
    local key=""

    case "$cloud" in
        gcp)
            ip=$(cat "$CLOUD_RUNNER_DIR/gcp/state/public-ip" 2>/dev/null || echo "")
            user="claude"
            key="~/.ssh/iceberg_benchmark_key"
            ;;
        azure)
            ip=$(cat "$CLOUD_RUNNER_DIR/azure/state/public-ip" 2>/dev/null || echo "")
            user="azureuser"
            key="~/.ssh/iceberg_benchmark_key"
            ;;
        aws)
            ip=$(cat "$CLOUD_RUNNER_DIR/aws/state/public-ip" 2>/dev/null || echo "")
            user="ubuntu"
            key="~/.ssh/iceberg-benchmark-aws.pem"
            ;;
    esac

    if [[ -n "$ip" ]]; then
        ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 \
            -i "$key" "$user@$ip" \
            "tail -1 ~/benchmark/benchmark.log 2>/dev/null | cut -c1-80" 2>/dev/null || echo ""
    fi
}

monitor_once() {
    clear
    echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${CYAN}                    REMAPPING BENCHMARK MONITOR                                 ${NC}"
    echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════════════════════${NC}"
    echo ""

    printf "%-10s %-15s %-60s\n" "Cloud" "Status" "Last Activity"
    echo "───────────────────────────────────────────────────────────────────────────────"

    # Check each cloud
    for cloud in aws gcp azure; do
        local status=""
        case "$cloud" in
            aws)   status=$(check_aws) ;;
            gcp)   status=$(check_gcp) ;;
            azure) status=$(check_azure) ;;
        esac

        local icon=$(get_status_icon "$status")
        local last_line=$(get_last_log_line "$cloud")

        printf "%-10s %s %-12s %-60s\n" "${cloud^^}" "$icon" "$status" "${last_line:0:60}"
    done

    echo ""
    echo "───────────────────────────────────────────────────────────────────────────────"
    echo -e "Updated: $(date '+%Y-%m-%d %H:%M:%S') | Refresh: ${INTERVAL}s | Ctrl+C to stop"
    echo ""

    # Check if all done
    local all_done=true
    for cloud in aws gcp azure; do
        local status=""
        case "$cloud" in
            aws)   status=$(check_aws) ;;
            gcp)   status=$(check_gcp) ;;
            azure) status=$(check_azure) ;;
        esac

        if [[ "$status" == "STARTED" || "$status" == "NOT_STARTED" ]]; then
            all_done=false
        fi
    done

    if $all_done; then
        echo -e "${GREEN}All benchmarks completed!${NC}"
        return 0
    fi

    return 1
}

main() {
    echo "Starting benchmark monitor (refresh every ${INTERVAL}s)..."
    echo "Press Ctrl+C to stop monitoring"
    sleep 2

    while true; do
        if monitor_once; then
            echo "All benchmarks finished. Exiting monitor."
            exit 0
        fi
        sleep "$INTERVAL"
    done
}

main
