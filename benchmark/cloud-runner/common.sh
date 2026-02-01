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

# Common functions for cloud benchmark runners
# Source this file from cloud-specific scripts

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info()  { echo -e "${BLUE}[INFO]${NC} $*"; }
log_ok()    { echo -e "${GREEN}[OK]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# State management
state_dir=""
init_state_dir() {
    local cloud="$1"
    state_dir="$(cd "$(dirname "${BASH_SOURCE[1]}")" && pwd)/state"
    mkdir -p "$state_dir"
    # Add to .gitignore if not already
    local gitignore="$(dirname "$state_dir")/.gitignore"
    if [[ ! -f "$gitignore" ]] || ! grep -q "^state/$" "$gitignore" 2>/dev/null; then
        echo "state/" >> "$gitignore"
    fi
}

state_get() {
    local key="$1"
    local file="$state_dir/$key"
    [[ -f "$file" ]] && cat "$file" || echo ""
}

state_set() {
    local key="$1"
    local value="$2"
    echo "$value" > "$state_dir/$key"
}

state_rm() {
    local key="$1"
    rm -f "$state_dir/$key"
}

state_exists() {
    local key="$1"
    [[ -f "$state_dir/$key" ]]
}

# SSH helpers
# Default SSH key location for benchmark VMs
BENCHMARK_SSH_KEY="${HOME}/.ssh/iceberg_benchmark_key"

# Ensure SSH key exists for benchmark VMs (passwordless for automation)
ensure_ssh_key() {
    if [[ -n "${SSH_KEY_FILE:-}" && -f "$SSH_KEY_FILE" ]]; then
        return 0  # User-specified key exists
    fi

    if [[ -f "$BENCHMARK_SSH_KEY" ]]; then
        SSH_KEY_FILE="$BENCHMARK_SSH_KEY"
        return 0  # Benchmark key already exists
    fi

    log_info "Generating SSH key for benchmark VMs..."
    ssh-keygen -t ed25519 -f "$BENCHMARK_SSH_KEY" -N "" -C "iceberg-benchmark" >/dev/null 2>&1
    chmod 600 "$BENCHMARK_SSH_KEY"
    chmod 644 "${BENCHMARK_SSH_KEY}.pub"
    SSH_KEY_FILE="$BENCHMARK_SSH_KEY"
    log_ok "SSH key generated: $BENCHMARK_SSH_KEY"
}

# Get SSH public key content for VM metadata
get_ssh_public_key() {
    ensure_ssh_key
    cat "${SSH_KEY_FILE}.pub"
}

# Set SSH_KEY_FILE to use a specific key, otherwise generate/use benchmark key
SSH_KEY_FILE="${SSH_KEY_FILE:-}"

get_ssh_opts() {
    local opts="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"
    if [[ -n "$SSH_KEY_FILE" && -f "$SSH_KEY_FILE" ]]; then
        opts="$opts -i $SSH_KEY_FILE"
    fi
    echo "$opts"
}

wait_for_ssh() {
    local host="$1"
    local user="$2"
    local max_attempts="${3:-30}"
    local attempt=1
    local ssh_opts=$(get_ssh_opts)

    log_info "Waiting for SSH to become available on $host..."
    while [[ $attempt -le $max_attempts ]]; do
        if ssh $ssh_opts "$user@$host" "echo ok" &>/dev/null; then
            log_ok "SSH is ready"
            return 0
        fi
        echo -n "."
        sleep 5
        ((attempt++))
    done
    echo ""
    log_error "SSH not available after $max_attempts attempts"
    return 1
}

remote_exec() {
    local host="$1"
    local user="$2"
    shift 2
    local ssh_opts=$(get_ssh_opts)
    ssh $ssh_opts "$user@$host" "$@"
}

remote_copy_to() {
    local host="$1"
    local user="$2"
    local src="$3"
    local dst="$4"
    local ssh_opts=$(get_ssh_opts)
    scp $ssh_opts "$src" "$user@$host:$dst"
}

remote_copy_from() {
    local host="$1"
    local user="$2"
    local src="$3"
    local dst="$4"
    local ssh_opts=$(get_ssh_opts)
    scp $ssh_opts -r "$user@$host:$src" "$dst"
}

# Benchmark execution (runs with nohup for reentrance)
# Note: work_dir is computed per-call using the SSH user parameter

start_benchmark_remote() {
    local host="$1"
    local user="$2"
    local jar_name="$3"
    local config_name="$4"
    local storage_uri="$5"
    local extra_args="${6:-}"
    local work_dir="/home/${user}/benchmark"

    log_info "Starting benchmark on $host..."

    # Create and run the benchmark script on remote
    # Note: Using unquoted RUNSCRIPT heredoc so variables are expanded from local script
    remote_exec "$host" "$user" bash -s <<EOF
set -e
mkdir -p ${work_dir}/results
cd ${work_dir}

# Create run script with expanded variables from local machine
cat > run.sh <<RUNSCRIPT
#!/bin/bash
set -e
cd ${work_dir}

echo "STARTED" > status
date -Iseconds > started_at

if java -Xmx8g -jar "${jar_name}" \\
    --config "${config_name}" \\
    --storage-uri "${storage_uri}" \\
    --output-dir results \\
    ${extra_args}; then
    echo "COMPLETED" > status
else
    echo "FAILED" > status
fi
date -Iseconds > finished_at
RUNSCRIPT
chmod +x run.sh

# Kill any existing benchmark
pkill -f "${jar_name}" 2>/dev/null || true

# Start with nohup
nohup ./run.sh > benchmark.log 2>&1 &
echo \$! > benchmark.pid
EOF

    log_ok "Benchmark started (PID saved to benchmark.pid)"
}

check_benchmark_status() {
    local host="$1"
    local user="$2"
    local work_dir="/home/${user}/benchmark"

    local status=$(remote_exec "$host" "$user" "cat ${work_dir}/status 2>/dev/null || echo 'NOT_STARTED'")
    echo "$status"
}

show_benchmark_progress() {
    local host="$1"
    local user="$2"
    local work_dir="/home/${user}/benchmark"

    local status=$(check_benchmark_status "$host" "$user")

    case "$status" in
        NOT_STARTED)
            log_warn "Benchmark has not been started"
            ;;
        STARTED)
            log_info "Benchmark is running..."
            log_info "Recent output:"
            remote_exec "$host" "$user" "tail -20 ${work_dir}/benchmark.log 2>/dev/null || echo '(no output yet)'"
            ;;
        COMPLETED)
            log_ok "Benchmark completed successfully"
            remote_exec "$host" "$user" "cat ${work_dir}/started_at ${work_dir}/finished_at" | \
                awk 'NR==1{start=$0} NR==2{print "Started: " start "\nFinished: " $0}'
            ;;
        FAILED)
            log_error "Benchmark failed"
            log_info "Last 50 lines of output:"
            remote_exec "$host" "$user" "tail -50 ${work_dir}/benchmark.log"
            ;;
    esac

    echo "$status"
}

tail_benchmark() {
    local host="$1"
    local user="$2"
    local work_dir="/home/${user}/benchmark"
    local ssh_opts=$(get_ssh_opts)

    log_info "Tailing benchmark log (Ctrl+C to stop)..."
    ssh $ssh_opts "$user@$host" "tail -f ${work_dir}/benchmark.log"
}

collect_results() {
    local host="$1"
    local user="$2"
    local local_dir="$3"
    local work_dir="/home/${user}/benchmark"

    local status=$(check_benchmark_status "$host" "$user")

    if [[ "$status" == "NOT_STARTED" ]]; then
        log_error "Benchmark has not been started"
        return 1
    fi

    if [[ "$status" == "STARTED" ]]; then
        log_warn "Benchmark is still running - collecting partial results"
    fi

    mkdir -p "$local_dir"

    log_info "Collecting results to $local_dir..."
    remote_copy_from "$host" "$user" "${work_dir}/results/" "$local_dir/"
    remote_copy_from "$host" "$user" "${work_dir}/benchmark.log" "$local_dir/"
    remote_copy_from "$host" "$user" "${work_dir}/status" "$local_dir/"

    log_ok "Results collected to $local_dir"
}

# Build helpers
PROJECT_ROOT=""
init_project_root() {
    # Go up 3 levels from cloud-runner/<cloud>/run.sh to get to repo root
    # e.g., /workspace/benchmark/cloud-runner/azure -> /workspace
    PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[1]}")/../../.." && pwd)"
}

build_benchmark_jar() {
    local benchmark="$1"  # remapping-microbenchmark or compaction-cloud

    # Check if JAR already exists
    local jar=$(ls -t "$PROJECT_ROOT/benchmark/$benchmark/build/libs/"*".jar" 2>/dev/null | head -1)
    if [[ -n "$jar" ]]; then
        log_info "Using existing JAR: $(basename "$jar")" >&2
        echo "$jar"
        return 0
    fi

    # Try to build (log to stderr so stdout only has the jar path)
    log_info "Building $benchmark..." >&2
    if ! (cd "$PROJECT_ROOT" && ./gradlew ":benchmark:$benchmark:shadowJar" -q); then
        log_error "Build failed. Run this on the host first:"
        log_error "  cd $PROJECT_ROOT && ./gradlew :benchmark:$benchmark:shadowJar"
        return 1
    fi

    jar=$(ls -t "$PROJECT_ROOT/benchmark/$benchmark/build/libs/"*".jar" | head -1)
    if [[ -z "$jar" ]]; then
        log_error "Failed to find built JAR"
        return 1
    fi

    echo "$jar"
}

# Usage helpers
print_common_usage() {
    cat <<EOF
Commands:
  start       Create VM if not exists, show IP
  stop        Terminate VM
  status      Show VM and benchmark status
  deploy      Build and upload JAR + config to VM
  run         Start benchmark (detached via nohup)
  tail        Tail benchmark log (Ctrl+C to stop)
  results     Download results from VM
  all         Full workflow: start -> deploy -> run -> wait -> results -> stop

Options:
  --instance-type TYPE   VM instance type (default: cloud-specific)
  --config FILE          Benchmark config file
  --benchmark NAME       Benchmark to run (remapping-microbenchmark or compaction-cloud)
  --keep                 Don't terminate VM after 'all' command
  --force                Force recreate VM even if exists
EOF
}
