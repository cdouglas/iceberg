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
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"

wait_for_ssh() {
    local host="$1"
    local user="$2"
    local max_attempts="${3:-30}"
    local attempt=1

    log_info "Waiting for SSH to become available on $host..."
    while [[ $attempt -le $max_attempts ]]; do
        if ssh $SSH_OPTS "$user@$host" "echo ok" &>/dev/null; then
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
    ssh $SSH_OPTS "$user@$host" "$@"
}

remote_copy_to() {
    local host="$1"
    local user="$2"
    local src="$3"
    local dst="$4"
    scp $SSH_OPTS "$src" "$user@$host:$dst"
}

remote_copy_from() {
    local host="$1"
    local user="$2"
    local src="$3"
    local dst="$4"
    scp $SSH_OPTS -r "$user@$host:$src" "$dst"
}

# Benchmark execution (runs with nohup for reentrance)
REMOTE_WORK_DIR="/home/\${USER}/benchmark"

start_benchmark_remote() {
    local host="$1"
    local user="$2"
    local jar_name="$3"
    local config_name="$4"
    local storage_uri="$5"
    local extra_args="${6:-}"

    log_info "Starting benchmark on $host..."

    # Create and run the benchmark script on remote
    remote_exec "$host" "$user" bash -s <<EOF
set -e
mkdir -p $REMOTE_WORK_DIR/results
cd $REMOTE_WORK_DIR

# Create run script
cat > run.sh <<'RUNSCRIPT'
#!/bin/bash
set -e
cd $REMOTE_WORK_DIR

echo "STARTED" > status
echo "\$(date -Iseconds)" > started_at

if java -Xmx8g -jar "$jar_name" \\
    --config "$config_name" \\
    --storage-uri "$storage_uri" \\
    --output-dir results \\
    $extra_args; then
    echo "COMPLETED" > status
else
    echo "FAILED" > status
fi
echo "\$(date -Iseconds)" > finished_at
RUNSCRIPT
chmod +x run.sh

# Kill any existing benchmark
pkill -f "$jar_name" 2>/dev/null || true

# Start with nohup
nohup ./run.sh > benchmark.log 2>&1 &
echo \$! > benchmark.pid
EOF

    log_ok "Benchmark started (PID saved to benchmark.pid)"
}

check_benchmark_status() {
    local host="$1"
    local user="$2"

    local status=$(remote_exec "$host" "$user" "cat $REMOTE_WORK_DIR/status 2>/dev/null || echo 'NOT_STARTED'")
    echo "$status"
}

show_benchmark_progress() {
    local host="$1"
    local user="$2"

    local status=$(check_benchmark_status "$host" "$user")

    case "$status" in
        NOT_STARTED)
            log_warn "Benchmark has not been started"
            ;;
        STARTED)
            log_info "Benchmark is running..."
            log_info "Recent output:"
            remote_exec "$host" "$user" "tail -20 $REMOTE_WORK_DIR/benchmark.log 2>/dev/null || echo '(no output yet)'"
            ;;
        COMPLETED)
            log_ok "Benchmark completed successfully"
            remote_exec "$host" "$user" "cat $REMOTE_WORK_DIR/started_at $REMOTE_WORK_DIR/finished_at" | \
                awk 'NR==1{start=$0} NR==2{print "Started: " start "\nFinished: " $0}'
            ;;
        FAILED)
            log_error "Benchmark failed"
            log_info "Last 50 lines of output:"
            remote_exec "$host" "$user" "tail -50 $REMOTE_WORK_DIR/benchmark.log"
            ;;
    esac

    echo "$status"
}

tail_benchmark() {
    local host="$1"
    local user="$2"

    log_info "Tailing benchmark log (Ctrl+C to stop)..."
    ssh $SSH_OPTS "$user@$host" "tail -f $REMOTE_WORK_DIR/benchmark.log"
}

collect_results() {
    local host="$1"
    local user="$2"
    local local_dir="$3"

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
    remote_copy_from "$host" "$user" "$REMOTE_WORK_DIR/results/" "$local_dir/"
    remote_copy_from "$host" "$user" "$REMOTE_WORK_DIR/benchmark.log" "$local_dir/"
    remote_copy_from "$host" "$user" "$REMOTE_WORK_DIR/status" "$local_dir/"

    log_ok "Results collected to $local_dir"
}

# Build helpers
PROJECT_ROOT=""
init_project_root() {
    PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[1]}")/../.." && pwd)"
}

build_benchmark_jar() {
    local benchmark="$1"  # remapping-microbenchmark or compaction-cloud

    log_info "Building $benchmark..."
    (cd "$PROJECT_ROOT" && ./gradlew ":benchmark:$benchmark:shadowJar" -q)

    local jar=$(ls -t "$PROJECT_ROOT/benchmark/$benchmark/build/libs/"*".jar" | head -1)
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
