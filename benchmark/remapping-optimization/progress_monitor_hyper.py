#!/usr/bin/env python3
"""
Monitor progress of hyper-parallel JMH benchmark containers.

Handles partitioned containers (36 total: 2 heavy × 6 + 6 light × 4).

Usage:
    python3 progress_monitor_hyper.py <timestamp>
    python3 progress_monitor_hyper.py <timestamp> --once
"""

import subprocess
import sys
import time
import re

# Heavy strategies: 6 partitions each (by sorted × numPositions)
HEAVY_STRATEGIES = ["streamJoin", "rangeQuery"]
HEAVY_PARTITIONS = 6

# Light strategies: 4 partitions each
LIGHT_STRATEGIES = [
    "linearSearch",
    "binarySearch",
    "intervalTree",
    "smartSelector",
    "streamJoinNoPushdown",
    "rangeQueryNoPushdown",
]
LIGHT_PARTITIONS = 4

CONTAINER_PREFIX = "jmh-hyper"


def run_cmd(cmd):
    """Run command and return output."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
        return result.stdout.strip()
    except Exception:
        return ""


def is_container_running(container_name):
    """Check if container is currently running."""
    output = run_cmd(f'docker ps --format "{{{{.Names}}}}" --filter "name={container_name}"')
    return container_name in output.split('\n')


def get_container_exit_code(container_name):
    """Get exit code of stopped container."""
    output = run_cmd(f'docker inspect {container_name} --format="{{{{.State.ExitCode}}}}"')
    try:
        return int(output)
    except ValueError:
        return -1


def parse_eta(eta_str):
    """Parse ETA string like '00:01:23' to seconds."""
    try:
        parts = eta_str.split(':')
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        else:
            return int(parts[0])
    except (ValueError, IndexError):
        return None


def format_eta(seconds):
    """Format seconds as HH:MM:SS."""
    if seconds is None or seconds < 0:
        return "--:--:--"
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours}:{minutes:02d}:{secs:02d}"


def get_container_progress(container_name):
    """Get progress from container logs."""
    if not is_container_running(container_name):
        exit_code = get_container_exit_code(container_name)
        if exit_code == 0:
            return 100, "done", 0
        elif exit_code > 0:
            return 100, f"failed:{exit_code}", 0
        else:
            return 0, "unknown", None

    logs = run_cmd(f'docker logs --tail 50 {container_name} 2>&1')
    progress_matches = re.findall(r'Run progress:\s*([\d.]+)%', logs)
    eta_matches = re.findall(r'ETA\s+(\d+:\d+:\d+|\d+:\d+)', logs)

    progress = 0
    eta_seconds = None

    if progress_matches:
        progress = int(float(progress_matches[-1]))
    if eta_matches:
        eta_seconds = parse_eta(eta_matches[-1])

    return progress, "running", eta_seconds


def draw_progress_bar(percent, width=40):
    """Draw ASCII progress bar."""
    filled = int(percent * width / 100)
    empty = width - filled
    return "[" + "#" * filled + "-" * empty + "]"


def clear_screen():
    """Clear terminal screen."""
    print("\033[2J\033[H", end="")


def get_all_containers(timestamp):
    """Get list of all container specs (strategy, partition)."""
    containers = []
    for strategy in HEAVY_STRATEGIES:
        for p in range(HEAVY_PARTITIONS):
            containers.append((strategy, p, f"{CONTAINER_PREFIX}-{strategy}-p{p}-{timestamp}"))
    for strategy in LIGHT_STRATEGIES:
        for p in range(LIGHT_PARTITIONS):
            containers.append((strategy, p, f"{CONTAINER_PREFIX}-{strategy}-p{p}-{timestamp}"))
    return containers


def monitor_progress(timestamp, once=False):
    """Monitor all containers until completion."""
    containers = get_all_containers(timestamp)
    total = len(containers)
    start_time = time.time()

    while True:
        clear_screen()

        completed = 0
        failed = 0
        running_count = 0
        total_progress = 0
        max_eta = 0

        # Group by strategy for display
        strategy_status = {}

        for strategy, partition, container_name in containers:
            pct, status, eta = get_container_progress(container_name)
            total_progress += pct

            if strategy not in strategy_status:
                strategy_status[strategy] = {"done": 0, "running": 0, "failed": 0, "total": 0, "max_eta": 0}

            strategy_status[strategy]["total"] += 1

            if status == "done":
                completed += 1
                strategy_status[strategy]["done"] += 1
            elif status.startswith("failed"):
                completed += 1
                failed += 1
                strategy_status[strategy]["failed"] += 1
            else:
                running_count += 1
                strategy_status[strategy]["running"] += 1
                if eta is not None:
                    max_eta = max(max_eta, eta)
                    strategy_status[strategy]["max_eta"] = max(strategy_status[strategy]["max_eta"], eta)

        overall = total_progress // total if total > 0 else 0

        # Header
        print("=" * 60)
        print("Hyper-Parallel JMH Benchmark Progress")
        print("=" * 60)
        print()

        # Overall progress
        bar = draw_progress_bar(overall)
        elapsed = time.time() - start_time
        elapsed_str = format_eta(elapsed)
        eta_str = format_eta(max_eta) if max_eta > 0 else "--:--:--"

        print(f"Overall: {bar} {overall:3d}%")
        print(f"Containers: {completed}/{total} complete ({running_count} running, {failed} failed)")
        print(f"Elapsed: {elapsed_str}  ETA: {eta_str}")
        print()

        # Per-strategy summary
        print("Strategy                    Done/Total  Status")
        print("-" * 60)

        for strategy in HEAVY_STRATEGIES + LIGHT_STRATEGIES:
            if strategy in strategy_status:
                s = strategy_status[strategy]
                done = s["done"]
                tot = s["total"]
                running = s["running"]
                fails = s["failed"]

                if done == tot:
                    status_str = "[DONE]"
                elif fails > 0:
                    status_str = f"[{fails} FAILED]"
                elif running > 0:
                    eta = format_eta(s["max_eta"]) if s["max_eta"] > 0 else "--:--"
                    status_str = f"ETA: {eta}"
                else:
                    status_str = "[waiting]"

                print(f"  {strategy:24s} {done:2d}/{tot:2d}    {status_str}")

        print()

        # Check if done
        if completed >= total:
            print("=" * 60)
            if failed > 0:
                print(f"COMPLETE with {failed} failures")
            else:
                print("ALL COMPLETE")
            print("=" * 60)
            break

        if once:
            break

        time.sleep(5)

    return failed == 0


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 progress_monitor_hyper.py <timestamp> [--once]")
        sys.exit(1)

    timestamp = sys.argv[1]
    once = "--once" in sys.argv

    success = monitor_progress(timestamp, once=once)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
