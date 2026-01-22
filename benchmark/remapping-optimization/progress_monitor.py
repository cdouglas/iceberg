#!/usr/bin/env python3
"""
Monitor progress of parallel JMH benchmark containers.

Usage:
    python3 progress_monitor.py <timestamp>
    python3 progress_monitor.py <timestamp> --once  # Single status check
"""

import subprocess
import sys
import time
import re
import shutil

STRATEGIES = [
    "linearSearch",
    "binarySearch",
    "intervalTree",
    "streamJoin",
    "rangeQuery",
    "smartSelector",
]

CONTAINER_PREFIX = "jmh-bench"


def get_terminal_width():
    """Get terminal width, default to 80."""
    return shutil.get_terminal_size((80, 24)).columns


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


def get_container_progress(container_name):
    """
    Get progress percentage from container logs.
    Returns: (progress_pct, status) where status is 'running', 'done', or 'failed:N'
    """
    if not is_container_running(container_name):
        exit_code = get_container_exit_code(container_name)
        if exit_code == 0:
            return 100, "done"
        elif exit_code > 0:
            return 100, f"failed:{exit_code}"
        else:
            return 0, "unknown"

    # Get last 100 lines of logs and find latest progress
    logs = run_cmd(f'docker logs --tail 100 {container_name} 2>&1')

    # JMH outputs: "# Run progress: 50.00% complete, ETA 00:01:23"
    matches = re.findall(r'Run progress:\s*([\d.]+)%', logs)
    if matches:
        return int(float(matches[-1])), "running"

    return 0, "running"


def draw_progress_bar(percent, width=30):
    """Draw ASCII progress bar."""
    filled = int(percent * width / 100)
    empty = width - filled
    return "[" + "#" * filled + "-" * empty + "]"


def clear_lines(n):
    """Move cursor up n lines and clear."""
    for _ in range(n):
        print("\033[A\033[K", end="")


def monitor_progress(timestamp, once=False):
    """Monitor all containers until completion."""
    total = len(STRATEGIES)
    first_display = True

    while True:
        progress_data = []
        total_progress = 0
        completed = 0
        failed = 0

        for strategy in STRATEGIES:
            container_name = f"{CONTAINER_PREFIX}-{strategy}-{timestamp}"
            pct, status = get_container_progress(container_name)
            progress_data.append((strategy, pct, status))
            total_progress += pct

            if status == "done":
                completed += 1
            elif status.startswith("failed"):
                completed += 1
                failed += 1

        overall = total_progress // total

        # Clear previous output (8 lines: header + blank + 6 strategies)
        if not first_display:
            clear_lines(8)
        first_display = False

        # Draw overall progress
        bar = draw_progress_bar(overall)
        print(f"Progress: {bar} {overall:3d}% ({completed}/{total} containers complete)")
        print()

        # Draw per-strategy progress
        for strategy, pct, status in progress_data:
            if status == "done":
                print(f"  {strategy:14s} [done]")
            elif status.startswith("failed"):
                exit_code = status.split(":")[1]
                print(f"  {strategy:14s} [FAILED exit {exit_code}]")
            else:
                print(f"  {strategy:14s} {pct:3d}%")

        # Check if done
        if completed >= total:
            break

        if once:
            break

        time.sleep(3)

    # Print failures if any
    if failed > 0:
        print()
        print(f"WARNING: {failed} container(s) failed")
        for strategy, pct, status in progress_data:
            if status.startswith("failed"):
                container_name = f"{CONTAINER_PREFIX}-{strategy}-{timestamp}"
                print(f"\n  {strategy} last log lines:")
                logs = run_cmd(f'docker logs --tail 10 {container_name} 2>&1')
                for line in logs.split('\n'):
                    print(f"    {line}")

    return failed == 0


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 progress_monitor.py <timestamp> [--once]")
        sys.exit(1)

    timestamp = sys.argv[1]
    once = "--once" in sys.argv

    success = monitor_progress(timestamp, once=once)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
