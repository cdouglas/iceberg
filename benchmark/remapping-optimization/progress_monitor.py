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
    """Format seconds as HH:MM:SS or MM:SS."""
    if seconds is None:
        return "--:--"
    seconds = int(seconds)
    if seconds < 0:
        return "--:--"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    else:
        return f"{minutes}:{secs:02d}"


def get_container_progress(container_name):
    """
    Get progress percentage and ETA from container logs.
    Returns: (progress_pct, status, eta_seconds)
    - status is 'running', 'done', or 'failed:N'
    - eta_seconds is remaining time in seconds or None
    """
    if not is_container_running(container_name):
        exit_code = get_container_exit_code(container_name)
        if exit_code == 0:
            return 100, "done", 0
        elif exit_code > 0:
            return 100, f"failed:{exit_code}", 0
        else:
            return 0, "unknown", None

    # Get last 100 lines of logs and find latest progress
    logs = run_cmd(f'docker logs --tail 100 {container_name} 2>&1')

    # JMH outputs: "# Run progress: 50.00% complete, ETA 00:01:23"
    progress_matches = re.findall(r'Run progress:\s*([\d.]+)%', logs)
    eta_matches = re.findall(r'ETA\s+(\d+:\d+:\d+|\d+:\d+)', logs)

    progress = 0
    eta_seconds = None

    if progress_matches:
        progress = int(float(progress_matches[-1]))

    if eta_matches:
        eta_seconds = parse_eta(eta_matches[-1])

    return progress, "running", eta_seconds


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
    start_time = time.time()

    while True:
        progress_data = []
        total_progress = 0
        completed = 0
        failed = 0
        max_eta = 0  # Track longest remaining time

        for strategy in STRATEGIES:
            container_name = f"{CONTAINER_PREFIX}-{strategy}-{timestamp}"
            pct, status, eta = get_container_progress(container_name)
            progress_data.append((strategy, pct, status, eta))
            total_progress += pct

            if status == "done":
                completed += 1
            elif status.startswith("failed"):
                completed += 1
                failed += 1
            elif eta is not None:
                max_eta = max(max_eta, eta)

        overall = total_progress // total

        # Clear previous output (8 lines: header + blank + 6 strategies)
        if not first_display:
            clear_lines(8)
        first_display = False

        # Calculate overall ETA (use max of individual ETAs)
        elapsed = time.time() - start_time
        if max_eta > 0:
            overall_eta = format_eta(max_eta)
        elif overall > 0:
            # Estimate from elapsed time
            estimated_total = elapsed * 100 / overall
            remaining = estimated_total - elapsed
            overall_eta = format_eta(remaining)
        else:
            overall_eta = "--:--"

        # Draw overall progress
        bar = draw_progress_bar(overall)
        print(f"Progress: {bar} {overall:3d}% ({completed}/{total} complete)  ETA: {overall_eta}")
        print()

        # Draw per-strategy progress
        for strategy, pct, status, eta in progress_data:
            if status == "done":
                print(f"  {strategy:14s} [done]")
            elif status.startswith("failed"):
                exit_code = status.split(":")[1]
                print(f"  {strategy:14s} [FAILED exit {exit_code}]")
            else:
                eta_str = format_eta(eta) if eta is not None else "--:--"
                print(f"  {strategy:14s} {pct:3d}%  ETA: {eta_str}")

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
        for strategy, pct, status, eta in progress_data:
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
