"""
Launch multiple C++ detector processes with different parameters.
Each process listens on a different port for dynamic pool updates.

Usage:
    python launch_detectors.py <snapshot_json> [--base-port 10000] [--quote-eth 0.1]

Example:
    python launch_detectors.py graph/snapshot_block_24589771.json
"""

import subprocess
import sys
import os
import signal
import time
import socket
import argparse
import json
import atexit


DETECT_BIN = os.path.join(os.path.dirname(__file__), "detection", "build", "detect")

# Global list for cleanup
_all_processes = []


def _cleanup():
    """Kill all child processes on exit."""
    for entry in _all_processes:
        proc = entry["proc"]
        if proc.poll() is None:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except (OSError, ProcessLookupError):
                pass
    # Give them a moment, then force kill
    time.sleep(0.5)
    for entry in _all_processes:
        proc = entry["proc"]
        if proc.poll() is None:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (OSError, ProcessLookupError):
                pass


atexit.register(_cleanup)


def wait_for_port(port, timeout=30):
    """Wait until a port is accepting connections."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection(("localhost", port), timeout=1):
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.1)
    return False


def launch_detectors(snapshot_json, configs, quote_eth=0.1):
    """
    Launch detector processes.

    Args:
        snapshot_json: path to the snapshot JSON file
        configs: list of dicts with keys: port, seed, k
        quote_eth: quote size in ETH

    Returns:
        list of subprocess.Popen objects
    """
    processes = []

    for cfg in configs:
        port = cfg["port"]
        seed = cfg["seed"]
        k = cfg["k"]

        cmd = [
            DETECT_BIN,
            snapshot_json,
            str(seed),
            str(quote_eth),
            str(port),
            str(k),
        ]

        print(f"Launching detector: seed={seed} k={k} port={port}")
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,  # new process group so we can kill it reliably
        )
        entry = {"proc": proc, **cfg}
        processes.append(entry)
        _all_processes.append(entry)

    # Wait for all ports to be ready
    print("Waiting for detectors to start...")
    for entry in processes:
        port = entry["port"]
        if wait_for_port(port):
            print(f"  Port {port} ready (seed={entry['seed']}, k={entry['k']})")
        else:
            print(f"  WARNING: Port {port} not ready after timeout")

    print(f"\nAll {len(processes)} detectors launched.")
    return processes


def stop_detectors(processes):
    """Stop all detector processes by killing their process groups."""
    for entry in processes:
        proc = entry["proc"]
        if proc.poll() is None:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except (OSError, ProcessLookupError):
                pass
    # Wait for graceful shutdown
    for entry in processes:
        proc = entry["proc"]
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                proc.wait(timeout=1)
            except (OSError, ProcessLookupError, subprocess.TimeoutExpired):
                pass
    print("All detectors stopped.")


def default_configs(base_port=10000):
    """Generate default detector configurations covering different k and seeds."""
    configs = []
    port = base_port
    for k in [2, 3, 4, 5]:
        for seed in [1, 2, 3, 42]:
            configs.append({"port": port, "seed": seed, "k": k})
            port += 1
    return configs


def main():
    parser = argparse.ArgumentParser(description="Launch arbitrage detectors")
    parser.add_argument("snapshot", help="Path to snapshot JSON file")
    parser.add_argument("--base-port", type=int, default=10000, help="Base port number")
    parser.add_argument("--quote-eth", type=float, default=0.1, help="Quote size in ETH")
    parser.add_argument("--configs", type=str, default=None,
                        help='JSON string of configs, e.g. \'[{"port":10000,"seed":42,"k":3}]\'')
    args = parser.parse_args()

    if args.configs:
        configs = json.loads(args.configs)
    else:
        configs = default_configs(args.base_port)

    processes = launch_detectors(args.snapshot, configs, args.quote_eth)

    # Write process info for the client to read
    info = [{"port": e["port"], "seed": e["seed"], "k": e["k"], "pid": e["proc"].pid}
            for e in processes]
    info_file = "detectors.json"
    with open(info_file, "w") as f:
        json.dump(info, f, indent=2)
    print(f"Detector info written to {info_file}")

    # Handle SIGTERM the same as Ctrl+C
    def sigterm_handler(signum, frame):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, sigterm_handler)

    # Keep running until interrupted
    print("\nPress Ctrl+C to stop all detectors.")
    try:
        while True:
            # Check if any process died
            for entry in processes:
                proc = entry["proc"]
                if proc.poll() is not None:
                    print(f"WARNING: Detector on port {entry['port']} exited with code {proc.returncode}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        stop_detectors(processes)


if __name__ == "__main__":
    main()
