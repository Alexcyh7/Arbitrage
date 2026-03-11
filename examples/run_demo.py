"""
Demo script: runs the full arbitrage detection pipeline.

1. Static detection (no server)
2. Dynamic detection with a single detector
3. Multi-process detection with 4 detectors

Usage:
    python examples/run_demo.py
"""

import subprocess
import socket
import json
import time
import os
import sys
import signal

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DETECT_BIN = os.path.join(ROOT, "detection", "build", "detect")
SNAPSHOT = os.path.join(ROOT, "graph", "snapshot_block_24589771.json")
EXAMPLE_DIR = os.path.join(ROOT, "examples")


def send_pool_update(port, json_file):
    """Send a pool update JSON file to a detector and return the response."""
    with open(json_file) as f:
        data = json.load(f)
    # Must send as single line
    msg = json.dumps(data) + "\n"

    sock = socket.create_connection(("localhost", port), timeout=10)
    sock.sendall(msg.encode())

    # Read response
    buf = ""
    while "\n" not in buf:
        chunk = sock.recv(65536)
        if not chunk:
            break
        buf += chunk.decode()
    sock.close()

    if buf.strip():
        return json.loads(buf.strip().split("\n")[0])
    return None


def wait_for_port(port, timeout=15):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection(("localhost", port), timeout=1):
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.2)
    return False


def print_separator(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def demo_static():
    """Demo 1: Static detection (no server)."""
    print_separator("Demo 1: Static Detection (k=3, seed=42, 0.1 ETH)")

    result = subprocess.run(
        [DETECT_BIN, SNAPSHOT, "42", "0.1", "0", "3"],
        capture_output=True, text=True, timeout=30
    )

    # Print filtered output (skip node mapping lines)
    in_mapping = False
    for line in result.stdout.split("\n"):
        if "Node Mapping" in line:
            in_mapping = True
            print(line)
            print("  ... (427 token-to-id mappings omitted) ...")
            continue
        if in_mapping:
            if line.strip() == "":
                in_mapping = False
            else:
                continue
        if line.strip():
            print(line)

    print("\n[Expected] Best cycle: DAI -> USDT -> WETH -> DAI")
    print("[Expected] Weight: ~0.000781 (positive = no arbitrage in static snapshot)")
    print("[Expected] Profit: ~-0.078% (small loss after fees)")


def demo_dynamic():
    """Demo 2: Dynamic detection with pool state update."""
    print_separator("Demo 2: Dynamic Detection (pool price shift)")

    # Start detector
    proc = subprocess.Popen(
        [DETECT_BIN, SNAPSHOT, "42", "0.1", "9999", "3"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        preexec_fn=os.setsid
    )

    try:
        if not wait_for_port(9999):
            print("ERROR: Detector failed to start")
            return

        # Test 1: Send unchanged pool
        print("--- Sending unchanged pool (no arbitrage expected) ---")
        resp = send_pool_update(9999, os.path.join(EXAMPLE_DIR, "pool_update_no_arb.json"))
        if resp:
            print(f"  Weight: {resp['weight']:.6f}")
            print(f"  Profitable: {resp.get('profitable', False)}")
            print(f"  Update time: {resp.get('update_us', 0)} us")
        print()

        # Test 2: Send modified pool (2% price shift creates arbitrage)
        print("--- Sending modified pool (2% sqrt_price increase) ---")
        resp = send_pool_update(9999, os.path.join(EXAMPLE_DIR, "pool_update_arb.json"))
        if resp:
            print(f"  Weight: {resp['weight']:.6f}")
            print(f"  Profitable: {resp.get('profitable', False)}")
            print(f"  Update time: {resp.get('update_us', 0)} us")
            if resp.get("profitable") and "route" in resp:
                route = resp["route"]
                print(f"  From: {route['fromAmount']} of {route['from'][:10]}...")
                print(f"  To:   {route['toAmount']} of {route['to'][:10]}...")
                print(f"  Profit: {route['profitPct']:.4f}%")
                print(f"  Hops: {len(route['route']['fills'])}")
                for i, fill in enumerate(route["route"]["fills"]):
                    src = fill["from"][:10] + "..."
                    dst = fill["to"][:10] + "..."
                    print(f"    {i+1}. {src} -> {dst} via {fill['pool'][:10]}... ({fill['source']})")

        print("\n[Expected] Unchanged pool: weight ~0.000781, not profitable")
        print("[Expected] Modified pool: weight ~-0.0385, profitable, ~+3.93% profit")
        print("[Expected] Route: USDC -> DAI -> WETH -> USDC (3 hops)")

    finally:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)


def demo_multi_process():
    """Demo 3: Multi-process detection."""
    print_separator("Demo 3: Multi-Process Detection (4 detectors)")

    configs = [
        {"port": 11001, "seed": 42, "k": 2},
        {"port": 11002, "seed": 42, "k": 3},
        {"port": 11003, "seed": 42, "k": 4},
        {"port": 11004, "seed": 42, "k": 5},
    ]

    procs = []
    try:
        # Launch detectors
        for cfg in configs:
            proc = subprocess.Popen(
                [DETECT_BIN, SNAPSHOT, str(cfg["seed"]), "0.1", str(cfg["port"]), str(cfg["k"])],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                preexec_fn=os.setsid
            )
            procs.append((cfg, proc))
            print(f"  Launched detector: k={cfg['k']} seed={cfg['seed']} port={cfg['port']}")

        # Wait for all to be ready
        print("  Waiting for detectors to start...")
        for cfg, proc in procs:
            if not wait_for_port(cfg["port"]):
                print(f"  WARNING: port {cfg['port']} not ready")

        print("\n--- Sending modified pool to all 4 detectors ---")
        results = []
        from concurrent.futures import ThreadPoolExecutor

        def query_detector(cfg):
            resp = send_pool_update(cfg["port"], os.path.join(EXAMPLE_DIR, "pool_update_arb.json"))
            return cfg, resp

        start = time.time()
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(query_detector, cfg) for cfg, _ in procs]
            for future in futures:
                cfg, resp = future.result()
                results.append((cfg, resp))
        elapsed = (time.time() - start) * 1000

        print(f"\n  Total wall-clock time: {elapsed:.1f} ms\n")

        best_weight = float("inf")
        best_cfg = None
        best_resp = None

        for cfg, resp in sorted(results, key=lambda x: x[0]["k"]):
            if resp:
                marker = ""
                if resp["weight"] < best_weight:
                    best_weight = resp["weight"]
                    best_cfg = cfg
                    best_resp = resp
                if resp.get("profitable"):
                    marker = " <<< ARBITRAGE"
                print(f"  k={cfg['k']}: weight={resp['weight']:.6f} "
                      f"profitable={resp.get('profitable', False)} "
                      f"time={resp.get('update_us', 0)}us{marker}")

        if best_resp and best_resp.get("profitable"):
            route = best_resp["route"]
            print(f"\n  Best result: k={best_cfg['k']} with {route['profitPct']:.4f}% profit")

        print("\n[Expected] k=2: weight ~-0.003, profitable")
        print("[Expected] k=3: weight ~-0.039, profitable (BEST)")
        print("[Expected] k=4: weight ~+0.002, not profitable")
        print("[Expected] k=5: weight ~-0.035, profitable")

    finally:
        for _, proc in procs:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                proc.wait(timeout=5)
            except Exception:
                pass


def main():
    # Check binary exists
    if not os.path.exists(DETECT_BIN):
        print(f"ERROR: Detector binary not found at {DETECT_BIN}")
        print("Build it first: cd detection && mkdir -p build && cd build && cmake .. && make -j$(nproc)")
        sys.exit(1)

    if not os.path.exists(SNAPSHOT):
        print(f"ERROR: Snapshot not found at {SNAPSHOT}")
        sys.exit(1)

    print("Arbitrage Detection System - Demo")
    print("=" * 60)

    demo_static()
    demo_dynamic()
    demo_multi_process()

    print_separator("All demos completed!")


if __name__ == "__main__":
    main()
