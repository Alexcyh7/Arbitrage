#!/usr/bin/env python3
"""
Unified runner:
1) Run data collection pipeline to generate fresh block data.
2) Replay generated blocks through C++ detector.
3) Write one detection result JSON per block.

Usage:
  python examples/run_data_detection.py
"""

import argparse
import datetime
import glob
import json
import os
import urllib.parse
import re
import signal
import socket
import subprocess
import sys
import time


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COLLECTOR = os.path.join(ROOT, "data_collection", "run_full_pipeline.py")
DATA_OUTPUT = os.path.join(ROOT, "data_collection", "output")
DETECT_BIN_CC = os.path.join(ROOT, "detection", "build", "detect")
DETECT_BIN_HP = os.path.join(ROOT, "detection_graphs", "build", "detect_graphs")
ROUTE_OUTPUT = os.path.join(ROOT, "route_output.json")
DEFAULT_RESULTS_DIR = os.path.join(ROOT, "cycles_results")


def _get_detector_bin(algorithm):
    """Return the binary path for the selected algorithm."""
    if algorithm == "hp-index":
        return DETECT_BIN_HP
    return DETECT_BIN_CC


def wait_for_port(port, timeout=20):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection(("localhost", port), timeout=1):
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.2)
    return False


def send_json_update(port, payload):
    msg = json.dumps(payload, separators=(",", ":")) + "\n"
    sock = socket.create_connection(("localhost", port), timeout=15)
    sock.sendall(msg.encode())

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


def _index_block_files(directory, pattern, block_regex):
    files = glob.glob(os.path.join(directory, pattern))
    indexed = {}
    for path in files:
        name = os.path.basename(path)
        m = re.search(block_regex, name)
        if not m:
            continue
        block = int(m.group(1))
        if block not in indexed or os.path.getmtime(path) > os.path.getmtime(indexed[block]):
            indexed[block] = path
    return indexed


def _load_events(path):
    if not path:
        return []
    with open(path) as f:
        data = json.load(f)
    return data.get("events", [])


def _latest_matching_file(pattern):
    files = glob.glob(pattern)
    if not files:
        return None
    return max(files, key=os.path.getmtime)


def _start_detector(snapshot_path, seed, quote_size_eth, port, k, algorithm="color-coding", hp_threshold=10):
    _ensure_port_available(port)
    detect_bin = _get_detector_bin(algorithm)
    # Both binaries: <json_file> <seed> <quote_size_eth> <port> <k> <extra>
    # color-coding extra = num_trials (default 1), hp-index extra = hp_threshold
    extra_arg = str(hp_threshold) if algorithm == "hp-index" else "1"
    cmd = [detect_bin, snapshot_path, str(seed), str(quote_size_eth), str(port), str(k), extra_arg]
    algo_label = "hp-index" if algorithm == "hp-index" else "color-coding"
    print(
        f"[detect] start ({algo_label}): {' '.join(cmd)}",
        flush=True,
    )
    proc = subprocess.Popen(
        cmd,
        stdout=None,
        stderr=None,
        preexec_fn=os.setsid,
        cwd=ROOT,
    )
    if not wait_for_port(port):
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            proc.wait(timeout=5)
        except Exception:
            pass
        raise RuntimeError(f"Detector failed to start on port {port} for snapshot: {snapshot_path}")
    return proc


def _ensure_port_available(port):
    """Best-effort cleanup for stale detector processes holding the port."""
    if port <= 0:
        return
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.settimeout(0.3)
        in_use = probe.connect_ex(("127.0.0.1", port)) == 0
        probe.close()
    except Exception:
        in_use = True
    if not in_use:
        return

    try:
        out = subprocess.check_output(["ss", "-ltnp"], text=True, stderr=subprocess.STDOUT)
    except Exception:
        return

    pids = set()
    for line in out.splitlines():
        if f":{port} " not in line:
            continue
        pids.update(int(x) for x in re.findall(r"pid=(\d+)", line))

    if not pids:
        return

    print(f"[detect][port] port {port} is busy, cleaning pids={sorted(pids)}", flush=True)
    for pid in sorted(pids):
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except Exception:
            continue

    time.sleep(0.4)

    for pid in sorted(pids):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            continue
        except Exception:
            continue
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            pass


def _stop_detector(proc):
    if not proc:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)
    except Exception:
        pass


def run_collection(args):
    cmd = [
        sys.executable,
        COLLECTOR,
        "--eth_url",
        args.eth_url,
        "--stream_blocks",
        str(args.stream_blocks),
        "--poll_interval",
        str(args.poll_interval),
        "--snapshot_interval",
        str(args.snapshot_interval),
        "--tvl_gt",
        str(args.tvl_gt),
    ]
    if args.start_block is not None:
        cmd.extend(["--start_block", str(args.start_block)])
    if args.allow_dynamic_only:
        cmd.append("--allow_dynamic_only")

    print("\n" + "=" * 70)
    print("Step 1/2: Running data collection pipeline")
    print("=" * 70)
    print("Command:", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=os.path.join(ROOT, "data_collection"))


def run_collection_and_detection_interleaved(args):
    """
    Real-time mode:
    - Run collection pipeline.
    - As each block is collected, immediately run detection for that block.
    - On each new full snapshot, reset detector and save static result.
    """
    output_dir = args.data_output
    results_dir = args.results_dir
    static_dir = os.path.join(results_dir, "static")
    dynamic_dir = os.path.join(results_dir, "dynamic")
    os.makedirs(static_dir, exist_ok=True)
    os.makedirs(dynamic_dir, exist_ok=True)

    snapshot_dir = os.path.join(output_dir, "full_state_every_10_blocks")
    v2_dir = os.path.join(output_dir, "events_v2_new")
    v3_dir = os.path.join(output_dir, "events_v3_new")
    os.makedirs(snapshot_dir, exist_ok=True)
    os.makedirs(v2_dir, exist_ok=True)
    os.makedirs(v3_dir, exist_ok=True)

    cmd = [
        sys.executable,
        "-u",
        COLLECTOR,
        "--eth_url",
        args.eth_url,
        "--stream_blocks",
        str(args.stream_blocks),
        "--poll_interval",
        str(args.poll_interval),
        "--snapshot_interval",
        str(args.snapshot_interval),
        "--tvl_gt",
        str(args.tvl_gt),
    ]
    if args.start_block is not None:
        cmd.extend(["--start_block", str(args.start_block)])
    if args.allow_dynamic_only:
        cmd.append("--allow_dynamic_only")

    print("\n" + "=" * 70)
    print("Running interleaved collection + detection")
    print("=" * 70)
    print("Collector command:", " ".join(cmd))

    collector_env = os.environ.copy()
    collector_env["PYTHONUNBUFFERED"] = "1"
    collector = subprocess.Popen(
        cmd,
        cwd=os.path.join(ROOT, "data_collection"),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=collector_env,
    )

    detector_proc = None
    current_snapshot_block = None
    processed_dynamic_blocks = set()

    snapshot_re = re.compile(r"snapshot_block_(\d+)\.json")
    block_re = re.compile(r"\[block (\d+)\].*events=(\d+)")

    try:
        for raw_line in collector.stdout:
            line = raw_line.rstrip("\n")
            print(line, flush=True)

            if "[snapshot] saved:" in line:
                sm = snapshot_re.search(line)
                if sm:
                    block = int(sm.group(1))
                    snapshot_path = os.path.join(snapshot_dir, f"snapshot_block_{block}.json")
                    if os.path.exists(snapshot_path):
                        _stop_detector(detector_proc)
                        detector_proc = _start_detector(
                            snapshot_path, args.seed, args.quote_size_eth, args.port, args.k,
                            algorithm=args.algorithm, hp_threshold=args.hp_threshold,
                        )
                        current_snapshot_block = block
                        print(f"[detect] graph reset at block {block}", flush=True)

                        static_result = {
                            "block_number": block,
                            "phase": "static_detect_cycle",
                            "algorithm": args.algorithm,
                            "snapshot_file": snapshot_path,
                            "timestamp": datetime.datetime.now().isoformat(),
                        }
                        if os.path.exists(ROUTE_OUTPUT):
                            try:
                                with open(ROUTE_OUTPUT) as f:
                                    static_result["route_output"] = json.load(f)
                            except Exception as e:
                                static_result["route_output_error"] = str(e)
                        else:
                            static_result["route_output"] = None

                        static_file = os.path.join(static_dir, f"static_result_block_{block}.json")
                        with open(static_file, "w") as f:
                            json.dump(static_result, f, indent=2)
                        print(f"[detect] static result -> {os.path.basename(static_file)}", flush=True)

            bm = block_re.search(line)
            if bm:
                block = int(bm.group(1))
                if block in processed_dynamic_blocks:
                    continue
                processed_dynamic_blocks.add(block)

                if detector_proc is None:
                    print(f"[detect][warn] skip block {block}: detector not ready (no snapshot yet)", flush=True)
                    continue

                v2_path = _latest_matching_file(os.path.join(v2_dir, f"uniswap_events_block_{block}_*.json"))
                v3_path = _latest_matching_file(os.path.join(v3_dir, f"uniswap_v3_events_block_{block}_*.json"))
                v2_events = _load_events(v2_path)
                v3_events = _load_events(v3_path)
                merged_events = v2_events + v3_events
                merged_events.sort(
                    key=lambda ev: (ev.get("transaction_index", 1 << 30), ev.get("log_index", 1 << 30))
                )

                payload = {
                    "block_info": {
                        "block_number": block,
                        "events_count": len(merged_events),
                        "scan_time": datetime.datetime.now().isoformat(),
                    },
                    "events": merged_events,
                }
                response = send_json_update(args.port, payload)

                result = {
                    "block_number": block,
                    "phase": "dynamic_detect_arbitrage_cycle",
                    "algorithm": args.algorithm,
                    "snapshot_block_in_use": current_snapshot_block,
                    "v2_events_file": v2_path,
                    "v3_events_file": v3_path,
                    "v2_events_count": len(v2_events),
                    "v3_events_count": len(v3_events),
                    "total_events_sent": len(merged_events),
                    "detector_response": response,
                    "timestamp": datetime.datetime.now().isoformat(),
                }
                out_file = os.path.join(dynamic_dir, f"dynamic_result_block_{block}.json")
                with open(out_file, "w") as f:
                    json.dump(result, f, indent=2)

                profitable = bool(response and response.get("profitable"))
                weight = response.get("weight") if response else None
                print(
                    f"[detect][block {block}] weight={weight} profitable={profitable} "
                    f"-> {os.path.basename(out_file)}",
                    flush=True,
                )

        rc = collector.wait()
        if rc != 0:
            raise RuntimeError(f"collection process failed with exit code {rc}")
    finally:
        _stop_detector(detector_proc)
        if collector.poll() is None:
            collector.terminate()
            collector.wait(timeout=5)


def run_detection_replay(args):
    print("\n" + "=" * 70)
    print("Step 2/2: Replaying collected blocks through detector")
    print("=" * 70)

    output_dir = args.data_output
    results_dir = args.results_dir
    os.makedirs(results_dir, exist_ok=True)
    static_dir = os.path.join(results_dir, "static")
    dynamic_dir = os.path.join(results_dir, "dynamic")
    os.makedirs(static_dir, exist_ok=True)
    os.makedirs(dynamic_dir, exist_ok=True)

    snapshot_dir = os.path.join(output_dir, "full_state_every_10_blocks")
    v2_dir = os.path.join(output_dir, "events_v2_new")
    v3_dir = os.path.join(output_dir, "events_v3_new")

    if not os.path.isdir(snapshot_dir):
        raise RuntimeError(f"Missing snapshot directory: {snapshot_dir}")
    if not os.path.isdir(v2_dir):
        raise RuntimeError(f"Missing V2 directory: {v2_dir}")
    if not os.path.isdir(v3_dir):
        raise RuntimeError(f"Missing V3 directory: {v3_dir}")

    snapshot_by_block = _index_block_files(
        snapshot_dir, "snapshot_block_*.json", r"snapshot_block_(\d+)\.json"
    )
    v2_by_block = _index_block_files(
        v2_dir, "uniswap_events_block_*_*.json", r"uniswap_events_block_(\d+)_\d+_\d+\.json"
    )
    v3_by_block = _index_block_files(
        v3_dir, "uniswap_v3_events_block_*_*.json", r"uniswap_v3_events_block_(\d+)_\d+_\d+\.json"
    )

    if not snapshot_by_block:
        raise RuntimeError("No snapshots found. Ensure static snapshot succeeded at least once.")

    min_block = min(snapshot_by_block.keys())
    max_block = max(set(snapshot_by_block.keys()) | set(v2_by_block.keys()) | set(v3_by_block.keys()))

    replay_start_block = min_block
    if args.start_block is not None:
        replay_start_block = max(min_block, args.start_block)

    print(f"Replay block range: {replay_start_block} -> {max_block}")
    print(
        f"Snapshots={len(snapshot_by_block)} "
        f"V2_blocks={len(v2_by_block)} V3_blocks={len(v3_by_block)}"
    )

    detector_proc = None
    current_snapshot_block = None
    preloaded_snapshot_block = None

    if replay_start_block > min_block:
        candidate_snapshot_blocks = [b for b in snapshot_by_block.keys() if b <= replay_start_block]
        if candidate_snapshot_blocks:
            preloaded_snapshot_block = max(candidate_snapshot_blocks)
            preload_snapshot_path = snapshot_by_block[preloaded_snapshot_block]
            detector_proc = _start_detector(
                preload_snapshot_path, args.seed, args.quote_size_eth, args.port, args.k,
                algorithm=args.algorithm, hp_threshold=args.hp_threshold,
            )
            current_snapshot_block = preloaded_snapshot_block
            print(
                f"[replay] preloaded snapshot block {preloaded_snapshot_block} "
                f"for start block {replay_start_block}"
            )

    try:
        for block in range(replay_start_block, max_block + 1):
            snapshot_reset = False
            snapshot_path = snapshot_by_block.get(block)
            if snapshot_path and block != preloaded_snapshot_block:
                _stop_detector(detector_proc)
                detector_proc = _start_detector(
                    snapshot_path, args.seed, args.quote_size_eth, args.port, args.k,
                    algorithm=args.algorithm, hp_threshold=args.hp_threshold,
                )
                current_snapshot_block = block
                snapshot_reset = True
                print(f"[block {block}] reset graph from {os.path.basename(snapshot_path)}")

                # Static phase result: detector computes initial best cycle on snapshot load.
                static_result = {
                    "block_number": block,
                    "phase": "static_detect_cycle",
                    "snapshot_file": snapshot_path,
                    "timestamp": datetime.datetime.now().isoformat(),
                }
                if os.path.exists(ROUTE_OUTPUT):
                    try:
                        with open(ROUTE_OUTPUT) as f:
                            static_result["route_output"] = json.load(f)
                    except Exception as e:
                        static_result["route_output_error"] = str(e)
                else:
                    static_result["route_output"] = None

                static_file = os.path.join(static_dir, f"static_result_block_{block}.json")
                with open(static_file, "w") as f:
                    json.dump(static_result, f, indent=2)

            if detector_proc is None:
                continue

            v2_events = _load_events(v2_by_block.get(block))
            v3_events = _load_events(v3_by_block.get(block))
            merged_events = v2_events + v3_events
            merged_events.sort(
                key=lambda ev: (ev.get("transaction_index", 1 << 30), ev.get("log_index", 1 << 30))
            )

            payload = {
                "block_info": {
                    "block_number": block,
                    "events_count": len(merged_events),
                    "scan_time": datetime.datetime.now().isoformat(),
                },
                "events": merged_events,
            }
            response = send_json_update(args.port, payload)

            result = {
                "block_number": block,
                "phase": "dynamic_detect_arbitrage_cycle",
                "snapshot_reset": snapshot_reset,
                "snapshot_block_in_use": current_snapshot_block,
                "snapshot_file": snapshot_path,
                "v2_events_file": v2_by_block.get(block),
                "v3_events_file": v3_by_block.get(block),
                "v2_events_count": len(v2_events),
                "v3_events_count": len(v3_events),
                "total_events_sent": len(merged_events),
                "detector_response": response,
                "timestamp": datetime.datetime.now().isoformat(),
            }
            out_file = os.path.join(dynamic_dir, f"dynamic_result_block_{block}.json")
            with open(out_file, "w") as f:
                json.dump(result, f, indent=2)

            profitable = bool(response and response.get("profitable"))
            weight = response.get("weight") if response else None
            print(
                f"[block {block}] v2={len(v2_events)} v3={len(v3_events)} "
                f"weight={weight} profitable={profitable}"
            )

        print(f"\nDone. Detection results saved to:")
        print(f"  static:  {static_dir}")
        print(f"  dynamic: {dynamic_dir}")
    finally:
        _stop_detector(detector_proc)


def parse_args():
    parser = argparse.ArgumentParser(description="Run data collection then run detector replay.")

    # Collection args
    parser.add_argument(
        "--eth_url",
        default="https://mainnet.infura.io/v3/3cfb4dfb858643278b85b2977df40068",
        help="Ethereum RPC URL for data collection.",
    )
    parser.add_argument("--stream_blocks", type=int, default=50, help="Number of streamed blocks.")
    parser.add_argument("--poll_interval", type=float, default=0.2, help="Polling interval seconds.")
    parser.add_argument("--snapshot_interval", type=int, default=10, help="Take full snapshot every N blocks.")
    parser.add_argument("--tvl_gt", type=float, default=100.0, help="TVL threshold for static snapshots.")
    parser.add_argument(
        "--allow_dynamic_only",
        action="store_true",
        help="If static snapshot unavailable, continue collection in dynamic-only mode.",
    )
    parser.add_argument(
        "--skip_collection",
        action="store_true",
        help="Skip running collection and only replay existing output data.",
    )

    # Detection replay args
    parser.add_argument("--data_output", default=DATA_OUTPUT, help="Path to collection output directory.")
    parser.add_argument(
        "--results_dir",
        default=DEFAULT_RESULTS_DIR,
        help="Directory for per-block detection results (default: Arbitrage-main/cycles_results).",
    )
    parser.add_argument("--port", type=int, default=12000, help="Detector TCP port.")
    parser.add_argument("--seed", type=int, default=42, help="Detector seed.")
    parser.add_argument("--quote_size_eth", type=float, default=0.1, help="Detector quote size in ETH.")
    parser.add_argument("--k", type=int, default=3, help="Cycle length parameter k.")
    parser.add_argument(
        "--start_block",
        type=int,
        default=None,
        help="Optional fixed start block. For collection: bootstrap snapshot block; streaming starts from next block. For replay: start replay from this block.",
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Run collection first then replay (default is interleaved real-time mode).",
    )
    parser.add_argument(
        "--algorithm",
        choices=["color-coding", "hp-index"],
        default="color-coding",
        help="Detection algorithm: 'color-coding' (randomized DP) or 'hp-index' (GraphS deterministic). Default: color-coding.",
    )
    parser.add_argument(
        "--hp_threshold",
        type=int,
        default=10,
        help="Hot-point degree threshold for hp-index algorithm (default: 10). Ignored for color-coding.",
    )
    return parser.parse_args()


def validate_eth_url_or_die(eth_url):
    parsed = urllib.parse.unquote(eth_url)
    lowered = parsed.lower()
    # Common placeholders copied from docs/commands.
    if "<your_key>" in lowered or "your_key" in lowered:
        raise RuntimeError(
            "Invalid --eth_url: it still contains placeholder text '<YOUR_KEY>'.\n"
            "Please provide a real RPC URL, for example:\n"
            "  --eth_url \"https://mainnet.infura.io/v3/<actual_project_id>\""
        )


def main():
    args = parse_args()
    validate_eth_url_or_die(args.eth_url)

    if not os.path.exists(COLLECTOR):
        print(f"ERROR: collector script not found: {COLLECTOR}")
        sys.exit(1)

    detect_bin = _get_detector_bin(args.algorithm)
    if not os.path.exists(detect_bin):
        print(f"ERROR: detector binary not found: {detect_bin}")
        if args.algorithm == "hp-index":
            print("Build it first: cd detection_graphs && mkdir -p build && cd build && cmake .. && make -j$(nproc)")
        else:
            print("Build it first: cd detection && mkdir -p build && cd build && cmake .. && make -j$(nproc)")
        sys.exit(1)

    print(f"Algorithm: {args.algorithm}", flush=True)

    if args.skip_collection:
        print("Skipping collection; using existing data output.")
        run_detection_replay(args)
        return

    if args.sequential:
        run_collection(args)
        run_detection_replay(args)
    else:
        run_collection_and_detection_interleaved(args)


if __name__ == "__main__":
    main()
