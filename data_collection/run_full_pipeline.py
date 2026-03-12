#!/usr/bin/env python3
"""
Unified pipeline:
1) Take a full static snapshot at current latest block.
2) Start streaming latest blocks for Uniswap V2+V3 events.
3) Save incremental events for each block.
4) Take another full static snapshot every N streamed blocks.
"""

import argparse
import datetime
import json
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from web3 import Web3


BASE_DIR = Path(__file__).resolve().parent
DYNAMIC_DIR = BASE_DIR / "dynamic"
STATIC_DIR = BASE_DIR / "static_block"
OUTPUT_DIR = BASE_DIR / "output"
SNAPSHOT_DIR = OUTPUT_DIR / "full_state_every_10_blocks"
PIPELINE_LOG_DIR = OUTPUT_DIR / "pipeline_logs"
V2_EVENT_DIR = OUTPUT_DIR / "events_v2_new"
V3_EVENT_DIR = OUTPUT_DIR / "events_v3_new"


sys.path.insert(0, str(DYNAMIC_DIR))
import crawl_events_v2_streaming as v2_mod  # noqa: E402
import crawl_events_v3_streaming as v3_mod  # noqa: E402


ALL_TOPICS = (
    v2_mod.swap_event_signature,
    v2_mod.mint_event_signature,
    v2_mod.sync_event_signature,
    v2_mod.burn_event_signature,
    v2_mod.initialize_event_signature,
    v3_mod.swap_v3_signature,
    v3_mod.mint_v3_signature,
    v3_mod.burn_v3_signature,
    v3_mod.initialize_v3_signature,
)


def ensure_dirs() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    PIPELINE_LOG_DIR.mkdir(parents=True, exist_ok=True)
    V2_EVENT_DIR.mkdir(parents=True, exist_ok=True)
    V3_EVENT_DIR.mkdir(parents=True, exist_ok=True)


def run_static_snapshot(block: int, tvl_gt: float) -> Path:
    """
    Run static snapshot tool at a target block and copy result into pipeline output.
    Returns copied snapshot path.
    """
    # Priority:
    # 1) STATIC_BLOCK_BIN env override
    # 2) prebuilt binary in static_block/target/(release|debug)/static_block
    # 3) cargo run
    bin_override = os.environ.get("STATIC_BLOCK_BIN")
    binary_candidates = []
    if bin_override:
        binary_candidates.append(Path(bin_override))
    binary_candidates.extend(
        [
            STATIC_DIR / "target" / "release" / "static_block",
            STATIC_DIR / "target" / "debug" / "static_block",
        ]
    )
    binary = next((p for p in binary_candidates if p.exists() and p.is_file()), None)

    if binary is not None:
        cmd = [str(binary), "--block", str(block), "--tvl-gt", str(tvl_gt)]
    else:
        cargo_bin = shutil.which("cargo")
        if cargo_bin is None:
            hint = (
                "No static snapshot runner available.\n"
                "Please do one of the following:\n"
                "1) Install Rust/cargo (e.g. https://rustup.rs), or\n"
                "2) Build static_block binary and expose it via STATIC_BLOCK_BIN.\n"
                "   Example: export STATIC_BLOCK_BIN=/abs/path/to/static_block"
            )
            raise RuntimeError(hint)
        cmd = [
            cargo_bin,
            "run",
            "--",
            "--block",
            str(block),
            "--tvl-gt",
            str(tvl_gt),
        ]

    print(f"[snapshot] running: {' '.join(cmd)} (cwd={STATIC_DIR})")
    proc = subprocess.run(cmd, cwd=STATIC_DIR, capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr)
        stderr_text = proc.stderr or ""
        if "Invalid authentication key" in stderr_text:
            raise RuntimeError(
                "static snapshot auth failed: Tycho rejected API key.\n"
                "Set a valid TYCHO_API_KEY before running, for example:\n"
                "  export TYCHO_API_KEY=<your_tycho_key>\n"
                "Optional host override:\n"
                "  export TYCHO_URL=tycho-beta.propellerheads.xyz"
            )
        raise RuntimeError(f"static snapshot failed at block {block}")

    src = STATIC_DIR / f"snapshot_block_{block}.json"
    if not src.exists():
        raise FileNotFoundError(f"snapshot output not found: {src}")

    dst = SNAPSHOT_DIR / src.name
    if dst.exists():
        dst.unlink()
    shutil.move(str(src), str(dst))
    print(f"[snapshot] saved: {dst}")
    return dst


def stream_with_periodic_snapshots(
    eth_url: str,
    stream_blocks: int,
    poll_interval: float,
    snapshot_interval: int,
    tvl_gt: float,
    allow_dynamic_only: bool,
) -> None:
    w3 = Web3(Web3.HTTPProvider(eth_url))
    v2_mod.w3 = w3
    v3_mod.eth_node_url = eth_url

    latest_now = w3.eth.block_number
    print(f"[bootstrap] latest block now: {latest_now}")

    # 1) Initial full state at current latest block.
    static_available = True
    try:
        run_static_snapshot(latest_now, tvl_gt)
    except RuntimeError as e:
        if allow_dynamic_only:
            static_available = False
            print("[warn] static snapshot unavailable; continue in dynamic-only mode.")
            print(f"[warn] reason: {e}")
        else:
            raise

    print(f"[stream] start from next block: {latest_now + 1}")
    last_processed = latest_now
    processed = 0

    latency_rows = []
    start = time.time()

    while True:
        if stream_blocks > 0 and processed >= stream_blocks:
            break

        try:
            latest = w3.eth.block_number
        except Exception as e:  # noqa: BLE001
            print(f"[stream] failed to read latest block: {e}")
            time.sleep(poll_interval)
            continue

        if latest <= last_processed:
            time.sleep(poll_interval)
            continue

        for blk in range(last_processed + 1, latest + 1):
            block_obj = w3.eth.get_block(blk)
            t_received = time.time()
            t_block = t_received - block_obj.timestamp

            filter_params = {
                "fromBlock": hex(blk),
                "toBlock": hex(blk),
                "topics": [list(ALL_TOPICS)],
            }
            all_logs = w3.eth.get_logs(filter_params)

            with ThreadPoolExecutor(max_workers=2) as ex:
                f_v2 = ex.submit(
                    v2_mod.handle_new_block,
                    blk,
                    w3,
                    t_received_override=t_received,
                    logs_override=all_logs,
                    fast_mode=True,
                )
                f_v3 = ex.submit(
                    v3_mod.handle_new_block,
                    blk,
                    eth_url,
                    t_received_override=t_received,
                    logs_override=all_logs,
                    fast_mode=True,
                )
                _, t_update_v2 = f_v2.result()
                _, t_update_v3 = f_v3.result()

            print(
                f"[block {blk}] t_block={t_block:.3f}s "
                f"t_update_v2={t_update_v2 if t_update_v2 is not None else 'N/A'} "
                f"t_update_v3={t_update_v3 if t_update_v3 is not None else 'N/A'} "
                f"events={len(all_logs)}"
            )

            processed += 1
            last_processed = blk
            latency_rows.append(
                {
                    "block": blk,
                    "t_block": round(float(t_block), 6),
                    "t_update_v2": None if t_update_v2 is None else round(float(t_update_v2), 6),
                    "t_update_v3": None if t_update_v3 is None else round(float(t_update_v3), 6),
                    "events_count": len(all_logs),
                }
            )

            if static_available and snapshot_interval > 0 and processed % snapshot_interval == 0:
                try:
                    run_static_snapshot(blk, tvl_gt)
                except RuntimeError as e:
                    if allow_dynamic_only:
                        static_available = False
                        print(
                            "[warn] periodic static snapshot failed; "
                            "disable further static snapshots and continue streaming."
                        )
                        print(f"[warn] reason: {e}")
                    else:
                        raise

            if stream_blocks > 0 and processed >= stream_blocks:
                break

    summary = {
        "eth_url": eth_url,
        "started_at_iso": datetime.datetime.fromtimestamp(start).isoformat(),
        "ended_at_iso": datetime.datetime.now().isoformat(),
        "elapsed_seconds": round(time.time() - start, 3),
        "stream_blocks": stream_blocks,
        "snapshot_interval": snapshot_interval,
        "rows": latency_rows,
    }
    out = PIPELINE_LOG_DIR / f"pipeline_summary_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"[done] pipeline summary saved: {out}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Full pipeline: static snapshot at current block + dynamic streaming + periodic snapshots"
    )
    parser.add_argument(
        "--eth_url",
        type=str,
        default="https://mainnet.infura.io/v3/3cfb4dfb858643278b85b2977df40068",
        help="Ethereum RPC URL",
    )
    parser.add_argument(
        "--stream_blocks",
        type=int,
        default=50,
        help="How many new blocks to stream. <=0 means run forever.",
    )
    parser.add_argument(
        "--poll_interval",
        type=float,
        default=0.2,
        help="Polling interval in seconds when waiting for new blocks.",
    )
    parser.add_argument(
        "--snapshot_interval",
        type=int,
        default=10,
        help="Take a full snapshot every N streamed blocks.",
    )
    parser.add_argument(
        "--tvl_gt",
        type=float,
        default=100.0,
        help="TVL threshold for static snapshots.",
    )
    parser.add_argument(
        "--allow_dynamic_only",
        action="store_true",
        help="If static snapshot runner is unavailable, continue with dynamic streaming only.",
    )
    args = parser.parse_args()

    ensure_dirs()
    # Dynamic modules write to relative paths (events_v2_new/events_v3_new).
    # Run from output dir so those folders are created under output/.
    os.chdir(OUTPUT_DIR)
    stream_with_periodic_snapshots(
        eth_url=args.eth_url,
        stream_blocks=args.stream_blocks,
        poll_interval=args.poll_interval,
        snapshot_interval=args.snapshot_interval,
        tvl_gt=args.tvl_gt,
        allow_dynamic_only=args.allow_dynamic_only,
    )


if __name__ == "__main__":
    main()
