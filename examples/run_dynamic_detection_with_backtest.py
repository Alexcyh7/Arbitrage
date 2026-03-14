#!/usr/bin/env python3
"""
Run dynamic detection and then run next-block position backtest for multiple blocks.

This script does NOT modify existing detection code. It is a wrapper that:
1) Executes examples/run_data_detection.py
2) Finds dynamic_result_block_*.json produced/updated in this run
3) Runs examples/backtest_next_block_positions.py logic for each selected block
"""

import argparse
import glob
import json
import os
import re
import subprocess
import sys
import time
from typing import Dict, List


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXAMPLES_DIR = os.path.join(ROOT, "examples")
RUN_DETECTION = os.path.join(EXAMPLES_DIR, "run_data_detection.py")
DEFAULT_RESULTS_DIR = os.path.join(ROOT, "cycles_results")
DEFAULT_DYNAMIC_DIR = os.path.join(DEFAULT_RESULTS_DIR, "dynamic")
DEFAULT_BACKTEST_OUT_DIR = os.path.join(DEFAULT_RESULTS_DIR, "backtest_next_block")

if EXAMPLES_DIR not in sys.path:
    sys.path.insert(0, EXAMPLES_DIR)

from backtest_next_block_positions import run_backtest_for_block


_RE_DYN = re.compile(r"dynamic_result_block_(\d+)\.json$")
WETH_ADDR = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"


def _dynamic_files_with_mtime(dynamic_dir: str) -> Dict[int, tuple]:
    out = {}
    for path in glob.glob(os.path.join(dynamic_dir, "dynamic_result_block_*.json")):
        name = os.path.basename(path)
        m = _RE_DYN.match(name)
        if not m:
            continue
        blk = int(m.group(1))
        out[blk] = (path, os.path.getmtime(path))
    return out


def _run_detection(args) -> None:
    cmd = [
        sys.executable,
        RUN_DETECTION,
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
        "--data_output",
        args.data_output,
        "--results_dir",
        args.results_dir,
        "--port",
        str(args.port),
        "--seed",
        str(args.seed),
        "--quote_size_eth",
        str(args.quote_size_eth),
        "--k",
        str(args.k),
        "--algorithm",
        args.algorithm,
        "--hp_threshold",
        str(args.hp_threshold),
    ]

    if args.start_block is not None:
        cmd.extend(["--start_block", str(args.start_block)])
    if args.allow_dynamic_only:
        cmd.append("--allow_dynamic_only")
    if args.skip_collection:
        cmd.append("--skip_collection")
    if args.sequential:
        cmd.append("--sequential")

    print("[wrapper] running dynamic detection command:")
    print(" ".join(cmd))
    subprocess.run(cmd, check=True, cwd=ROOT)


def _select_blocks_for_backtest(
    before: Dict[int, tuple],
    after: Dict[int, tuple],
    started_at: float,
    block_start: int | None,
    block_end: int | None,
) -> List[int]:
    selected = []
    for blk, (_, mtime) in after.items():
        if mtime < started_at:
            continue
        if block_start is not None and blk < block_start:
            continue
        if block_end is not None and blk > block_end:
            continue
        selected.append(blk)
    selected = sorted(set(selected))

    # Fallback: if files were overwritten but mtime granularity misses, use blocks not present before.
    if not selected:
        for blk in after:
            if blk not in before:
                if block_start is not None and blk < block_start:
                    continue
                if block_end is not None and blk > block_end:
                    continue
                selected.append(blk)
        selected = sorted(set(selected))
    return selected


def _first_divergence_from_positions(positions: dict) -> dict:
    """
    Find where top/middle/bottom first diverge.
    - Compares per-hop output first.
    - If all hop outputs equal, compares final to_amount.
    Returns a structured dict for summary.
    """
    top = positions.get("top", {}) or {}
    mid = positions.get("middle", {}) or {}
    bot = positions.get("bottom", {}) or {}

    top_hops = top.get("hop_results", []) or []
    mid_hops = mid.get("hop_results", []) or []
    bot_hops = bot.get("hop_results", []) or []
    min_len = min(len(top_hops), len(mid_hops), len(bot_hops))

    for i in range(min_len):
        t = str(top_hops[i].get("output"))
        m = str(mid_hops[i].get("output"))
        b = str(bot_hops[i].get("output"))
        if not (t == m == b):
            return {
                "has_divergence": True,
                "kind": "hop_output",
                "first_diff_hop": i + 1,
                "outputs": {"top": t, "middle": m, "bottom": b},
            }

    # Different path lengths are also divergence.
    if not (len(top_hops) == len(mid_hops) == len(bot_hops)):
        return {
            "has_divergence": True,
            "kind": "hop_count",
            "first_diff_hop": min_len + 1,
            "hop_counts": {"top": len(top_hops), "middle": len(mid_hops), "bottom": len(bot_hops)},
        }

    top_to = str(top.get("to_amount"))
    mid_to = str(mid.get("to_amount"))
    bot_to = str(bot.get("to_amount"))
    if not (top_to == mid_to == bot_to):
        return {
            "has_divergence": True,
            "kind": "final_to_amount",
            "first_diff_hop": None,
            "to_amount": {"top": top_to, "middle": mid_to, "bottom": bot_to},
        }

    return {"has_divergence": False, "kind": None, "first_diff_hop": None}


def parse_args():
    p = argparse.ArgumentParser(
        description="Run dynamic detection and then next-block top/middle/bottom backtest for multiple blocks."
    )

    # Detection args (kept aligned with run_data_detection.py)
    p.add_argument(
        "--eth_url",
        # default="https://mainnet.infura.io/v3/3cfb4dfb858643278b85b2977df40068",
        default="http://127.0.0.1:4291",
        help="Ethereum RPC URL for data collection.",
    )
    p.add_argument("--stream_blocks", type=int, default=50, help="Number of streamed blocks.")
    p.add_argument("--poll_interval", type=float, default=0.2, help="Polling interval seconds.")
    p.add_argument("--snapshot_interval", type=int, default=10, help="Take full snapshot every N blocks.")
    p.add_argument("--tvl_gt", type=float, default=100.0, help="TVL threshold for static snapshots.")
    p.add_argument("--allow_dynamic_only", action="store_true", help="Continue collection in dynamic-only mode.")
    p.add_argument("--skip_collection", action="store_true", help="Skip collection and replay existing output data.")
    p.add_argument(
        "--data_output",
        default=os.path.join(ROOT, "data_collection", "output"),
        help="Path to collection output directory.",
    )
    p.add_argument("--results_dir", default=DEFAULT_RESULTS_DIR, help="Directory for detection results.")
    p.add_argument("--port", type=int, default=12000, help="Detector TCP port.")
    p.add_argument("--seed", type=int, default=42, help="Detector seed.")
    p.add_argument("--quote_size_eth", type=float, default=0.01, help="Detector quote size in ETH.")
    p.add_argument("--k", type=int, default=3, help="Cycle length parameter k.")
    p.add_argument("--start_block", type=int, default=None, help="Optional fixed start block.")
    p.add_argument("--sequential", action="store_true", help="Run collection then replay.")
    p.add_argument(
        "--algorithm",
        choices=["color-coding", "hp-index"],
        default="color-coding",
        help="Detection algorithm.",
    )
    p.add_argument("--hp_threshold", type=int, default=10, help="HP-index threshold.")

    # Backtest args
    p.add_argument(
        "--backtest_only",
        action="store_true",
        help="Skip detection, only run backtest on existing dynamic_result files in selected range.",
    )
    p.add_argument("--backtest_block_start", type=int, default=None, help="Backtest start block (inclusive).")
    p.add_argument("--backtest_block_end", type=int, default=None, help="Backtest end block (inclusive).")
    p.add_argument(
        "--backtest_out_dir",
        default=DEFAULT_BACKTEST_OUT_DIR,
        help="Output directory for next-block backtest reports.",
    )
    p.add_argument(
        "--gas_units",
        type=int,
        default=300000,
        help="Estimated gas used per successful execution (default: 300000).",
    )
    p.add_argument(
        "--gas_price_gwei",
        type=float,
        default=30.0,
        help="Estimated effective gas price in gwei (default: 30).",
    )
    return p.parse_args()


def main():
    args = parse_args()
    dynamic_dir = os.path.join(args.results_dir, "dynamic")
    os.makedirs(dynamic_dir, exist_ok=True)

    before = _dynamic_files_with_mtime(dynamic_dir)
    started_at = time.time()

    if not args.backtest_only:
        _run_detection(args)

    after = _dynamic_files_with_mtime(dynamic_dir)

    if args.backtest_only:
        blocks = sorted(after.keys())
        if args.backtest_block_start is not None:
            blocks = [b for b in blocks if b >= args.backtest_block_start]
        if args.backtest_block_end is not None:
            blocks = [b for b in blocks if b <= args.backtest_block_end]
    else:
        blocks = _select_blocks_for_backtest(
            before=before,
            after=after,
            started_at=started_at,
            block_start=args.backtest_block_start,
            block_end=args.backtest_block_end,
        )

    if not blocks:
        print("[wrapper] no blocks selected for backtest.")
        return

    print(f"[wrapper] selected {len(blocks)} blocks for next-block position backtest:")
    print("[wrapper] " + ", ".join(map(str, blocks)))

    outputs = []
    failed = []
    per_block_report = []
    gas_eth = float(args.gas_units) * float(args.gas_price_gwei) * 1e-9
    gas_wei = int(gas_eth * 1e18)
    for blk in blocks:
        try:
            out_path = run_backtest_for_block(
                block=blk,
                dynamic_dir=dynamic_dir,
                events_v2_dir=os.path.join(args.data_output, "events_v2_new"),
                events_v3_dir=os.path.join(args.data_output, "events_v3_new"),
                snapshot_dir=os.path.join(args.data_output, "full_state_every_10_blocks"),
                out_dir=args.backtest_out_dir,
            )
            print(f"[wrapper][ok] block={blk} -> {out_path}")
            outputs.append({"block": blk, "report": out_path})

            data = json.load(open(out_path, "r", encoding="utf-8"))
            positions = data.get("positions", {})
            route_meta = data.get("route_from_detector", {})
            from_token = (route_meta.get("from") or "").lower()
            from_amount = int(route_meta.get("fromAmount", "0"))

            pos_pct = {}
            pos_net_pct = {}
            for pos_name in ("top", "middle", "bottom"):
                pos = positions.get(pos_name, {})
                pct = pos.get("profit_pct")
                pos_pct[pos_name] = pct
                if pct is None:
                    pos_net_pct[pos_name] = None
                    continue

                # Net pct after gas: only robustly computable when start token is WETH.
                if from_token == WETH_ADDR and from_amount > 0:
                    gas_pct = (gas_wei / from_amount) * 100.0
                    pos_net_pct[pos_name] = float(pct) - gas_pct
                else:
                    pos_net_pct[pos_name] = None

            per_block_report.append(
                {
                    "block": blk,
                    "from_token": route_meta.get("from"),
                    "from_amount": route_meta.get("fromAmount"),
                    "profit_pct": pos_pct,
                    "net_profit_pct_after_gas": pos_net_pct,
                    "divergence": _first_divergence_from_positions(positions),
                    "gas_assumption": {
                        "gas_units": args.gas_units,
                        "gas_price_gwei": args.gas_price_gwei,
                        "gas_eth": gas_eth,
                        "gas_wei": str(gas_wei),
                        "net_pct_computed_only_for_weth_start": True,
                    },
                    "report": out_path,
                }
            )
        except Exception as e:
            print(f"[wrapper][fail] block={blk}: {e}")
            failed.append({"block": blk, "error": str(e)})

    summary = {
        "total_blocks_selected": len(blocks),
        "ok": len(outputs),
        "failed": len(failed),
        "gas_assumption_global": {
            "gas_units": args.gas_units,
            "gas_price_gwei": args.gas_price_gwei,
            "gas_eth": gas_eth,
            "gas_wei": str(gas_wei),
            "note": "net_profit_pct_after_gas is only computed when route start token is WETH.",
        },
        "per_block_positions": per_block_report,
        "outputs": outputs,
        "errors": failed,
    }
    os.makedirs(args.backtest_out_dir, exist_ok=True)
    summary_path = os.path.join(args.backtest_out_dir, "batch_backtest_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print(f"[wrapper] summary written: {summary_path}")
    if failed:
        sys.exit(2)


if __name__ == "__main__":
    main()

