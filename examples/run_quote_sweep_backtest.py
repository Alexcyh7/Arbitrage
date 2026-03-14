#!/usr/bin/env python3
"""
Run quote-size sweep for dynamic detection + next-block backtest.

Goals:
1) Test multiple quote sizes (default: 0.01, 0.05, 0.1 ETH).
2) Test same block window with 10-block grouping:
   one full-state snapshot + 9 dynamic blocks, then repeat.
3) Put results under a dedicated folder named with block range.
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WRAPPER = os.path.join(ROOT, "examples", "run_dynamic_detection_with_backtest.py")
DEFAULT_SWEEP_ROOT = os.path.join(ROOT, "cycles_results", "quote_sweep")


def _qtag(q: float) -> str:
    # 0.01 -> 0p01, 0.1 -> 0p1
    s = f"{q}".rstrip("0").rstrip(".")
    return s.replace(".", "p")


def parse_args():
    p = argparse.ArgumentParser(
        description="Sweep quote sizes and run dynamic detection + backtest on the same block range."
    )
    p.add_argument("--start_block", type=int, required=True, help="Start block for collection bootstrap.")
    p.add_argument(
        "--stream_blocks",
        type=int,
        default=100,
        help="Number of streamed dynamic blocks (default: 100).",
    )
    p.add_argument(
        "--group_size",
        type=int,
        default=10,
        help="Group size for recompute cadence (snapshot interval). Default: 10.",
    )
    p.add_argument(
        "--quote_sizes",
        default="0.01,0.05,0.1",
        help="Comma-separated quote sizes in ETH, e.g. '0.01,0.05,0.1'.",
    )
    p.add_argument("--eth_url", default="http://127.0.0.1:4291", help="Ethereum RPC URL.")
    p.add_argument("--algorithm", choices=["color-coding", "hp-index"], default="hp-index")
    p.add_argument("--k", type=int, default=3, help="Cycle length parameter.")
    p.add_argument("--hp_threshold", type=int, default=10, help="HP-index threshold.")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--poll_interval", type=float, default=0.2)
    p.add_argument("--tvl_gt", type=float, default=100.0)
    p.add_argument("--base_port", type=int, default=12001, help="Base detector port; +1 per quote.")
    p.add_argument(
        "--data_output",
        default=os.path.join(ROOT, "data_collection", "output"),
        help="Shared data output directory.",
    )
    p.add_argument("--gas_units", type=int, default=220000)
    p.add_argument("--gas_price_gwei", type=float, default=8.0)
    p.add_argument("--sweep_root", default=DEFAULT_SWEEP_ROOT)
    return p.parse_args()


def main():
    args = parse_args()
    if not os.path.exists(WRAPPER):
        raise FileNotFoundError(f"Wrapper script not found: {WRAPPER}")

    quote_sizes = [float(x.strip()) for x in args.quote_sizes.split(",") if x.strip()]
    if not quote_sizes:
        raise RuntimeError("No quote sizes parsed from --quote_sizes")

    end_block = args.start_block + args.stream_blocks
    run_label = f"blocks_{args.start_block}_{end_block}"
    run_root = os.path.join(args.sweep_root, run_label)
    os.makedirs(run_root, exist_ok=True)

    print(f"[sweep] run_root: {run_root}")
    print(f"[sweep] quote_sizes: {quote_sizes}")

    runs = []
    for i, q in enumerate(quote_sizes):
        tag = _qtag(q)
        quote_root = os.path.join(run_root, f"quote_{tag}")
        results_dir = os.path.join(quote_root, "detection_results")
        backtest_out_dir = os.path.join(quote_root, "backtest_results")
        os.makedirs(results_dir, exist_ok=True)
        os.makedirs(backtest_out_dir, exist_ok=True)

        port = args.base_port + i
        cmd = [
            sys.executable,
            WRAPPER,
            "--eth_url",
            args.eth_url,
            "--stream_blocks",
            str(args.stream_blocks),
            "--poll_interval",
            str(args.poll_interval),
            "--snapshot_interval",
            str(args.group_size),
            "--tvl_gt",
            str(args.tvl_gt),
            "--data_output",
            args.data_output,
            "--results_dir",
            results_dir,
            "--port",
            str(port),
            "--seed",
            str(args.seed),
            "--quote_size_eth",
            str(q),
            "--k",
            str(args.k),
            "--start_block",
            str(args.start_block),
            "--algorithm",
            args.algorithm,
            "--hp_threshold",
            str(args.hp_threshold),
            "--backtest_block_start",
            str(args.start_block + 1),
            "--backtest_block_end",
            str(end_block),
            "--backtest_out_dir",
            backtest_out_dir,
            "--gas_units",
            str(args.gas_units),
            "--gas_price_gwei",
            str(args.gas_price_gwei),
        ]

        # First quote runs collection. Later quotes reuse same collected files.
        if i > 0:
            cmd.append("--skip_collection")

        print(f"\n[sweep] quote_size_eth={q} port={port}")
        print("[sweep] cmd:", " ".join(cmd))
        proc = subprocess.run(cmd, check=False, cwd=ROOT)
        if proc.returncode != 0:
            print(
                f"[sweep][warn] quote_size_eth={q} finished with non-zero exit code "
                f"{proc.returncode}. Continue to next quote."
            )

        runs.append(
            {
                "quote_size_eth": q,
                "port": port,
                "results_dir": results_dir,
                "backtest_out_dir": backtest_out_dir,
                "batch_summary": os.path.join(backtest_out_dir, "batch_backtest_summary.json"),
                "skip_collection": i > 0,
                "exit_code": proc.returncode,
            }
        )

    manifest = {
        "created_at": datetime.now().isoformat(),
        "start_block": args.start_block,
        "end_block_exclusive": end_block + 1,
        "dynamic_blocks_count": args.stream_blocks,
        "group_size": args.group_size,
        "quote_sizes": quote_sizes,
        "gas_units": args.gas_units,
        "gas_price_gwei": args.gas_price_gwei,
        "run_root": run_root,
        "runs": runs,
    }
    manifest_path = os.path.join(run_root, "sweep_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print("\n[sweep] done.")
    print(f"[sweep] manifest: {manifest_path}")


if __name__ == "__main__":
    main()

