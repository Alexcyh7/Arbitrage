#!/usr/bin/env python3
"""
Run the static_block crawler every 5 blocks, 5 times in total; record crawl time per run.

Usage:
  python run_every_5_blocks.py [start_block]
  # default start_block: 24589771

Output:
  - Prints elapsed time for each of the 5 runs.
  - Writes crawl_times.json with block numbers and seconds.
"""

import json
import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
INTERVAL = 5
NUM_RUNS = 5
TVL_GT = 100


def find_binary():
    """Prefer built binary for accurate timing."""
    for rel in ["target/release/static_block", "target/debug/static_block"]:
        p = SCRIPT_DIR / rel
        if p.exists():
            return [str(p)]
    return None


def run_crawl(block: int) -> float:
    """Run crawler for one block; return elapsed seconds."""
    binary = find_binary()
    if binary:
        cmd = binary + ["--block", str(block), "--tvl-gt", str(TVL_GT)]
        cwd = SCRIPT_DIR
    else:
        cmd = ["cargo", "run", "--", "--block", str(block), "--tvl-gt", str(TVL_GT)]
        cwd = SCRIPT_DIR
    start = time.perf_counter()
    out = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    elapsed = time.perf_counter() - start
    if out.returncode != 0:
        print(out.stderr or out.stdout, file=sys.stderr)
        raise RuntimeError(f"Crawl failed for block {block}: exit code {out.returncode}")
    return elapsed


def main():
    start_block = int(sys.argv[1]) if len(sys.argv) > 1 else 24589771
    blocks = [start_block + i * INTERVAL for i in range(NUM_RUNS)]

    results = []
    print(f"Start block: {start_block}, interval: {INTERVAL}, runs: {NUM_RUNS}")
    print("-" * 50)

    for i, block in enumerate(blocks):
        print(f"Run {i + 1}/{NUM_RUNS} block {block} ... ", end="", flush=True)
        t = run_crawl(block)
        results.append({"block": block, "seconds": round(t, 2)})
        print(f"{t:.2f}s")

    out_path = SCRIPT_DIR / "crawl_times.json"
    with open(out_path, "w") as f:
        json.dump(
            {"start_block": start_block, "interval": INTERVAL, "runs": results},
            f,
            indent=2,
        )
    print("-" * 50)
    total = sum(r["seconds"] for r in results)
    print(f"Total: {total:.2f}s | Wrote {out_path}")


if __name__ == "__main__":
    main()
