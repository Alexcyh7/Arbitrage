#!/usr/bin/env python3
"""
Run V2 and V3 Uniswap event crawlers together. T_block measured ONCE per block (shared).
Optimized: single get_logs, V2+V3 parallel, fast_mode (no sleep), lower poll_interval.
"""
import argparse
import datetime
import os
import time
from concurrent.futures import ThreadPoolExecutor
from web3 import Web3

import crawl_events_v2_streaming as v2_mod
import crawl_events_v3_streaming as v3_mod

# All topic0 signatures for single get_logs (V2 + V3)
ALL_TOPICS = (
    v2_mod.swap_event_signature, v2_mod.mint_event_signature, v2_mod.sync_event_signature,
    v2_mod.burn_event_signature, v2_mod.initialize_event_signature,
    v3_mod.swap_v3_signature, v3_mod.mint_v3_signature, v3_mod.burn_v3_signature,
    v3_mod.initialize_v3_signature,
)


def stream_combined(max_blocks, poll_interval, eth_url):
    """Single get_logs per block, V2+V3 parallel parse/write, fast_mode for minimal latency."""
    w3 = Web3(Web3.HTTPProvider(eth_url))
    v2_mod.w3 = w3
    v3_mod.eth_node_url = eth_url

    os.makedirs("events_v2_new", exist_ok=True)
    os.makedirs("events_v3_new", exist_ok=True)

    t_block_list = []
    t_update_v2_list = []
    t_update_v3_list = []
    per_block_records = []  # [(block, t_block, t_update_v2, t_update_v3), ...]

    try:
        last_processed = w3.eth.block_number - 1
    except Exception as e:
        print(f"无法获取最新区块号: {e}")
        return

    print(f"Start streaming from block {last_processed + 1} (single get_logs, V2+V3 parallel, fast_mode)")
    processed = 0
    run_start = time.time()

    while processed < max_blocks:
        try:
            latest = w3.eth.block_number
        except Exception as e:
            print(f"获取最新区块号失败: {e}")
            time.sleep(poll_interval)
            continue

        if latest <= last_processed:
            time.sleep(poll_interval)
            continue

        for blk in range(last_processed + 1, latest + 1):
            # Step 1: get_block + get_logs (ONE RPC for logs instead of two)
            block = w3.eth.get_block(blk)
            t_received = time.time()
            t_block = t_received - block.timestamp

            filter_params = {
                'fromBlock': hex(blk),
                'toBlock': hex(blk),
                'topics': [list(ALL_TOPICS)],
            }
            all_logs = w3.eth.get_logs(filter_params)

            # Step 2: V2 and V3 in parallel (parse + write, no get_logs, fast_mode)
            with ThreadPoolExecutor(max_workers=2) as ex:
                f_v2 = ex.submit(v2_mod.handle_new_block, blk, w3,
                                 t_received_override=t_received, logs_override=all_logs, fast_mode=True)
                f_v3 = ex.submit(v3_mod.handle_new_block, blk, eth_url,
                                 t_received_override=t_received, logs_override=all_logs, fast_mode=True)
                _, t_update_v2 = f_v2.result()
                _, t_update_v3 = f_v3.result()

            t_block_list.append(t_block)
            if t_update_v2 is not None:
                t_update_v2_list.append(t_update_v2)
            if t_update_v3 is not None:
                t_update_v3_list.append(t_update_v3)
            per_block_records.append((blk, t_block, t_update_v2, t_update_v3))

            v2_s = f"{t_update_v2:.3f}s" if t_update_v2 is not None else "N/A"
            v3_s = f"{t_update_v3:.3f}s" if t_update_v3 is not None else "N/A"
            print(f"   [Block {blk}] T_block={t_block:.3f}s | T_update V2={v2_s}, V3={v3_s}")

            processed += 1
            last_processed = blk
            if processed >= max_blocks:
                break

    # Summary
    print("\n" + "=" * 60)
    print("LATENCY SUMMARY (V2 + V3, T_block measured once per block)")
    print("=" * 60)

    if t_block_list:
        print(f"\nT_block (block mined→received, once): avg={sum(t_block_list)/len(t_block_list):.3f}s, "
              f"min={min(t_block_list):.3f}s, max={max(t_block_list):.3f}s, n={len(t_block_list)}")

    if t_update_v2_list:
        print(f"\nT_update V2 (received→state updated): avg={sum(t_update_v2_list)/len(t_update_v2_list):.3f}s, "
              f"min={min(t_update_v2_list):.3f}s, max={max(t_update_v2_list):.3f}s, n={len(t_update_v2_list)}")

    if t_update_v3_list:
        print(f"T_update V3 (received→state updated): avg={sum(t_update_v3_list)/len(t_update_v3_list):.3f}s, "
              f"min={min(t_update_v3_list):.3f}s, max={max(t_update_v3_list):.3f}s, n={len(t_update_v3_list)}")

    print("\n" + "=" * 60)
    print("📁 V2 data: events_v2_new/")
    print("📁 V3 data: events_v3_new/")

    # Write results to txt file
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = "events_combined"
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"latency_{ts}.txt")
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(f"Latency results - {datetime.datetime.now().isoformat()}\n")
        f.write(f"stream_blocks={max_blocks}, poll_interval={poll_interval}s, eth_url={eth_url}\n")
        f.write("=" * 60 + "\n\n")
        f.write("Per-block:\n")
        f.write(f"{'block':<10} {'T_block(s)':<12} {'T_update_V2(s)':<14} {'T_update_V3(s)':<14}\n")
        f.write("-" * 50 + "\n")
        for blk, tb, tu2, tu3 in per_block_records:
            tu2_s = f"{tu2:.3f}" if tu2 is not None else "N/A"
            tu3_s = f"{tu3:.3f}" if tu3 is not None else "N/A"
            f.write(f"{blk:<10} {tb:<12.3f} {tu2_s:<14} {tu3_s:<14}\n")
        f.write("\n" + "=" * 60 + "\n")
        f.write("Summary:\n")
        if t_block_list:
            f.write(f"T_block: avg={sum(t_block_list)/len(t_block_list):.3f}s, min={min(t_block_list):.3f}s, max={max(t_block_list):.3f}s, n={len(t_block_list)}\n")
        if t_update_v2_list:
            f.write(f"T_update V2: avg={sum(t_update_v2_list)/len(t_update_v2_list):.3f}s, min={min(t_update_v2_list):.3f}s, max={max(t_update_v2_list):.3f}s, n={len(t_update_v2_list)}\n")
        if t_update_v3_list:
            f.write(f"T_update V3: avg={sum(t_update_v3_list)/len(t_update_v3_list):.3f}s, min={min(t_update_v3_list):.3f}s, max={max(t_update_v3_list):.3f}s, n={len(t_update_v3_list)}\n")
        f.write(f"\nTotal elapsed: {time.time() - run_start:.1f}s\n")
    print(f"📄 Latency saved to: {out_file}")


def main():
    parser = argparse.ArgumentParser(description="Run V2 and V3 crawlers together, T_block once per block")
    parser.add_argument("--stream_blocks", type=int, default=10, help="流式处理区块数")
    parser.add_argument("--poll_interval", type=float, default=0.2, help="轮询最新区块间隔(秒)，越小检测越快")
    # parser.add_argument("--eth_url", type=str, default="http://127.0.0.1:4291", help="Ethereum node RPC URL")
    parser.add_argument("--eth_url", type=str, default="https://mainnet.infura.io/v3/3cfb4dfb858643278b85b2977df40068", help="Ethereum node RPC URL")
    args = parser.parse_args()

    print("=" * 60)
    print("V2 + V3 combined (T_block measured once per block)")
    print(f"  stream_blocks={args.stream_blocks}, poll_interval={args.poll_interval}s")
    print(f"  eth_url={args.eth_url}")
    print("=" * 60)

    start = time.time()
    stream_combined(args.stream_blocks, args.poll_interval, args.eth_url)
    print(f"\nTotal elapsed: {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
