#!/usr/bin/env python3
"""
Backtest detected cycle at next block positions.

For a detection result at block B, this script:
1) Reconstructs pool state at end of block B (starting from snapshot_block_in_use).
2) Applies block B+1 events at three positions:
   - top:    apply 0% of B+1 events, then execute cycle
   - middle: apply 50% of B+1 events, then execute cycle
   - bottom: apply 100% of B+1 events, then execute cycle
3) Simulates route execution and writes a JSON report.

This is a standalone backtest utility and does not modify existing runtime logic.
"""

import argparse
import copy
import glob
import json
import os
import re
import sys
from typing import Dict, List, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from simulator import get_v2_amount_out_with_pool_state, get_v3_amount_out_with_pool_state


DEFAULT_DYNAMIC_DIR = os.path.join(ROOT, "cycles_results", "dynamic")
DEFAULT_EVENTS_V2_DIR = os.path.join(ROOT, "data_collection", "output", "events_v2_new")
DEFAULT_EVENTS_V3_DIR = os.path.join(ROOT, "data_collection", "output", "events_v3_new")
DEFAULT_SNAPSHOT_DIR = os.path.join(ROOT, "data_collection", "output", "full_state_every_10_blocks")
DEFAULT_OUT_DIR = os.path.join(ROOT, "cycles_results", "backtest_next_block")

_RE_V2 = re.compile(r"uniswap_events_block_(\d+)_")
_RE_V3 = re.compile(r"uniswap_v3_events_block_(\d+)_")
_RE_DYN = re.compile(r"dynamic_result_block_(\d+)\.json")


def _hex_to_int(h: str) -> int:
    raw = int(h, 16)
    hex_digits = h[2:] if h.startswith(("0x", "0X")) else h
    if len(hex_digits) % 2 != 0:
        hex_digits = "0" + hex_digits
    bits = len(hex_digits) * 4
    if raw >= (1 << (bits - 1)):
        raw -= 1 << bits
    return raw


def _int_to_hex(v: int) -> str:
    if v >= 0:
        return hex(v)
    bits = max(8, (abs(v).bit_length() + 8))
    bits = ((bits + 7) // 8) * 8
    return hex((1 << bits) + v)


def _load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _index_latest_by_block(directory: str, regex: re.Pattern) -> Dict[int, str]:
    indexed: Dict[int, str] = {}
    for path in glob.glob(os.path.join(directory, "*.json")):
        name = os.path.basename(path)
        m = regex.search(name)
        if not m:
            continue
        blk = int(m.group(1))
        if blk not in indexed or os.path.getmtime(path) > os.path.getmtime(indexed[blk]):
            indexed[blk] = path
    return indexed


def _pool_address_from_event(ev: dict) -> str:
    return (ev.get("pair_address") or ev.get("pool_address") or "").lower()


def _event_sort_key(ev: dict) -> Tuple[int, int]:
    return (int(ev.get("transaction_index", 1 << 30)), int(ev.get("log_index", 1 << 30)))


def _apply_v2_event_to_pool(pool: dict, parsed_event: dict) -> None:
    if parsed_event.get("event_type") != "Sync":
        return
    if "reserve0" not in parsed_event or "reserve1" not in parsed_event:
        return
    reserve0 = int(parsed_event["reserve0"])
    reserve1 = int(parsed_event["reserve1"])
    attrs = pool["state"]["attributes"]
    attrs["reserve0"] = hex(reserve0)
    attrs["reserve1"] = hex(reserve1)
    balances = pool["state"].setdefault("balances", {})
    t0, t1 = pool["component"]["tokens"][0], pool["component"]["tokens"][1]
    balances[t0] = hex(reserve0)
    balances[t1] = hex(reserve1)


def _apply_v3_tick_delta(attrs: dict, tick_idx: int, delta: int) -> None:
    key = f"ticks/{tick_idx}/net-liquidity"
    prev = _hex_to_int(attrs[key]) if key in attrs else 0
    attrs[key] = _int_to_hex(prev + delta)


def _apply_v3_event_to_pool(pool: dict, parsed_event: dict) -> None:
    attrs = pool["state"]["attributes"]
    et = parsed_event.get("event_type")

    # Keep runtime state aligned when available.
    if all(k in parsed_event for k in ("sqrtPriceX96", "liquidity", "tick")):
        attrs["sqrt_price_x96"] = hex(int(parsed_event["sqrtPriceX96"]))
        attrs["liquidity"] = hex(int(parsed_event["liquidity"]))
        attrs["tick"] = _int_to_hex(int(parsed_event["tick"]))

    # Apply Mint/Burn net-liquidity deltas on tick boundaries.
    if et in ("Mint", "Burn") and all(k in parsed_event for k in ("tick_lower", "tick_upper", "amount")):
        tick_lower = int(parsed_event["tick_lower"])
        tick_upper = int(parsed_event["tick_upper"])
        amount = int(parsed_event["amount"])
        # Guard against malformed historical files where int24 decoding failed.
        if abs(tick_lower) > 1_000_000 or abs(tick_upper) > 1_000_000:
            return
        sign = 1 if et == "Mint" else -1
        _apply_v3_tick_delta(attrs, tick_lower, sign * amount)
        _apply_v3_tick_delta(attrs, tick_upper, -sign * amount)


def _merge_block_events(v2_file: str | None, v3_file: str | None) -> List[dict]:
    events = []
    if v2_file and os.path.exists(v2_file):
        events.extend(_load_json(v2_file).get("events", []))
    if v3_file and os.path.exists(v3_file):
        events.extend(_load_json(v3_file).get("events", []))
    events.sort(key=_event_sort_key)
    return events


def _apply_events(state_by_pool: Dict[str, dict], events: List[dict]) -> int:
    applied = 0
    for ev in events:
        pool_addr = _pool_address_from_event(ev)
        if not pool_addr:
            continue
        pool = state_by_pool.get(pool_addr)
        if pool is None:
            continue
        parsed = ev.get("parsed_event", {})
        if not isinstance(parsed, dict):
            continue

        is_v3 = pool.get("protocol_system") == "uniswap_v3"
        if is_v3:
            _apply_v3_event_to_pool(pool, parsed)
            # Count only events that can affect state.
            if parsed.get("event_type") in ("Swap", "Mint", "Burn", "Initialize"):
                applied += 1
        else:
            before = pool["state"]["attributes"].get("reserve0"), pool["state"]["attributes"].get("reserve1")
            _apply_v2_event_to_pool(pool, parsed)
            after = pool["state"]["attributes"].get("reserve0"), pool["state"]["attributes"].get("reserve1")
            if before != after:
                applied += 1
    return applied


def _simulate_route(route: dict, state_by_pool: Dict[str, dict]) -> dict:
    fills = route.get("route", {}).get("fills", [])
    from_amount = int(route.get("fromAmount", "0"))
    cur = from_amount
    hop_results = []

    for i, fill in enumerate(fills, start=1):
        pool_addr = fill["pool"].lower()
        pool = state_by_pool.get(pool_addr)
        if pool is None:
            return {
                "ok": False,
                "error": f"pool_not_found:{pool_addr}",
                "failed_hop": i,
                "hop_results": hop_results,
            }

        src = fill["from"]
        source = fill.get("source", "")
        if source == "Uniswap_V2":
            out, updated = get_v2_amount_out_with_pool_state(pool, src, cur)
        else:
            out, updated = get_v3_amount_out_with_pool_state(pool, src, cur)

        state_by_pool[pool_addr] = updated
        hop_results.append(
            {
                "hop": i,
                "pool": pool_addr,
                "source": source,
                "from": src,
                "to": fill["to"],
                "input": str(cur),
                "output": str(out),
            }
        )
        cur = out

    profit = cur - from_amount
    profit_pct = (profit / from_amount * 100.0) if from_amount > 0 else 0.0
    return {
        "ok": True,
        "from_amount": str(from_amount),
        "to_amount": str(cur),
        "profit": str(profit),
        "profit_pct": profit_pct,
        "profitable": profit > 0,
        "hop_results": hop_results,
    }


def _top_result_from_detector(route: dict) -> dict:
    """
    Build top-position result directly from detector route payload.
    This keeps top exactly aligned with online detector simulation output.
    """
    fills = route.get("route", {}).get("fills", [])
    from_amount = int(route.get("fromAmount", "0"))
    to_amount = int(route.get("toAmount", "0"))
    profit = to_amount - from_amount

    hop_results = []
    cur_in = from_amount
    for i, fill in enumerate(fills, start=1):
        out = int(fill.get("expected_output", "0"))
        hop_results.append(
            {
                "hop": i,
                "pool": fill["pool"].lower(),
                "source": fill.get("source", ""),
                "from": fill["from"],
                "to": fill["to"],
                "input": str(cur_in),
                "output": str(out),
            }
        )
        cur_in = out

    return {
        "ok": True,
        "from_amount": str(from_amount),
        "to_amount": str(to_amount),
        "profit": str(profit),
        "profit_pct": float(route.get("profitPct", 0.0)),
        "profitable": bool(route.get("profitable", profit > 0)),
        "hop_results": hop_results,
        "events_applied_before_exec": 0,
        "events_considered_before_exec": 0,
        "source": "detector_route",
    }


def _state_from_snapshot(snapshot_path: str) -> Dict[str, dict]:
    data = _load_json(snapshot_path)
    by_pool = {}
    for entry in data:
        pool_addr = entry.get("component", {}).get("id", "").lower()
        static_addr = (
            entry.get("component", {})
            .get("static_attributes", {})
            .get("pool_address", "")
            .lower()
        )
        if static_addr:
            pool_addr = static_addr
        if pool_addr:
            by_pool[pool_addr] = entry
    return by_pool


def run_backtest_for_block(
    block: int,
    dynamic_dir: str,
    events_v2_dir: str,
    events_v3_dir: str,
    snapshot_dir: str,
    out_dir: str,
) -> str:
    dyn_file = os.path.join(dynamic_dir, f"dynamic_result_block_{block}.json")
    if not os.path.exists(dyn_file):
        raise FileNotFoundError(f"dynamic result not found: {dyn_file}")
    dyn = _load_json(dyn_file)

    detector_resp = dyn.get("detector_response") or {}
    route = detector_resp.get("route") or {}
    fills = route.get("route", {}).get("fills", [])
    if not fills:
        raise RuntimeError(f"dynamic_result_block_{block} has no route fills")

    snapshot_block = int(dyn["snapshot_block_in_use"])
    snapshot_path = os.path.join(snapshot_dir, f"snapshot_block_{snapshot_block}.json")
    if not os.path.exists(snapshot_path):
        raise FileNotFoundError(f"snapshot not found: {snapshot_path}")

    v2_by_block = _index_latest_by_block(events_v2_dir, _RE_V2)
    v3_by_block = _index_latest_by_block(events_v3_dir, _RE_V3)

    # 1) Rebuild end-of-block-B state from snapshot_block_in_use.
    state_eob = _state_from_snapshot(snapshot_path)
    replay_applied = 0
    for b in range(snapshot_block + 1, block + 1):
        merged = _merge_block_events(v2_by_block.get(b), v3_by_block.get(b))
        replay_applied += _apply_events(state_eob, merged)

    # 2) Prepare next block events and three positions.
    next_block = block + 1
    next_events = _merge_block_events(v2_by_block.get(next_block), v3_by_block.get(next_block))
    mid_idx = len(next_events) // 2

    position_slices = {
        "top": next_events[:0],
        "middle": next_events[:mid_idx],
        "bottom": next_events,
    }

    results = {}
    for pos, evs in position_slices.items():
        state_pos = copy.deepcopy(state_eob)
        applied = _apply_events(state_pos, evs)
        sim = _simulate_route(route, state_pos)
        sim["events_applied_before_exec"] = applied
        sim["events_considered_before_exec"] = len(evs)
        results[pos] = sim

    # Keep top exactly identical to detector's own simulation result.
    # Preserve recomputed top for debugging drift between Python/C++ math paths.
    results["top_recomputed"] = results["top"]
    results["top"] = _top_result_from_detector(route)

    report = {
        "block": block,
        "next_block": next_block,
        "snapshot_block_in_use": snapshot_block,
        "snapshot_file": snapshot_path,
        "dynamic_result_file": dyn_file,
        "route_from_detector": {
            "weight": detector_resp.get("weight"),
            "profitable_flag": detector_resp.get("profitable"),
            "from": route.get("from"),
            "fromAmount": route.get("fromAmount"),
            "hops": fills,
        },
        "replay_to_block_applied_events": replay_applied,
        "next_block_total_events": len(next_events),
        "positions": results,
    }

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"next_block_position_backtest_{block}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    return out_path


def parse_args():
    p = argparse.ArgumentParser(description="Backtest detected cycle at next-block top/middle/bottom positions.")
    p.add_argument("--block", type=int, required=True, help="Detection block number B (uses dynamic_result_block_B.json).")
    p.add_argument("--dynamic_dir", default=DEFAULT_DYNAMIC_DIR, help="Directory containing dynamic_result_block_*.json")
    p.add_argument("--events_v2_dir", default=DEFAULT_EVENTS_V2_DIR, help="Directory containing V2 event json files.")
    p.add_argument("--events_v3_dir", default=DEFAULT_EVENTS_V3_DIR, help="Directory containing V3 event json files.")
    p.add_argument("--snapshot_dir", default=DEFAULT_SNAPSHOT_DIR, help="Directory containing snapshot_block_*.json")
    p.add_argument("--out_dir", default=DEFAULT_OUT_DIR, help="Output directory for backtest reports.")
    return p.parse_args()


def main():
    args = parse_args()
    out_path = run_backtest_for_block(
        block=args.block,
        dynamic_dir=args.dynamic_dir,
        events_v2_dir=args.events_v2_dir,
        events_v3_dir=args.events_v3_dir,
        snapshot_dir=args.snapshot_dir,
        out_dir=args.out_dir,
    )
    print(f"Backtest report written: {out_path}")


if __name__ == "__main__":
    main()

