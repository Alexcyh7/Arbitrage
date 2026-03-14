# Backtest Guide

This document explains how to run and interpret the backtest workflow in this repository.

## Scope

Current backtest-related entry scripts:

- `examples/backtest_next_block_positions.py`
- `examples/run_dynamic_detection_with_backtest.py`
- `examples/run_quote_sweep_backtest.py`

They are standalone utilities and do not require changing the main detector logic.

---

## 1) Single-block next-block position backtest

Script:

- `examples/backtest_next_block_positions.py`

Purpose:

- For detection block `B`, evaluate the detected route in block `B+1` at:
  - `top` (before any new events in `B+1`)
  - `middle` (after first half of events in `B+1`)
  - `bottom` (after all events in `B+1`)

Run:

```bash
cd /home/bingqiao/contract/Arbitrage-main
python examples/backtest_next_block_positions.py --block 24610002
```

Output:

- `cycles_results/backtest_next_block/next_block_position_backtest_<B>.json`

Notes:

- `positions.top` is aligned with detector route output to ensure consistency with online simulation.
- `positions.top_recomputed` stores the Python recomputed top result for drift diagnostics.

---

## 2) Detection + multi-block backtest in one run

Script:

- `examples/run_dynamic_detection_with_backtest.py`

Purpose:

- Run dynamic detection first, then run next-block top/middle/bottom backtest for selected blocks.

Run (full pipeline):

```bash
cd /home/bingqiao/contract/Arbitrage-main
python examples/run_dynamic_detection_with_backtest.py \
  --algorithm hp-index \
  --eth_url "http://127.0.0.1:4291" \
  --start_block 24610000 \
  --stream_blocks 20 \
  --quote_size_eth 0.01 \
  --k 3 \
  --port 12001
```

Run (backtest only, using existing dynamic results):

```bash
python examples/run_dynamic_detection_with_backtest.py \
  --backtest_only \
  --backtest_block_start 24610001 \
  --backtest_block_end 24610020 \
  --gas_units 220000 \
  --gas_price_gwei 8
```

Key outputs:

- Per block:
  - `next_block_position_backtest_<B>.json`
- Batch:
  - `batch_backtest_summary.json`

Summary fields include:

- `profit_pct` for `top/middle/bottom`
- `net_profit_pct_after_gas` (when start token is WETH)
- `divergence`:
  - `has_divergence`
  - `kind` (`hop_output`, `hop_count`, `final_to_amount`)
  - `first_diff_hop` (1-based hop index where top/middle/bottom first diverge)

---

## Algorithm selection (`--algorithm`)

Available options:

- `--algorithm color-coding`
- `--algorithm hp-index`

### `hp-index` (recommended default for replay/backtest)

Use when:

- You want stable and deterministic behavior across runs.
- You run long block windows and need consistent comparisons.
- You care about reproducibility for quote sweep experiments.

Key parameters:

- `--k`: cycle length parameter (default `3`)
- `--hp_threshold`: hot-point threshold (default `10`)

Practical notes:

- Usually better for structured offline replay and summary analysis.
- Lower `hp_threshold` can increase sensitivity but may increase noise/work.

### `color-coding`

Use when:

- You want exploratory search with randomized behavior.
- You are testing whether randomized sampling discovers extra candidates.

Practical notes:

- It is randomized and can vary run-to-run.
- For strict A/B comparisons across quote sizes, this adds variance.

### Quick recommendation

For most backtest/sweep workflows in this repo:

- Start with `--algorithm hp-index --k 3 --hp_threshold 10`
- Only switch to `color-coding` for exploratory runs.

---

## 3) Quote-size sweep backtest

Script:

- `examples/run_quote_sweep_backtest.py`

Purpose:

- Sweep multiple quote sizes on the same block window.
- Group by snapshot interval (for example, 10 blocks per group means `full state + 9 dynamic`).
- Save results to a dedicated folder named with block range.

Example:

```bash
cd /home/bingqiao/contract/Arbitrage-main
python examples/run_quote_sweep_backtest.py \
  --start_block 24620000 \
  --stream_blocks 100 \
  --group_size 10 \
  --quote_sizes "0.01,0.05,0.1" \
  --algorithm hp-index \
  --k 3 \
  --base_port 12001 \
  --gas_units 220000 \
  --gas_price_gwei 8
```

Output layout:

- `cycles_results/quote_sweep/blocks_<start>_<end>/quote_0p01/...`
- `cycles_results/quote_sweep/blocks_<start>_<end>/quote_0p05/...`
- `cycles_results/quote_sweep/blocks_<start>_<end>/quote_0p1/...`
- Manifest:
  - `cycles_results/quote_sweep/blocks_<start>_<end>/sweep_manifest.json`

Important behavior:

- If one quote run has some failed blocks, sweep does **not** stop; it continues to next quote.
- Per-quote `exit_code` is written to `sweep_manifest.json`.

---

## Gas assumptions and interpretation

Backtest summary uses:

- `gas_units`: estimated gas usage
- `gas_price_gwei`: estimated effective gas price

Cost formula:

- `gas_cost_eth = gas_units * gas_price_gwei * 1e-9`

For example:

- `220000 @ 8 gwei` -> `0.00176 ETH`

`net_profit_pct_after_gas` is computed as:

- `profit_pct - gas_cost_pct_of_input`

This is currently computed only when route start token is WETH.

---

## Common messages

### `dynamic_result_block_<B> has no route fills`

Meaning:

- Detector did not output a simulatable route for block `B` (commonly non-negative weight / not profitable).

Impact:

- That block is marked failed in backtest summary.
- Other blocks still continue.

### `Client disconnected`

Meaning:

- TCP client (orchestrator) closed the connection after sending/receiving a batch.
- This is usually normal unless followed by a hard error.

---

## Suggested baseline parameters

For small input (`0.01 WETH`), a practical baseline:

- `gas_units=220000`
- `gas_price_gwei=8`

For sensitivity checks:

- optimistic: `160000 @ 6 gwei`
- neutral: `200000 @ 8 gwei`
- conservative: `260000 @ 12 gwei`

