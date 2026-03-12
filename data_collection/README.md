# Full Data Collection Pipeline

This folder contains a unified pipeline that does exactly this:

1. Take a **full static snapshot** at the current latest block.
2. Start **streaming Uniswap V2 + V3 events** from the next block onward.
3. Save:
   - a **full snapshot every N blocks** (default `10`)
   - parsed per-protocol event outputs (`output/events_v2_new/`, `output/events_v3_new/`)

---

## What It Produces

- Initial full state at current latest block
- Continuous per-block V2+V3 event stream
- Periodic full states every `snapshot_interval` blocks
- A run summary with per-block latency stats

---

## Folder Layout

- `dynamic/`
  - `crawl_events_v2_streaming.py`
  - `crawl_events_v3_streaming.py`
  - `crawl_events_combined_streaming.py`
- `static_block/`
  - Rust snapshot tool (`Cargo.toml`, `src/*`)
- `run_full_pipeline.py`
  - Unified runner
- `output/` (created at runtime)
  - `full_state_every_10_blocks/`
  - `pipeline_logs/`

---

## Prerequisites

### 1) Python

Use Python 3.10+ and install:

```bash
pip install web3 eth_utils pandas
```

### 2) Ethereum RPC

You need a working Ethereum endpoint, for example:

- Infura: `https://mainnet.infura.io/v3/<YOUR_KEY>`
- or your own node URL

### 3) Static snapshot runner (choose one)

`run_full_pipeline.py` supports three ways to run static snapshots:

1. `STATIC_BLOCK_BIN` environment variable (highest priority)
2. Prebuilt binary in:
   - `static_block/target/release/static_block`
   - `static_block/target/debug/static_block`
3. `cargo run` fallback (requires Rust + cargo installed)

If none is available, the script will fail with a clear error.

### 4) Tycho settings (for static snapshot)

Optional environment variables:

- `TYCHO_URL` (default: `tycho-beta.propellerheads.xyz`)
- `TYCHO_API_KEY` (default: `sampletoken`)

---

## Quick Start

From this folder (`contract/data_collection/full`):

```bash
export TYCHO_API_KEY="<your_valid_tycho_key>"
export TYCHO_URL="tycho-beta.propellerheads.xyz"   # optional, this is also the default
cd /data_collection/full
python3 run_full_pipeline.py \
  --eth_url "https://mainnet.infura.io/v3/<YOUR_KEY>" \
  --stream_blocks 50 \
  --poll_interval 0.2 \
  --snapshot_interval 10 \
  --tvl_gt 100
```

---

## Parameters

- `--eth_url`: Ethereum RPC URL
- `--stream_blocks`: number of new blocks to stream
  - `> 0`: stop after N streamed blocks
  - `<= 0`: run forever
- `--poll_interval`: wait time (seconds) when no new blocks
- `--snapshot_interval`: take full snapshot every N streamed blocks
- `--tvl_gt`: TVL threshold used by static snapshot (`static_block`)
- `--allow_dynamic_only`: if static runner is unavailable, continue with streaming only

---

## Output Files

### A) Full snapshot files

Path:

- `output/full_state_every_10_blocks/snapshot_block_{block}.json`

Written:

- once at startup (latest block at bootstrap)
- then every `snapshot_interval` streamed blocks

### B) Pipeline summary

Path:

- `output/pipeline_logs/pipeline_summary_*.json`

Contains:

- per-block `t_block`, `t_update_v2`, `t_update_v3`
- matched event count
- run timing metadata

### C) Protocol-specific outputs

Dynamic modules also write parsed protocol outputs:

- `output/events_v2_new/`
- `output/events_v3_new/`

---

## Run Without Cargo

If `cargo` is not available, do this:

1. Build or obtain `static_block` binary on any machine.
2. Place it somewhere accessible on this machine.
3. Export:

```bash
export STATIC_BLOCK_BIN=/absolute/path/to/static_block
```

4. Run pipeline normally.

Or run without static snapshots:

```bash
python3 run_full_pipeline.py \
  --eth_url "https://mainnet.infura.io/v3/<YOUR_KEY>" \
  --stream_blocks 50 \
  --poll_interval 0.2 \
  --snapshot_interval 10 \
  --tvl_gt 100 \
  --allow_dynamic_only
```

---

## Common Errors

### `FileNotFoundError: cargo`

Cause:

- No `cargo` in current environment, and no static binary found.

Fix:

- install Rust/cargo, or set `STATIC_BLOCK_BIN`.

### Snapshot command fails with Tycho auth/network errors

Check:

- `TYCHO_URL` and `TYCHO_API_KEY`
- outbound network access
- endpoint availability

Example:

```bash
export TYCHO_API_KEY="<your_valid_tycho_key>"
export TYCHO_URL="tycho-beta.propellerheads.xyz"
```

### No new event files appearing

Check:

- RPC endpoint is healthy
- `--eth_url` is correct
- chain is advancing
- process is still running

---

## Recommended First Test

Use a short run first:

```bash
python3 run_full_pipeline.py \
  --eth_url "https://mainnet.infura.io/v3/<YOUR_KEY>" \
  --stream_blocks 5 \
  --poll_interval 0.5 \
  --snapshot_interval 2 \
  --tvl_gt 100
```

Then verify:

- at least one file in `output/full_state_every_10_blocks/`
- files are created under `output/events_v2_new/` and/or `output/events_v3_new/`
- one summary file in `output/pipeline_logs/`
