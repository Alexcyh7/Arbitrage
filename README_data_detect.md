# Arbitrage

End-to-end arbitrage workflow on Ethereum Uniswap V2/V3:

1. collect data (`data_collection/run_full_pipeline.py`)
2. detect cycles (`detection/build/detect`)
3. orchestrate both with one command (`examples/run_data_detection.py`)

---

## 1) Prerequisites

- Linux + Python 3.10+
- C++ toolchain: `cmake`, `make`, `g++`
- Python packages:
  - `web3`
  - `eth_utils`
  - `pandas`
- Rust/cargo (for static snapshot in `data_collection/static_block`)

Install Python deps:

```bash
pip install web3 eth_utils pandas
```

Build detector once:

```bash
cd /Arbitrage/detection
rm -rf build
cmake -S . -B build
cmake --build build -j$(nproc)
```

---

## 2) Environment Variables (export)

Run these before starting:

```bash
export TYCHO_API_KEY="<your_valid_tycho_key>"
export TYCHO_URL="tycho-beta.propellerheads.xyz"   # optional, default is this
export ETH_URL="https://mainnet.infura.io/v3/<your_infura_key>"
```

If `cargo` is not in current shell (optional fix):

```bash
export PATH="$HOME/.cargo/bin:$PATH"
```

If you do not want to use `cargo run` for static snapshot, provide binary directly (optional):

```bash
export STATIC_BLOCK_BIN="/absolute/path/to/static_block"
```

---

## 3) One-Command Full Run (Collection + Detection)

This is the main entrypoint and recommended mode.

```bash
cd /Arbitrage
python3 examples/run_data_detection.py \
  --eth_url "$ETH_URL" \
  --stream_blocks 50 \
  --poll_interval 0.2 \
  --snapshot_interval 10 \
  --tvl_gt 100 \
  --k 3 \
  --seed 42 \
  --quote_size_eth 0.1
```

Behavior:

- when a new full snapshot block arrives, detector resets and runs static cycle detection
- when each block’s V2/V3 events arrive, detector runs dynamic arbitrage detection immediately

Outputs:

- `cycles_results/static/static_result_block_{block}.json`
- `cycles_results/dynamic/dynamic_result_block_{block}.json`

---

## 4) Run in Two Separate Steps

### A. Run Data Collection Only

```bash
cd /Arbitrage/data_collection
python3 run_full_pipeline.py \
  --eth_url "$ETH_URL" \
  --stream_blocks 50 \
  --poll_interval 0.2 \
  --snapshot_interval 10 \
  --tvl_gt 100
```

If static snapshot may fail but you still want dynamic event files:

```bash
python3 run_full_pipeline.py \
  --eth_url "$ETH_URL" \
  --stream_blocks 50 \
  --poll_interval 0.2 \
  --snapshot_interval 10 \
  --tvl_gt 100 \
  --allow_dynamic_only
```

Collection outputs:

- `data_collection/output/full_state_every_10_blocks/`
- `data_collection/output/events_v2_new/`
- `data_collection/output/events_v3_new/`
- `data_collection/output/pipeline_logs/`

### B. Run Detection Only (Replay Existing Data)

```bash
cd /Arbitrage
python3 examples/run_data_detection.py --skip_collection
```

Custom output folder:

```bash
python3 examples/run_data_detection.py \
  --skip_collection \
  --results_dir "/Arbitrage/cycles_results_custom"
```

If you explicitly want old sequential behavior (collect first, then replay):

```bash
python3 examples/run_data_detection.py --sequential --eth_url "$ETH_URL"
```

---

## 5) Common Errors and Fixes

- `401 Unauthorized ... infura ... <YOUR_KEY>`
  - `--eth_url` still contains placeholder; replace with real key
- `Invalid authentication key` (Tycho)
  - set valid `TYCHO_API_KEY`
- `Exec format error: detection/build/detect`
  - binary architecture mismatch; rebuild on this Linux machine
- `No static snapshot runner available`
  - install Rust/cargo or set `STATIC_BLOCK_BIN`

---

## 6) Minimal Smoke Test

```bash
cd /Arbitrage
python3 examples/run_data_detection.py \
  --eth_url "$ETH_URL" \
  --stream_blocks 5 \
  --poll_interval 0.5 \
  --snapshot_interval 2 \
  --tvl_gt 100
```
