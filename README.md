# Crypto Arbitrage Detection

Real-time crypto arbitrage detection system for Ethereum DEX pools (Uniswap V2/V3). The system loads on-chain pool snapshots, constructs a weighted token graph, detects negative-weight cycles (arbitrage opportunities), and simulates the actual swap path to produce executable route JSON.

## Architecture

```
Snapshot JSON ──> C++ Detection Engine ──> Route JSON
                       |  ^
                       |  |  (TCP, per-pool state updates)
                       v  |
               Python Orchestrator
              (multi-process, threaded)
```

**Three stages:**
1. **Graph Construction** — Load pool data, compute quote sizes (ETH-denominated), build weighted directed graph where edge weight = `-log(output / quote_size)`
2. **Cycle Detection** — Two algorithms available:
   - **Color-coding DP** (randomized) — k-cycle color-coding finds negative-weight cycles
   - **HP-Index** (deterministic, based on [GraphS, VLDB 2018](https://doi.org/10.14778/3236187.3236217)) — Hot Point index precomputes paths between high-degree vertices for faster cycle search
3. **Swap Simulation** — Walk the detected cycle with actual token amounts through the pool math (V2 constant-product / V3 tick-based), output route JSON

## Project Structure

```
arbitrage/
├── simulator.py                # Python V2/V3 swap simulator (reference impl)
├── launch_detectors.py         # Launch multiple C++ detector processes
├── arbitrage_client.py         # Python client: send updates, collect best result
├── token_ranking.txt           # Token priority for cycle output ordering
├── detection/                  # Color-coding DP detector
│   ├── main.cpp                # Entry point: load, detect, simulate, TCP server
│   ├── pool.h                  # Pool parsing, V2/V3 swap math (C++)
│   ├── cycle_detector.h/cpp    # k-cycle color-coding DP detection
│   ├── directed_graph.h/cpp    # Weighted directed graph with dynamic edge updates
│   └── CMakeLists.txt          # Build config (fetches nlohmann/json)
├── detection_graphs/           # HP-Index (GraphS) detector
│   ├── main.cpp                # Entry point: same interface as detection/main.cpp
│   ├── hp_index.h/cpp          # Hot Point index build, 3-step search, maintenance
│   ├── pool.h                  # Pool parsing (shared with detection/)
│   ├── directed_graph.h/cpp    # Graph structure (shared with detection/)
│   └── CMakeLists.txt          # Build config
├── examples/
│   ├── run_data_detection.py   # Full pipeline: collection + detection (supports --algorithm)
│   ├── run_demo.py             # Full demo script (static + dynamic + multi-process)
│   ├── pool_update_no_arb.json # Example: unchanged pool state (no arbitrage)
│   ├── pool_update_arb.json    # Example: 2% price shift (creates arbitrage)
│   └── expected_output.txt     # Expected demo output for verification
├── graph/
│   ├── snapshot_block_*.json   # Pool snapshot data (hex-encoded)
│   └── example_output.json     # Example route output format
└── .gitignore
```

## Building

Build both detectors:

```bash
# Color-coding detector
cd detection
mkdir -p build && cd build
cmake .. && make -j$(nproc)
cd ../..

# HP-Index detector
cd detection_graphs
mkdir -p build && cd build
cmake .. && make -j$(nproc)
cd ../..
```

## Usage

### Single Detector (Static)

Detect arbitrage in a snapshot without dynamic updates:

```bash
# Color-coding
./detection/build/detect <snapshot.json> <seed> <quote_size_eth> 0 [k] [num_trials]

# HP-Index
./detection_graphs/build/detect_graphs <snapshot.json> <seed> <quote_size_eth> 0 [k] [hp_threshold]
```

- `seed` — random seed for color-coding (ignored by HP-Index, which is deterministic)
- `quote_size_eth` — amount of ETH to use as reference quote size (e.g. `0.1`)
- `0` — port=0 means no TCP server, exit after detection
- `k` — max cycle length (default 3, supports 2-5)
- `num_trials` — color-coding only: number of random colorings to try (default 1)
- `hp_threshold` — HP-Index only: degree threshold for hot points (default 10)

Example:
```bash
# Color-coding
./detection/build/detect graph/snapshot_block_24589771.json 42 0.1 0 3

# HP-Index
./detection_graphs/build/detect_graphs graph/snapshot_block_24589771.json 42 0.1 0 3 10
```

### Single Detector (Dynamic)

Start a detector that listens for pool state updates on a TCP port:

```bash
# Color-coding
./detection/build/detect graph/snapshot_block_24589771.json 42 0.1 9999 3

# HP-Index
./detection_graphs/build/detect_graphs graph/snapshot_block_24589771.json 42 0.1 9999 3 10
```

Send pool updates (same JSON format as snapshot entries, one per line):
```bash
echo '<pool_json>' | nc localhost 9999
```

The detector responds with a JSON line containing the current best cycle weight, profitability, and full route simulation.

### Multi-Process Detection

Launch multiple detectors with different parameters for better coverage:

```bash
# Launch with default configs (k=2,3,4,5 x seeds=1,2,3,42 = 16 processes)
python launch_detectors.py graph/snapshot_block_24589771.json

# Or with custom configs
python launch_detectors.py graph/snapshot_block_24589771.json \
  --configs '[{"port":10001,"seed":42,"k":2},{"port":10002,"seed":42,"k":3},{"port":10003,"seed":1,"k":3},{"port":10004,"seed":42,"k":5}]'
```

This writes `detectors.json` with port/pid info. Press Ctrl+C to stop all detectors.

Then send updates from the client:

```bash
# Send a pool update to all detectors and get the best result
python arbitrage_client.py pool_update.json detectors.json
```

Or use programmatically:

```python
from arbitrage_client import ArbitrageClient

client = ArbitrageClient.from_detectors_file("detectors.json")
client.connect_all()

result = client.send_update(pool_json_string)
if result["best_result"] and result["best_result"]["profitable"]:
    route = result["best_result"]["route"]
    print(f"Arbitrage found! Profit: {route['profitPct']:.4f}%")

client.close_all()
```

## Pool Snapshot Format

Each pool entry in the snapshot JSON:

```json
{
  "protocol_system": "uniswap_v2" | "uniswap_v3",
  "component": {
    "id": "0x...",
    "tokens": ["0xtoken0", "0xtoken1"],
    "static_attributes": {
      "pool_address": "0x...",
      "fee": "0x1e"
    }
  },
  "state": {
    "attributes": {
      "reserve0": "0x...",
      "reserve1": "0x...",
      "sqrt_price_x96": "0x...",
      "tick": "0x...",
      "liquidity": "0x...",
      "ticks/{idx}/net-liquidity": "0x..."
    }
  },
  "block": 24589771
}
```

- V2 pools: `reserve0`, `reserve1`, `fee` (in 1/10000, e.g. 0x1e=30 -> 0.3%)
- V3 pools: `sqrt_price_x96`, `tick`, `liquidity`, tick map, `fee` (in 1/1000000, e.g. 0xbb8=3000 -> 0.3%)
- All numeric values are hex-encoded. Signed values (tick, net-liquidity) use two's complement.

## Route Output Format

```json
{
  "blockNumber": 24589772,
  "from": "0xtoken_address",
  "to": "0xtoken_address",
  "fromAmount": "212842770",
  "toAmount": "221204330",
  "profit": "8361560",
  "profitPct": 3.9285,
  "profitable": true,
  "route": {
    "fills": [
      {
        "from": "0x...",
        "to": "0x...",
        "pool": "0x...",
        "source": "Uniswap_V3",
        "proportionBps": "10000",
        "expected_output": "212837171948272943104"
      }
    ]
  }
}
```

## Dynamic Update Logic

When a pool state update arrives via TCP:

- **WETH pool**: Recompute the quote size for the non-WETH token (across all its WETH pools), then recalculate all graph edges involving that token
- **Non-WETH pool**: Recompute only the two directed edges for that pool

Changed edges are fed to the detector's incremental DP update (no full recomputation needed).

## Algorithms

### Color-Coding DP (`detection/`)

A **randomized** algorithm:

1. Assign each token a random color from {0, ..., k-1}
2. Find minimum-weight cycles where all nodes have distinct colors (DP on color subsets)
3. Negative-weight cycle = arbitrage (since edge weight = `-log(exchange_rate)`)
4. Multiple seeds improve coverage (each seed tries a different random coloring)

Supports incremental edge updates: when a pool's state changes, only affected DP entries are recomputed.

### HP-Index (`detection_graphs/`)

A **deterministic** algorithm based on the [GraphS paper (VLDB 2018)](https://doi.org/10.14778/3236187.3236217):

1. Identify "hot points" — vertices with degree >= threshold
2. Precompute index paths between hot points (length <= k, no intermediate hot points)
3. On edge update, run 3-step search: forward DFS from target, backward DFS from source, combine via index lookup
4. Exhaustive initial scan finds the globally best negative cycle

Advantages over color-coding: deterministic (no missed cycles due to random coloring), ~2-2.5x faster dynamic updates.

## Token Ranking

Both detectors load `token_ranking.txt` to control cycle output ordering. The cycle is rotated so the highest-priority token appears first. Format (one token per line):

```
0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 WETH
0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 USDC
0xdac17f958d2ee523a2206206994597c13d831ec7 USDT
```

The file is searched next to the snapshot JSON, then in the working directory.
