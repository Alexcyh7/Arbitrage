//! Standalone binary: crawl Uniswap V2 and V3 pool snapshots at a given block.
//! Writes snapshot_block_{N}.json with component + raw state per pool.

use std::env;

mod tycho_rpc;
mod utils;

use clap::Parser;
use serde::Serialize;
use tracing_subscriber::EnvFilter;
use tycho_common::dto::{Chain, ResponseProtocolState};

use crate::tycho_rpc::{
    discover_all_pools,
    fetch_states_at_block,
    resolve_latest_processed_block,
    TychoRpc,
};
use crate::utils::get_default_url;

#[derive(Parser)]
struct Cli {
    /// Minimum TVL (in native units) for pools to include.
    #[arg(long, default_value_t = 100.0)]
    tvl_gt: f64,

    /// Block number for the snapshot. 0 means "latest processed" by Tycho (via WebSocket).
    #[arg(long, default_value_t = 0)]
    block: u64,
}

#[derive(Serialize)]
struct SnapshotEntry {
    block: u64,
    protocol_system: String,
    component_id: String,
    component: tycho_common::dto::ProtocolComponent,
    state: ResponseProtocolState,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let chain = Chain::Ethereum;

    let tycho_url = env::var("TYCHO_URL").unwrap_or_else(|_| {
        get_default_url(&chain).unwrap_or_else(|| panic!("Unknown URL for chain {chain:?}"))
    });

    let tycho_api_key: String =
        env::var("TYCHO_API_KEY").unwrap_or_else(|_| "sampletoken".to_string());

    let base_url = format!("https://{tycho_url}");
    let rpc = TychoRpc::new(&base_url, Some(tycho_api_key.as_str()));

    let effective_block = if cli.block == 0 {
        println!("Resolving latest processed block via Tycho WebSocket...");
        resolve_latest_processed_block(&tycho_url, Some(tycho_api_key.as_str()), "uniswap_v3")
            .await
            .unwrap_or(0)
    } else {
        cli.block
    };

    println!("Taking RPC snapshot at block {effective_block}...");

    let mut snapshot_entries: Vec<SnapshotEntry> = Vec::new();

    for protocol_system in ["uniswap_v2", "uniswap_v3"] {
        let pools = discover_all_pools(&rpc, protocol_system, cli.tvl_gt, None)
            .await
            .expect("Failed to discover pools");

        if pools.is_empty() {
            println!(
                "[{protocol_system}] No pools found above TVL threshold {}.",
                cli.tvl_gt
            );
            continue;
        }

        let component_ids: Vec<String> = pools.iter().map(|p| p.component_id.clone()).collect();
        let states = fetch_states_at_block(&rpc, protocol_system, effective_block, &component_ids)
            .await
            .expect("Failed to fetch states at block");

        for (pool, state_opt) in pools.iter().zip(states.iter()) {
            if let Some(state) = state_opt {
                snapshot_entries.push(SnapshotEntry {
                    block: effective_block,
                    protocol_system: protocol_system.to_string(),
                    component_id: pool.component_id.clone(),
                    component: pool.component.clone(),
                    state: state.clone(),
                });
            }
        }

        let count = states.iter().filter_map(|s| s.as_ref()).count();
        println!("[{protocol_system}] Snapshot: {count} pools with state.");
    }

    let total = snapshot_entries.len();
    println!("Snapshot completed at block {effective_block}. Total entries: {total}.");

    let snapshot_path = format!("snapshot_block_{effective_block}.json");
    let snapshot_file = std::fs::File::create(&snapshot_path)?;
    serde_json::to_writer_pretty(snapshot_file, &snapshot_entries)?;
    println!("Wrote snapshot JSON to {snapshot_path}");

    Ok(())
}
