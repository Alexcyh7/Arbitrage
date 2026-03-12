//! Minimal Tycho RPC wrapper for pool discovery and state-at-block.
//! Standalone: uses only tycho-client and tycho-common.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::anyhow;
use tokio::time::timeout;
use tycho_client::{
    deltas::{DeltasClient, SubscriptionOptions, WsDeltasClient},
    rpc::{HttpRPCClientOptions, RPCClient, RPC_CLIENT_CONCURRENCY},
    HttpRPCClient, RPCError,
};
use tycho_common::{
    dto::{
        BlockParam,
        Chain,
        ExtractorIdentity,
        PaginationParams,
        ProtocolComponentsRequestBody,
        ProtocolStateRequestBody,
        ResponseProtocolState,
        VersionParam,
    },
    Bytes,
};

pub type TychoError = anyhow::Error;

pub struct TychoRpc {
    inner: HttpRPCClient,
}

impl TychoRpc {
    pub fn new(base_url: &str, api_key: Option<&str>) -> Self {
        let opts = HttpRPCClientOptions::new().with_auth_key(api_key.map(|s| s.to_string()));
        let inner = HttpRPCClient::new(base_url, opts)
            .unwrap_or_else(|e| panic!("Failed to create Tycho RPC client: {e}"));
        Self { inner }
    }

    pub async fn protocol_components_all(
        &self,
        protocol_system: &str,
        tvl_gt_native: f64,
    ) -> Result<Vec<tycho_common::dto::ProtocolComponent>, TychoError> {
        let mut body =
            ProtocolComponentsRequestBody::system_filtered(protocol_system, Some(tvl_gt_native), Chain::Ethereum);
        body.pagination = PaginationParams::new(0, 500);
        let res: tycho_common::dto::ProtocolComponentRequestResponse = self
            .inner
            .get_protocol_components_paginated(&body, None, RPC_CLIENT_CONCURRENCY)
            .await
            .map_err(map_rpc_err)?;
        Ok(res.protocol_components)
    }

    pub async fn protocol_state_at_block(
        &self,
        protocol_system: &str,
        block: u64,
        protocol_ids: &[String],
    ) -> Result<Vec<Option<ResponseProtocolState>>, TychoError> {
        if protocol_ids.len() > 100 {
            return Err(anyhow!(
                "protocol_state request supports at most 100 ids per call (got {})",
                protocol_ids.len()
            ));
        }
        let version = if block == 0 {
            VersionParam::default()
        } else {
            VersionParam::new(
                None,
                Some({
                    #[allow(deprecated)]
                    BlockParam {
                        hash: None,
                        chain: Some(Chain::Ethereum),
                        number: Some(block as i64),
                    }
                }),
            )
        };

        let body = ProtocolStateRequestBody {
            protocol_ids: Some(protocol_ids.to_vec()),
            protocol_system: protocol_system.to_string(),
            chain: Chain::Ethereum,
            include_balances: true,
            version,
            pagination: Default::default(),
        };
        let res: tycho_common::dto::ProtocolStateRequestResponse = self
            .inner
            .get_protocol_states(&body)
            .await
            .map_err(map_rpc_err)?;
        let mut by_id: HashMap<String, ResponseProtocolState> =
            res.states.into_iter().map(|s| (s.component_id.clone(), s)).collect();
        Ok(protocol_ids.iter().map(|id| by_id.remove(id)).collect())
    }
}

fn map_rpc_err(err: RPCError) -> TychoError {
    anyhow!(err.to_string())
}

#[derive(Clone)]
pub struct PoolRef {
    pub component_id: String,
    pub component: tycho_common::dto::ProtocolComponent,
}

pub async fn discover_all_pools(
    rpc: &TychoRpc,
    protocol_system: &str,
    tvl_gt_native: f64,
    token_allowlist: Option<&[Bytes]>,
) -> Result<Vec<PoolRef>, TychoError> {
    let components = rpc
        .protocol_components_all(protocol_system, tvl_gt_native)
        .await?;

    let allowlist_set: Option<std::collections::HashSet<&Bytes>> =
        token_allowlist.map(|tokens| tokens.iter().collect());

    let mut out = Vec::new();
    'component: for c in components {
        if let Some(ref allowlist) = allowlist_set {
            for t in &c.tokens {
                if !allowlist.contains(t) {
                    continue 'component;
                }
            }
        }
        out.push(PoolRef { component_id: c.id.clone(), component: c });
    }
    Ok(out)
}

pub async fn fetch_states_at_block(
    rpc: &TychoRpc,
    protocol_system: &str,
    block: u64,
    component_ids: &[String],
) -> Result<Vec<Option<ResponseProtocolState>>, TychoError> {
    const CHUNK_SIZE: usize = 100;

    if component_ids.is_empty() {
        return Ok(vec![]);
    }
    let mut out = Vec::with_capacity(component_ids.len());
    for chunk in component_ids.chunks(CHUNK_SIZE) {
        let chunk_vec: Vec<String> = chunk.to_vec();
        let states = rpc
            .protocol_state_at_block(protocol_system, block, &chunk_vec)
            .await?;
        out.extend(states);
    }
    Ok(out)
}

pub async fn resolve_latest_processed_block(
    tycho_host: &str,
    tycho_api_key: Option<&str>,
    extractor_name: &str,
) -> Result<u64, TychoError> {
    let host = tycho_host
        .trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/');
    let ws_url = format!("wss://{host}");

    let ws = WsDeltasClient::new(&ws_url, tycho_api_key)
        .map_err(|e| anyhow!("ws client: {e}"))?;
    let _jh = ws
        .connect()
        .await
        .map_err(|e| anyhow!("ws connect: {e}"))?;

    let extractor_id = ExtractorIdentity::new(Chain::Ethereum, extractor_name);
    let (_sub_id, mut rx) = ws
        .subscribe(extractor_id, SubscriptionOptions::new().with_state(false))
        .await
        .map_err(|e| anyhow!("ws subscribe: {e}"))?;

    let msg = timeout(Duration::from_secs(15), rx.recv())
        .await
        .map_err(|_| anyhow!("ws timeout waiting for block"))?
        .ok_or_else(|| anyhow!("ws channel closed"))?;

    Ok(msg.block.number)
}
