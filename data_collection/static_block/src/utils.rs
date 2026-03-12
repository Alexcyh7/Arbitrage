//! Minimal helpers for Tycho URL (Ethereum).

use tycho_common::dto::Chain;

/// Default Tycho host for the given chain. This crate supports Ethereum only.
pub fn get_default_url(chain: &Chain) -> Option<String> {
    match chain {
        Chain::Ethereum => Some("tycho-beta.propellerheads.xyz".to_string()),
        _ => None,
    }
}
