use casper_json_rpc::Error as RpcError;
use casper_types::{Chainspec, EvmConfig};
use serde::Deserialize;

use super::{super::NodeClient, types::internal_error};

/// Tolerant view of just the chainspec's `[evm]` section, used as a fallback when the full
/// `Chainspec` fails to deserialize (e.g. sidecar / node version skew).
#[derive(Deserialize)]
struct ChainspecEvmConfig {
    evm: EvmConfig,
}

pub(super) async fn read_evm_config(node_client: &dyn NodeClient) -> Result<EvmConfig, RpcError> {
    // `read_chainspec_bytes` is served from `NodeStateCache` (no binary port round-trip) once the
    // cache is hydrated, so a single fetch here is cheap. Parse the whole `Chainspec` when we can;
    // fall back to just the `[evm]` section otherwise.
    let raw = node_client
        .read_chainspec_bytes()
        .await
        .map_err(internal_error)?;
    let text = std::str::from_utf8(raw.chainspec_bytes())
        .map_err(|error| internal_error(format!("invalid chainspec bytes: {error}")))?;
    if let Ok(chainspec) = toml::from_str::<Chainspec>(text) {
        return Ok(chainspec.evm_config);
    }
    let parsed = toml::from_str::<ChainspecEvmConfig>(text)
        .map_err(|error| internal_error(format!("invalid chainspec toml: {error}")))?;
    Ok(parsed.evm)
}
