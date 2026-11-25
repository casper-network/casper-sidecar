use casper_json_rpc::Error as RpcError;
use casper_types::{Chainspec, EvmConfig};
use serde::{Deserialize, Deserializer};

use super::{super::NodeClient, types::internal_error};

/// Tolerant view of just the chainspec's `[evm]` section, used as a fallback when the full
/// `Chainspec` fails to deserialize (e.g. sidecar / node version skew).
#[derive(Deserialize)]
struct ChainspecEvmConfig {
    #[serde(deserialize_with = "deserialize_evm_config")]
    evm: EvmConfig,
}

pub(super) fn deserialize_evm_config<'de, D>(deserializer: D) -> Result<EvmConfig, D::Error>
where
    D: Deserializer<'de>,
{
    let mut config = toml::Table::deserialize(deserializer)?;
    // Older node chainspecs omitted the preinstall list when it was empty.
    config
        .entry("preinstalls")
        .or_insert_with(|| toml::Value::Array(Vec::new()));
    config.try_into().map_err(serde::de::Error::custom)
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
