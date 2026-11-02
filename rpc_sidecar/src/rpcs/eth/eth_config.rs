//! Casper's adaptation of EIP-7910 configuration discovery.

use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, LazyLock},
};

use async_trait::async_trait;
use casper_binary_port::InformationRequest;
use casper_json_rpc::Error as RpcError;
use casper_types::{
    AUCTION_LANE_ID, ActivationPoint, ChainspecRawBytes, EvmConfig, EvmSpec,
    INSTALL_UPGRADE_LANE_ID, MINIMUM_WEI_PER_MOTE, MINT_LANE_ID, ProtocolVersion, evm,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    super::{NodeClient, RpcWithoutParams, docs::DocExample},
    eth_u256::EthU256,
    types::internal_error,
};

/// `eth_config`, using Casper protocol versions and era activation points.
pub struct Config;

/// Current, next and last installed EVM configuration snapshots.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ConfigResult {
    current: ConfigSnapshot,
    next: Option<ConfigSnapshot>,
    last: Option<ConfigSnapshot>,
}

/// A configuration snapshot. Ethereum-only fields unavailable on Casper are null.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ConfigSnapshot {
    activation_time: Option<u64>,
    blob_schedule: Option<BlobSchedule>,
    chain_id: EthU256,
    fork_id: Option<String>,
    precompiles: BTreeMap<String, evm::Address>,
    system_contracts: BTreeMap<String, evm::Address>,
    casper: CasperConfig,
}

/// Ethereum blob parameters, unavailable on Casper.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct BlobSchedule {
    base_fee_update_fraction: u64,
    max: u64,
    target: u64,
}

/// The native protocol information and complete EVM configuration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct CasperConfig {
    protocol_version: ProtocolVersion,
    activation_point: ActivationPoint,
    // EvmConfig serializes transaction lanes as five-element arrays; its own
    // derived schema describes the underlying structs instead of that wire form.
    #[schemars(with = "EvmConfigSchema")]
    evm_config: EvmConfig,
}

#[derive(JsonSchema)]
#[allow(dead_code)]
struct EvmConfigSchema {
    enabled: bool,
    chain_id: u64,
    spec: EvmSpec,
    block_gas_limit: u64,
    base_fee: u64,
    wei_per_mote: u64,
    transaction_lanes: Vec<[u64; 5]>,
    preinstalls: Vec<(evm::Address, evm::Hash)>,
}

#[derive(Deserialize)]
struct ChainspecProjection {
    protocol: ProtocolProjection,
    evm: EvmConfig,
    #[serde(default)]
    transactions: Option<TransactionsProjection>,
}

#[derive(Deserialize)]
struct ProtocolProjection {
    version: ProtocolVersion,
    activation_point: ActivationPoint,
}

#[derive(Deserialize)]
struct TransactionsProjection {
    #[serde(default)]
    v1: Option<TransactionV1Projection>,
}

#[derive(Deserialize)]
struct TransactionV1Projection {
    #[serde(default)]
    wasm_lanes: Vec<[u64; 5]>,
}

impl ChainspecProjection {
    fn from_raw(raw: &ChainspecRawBytes) -> Result<Self, RpcError> {
        let text = std::str::from_utf8(raw.chainspec_bytes())
            .map_err(|error| internal_error(format!("invalid chainspec bytes: {error}")))?;
        toml::from_str(text)
            .map_err(|error| internal_error(format!("invalid eth_config chainspec: {error}")))
    }

    /// Staged documents are not validated as full chainspecs by the node's
    /// discovery endpoint. Check the EVM invariants needed to advertise them.
    fn validate_future(&self) -> Result<(), RpcError> {
        let evm = &self.evm;
        if matches!(self.protocol.activation_point, ActivationPoint::Genesis(_)) {
            return Err(internal_error(
                "future protocol has a genesis activation point",
            ));
        }
        if evm.wei_per_mote < MINIMUM_WEI_PER_MOTE || u64::try_from(evm.base_fee_wei()).is_err() {
            return Err(internal_error("future EVM fee configuration is invalid"));
        }
        if evm.enabled && evm.transaction_lanes().is_empty() {
            return Err(internal_error(
                "future EVM configuration has no transaction lanes",
            ));
        }
        let mut lane_ids = HashSet::from([MINT_LANE_ID, AUCTION_LANE_ID, INSTALL_UPGRADE_LANE_ID]);
        if let Some(v1) = self
            .transactions
            .as_ref()
            .and_then(|config| config.v1.as_ref())
        {
            for lane in &v1.wasm_lanes {
                let id =
                    u8::try_from(lane[0]).map_err(|_| internal_error("invalid Wasm lane id"))?;
                lane_ids.insert(id);
            }
        }
        for lane in evm.transaction_lanes() {
            if !lane_ids.insert(lane.id()) {
                return Err(internal_error(
                    "future EVM transaction lane id is duplicated or reserved",
                ));
            }
        }
        let mut preinstalls = HashSet::new();
        if evm.enabled
            && evm
                .preinstalls
                .iter()
                .any(|(address, _)| !preinstalls.insert(*address))
        {
            return Err(internal_error(
                "future EVM preinstall address is duplicated",
            ));
        }
        Ok(())
    }

    fn into_snapshot(self) -> ConfigSnapshot {
        let activation_point = self.protocol.activation_point;
        let activation_time = match activation_point {
            ActivationPoint::Genesis(_) => Some(0),
            ActivationPoint::EraId(_) => None,
        };
        ConfigSnapshot {
            activation_time,
            blob_schedule: None,
            chain_id: EthU256::from(self.evm.chain_id),
            fork_id: None,
            precompiles: self.evm.active_precompiles(),
            system_contracts: self.evm.active_system_contracts(),
            casper: CasperConfig {
                protocol_version: self.protocol.version,
                activation_point,
                evm_config: self.evm,
            },
        }
    }
}

async fn read_chainspec(
    node: &dyn NodeClient,
    request: InformationRequest,
) -> Result<Option<ChainspecRawBytes>, RpcError> {
    let response = node.read_info(request).await.map_err(internal_error)?;
    crate::parse_response::<ChainspecRawBytes>(&response.into()).map_err(internal_error)
}

async fn read_future(
    node: &dyn NodeClient,
    version: ProtocolVersion,
) -> Result<ConfigSnapshot, RpcError> {
    let raw = node
        .read_future_chainspec(version)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            internal_error(format!(
                "staged protocol {version} disappeared; retry eth_config"
            ))
        })?;
    let document = ChainspecProjection::from_raw(&raw)?;
    if document.protocol.version != version {
        return Err(internal_error(format!(
            "staged protocol {version} has a different chainspec version"
        )));
    }
    document.validate_future()?;
    Ok(document.into_snapshot())
}

#[async_trait]
impl RpcWithoutParams for Config {
    const METHOD: &'static str = "eth_config";
    type ResponseResult = ConfigResult;

    async fn do_handle_request(
        node: Arc<dyn NodeClient>,
    ) -> Result<Self::ResponseResult, RpcError> {
        // The version list is always read from the node; future documents may be served
        // from the SSE-fed, periodically reconciled cache.
        let raw = read_chainspec(node.as_ref(), InformationRequest::ChainspecRawBytes)
            .await?
            .ok_or_else(|| internal_error("current chainspec is unavailable"))?;
        let current = ChainspecProjection::from_raw(&raw)?;
        let current_version = current.protocol.version;
        let mut versions = node
            .read_staged_protocol_versions()
            .await
            .map_err(internal_error)?;
        versions.retain(|version| *version > current_version);
        versions.sort_unstable();
        versions.dedup();
        let (next, last) = match (versions.first(), versions.last()) {
            (Some(first), Some(last)) => {
                let next = read_future(node.as_ref(), *first).await?;
                let last = if first == last {
                    next.clone()
                } else {
                    read_future(node.as_ref(), *last).await?
                };
                (Some(next), Some(last))
            }
            _ => (None, None),
        };
        Ok(ConfigResult {
            current: current.into_snapshot(),
            next,
            last,
        })
    }
}

impl DocExample for ConfigResult {
    fn doc_example() -> &'static Self {
        static EXAMPLE: LazyLock<ConfigResult> = LazyLock::new(|| {
            let evm = EvmConfig {
                enabled: true,
                chain_id: 1_129_533_445,
                transaction_lanes: vec![casper_types::TransactionLaneDefinition::new(
                    100, 131_072, 1024, 1_000_000, 50,
                )],
                ..EvmConfig::default()
            };
            let current = ChainspecProjection {
                protocol: ProtocolProjection {
                    version: ProtocolVersion::V2_0_0,
                    activation_point: ActivationPoint::Genesis(casper_types::Timestamp::zero()),
                },
                evm,
                transactions: None,
            }
            .into_snapshot();
            ConfigResult {
                current,
                next: None,
                last: None,
            }
        });
        &EXAMPLE
    }
}

#[cfg(test)]
mod tests {
    use casper_binary_port::{BinaryResponse, Command, ErrorCode};
    use casper_json_rpc::{ConfigLimit, JsonRpcOptions, RequestHandlersBuilder};
    use serde_json::{Value, json};

    use super::*;
    use crate::rpcs::test_utils::BinaryPortMock;

    fn chainspec(
        version: &str,
        activation: &str,
        enabled: bool,
        chain_id: u64,
        lanes: &str,
    ) -> ChainspecRawBytes {
        let source = format!(
            r#"
[protocol]
version = "{version}"
activation_point = {activation}
[transactions.v1]
wasm_lanes = [[3, 1000, 100, 100000, 1]]
[evm]
enabled = {enabled}
chain_id = {chain_id}
spec = "osaka"
block_gas_limit = 30000000
base_fee = 5000
wei_per_mote = 1000000000
transaction_lanes = {lanes}
"#
        );
        ChainspecRawBytes::new(source.into_bytes().into(), None, None)
    }

    fn enabled(version: &str, activation: &str, chain_id: u64) -> ChainspecRawBytes {
        chainspec(
            version,
            activation,
            true,
            chain_id,
            "[[100, 131072, 1024, 1000000, 50]]",
        )
    }

    async fn respond(
        client: &BinaryPortMock,
        request: InformationRequest,
        response: BinaryResponse,
    ) {
        client
            .when_then(Command::Get(request.try_into().unwrap()), response)
            .await;
    }

    async fn current_and_versions(
        client: &BinaryPortMock,
        raw: ChainspecRawBytes,
        versions: Vec<ProtocolVersion>,
    ) {
        respond(
            client,
            InformationRequest::ChainspecRawBytes,
            BinaryResponse::from_value(raw),
        )
        .await;
        respond(
            client,
            InformationRequest::StagedProtocolVersions,
            BinaryResponse::from_value(versions),
        )
        .await;
    }

    async fn future(client: &BinaryPortMock, version: ProtocolVersion, raw: ChainspecRawBytes) {
        respond(
            client,
            InformationRequest::FutureChainspec {
                protocol_version: version,
            },
            BinaryResponse::from_value(raw),
        )
        .await;
    }

    #[tokio::test]
    async fn returns_current_osaka_configuration_and_empty_future() {
        let client = Arc::new(BinaryPortMock::new());
        current_and_versions(
            &client,
            enabled("2.2.2", "\"2026-01-01T00:00:00Z\"", 1129533445),
            vec![],
        )
        .await;
        let result = Config::do_handle_request(client.clone()).await.unwrap();
        client.verify_no_lingering().await;
        let value = serde_json::to_value(&result).unwrap();
        assert_eq!(value["current"]["chainId"], "0x43535005");
        assert_eq!(value["current"]["activationTime"], 0);
        assert_eq!(value["current"]["forkId"], Value::Null);
        assert_eq!(value["current"]["blobSchedule"], Value::Null);
        assert_eq!(value["next"], Value::Null);
        assert_eq!(value["last"], Value::Null);
        assert_eq!(value["current"]["casper"]["protocolVersion"], "2.2.2");
        assert_eq!(value["current"]["casper"]["evmConfig"]["spec"], "osaka");
        assert_eq!(
            value["current"]["casper"]["evmConfig"]["transaction_lanes"],
            json!([[100, 131072, 1024, 1000000, 50]])
        );
        let precompiles = value["current"]["precompiles"].as_object().unwrap();
        assert_eq!(precompiles.len(), 18);
        assert_eq!(
            precompiles["P256VERIFY"],
            "0x0000000000000000000000000000000000000100"
        );
        assert_eq!(
            precompiles["KZG_POINT_EVALUATION"],
            "0x000000000000000000000000000000000000000a"
        );
        assert_eq!(
            value["current"]["systemContracts"],
            json!({
                "BEACON_ROOTS_ADDRESS": "0x000f3df6d732807ef1319fb7b8bb8522d0beac02",
                "HISTORY_STORAGE_ADDRESS": "0x0000f90827f1c53a10cb7a02335b175320002935"
            })
        );
        let schema = serde_json::to_value(schemars::schema_for!(ConfigResult)).unwrap();
        assert!(jsonschema::validator_for(&schema).unwrap().is_valid(&value));
    }

    #[tokio::test]
    async fn serves_eth_config_as_a_parameterless_json_rpc() {
        let client = Arc::new(BinaryPortMock::new());
        current_and_versions(&client, enabled("2.2.2", "10", 7), vec![]).await;
        let mut builder = RequestHandlersBuilder::new();
        Config::register_as_handler(client.clone(), &mut builder, ConfigLimit::default());
        let route =
            casper_json_rpc::route("rpc", 65536, builder.build(), JsonRpcOptions::default());
        let invalid = warp::test::request()
            .method("POST")
            .path("/rpc")
            .json(&json!({"jsonrpc":"2.0", "id":1, "method":"eth_config", "params":[1]}))
            .reply(&route)
            .await;
        let invalid: Value = serde_json::from_slice(invalid.body()).unwrap();
        assert_eq!(invalid["error"]["code"], -32602);
        let response = warp::test::request()
            .method("POST")
            .path("/rpc")
            .json(&json!({"jsonrpc":"2.0", "id":2, "method":"eth_config", "params":[]}))
            .reply(&route)
            .await;
        let response: Value = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(response["id"], 2);
        assert_eq!(response["result"]["current"]["chainId"], "0x7");
        assert_eq!(
            response["result"]["current"]["precompiles"]["P256VERIFY"],
            "0x0000000000000000000000000000000000000100"
        );
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn returns_one_staged_disabled_configuration_as_next_and_last() {
        let client = Arc::new(BinaryPortMock::new());
        let version = ProtocolVersion::from_parts(2, 3, 0);
        current_and_versions(&client, enabled("2.2.2", "10", 7), vec![version]).await;
        future(&client, version, chainspec("2.3.0", "20", false, 7, "[]")).await;
        let result = Config::do_handle_request(client.clone()).await.unwrap();
        client.verify_no_lingering().await;
        let value = serde_json::to_value(result).unwrap();
        assert_eq!(value["next"], value["last"]);
        assert_eq!(value["next"]["activationTime"], Value::Null);
        assert_eq!(value["next"]["casper"]["activationPoint"], 20);
        assert_eq!(value["next"]["casper"]["protocolVersion"], "2.3.0");
        assert_eq!(value["next"]["precompiles"], json!({}));
        assert_eq!(value["next"]["systemContracts"], json!({}));
    }

    #[tokio::test]
    async fn uses_cached_future_chainspec_and_reflects_reconciled_edits() {
        use crate::{
            binary_port_cache::HeedBinaryPortCache, caching_node_client::CachingNodeClient,
            node_state_cache::NodeStateCache,
        };

        let node = Arc::new(BinaryPortMock::new());
        let state = Arc::new(NodeStateCache::new_for_test(
            true,
            std::time::Duration::ZERO,
        ));
        let client = Arc::new(CachingNodeClient::new(
            node.clone(),
            None::<Arc<HeedBinaryPortCache>>,
            state.clone(),
        ));
        let version = ProtocolVersion::from_parts(2, 3, 0);

        future(&node, version, enabled("2.3.0", "20", 8)).await;
        state
            .future_chainspecs
            .observe(node.as_ref(), version)
            .await;
        current_and_versions(&node, enabled("2.2.2", "10", 7), vec![version]).await;
        let first = Config::do_handle_request(client.clone()).await.unwrap();
        assert_eq!(first.next.unwrap().chain_id, EthU256::from(8u64));

        respond(
            &node,
            InformationRequest::StagedProtocolVersions,
            BinaryResponse::from_value(vec![version]),
        )
        .await;
        future(&node, version, enabled("2.3.0", "25", 9)).await;
        state.future_chainspecs.reconcile(node.as_ref()).await;
        current_and_versions(&node, enabled("2.2.2", "10", 7), vec![version]).await;
        let edited = Config::do_handle_request(client.clone())
            .await
            .unwrap()
            .next
            .unwrap();
        assert_eq!(edited.chain_id, EthU256::from(9u64));
        assert_eq!(
            edited.casper.activation_point,
            ActivationPoint::EraId(25.into())
        );

        current_and_versions(&node, enabled("2.2.2", "10", 7), vec![]).await;
        assert!(
            Config::do_handle_request(client)
                .await
                .unwrap()
                .next
                .is_none()
        );
        node.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn selects_nearest_and_furthest_staged_versions() {
        let client = Arc::new(BinaryPortMock::new());
        let next = ProtocolVersion::from_parts(2, 3, 0);
        let last = ProtocolVersion::from_parts(2, 5, 0);
        current_and_versions(
            &client,
            enabled("2.2.2", "10", 7),
            vec![last, ProtocolVersion::from_parts(2, 4, 0), next, next],
        )
        .await;
        future(&client, next, enabled("2.3.0", "20", 8)).await;
        future(&client, last, enabled("2.5.0", "50", 9)).await;
        let result = Config::do_handle_request(client.clone()).await.unwrap();
        client.verify_no_lingering().await;
        let value = serde_json::to_value(result).unwrap();
        assert_eq!(value["next"]["casper"]["protocolVersion"], "2.3.0");
        assert_eq!(value["last"]["casper"]["protocolVersion"], "2.5.0");
        assert_eq!(value["next"]["chainId"], "0x8");
        assert_eq!(value["last"]["chainId"], "0x9");
    }

    #[tokio::test]
    async fn reflects_staging_changes_on_each_request() {
        let client = Arc::new(BinaryPortMock::new());
        let version = ProtocolVersion::from_parts(2, 3, 0);
        current_and_versions(&client, enabled("2.2.2", "10", 7), vec![version]).await;
        future(&client, version, enabled("2.3.0", "20", 7)).await;
        current_and_versions(&client, enabled("2.2.2", "10", 7), vec![version]).await;
        future(&client, version, enabled("2.3.0", "25", 8)).await;
        current_and_versions(&client, enabled("2.2.2", "10", 7), vec![]).await;
        let first = Config::do_handle_request(client.clone()).await.unwrap();
        assert_eq!(
            first.next.unwrap().casper.activation_point,
            ActivationPoint::EraId(20.into())
        );
        let edited = Config::do_handle_request(client.clone()).await.unwrap();
        let edited = edited.next.unwrap();
        assert_eq!(
            edited.casper.activation_point,
            ActivationPoint::EraId(25.into())
        );
        assert_eq!(edited.chain_id, EthU256::from(8u64));
        assert!(
            Config::do_handle_request(client.clone())
                .await
                .unwrap()
                .next
                .is_none()
        );
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn rejects_missing_or_invalid_staged_configuration() {
        let version = ProtocolVersion::from_parts(2, 3, 0);
        let valid = enabled("2.3.0", "20", 7);
        let source = std::str::from_utf8(valid.chainspec_bytes()).unwrap();
        let unknown_spec = ChainspecRawBytes::new(
            source
                .replace("spec = \"osaka\"", "spec = \"future\"")
                .into_bytes()
                .into(),
            None,
            None,
        );
        let invalid_fee = ChainspecRawBytes::new(
            source
                .replace("wei_per_mote = 1000000000", "wei_per_mote = 1")
                .into_bytes()
                .into(),
            None,
            None,
        );
        for future_raw in [
            None,
            Some(chainspec("2.3.0", "20", true, 7, "[]")),
            Some(unknown_spec),
            Some(invalid_fee),
            Some(enabled("2.4.0", "20", 7)),
            Some(enabled("2.3.0", "\"2026-01-01T00:00:00Z\"", 7)),
            Some(chainspec(
                "2.3.0",
                "20",
                true,
                7,
                "[[3, 1000, 100, 100000, 1]]",
            )),
        ] {
            let client = Arc::new(BinaryPortMock::new());
            current_and_versions(&client, enabled("2.2.2", "10", 7), vec![version]).await;
            respond(
                &client,
                InformationRequest::FutureChainspec {
                    protocol_version: version,
                },
                BinaryResponse::from_option(future_raw),
            )
            .await;
            assert!(Config::do_handle_request(client.clone()).await.is_err());
            client.verify_no_lingering().await;
        }
    }

    #[tokio::test]
    async fn propagates_staged_discovery_errors() {
        let client = Arc::new(BinaryPortMock::new());
        respond(
            &client,
            InformationRequest::ChainspecRawBytes,
            BinaryResponse::from_value(enabled("2.2.2", "10", 7)),
        )
        .await;
        respond(
            &client,
            InformationRequest::StagedProtocolVersions,
            BinaryResponse::new_error(ErrorCode::InternalError),
        )
        .await;
        assert!(Config::do_handle_request(client.clone()).await.is_err());
        client.verify_no_lingering().await;
    }
}
