//! RPCs returning ancillary information.

use std::{collections::BTreeMap, str, sync::Arc};

use async_trait::async_trait;
use once_cell::sync::Lazy;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use casper_types::{
    binary_port::MinimalBlockInfo,
    execution::{ExecutionResult, ExecutionResultV2},
    ActivationPoint, AvailableBlockRange, Block, BlockHash, BlockSynchronizerStatus,
    ChainspecRawBytes, Deploy, DeployHash, Digest, EraId, ExecutionInfo, NextUpgrade, Peers,
    ProtocolVersion, PublicKey, TimeDiff, Timestamp, Transaction, TransactionHash, ValidatorChange,
};

use super::{
    docs::{DocExample, DOCS_EXAMPLE_API_VERSION},
    ApiVersion, Error, NodeClient, RpcError, RpcWithParams, RpcWithoutParams, CURRENT_API_VERSION,
};

static GET_DEPLOY_PARAMS: Lazy<GetDeployParams> = Lazy::new(|| GetDeployParams {
    deploy_hash: *Deploy::doc_example().hash(),
    finalized_approvals: true,
});
static GET_DEPLOY_RESULT: Lazy<GetDeployResult> = Lazy::new(|| GetDeployResult {
    api_version: DOCS_EXAMPLE_API_VERSION,
    deploy: Deploy::doc_example().clone(),
    execution_info: Some(ExecutionInfo {
        block_hash: *Block::example().hash(),
        block_height: Block::example().clone_header().height(),
        execution_result: Some(ExecutionResult::from(ExecutionResultV2::example().clone())),
    }),
});
static GET_TRANSACTION_PARAMS: Lazy<GetTransactionParams> = Lazy::new(|| GetTransactionParams {
    transaction_hash: Transaction::doc_example().hash(),
    finalized_approvals: true,
});
static GET_TRANSACTION_RESULT: Lazy<GetTransactionResult> = Lazy::new(|| GetTransactionResult {
    api_version: DOCS_EXAMPLE_API_VERSION,
    transaction: Transaction::doc_example().clone(),
    execution_info: Some(ExecutionInfo {
        block_hash: *Block::example().hash(),
        block_height: Block::example().height(),
        execution_result: Some(ExecutionResult::from(ExecutionResultV2::example().clone())),
    }),
});
static GET_PEERS_RESULT: Lazy<GetPeersResult> = Lazy::new(|| GetPeersResult {
    api_version: DOCS_EXAMPLE_API_VERSION,
    peers: Some(("tls:0101..0101".to_owned(), "127.0.0.1:54321".to_owned()))
        .into_iter()
        .collect::<BTreeMap<_, _>>()
        .into(),
});
static GET_VALIDATOR_CHANGES_RESULT: Lazy<GetValidatorChangesResult> = Lazy::new(|| {
    let change = JsonValidatorStatusChange::new(EraId::new(1), ValidatorChange::Added);
    let public_key = PublicKey::example().clone();
    let changes = vec![JsonValidatorChanges::new(public_key, vec![change])];
    GetValidatorChangesResult {
        api_version: DOCS_EXAMPLE_API_VERSION,
        changes,
    }
});
static GET_CHAINSPEC_RESULT: Lazy<GetChainspecResult> = Lazy::new(|| GetChainspecResult {
    api_version: DOCS_EXAMPLE_API_VERSION,
    chainspec_bytes: ChainspecRawBytes::new(vec![42, 42].into(), None, None),
});

static GET_STATUS_RESULT: Lazy<GetStatusResult> = Lazy::new(|| GetStatusResult {
    peers: GET_PEERS_RESULT.peers.clone(),
    api_version: DOCS_EXAMPLE_API_VERSION,
    chainspec_name: String::from("casper-example"),
    starting_state_root_hash: Digest::default(),
    last_added_block_info: Some(MinimalBlockInfo::from(Block::example().clone())),
    our_public_signing_key: Some(PublicKey::example().clone()),
    round_length: Some(TimeDiff::from_millis(1 << 16)),
    next_upgrade: Some(NextUpgrade::new(
        ActivationPoint::EraId(EraId::from(42)),
        ProtocolVersion::from_parts(2, 0, 1),
    )),
    uptime: TimeDiff::from_seconds(13),
    reactor_state: "Initialize".to_owned(),
    last_progress: Timestamp::from(0),
    available_block_range: AvailableBlockRange::RANGE_0_0,
    block_sync: BlockSynchronizerStatus::example().clone(),
    latest_switch_block_hash: Some(BlockHash::default()),
    #[cfg(not(test))]
    build_version: version_string(),

    //  Prevent these values from changing between test sessions
    #[cfg(test)]
    build_version: String::from("1.0.0-xxxxxxxxx@DEBUG"),
});

/// Params for "info_get_deploy" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetDeployParams {
    /// The deploy hash.
    pub deploy_hash: DeployHash,
    /// Whether to return the deploy with the finalized approvals substituted. If `false` or
    /// omitted, returns the deploy with the approvals that were originally received by the node.
    #[serde(default = "finalized_approvals_default")]
    pub finalized_approvals: bool,
}

/// The default for `GetDeployParams::finalized_approvals` and
/// `GetTransactionParams::finalized_approvals`.
fn finalized_approvals_default() -> bool {
    false
}

impl DocExample for GetDeployParams {
    fn doc_example() -> &'static Self {
        &GET_DEPLOY_PARAMS
    }
}

/// Result for "info_get_deploy" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetDeployResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The deploy.
    pub deploy: Deploy,
    /// Execution info, if available.
    #[serde(skip_serializing_if = "Option::is_none", flatten)]
    pub execution_info: Option<ExecutionInfo>,
}

impl DocExample for GetDeployResult {
    fn doc_example() -> &'static Self {
        &GET_DEPLOY_RESULT
    }
}

/// "info_get_deploy" RPC.
pub struct GetDeploy {}

#[async_trait]
impl RpcWithParams for GetDeploy {
    const METHOD: &'static str = "info_get_deploy";
    type RequestParams = GetDeployParams;
    type ResponseResult = GetDeployResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: Self::RequestParams,
    ) -> Result<Self::ResponseResult, RpcError> {
        let hash = TransactionHash::from(params.deploy_hash);
        let (transaction, execution_info) = node_client
            .read_transaction_with_execution_info(hash, params.finalized_approvals)
            .await
            .map_err(|err| Error::NodeRequest("transaction", err))?
            .ok_or(Error::NoDeployWithHash(params.deploy_hash))?
            .into_inner();

        let deploy = match transaction {
            Transaction::Deploy(deploy) => deploy,
            Transaction::V1(_) => return Err(Error::FoundTransactionInsteadOfDeploy.into()),
        };

        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            deploy,
            execution_info,
        })
    }
}

/// Params for "info_get_transaction" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetTransactionParams {
    /// The transaction hash.
    pub transaction_hash: TransactionHash,
    /// Whether to return the transaction with the finalized approvals substituted. If `false` or
    /// omitted, returns the transaction with the approvals that were originally received by the
    /// node.
    #[serde(default = "finalized_approvals_default")]
    pub finalized_approvals: bool,
}

impl DocExample for GetTransactionParams {
    fn doc_example() -> &'static Self {
        &GET_TRANSACTION_PARAMS
    }
}

/// Result for "info_get_transaction" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetTransactionResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The transaction.
    pub transaction: Transaction,
    /// Execution info, if available.
    #[serde(skip_serializing_if = "Option::is_none", flatten)]
    pub execution_info: Option<ExecutionInfo>,
}

impl DocExample for GetTransactionResult {
    fn doc_example() -> &'static Self {
        &GET_TRANSACTION_RESULT
    }
}

/// "info_get_transaction" RPC.
pub struct GetTransaction {}

#[async_trait]
impl RpcWithParams for GetTransaction {
    const METHOD: &'static str = "info_get_transaction";
    type RequestParams = GetTransactionParams;
    type ResponseResult = GetTransactionResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: Self::RequestParams,
    ) -> Result<Self::ResponseResult, RpcError> {
        let (transaction, execution_info) = node_client
            .read_transaction_with_execution_info(
                params.transaction_hash,
                params.finalized_approvals,
            )
            .await
            .map_err(|err| Error::NodeRequest("transaction", err))?
            .ok_or(Error::NoTransactionWithHash(params.transaction_hash))?
            .into_inner();

        Ok(Self::ResponseResult {
            transaction,
            api_version: CURRENT_API_VERSION,
            execution_info,
        })
    }
}

/// Result for "info_get_peers" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetPeersResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The node ID and network address of each connected peer.
    pub peers: Peers,
}

impl DocExample for GetPeersResult {
    fn doc_example() -> &'static Self {
        &GET_PEERS_RESULT
    }
}

/// "info_get_peers" RPC.
pub struct GetPeers {}

#[async_trait]
impl RpcWithoutParams for GetPeers {
    const METHOD: &'static str = "info_get_peers";
    type ResponseResult = GetPeersResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
    ) -> Result<Self::ResponseResult, RpcError> {
        let peers = node_client
            .read_peers()
            .await
            .map_err(|err| Error::NodeRequest("peers", err))?;
        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            peers,
        })
    }
}

/// A single change to a validator's status in the given era.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JsonValidatorStatusChange {
    /// The era in which the change occurred.
    era_id: EraId,
    /// The change in validator status.
    validator_change: ValidatorChange,
}

impl JsonValidatorStatusChange {
    pub(crate) fn new(era_id: EraId, validator_change: ValidatorChange) -> Self {
        JsonValidatorStatusChange {
            era_id,
            validator_change,
        }
    }
}

/// The changes in a validator's status.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JsonValidatorChanges {
    /// The public key of the validator.
    public_key: PublicKey,
    /// The set of changes to the validator's status.
    status_changes: Vec<JsonValidatorStatusChange>,
}

impl JsonValidatorChanges {
    pub(crate) fn new(
        public_key: PublicKey,
        status_changes: Vec<JsonValidatorStatusChange>,
    ) -> Self {
        JsonValidatorChanges {
            public_key,
            status_changes,
        }
    }
}

/// Result for the "info_get_validator_changes" RPC.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetValidatorChangesResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The validators' status changes.
    pub changes: Vec<JsonValidatorChanges>,
}

impl GetValidatorChangesResult {
    pub(crate) fn new(changes: BTreeMap<PublicKey, Vec<(EraId, ValidatorChange)>>) -> Self {
        let changes = changes
            .into_iter()
            .map(|(public_key, mut validator_changes)| {
                validator_changes.sort();
                let status_changes = validator_changes
                    .into_iter()
                    .map(|(era_id, validator_change)| {
                        JsonValidatorStatusChange::new(era_id, validator_change)
                    })
                    .collect();
                JsonValidatorChanges::new(public_key, status_changes)
            })
            .collect();
        GetValidatorChangesResult {
            api_version: CURRENT_API_VERSION,
            changes,
        }
    }
}

impl DocExample for GetValidatorChangesResult {
    fn doc_example() -> &'static Self {
        &GET_VALIDATOR_CHANGES_RESULT
    }
}

/// "info_get_validator_changes" RPC.
pub struct GetValidatorChanges {}

#[async_trait]
impl RpcWithoutParams for GetValidatorChanges {
    const METHOD: &'static str = "info_get_validator_changes";
    type ResponseResult = GetValidatorChangesResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
    ) -> Result<Self::ResponseResult, RpcError> {
        let changes = node_client
            .read_validator_changes()
            .await
            .map_err(|err| Error::NodeRequest("validator changes", err))?;
        Ok(Self::ResponseResult::new(changes.into()))
    }
}

/// Result for the "info_get_chainspec" RPC.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
pub struct GetChainspecResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The chainspec file bytes.
    pub chainspec_bytes: ChainspecRawBytes,
}

impl DocExample for GetChainspecResult {
    fn doc_example() -> &'static Self {
        &GET_CHAINSPEC_RESULT
    }
}

/// "info_get_chainspec" RPC.
pub struct GetChainspec {}

#[async_trait]
impl RpcWithoutParams for GetChainspec {
    const METHOD: &'static str = "info_get_chainspec";
    type ResponseResult = GetChainspecResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
    ) -> Result<Self::ResponseResult, RpcError> {
        let chainspec_bytes = node_client
            .read_chainspec_bytes()
            .await
            .map_err(|err| Error::NodeRequest("chainspec bytes", err))?;

        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            chainspec_bytes,
        })
    }
}

/// Result for "info_get_status" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetStatusResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The node ID and network address of each connected peer.
    pub peers: Peers,
    /// The compiled node version.
    pub build_version: String,
    /// The chainspec name.
    pub chainspec_name: String,
    /// The state root hash of the lowest block in the available block range.
    pub starting_state_root_hash: Digest,
    /// The minimal info of the last block from the linear chain.
    pub last_added_block_info: Option<MinimalBlockInfo>,
    /// Our public signing key.
    pub our_public_signing_key: Option<PublicKey>,
    /// The next round length if this node is a validator.
    pub round_length: Option<TimeDiff>,
    /// Information about the next scheduled upgrade.
    pub next_upgrade: Option<NextUpgrade>,
    /// Time that passed since the node has started.
    pub uptime: TimeDiff,
    /// The name of the current state of node reactor.
    pub reactor_state: String,
    /// Timestamp of the last recorded progress in the reactor.
    pub last_progress: Timestamp,
    /// The available block range in storage.
    pub available_block_range: AvailableBlockRange,
    /// The status of the block synchronizer builders.
    pub block_sync: BlockSynchronizerStatus,
    /// The hash of the latest switch block.
    pub latest_switch_block_hash: Option<BlockHash>,
}

impl DocExample for GetStatusResult {
    fn doc_example() -> &'static Self {
        &GET_STATUS_RESULT
    }
}

/// "info_get_status" RPC.
pub struct GetStatus {}

#[async_trait]
impl RpcWithoutParams for GetStatus {
    const METHOD: &'static str = "info_get_status";
    type ResponseResult = GetStatusResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
    ) -> Result<Self::ResponseResult, RpcError> {
        let status = node_client
            .read_node_status()
            .await
            .map_err(|err| Error::NodeRequest("node status", err))?;

        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            peers: status.peers,
            chainspec_name: status.chainspec_name,
            starting_state_root_hash: status.starting_state_root_hash,
            last_added_block_info: status.last_added_block_info,
            our_public_signing_key: status.our_public_signing_key,
            round_length: status.round_length,
            next_upgrade: status.next_upgrade,
            uptime: status.uptime,
            reactor_state: status.reactor_state.into_inner(),
            last_progress: status.last_progress,
            available_block_range: status.available_block_range,
            block_sync: status.block_sync,
            latest_switch_block_hash: status.latest_switch_block_hash,
            build_version: status.build_version,
        })
    }
}

#[cfg(not(test))]
fn version_string() -> String {
    use std::env;
    use tracing::warn;

    let mut version = env!("CARGO_PKG_VERSION").to_string();
    if let Ok(git_sha) = env::var("VERGEN_GIT_SHA") {
        version = format!("{}-{}", version, git_sha);
    } else {
        warn!(
            "vergen env var unavailable, casper-node build version will not include git short hash"
        );
    }

    // Add a `@DEBUG` (or similar) tag to release string on non-release builds.
    if env!("SIDECAR_BUILD_PROFILE") != "release" {
        version += "@";
        let profile = env!("SIDECAR_BUILD_PROFILE").to_uppercase();
        version.push_str(&profile);
    }

    version
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use crate::{rpcs::ErrorCode, ClientError, SUPPORTED_PROTOCOL_VERSION};
    use casper_types::{
        binary_port::{
            BinaryRequest, BinaryResponse, BinaryResponseAndRequest, GetRequest,
            InformationRequest, InformationRequestTag, TransactionWithExecutionInfo,
        },
        bytesrepr::{FromBytes, ToBytes},
        testing::TestRng,
        BlockHash, TransactionV1,
    };
    use pretty_assertions::assert_eq;
    use rand::Rng;

    use super::*;

    #[tokio::test]
    async fn should_read_transaction() {
        let rng = &mut TestRng::new();
        let transaction = Transaction::from(TransactionV1::random(rng));
        let execution_info = ExecutionInfo {
            block_hash: BlockHash::random(rng),
            block_height: rng.gen(),
            execution_result: Some(ExecutionResult::random(rng)),
        };
        let finalized_approvals = rng.gen();

        let resp = GetTransaction::do_handle_request(
            Arc::new(ValidTransactionMock::new(
                TransactionWithExecutionInfo::new(
                    transaction.clone(),
                    Some(execution_info.clone()),
                ),
                finalized_approvals,
            )),
            GetTransactionParams {
                transaction_hash: transaction.hash(),
                finalized_approvals,
            },
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetTransactionResult {
                api_version: CURRENT_API_VERSION,
                transaction,
                execution_info: Some(execution_info),
            }
        );
    }

    #[tokio::test]
    async fn should_read_deploy_via_get_transaction() {
        let rng = &mut TestRng::new();
        let deploy = Deploy::random(rng);
        let execution_info = ExecutionInfo {
            block_hash: BlockHash::random(rng),
            block_height: rng.gen(),
            execution_result: Some(ExecutionResult::random(rng)),
        };
        let finalized_approvals = rng.gen();

        let resp = GetTransaction::do_handle_request(
            Arc::new(ValidTransactionMock::new(
                TransactionWithExecutionInfo::new(
                    Transaction::Deploy(deploy.clone()),
                    Some(execution_info.clone()),
                ),
                finalized_approvals,
            )),
            GetTransactionParams {
                transaction_hash: deploy.hash().into(),
                finalized_approvals,
            },
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetTransactionResult {
                api_version: CURRENT_API_VERSION,
                transaction: deploy.into(),
                execution_info: Some(execution_info),
            }
        );
    }

    #[tokio::test]
    async fn should_read_deploy_via_get_deploy() {
        let rng = &mut TestRng::new();
        let deploy = Deploy::random(rng);
        let execution_info = ExecutionInfo {
            block_hash: BlockHash::random(rng),
            block_height: rng.gen(),
            execution_result: Some(ExecutionResult::random(rng)),
        };
        let finalized_approvals = rng.gen();

        let resp = GetDeploy::do_handle_request(
            Arc::new(ValidTransactionMock::new(
                TransactionWithExecutionInfo::new(
                    Transaction::Deploy(deploy.clone()),
                    Some(execution_info.clone()),
                ),
                finalized_approvals,
            )),
            GetDeployParams {
                deploy_hash: *deploy.hash(),
                finalized_approvals,
            },
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetDeployResult {
                api_version: CURRENT_API_VERSION,
                deploy,
                execution_info: Some(execution_info),
            }
        );
    }

    #[tokio::test]
    async fn should_reject_transaction_when_asking_for_deploy() {
        let rng = &mut TestRng::new();
        let transaction = TransactionV1::random(rng);
        let execution_info = ExecutionInfo {
            block_hash: BlockHash::random(rng),
            block_height: rng.gen(),
            execution_result: Some(ExecutionResult::random(rng)),
        };
        let finalized_approvals = rng.gen();

        let err = GetDeploy::do_handle_request(
            Arc::new(ValidTransactionMock::new(
                TransactionWithExecutionInfo::new(
                    Transaction::V1(transaction.clone()),
                    Some(execution_info.clone()),
                ),
                finalized_approvals,
            )),
            GetDeployParams {
                deploy_hash: DeployHash::new(*transaction.hash().inner()),
                finalized_approvals,
            },
        )
        .await
        .expect_err("should reject request");

        assert_eq!(err.code(), ErrorCode::VariantMismatch as i64);
    }

    struct ValidTransactionMock {
        transaction_bytes: Vec<u8>,
        should_request_approvals: bool,
    }

    impl ValidTransactionMock {
        fn new(info: TransactionWithExecutionInfo, should_request_approvals: bool) -> Self {
            let transaction_bytes = info.to_bytes().expect("should serialize transaction");
            ValidTransactionMock {
                transaction_bytes,
                should_request_approvals,
            }
        }
    }

    #[async_trait]
    impl NodeClient for ValidTransactionMock {
        async fn send_request(
            &self,
            req: BinaryRequest,
        ) -> Result<BinaryResponseAndRequest, ClientError> {
            match req {
                BinaryRequest::Get(GetRequest::Information { info_type_tag, key })
                    if InformationRequestTag::try_from(info_type_tag)
                        == Ok(InformationRequestTag::Transaction) =>
                {
                    let req = InformationRequest::try_from((
                        InformationRequestTag::try_from(info_type_tag).unwrap(),
                        &key[..],
                    ))
                    .unwrap();
                    assert!(matches!(
                        req,
                        InformationRequest::Transaction { with_finalized_approvals, .. }
                            if with_finalized_approvals == self.should_request_approvals
                    ));
                    let (transaction, _) =
                        TransactionWithExecutionInfo::from_bytes(&self.transaction_bytes)
                            .expect("should deserialize transaction");
                    Ok(BinaryResponseAndRequest::new(
                        BinaryResponse::from_value(transaction, SUPPORTED_PROTOCOL_VERSION),
                        &[],
                    ))
                }
                req => unimplemented!("unexpected request: {:?}", req),
            }
        }
    }
}
