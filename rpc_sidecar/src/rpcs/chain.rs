//! RPCs related to the block chain.

mod era_summary;

use std::{clone::Clone, str, sync::Arc};

use async_trait::async_trait;
use once_cell::sync::Lazy;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use casper_types::{
    global_state::TrieMerkleProof, BlockHash, BlockHeader, BlockHeaderV2, BlockIdentifier, Digest,
    GlobalStateIdentifier, JsonBlockWithSignatures, Key, StoredValue, Transfer,
};

use super::{
    common,
    docs::{DocExample, DOCS_EXAMPLE_API_VERSION},
    ApiVersion, Error, NodeClient, RpcError, RpcWithOptionalParams, CURRENT_API_VERSION,
};
pub use era_summary::EraSummary;
use era_summary::ERA_SUMMARY;

static GET_BLOCK_PARAMS: Lazy<GetBlockParams> = Lazy::new(|| GetBlockParams {
    block_identifier: BlockIdentifier::Hash(*JsonBlockWithSignatures::example().block.hash()),
});
static GET_BLOCK_RESULT: Lazy<GetBlockResult> = Lazy::new(|| GetBlockResult {
    api_version: DOCS_EXAMPLE_API_VERSION,
    block_with_signatures: Some(JsonBlockWithSignatures::example().clone()),
});
static GET_BLOCK_TRANSFERS_PARAMS: Lazy<GetBlockTransfersParams> =
    Lazy::new(|| GetBlockTransfersParams {
        block_identifier: BlockIdentifier::Hash(*BlockHash::example()),
    });
static GET_BLOCK_TRANSFERS_RESULT: Lazy<GetBlockTransfersResult> =
    Lazy::new(|| GetBlockTransfersResult {
        api_version: DOCS_EXAMPLE_API_VERSION,
        block_hash: Some(*BlockHash::example()),
        transfers: Some(vec![Transfer::example().clone()]),
    });
static GET_STATE_ROOT_HASH_PARAMS: Lazy<GetStateRootHashParams> =
    Lazy::new(|| GetStateRootHashParams {
        block_identifier: BlockIdentifier::Height(BlockHeaderV2::example().height()),
    });
static GET_STATE_ROOT_HASH_RESULT: Lazy<GetStateRootHashResult> =
    Lazy::new(|| GetStateRootHashResult {
        api_version: DOCS_EXAMPLE_API_VERSION,
        state_root_hash: Some(*BlockHeaderV2::example().state_root_hash()),
    });
static GET_ERA_INFO_PARAMS: Lazy<GetEraInfoParams> = Lazy::new(|| GetEraInfoParams {
    block_identifier: BlockIdentifier::Hash(ERA_SUMMARY.block_hash),
});
static GET_ERA_INFO_RESULT: Lazy<GetEraInfoResult> = Lazy::new(|| GetEraInfoResult {
    api_version: DOCS_EXAMPLE_API_VERSION,
    era_summary: Some(ERA_SUMMARY.clone()),
});
static GET_ERA_SUMMARY_PARAMS: Lazy<GetEraSummaryParams> = Lazy::new(|| GetEraSummaryParams {
    block_identifier: BlockIdentifier::Hash(ERA_SUMMARY.block_hash),
});
static GET_ERA_SUMMARY_RESULT: Lazy<GetEraSummaryResult> = Lazy::new(|| GetEraSummaryResult {
    api_version: DOCS_EXAMPLE_API_VERSION,
    era_summary: ERA_SUMMARY.clone(),
});

/// Params for "chain_get_block" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetBlockParams {
    /// The block identifier.
    pub block_identifier: BlockIdentifier,
}

impl DocExample for GetBlockParams {
    fn doc_example() -> &'static Self {
        &GET_BLOCK_PARAMS
    }
}

/// Result for "chain_get_block" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetBlockResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The block, if found.
    pub block_with_signatures: Option<JsonBlockWithSignatures>,
}

impl DocExample for GetBlockResult {
    fn doc_example() -> &'static Self {
        &GET_BLOCK_RESULT
    }
}

/// "chain_get_block" RPC.
pub struct GetBlock {}

#[async_trait]
impl RpcWithOptionalParams for GetBlock {
    const METHOD: &'static str = "chain_get_block";
    type OptionalRequestParams = GetBlockParams;
    type ResponseResult = GetBlockResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        maybe_params: Option<Self::OptionalRequestParams>,
    ) -> Result<Self::ResponseResult, RpcError> {
        let identifier = maybe_params.map(|params| params.block_identifier);
        let (block, signatures) = common::get_signed_block(&*node_client, identifier)
            .await?
            .into_inner();
        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            block_with_signatures: Some(JsonBlockWithSignatures::new(block, Some(signatures))),
        })
    }
}

/// Params for "chain_get_block_transfers" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetBlockTransfersParams {
    /// The block hash.
    pub block_identifier: BlockIdentifier,
}

impl DocExample for GetBlockTransfersParams {
    fn doc_example() -> &'static Self {
        &GET_BLOCK_TRANSFERS_PARAMS
    }
}

/// Result for "chain_get_block_transfers" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetBlockTransfersResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The block hash, if found.
    pub block_hash: Option<BlockHash>,
    /// The block's transfers, if found.
    pub transfers: Option<Vec<Transfer>>,
}

impl DocExample for GetBlockTransfersResult {
    fn doc_example() -> &'static Self {
        &GET_BLOCK_TRANSFERS_RESULT
    }
}

/// "chain_get_block_transfers" RPC.
pub struct GetBlockTransfers {}

#[async_trait]
impl RpcWithOptionalParams for GetBlockTransfers {
    const METHOD: &'static str = "chain_get_block_transfers";
    type OptionalRequestParams = GetBlockTransfersParams;
    type ResponseResult = GetBlockTransfersResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        maybe_params: Option<Self::OptionalRequestParams>,
    ) -> Result<Self::ResponseResult, RpcError> {
        let identifier = maybe_params.map(|params| params.block_identifier);
        let header = common::get_block_header(&*node_client, identifier).await?;
        let transfers = node_client
            .read_block_transfers(header.block_hash())
            .await
            .map_err(|err| Error::NodeRequest("block transfers", err))?;
        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            block_hash: Some(header.block_hash()),
            transfers,
        })
    }
}

/// Params for "chain_get_state_root_hash" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetStateRootHashParams {
    /// The block hash.
    pub block_identifier: BlockIdentifier,
}

impl DocExample for GetStateRootHashParams {
    fn doc_example() -> &'static Self {
        &GET_STATE_ROOT_HASH_PARAMS
    }
}

/// Result for "chain_get_state_root_hash" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetStateRootHashResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// Hex-encoded hash of the state root.
    pub state_root_hash: Option<Digest>,
}

impl DocExample for GetStateRootHashResult {
    fn doc_example() -> &'static Self {
        &GET_STATE_ROOT_HASH_RESULT
    }
}

/// "chain_get_state_root_hash" RPC.
pub struct GetStateRootHash {}

#[async_trait]
impl RpcWithOptionalParams for GetStateRootHash {
    const METHOD: &'static str = "chain_get_state_root_hash";
    type OptionalRequestParams = GetStateRootHashParams;
    type ResponseResult = GetStateRootHashResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        maybe_params: Option<Self::OptionalRequestParams>,
    ) -> Result<Self::ResponseResult, RpcError> {
        let identifier = maybe_params.map(|params| params.block_identifier);
        let block_header = common::get_block_header(&*node_client, identifier).await?;
        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            state_root_hash: Some(*block_header.state_root_hash()),
        })
    }
}

/// Params for "chain_get_era_info" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetEraInfoParams {
    /// The block identifier.
    pub block_identifier: BlockIdentifier,
}

impl DocExample for GetEraInfoParams {
    fn doc_example() -> &'static Self {
        &GET_ERA_INFO_PARAMS
    }
}

/// Result for "chain_get_era_info" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetEraInfoResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The era summary.
    pub era_summary: Option<EraSummary>,
}

impl DocExample for GetEraInfoResult {
    fn doc_example() -> &'static Self {
        &GET_ERA_INFO_RESULT
    }
}

/// "chain_get_era_info_by_switch_block" RPC
pub struct GetEraInfoBySwitchBlock {}

#[async_trait]
impl RpcWithOptionalParams for GetEraInfoBySwitchBlock {
    const METHOD: &'static str = "chain_get_era_info_by_switch_block";
    type OptionalRequestParams = GetEraInfoParams;
    type ResponseResult = GetEraInfoResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        maybe_params: Option<Self::OptionalRequestParams>,
    ) -> Result<Self::ResponseResult, RpcError> {
        let block_header = match maybe_params {
            Some(params) => {
                common::get_block_header(&*node_client, Some(params.block_identifier)).await?
            }
            None => common::get_latest_switch_block_header(&*node_client).await?,
        };
        let era_summary = if block_header.is_switch_block() {
            Some(get_era_summary_by_block(node_client, &block_header).await?)
        } else {
            None
        };

        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            era_summary,
        })
    }
}

/// Params for "chain_get_era_summary" RPC response.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetEraSummaryParams {
    /// The block identifier.
    pub block_identifier: BlockIdentifier,
}

impl DocExample for GetEraSummaryParams {
    fn doc_example() -> &'static Self {
        &GET_ERA_SUMMARY_PARAMS
    }
}

/// Result for "chain_get_era_summary" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetEraSummaryResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The era summary.
    pub era_summary: EraSummary,
}

impl DocExample for GetEraSummaryResult {
    fn doc_example() -> &'static Self {
        &GET_ERA_SUMMARY_RESULT
    }
}

/// "chain_get_era_summary" RPC
pub struct GetEraSummary {}

#[async_trait]
impl RpcWithOptionalParams for GetEraSummary {
    const METHOD: &'static str = "chain_get_era_summary";
    type OptionalRequestParams = GetEraSummaryParams;
    type ResponseResult = GetEraSummaryResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        maybe_params: Option<Self::OptionalRequestParams>,
    ) -> Result<Self::ResponseResult, RpcError> {
        let block_header = match maybe_params {
            Some(params) => {
                common::get_block_header(&*node_client, Some(params.block_identifier)).await?
            }
            None => common::get_latest_switch_block_header(&*node_client).await?,
        };

        let era_summary = get_era_summary_by_block(node_client, &block_header).await?;

        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            era_summary,
        })
    }
}

async fn get_era_summary_by_block(
    node_client: Arc<dyn NodeClient>,
    block_header: &BlockHeader,
) -> Result<EraSummary, Error> {
    fn create_era_summary(
        block_header: &BlockHeader,
        stored_value: StoredValue,
        merkle_proof: Vec<TrieMerkleProof<Key, StoredValue>>,
    ) -> Result<EraSummary, Error> {
        Ok(EraSummary {
            block_hash: block_header.block_hash(),
            era_id: block_header.era_id(),
            stored_value,
            state_root_hash: *block_header.state_root_hash(),
            merkle_proof: common::encode_proof(&merkle_proof)?,
        })
    }

    let state_identifier = GlobalStateIdentifier::StateRootHash(*block_header.state_root_hash());
    let result = node_client
        .query_global_state(Some(state_identifier), Key::EraSummary, vec![])
        .await
        .map_err(|err| Error::NodeRequest("era summary", err))?;

    let era_summary = if let Some(result) = result {
        let (value, merkle_proof) = result.into_inner();
        create_era_summary(block_header, value, merkle_proof)?
    } else {
        let (result, merkle_proof) = node_client
            .query_global_state(
                Some(state_identifier),
                Key::EraInfo(block_header.era_id()),
                vec![],
            )
            .await
            .map_err(|err| Error::NodeRequest("era info", err))?
            .ok_or(Error::GlobalStateEntryNotFound)?
            .into_inner();

        create_era_summary(block_header, result, merkle_proof)?
    };
    Ok(era_summary)
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use crate::{ClientError, SUPPORTED_PROTOCOL_VERSION};
    use casper_binary_port::{
        BinaryRequest, BinaryResponse, BinaryResponseAndRequest, GetRequest,
        GlobalStateEntityQualifier, GlobalStateQueryResult, InformationRequestTag, RecordId,
    };
    use casper_types::{
        system::auction::EraInfo, testing::TestRng, Block, BlockSignaturesV1, BlockSignaturesV2,
        ChainNameDigest, SignedBlock, TestBlockBuilder, TestBlockV1Builder,
    };
    use pretty_assertions::assert_eq;
    use rand::Rng;

    use super::*;

    #[tokio::test]
    async fn should_read_block_v2() {
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().build(rng));
        let signatures = BlockSignaturesV2::new(
            *block.hash(),
            block.height(),
            block.era_id(),
            ChainNameDigest::random(rng),
        );
        let resp = GetBlock::do_handle_request(
            Arc::new(ValidBlockMock {
                block: SignedBlock::new(block.clone(), signatures.into()),
                transfers: vec![],
            }),
            None,
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetBlockResult {
                api_version: CURRENT_API_VERSION,
                block_with_signatures: Some(JsonBlockWithSignatures::new(block, None)),
            }
        );
    }

    #[tokio::test]
    async fn should_read_block_v1() {
        let rng = &mut TestRng::new();
        let block = TestBlockV1Builder::new().build(rng);

        let resp = GetBlock::do_handle_request(
            Arc::new(ValidBlockMock {
                block: SignedBlock::new(
                    Block::V1(block.clone()),
                    BlockSignaturesV1::new(*block.hash(), block.era_id()).into(),
                ),
                transfers: vec![],
            }),
            None,
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetBlockResult {
                api_version: CURRENT_API_VERSION,
                block_with_signatures: Some(JsonBlockWithSignatures::new(Block::V1(block), None)),
            }
        );
    }

    #[tokio::test]
    async fn should_read_block_transfers() {
        let rng = &mut TestRng::new();
        let block = TestBlockBuilder::new().build(rng);

        let mut transfers = vec![];
        for _ in 0..rng.gen_range(0..10) {
            transfers.push(Transfer::random(rng));
        }
        let signatures = BlockSignaturesV2::new(
            *block.hash(),
            block.height(),
            block.era_id(),
            ChainNameDigest::random(rng),
        );
        let resp = GetBlockTransfers::do_handle_request(
            Arc::new(ValidBlockMock {
                block: SignedBlock::new(Block::V2(block.clone()), signatures.into()),
                transfers: transfers.clone(),
            }),
            None,
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetBlockTransfersResult {
                api_version: CURRENT_API_VERSION,
                block_hash: Some(*block.hash()),
                transfers: Some(transfers),
            }
        );
    }

    #[tokio::test]
    async fn should_read_block_state_root_hash() {
        let rng = &mut TestRng::new();
        let block = TestBlockBuilder::new().build(rng);

        let signatures = BlockSignaturesV2::new(
            *block.hash(),
            block.height(),
            block.era_id(),
            ChainNameDigest::random(rng),
        );
        let resp = GetStateRootHash::do_handle_request(
            Arc::new(ValidBlockMock {
                block: SignedBlock::new(Block::V2(block.clone()), signatures.into()),
                transfers: vec![],
            }),
            None,
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetStateRootHashResult {
                api_version: CURRENT_API_VERSION,
                state_root_hash: Some(*block.state_root_hash()),
            }
        );
    }

    #[tokio::test]
    async fn should_read_block_era_summary() {
        let rng = &mut TestRng::new();
        let block = TestBlockBuilder::new().build(rng);

        let resp = GetEraSummary::do_handle_request(
            Arc::new(ValidEraSummaryMock {
                block: Block::V2(block.clone()),
                expect_no_block_identifier: true,
            }),
            None,
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetEraSummaryResult {
                api_version: CURRENT_API_VERSION,
                era_summary: EraSummary {
                    block_hash: *block.hash(),
                    era_id: block.era_id(),
                    stored_value: StoredValue::EraInfo(EraInfo::new()),
                    state_root_hash: *block.state_root_hash(),
                    merkle_proof: String::from("00000000"),
                }
            }
        );
    }

    #[tokio::test]
    async fn should_read_block_era_summary_with_block_id() {
        let rng = &mut TestRng::new();
        let block = TestBlockBuilder::new().build(rng);

        let resp = GetEraSummary::do_handle_request(
            Arc::new(ValidEraSummaryMock {
                block: Block::V2(block.clone()),
                expect_no_block_identifier: false,
            }),
            Some(GetEraSummaryParams {
                block_identifier: BlockIdentifier::Hash(*block.hash()),
            }),
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetEraSummaryResult {
                api_version: CURRENT_API_VERSION,
                era_summary: EraSummary {
                    block_hash: *block.hash(),
                    era_id: block.era_id(),
                    stored_value: StoredValue::EraInfo(EraInfo::new()),
                    state_root_hash: *block.state_root_hash(),
                    merkle_proof: String::from("00000000"),
                }
            }
        );
    }

    #[tokio::test]
    async fn should_read_block_era_info_by_switch_block() {
        let rng = &mut TestRng::new();
        let block = TestBlockBuilder::new().switch_block(true).build(rng);

        let resp = GetEraInfoBySwitchBlock::do_handle_request(
            Arc::new(ValidEraSummaryMock {
                block: Block::V2(block.clone()),
                expect_no_block_identifier: true,
            }),
            None,
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetEraInfoResult {
                api_version: CURRENT_API_VERSION,
                era_summary: Some(EraSummary {
                    block_hash: *block.hash(),
                    era_id: block.era_id(),
                    stored_value: StoredValue::EraInfo(EraInfo::new()),
                    state_root_hash: *block.state_root_hash(),
                    merkle_proof: String::from("00000000"),
                })
            }
        );
    }

    #[tokio::test]
    async fn should_read_block_era_info_by_switch_block_with_block_id() {
        let rng = &mut TestRng::new();
        let block = TestBlockBuilder::new().switch_block(true).build(rng);

        let resp = GetEraInfoBySwitchBlock::do_handle_request(
            Arc::new(ValidEraSummaryMock {
                block: Block::V2(block.clone()),
                expect_no_block_identifier: false,
            }),
            Some(GetEraInfoParams {
                block_identifier: BlockIdentifier::Hash(*block.hash()),
            }),
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetEraInfoResult {
                api_version: CURRENT_API_VERSION,
                era_summary: Some(EraSummary {
                    block_hash: *block.hash(),
                    era_id: block.era_id(),
                    stored_value: StoredValue::EraInfo(EraInfo::new()),
                    state_root_hash: *block.state_root_hash(),
                    merkle_proof: String::from("00000000"),
                })
            }
        );
    }

    #[tokio::test]
    async fn should_read_none_block_era_info_by_switch_block_for_non_switch() {
        let rng = &mut TestRng::new();
        let block = TestBlockBuilder::new().switch_block(false).build(rng);

        let resp = GetEraInfoBySwitchBlock::do_handle_request(
            Arc::new(ValidEraSummaryMock {
                block: Block::V2(block.clone()),
                expect_no_block_identifier: true,
            }),
            None,
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetEraInfoResult {
                api_version: CURRENT_API_VERSION,
                era_summary: None
            }
        );
    }

    struct ValidBlockMock {
        block: SignedBlock,
        transfers: Vec<Transfer>,
    }

    #[async_trait]
    impl NodeClient for ValidBlockMock {
        async fn send_request(
            &self,
            req: BinaryRequest,
        ) -> Result<BinaryResponseAndRequest, ClientError> {
            match req {
                BinaryRequest::Get(GetRequest::Information { info_type_tag, .. })
                    if InformationRequestTag::try_from(info_type_tag)
                        == Ok(InformationRequestTag::SignedBlock) =>
                {
                    Ok(BinaryResponseAndRequest::new(
                        BinaryResponse::from_value(self.block.clone(), SUPPORTED_PROTOCOL_VERSION),
                        &[],
                        0,
                    ))
                }
                BinaryRequest::Get(GetRequest::Information { info_type_tag, .. })
                    if InformationRequestTag::try_from(info_type_tag)
                        == Ok(InformationRequestTag::BlockHeader) =>
                {
                    Ok(BinaryResponseAndRequest::new(
                        BinaryResponse::from_value(
                            self.block.block().clone_header(),
                            SUPPORTED_PROTOCOL_VERSION,
                        ),
                        &[],
                        0,
                    ))
                }
                BinaryRequest::Get(GetRequest::Record {
                    record_type_tag, ..
                }) if RecordId::try_from(record_type_tag) == Ok(RecordId::Transfer) => {
                    Ok(BinaryResponseAndRequest::new_legacy_test_response(
                        RecordId::Transfer,
                        &self.transfers,
                        SUPPORTED_PROTOCOL_VERSION,
                    ))
                }
                req => unimplemented!("unexpected request: {:?}", req),
            }
        }
    }

    struct ValidEraSummaryMock {
        block: Block,
        expect_no_block_identifier: bool,
    }

    #[async_trait]
    impl NodeClient for ValidEraSummaryMock {
        async fn send_request(
            &self,
            req: BinaryRequest,
        ) -> Result<BinaryResponseAndRequest, ClientError> {
            let expected_tag = if self.expect_no_block_identifier {
                InformationRequestTag::LatestSwitchBlockHeader
            } else {
                InformationRequestTag::BlockHeader
            };
            match req {
                BinaryRequest::Get(GetRequest::Information { info_type_tag, .. })
                    if InformationRequestTag::try_from(info_type_tag) == Ok(expected_tag) =>
                {
                    Ok(BinaryResponseAndRequest::new(
                        BinaryResponse::from_value(
                            self.block.clone_header(),
                            SUPPORTED_PROTOCOL_VERSION,
                        ),
                        &[],
                        0,
                    ))
                }
                BinaryRequest::Get(GetRequest::State(req))
                    if matches!(
                        req.clone().destructure().1,
                        GlobalStateEntityQualifier::Item {
                            base_key: Key::EraSummary,
                            ..
                        }
                    ) =>
                {
                    Ok(BinaryResponseAndRequest::new(
                        BinaryResponse::from_value(
                            GlobalStateQueryResult::new(
                                StoredValue::EraInfo(EraInfo::new()),
                                vec![],
                            ),
                            SUPPORTED_PROTOCOL_VERSION,
                        ),
                        &[],
                        0,
                    ))
                }
                req => unimplemented!("unexpected request: {:?}", req),
            }
        }
    }
}
