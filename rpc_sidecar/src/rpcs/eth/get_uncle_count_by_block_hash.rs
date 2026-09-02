use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use casper_json_rpc::{Error as RpcError, Params};
use casper_types::{BlockIdentifier, evm};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    super::{NodeClient, RpcWithParams},
    eth_u256::EthU256,
    projection::{block_exists, evm_hash_to_block_hash},
    types::parse_positional_params,
};
use crate::rpcs::docs::DocExample;

static GET_UNCLE_COUNT_BY_BLOCK_HASH_PARAMS_EXAMPLE: LazyLock<GetUncleCountByBlockHashParams> =
    LazyLock::new(|| GetUncleCountByBlockHashParams {
        block_hash: evm::Hash::ZERO,
    });

/// Params for `eth_getUncleCountByBlockHash`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetUncleCountByBlockHashParams {
    block_hash: evm::Hash,
}

impl DocExample for GetUncleCountByBlockHashParams {
    fn doc_example() -> &'static Self {
        &GET_UNCLE_COUNT_BY_BLOCK_HASH_PARAMS_EXAMPLE
    }
}

/// `eth_getUncleCountByBlockHash`.
///
/// Casper blocks never have uncles, so this returns `0x0` for a known block, or `null` if no
/// such block is known.
pub struct GetUncleCountByBlockHash;

#[async_trait]
impl RpcWithParams for GetUncleCountByBlockHash {
    const METHOD: &'static str = "eth_getUncleCountByBlockHash";
    type RequestParams = GetUncleCountByBlockHashParams;
    type ResponseResult = Option<EthU256>;

    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        let (block_hash,) = parse_positional_params::<(evm::Hash,)>(maybe_params)?;
        Ok(GetUncleCountByBlockHashParams { block_hash })
    }

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: GetUncleCountByBlockHashParams,
    ) -> Result<Option<EthU256>, RpcError> {
        let identifier = BlockIdentifier::Hash(evm_hash_to_block_hash(params.block_hash));
        Ok(block_exists(node_client.as_ref(), Some(identifier))
            .await?
            .then_some(EthU256::ZERO))
    }
}

#[cfg(test)]
mod tests {
    use casper_binary_port::{BinaryResponse, Command, InformationRequest};
    use casper_types::{Block, BlockHeader, BlockIdentifier, TestBlockBuilder, testing::TestRng};
    use serde_json::json;

    use super::*;
    use crate::rpcs::{eth::types::block_hash_to_evm_hash, test_utils::BinaryPortMock};

    #[tokio::test]
    async fn known_block_reports_zero() {
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().height(42).build(rng));
        let client = Arc::new(BinaryPortMock::new());
        client
            .add_block_header_req_res(
                block.clone_header(),
                InformationRequest::BlockHeader(Some(BlockIdentifier::Hash(*block.hash()))),
            )
            .await;

        let count = GetUncleCountByBlockHash::do_handle_request(
            client.clone(),
            GetUncleCountByBlockHashParams {
                block_hash: block_hash_to_evm_hash(*block.hash()),
            },
        )
        .await
        .unwrap()
        .expect("a known block reports a count");

        assert_eq!(count, EthU256::ZERO);
        assert_eq!(serde_json::to_value(count).unwrap(), json!("0x0"));
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn unknown_block_returns_null() {
        let hash = evm::Hash::new([0x44; evm::HASH_LENGTH]);
        let client = Arc::new(BinaryPortMock::new());
        let request = InformationRequest::BlockHeader(Some(BlockIdentifier::Hash(
            evm_hash_to_block_hash(hash),
        )))
        .try_into()
        .unwrap();
        client
            .when_then(
                Command::Get(request),
                BinaryResponse::from_option(None::<BlockHeader>),
            )
            .await;

        let count = GetUncleCountByBlockHash::do_handle_request(
            client.clone(),
            GetUncleCountByBlockHashParams { block_hash: hash },
        )
        .await
        .expect("a nonexistent block must resolve to null, not an error");

        assert_eq!(count, None);
        client.verify_no_lingering().await;
    }

    #[test]
    fn requires_a_block_hash() {
        let error = GetUncleCountByBlockHash::try_parse_params(Some(casper_json_rpc::Params::Array(
            Vec::new(),
        )))
        .expect_err("an omitted block hash must be rejected");
        assert_eq!(
            error.code(),
            casper_json_rpc::ReservedErrorCode::InvalidParams as i64
        );
    }
}
