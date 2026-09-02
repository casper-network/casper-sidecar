use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use casper_json_rpc::{Error as RpcError, Params};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    super::{NodeClient, RpcWithParams},
    eth_u256::EthU256,
    projection::block_exists,
    types::{BlockNumberParam, BlockTag, parse_positional_params},
};
use crate::rpcs::docs::DocExample;

static GET_UNCLE_COUNT_BY_BLOCK_NUMBER_PARAMS_EXAMPLE: LazyLock<GetUncleCountByBlockNumberParams> =
    LazyLock::new(|| GetUncleCountByBlockNumberParams {
        block: BlockNumberParam::Tag(BlockTag::Latest),
    });

/// Params for `eth_getUncleCountByBlockNumber`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetUncleCountByBlockNumberParams {
    block: BlockNumberParam,
}

impl DocExample for GetUncleCountByBlockNumberParams {
    fn doc_example() -> &'static Self {
        &GET_UNCLE_COUNT_BY_BLOCK_NUMBER_PARAMS_EXAMPLE
    }
}

/// `eth_getUncleCountByBlockNumber`.
///
/// Casper blocks never have uncles, so this returns `0x0` for a known block, or `null` if no
/// such block is known.
pub struct GetUncleCountByBlockNumber;

#[async_trait]
impl RpcWithParams for GetUncleCountByBlockNumber {
    const METHOD: &'static str = "eth_getUncleCountByBlockNumber";
    type RequestParams = GetUncleCountByBlockNumberParams;
    type ResponseResult = Option<EthU256>;

    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        let (block,) = parse_positional_params::<(BlockNumberParam,)>(maybe_params)?;
        Ok(GetUncleCountByBlockNumberParams { block })
    }

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: GetUncleCountByBlockNumberParams,
    ) -> Result<Option<EthU256>, RpcError> {
        Ok(block_exists(node_client.as_ref(), params.block.identifier()?)
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
    use crate::rpcs::test_utils::BinaryPortMock;

    #[tokio::test]
    async fn known_block_reports_zero() {
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().height(42).build(rng));
        let client = Arc::new(BinaryPortMock::new());
        client
            .add_block_header_req_res(
                block.clone_header(),
                InformationRequest::BlockHeader(None),
            )
            .await;

        let count = GetUncleCountByBlockNumber::do_handle_request(
            client.clone(),
            GetUncleCountByBlockNumberParams {
                block: BlockNumberParam::Tag(BlockTag::Latest),
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
        let client = Arc::new(BinaryPortMock::new());
        let request = InformationRequest::BlockHeader(Some(BlockIdentifier::Height(7)))
            .try_into()
            .unwrap();
        client
            .when_then(
                Command::Get(request),
                BinaryResponse::from_option(None::<BlockHeader>),
            )
            .await;

        let count = GetUncleCountByBlockNumber::do_handle_request(
            client.clone(),
            GetUncleCountByBlockNumberParams {
                block: BlockNumberParam::Height(EthU256::from(7u64)),
            },
        )
        .await
        .expect("a nonexistent block must resolve to null, not an error");

        assert_eq!(count, None);
        client.verify_no_lingering().await;
    }

    #[test]
    fn requires_a_block_selector() {
        let error = GetUncleCountByBlockNumber::try_parse_params(Some(
            casper_json_rpc::Params::Array(Vec::new()),
        ))
        .expect_err("an omitted block selector must be rejected");
        assert_eq!(
            error.code(),
            casper_json_rpc::ReservedErrorCode::InvalidParams as i64
        );
    }
}
