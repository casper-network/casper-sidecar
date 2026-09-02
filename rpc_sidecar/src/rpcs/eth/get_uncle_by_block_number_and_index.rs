use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use casper_json_rpc::{Error as RpcError, Params};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    super::{NodeClient, RpcWithParams},
    eth_u256::EthU256,
    get_block_by_number::BlockResponse,
    types::{BlockNumberParam, BlockTag, parse_positional_params},
};
use crate::rpcs::docs::DocExample;

static GET_UNCLE_BY_BLOCK_NUMBER_AND_INDEX_PARAMS_EXAMPLE: LazyLock<
    GetUncleByBlockNumberAndIndexParams,
> = LazyLock::new(|| GetUncleByBlockNumberAndIndexParams {
    block: BlockNumberParam::Tag(BlockTag::Latest),
    index: EthU256::ZERO,
});

/// Params for `eth_getUncleByBlockNumberAndIndex`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetUncleByBlockNumberAndIndexParams {
    block: BlockNumberParam,
    index: EthU256,
}

impl DocExample for GetUncleByBlockNumberAndIndexParams {
    fn doc_example() -> &'static Self {
        &GET_UNCLE_BY_BLOCK_NUMBER_AND_INDEX_PARAMS_EXAMPLE
    }
}

/// `eth_getUncleByBlockNumberAndIndex`.
///
/// Casper blocks always have zero uncles. This rpc method will always return None
pub struct GetUncleByBlockNumberAndIndex;

#[async_trait]
impl RpcWithParams for GetUncleByBlockNumberAndIndex {
    const METHOD: &'static str = "eth_getUncleByBlockNumberAndIndex";
    type RequestParams = GetUncleByBlockNumberAndIndexParams;
    type ResponseResult = Option<BlockResponse>;

    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        let (block, index) = parse_positional_params::<(BlockNumberParam, EthU256)>(maybe_params)?;
        Ok(GetUncleByBlockNumberAndIndexParams { block, index })
    }

    async fn do_handle_request(
        _node_client: Arc<dyn NodeClient>,
        _params: GetUncleByBlockNumberAndIndexParams,
    ) -> Result<Option<BlockResponse>, RpcError> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::rpcs::test_utils::BinaryPortMock;

    #[tokio::test]
    async fn always_returns_null_without_querying_the_node() {
        let client = Arc::new(BinaryPortMock::new());
        let uncle = GetUncleByBlockNumberAndIndex::do_handle_request(
            client.clone(),
            GetUncleByBlockNumberAndIndexParams {
                block: BlockNumberParam::Tag(BlockTag::Latest),
                index: EthU256::ZERO,
            },
        )
        .await
        .unwrap();

        assert_eq!(uncle, None);
        client.verify_no_lingering().await;
    }

    #[test]
    fn requires_a_block_selector_and_an_index() {
        let error = GetUncleByBlockNumberAndIndex::try_parse_params(Some(
            casper_json_rpc::Params::Array(vec![json!("latest")]),
        ))
        .expect_err("a missing index must be rejected");
        assert_eq!(
            error.code(),
            casper_json_rpc::ReservedErrorCode::InvalidParams as i64
        );
    }
}
