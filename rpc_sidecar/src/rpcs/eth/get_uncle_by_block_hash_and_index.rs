use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use casper_json_rpc::{Error as RpcError, Params};
use casper_types::evm;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    super::{NodeClient, RpcWithParams},
    eth_u256::EthU256,
    get_block_by_number::BlockResponse,
    types::parse_positional_params,
};
use crate::rpcs::docs::DocExample;

static GET_UNCLE_BY_BLOCK_HASH_AND_INDEX_PARAMS_EXAMPLE: LazyLock<
    GetUncleByBlockHashAndIndexParams,
> = LazyLock::new(|| GetUncleByBlockHashAndIndexParams {
    block_hash: evm::Hash::ZERO,
    index: EthU256::ZERO,
});

/// Params for `eth_getUncleByBlockHashAndIndex`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetUncleByBlockHashAndIndexParams {
    block_hash: evm::Hash,
    index: EthU256,
}

impl DocExample for GetUncleByBlockHashAndIndexParams {
    fn doc_example() -> &'static Self {
        &GET_UNCLE_BY_BLOCK_HASH_AND_INDEX_PARAMS_EXAMPLE
    }
}

/// `eth_getUncleByBlockHashAndIndex`.
///
/// Casper blocks always have zero uncles. This rpc method will always return None
pub struct GetUncleByBlockHashAndIndex;

#[async_trait]
impl RpcWithParams for GetUncleByBlockHashAndIndex {
    const METHOD: &'static str = "eth_getUncleByBlockHashAndIndex";
    type RequestParams = GetUncleByBlockHashAndIndexParams;
    type ResponseResult = Option<BlockResponse>;

    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        let (block_hash, index) = parse_positional_params::<(evm::Hash, EthU256)>(maybe_params)?;
        Ok(GetUncleByBlockHashAndIndexParams { block_hash, index })
    }

    async fn do_handle_request(
        _node_client: Arc<dyn NodeClient>,
        _params: GetUncleByBlockHashAndIndexParams,
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
        let uncle = GetUncleByBlockHashAndIndex::do_handle_request(
            client.clone(),
            GetUncleByBlockHashAndIndexParams {
                block_hash: evm::Hash::new([0x2a; evm::HASH_LENGTH]),
                index: EthU256::ZERO,
            },
        )
        .await
        .unwrap();

        assert_eq!(uncle, None);
        client.verify_no_lingering().await;
    }

    #[test]
    fn requires_a_block_hash_and_an_index() {
        let error = GetUncleByBlockHashAndIndex::try_parse_params(Some(
            casper_json_rpc::Params::Array(vec![json!(format!("0x{}", "2a".repeat(32)))]),
        ))
        .expect_err("a missing index must be rejected");
        assert_eq!(
            error.code(),
            casper_json_rpc::ReservedErrorCode::InvalidParams as i64
        );
    }
}
