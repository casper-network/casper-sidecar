use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use casper_json_rpc::{Error as RpcError, Params};
use casper_types::{BlockIdentifier, evm};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    super::{NodeClient, RpcWithParams},
    get_block_by_number::{BlockResponse, get_block},
    projection::evm_hash_to_block_hash,
    types::parse_positional_params,
};
use crate::rpcs::docs::DocExample;

static GET_BLOCK_BY_HASH_PARAMS_EXAMPLE: LazyLock<GetBlockByHashParams> =
    LazyLock::new(|| GetBlockByHashParams {
        block_hash: evm::Hash::ZERO,
        full_transactions: false,
    });

/// Params for `eth_getBlockByHash`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetBlockByHashParams {
    block_hash: evm::Hash,
    full_transactions: bool,
}

impl DocExample for GetBlockByHashParams {
    fn doc_example() -> &'static Self {
        &GET_BLOCK_BY_HASH_PARAMS_EXAMPLE
    }
}

#[derive(Deserialize)]
struct PositionalParams(evm::Hash, #[serde(default)] bool);

impl From<PositionalParams> for GetBlockByHashParams {
    fn from(params: PositionalParams) -> Self {
        GetBlockByHashParams {
            block_hash: params.0,
            full_transactions: params.1,
        }
    }
}

/// `eth_getBlockByHash`.
pub struct GetBlockByHash;

#[async_trait]
impl RpcWithParams for GetBlockByHash {
    const METHOD: &'static str = "eth_getBlockByHash";
    type RequestParams = GetBlockByHashParams;
    type ResponseResult = Option<BlockResponse>;

    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        parse_positional_params::<PositionalParams>(maybe_params).map(Into::into)
    }

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: GetBlockByHashParams,
    ) -> Result<Option<BlockResponse>, RpcError> {
        get_block(
            node_client,
            Some(BlockIdentifier::Hash(evm_hash_to_block_hash(
                params.block_hash,
            ))),
            params.full_transactions,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use casper_binary_port::{BinaryResponse, Command, InformationRequest};
    use casper_types::{
        Block, BlockSignatures, BlockWithSignatures, ChainspecRawBytes, TestBlockBuilder,
        testing::TestRng,
    };
    use serde_json::json;

    use super::*;
    use crate::rpcs::{eth::types::block_hash_to_evm_hash, test_utils::BinaryPortMock};

    #[tokio::test]
    async fn reads_block_from_metamask_hash_params() {
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().height(39).build(rng));
        let block_hash = *block.hash();
        let evm_hash = block_hash_to_evm_hash(block_hash);
        let client = Arc::new(BinaryPortMock::new());
        add_chainspec(&client).await;
        client
            .add_block_with_signatures(
                BlockWithSignatures::new(block, BlockSignatures::random(rng)),
                InformationRequest::BlockWithSignatures(Some(BlockIdentifier::Hash(block_hash))),
            )
            .await;

        let params = GetBlockByHash::try_parse_params(Some(Params::Array(vec![
            json!(format!("0x{}", evm_hash.to_hex_string())),
            json!(false),
        ])))
        .expect("MetaMask block-by-hash params should parse");
        let response = GetBlockByHash::do_handle_request(client.clone(), params)
            .await
            .expect("block lookup should succeed")
            .expect("block should exist");

        let response = serde_json::to_value(response).expect("block response should serialize");
        assert_eq!(response["hash"], json!(evm_hash));
        assert_eq!(response["number"], json!("0x27"));
        client.verify_no_lingering().await;
    }

    async fn add_chainspec(client: &BinaryPortMock) {
        let request = InformationRequest::ChainspecRawBytes
            .try_into()
            .expect("chainspec information request should convert");
        let chainspec = ChainspecRawBytes::new(
            br#"
[evm]
enabled = true
chain_id = 7
spec = "prague"
block_gas_limit = 30000000
base_fee = 5000
wei_per_mote = 1000000000
transaction_lanes = [[100, 131072, 1024, 1000000, 50]]
"#
            .to_vec()
            .into(),
            None,
            None,
        );
        client
            .when_then(Command::Get(request), BinaryResponse::from_value(chainspec))
            .await;
    }
}
