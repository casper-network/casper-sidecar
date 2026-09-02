use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use casper_json_rpc::{Error as RpcError, Params};
use casper_types::evm;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    super::{NodeClient, RpcWithParams},
    eth_u256::EthU256,
    types::{
        BlockNumberParam, BlockTag, EthAddress, EthBytesMax32, HexData, StateBlockParam,
        parse_positional_params,
    },
};
use crate::rpcs::{docs::DocExample, eth::types::method_not_supported};

static GET_PROOF_PARAMS_EXAMPLE: LazyLock<GetProofParams> = LazyLock::new(|| GetProofParams {
    address: EthAddress::from(evm::Address::ZERO),
    storage_keys: Vec::new(),
    block: StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Latest)),
});

/// Params for `eth_getProof`.
///
/// Per the Ethereum Execution API specification, `block` is optional and defaults to `latest`
/// when omitted, and each storage key is a `bytesMax32` value: a compact key such as `0x1` is
/// accepted and left-padded to a full 32-byte word.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetProofParams {
    address: EthAddress,
    storage_keys: Vec<EthBytesMax32>,
    #[serde(default)]
    block: StateBlockParam,
}

impl DocExample for GetProofParams {
    fn doc_example() -> &'static Self {
        &GET_PROOF_PARAMS_EXAMPLE
    }
}

#[derive(Deserialize)]
struct PositionalParams(EthAddress, Vec<EthBytesMax32>, #[serde(default)] StateBlockParam);

impl From<PositionalParams> for GetProofParams {
    fn from(params: PositionalParams) -> Self {
        GetProofParams {
            address: params.0,
            storage_keys: params.1,
            block: params.2,
        }
    }
}

/// A single EIP-1186 Merkle-Patricia storage proof entry.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StorageProofResponse {
    key: evm::Hash,
    value: EthU256,
    proof: Vec<HexData>,
}

/// EIP-1186 account and storage proof response for `eth_getProof`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GetProofResponse {
    address: EthAddress,
    account_proof: Vec<HexData>,
    balance: EthU256,
    code_hash: evm::Hash,
    nonce: EthU256,
    storage_hash: evm::Hash,
    storage_proof: Vec<StorageProofResponse>,
}

static GET_PROOF_RESPONSE_EXAMPLE: LazyLock<GetProofResponse> =
    LazyLock::new(|| GetProofResponse {
        address: EthAddress::from(evm::Address::ZERO),
        account_proof: Vec::new(),
        balance: EthU256::ZERO,
        code_hash: evm::Hash::ZERO,
        nonce: EthU256::ZERO,
        storage_hash: evm::Hash::ZERO,
        storage_proof: Vec::new(),
    });

impl DocExample for GetProofResponse {
    fn doc_example() -> &'static Self {
        &GET_PROOF_RESPONSE_EXAMPLE
    }
}

/// `eth_getProof`.
///
/// Always returns an EIP-1474 `-32004` ("method not supported") error. An EIP-1186 proof cannot
/// be reproduced in a Casper environment: account and contract data is stored in a different
/// layout, hashing is blake2b-256 rather than keccak256, and global state also holds non-EVM
/// data that is opaque to the `eth_*` endpoints.
pub struct GetProof;

#[async_trait]
impl RpcWithParams for GetProof {
    const METHOD: &'static str = "eth_getProof";
    type RequestParams = GetProofParams;
    type ResponseResult = GetProofResponse;

    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        parse_positional_params::<PositionalParams>(maybe_params).map(Into::into)
    }

    async fn do_handle_request(
        _node_client: Arc<dyn NodeClient>,
        _params: GetProofParams,
    ) -> Result<GetProofResponse, RpcError> {
        Err(method_not_supported(
            "eth_getProof is not supported: an EIP-1186 Merkle-Patricia proof cannot be \
             reproduced against Casper global state (blake2b-256 hashing, non-EVM data layout)",
        ))
    }
}

#[cfg(test)]
mod tests {
    use casper_types::{U256, evm};

    use super::*;
    use crate::rpcs::{eth::types::EthApiErrorCode, test_utils::BinaryPortMock};

    #[tokio::test]
    async fn always_reports_method_not_supported() {
        let error = GetProof::do_handle_request(
            Arc::new(BinaryPortMock::new()),
            GetProofParams {
                address: EthAddress::from(evm::Address::ZERO),
                storage_keys: Vec::new(),
                block: StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Latest)),
            },
        )
        .await
        .expect_err("eth_getProof must always report unsupported");

        let error = serde_json::to_value(error).unwrap();
        assert_eq!(error["code"], EthApiErrorCode::MethodNotSupported as i64);
        assert_eq!(error["message"], "method not supported");
    }

    #[test]
    fn parses_address_storage_keys_and_block() {
        let params = GetProof::try_parse_params(Some(casper_json_rpc::Params::Array(vec![
            serde_json::json!(format!("0x{}", "01".repeat(evm::ADDRESS_LENGTH))),
            serde_json::json!([format!("0x{}", "02".repeat(evm::HASH_LENGTH))]),
            serde_json::json!("latest"),
        ])))
        .expect("address, storage keys and block should parse");

        assert_eq!(params.storage_keys.len(), 1);
        assert_eq!(
            params.block,
            StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Latest))
        );
    }

    #[test]
    fn compact_storage_keys_are_accepted_and_left_padded() {
        let params = GetProof::try_parse_params(Some(casper_json_rpc::Params::Array(vec![
            serde_json::json!(format!("0x{}", "01".repeat(evm::ADDRESS_LENGTH))),
            serde_json::json!(["0x1", format!("0x{}1", "0".repeat(63))]),
        ])))
        .expect("compact storage keys should parse");

        assert_eq!(
            params
                .storage_keys
                .iter()
                .map(|slot| slot.value())
                .collect::<Vec<_>>(),
            vec![U256::one(), U256::one()]
        );
    }

    #[test]
    fn rejects_storage_keys_wider_than_32_bytes() {
        let error = GetProof::try_parse_params(Some(casper_json_rpc::Params::Array(vec![
            serde_json::json!(format!("0x{}", "01".repeat(evm::ADDRESS_LENGTH))),
            serde_json::json!([format!("0x{}", "02".repeat(evm::HASH_LENGTH + 1))]),
        ])))
        .expect_err("an over-wide storage key should be rejected");

        assert_eq!(
            error.code(),
            casper_json_rpc::ReservedErrorCode::InvalidParams as i64
        );
    }

    #[test]
    fn block_selector_defaults_to_latest_when_omitted() {
        let params = GetProof::try_parse_params(Some(casper_json_rpc::Params::Array(vec![
            serde_json::json!(format!("0x{}", "01".repeat(evm::ADDRESS_LENGTH))),
            serde_json::json!([format!("0x{}", "02".repeat(evm::HASH_LENGTH))]),
        ])))
        .expect("omitted block selector should default to latest");

        assert_eq!(
            params.block,
            StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Latest))
        );
    }

    #[test]
    fn requires_address_and_storage_keys() {
        let address = serde_json::json!(format!("0x{}", "01".repeat(evm::ADDRESS_LENGTH)));
        for params in [vec![], vec![address.clone()]] {
            let error = GetProof::try_parse_params(Some(casper_json_rpc::Params::Array(params)))
                .expect_err("eth_getProof requires an address and storage keys");
            assert_eq!(
                error.code(),
                casper_json_rpc::ReservedErrorCode::InvalidParams as i64
            );
        }
    }
}
