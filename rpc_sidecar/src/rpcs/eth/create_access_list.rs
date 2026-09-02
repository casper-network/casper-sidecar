use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use casper_json_rpc::{Error as RpcError, Params};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    super::{NodeClient, RpcWithParams},
    call::CallObject,
    eth_u256::EthU256,
    transaction_response::AccessListItemResponse,
    types::{
        BlockNumberParam, BlockTag, StateBlockParam, method_not_supported, parse_positional_params,
    },
};
use crate::rpcs::docs::DocExample;

static CREATE_ACCESS_LIST_PARAMS_EXAMPLE: LazyLock<CreateAccessListParams> =
    LazyLock::new(|| CreateAccessListParams {
        call: CallObject::default(),
        block: StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Latest)),
    });

/// Params for `eth_createAccessList`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateAccessListParams {
    call: CallObject,
    #[serde(default)]
    block: StateBlockParam,
}

impl DocExample for CreateAccessListParams {
    fn doc_example() -> &'static Self {
        &CREATE_ACCESS_LIST_PARAMS_EXAMPLE
    }
}

#[derive(Deserialize)]
struct PositionalParams(CallObject, #[serde(default)] StateBlockParam);

impl From<PositionalParams> for CreateAccessListParams {
    fn from(params: PositionalParams) -> Self {
        CreateAccessListParams {
            call: params.0,
            block: params.1,
        }
    }
}

/// Response for `eth_createAccessList`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateAccessListResponse {
    access_list: Vec<AccessListItemResponse>,
    gas_used: EthU256,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

static CREATE_ACCESS_LIST_RESPONSE_EXAMPLE: LazyLock<CreateAccessListResponse> =
    LazyLock::new(|| CreateAccessListResponse {
        access_list: Vec::new(),
        gas_used: EthU256::ZERO,
        error: None,
    });

impl DocExample for CreateAccessListResponse {
    fn doc_example() -> &'static Self {
        &CREATE_ACCESS_LIST_RESPONSE_EXAMPLE
    }
}

/// `eth_createAccessList`.
///
/// Always returns an EIP-1474 `-32004` ("method not supported") error: Casper's EVM rejects any
/// transaction carrying a non-empty EIP-2930 access list, so there is never a real access list
/// to suggest.
pub struct CreateAccessList;

#[async_trait]
impl RpcWithParams for CreateAccessList {
    const METHOD: &'static str = "eth_createAccessList";
    type RequestParams = CreateAccessListParams;
    type ResponseResult = CreateAccessListResponse;

    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        parse_positional_params::<PositionalParams>(maybe_params).map(Into::into)
    }

    async fn do_handle_request(
        _node_client: Arc<dyn NodeClient>,
        _params: CreateAccessListParams,
    ) -> Result<CreateAccessListResponse, RpcError> {
        Err(method_not_supported(
            "eth_createAccessList is not supported: this chain rejects any transaction \
             carrying a non-empty EIP-2930 access list, so no access list can ever be suggested",
        ))
    }
}

#[cfg(test)]
mod tests {
    use casper_types::evm;

    use super::*;
    use crate::rpcs::{eth::types::EthApiErrorCode, test_utils::BinaryPortMock};

    #[tokio::test]
    async fn always_reports_method_not_supported() {
        let error = CreateAccessList::do_handle_request(
            Arc::new(BinaryPortMock::new()),
            CreateAccessListParams {
                call: CallObject::default(),
                block: StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Latest)),
            },
        )
        .await
        .expect_err("eth_createAccessList must always report unsupported");

        // EIP-1474's non-standard error-code table assigns `-32004` to "method not supported".
        let error = serde_json::to_value(error).unwrap();
        assert_eq!(error["code"], EthApiErrorCode::MethodNotSupported as i64);
        assert_eq!(error["message"], "method not supported");
    }

    #[test]
    fn accepts_a_call_object_and_optional_block_selector() {
        let params =
            CreateAccessList::try_parse_params(Some(casper_json_rpc::Params::Array(vec![
                serde_json::json!({
                    "to": format!("0x{}", "01".repeat(evm::ADDRESS_LENGTH)),
                }),
            ])))
            .expect("call object without a block selector should parse");

        assert_eq!(
            params.block,
            StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Latest))
        );
    }
}
