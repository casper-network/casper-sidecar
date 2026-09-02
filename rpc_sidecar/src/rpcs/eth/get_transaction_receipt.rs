use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use casper_json_rpc::{Error as RpcError, Params};
use casper_types::evm;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    super::{NodeClient, RpcWithParams},
    eth_u256::EthU256,
    projection::{LogResponse, ProjectedReceipt, project_transaction_receipt},
    types::{EthAddress, HexData, parse_positional_params},
};
use crate::rpcs::docs::DocExample;

static GET_TRANSACTION_RECEIPT_PARAMS_EXAMPLE: LazyLock<GetTransactionReceiptParams> =
    LazyLock::new(|| GetTransactionReceiptParams {
        transaction_hash: evm::Hash::ZERO,
    });
static RECEIPT_EXAMPLE: LazyLock<Option<TransactionReceiptResponse>> = LazyLock::new(|| None);

/// Params for `eth_getTransactionReceipt`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetTransactionReceiptParams {
    transaction_hash: evm::Hash,
}

impl GetTransactionReceiptParams {
    fn transaction_hash(&self) -> evm::Hash {
        self.transaction_hash
    }
}

impl DocExample for GetTransactionReceiptParams {
    fn doc_example() -> &'static Self {
        &GET_TRANSACTION_RECEIPT_PARAMS_EXAMPLE
    }
}

/// Ethereum transaction receipt response.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TransactionReceiptResponse {
    #[serde(rename = "type")]
    transaction_type: EthU256,
    transaction_hash: evm::Hash,
    block_hash: evm::Hash,
    block_number: EthU256,
    from: EthAddress,
    to: Option<EthAddress>,
    contract_address: Option<EthAddress>,
    status: EthU256,
    gas_used: EthU256,
    effective_gas_price: EthU256,
    logs: Vec<LogResponse>,
    logs_bloom: HexData,
    transaction_index: EthU256,
    cumulative_gas_used: EthU256,
}

impl DocExample for Option<TransactionReceiptResponse> {
    fn doc_example() -> &'static Self {
        &RECEIPT_EXAMPLE
    }
}

impl From<ProjectedReceipt> for TransactionReceiptResponse {
    fn from(receipt: ProjectedReceipt) -> Self {
        TransactionReceiptResponse {
            transaction_type: receipt.transaction_type,
            transaction_hash: receipt.transaction_hash,
            block_hash: receipt.block_hash,
            block_number: receipt.block_number,
            from: receipt.from,
            to: receipt.to,
            contract_address: receipt.contract_address,
            status: receipt.status,
            gas_used: receipt.gas_used,
            effective_gas_price: receipt.effective_gas_price,
            logs: receipt.logs,
            logs_bloom: receipt.logs_bloom,
            transaction_index: receipt.transaction_index,
            cumulative_gas_used: receipt.cumulative_gas_used,
        }
    }
}

/// `eth_getTransactionReceipt`.
pub struct GetTransactionReceipt;

#[async_trait]
impl RpcWithParams for GetTransactionReceipt {
    const METHOD: &'static str = "eth_getTransactionReceipt";
    type RequestParams = GetTransactionReceiptParams;
    type ResponseResult = Option<TransactionReceiptResponse>;

    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        let (transaction_hash,) = parse_positional_params::<(evm::Hash,)>(maybe_params)?;
        Ok(GetTransactionReceiptParams { transaction_hash })
    }

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: GetTransactionReceiptParams,
    ) -> Result<Option<TransactionReceiptResponse>, RpcError> {
        let Some((_block_hash, receipt)) =
            project_transaction_receipt(node_client, params.transaction_hash()).await?
        else {
            return Ok(None);
        };

        Ok(Some(TransactionReceiptResponse::from(receipt)))
    }
}
