use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use casper_json_rpc::{Error as RpcError, Params};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    super::{NodeClient, RpcWithParams},
    call::{CallObject, ensure_evm_call_succeeded, execute_evm_call},
    eth_u256::EthU256,
    types::parse_positional_params,
};
use crate::rpcs::docs::DocExample;

static ESTIMATE_GAS_PARAMS_EXAMPLE: LazyLock<EstimateGasParams> =
    LazyLock::new(|| EstimateGasParams {
        call: CallObject::default(),
    });

/// Params for `eth_estimateGas`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct EstimateGasParams {
    call: CallObject,
}

impl DocExample for EstimateGasParams {
    fn doc_example() -> &'static Self {
        &ESTIMATE_GAS_PARAMS_EXAMPLE
    }
}

/// `eth_estimateGas`.
pub struct EstimateGas;

#[async_trait]
impl RpcWithParams for EstimateGas {
    const METHOD: &'static str = "eth_estimateGas";
    type RequestParams = EstimateGasParams;
    type ResponseResult = EthU256;

    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        let (call,) = parse_positional_params::<(CallObject,)>(maybe_params)?;
        Ok(EstimateGasParams { call })
    }

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: EstimateGasParams,
    ) -> Result<EthU256, RpcError> {
        let result = execute_evm_call(node_client.as_ref(), &params.call, None).await?;
        ensure_evm_call_succeeded(&result)?;
        Ok(EthU256::from(result.evm_receipt().gas_used))
    }
}

#[cfg(test)]
mod tests {
    use casper_binary_port::{
        BinaryResponse, Command, EvmSpeculativeExecutionResult, InformationRequest,
    };
    use casper_types::{
        BlockHash, ChainspecRawBytes, Gas,
        bytesrepr::Bytes,
        evm::{self, Receipt, ReceiptStatus},
        execution::Effects,
    };
    use serde_json::json;

    use super::*;
    use crate::rpcs::test_utils::BinaryPortMock;

    const GAS_USED: u64 = 21_000;

    #[tokio::test]
    async fn returns_gas_consumed_by_successful_speculative_execution() {
        let client = BinaryPortMock::new();
        let chainspec = chainspec();
        let chainspec_request = InformationRequest::ChainspecRawBytes
            .try_into()
            .expect("chainspec information request should convert");
        client
            .when_then(
                Command::Get(chainspec_request),
                BinaryResponse::from_value(chainspec),
            )
            .await;

        let params = EstimateGas::try_parse_params(Some(Params::Array(vec![json!({
            "from": format!("0x{}", "01".repeat(evm::ADDRESS_LENGTH)),
            "to": format!("0x{}", "02".repeat(evm::ADDRESS_LENGTH)),
            "value": "0x5"
        })])))
        .expect("estimate transaction should parse");
        let call = params.call.clone();
        let transaction =
            super::super::call::new_evm_call_transaction(&call, evm_config()).unwrap();
        let result = EvmSpeculativeExecutionResult::new(
            BlockHash::new([3; 32].into()),
            Gas::new(30_000_000u64),
            Gas::new(GAS_USED),
            Effects::new(),
            None,
            Receipt {
                status: ReceiptStatus::Success,
                gas_used: GAS_USED,
                effective_gas_price: evm_config().base_fee_wei(),
                contract_address: None,
                logs: Vec::new(),
            },
            Bytes::new(),
        );
        client
            .when_then(
                Command::TrySpeculativeExec {
                    transaction: casper_types::Transaction::Evm(Box::new(transaction)),
                    block_identifier: None,
                },
                BinaryResponse::from_value(result),
            )
            .await;

        let estimate = EstimateGas::do_handle_request(Arc::new(client), params)
            .await
            .expect("gas estimation should succeed");

        assert_eq!(estimate, EthU256::from(GAS_USED));
        assert_eq!(serde_json::to_value(estimate).unwrap(), json!("0x5208"));
    }

    #[test]
    fn parses_metamask_transaction_fields() {
        let from = format!("0x{}", "01".repeat(evm::ADDRESS_LENGTH));
        let to = format!("0x{}", "02".repeat(evm::ADDRESS_LENGTH));
        EstimateGas::try_parse_params(Some(Params::Array(vec![json!({
            "from": from,
            "to": to,
            "data": "0x",
            "value": "0x4563918244f40000",
            "type": "0x2"
        })])))
        .expect("MetaMask estimate transaction should parse");
    }

    fn chainspec() -> ChainspecRawBytes {
        ChainspecRawBytes::new(
            br#"
[evm]
enabled = true
chain_id = 7
spec = "prague"
block_gas_limit = 30000000
base_fee = 3
wei_per_mote = 1000000000
transaction_lanes = [[100, 131072, 1024, 1000000, 50]]
"#
            .to_vec()
            .into(),
            None,
            None,
        )
    }

    fn evm_config() -> casper_types::EvmConfig {
        casper_types::EvmConfig {
            enabled: true,
            chain_id: 7,
            base_fee: 3,
            wei_per_mote: 1_000_000_000,
            ..Default::default()
        }
    }
}
