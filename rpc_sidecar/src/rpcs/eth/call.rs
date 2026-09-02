use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use casper_binary_port::EvmSpeculativeExecutionResult;
use casper_json_rpc::{Error as RpcError, ErrorCodeT, Params, ReservedErrorCode};
use casper_types::{
    BlockIdentifier, EvmAddr, EvmConfig, EvmTransaction, GlobalStateIdentifier, Key, TimeDiff,
    Timestamp, U256, evm,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    super::{Error as RpcServerError, NodeClient, RpcWithParams},
    config::read_evm_config,
    eth_u256::EthU256,
    types::{
        BlockNumberParam, BlockTag, DEFAULT_ETH_CALL_GAS_LIMIT, EthAddress, HexData, PendingPolicy,
        StateBlockParam, internal_error, invalid_params, parse_positional_params,
    },
};
use crate::{ClientError, rpcs::docs::DocExample};

static CALL_PARAMS_EXAMPLE: LazyLock<CallParams> = LazyLock::new(|| CallParams {
    call: CallObject {
        from: Some(EthAddress::from(evm::Address::ZERO)),
        to: None,
        data: Some(HexData::from(Vec::new())),
        input: None,
        value: None,
        gas: None,
        gas_price: None,
    },
    block: StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Latest)),
});

const DEFAULT_EVM_CALL_TTL: TimeDiff = TimeDiff::from_seconds(300);

/// Call object accepted by `eth_call` and `eth_estimateGas`.
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CallObject {
    from: Option<EthAddress>,
    to: Option<EthAddress>,
    data: Option<HexData>,
    input: Option<HexData>,
    value: Option<EthU256>,
    gas: Option<EthU256>,
    gas_price: Option<EthU256>,
}

impl CallObject {
    fn from(&self) -> evm::Address {
        self.from
            .map(EthAddress::into_inner)
            .unwrap_or(evm::Address::ZERO)
    }

    fn to(&self) -> Option<evm::Address> {
        self.to.map(EthAddress::into_inner)
    }

    fn input(&self) -> Result<Vec<u8>, RpcError> {
        match (&self.data, &self.input) {
            (Some(data), Some(input)) if data != input => Err(invalid_params(
                "eth_call data and input fields must match when both are supplied",
            )),
            (Some(data), _) => Ok(data.clone().into_bytes()),
            (_, Some(input)) => Ok(input.clone().into_bytes()),
            (None, None) => Ok(Vec::new()),
        }
    }

    fn value(&self) -> U256 {
        self.value
            .as_ref()
            .map(|value| value.value())
            .unwrap_or_else(U256::zero)
    }

    fn gas_limit(&self) -> Result<u64, RpcError> {
        self.gas
            .map(EthU256::as_u64)
            .transpose()
            .map_err(invalid_params)
            .map(|maybe_gas| maybe_gas.unwrap_or(DEFAULT_ETH_CALL_GAS_LIMIT))
    }

    fn gas_price(&self, default_base_fee: u128) -> Result<u128, RpcError> {
        self.gas_price
            .map(eth_u256_to_u128)
            .transpose()
            .map(|maybe_gas_price| maybe_gas_price.unwrap_or(default_base_fee))
    }
}

/// Params for `eth_call`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CallParams {
    call: CallObject,
    #[serde(default)]
    block: StateBlockParam,
}

impl CallParams {
    fn call(&self) -> &CallObject {
        &self.call
    }
}

impl DocExample for CallParams {
    fn doc_example() -> &'static Self {
        &CALL_PARAMS_EXAMPLE
    }
}

#[derive(Deserialize)]
struct PositionalParams(CallObject, #[serde(default)] StateBlockParam);

impl From<PositionalParams> for CallParams {
    fn from(params: PositionalParams) -> Self {
        CallParams {
            call: params.0,
            block: params.1,
        }
    }
}

fn eth_u256_to_u128(value: EthU256) -> Result<u128, RpcError> {
    let value = value.value();
    if value > U256::from(u128::MAX) {
        return Err(invalid_params("quantity exceeds u128"));
    }
    let mut bytes = [0u8; 32];
    value.to_big_endian(&mut bytes);
    Ok(u128::from_be_bytes(
        bytes[16..]
            .try_into()
            .expect("slice should contain exactly sixteen bytes"),
    ))
}

pub(super) fn new_evm_call_transaction(
    call: &CallObject,
    evm_config: EvmConfig,
) -> Result<EvmTransaction, RpcError> {
    let from = call.from();
    Ok(EvmTransaction::new_unsigned_call(
        Timestamp::zero(),
        DEFAULT_EVM_CALL_TTL,
        evm_config.chain_id,
        from,
        call.to(),
        call.value(),
        call.input()?,
        call.gas_limit()?,
        call.gas_price(evm_config.base_fee_wei())?,
    ))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EthCallError {
    message: String,
    data: HexData,
    gas_used: EthU256,
}

#[derive(Copy, Clone, Debug, Deserialize, Eq, PartialEq)]
#[repr(i64)]
enum EthExecutionErrorCode {
    ExecutionReverted = 3,
}

impl From<EthExecutionErrorCode> for (i64, &'static str) {
    fn from(error_code: EthExecutionErrorCode) -> Self {
        match error_code {
            EthExecutionErrorCode::ExecutionReverted => (error_code as i64, "execution reverted"),
        }
    }
}

impl ErrorCodeT for EthExecutionErrorCode {}

pub(super) async fn execute_evm_call(
    node_client: &dyn NodeClient,
    call: &CallObject,
    block_identifier: Option<BlockIdentifier>,
) -> Result<EvmSpeculativeExecutionResult, RpcError> {
    let evm_config = read_evm_config(node_client).await?;
    let transaction = new_evm_call_transaction(call, evm_config)?;
    match node_client.evm_call(transaction, block_identifier).await {
        Ok(result) => Ok(result),
        // Explicit selectors have already resolved to a known block header.  A subsequent
        // not-found response therefore means that block's historical state is unavailable.
        Err(ClientError::NotFound) if block_identifier.is_some() => Err(
            RpcServerError::NodeRequest("EVM call state", ClientError::UnknownStateRootHash).into(),
        ),
        Err(ClientError::NotFound) => {
            let available_block_range = node_client
                .read_available_block_range()
                .await
                .map_err(internal_error)?;
            Err(RpcServerError::NoBlockFound(block_identifier, available_block_range).into())
        }
        Err(ClientError::UnsupportedRequest | ClientError::MalformedCommand)
            if block_identifier.is_some() =>
        {
            Err(invalid_params(
                "eth_call is not supported at the requested block",
            ))
        }
        Err(error) => Err(internal_error(error)),
    }
}

async fn ensure_evm_call_state_available(
    node_client: &dyn NodeClient,
    block_identifier: BlockIdentifier,
) -> Result<(), RpcError> {
    // Speculative execution currently collapses a missing historical state root into a generic
    // transaction error.  Probe the selected root first so callers retain the existing
    // NoSuchStateRoot JSON-RPC error instead.
    node_client
        .query_global_state(
            Some(GlobalStateIdentifier::from(block_identifier)),
            Key::Evm(EvmAddr::Account(evm::Address::ZERO)),
            vec![],
        )
        .await
        .map(|_| ())
        .map_err(|error| RpcServerError::NodeRequest("EVM call state", error).into())
}

pub(super) fn ensure_evm_call_succeeded(
    result: &EvmSpeculativeExecutionResult,
) -> Result<(), RpcError> {
    let receipt = result.evm_receipt();
    if receipt.status.is_success() {
        return Ok(());
    }
    if matches!(receipt.status, evm::ReceiptStatus::Revert) {
        return Err(RpcError::new(
            EthExecutionErrorCode::ExecutionReverted,
            HexData::from(result.evm_output()),
        ));
    }
    Err(RpcError::new(
        ReservedErrorCode::InternalError,
        EthCallError {
            message: receipt
                .status
                .message()
                .unwrap_or("EVM call failed")
                .to_string(),
            data: HexData::from(result.evm_output()),
            gas_used: EthU256::from(receipt.gas_used),
        },
    ))
}

/// `eth_call`.
pub struct Call;

#[async_trait]
impl RpcWithParams for Call {
    const METHOD: &'static str = "eth_call";
    type RequestParams = CallParams;
    type ResponseResult = HexData;

    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        parse_positional_params::<PositionalParams>(maybe_params).map(Into::into)
    }

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: CallParams,
    ) -> Result<HexData, RpcError> {
        let block_identifier = params
            .block
            .resolve_block_identifier(node_client.as_ref(), PendingPolicy::Reject)
            .await?;
        if let Some(block_identifier) = block_identifier {
            ensure_evm_call_state_available(node_client.as_ref(), block_identifier).await?;
        }
        let result =
            execute_evm_call(node_client.as_ref(), params.call(), block_identifier).await?;
        ensure_evm_call_succeeded(&result)?;
        Ok(HexData::from(result.evm_output()))
    }
}

#[cfg(test)]
mod tests {
    use casper_binary_port::{
        BinaryResponse, Command, ErrorCode as BinaryPortErrorCode, GetRequest,
        GlobalStateEntityQualifier, GlobalStateQueryResult, GlobalStateRequest, InformationRequest,
    };
    use casper_types::{
        AvailableBlockRange, Block, BlockHash, BlockHeader, ChainspecRawBytes, Gas,
        TestBlockBuilder,
        bytesrepr::Bytes,
        evm::{Receipt, ReceiptStatus},
        execution::Effects,
        testing::TestRng,
    };
    use serde_json::json;

    use super::*;
    use crate::rpcs::{
        eth::types::{BlockHashParam, block_hash_to_evm_hash},
        test_utils::BinaryPortMock,
    };

    const EVM_CHAIN_ID: u64 = 7;
    const EVM_BASE_FEE: u64 = 3;
    const EVM_WEI_PER_MOTE: u64 = 1_000_000_000;

    #[test]
    fn eth_call_accepts_numeric_block_height() {
        let params = Call::try_parse_params(Some(Params::Array(vec![
            json!({
                "to": format!("0x{}", "01".repeat(evm::ADDRESS_LENGTH)),
                "data": "0x95d89b41",
            }),
            json!("0xc"),
        ])))
        .expect("numeric block selector should parse");

        assert_eq!(
            params.block,
            StateBlockParam::Number(BlockNumberParam::Height(EthU256::from(12u64)))
        );
    }

    #[test]
    fn eth_call_accepts_block_hash_object() {
        let evm_hash = evm::Hash::new([7; evm::HASH_LENGTH]);
        let params = Call::try_parse_params(Some(Params::Array(vec![
            json!({}),
            json!({
                "blockHash": format!("0x{}", evm_hash.to_hex_string()),
                "requireCanonical": true,
            }),
        ])))
        .expect("block hash selector should parse");

        assert_eq!(
            params.block,
            StateBlockParam::HashObject(BlockHashParam {
                block_hash: evm_hash,
                require_canonical: true,
            })
        );
    }

    #[test]
    fn eth_call_defaults_to_latest_block() {
        let params = Call::try_parse_params(Some(Params::Array(vec![json!({})])))
            .expect("omitted block selector should default to latest");

        assert_eq!(
            params.block,
            StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Latest))
        );
    }

    #[test]
    fn eth_call_returns_geth_compatible_revert_error() {
        for (output, expected_data) in [(Vec::new(), "0x"), (vec![0xde, 0xad], "0xdead")] {
            let error = ensure_evm_call_succeeded(&execution_result(ReceiptStatus::Revert, output))
                .expect_err("reverted EVM call should fail");

            assert_eq!(
                serde_json::to_value(error).unwrap(),
                json!({
                    "code": 3,
                    "message": "execution reverted",
                    "data": expected_data,
                })
            );
        }
    }

    #[tokio::test]
    async fn eth_call_rejects_pending_block_selector() {
        let error = StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Pending))
            .resolve_block_identifier(&BinaryPortMock::new(), PendingPolicy::Reject)
            .await
            .expect_err("pending state should be rejected");

        assert_eq!(
            error,
            invalid_params("eth_call does not support pending state")
        );
    }

    #[tokio::test]
    async fn eth_call_sends_numeric_height_to_speculative_execution() {
        let client = BinaryPortMock::new();
        let block = Block::V2(
            TestBlockBuilder::new()
                .height(12)
                .build(&mut TestRng::new()),
        );
        client
            .add_block_header_req_res(
                block.clone_header(),
                InformationRequest::BlockHeader(Some(BlockIdentifier::Height(12))),
            )
            .await;
        add_state_probe_response(
            &client,
            BlockIdentifier::Height(12),
            BinaryResponse::from_option::<GlobalStateQueryResult>(None),
        )
        .await;
        let chainspec_request = InformationRequest::ChainspecRawBytes
            .try_into()
            .expect("chainspec information request should convert");
        client
            .when_then(
                Command::Get(chainspec_request),
                BinaryResponse::from_value(chainspec()),
            )
            .await;

        let params = CallParams {
            call: CallObject::default(),
            block: StateBlockParam::Number(BlockNumberParam::Height(EthU256::from(12u64))),
        };
        let transaction =
            new_evm_call_transaction(&params.call, evm_config()).expect("transaction should build");
        client
            .when_then(
                Command::TrySpeculativeExec {
                    transaction: casper_types::Transaction::Evm(Box::new(transaction)),
                    block_identifier: Some(BlockIdentifier::Height(12)),
                },
                BinaryResponse::from_value(successful_result()),
            )
            .await;

        let result = Call::do_handle_request(Arc::new(client), params)
            .await
            .expect("historical call should succeed");

        assert_eq!(serde_json::to_value(result).unwrap(), json!("0x2a"));
    }

    #[tokio::test]
    async fn eth_call_sends_block_hash_to_speculative_execution() {
        let client = BinaryPortMock::new();
        let block = Block::V2(
            TestBlockBuilder::new()
                .height(12)
                .build(&mut TestRng::new()),
        );
        let block_hash = *block.hash();
        client
            .add_block_header_req_res(
                block.clone_header(),
                InformationRequest::BlockHeader(Some(BlockIdentifier::Hash(block_hash))),
            )
            .await;
        add_state_probe_response(
            &client,
            BlockIdentifier::Hash(block_hash),
            BinaryResponse::from_option::<GlobalStateQueryResult>(None),
        )
        .await;
        let chainspec_request = InformationRequest::ChainspecRawBytes
            .try_into()
            .expect("chainspec information request should convert");
        client
            .when_then(
                Command::Get(chainspec_request),
                BinaryResponse::from_value(chainspec()),
            )
            .await;

        let params = CallParams {
            call: CallObject::default(),
            block: StateBlockParam::Hash(block_hash_to_evm_hash(block_hash)),
        };
        let transaction =
            new_evm_call_transaction(&params.call, evm_config()).expect("transaction should build");
        client
            .when_then(
                Command::TrySpeculativeExec {
                    transaction: casper_types::Transaction::Evm(Box::new(transaction)),
                    block_identifier: Some(BlockIdentifier::Hash(block_hash)),
                },
                BinaryResponse::from_value(successful_result()),
            )
            .await;

        let result = Call::do_handle_request(Arc::new(client), params)
            .await
            .expect("call at a block hash should succeed");

        assert_eq!(serde_json::to_value(result).unwrap(), json!("0x2a"));
    }

    #[tokio::test]
    async fn eth_call_enforces_require_canonical() {
        let rng = &mut TestRng::new();
        let selected = Block::V2(TestBlockBuilder::new().height(12).build(rng));
        let canonical = Block::V2(TestBlockBuilder::new().height(12).build(rng));
        let selected_hash = *selected.hash();
        let client = BinaryPortMock::new();
        client
            .add_block_header_req_res(
                selected.clone_header(),
                InformationRequest::BlockHeader(Some(BlockIdentifier::Hash(selected_hash))),
            )
            .await;
        client
            .add_block_header_req_res(
                canonical.clone_header(),
                InformationRequest::BlockHeader(Some(BlockIdentifier::Height(12))),
            )
            .await;

        let error = Call::do_handle_request(
            Arc::new(client),
            CallParams {
                call: CallObject::default(),
                block: StateBlockParam::HashObject(BlockHashParam {
                    block_hash: block_hash_to_evm_hash(selected_hash),
                    require_canonical: true,
                }),
            },
        )
        .await
        .expect_err("eth_call must reject a noncanonical block");

        assert_eq!(error.code(), crate::rpcs::ErrorCode::InvalidBlock as i64);
    }

    #[tokio::test]
    async fn eth_call_reports_missing_historical_block() {
        let client = BinaryPortMock::new();
        let block_header_request =
            InformationRequest::BlockHeader(Some(BlockIdentifier::Height(12)))
                .try_into()
                .expect("block-header information request should convert");
        client
            .when_then(
                Command::Get(block_header_request),
                BinaryResponse::from_option(None::<BlockHeader>),
            )
            .await;

        let params = CallParams {
            call: CallObject::default(),
            block: StateBlockParam::Number(BlockNumberParam::Height(EthU256::from(12u64))),
        };
        let available_range_request = InformationRequest::AvailableBlockRange
            .try_into()
            .expect("available range information request should convert");
        client
            .when_then(
                Command::Get(available_range_request),
                BinaryResponse::from_value(AvailableBlockRange::new(20, 30)),
            )
            .await;

        let error = Call::do_handle_request(Arc::new(client), params)
            .await
            .expect_err("missing historical block should fail");

        assert_eq!(error.code(), crate::rpcs::ErrorCode::NoSuchBlock as i64);
    }

    #[tokio::test]
    async fn eth_call_reports_pruned_historical_state() {
        let client = Arc::new(BinaryPortMock::new());
        let block = Block::V2(
            TestBlockBuilder::new()
                .height(12)
                .build(&mut TestRng::new()),
        );
        client
            .add_block_header_req_res(
                block.clone_header(),
                InformationRequest::BlockHeader(Some(BlockIdentifier::Height(12))),
            )
            .await;
        add_state_probe_response(
            &client,
            BlockIdentifier::Height(12),
            BinaryResponse::new_error(BinaryPortErrorCode::RootNotFound),
        )
        .await;

        let params = CallParams {
            call: CallObject::default(),
            block: StateBlockParam::Number(BlockNumberParam::Height(EthU256::from(12u64))),
        };

        let error = Call::do_handle_request(client.clone(), params)
            .await
            .expect_err("known block with unavailable state should fail");

        assert_eq!(error.code(), crate::rpcs::ErrorCode::NoSuchStateRoot as i64);
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn eth_call_reports_unsupported_historical_execution() {
        let rng = &mut TestRng::new();
        for error_code in [
            BinaryPortErrorCode::UnsupportedRequest,
            BinaryPortErrorCode::MalformedCommand,
        ] {
            let client = BinaryPortMock::new();
            let block = Block::V2(TestBlockBuilder::new().height(12).build(rng));
            client
                .add_block_header_req_res(
                    block.clone_header(),
                    InformationRequest::BlockHeader(Some(BlockIdentifier::Height(12))),
                )
                .await;
            add_state_probe_response(
                &client,
                BlockIdentifier::Height(12),
                BinaryResponse::from_option::<GlobalStateQueryResult>(None),
            )
            .await;
            let chainspec_request = InformationRequest::ChainspecRawBytes
                .try_into()
                .expect("chainspec information request should convert");
            client
                .when_then(
                    Command::Get(chainspec_request),
                    BinaryResponse::from_value(chainspec()),
                )
                .await;

            let params = CallParams {
                call: CallObject::default(),
                block: StateBlockParam::Number(BlockNumberParam::Height(EthU256::from(12u64))),
            };
            let transaction = new_evm_call_transaction(&params.call, evm_config())
                .expect("transaction should build");
            client
                .when_then(
                    Command::TrySpeculativeExec {
                        transaction: casper_types::Transaction::Evm(Box::new(transaction)),
                        block_identifier: Some(BlockIdentifier::Height(12)),
                    },
                    BinaryResponse::new_error(error_code),
                )
                .await;

            let error = Call::do_handle_request(Arc::new(client), params)
                .await
                .expect_err("unsupported historical block should fail");

            assert_eq!(error.code(), ReservedErrorCode::InvalidParams as i64);
        }
    }

    async fn add_state_probe_response(
        client: &BinaryPortMock,
        block_identifier: BlockIdentifier,
        response: BinaryResponse,
    ) {
        let request = GlobalStateRequest::new(
            Some(GlobalStateIdentifier::from(block_identifier)),
            GlobalStateEntityQualifier::Item {
                base_key: Key::Evm(EvmAddr::Account(evm::Address::ZERO)),
                path: Vec::new(),
            },
        );
        client
            .when_then(Command::Get(GetRequest::State(Box::new(request))), response)
            .await;
    }

    #[test]
    fn eth_call_builds_unsigned_evm_transaction() {
        let from = evm::Address::new([1; evm::ADDRESS_LENGTH]);
        let to = evm::Address::new([2; evm::ADDRESS_LENGTH]);
        let input = vec![0xde, 0xad];

        let transaction = new_evm_call_transaction(
            &CallObject {
                from: Some(EthAddress::from(from)),
                to: Some(EthAddress::from(to)),
                data: Some(HexData::from(input.clone())),
                input: None,
                value: Some(EthU256::from(U256::from(1))),
                gas: Some(EthU256::from(1_000u64)),
                gas_price: None,
            },
            evm_config(),
        )
        .expect("transaction should build");

        assert!(transaction.is_unsigned_call());
        assert!(transaction.approval().is_none());
        assert_eq!(transaction.chain_id(), Some(EVM_CHAIN_ID));
        assert_eq!(transaction.from(), from);
        assert_eq!(
            transaction.initiator_addr(),
            casper_types::InitiatorAddr::Eoa(from)
        );
        assert_eq!(transaction.to(), Some(to));
        assert_eq!(transaction.value(), U256::from(1));
        assert_eq!(transaction.input(), input.as_slice());
        assert_eq!(transaction.gas_limit(), 1_000);
        assert_eq!(
            transaction.gas_price(),
            Some(u128::from(EVM_BASE_FEE) * u128::from(EVM_WEI_PER_MOTE))
        );
    }

    #[test]
    fn eth_call_uses_explicit_gas_price() {
        let gas_price = u128::from(EVM_BASE_FEE) + 1;

        let transaction = new_evm_call_transaction(
            &CallObject {
                gas_price: Some(EthU256::from(gas_price)),
                ..CallObject::default()
            },
            evm_config(),
        )
        .expect("transaction should build");

        assert_eq!(transaction.gas_price(), Some(gas_price));
    }

    #[test]
    fn eth_u256_to_u128_rejects_overflow() {
        let result = eth_u256_to_u128(EthU256::from(U256::from(u128::MAX) + U256::one()));

        assert!(matches!(
            result,
            Err(error) if error.code() == ReservedErrorCode::InvalidParams as i64
        ));
    }

    fn evm_config() -> EvmConfig {
        EvmConfig {
            enabled: true,
            chain_id: EVM_CHAIN_ID,
            base_fee: EVM_BASE_FEE,
            wei_per_mote: EVM_WEI_PER_MOTE,
            ..Default::default()
        }
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
"#
            .to_vec()
            .into(),
            None,
            None,
        )
    }

    fn successful_result() -> EvmSpeculativeExecutionResult {
        execution_result(ReceiptStatus::Success, vec![0x2a])
    }

    fn execution_result(status: ReceiptStatus, output: Vec<u8>) -> EvmSpeculativeExecutionResult {
        EvmSpeculativeExecutionResult::new(
            BlockHash::new([3; 32].into()),
            Gas::new(30_000_000u64),
            Gas::new(21_000u64),
            Effects::new(),
            None,
            Receipt {
                status,
                gas_used: 21_000,
                effective_gas_price: evm_config().base_fee_wei(),
                contract_address: None,
                logs: Vec::new(),
            },
            Bytes::from(output),
        )
    }
}
