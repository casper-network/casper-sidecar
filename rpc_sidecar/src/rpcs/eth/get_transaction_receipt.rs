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

#[cfg(test)]
mod tests {
    use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope, crypto::secp256k1};
    use alloy_eips::{Encodable2718, eip2930::AccessList};
    use alloy_primitives::{B256, Bytes as AlloyBytes, TxKind, U256};
    use casper_binary_port::{
        BinaryResponse, Command, ErrorCode, InformationRequest, TransactionWithExecutionInfo,
    };
    use casper_json_rpc::ReservedErrorCode;
    use casper_types::{
        Block, BlockIdentifier, BlockSignatures, BlockWithSignatures, EvmTransaction,
        ExecutionInfo, TestBlockBuilder, TimeDiff, Timestamp, Transaction, TransactionHash,
        execution::{EvmExecutionResult, ExecutionResult, ExecutionResultV2},
        testing::TestRng,
    };
    use serde_json::json;

    use super::*;
    use crate::rpcs::{eth::types::block_hash_to_evm_hash, test_utils::BinaryPortMock};

    const KZG_PROBE_GAS_USED: u64 = 75_604;

    #[tokio::test]
    async fn kzg_precompile_receipt_is_null_until_execution_result_is_available() {
        let rng = &mut TestRng::new();
        let transaction = kzg_precompile_probe_transaction();
        let block = probe_block(&transaction, rng);
        let pending_info = ExecutionInfo {
            block_hash: *block.hash(),
            block_height: block.height(),
            execution_result: None,
        };
        let client = Arc::new(BinaryPortMock::new());
        add_transaction_response(&client, &transaction, &pending_info).await;

        let receipt = lookup_receipt(client.clone(), &transaction)
            .await
            .expect("a pending KZG probe receipt must return null, not Internal error");
        assert_eq!(serde_json::to_value(receipt).unwrap(), json!(null));
        client.verify_no_lingering().await;

        assert_successful_kzg_receipt(client, &transaction, &block, rng).await;
    }

    #[tokio::test]
    async fn kzg_precompile_receipt_is_null_until_block_is_available() {
        let rng = &mut TestRng::new();
        let transaction = kzg_precompile_probe_transaction();
        let block = probe_block(&transaction, rng);
        let client = Arc::new(BinaryPortMock::new());
        add_transaction_response(
            &client,
            &transaction,
            &successful_execution_info(&block, rng),
        )
        .await;
        client
            .when_then(
                Command::Get(
                    InformationRequest::BlockWithSignatures(Some(BlockIdentifier::Hash(
                        *block.hash(),
                    )))
                    .try_into()
                    .unwrap(),
                ),
                BinaryResponse::from_option(None::<BlockWithSignatures>),
            )
            .await;

        let receipt = lookup_receipt(client.clone(), &transaction)
            .await
            .expect("a KZG probe awaiting its block must return null, not Internal error");
        assert_eq!(serde_json::to_value(receipt).unwrap(), json!(null));
        client.verify_no_lingering().await;

        assert_successful_kzg_receipt(client, &transaction, &block, rng).await;
    }

    #[tokio::test]
    async fn non_evm_execution_result_remains_an_internal_error() {
        let rng = &mut TestRng::new();
        let transaction = kzg_precompile_probe_transaction();
        let block = probe_block(&transaction, rng);
        let client = Arc::new(BinaryPortMock::new());
        add_transaction_response(
            &client,
            &transaction,
            &ExecutionInfo {
                block_hash: *block.hash(),
                block_height: block.height(),
                execution_result: Some(ExecutionResult::V2(Box::new(ExecutionResultV2::random(
                    rng,
                )))),
            },
        )
        .await;

        let error = lookup_receipt(client.clone(), &transaction)
            .await
            .expect_err("a non-EVM result is inconsistent, not a pending receipt");
        assert_eq!(
            error,
            RpcError::new(
                ReservedErrorCode::InternalError,
                "EVM transaction did not resolve to EVM execution result",
            )
        );
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn node_failure_is_not_reported_as_a_pending_receipt() {
        let transaction = kzg_precompile_probe_transaction();
        let client = Arc::new(BinaryPortMock::new());
        client
            .when_then(
                transaction_request(&transaction),
                BinaryResponse::new_error(ErrorCode::InternalError),
            )
            .await;

        let error = lookup_receipt(client.clone(), &transaction)
            .await
            .expect_err("node failures must remain errors, not null receipts");
        assert_eq!(error.code(), ReservedErrorCode::InternalError as i64);
        client.verify_no_lingering().await;
    }

    async fn lookup_receipt(
        client: Arc<BinaryPortMock>,
        transaction: &EvmTransaction,
    ) -> Result<Option<TransactionReceiptResponse>, RpcError> {
        GetTransactionReceipt::do_handle_request(
            client,
            GetTransactionReceiptParams {
                transaction_hash: transaction.hash().hash(),
            },
        )
        .await
    }

    async fn assert_successful_kzg_receipt(
        client: Arc<BinaryPortMock>,
        transaction: &EvmTransaction,
        block: &Block,
        rng: &mut TestRng,
    ) {
        let execution_info = successful_execution_info(block, rng);
        add_transaction_response(&client, transaction, &execution_info).await;
        client
            .add_block_with_signatures(
                BlockWithSignatures::new(block.clone(), BlockSignatures::random(rng)),
                InformationRequest::BlockWithSignatures(Some(BlockIdentifier::Hash(*block.hash()))),
            )
            .await;
        add_transaction_response(&client, transaction, &execution_info).await;

        let receipt = lookup_receipt(client.clone(), transaction)
            .await
            .expect("a successful KZG probe must not produce an RPC error")
            .expect("the complete execution result and block must produce a receipt");
        let receipt = serde_json::to_value(receipt).unwrap();
        assert_eq!(receipt["transactionHash"], json!(transaction.hash().hash()));
        assert_eq!(
            receipt["blockHash"],
            json!(block_hash_to_evm_hash(block.hash()))
        );
        assert_eq!(receipt["type"], json!("0x2"));
        assert_eq!(receipt["status"], json!("0x1"));
        assert_eq!(
            receipt["gasUsed"],
            json!(format!("{KZG_PROBE_GAS_USED:#x}"))
        );
        assert_eq!(receipt["contractAddress"], json!(null));
        client.verify_no_lingering().await;
    }

    fn probe_block(transaction: &EvmTransaction, rng: &mut TestRng) -> Block {
        Block::V2(
            TestBlockBuilder::new()
                .height(29)
                .transactions([&Transaction::from(transaction.clone())])
                .build(rng),
        )
    }

    fn successful_execution_info(block: &Block, rng: &mut TestRng) -> ExecutionInfo {
        let mut result = EvmExecutionResult::random(rng);
        result.error_message = None;
        result.receipt = evm::Receipt {
            status: evm::ReceiptStatus::Success,
            gas_used: KZG_PROBE_GAS_USED,
            effective_gas_price: 5_000_000_000_000,
            contract_address: None,
            logs: Vec::new(),
        };
        ExecutionInfo {
            block_hash: *block.hash(),
            block_height: block.height(),
            execution_result: Some(ExecutionResult::Evm(Box::new(result))),
        }
    }

    fn transaction_request(transaction: &EvmTransaction) -> Command {
        Command::Get(
            InformationRequest::Transaction {
                hash: TransactionHash::from(transaction.hash()),
                with_finalized_approvals: true,
            }
            .try_into()
            .unwrap(),
        )
    }

    async fn add_transaction_response(
        client: &BinaryPortMock,
        transaction: &EvmTransaction,
        execution_info: &ExecutionInfo,
    ) {
        client
            .when_then(
                transaction_request(transaction),
                BinaryResponse::from_option(Some(TransactionWithExecutionInfo::new(
                    Transaction::from(transaction.clone()),
                    Some(execution_info.clone()),
                ))),
            )
            .await;
    }

    fn kzg_precompile_probe_transaction() -> EvmTransaction {
        // The conformance report's proxies/kzg/zero-poly probe calls
        // PrecompileProbe.probeTx(0x0a, proof) in an ordinary type-2 transaction.
        // Its zero-polynomial opening uses infinity for both commitment and proof;
        // it is not a type-3 blob transaction. The observed receipt used 75,604 gas.
        let mut input = vec![0x7f, 0xdb, 0xf3, 0x89];
        input.extend(U256::from(10).to_be_bytes::<32>());
        input.extend(U256::from(64).to_be_bytes::<32>());
        input.extend(U256::from(192).to_be_bytes::<32>());
        input.extend(
            base16::decode("010657f37554c781402a22917dee2f75def7ab966d7b770905398eba3c444014")
                .unwrap(),
        );
        input.extend([0; 64]); // z = 0, y = 0.
        for _ in 0..2 {
            input.push(0xc0);
            input.extend([0; 47]);
        }
        let transaction = TxEip1559 {
            chain_id: 1_129_533_445,
            nonce: 1,
            gas_limit: 119_505,
            max_fee_per_gas: 10_000_000_000_000,
            max_priority_fee_per_gas: 0,
            to: TxKind::Call("e54fa56775726a027b5dad5510a675ffa8f69fa6".parse().unwrap()),
            value: U256::ZERO,
            access_list: AccessList::default(),
            input: AlloyBytes::from(input),
        };
        let signature =
            secp256k1::sign_message(B256::from([7; 32]), transaction.signature_hash()).unwrap();
        let envelope: TxEnvelope = transaction.into_signed(signature).into();
        EvmTransaction::from_signed_rlp(
            envelope.encoded_2718(),
            Timestamp::from(1_000),
            TimeDiff::from_seconds(300),
        )
        .unwrap()
    }
}
