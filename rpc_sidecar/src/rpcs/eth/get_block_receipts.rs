use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use casper_json_rpc::{Error as RpcError, Params};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    super::{NodeClient, RpcWithParams},
    get_transaction_receipt::TransactionReceiptResponse,
    projection::project_block,
    types::{BlockNumberParam, BlockTag, PendingPolicy, StateBlockParam, parse_positional_params},
};
use crate::rpcs::{ErrorCode, docs::DocExample};

static GET_BLOCK_RECEIPTS_PARAMS_EXAMPLE: LazyLock<GetBlockReceiptsParams> =
    LazyLock::new(|| GetBlockReceiptsParams {
        block: StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Latest)),
    });
static BLOCK_RECEIPTS_RESPONSE_EXAMPLE: LazyLock<Option<Vec<TransactionReceiptResponse>>> =
    LazyLock::new(|| None);

/// Params for `eth_getBlockReceipts`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetBlockReceiptsParams {
    block: StateBlockParam,
}

impl DocExample for GetBlockReceiptsParams {
    fn doc_example() -> &'static Self {
        &GET_BLOCK_RECEIPTS_PARAMS_EXAMPLE
    }
}

impl DocExample for Option<Vec<TransactionReceiptResponse>> {
    fn doc_example() -> &'static Self {
        &BLOCK_RECEIPTS_RESPONSE_EXAMPLE
    }
}

/// `eth_getBlockReceipts`.
///
/// Returns the transaction receipts for
/// every transaction in the block identified by `block`, in transaction-index order, or `null`
/// if no such block is known.
pub struct GetBlockReceipts;

#[async_trait]
impl RpcWithParams for GetBlockReceipts {
    const METHOD: &'static str = "eth_getBlockReceipts";
    type RequestParams = GetBlockReceiptsParams;
    type ResponseResult = Option<Vec<TransactionReceiptResponse>>;

    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        let (block,) = parse_positional_params::<(StateBlockParam,)>(maybe_params)?;
        Ok(GetBlockReceiptsParams { block })
    }

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: GetBlockReceiptsParams,
    ) -> Result<Option<Vec<TransactionReceiptResponse>>, RpcError> {
        let identifier = match params
            .block
            .resolve_block_identifier(node_client.as_ref(), PendingPolicy::Latest)
            .await
        {
            Ok(identifier) => identifier,
            Err(error) if error.code() == ErrorCode::NoSuchBlock as i64 => return Ok(None),
            Err(error) => return Err(error),
        };
        let Some(block) = project_block(node_client, identifier, false).await? else {
            return Ok(None);
        };
        Ok(Some(
            block
                .receipts
                .into_iter()
                .map(TransactionReceiptResponse::from)
                .collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope, crypto::secp256k1};
    use alloy_eips::{Encodable2718, eip2930::AccessList};
    use alloy_primitives::{Address as AlloyAddress, B256, Bytes as AlloyBytes, TxKind, U256};
    use casper_binary_port::{
        BinaryResponse, Command, InformationRequest, TransactionWithExecutionInfo,
    };
    use casper_types::{
        Block, BlockHeader, BlockIdentifier, BlockSignatures, BlockWithSignatures, EvmTransaction,
        ExecutionInfo, TestBlockBuilder, TimeDiff, Timestamp, Transaction, TransactionHash, evm,
        execution::{EvmExecutionResult, ExecutionResult},
        testing::TestRng,
    };
    use serde_json::json;

    use super::*;
    use crate::rpcs::{
        eth::{eth_u256::EthU256, get_transaction_receipt::GetTransactionReceipt},
        test_utils::BinaryPortMock,
    };

    #[tokio::test]
    async fn block_receipts_match_individual_receipt_lookup() {
        let rng = &mut TestRng::new();
        let transaction = fixture_transaction();
        let block = Block::V2(
            TestBlockBuilder::new()
                .height(42)
                .transactions([&Transaction::from(transaction.clone())])
                .build(rng),
        );
        let mut evm_result = EvmExecutionResult::random(rng);
        evm_result.receipt.effective_gas_price = 1_500;
        let execution_info = ExecutionInfo {
            block_hash: *block.hash(),
            block_height: block.height(),
            execution_result: Some(ExecutionResult::Evm(Box::new(evm_result))),
        };

        let block_receipts_client = Arc::new(BinaryPortMock::new());
        add_projected_block_responses(
            &block_receipts_client,
            &block,
            &transaction,
            &execution_info,
            None,
            rng,
        )
        .await;
        let receipts = GetBlockReceipts::do_handle_request(
            block_receipts_client.clone(),
            GetBlockReceiptsParams {
                block: StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Latest)),
            },
        )
        .await
        .unwrap()
        .expect("known block should have receipts");
        assert_eq!(receipts.len(), 1);
        block_receipts_client.verify_no_lingering().await;

        let individual_client = Arc::new(BinaryPortMock::new());
        add_transaction_response(&individual_client, &transaction, &execution_info).await;
        individual_client
            .add_block_with_signatures(
                BlockWithSignatures::new(block.clone(), BlockSignatures::random(rng)),
                InformationRequest::BlockWithSignatures(Some(BlockIdentifier::Hash(*block.hash()))),
            )
            .await;
        add_transaction_response(&individual_client, &transaction, &execution_info).await;
        let receipt_params =
            GetTransactionReceipt::try_parse_params(Some(casper_json_rpc::Params::Array(vec![
                json!(format!("0x{}", transaction.hash().hash().to_hex_string())),
            ])))
            .expect("transaction hash should parse");
        let individual =
            GetTransactionReceipt::do_handle_request(individual_client.clone(), receipt_params)
                .await
                .unwrap()
                .expect("known transaction should have a receipt");
        individual_client.verify_no_lingering().await;

        assert_eq!(receipts[0], individual);
    }

    #[tokio::test]
    async fn unknown_block_returns_null() {
        let client = Arc::new(BinaryPortMock::new());
        let header_request = InformationRequest::BlockHeader(Some(BlockIdentifier::Height(7)))
            .try_into()
            .unwrap();
        client
            .when_then(
                Command::Get(header_request),
                BinaryResponse::from_option(None::<BlockHeader>),
            )
            .await;
        let range_request = InformationRequest::AvailableBlockRange.try_into().unwrap();
        client
            .when_then(
                Command::Get(range_request),
                BinaryResponse::from_value(casper_types::AvailableBlockRange::new(0, 5)),
            )
            .await;

        let receipts = GetBlockReceipts::do_handle_request(
            client.clone(),
            GetBlockReceiptsParams {
                block: StateBlockParam::Number(BlockNumberParam::Height(EthU256::from(7u64))),
            },
        )
        .await
        .expect("a nonexistent block must resolve to null, not an error");

        assert_eq!(receipts, None);
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn empty_block_returns_empty_array_not_null() {
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().height(42).build(rng));
        let client = Arc::new(BinaryPortMock::new());
        client
            .add_block_with_signatures(
                BlockWithSignatures::new(block, BlockSignatures::random(rng)),
                InformationRequest::BlockWithSignatures(None),
            )
            .await;

        let receipts = GetBlockReceipts::do_handle_request(
            client.clone(),
            GetBlockReceiptsParams {
                block: StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Latest)),
            },
        )
        .await
        .unwrap()
        .expect("a known, empty block must return `Some([])`, not null");

        assert!(receipts.is_empty());
        assert_eq!(serde_json::to_value(Some(receipts)).unwrap(), json!([]));
        client.verify_no_lingering().await;
    }

    #[test]
    fn requires_a_block_selector() {
        let error =
            GetBlockReceipts::try_parse_params(Some(casper_json_rpc::Params::Array(Vec::new())))
                .expect_err("an omitted block selector must be rejected, not defaulted");
        assert_eq!(
            error.code(),
            casper_json_rpc::ReservedErrorCode::InvalidParams as i64
        );
    }

    #[test]
    fn positional_params_accept_a_block_tag() {
        let params = GetBlockReceipts::try_parse_params(Some(casper_json_rpc::Params::Array(
            vec![json!("latest")],
        )))
        .expect("a block tag should parse");
        assert_eq!(
            params.block,
            StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Latest))
        );
    }

    #[test]
    fn positional_params_accept_a_block_hash() {
        let hash = evm::Hash::new([0x2a; evm::HASH_LENGTH]);
        let params = GetBlockReceipts::try_parse_params(Some(casper_json_rpc::Params::Array(
            vec![json!(format!("0x{}", hash.to_hex_string()))],
        )))
        .expect("a raw block hash should parse");
        assert_eq!(params.block, StateBlockParam::Hash(hash));
    }

    async fn add_projected_block_responses(
        client: &BinaryPortMock,
        block: &Block,
        transaction: &EvmTransaction,
        execution_info: &ExecutionInfo,
        identifier: Option<BlockIdentifier>,
        rng: &mut TestRng,
    ) {
        client
            .add_block_with_signatures(
                BlockWithSignatures::new(block.clone(), BlockSignatures::random(rng)),
                InformationRequest::BlockWithSignatures(identifier),
            )
            .await;
        add_transaction_response(client, transaction, execution_info).await;
    }

    async fn add_transaction_response(
        client: &BinaryPortMock,
        transaction: &EvmTransaction,
        execution_info: &ExecutionInfo,
    ) {
        let request = InformationRequest::Transaction {
            hash: TransactionHash::from(transaction.hash()),
            with_finalized_approvals: true,
        }
        .try_into()
        .unwrap();
        client
            .when_then(
                Command::Get(request),
                BinaryResponse::from_option(Some(TransactionWithExecutionInfo::new(
                    Transaction::from(transaction.clone()),
                    Some(execution_info.clone()),
                ))),
            )
            .await;
    }

    fn fixture_transaction() -> EvmTransaction {
        let transaction = TxEip1559 {
            chain_id: 7,
            nonce: 1,
            gas_limit: 60_000,
            max_fee_per_gas: 3_000,
            max_priority_fee_per_gas: 0,
            to: TxKind::Call(AlloyAddress::from([0x55; 20])),
            value: U256::from(13),
            access_list: AccessList::default(),
            input: AlloyBytes::from(vec![0xbe, 0xef]),
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
