use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use casper_json_rpc::{Error as RpcError, Params};
use casper_types::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    super::{NodeClient, RpcWithParams},
    eth_u256::EthU256,
    get_transaction_by_hash::get_transaction_by_hash_at,
    projection::block_evm_transaction_hashes,
    transaction_response::TransactionResponse,
    types::{BlockNumberParam, BlockTag, invalid_params, parse_positional_params},
};
use crate::rpcs::docs::DocExample;

static GET_TRANSACTION_BY_BLOCK_NUMBER_AND_INDEX_PARAMS_EXAMPLE: LazyLock<
    GetTransactionByBlockNumberAndIndexParams,
> = LazyLock::new(|| GetTransactionByBlockNumberAndIndexParams {
    block: BlockNumberParam::Tag(BlockTag::Latest),
    index: EthU256::ZERO,
});

/// Params for `eth_getTransactionByBlockNumberAndIndex`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetTransactionByBlockNumberAndIndexParams {
    block: BlockNumberParam,
    index: EthU256,
}

impl DocExample for GetTransactionByBlockNumberAndIndexParams {
    fn doc_example() -> &'static Self {
        &GET_TRANSACTION_BY_BLOCK_NUMBER_AND_INDEX_PARAMS_EXAMPLE
    }
}

/// `eth_getTransactionByBlockNumberAndIndex`.
///
/// Returns the transaction at `index` within the block identified by `blockNumber` (or a block
/// tag), or `null` if the block or the index within it does not exist.
pub struct GetTransactionByBlockNumberAndIndex;

#[async_trait]
impl RpcWithParams for GetTransactionByBlockNumberAndIndex {
    const METHOD: &'static str = "eth_getTransactionByBlockNumberAndIndex";
    type RequestParams = GetTransactionByBlockNumberAndIndexParams;
    type ResponseResult = Option<TransactionResponse>;

    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        let (block, index) = parse_positional_params::<(BlockNumberParam, EthU256)>(maybe_params)?;
        Ok(GetTransactionByBlockNumberAndIndexParams { block, index })
    }

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: GetTransactionByBlockNumberAndIndexParams,
    ) -> Result<Option<TransactionResponse>, RpcError> {
        let index = params.index.as_usize().map_err(|err| {
            invalid_params(format!(
                "argument 'index' ({:?}) is invalid: {err}",
                params.index
            ))
        })?;
        let Some(hashes) =
            block_evm_transaction_hashes(node_client.as_ref(), params.block.identifier()?).await?
        else {
            return Ok(None);
        };
        let Some(&hash) = hashes.get(index) else {
            return Ok(None);
        };
        get_transaction_by_hash_at(node_client, hash, Timestamp::now()).await
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope, crypto::secp256k1};
    use alloy_eips::{Encodable2718, eip2930::AccessList};
    use alloy_primitives::{Address as AlloyAddress, B256, Bytes as AlloyBytes, TxKind, U256};
    use casper_binary_port::{
        BinaryResponse, Command, InformationRequest, TransactionWithExecutionInfo,
    };
    use casper_json_rpc::ReservedErrorCode;
    use casper_types::{
        Block, BlockIdentifier, BlockSignatures, BlockWithSignatures, EvmTransaction,
        EvmTransactionHash, ExecutionInfo, TestBlockBuilder, TimeDiff, Transaction, TransactionHash,
        evm,
        execution::{EvmExecutionResult, ExecutionResult},
        testing::TestRng,
    };
    use serde_json::json;

    use super::*;
    use crate::rpcs::{eth::types::block_hash_to_evm_hash, test_utils::BinaryPortMock};

    #[tokio::test]
    async fn returns_the_transaction_at_the_index() {
        let rng = &mut TestRng::new();
        let target = fixture_transaction(2);
        let transactions = vec![
            Transaction::from(fixture_transaction(1)),
            Transaction::from(target.clone()),
        ];
        let block = Block::V2(
            TestBlockBuilder::new()
                .height(42)
                .transactions(&transactions)
                .build(rng),
        );
        let mut evm_result = EvmExecutionResult::random(rng);
        evm_result.receipt.effective_gas_price = 1_500;
        let execution_info = ExecutionInfo {
            block_hash: *block.hash(),
            block_height: block.height(),
            execution_result: Some(ExecutionResult::Evm(Box::new(evm_result))),
        };

        let client = Arc::new(BinaryPortMock::new());
        client
            .add_block_with_signatures(
                BlockWithSignatures::new(block.clone(), BlockSignatures::random(rng)),
                InformationRequest::BlockWithSignatures(None),
            )
            .await;
        add_transaction_response(
            &client,
            target.hash().hash(),
            Some(TransactionWithExecutionInfo::new(
                Transaction::from(target.clone()),
                Some(execution_info),
            )),
        )
        .await;
        client
            .add_block_with_signatures(
                BlockWithSignatures::new(block.clone(), BlockSignatures::random(rng)),
                InformationRequest::BlockWithSignatures(Some(BlockIdentifier::Hash(*block.hash()))),
            )
            .await;

        let response = GetTransactionByBlockNumberAndIndex::do_handle_request(
            client.clone(),
            GetTransactionByBlockNumberAndIndexParams {
                block: BlockNumberParam::Tag(BlockTag::Latest),
                index: EthU256::from(1u64),
            },
        )
        .await
        .unwrap()
        .expect("index 1 is the second transaction");
        let response = serde_json::to_value(response).unwrap();

        assert_eq!(response["transactionIndex"], json!("0x1"));
        assert_eq!(
            response["blockHash"],
            json!(block_hash_to_evm_hash(*block.hash()))
        );
        assert_eq!(response["gasPrice"], json!("0x5dc"));
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn out_of_range_index_returns_null() {
        let rng = &mut TestRng::new();
        let block = Block::V2(
            TestBlockBuilder::new()
                .height(42)
                .transactions(&[Transaction::from(fixture_transaction(1))])
                .build(rng),
        );
        let client = Arc::new(BinaryPortMock::new());
        client
            .add_block_with_signatures(
                BlockWithSignatures::new(block, BlockSignatures::random(rng)),
                InformationRequest::BlockWithSignatures(Some(BlockIdentifier::Height(42))),
            )
            .await;

        let response = GetTransactionByBlockNumberAndIndex::do_handle_request(
            client.clone(),
            GetTransactionByBlockNumberAndIndexParams {
                block: BlockNumberParam::Height(EthU256::from(42u64)),
                index: EthU256::from(9u64),
            },
        )
        .await
        .expect("an out-of-range index must resolve to null, not an error");

        assert_eq!(response, None);
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn unknown_block_returns_null() {
        let client = Arc::new(BinaryPortMock::new());
        let request = InformationRequest::BlockWithSignatures(Some(BlockIdentifier::Height(7)))
            .try_into()
            .unwrap();
        client
            .when_then(
                Command::Get(request),
                BinaryResponse::from_option(None::<BlockWithSignatures>),
            )
            .await;

        let response = GetTransactionByBlockNumberAndIndex::do_handle_request(
            client.clone(),
            GetTransactionByBlockNumberAndIndexParams {
                block: BlockNumberParam::Height(EthU256::from(7u64)),
                index: EthU256::ZERO,
            },
        )
        .await
        .expect("a nonexistent block must resolve to null, not an error");

        assert_eq!(response, None);
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn an_index_wider_than_usize_is_rejected() {
        let client = Arc::new(BinaryPortMock::new());
        let error = GetTransactionByBlockNumberAndIndex::do_handle_request(
            client.clone(),
            GetTransactionByBlockNumberAndIndexParams {
                block: BlockNumberParam::Tag(BlockTag::Latest),
                index: EthU256::from(u128::from(u64::MAX) + 1),
            },
        )
        .await
        .expect_err("an index that cannot be a usize must be an invalid-params error");

        assert_eq!(error.code(), ReservedErrorCode::InvalidParams as i64);
        client.verify_no_lingering().await;
    }

    #[test]
    fn requires_a_block_selector_and_an_index() {
        let error = GetTransactionByBlockNumberAndIndex::try_parse_params(Some(
            casper_json_rpc::Params::Array(vec![json!("latest")]),
        ))
        .expect_err("a missing index must be rejected");
        assert_eq!(error.code(), ReservedErrorCode::InvalidParams as i64);
    }

    async fn add_transaction_response(
        client: &BinaryPortMock,
        hash: evm::Hash,
        response: Option<TransactionWithExecutionInfo>,
    ) {
        let transaction_hash = TransactionHash::from(EvmTransactionHash::from_raw(hash.value()));
        let request = InformationRequest::Transaction {
            hash: transaction_hash,
            with_finalized_approvals: true,
        }
        .try_into()
        .unwrap();
        client
            .when_then(Command::Get(request), BinaryResponse::from_option(response))
            .await;
    }

    fn fixture_transaction(nonce: u64) -> EvmTransaction {
        let transaction = TxEip1559 {
            chain_id: 7,
            nonce,
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
