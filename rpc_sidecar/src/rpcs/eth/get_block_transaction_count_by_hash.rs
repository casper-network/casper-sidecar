use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use casper_json_rpc::{Error as RpcError, Params};
use casper_types::{BlockIdentifier, evm};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    super::{NodeClient, RpcWithParams},
    eth_u256::EthU256,
    projection::{block_evm_transaction_hashes, evm_hash_to_block_hash},
    types::parse_positional_params,
};
use crate::rpcs::docs::DocExample;

static GET_BLOCK_TRANSACTION_COUNT_BY_HASH_PARAMS_EXAMPLE: LazyLock<
    GetBlockTransactionCountByHashParams,
> = LazyLock::new(|| GetBlockTransactionCountByHashParams {
    block_hash: evm::Hash::ZERO,
});
/// Params for `eth_getBlockTransactionCountByHash`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetBlockTransactionCountByHashParams {
    block_hash: evm::Hash,
}

impl DocExample for GetBlockTransactionCountByHashParams {
    fn doc_example() -> &'static Self {
        &GET_BLOCK_TRANSACTION_COUNT_BY_HASH_PARAMS_EXAMPLE
    }
}

/// `eth_getBlockTransactionCountByHash`.
///
/// Returns the number of transactions in the block identified by `blockHash`, or `null` if no
/// such block is known.
pub struct GetBlockTransactionCountByHash;

#[async_trait]
impl RpcWithParams for GetBlockTransactionCountByHash {
    const METHOD: &'static str = "eth_getBlockTransactionCountByHash";
    type RequestParams = GetBlockTransactionCountByHashParams;
    type ResponseResult = Option<EthU256>;

    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        let (block_hash,) = parse_positional_params::<(evm::Hash,)>(maybe_params)?;
        Ok(GetBlockTransactionCountByHashParams { block_hash })
    }

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: GetBlockTransactionCountByHashParams,
    ) -> Result<Option<EthU256>, RpcError> {
        let identifier = BlockIdentifier::Hash(evm_hash_to_block_hash(params.block_hash));
        Ok(
            block_evm_transaction_hashes(node_client.as_ref(), Some(identifier))
                .await?
                .map(|hashes| EthU256::from(hashes.len())),
        )
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope, crypto::secp256k1};
    use alloy_eips::{Encodable2718, eip2930::AccessList};
    use alloy_primitives::{Address as AlloyAddress, B256, Bytes as AlloyBytes, TxKind, U256};
    use casper_binary_port::{BinaryResponse, Command, InformationRequest};
    use casper_types::{
        Block, BlockIdentifier, BlockSignatures, BlockWithSignatures, Deploy, EvmTransaction,
        TestBlockBuilder, TimeDiff, Timestamp, Transaction, testing::TestRng,
    };
    use serde_json::json;

    use super::*;
    use crate::rpcs::{eth::types::block_hash_to_evm_hash, test_utils::BinaryPortMock};

    #[tokio::test]
    async fn known_block_reports_the_evm_transaction_count() {
        let rng = &mut TestRng::new();
        // A native transaction is not counted: only EVM transactions are visible through `eth_*`.
        let transactions = vec![
            Transaction::from(fixture_transaction(1)),
            Transaction::from(Deploy::random(rng)),
            Transaction::from(fixture_transaction(2)),
        ];
        let block = Block::V2(
            TestBlockBuilder::new()
                .height(42)
                .transactions(&transactions)
                .build(rng),
        );
        let client = Arc::new(BinaryPortMock::new());
        add_block_response(&client, &block, rng).await;

        let count = GetBlockTransactionCountByHash::do_handle_request(
            client.clone(),
            GetBlockTransactionCountByHashParams {
                block_hash: block_hash_to_evm_hash(*block.hash()),
            },
        )
        .await
        .unwrap()
        .expect("known block should report a count");

        assert_eq!(count, EthU256::from(2u64));
        assert_eq!(serde_json::to_value(count).unwrap(), json!("0x2"));
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn empty_block_reports_zero_not_null() {
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().height(42).build(rng));
        let client = Arc::new(BinaryPortMock::new());
        add_block_response(&client, &block, rng).await;

        let count = GetBlockTransactionCountByHash::do_handle_request(
            client.clone(),
            GetBlockTransactionCountByHashParams {
                block_hash: block_hash_to_evm_hash(*block.hash()),
            },
        )
        .await
        .unwrap()
        .expect("a known, empty block must return `Some(0)`, not null");

        assert_eq!(count, EthU256::ZERO);
        assert_eq!(serde_json::to_value(count).unwrap(), json!("0x0"));
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn unknown_block_returns_null() {
        let hash = evm::Hash::new([0x44; evm::HASH_LENGTH]);
        let client = Arc::new(BinaryPortMock::new());
        let request = InformationRequest::BlockWithSignatures(Some(BlockIdentifier::Hash(
            evm_hash_to_block_hash(hash),
        )))
        .try_into()
        .unwrap();
        client
            .when_then(
                Command::Get(request),
                BinaryResponse::from_option(None::<BlockWithSignatures>),
            )
            .await;

        let count = GetBlockTransactionCountByHash::do_handle_request(
            client.clone(),
            GetBlockTransactionCountByHashParams { block_hash: hash },
        )
        .await
        .expect("a nonexistent block must resolve to null, not an error");

        assert_eq!(count, None);
        client.verify_no_lingering().await;
    }

    #[test]
    fn accepts_a_raw_block_hash() {
        let hash = evm::Hash::new([0x2a; evm::HASH_LENGTH]);
        let params = GetBlockTransactionCountByHash::try_parse_params(Some(
            casper_json_rpc::Params::Array(vec![json!(format!("0x{}", hash.to_hex_string()))]),
        ))
        .expect("a raw block hash should parse");
        assert_eq!(params.block_hash, hash);
    }

    #[test]
    fn requires_a_block_hash() {
        let error = GetBlockTransactionCountByHash::try_parse_params(Some(
            casper_json_rpc::Params::Array(Vec::new()),
        ))
        .expect_err("an omitted block hash must be rejected");
        assert_eq!(
            error.code(),
            casper_json_rpc::ReservedErrorCode::InvalidParams as i64
        );
    }

    async fn add_block_response(client: &BinaryPortMock, block: &Block, rng: &mut TestRng) {
        client
            .add_block_with_signatures(
                BlockWithSignatures::new(block.clone(), BlockSignatures::random(rng)),
                InformationRequest::BlockWithSignatures(Some(BlockIdentifier::Hash(*block.hash()))),
            )
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
