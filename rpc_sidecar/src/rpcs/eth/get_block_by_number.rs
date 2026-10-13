use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use casper_json_rpc::{Error as RpcError, Params};
use casper_types::{BlockIdentifier, evm};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    super::{NodeClient, RpcWithParams},
    config::read_evm_config,
    eth_u256::EthU256,
    projection::project_block,
    transaction_response::BlockTransactions,
    types::{BlockNumberParam, BlockTag, EthAddress, HexData, parse_positional_params},
};
use crate::rpcs::docs::DocExample;

static GET_BLOCK_BY_NUMBER_PARAMS_EXAMPLE: LazyLock<GetBlockByNumberParams> =
    LazyLock::new(|| GetBlockByNumberParams {
        block: BlockNumberParam::Tag(BlockTag::Latest),
        full_transactions: false,
    });
static BLOCK_RESPONSE_EXAMPLE: LazyLock<Option<BlockResponse>> = LazyLock::new(|| None);

/// Params for `eth_getBlockByNumber`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetBlockByNumberParams {
    block: BlockNumberParam,
    full_transactions: bool,
}

impl GetBlockByNumberParams {
    fn identifier(&self) -> Result<Option<casper_types::BlockIdentifier>, RpcError> {
        self.block.identifier()
    }

    fn full_transactions(&self) -> bool {
        self.full_transactions
    }
}

impl DocExample for GetBlockByNumberParams {
    fn doc_example() -> &'static Self {
        &GET_BLOCK_BY_NUMBER_PARAMS_EXAMPLE
    }
}

#[derive(Deserialize)]
struct PositionalParams(BlockNumberParam, #[serde(default)] bool);

impl From<PositionalParams> for GetBlockByNumberParams {
    fn from(params: PositionalParams) -> Self {
        GetBlockByNumberParams {
            block: params.0,
            full_transactions: params.1,
        }
    }
}

/// Ethereum block response returned by block lookup RPCs.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BlockResponse {
    number: EthU256,
    hash: evm::Hash,
    parent_hash: evm::Hash,
    parent_beacon_block_root: evm::Hash,
    nonce: Option<HexData>,
    mix_hash: evm::Hash,
    sha3_uncles: evm::Hash,
    logs_bloom: HexData,
    transactions_root: evm::Hash,
    state_root: evm::Hash,
    receipts_root: evm::Hash,
    miner: EthAddress,
    difficulty: EthU256,
    total_difficulty: EthU256,
    extra_data: HexData,
    size: EthU256,
    gas_limit: EthU256,
    gas_used: EthU256,
    timestamp: EthU256,
    transactions: BlockTransactions,
    uncles: Vec<evm::Hash>,
    base_fee_per_gas: EthU256,
}

impl DocExample for Option<BlockResponse> {
    fn doc_example() -> &'static Self {
        &BLOCK_RESPONSE_EXAMPLE
    }
}

/// `eth_getBlockByNumber`.
pub struct GetBlockByNumber;

pub(super) async fn get_block(
    node_client: Arc<dyn NodeClient>,
    identifier: Option<BlockIdentifier>,
    full_transactions: bool,
) -> Result<Option<BlockResponse>, RpcError> {
    let evm_config = read_evm_config(node_client.as_ref()).await?;
    let Some(block) = project_block(node_client, identifier, full_transactions).await? else {
        return Ok(None);
    };
    Ok(Some(BlockResponse {
        number: block.number,
        hash: block.hash,
        parent_hash: block.parent_hash,
        parent_beacon_block_root: block.parent_beacon_block_root,
        nonce: Some(HexData::from(vec![0; 8])),
        mix_hash: evm::Hash::ZERO,
        sha3_uncles: evm::EMPTY_CODE_HASH,
        logs_bloom: block.logs_bloom,
        transactions_root: block.transactions_root,
        state_root: block.state_root,
        receipts_root: block.receipts_root,
        miner: block.miner,
        difficulty: EthU256::from(0u8),
        total_difficulty: EthU256::from(0u8),
        extra_data: HexData::from(Vec::new()),
        size: EthU256::from(0u8),
        gas_limit: EthU256::from(evm_config.block_gas_limit),
        gas_used: block.gas_used,
        timestamp: block.timestamp,
        transactions: block.transactions,
        uncles: Vec::new(),
        base_fee_per_gas: EthU256::from(evm_config.base_fee_wei()),
    }))
}

#[async_trait]
impl RpcWithParams for GetBlockByNumber {
    const METHOD: &'static str = "eth_getBlockByNumber";
    type RequestParams = GetBlockByNumberParams;
    type ResponseResult = Option<BlockResponse>;

    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        parse_positional_params::<PositionalParams>(maybe_params).map(Into::into)
    }

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: GetBlockByNumberParams,
    ) -> Result<Option<BlockResponse>, RpcError> {
        get_block(
            node_client,
            params.identifier()?,
            params.full_transactions(),
        )
        .await
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
        Block, BlockSignatures, BlockWithSignatures, ChainspecRawBytes, EvmTransaction,
        ExecutionInfo, TestBlockBuilder, TimeDiff, Timestamp, Transaction, TransactionHash,
        execution::{EvmExecutionResult, ExecutionResult},
        testing::TestRng,
    };
    use serde_json::json;

    use super::*;
    use crate::rpcs::{
        eth::get_transaction_by_hash::get_transaction_by_hash_at, test_utils::BinaryPortMock,
    };

    #[tokio::test]
    async fn full_block_transaction_matches_individual_lookup() {
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

        let hash_client = Arc::new(BinaryPortMock::new());
        add_chainspec(&hash_client).await;
        add_projected_block_responses(
            &hash_client,
            &block,
            &transaction,
            &execution_info,
            None,
            rng,
        )
        .await;
        let hash_response = GetBlockByNumber::do_handle_request(
            hash_client.clone(),
            GetBlockByNumberParams {
                block: BlockNumberParam::Tag(BlockTag::Latest),
                full_transactions: false,
            },
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            hash_response.transactions.hashes(),
            Some(&[transaction.hash().hash()][..])
        );
        hash_client.verify_no_lingering().await;

        let full_client = Arc::new(BinaryPortMock::new());
        add_chainspec(&full_client).await;
        add_projected_block_responses(
            &full_client,
            &block,
            &transaction,
            &execution_info,
            None,
            rng,
        )
        .await;
        let full_response = GetBlockByNumber::do_handle_request(
            full_client.clone(),
            GetBlockByNumberParams {
                block: BlockNumberParam::Tag(BlockTag::Latest),
                full_transactions: true,
            },
        )
        .await
        .unwrap()
        .unwrap();
        let hydrated = full_response
            .transactions
            .full()
            .expect("full transaction objects should be returned")
            .first()
            .cloned()
            .unwrap();
        full_client.verify_no_lingering().await;

        let lookup_client = Arc::new(BinaryPortMock::new());
        add_transaction_response(&lookup_client, &transaction, &execution_info).await;
        lookup_client
            .add_block_with_signatures(
                BlockWithSignatures::new(block.clone(), BlockSignatures::random(rng)),
                InformationRequest::BlockWithSignatures(Some(BlockIdentifier::Hash(*block.hash()))),
            )
            .await;
        let individual = get_transaction_by_hash_at(
            lookup_client.clone(),
            transaction.hash().hash(),
            Timestamp::now(),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(hydrated, individual);
        lookup_client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn reports_block_proposer_as_miner() {
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().build(rng));
        let expected_miner = EthAddress::from(evm::Address::from_block_proposer_public_key(
            block.proposer(),
        ));
        let node_client = Arc::new(BinaryPortMock::new());
        add_chainspec(&node_client).await;
        node_client
            .add_block_with_signatures(
                BlockWithSignatures::new(block, BlockSignatures::random(rng)),
                InformationRequest::BlockWithSignatures(None),
            )
            .await;

        let response = GetBlockByNumber::do_handle_request(
            node_client.clone(),
            GetBlockByNumberParams {
                block: BlockNumberParam::Tag(BlockTag::Latest),
                full_transactions: false,
            },
        )
        .await
        .expect("request should succeed")
        .expect("block should be returned");

        assert_eq!(response.miner, expected_miner);
        assert_eq!(response.base_fee_per_gas, EthU256::from(3_000_000_000u64));
        assert_eq!(response.gas_limit, EthU256::from(12_345_678u64));
        node_client.verify_no_lingering().await;
    }

    #[test]
    fn serializes_parent_beacon_block_root_as_parent_hash() {
        let parent_hash = evm::Hash::new([7; evm::HASH_LENGTH]);
        let response = BlockResponse {
            number: EthU256::from(1u8),
            hash: evm::Hash::new([8; evm::HASH_LENGTH]),
            parent_hash,
            parent_beacon_block_root: parent_hash,
            nonce: Some(HexData::from(vec![0; 8])),
            mix_hash: evm::Hash::ZERO,
            sha3_uncles: evm::EMPTY_CODE_HASH,
            logs_bloom: HexData::from(Vec::new()),
            transactions_root: evm::Hash::ZERO,
            state_root: evm::Hash::ZERO,
            receipts_root: evm::Hash::ZERO,
            miner: EthAddress::from(evm::Address::ZERO),
            difficulty: EthU256::from(0u8),
            total_difficulty: EthU256::from(0u8),
            extra_data: HexData::from(Vec::new()),
            size: EthU256::from(0u8),
            gas_limit: EthU256::from(30_000_000u64),
            gas_used: EthU256::from(0u8),
            timestamp: EthU256::from(1u8),
            transactions: BlockTransactions::Hashes(Vec::new()),
            uncles: Vec::new(),
            base_fee_per_gas: EthU256::from(0u8),
        };

        let value = serde_json::to_value(response).expect("response should serialize");

        assert_eq!(
            value["parentBeaconBlockRoot"],
            serde_json::to_value(parent_hash).expect("hash should serialize")
        );
        assert_eq!(value["parentBeaconBlockRoot"], value["parentHash"]);
        assert_ne!(value["parentBeaconBlockRoot"], json!(null));
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
block_gas_limit = 12345678
base_fee = 3
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
