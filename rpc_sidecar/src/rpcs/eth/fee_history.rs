use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use casper_json_rpc::{Error as RpcError, Params};
use casper_types::BlockIdentifier;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    super::{NodeClient, RpcWithParams},
    config::read_evm_config,
    eth_u256::EthU256,
    projection::project_block,
    types::{BlockNumberParam, BlockTag, internal_error, invalid_params, parse_positional_params},
};
use crate::rpcs::docs::DocExample;

const MAX_FEE_HISTORY_BLOCK_COUNT: u64 = 1_024;

static FEE_HISTORY_PARAMS_EXAMPLE: LazyLock<FeeHistoryParams> =
    LazyLock::new(|| FeeHistoryParams {
        block_count: EthU256::from(1u8),
        newest_block: BlockNumberParam::Tag(BlockTag::Latest),
        reward_percentiles: vec![25.0, 75.0],
    });
static FEE_HISTORY_RESULT_EXAMPLE: LazyLock<FeeHistoryResult> =
    LazyLock::new(|| FeeHistoryResult {
        oldest_block: EthU256::from(1u8),
        base_fee_per_gas: vec![EthU256::ZERO, EthU256::ZERO],
        gas_used_ratio: vec![0.0],
        reward: Some(vec![vec![EthU256::ZERO, EthU256::ZERO]]),
    });

/// Params for `eth_feeHistory`.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct FeeHistoryParams {
    block_count: EthU256,
    newest_block: BlockNumberParam,
    #[serde(default)]
    reward_percentiles: Vec<f64>,
}

impl DocExample for FeeHistoryParams {
    fn doc_example() -> &'static Self {
        &FEE_HISTORY_PARAMS_EXAMPLE
    }
}

#[derive(Deserialize)]
struct PositionalParams(EthU256, BlockNumberParam, #[serde(default)] Vec<f64>);

impl From<PositionalParams> for FeeHistoryParams {
    fn from(params: PositionalParams) -> Self {
        FeeHistoryParams {
            block_count: params.0,
            newest_block: params.1,
            reward_percentiles: params.2,
        }
    }
}

/// Fee history for the requested block range.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FeeHistoryResult {
    oldest_block: EthU256,
    base_fee_per_gas: Vec<EthU256>,
    gas_used_ratio: Vec<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reward: Option<Vec<Vec<EthU256>>>,
}

impl DocExample for FeeHistoryResult {
    fn doc_example() -> &'static Self {
        &FEE_HISTORY_RESULT_EXAMPLE
    }
}

/// `eth_feeHistory`.
pub struct FeeHistory;

#[async_trait]
impl RpcWithParams for FeeHistory {
    const METHOD: &'static str = "eth_feeHistory";
    type RequestParams = FeeHistoryParams;
    type ResponseResult = FeeHistoryResult;

    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        parse_positional_params::<PositionalParams>(maybe_params).map(Into::into)
    }

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: FeeHistoryParams,
    ) -> Result<FeeHistoryResult, RpcError> {
        let block_count = validate_block_count(params.block_count)?;
        validate_reward_percentiles(&params.reward_percentiles)?;

        let evm_config = read_evm_config(node_client.as_ref()).await?;
        if evm_config.block_gas_limit == 0 {
            return Err(internal_error(
                "configured EVM block gas limit must be greater than zero",
            ));
        }
        let newest_height =
            resolve_newest_height(node_client.as_ref(), params.newest_block).await?;
        let history_len = block_count.min(newest_height.saturating_add(1));
        let oldest_height = newest_height.saturating_sub(history_len.saturating_sub(1));
        let history_len_usize =
            usize::try_from(history_len).expect("fee history length is capped at 1024");

        let mut gas_used_ratio = Vec::with_capacity(history_len_usize);
        for height in oldest_height..=newest_height {
            let block = project_block(
                node_client.clone(),
                Some(BlockIdentifier::Height(height)),
                false,
            )
            .await?
            .ok_or_else(|| invalid_params(format!("fee history block {height} was not found")))?;
            gas_used_ratio.push(block_gas_used_ratio(
                block.gas_used,
                evm_config.block_gas_limit,
            )?);
        }

        let base_fee = EthU256::from(evm_config.base_fee_wei());
        let base_fee_per_gas = vec![base_fee; history_len_usize + 1];
        let reward = if params.reward_percentiles.is_empty() {
            None
        } else {
            Some(vec![
                vec![EthU256::ZERO; params.reward_percentiles.len()];
                history_len_usize
            ])
        };

        Ok(FeeHistoryResult {
            oldest_block: EthU256::from(oldest_height),
            base_fee_per_gas,
            gas_used_ratio,
            reward,
        })
    }
}

fn validate_block_count(block_count: EthU256) -> Result<u64, RpcError> {
    let block_count = block_count.as_u64().map_err(invalid_params)?;
    if !(1..=MAX_FEE_HISTORY_BLOCK_COUNT).contains(&block_count) {
        return Err(invalid_params(format!(
            "block count must be between 1 and {MAX_FEE_HISTORY_BLOCK_COUNT}"
        )));
    }
    Ok(block_count)
}

fn validate_reward_percentiles(percentiles: &[f64]) -> Result<(), RpcError> {
    for percentile in percentiles {
        if !percentile.is_finite() || !(0.0..=100.0).contains(percentile) {
            return Err(invalid_params(
                "reward percentiles must be finite values between 0 and 100",
            ));
        }
    }
    if percentiles.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid_params(
            "reward percentiles must be strictly increasing",
        ));
    }
    Ok(())
}

async fn resolve_newest_height(
    node_client: &dyn NodeClient,
    newest_block: BlockNumberParam,
) -> Result<u64, RpcError> {
    if let Some(height) = newest_block.height()? {
        return Ok(height);
    }
    node_client
        .read_block_header(None)
        .await
        .map_err(internal_error)?
        .map(|header| header.height())
        .ok_or_else(|| internal_error("node has no complete blocks"))
}

fn block_gas_used_ratio(gas_used: EthU256, gas_limit: u64) -> Result<f64, RpcError> {
    let gas_used = gas_used.as_u64().map_err(internal_error)?;
    Ok(gas_used as f64 / gas_limit as f64)
}

#[cfg(test)]
mod tests {
    use casper_binary_port::{BinaryResponse, Command, InformationRequest};
    use casper_types::{
        Block, BlockIdentifier, BlockSignatures, BlockWithSignatures, ChainspecRawBytes,
        TestBlockBuilder, testing::TestRng,
    };

    use super::*;
    use crate::rpcs::test_utils::BinaryPortMock;

    #[tokio::test]
    async fn returns_constant_base_fee_zero_rewards_and_block_ratios() {
        let rng = &mut TestRng::new();
        let block_10 = Block::V2(TestBlockBuilder::new().height(10).build(rng));
        let block_11 = Block::V2(TestBlockBuilder::new().height(11).build(rng));
        let client = Arc::new(BinaryPortMock::new());
        add_chainspec(&client).await;
        client
            .add_block_header_req_res(
                block_11.clone().take_header(),
                InformationRequest::BlockHeader(None),
            )
            .await;
        add_block(&client, block_10, rng).await;
        add_block(&client, block_11, rng).await;

        let result = FeeHistory::do_handle_request(
            client.clone(),
            FeeHistoryParams {
                block_count: EthU256::from(2u64),
                newest_block: BlockNumberParam::Tag(BlockTag::Latest),
                reward_percentiles: vec![10.0, 90.0],
            },
        )
        .await
        .expect("fee history should succeed");

        let base_fee = EthU256::from(3_000_000_000u64);
        assert_eq!(result.oldest_block, EthU256::from(10u64));
        assert_eq!(result.base_fee_per_gas, vec![base_fee, base_fee, base_fee]);
        assert_eq!(result.gas_used_ratio, vec![0.0, 0.0]);
        assert_eq!(
            result.reward,
            Some(vec![
                vec![EthU256::ZERO, EthU256::ZERO],
                vec![EthU256::ZERO, EthU256::ZERO],
            ])
        );
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn rejects_invalid_count_and_percentiles_before_querying_node() {
        let client = Arc::new(BinaryPortMock::new());
        for params in [
            FeeHistoryParams {
                block_count: EthU256::ZERO,
                newest_block: BlockNumberParam::Tag(BlockTag::Latest),
                reward_percentiles: Vec::new(),
            },
            FeeHistoryParams {
                block_count: EthU256::from(1u8),
                newest_block: BlockNumberParam::Tag(BlockTag::Latest),
                reward_percentiles: vec![50.0, 50.0],
            },
            FeeHistoryParams {
                block_count: EthU256::from(1u8),
                newest_block: BlockNumberParam::Tag(BlockTag::Latest),
                reward_percentiles: vec![101.0],
            },
        ] {
            FeeHistory::do_handle_request(client.clone(), params)
                .await
                .expect_err("invalid fee history params should fail");
        }
        client.verify_no_lingering().await;
    }

    #[test]
    fn computes_gas_used_ratio() {
        assert_eq!(
            block_gas_used_ratio(EthU256::from(15_000_000u64), 30_000_000).unwrap(),
            0.5
        );
    }

    async fn add_chainspec(client: &BinaryPortMock) {
        let request = InformationRequest::ChainspecRawBytes
            .try_into()
            .expect("chainspec information request should convert");
        client
            .when_then(
                Command::Get(request),
                BinaryResponse::from_value(chainspec()),
            )
            .await;
    }

    async fn add_block(client: &BinaryPortMock, block: Block, rng: &mut TestRng) {
        let height = block.height();
        client
            .add_block_with_signatures(
                BlockWithSignatures::new(block, BlockSignatures::random(rng)),
                InformationRequest::BlockWithSignatures(Some(BlockIdentifier::Height(height))),
            )
            .await;
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
}
