use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use casper_json_rpc::{Error as RpcError, Params};
use casper_types::{BlockIdentifier, evm};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use super::{
    super::NodeClient,
    eth_u256::EthU256,
    projection::{LogResponse, evm_hash_to_block_hash, project_block_logs},
    types::{BlockNumberParam, EthAddress, invalid_params, parse_positional_params},
};
use crate::rpcs::docs::DocExample;

static GET_LOGS_PARAMS_EXAMPLE: RawLogFilter = RawLogFilter {
    from_block: None,
    to_block: None,
    block_hash: None,
    address: None,
    topics: None,
};
static LOGS_EXAMPLE: Vec<LogResponse> = Vec::new();
static BOOL_EXAMPLE: bool = true;
static FILTER_ID_PARAMS_EXAMPLE: FilterIdParams = FilterIdParams {
    filter_id: EthU256::ZERO,
};
const FILTER_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const MAX_INSTALLED_FILTERS: usize = 1024;

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub(crate) enum AddressFilter {
    Single(EthAddress),
    AnyOf(Vec<EthAddress>),
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub(crate) enum TopicFilter {
    Exact(evm::Topic),
    AnyOf(Vec<evm::Topic>),
}

/// Log filter object accepted by Ethereum log RPCs.
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RawLogFilter {
    #[serde(default)]
    from_block: Option<BlockNumberParam>,
    #[serde(default)]
    to_block: Option<BlockNumberParam>,
    #[serde(default)]
    block_hash: Option<evm::Hash>,
    #[serde(default)]
    address: Option<AddressFilter>,
    #[serde(default)]
    topics: Option<Vec<Option<TopicFilter>>>,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct FilterIdParams {
    pub(crate) filter_id: EthU256,
}

impl DocExample for RawLogFilter {
    fn doc_example() -> &'static Self {
        &GET_LOGS_PARAMS_EXAMPLE
    }
}

impl DocExample for Vec<LogResponse> {
    fn doc_example() -> &'static Self {
        &LOGS_EXAMPLE
    }
}

impl DocExample for FilterIdParams {
    fn doc_example() -> &'static Self {
        &FILTER_ID_PARAMS_EXAMPLE
    }
}

impl DocExample for bool {
    fn doc_example() -> &'static Self {
        &BOOL_EXAMPLE
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) struct LogFilter {
    from_block: Option<BlockNumberParam>,
    to_block: Option<BlockNumberParam>,
    block_hash: Option<evm::Hash>,
    addresses: Option<Vec<EthAddress>>,
    topics: Vec<Option<Vec<evm::Topic>>>,
}

impl LogFilter {
    pub(crate) fn matches(&self, log: &LogResponse) -> bool {
        if let Some(addresses) = &self.addresses {
            if !addresses.contains(&log.address) {
                return false;
            }
        }

        for (index, maybe_topics) in self.topics.iter().enumerate() {
            let Some(topics) = maybe_topics else {
                continue;
            };
            let Some(log_topic) = log.topics.get(index) else {
                return false;
            };
            if !topics.contains(log_topic) {
                return false;
            }
        }

        true
    }

    pub(crate) fn block_hash(&self) -> Option<evm::Hash> {
        self.block_hash
    }

    pub(super) fn has_block_range_bound(&self) -> bool {
        self.from_block.is_some() || self.to_block.is_some()
    }

    pub(super) fn from_block_height_or_latest(&self, latest_height: u64) -> Result<u64, RpcError> {
        Ok(match self.from_block {
            Some(from_block) => from_block.height()?.unwrap_or(latest_height),
            None => latest_height,
        })
    }

    pub(super) fn to_block_height(&self, latest_height: u64) -> Result<u64, RpcError> {
        Ok(match self.to_block {
            Some(to_block) => to_block.height()?.unwrap_or(latest_height),
            None => latest_height,
        })
    }

    pub(super) fn finite_to_block_height(&self) -> Result<Option<u64>, RpcError> {
        self.to_block
            .and_then(|to_block| to_block.height().transpose())
            .transpose()
    }
}

impl TryFrom<RawLogFilter> for LogFilter {
    type Error = RpcError;

    fn try_from(value: RawLogFilter) -> Result<Self, Self::Error> {
        if value.block_hash.is_some() && (value.from_block.is_some() || value.to_block.is_some()) {
            return Err(invalid_params(
                "'blockHash' is mutually exclusive with 'fromBlock' and 'toBlock'",
            ));
        }

        let addresses = value.address.map(|address| match address {
            AddressFilter::Single(address) => vec![address],
            AddressFilter::AnyOf(addresses) => addresses,
        });
        let topics = value
            .topics
            .unwrap_or_default()
            .into_iter()
            .map(|maybe_topic| {
                maybe_topic.map(|topic| match topic {
                    TopicFilter::Exact(topic) => vec![topic],
                    TopicFilter::AnyOf(topics) => topics,
                })
            })
            .collect();

        Ok(LogFilter {
            from_block: value.from_block,
            to_block: value.to_block,
            block_hash: value.block_hash,
            addresses,
            topics,
        })
    }
}

pub(crate) async fn latest_block_height(
    node_client: Arc<dyn NodeClient>,
) -> Result<Option<u64>, RpcError> {
    Ok(node_client
        .read_block_with_signatures(None)
        .await
        .map_err(super::types::internal_error)?
        .map(|block| block.block().height()))
}

pub(crate) async fn logs_for_filter(
    node_client: Arc<dyn NodeClient>,
    filter: &LogFilter,
    max_block_range: u64,
) -> Result<Vec<LogResponse>, RpcError> {
    if let Some(block_hash) = filter.block_hash {
        return logs_for_block(
            node_client,
            Some(BlockIdentifier::Hash(evm_hash_to_block_hash(block_hash))),
            filter,
        )
        .await;
    }

    let Some(latest_height) = latest_block_height(node_client.clone()).await? else {
        return Ok(Vec::new());
    };
    let from_height = filter.from_block_height_or_latest(latest_height)?;
    let to_height = filter.to_block_height(latest_height)?;
    ensure_log_block_range_within_limit(from_height, to_height, max_block_range)?;
    logs_for_block_range(node_client, filter, from_height, to_height).await
}

pub(crate) fn ensure_log_block_range_within_limit(
    from_height: u64,
    to_height: u64,
    max_block_range: u64,
) -> Result<(), RpcError> {
    if from_height > to_height {
        return Ok(());
    }
    let block_count = to_height.saturating_sub(from_height).saturating_add(1);
    if block_count > max_block_range {
        return Err(invalid_params(format!(
            "log block range of {block_count} blocks exceeds configured maximum of \
            {max_block_range} blocks"
        )));
    }
    Ok(())
}

pub(crate) async fn logs_for_block_range(
    node_client: Arc<dyn NodeClient>,
    filter: &LogFilter,
    from_height: u64,
    to_height: u64,
) -> Result<Vec<LogResponse>, RpcError> {
    if from_height > to_height {
        return Ok(Vec::new());
    }

    let mut logs = Vec::new();
    for height in from_height..=to_height {
        logs.extend(
            logs_for_block(
                node_client.clone(),
                Some(BlockIdentifier::Height(height)),
                filter,
            )
            .await?,
        );
    }
    Ok(logs)
}

pub(crate) async fn logs_for_block_height(
    node_client: Arc<dyn NodeClient>,
    filter: &LogFilter,
    height: u64,
) -> Result<Vec<LogResponse>, RpcError> {
    logs_for_block(node_client, Some(BlockIdentifier::Height(height)), filter).await
}

async fn logs_for_block(
    node_client: Arc<dyn NodeClient>,
    identifier: Option<BlockIdentifier>,
    filter: &LogFilter,
) -> Result<Vec<LogResponse>, RpcError> {
    let Some(logs) = project_block_logs(node_client, identifier).await? else {
        return Ok(Vec::new());
    };
    Ok(logs.into_iter().filter(|log| filter.matches(log)).collect())
}

#[derive(Clone, Debug)]
pub(super) struct StoredFilter {
    pub(super) filter: LogFilter,
    pub(super) next_block: u64,
    pub(super) block_hash_polled: bool,
    last_accessed: Instant,
}

impl StoredFilter {
    pub(super) fn new(filter: LogFilter, next_block: u64) -> Self {
        Self {
            filter,
            next_block,
            block_hash_polled: false,
            last_accessed: Instant::now(),
        }
    }
}

#[derive(Debug, Default)]
pub struct EthFilterState {
    next_id: AtomicU64,
    filters: Mutex<HashMap<u64, StoredFilter>>,
}

impl EthFilterState {
    pub(crate) fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            filters: Mutex::new(HashMap::new()),
        }
    }

    fn next_filter_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) async fn insert(&self, filter: StoredFilter) -> u64 {
        let filter_id = self.next_filter_id();
        let now = Instant::now();
        let mut filters = self.filters.lock().await;
        prune_expired_filters(&mut filters, now);
        prune_to_capacity(&mut filters);
        filters.insert(
            filter_id,
            StoredFilter {
                last_accessed: now,
                ..filter
            },
        );
        filter_id
    }

    pub(super) async fn get(&self, filter_id: u64) -> Option<StoredFilter> {
        let now = Instant::now();
        let mut filters = self.filters.lock().await;
        prune_expired_filters(&mut filters, now);
        filters.get_mut(&filter_id).map(|stored| {
            stored.last_accessed = now;
            stored.clone()
        })
    }

    pub(super) async fn filter(&self, filter_id: u64) -> Option<LogFilter> {
        let now = Instant::now();
        let mut filters = self.filters.lock().await;
        prune_expired_filters(&mut filters, now);
        filters.get_mut(&filter_id).map(|stored| {
            stored.last_accessed = now;
            stored.filter.clone()
        })
    }

    pub(super) async fn mark_block_hash_polled(&self, filter_id: u64) {
        let now = Instant::now();
        let mut filters = self.filters.lock().await;
        prune_expired_filters(&mut filters, now);
        if let Some(stored) = filters.get_mut(&filter_id) {
            stored.block_hash_polled = true;
            stored.last_accessed = now;
        }
    }

    pub(super) async fn set_next_block(&self, filter_id: u64, next_block: u64) {
        let now = Instant::now();
        let mut filters = self.filters.lock().await;
        prune_expired_filters(&mut filters, now);
        if let Some(stored) = filters.get_mut(&filter_id) {
            stored.next_block = next_block;
            stored.last_accessed = now;
        }
    }

    pub(super) async fn remove(&self, filter_id: u64) -> bool {
        let now = Instant::now();
        let mut filters = self.filters.lock().await;
        prune_expired_filters(&mut filters, now);
        filters.remove(&filter_id).is_some()
    }
}

fn prune_expired_filters(filters: &mut HashMap<u64, StoredFilter>, now: Instant) {
    filters.retain(|_, stored| now.duration_since(stored.last_accessed) <= FILTER_IDLE_TIMEOUT);
}

fn prune_to_capacity(filters: &mut HashMap<u64, StoredFilter>) {
    while filters.len() >= MAX_INSTALLED_FILTERS {
        let Some(oldest_id) = filters
            .iter()
            .min_by_key(|(_, stored)| stored.last_accessed)
            .map(|(filter_id, _)| *filter_id)
        else {
            break;
        };
        filters.remove(&oldest_id);
    }
}

pub(super) fn filter_id_from_params(maybe_params: Option<Params>) -> Result<u64, RpcError> {
    let (filter_id,) = parse_positional_params::<(EthU256,)>(maybe_params)?;
    filter_id.as_u64().map_err(invalid_params)
}

pub(super) fn filter_id_result(filter_id: u64) -> EthU256 {
    EthU256::from(filter_id)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::rpcs::eth::types::HexData;

    fn hash(byte: u8) -> evm::Hash {
        evm::Hash::new([byte; evm::HASH_LENGTH])
    }

    fn topic(byte: u8) -> evm::Topic {
        evm::Topic::new([byte; evm::HASH_LENGTH])
    }

    fn address(byte: u8) -> EthAddress {
        EthAddress::from(evm::Address::new([byte; evm::ADDRESS_LENGTH]))
    }

    fn log(address: EthAddress, topics: Vec<evm::Topic>) -> LogResponse {
        LogResponse {
            address,
            topics,
            data: HexData::default(),
            block_hash: hash(9),
            block_number: EthU256::from(1u8),
            transaction_hash: hash(8),
            transaction_index: EthU256::from(0u8),
            log_index: EthU256::from(0u8),
            removed: false,
        }
    }

    fn parsed_filter(value: serde_json::Value) -> Result<LogFilter, RpcError> {
        let raw = serde_json::from_value::<RawLogFilter>(value).unwrap();
        LogFilter::try_from(raw)
    }

    fn default_filter() -> LogFilter {
        LogFilter::try_from(RawLogFilter::default()).unwrap()
    }

    #[test]
    fn parses_single_and_list_addresses() {
        let single = parsed_filter(json!({ "address": String::from(address(1)) })).unwrap();
        assert!(single.matches(&log(address(1), vec![])));
        assert!(!single.matches(&log(address(2), vec![])));

        let list = parsed_filter(json!({
            "address": [String::from(address(1)), String::from(address(2))]
        }))
        .unwrap();
        assert!(list.matches(&log(address(2), vec![])));
        assert!(!list.matches(&log(address(3), vec![])));
    }

    #[test]
    fn parses_topic_exact_null_and_or_filters() {
        let topic_a = topic(1);
        let topic_b = topic(2);
        let topic_c = topic(3);
        let filter = parsed_filter(json!({
            "topics": [
                topic_a.to_string(),
                null,
                [topic_b.to_string(), topic_c.to_string()]
            ]
        }))
        .unwrap();

        assert!(filter.matches(&log(address(1), vec![topic_a, topic(9), topic_c])));
        assert!(!filter.matches(&log(address(1), vec![topic_b, topic(9), topic_c])));
        assert!(!filter.matches(&log(address(1), vec![topic_a, topic(9)])));
    }

    #[test]
    fn rejects_block_hash_range_conflict() {
        let err = parsed_filter(json!({
            "fromBlock": "earliest",
            "blockHash": hash(1).to_string(),
        }))
        .unwrap_err();

        assert_eq!(
            err,
            invalid_params("'blockHash' is mutually exclusive with 'fromBlock' and 'toBlock'")
        );
    }

    #[test]
    fn validates_log_block_range_limit() {
        assert!(ensure_log_block_range_within_limit(0, 9_999, 10_000).is_ok());
        assert!(ensure_log_block_range_within_limit(10, 9, 10_000).is_ok());

        let err = ensure_log_block_range_within_limit(0, 10_000, 10_000).unwrap_err();
        assert_eq!(
            err,
            invalid_params(
                "log block range of 10001 blocks exceeds configured maximum of 10000 blocks"
            )
        );
    }

    #[test]
    fn empty_topic_or_array_matches_nothing() {
        let topic = topic(1);
        let filter = parsed_filter(json!({ "topics": [[]] })).unwrap();

        assert!(!filter.matches(&log(address(1), vec![topic])));
        assert!(!filter.matches(&log(address(1), vec![])));
    }

    #[tokio::test]
    async fn filter_state_expires_idle_filters() {
        let state = EthFilterState::new();
        let filter_id = state.insert(StoredFilter::new(default_filter(), 0)).await;

        {
            let mut filters = state.filters.lock().await;
            filters.get_mut(&filter_id).unwrap().last_accessed = Instant::now()
                .checked_sub(FILTER_IDLE_TIMEOUT + Duration::from_secs(1))
                .unwrap();
        }

        assert!(state.get(filter_id).await.is_none());
    }

    #[tokio::test]
    async fn filter_state_caps_installed_filters() {
        let state = EthFilterState::new();
        let mut filter_ids = Vec::with_capacity(MAX_INSTALLED_FILTERS);

        for _ in 0..MAX_INSTALLED_FILTERS {
            filter_ids.push(state.insert(StoredFilter::new(default_filter(), 0)).await);
        }
        {
            let mut filters = state.filters.lock().await;
            filters.get_mut(&filter_ids[0]).unwrap().last_accessed =
                Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        }

        let newest_filter_id = state.insert(StoredFilter::new(default_filter(), 0)).await;

        assert!(state.get(filter_ids[0]).await.is_none());
        assert!(state.get(newest_filter_id).await.is_some());
        assert_eq!(state.filters.lock().await.len(), MAX_INSTALLED_FILTERS);
    }
}
