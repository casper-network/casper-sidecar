use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use async_trait::async_trait;
use casper_json_rpc::{ConfigLimit, Error as RpcError, RequestHandlersBuilder};
use casper_types::BlockSynchronizerStatus;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::warn;

use super::{
    super::{NodeClient, RpcWithoutParams},
    eth_u256::EthU256,
    types::internal_error,
};
use crate::{ClientError, node_client::RestNodeStatus, rpcs::docs::DocExample};

static SYNCING_RESULT_EXAMPLE: LazyLock<SyncingResult> =
    LazyLock::new(|| SyncingResult::NotSyncing(false));

/// How often the node's `/status` endpoint is polled to refresh the cached `eth_syncing` result.
const POLL_INTERVAL: Duration = Duration::from_secs(2);
/// Maximum number of attempts made to fetch `/status` on each poll before giving up until the
/// next poll.
const MAX_FETCH_ATTEMPTS: u32 = 3;
/// Timeout applied to each individual `/status` fetch attempt.
const FETCH_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);

/// Reactor states in which the node is still catching up to the tip of the chain, as opposed to
/// having caught up and either following or validating the tip.
fn is_catching_up(reactor_state: &str) -> bool {
    matches!(reactor_state, "Initialize" | "CatchUp")
}

fn highest_known_block_height(block_sync: &BlockSynchronizerStatus, fallback: u64) -> u64 {
    #[derive(Default, Deserialize)]
    struct BlockSyncStatusMirror {
        #[serde(default)]
        block_height: Option<u64>,
    }

    #[derive(Default, Deserialize)]
    struct BlockSynchronizerStatusMirror {
        #[serde(default)]
        historical: Option<BlockSyncStatusMirror>,
        #[serde(default)]
        forward: Option<BlockSyncStatusMirror>,
    }

    let mirror: BlockSynchronizerStatusMirror = serde_json::to_value(block_sync)
        .ok()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default();

    let historical = mirror.historical.and_then(|status| status.block_height);
    let forward = mirror.forward.and_then(|status| status.block_height);

    historical
        .into_iter()
        .chain(forward)
        .max()
        .unwrap_or(fallback)
}

/// Sync progress reported while the node has not yet caught up to the tip of the chain.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SyncingStatus {
    /// Height of the lowest block held contiguously by this node.
    starting_block: EthU256,
    /// Height of the highest block held contiguously by this node.
    current_block: EthU256,
    /// Height of the highest block this node is aware of, whether or not it holds it yet.
    highest_block: EthU256,
}

/// `eth_syncing` result: `false` once the node has caught up to tip, otherwise sync progress.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub(crate) enum SyncingResult {
    NotSyncing(bool),
    Syncing(SyncingStatus),
}

impl DocExample for SyncingResult {
    fn doc_example() -> &'static Self {
        &SYNCING_RESULT_EXAMPLE
    }
}

/// `eth_syncing`.
///
/// Holds the most recently computed [`SyncingResult`], refreshed by a background task polling
/// the node's REST `/status` endpoint every [`POLL_INTERVAL`]. `SyncingStatus::starting_block`
/// within it stays fixed for the duration of a catch-up instead of drifting with `current_block`
/// on every poll: each refresh carries it forward from the previous cached value, only resetting
/// to the freshly observed `current_block` when the previous value wasn't itself a catch-up (i.e.
/// this is a fresh catch-up starting from scratch).
///
/// Handling an `eth_syncing` request never itself talks to the node - it only ever serves the
/// cached result, and reports an error if the cache hasn't been populated yet.
pub struct Syncing {
    cached_result: Mutex<Option<SyncingResult>>,
}

impl Syncing {
    pub const METHOD: &'static str = "eth_syncing";

    pub(crate) fn new() -> Self {
        Self {
            cached_result: Mutex::new(None),
        }
    }

    pub(crate) fn register_as_handler(
        state: Arc<Syncing>,
        node_client: Arc<dyn NodeClient>,
        handlers_builder: &mut RequestHandlersBuilder,
        limit: ConfigLimit,
    ) {
        tokio::spawn(Self::run_status_updater(Arc::clone(&state), node_client));

        let handler = move |maybe_params| {
            let state = Arc::clone(&state);
            async move {
                Self::check_no_params(maybe_params)?;
                Self::do_handle_request(state).await
            }
        };
        handlers_builder.register_handler(Self::METHOD, handler, &limit);
    }

    async fn do_handle_request(state: Arc<Syncing>) -> Result<SyncingResult, RpcError> {
        state.cached_result.lock().await.ok_or_else(|| {
            internal_error("node sync status is not yet available, try again shortly")
        })
    }

    async fn run_status_updater(state: Arc<Syncing>, node_client: Arc<dyn NodeClient>) {
        let mut ticker = tokio::time::interval(POLL_INTERVAL);
        loop {
            ticker.tick().await;
            match Self::fetch_status_with_retries(&node_client).await {
                Ok(status) => {
                    let result = state.compute_result(&status).await;
                    *state.cached_result.lock().await = Some(result);
                }
                Err(err) => {
                    warn!(%err, "eth_syncing: failed to refresh node status after retries");
                    *state.cached_result.lock().await = None;
                }
            }
        }
    }

    /// Fetches the node's `/status` endpoint, retrying up to [`MAX_FETCH_ATTEMPTS`] times, each
    /// attempt bounded by [`FETCH_ATTEMPT_TIMEOUT`].
    async fn fetch_status_with_retries(
        node_client: &Arc<dyn NodeClient>,
    ) -> Result<RestNodeStatus, ClientError> {
        let mut last_err = None;
        for attempt in 1..=MAX_FETCH_ATTEMPTS {
            match tokio::time::timeout(FETCH_ATTEMPT_TIMEOUT, node_client.read_rest_node_status())
                .await
            {
                Ok(Ok(status)) => return Ok(status),
                Ok(Err(err)) => {
                    warn!(%err, attempt, "eth_syncing: failed to fetch node status");
                    last_err = Some(err);
                }
                Err(_) => {
                    warn!(
                        attempt,
                        timeout_secs = FETCH_ATTEMPT_TIMEOUT.as_secs(),
                        "eth_syncing: timed out fetching node status"
                    );
                    last_err = Some(ClientError::RestRequestFailed(format!(
                        "timed out after {}s",
                        FETCH_ATTEMPT_TIMEOUT.as_secs()
                    )));
                }
            }
        }
        Err(last_err.expect("loop always runs at least once and sets last_err on every failure"))
    }

    /// Computes the `eth_syncing` result for a freshly fetched status, carrying `starting_block`
    /// forward from the previous cached result (see the struct docs).
    async fn compute_result(&self, status: &RestNodeStatus) -> SyncingResult {
        if !is_catching_up(&status.reactor_state) {
            return SyncingResult::NotSyncing(false);
        }

        let current_block = status.available_block_range.high();
        let starting_block = match *self.cached_result.lock().await {
            Some(SyncingResult::Syncing(SyncingStatus { starting_block, .. })) => {
                starting_block.as_u64().unwrap_or(current_block)
            }
            _ => current_block,
        };
        let highest_block =
            highest_known_block_height(&status.block_sync, current_block).max(current_block);

        SyncingResult::Syncing(SyncingStatus {
            starting_block: EthU256::from(starting_block),
            current_block: EthU256::from(current_block),
            highest_block: EthU256::from(highest_block),
        })
    }
}

#[async_trait]
impl RpcWithoutParams for Syncing {
    const METHOD: &'static str = Syncing::METHOD;
    type ResponseResult = SyncingResult;

    async fn do_handle_request(
        _node_client: Arc<dyn NodeClient>,
    ) -> Result<Self::ResponseResult, RpcError> {
        Err(internal_error(
            "eth_syncing requires process-local sync-start state",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use async_trait::async_trait;
    use casper_binary_port::{BinaryResponseAndRequest, Command};
    use casper_types::{AvailableBlockRange, BlockSynchronizerStatus};
    use serde_json::json;

    use super::*;
    use crate::{ClientError, node_client::RestNodeStatus};

    /// Populates `state`'s cache as the background updater would for a freshly fetched `status`,
    /// without going through the node client or the polling loop.
    async fn poll_and_cache(state: &Syncing, status: RestNodeStatus) -> SyncingResult {
        let result = state.compute_result(&status).await;
        *state.cached_result.lock().await = Some(result);
        result
    }

    /// Repeatedly yields to the executor so that any tasks woken by a `tokio::spawn` or a
    /// `tokio::time::advance` get to run to completion before shared state is inspected. Virtual
    /// time stays paused throughout, so this adds no real wall-clock delay.
    async fn settle() {
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
    }

    fn rest_status(reactor_state: &str, block_sync: BlockSynchronizerStatus) -> RestNodeStatus {
        RestNodeStatus {
            reactor_state: reactor_state.to_string(),
            available_block_range: AvailableBlockRange::new(5, 100),
            block_sync,
        }
    }

    #[tokio::test]
    async fn errors_when_cache_not_yet_populated() {
        let state = Arc::new(Syncing::new());

        let result = Syncing::do_handle_request(state).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn reports_false_once_caught_up() {
        let state = Arc::new(Syncing::new());
        let status = rest_status("KeepUp", BlockSynchronizerStatus::new(None, None));
        poll_and_cache(&state, status).await;

        let result = Syncing::do_handle_request(state.clone())
            .await
            .expect("syncing lookup should succeed");

        assert_eq!(result, SyncingResult::NotSyncing(false));
        assert_eq!(serde_json::to_value(result).unwrap(), json!(false));
    }

    #[tokio::test]
    async fn reports_progress_while_catching_up() {
        let state = Arc::new(Syncing::new());
        let block_sync = BlockSynchronizerStatus::new(
            Some(casper_types::BlockSyncStatus::new(
                Default::default(),
                Some(250),
                "have block header(250)".to_string(),
            )),
            None,
        );
        poll_and_cache(&state, rest_status("CatchUp", block_sync)).await;

        let result = Syncing::do_handle_request(state.clone())
            .await
            .expect("syncing lookup should succeed");

        assert_eq!(
            result,
            SyncingResult::Syncing(SyncingStatus {
                starting_block: EthU256::from(100u64),
                current_block: EthU256::from(100u64),
                highest_block: EthU256::from(250u64),
            })
        );
    }

    #[tokio::test]
    async fn falls_back_to_current_block_when_sync_target_unknown() {
        let state = Arc::new(Syncing::new());
        poll_and_cache(
            &state,
            rest_status("Initialize", BlockSynchronizerStatus::new(None, None)),
        )
        .await;

        let result = Syncing::do_handle_request(state.clone())
            .await
            .expect("syncing lookup should succeed");

        assert_eq!(
            result,
            SyncingResult::Syncing(SyncingStatus {
                starting_block: EthU256::from(100u64),
                current_block: EthU256::from(100u64),
                highest_block: EthU256::from(100u64),
            })
        );
    }

    #[tokio::test]
    async fn starting_block_stays_fixed_while_catching_up() {
        let block_sync = BlockSynchronizerStatus::new(
            Some(casper_types::BlockSyncStatus::new(
                Default::default(),
                Some(250),
                "have block header(250)".to_string(),
            )),
            None,
        );
        let state = Arc::new(Syncing::new());

        let first = poll_and_cache(
            &state,
            RestNodeStatus {
                reactor_state: "CatchUp".to_string(),
                available_block_range: AvailableBlockRange::new(5, 100),
                block_sync: block_sync.clone(),
            },
        )
        .await;
        let second = poll_and_cache(
            &state,
            RestNodeStatus {
                reactor_state: "CatchUp".to_string(),
                available_block_range: AvailableBlockRange::new(50, 180),
                block_sync,
            },
        )
        .await;

        assert_eq!(
            first,
            SyncingResult::Syncing(SyncingStatus {
                starting_block: EthU256::from(100u64),
                current_block: EthU256::from(100u64),
                highest_block: EthU256::from(250u64),
            })
        );
        assert_eq!(
            second,
            SyncingResult::Syncing(SyncingStatus {
                starting_block: EthU256::from(100u64),
                current_block: EthU256::from(180u64),
                highest_block: EthU256::from(250u64),
            })
        );
        assert_eq!(
            Syncing::do_handle_request(state.clone())
                .await
                .expect("syncing lookup should succeed"),
            second
        );
    }

    #[tokio::test]
    async fn starting_block_cache_clears_once_caught_up() {
        let no_block_sync = || BlockSynchronizerStatus::new(None, None);
        let state = Arc::new(Syncing::new());

        let first = poll_and_cache(
            &state,
            RestNodeStatus {
                reactor_state: "CatchUp".to_string(),
                available_block_range: AvailableBlockRange::new(5, 100),
                block_sync: no_block_sync(),
            },
        )
        .await;
        let second = poll_and_cache(
            &state,
            RestNodeStatus {
                reactor_state: "KeepUp".to_string(),
                available_block_range: AvailableBlockRange::new(100, 100),
                block_sync: no_block_sync(),
            },
        )
        .await;
        let third = poll_and_cache(
            &state,
            RestNodeStatus {
                reactor_state: "CatchUp".to_string(),
                available_block_range: AvailableBlockRange::new(120, 300),
                block_sync: no_block_sync(),
            },
        )
        .await;

        assert_eq!(
            first,
            SyncingResult::Syncing(SyncingStatus {
                starting_block: EthU256::from(100u64),
                current_block: EthU256::from(100u64),
                highest_block: EthU256::from(100u64),
            })
        );
        assert_eq!(second, SyncingResult::NotSyncing(false));
        assert_eq!(
            third,
            SyncingResult::Syncing(SyncingStatus {
                starting_block: EthU256::from(300u64),
                current_block: EthU256::from(300u64),
                highest_block: EthU256::from(300u64),
            })
        );
    }

    /// A `NodeClient` whose `read_rest_node_status` fails a fixed number of times before
    /// succeeding, to exercise the retry behavior of `fetch_status_with_retries`.
    struct FailNTimesMock {
        remaining_failures: AtomicU32,
        status: RestNodeStatus,
    }

    #[async_trait]
    impl NodeClient for FailNTimesMock {
        async fn send_request(
            &self,
            req: Command,
        ) -> Result<BinaryResponseAndRequest, ClientError> {
            unimplemented!("eth_syncing should not use the binary port, got: {:?}", req)
        }

        async fn read_rest_node_status(&self) -> Result<RestNodeStatus, ClientError> {
            if self.remaining_failures.fetch_sub(1, Ordering::SeqCst) > 0 {
                Err(ClientError::RestRequestFailed("mock failure".to_string()))
            } else {
                Ok(self.status.clone())
            }
        }
    }

    #[tokio::test]
    async fn fetch_retries_until_success_within_max_attempts() {
        let status = rest_status("KeepUp", BlockSynchronizerStatus::new(None, None));
        let client: Arc<dyn NodeClient> = Arc::new(FailNTimesMock {
            remaining_failures: AtomicU32::new(MAX_FETCH_ATTEMPTS - 1),
            status: status.clone(),
        });

        let result = Syncing::fetch_status_with_retries(&client)
            .await
            .expect("should succeed within max attempts");

        assert_eq!(result, status);
    }

    #[tokio::test]
    async fn fetch_gives_up_after_max_attempts() {
        let client: Arc<dyn NodeClient> = Arc::new(FailNTimesMock {
            remaining_failures: AtomicU32::new(MAX_FETCH_ATTEMPTS + 10),
            status: rest_status("KeepUp", BlockSynchronizerStatus::new(None, None)),
        });

        let result = Syncing::fetch_status_with_retries(&client).await;

        assert!(result.is_err());
    }

    /// A `NodeClient` whose `read_rest_node_status` never resolves, to exercise the per-attempt
    /// timeout of `fetch_status_with_retries`.
    struct HangingMock;

    #[async_trait]
    impl NodeClient for HangingMock {
        async fn send_request(
            &self,
            req: Command,
        ) -> Result<BinaryResponseAndRequest, ClientError> {
            unimplemented!("eth_syncing should not use the binary port, got: {:?}", req)
        }

        async fn read_rest_node_status(&self) -> Result<RestNodeStatus, ClientError> {
            std::future::pending().await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn fetch_times_out_on_slow_responses() {
        let client: Arc<dyn NodeClient> = Arc::new(HangingMock);

        let result = Syncing::fetch_status_with_retries(&client).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn highest_block_uses_max_of_historical_and_forward_sync_targets() {
        let block_sync = BlockSynchronizerStatus::new(
            Some(casper_types::BlockSyncStatus::new(
                Default::default(),
                Some(400),
                "have block header(400)".to_string(),
            )),
            Some(casper_types::BlockSyncStatus::new(
                Default::default(),
                Some(150),
                "have block header(150)".to_string(),
            )),
        );
        let state = Arc::new(Syncing::new());

        let result = poll_and_cache(&state, rest_status("CatchUp", block_sync)).await;

        assert_eq!(
            result,
            SyncingResult::Syncing(SyncingStatus {
                starting_block: EthU256::from(100u64),
                current_block: EthU256::from(100u64),
                highest_block: EthU256::from(400u64),
            })
        );
    }

    #[tokio::test]
    async fn syncing_result_serializes_as_camel_case_hex_quantities() {
        let result = SyncingResult::Syncing(SyncingStatus {
            starting_block: EthU256::from(100u64),
            current_block: EthU256::from(180u64),
            highest_block: EthU256::from(250u64),
        });

        assert_eq!(
            serde_json::to_value(result).unwrap(),
            json!({
                "startingBlock": "0x64",
                "currentBlock": "0xb4",
                "highestBlock": "0xfa",
            })
        );
    }

    /// A `NodeClient` whose `read_rest_node_status` succeeds every call, reporting an
    /// ever-increasing `current_block` so each poll is distinguishable from the last.
    struct IncreasingStatusMock {
        calls: AtomicU32,
    }

    #[async_trait]
    impl NodeClient for IncreasingStatusMock {
        async fn send_request(
            &self,
            req: Command,
        ) -> Result<BinaryResponseAndRequest, ClientError> {
            unimplemented!("eth_syncing should not use the binary port, got: {:?}", req)
        }

        async fn read_rest_node_status(&self) -> Result<RestNodeStatus, ClientError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let high = 100 + u64::from(call) * 10;
            Ok(RestNodeStatus {
                reactor_state: "CatchUp".to_string(),
                available_block_range: AvailableBlockRange::new(5, high),
                block_sync: BlockSynchronizerStatus::new(None, None),
            })
        }
    }

    /// A `NodeClient` whose `read_rest_node_status` always fails, without ever blocking - used to
    /// exercise what the background updater does once retries are exhausted.
    struct AlwaysFailMock;

    #[async_trait]
    impl NodeClient for AlwaysFailMock {
        async fn send_request(
            &self,
            req: Command,
        ) -> Result<BinaryResponseAndRequest, ClientError> {
            unimplemented!("eth_syncing should not use the binary port, got: {:?}", req)
        }

        async fn read_rest_node_status(&self) -> Result<RestNodeStatus, ClientError> {
            Err(ClientError::RestRequestFailed("mock failure".to_string()))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn status_updater_polls_immediately_then_every_poll_interval() {
        let state = Arc::new(Syncing::new());
        let client: Arc<dyn NodeClient> = Arc::new(IncreasingStatusMock {
            calls: AtomicU32::new(0),
        });
        let updater = tokio::spawn(Syncing::run_status_updater(state.clone(), client));

        // The first poll fires immediately on start, with no need to advance the clock.
        settle().await;
        let first = Syncing::do_handle_request(state.clone())
            .await
            .expect("cache should be populated after the initial poll");
        assert_eq!(
            first,
            SyncingResult::Syncing(SyncingStatus {
                starting_block: EthU256::from(100u64),
                current_block: EthU256::from(100u64),
                highest_block: EthU256::from(100u64),
            })
        );

        // Advancing by exactly one poll interval should trigger exactly one more poll.
        tokio::time::advance(POLL_INTERVAL).await;
        settle().await;
        let second = Syncing::do_handle_request(state.clone())
            .await
            .expect("cache should be refreshed after the second poll");
        assert_eq!(
            second,
            SyncingResult::Syncing(SyncingStatus {
                starting_block: EthU256::from(100u64),
                current_block: EthU256::from(110u64),
                highest_block: EthU256::from(110u64),
            })
        );

        updater.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn status_updater_flushes_cache_on_failure() {
        let state = Arc::new(Syncing::new());
        poll_and_cache(
            &state,
            rest_status("KeepUp", BlockSynchronizerStatus::new(None, None)),
        )
        .await;

        let client: Arc<dyn NodeClient> = Arc::new(AlwaysFailMock);
        let updater = tokio::spawn(Syncing::run_status_updater(state.clone(), client));

        // The first poll runs, exhausts all retries, and gives up - but must leave the
        // previously cached result in place rather than clearing or erroring it out.
        settle().await;
        let result = Syncing::do_handle_request(state.clone())
            .await
            .err()
            .expect("expected error");

        assert_eq!(
            result,
            internal_error("node sync status is not yet available, try again shortly")
        );

        updater.abort();
    }
}
