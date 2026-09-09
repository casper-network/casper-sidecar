//! A process-wide cache of node-derived state, shared across the RPC server, the caching node
//! client and the background updater tasks.
//!
//! It holds several independent pieces of information, each behind its own lock:
//!
//! * `node_syncing_status` - the most recently computed `eth_syncing` result, refreshed by the
//!   background poller in [`crate::rpcs::eth::syncing`].
//! * `latest_block` / `latest_block_header` - the newest block observed on the sidecar's SSE
//!   `BlockAdded` feed, together with the [`Instant`] it was observed. These are only present
//!   (outer `Some`) when the SSE server is enabled; without a feed there is nothing to populate
//!   them and they must never be consulted.
//! * `chainspec` - the node's `(ProtocolVersion, Chainspec)` plus the raw bytes, so hot config
//!   reads (`eth_chainId`, `eth_netVersion`, ...) don't re-fetch and re-parse the chainspec on
//!   every call. Hydrated once after connecting to the node and refreshed whenever the observed
//!   protocol version changes.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use casper_types::{Block, BlockHeader, Chainspec, ChainspecRawBytes, ProtocolVersion, TimeDiff};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::{ClientError, NodeClient, rpcs::eth::syncing::SyncingResult};

/// How often the SSE-disabled fallback poller asks the node for its protocol version.
pub(crate) const PROTOCOL_VERSION_POLL_INTERVAL: Duration = Duration::from_secs(60);

struct ChainspecEntry {
    protocol_version: ProtocolVersion,
    chainspec: Arc<Chainspec>,
    raw: Arc<ChainspecRawBytes>,
}

/// Optional cache of a value with the timestamp when it was stored.
type ObservedAt<T> = Option<Mutex<Option<(Instant, T)>>>;

pub struct NodeStateCache {
    node_syncing_status: Mutex<Option<SyncingResult>>,
    latest_block: ObservedAt<Arc<Block>>,
    latest_block_header: ObservedAt<BlockHeader>,
    chainspec: Mutex<Option<ChainspecEntry>>,
    /// How long an SSE-observed block/header is considered fresh. `Duration::ZERO` => never fresh (always fallback to querying the node).
    latest_block_ttl: Duration,
}

impl NodeStateCache {
    /// Creates a cache. `latest_block` / `latest_block_header` are tracked only when
    /// `sse_enabled` is `true`.
    pub fn new(sse_enabled: bool, latest_block_ttl: TimeDiff) -> Self {
        Self::with_ttl(
            sse_enabled,
            Duration::from_millis(latest_block_ttl.millis()),
        )
    }

    fn with_ttl(sse_enabled: bool, latest_block_ttl: Duration) -> Self {
        Self {
            node_syncing_status: Mutex::new(None),
            latest_block: sse_enabled.then(|| Mutex::new(None)),
            latest_block_header: sse_enabled.then(|| Mutex::new(None)),
            chainspec: Mutex::new(None),
            latest_block_ttl,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(sse_enabled: bool, latest_block_ttl: Duration) -> Self {
        Self::with_ttl(sse_enabled, latest_block_ttl)
    }

    #[cfg(test)]
    async fn seed_chainspec_for_test(&self, protocol_version: ProtocolVersion) {
        *self.chainspec.lock().await = Some(ChainspecEntry {
            protocol_version,
            chainspec: Arc::new(Chainspec::default()),
            raw: Arc::new(ChainspecRawBytes::new(Vec::new().into(), None, None)),
        });
    }

    fn is_fresh(&self, observed: Instant) -> bool {
        !self.latest_block_ttl.is_zero() && observed.elapsed() <= self.latest_block_ttl
    }

    pub(crate) async fn syncing_status(&self) -> Option<SyncingResult> {
        *self.node_syncing_status.lock().await
    }

    pub(crate) async fn set_syncing_status(&self, status: Option<SyncingResult>) {
        *self.node_syncing_status.lock().await = status;
    }

    pub fn tracks_latest_block(&self) -> bool {
        self.latest_block.is_some()
    }

    /// Records a block seen on the SSE `BlockAdded` feed, stamped with the current instant.
    ///
    /// When a node is catching up it will produce BlockAdded events out of order, so an
    /// observation is only stored when it is strictly higher than what is already
    /// cached - the cache always reflects the highest block seen, never an older one.
    pub async fn observe_block(&self, block: Arc<Block>) {
        let Some(block_slot) = &self.latest_block else {
            return; // SSE disabled - nothing to track.
        };
        let now = Instant::now();
        let mut block_guard = block_slot.lock().await;
        if let Some((_, current)) = block_guard.as_ref()
            && current.height() >= block.height()
        {
            return;
        }
        if let Some(header_slot) = &self.latest_block_header {
            *header_slot.lock().await = Some((now, block.clone_header()));
        }
        *block_guard = Some((now, block));
    }

    /// The latest observed block, but only if it was seen recently enough to still be trusted.
    pub async fn trusted_latest_block(&self) -> Option<Arc<Block>> {
        let guard = self.latest_block.as_ref()?.lock().await;
        let (observed, block) = guard.as_ref()?;
        self.is_fresh(*observed).then(|| Arc::clone(block))
    }

    /// The latest observed block header, but only if it is still trusted (see above).
    pub async fn trusted_latest_block_header(&self) -> Option<BlockHeader> {
        let guard = self.latest_block_header.as_ref()?.lock().await;
        let (observed, header) = guard.as_ref()?;
        self.is_fresh(*observed).then(|| header.clone())
    }

    pub async fn chainspec(&self) -> Option<Arc<Chainspec>> {
        self.chainspec
            .lock()
            .await
            .as_ref()
            .map(|entry| Arc::clone(&entry.chainspec))
    }

    pub async fn chainspec_raw(&self) -> Option<Arc<ChainspecRawBytes>> {
        self.chainspec
            .lock()
            .await
            .as_ref()
            .map(|entry| Arc::clone(&entry.raw))
    }

    async fn cached_protocol_version(&self) -> Option<ProtocolVersion> {
        self.chainspec
            .lock()
            .await
            .as_ref()
            .map(|entry| entry.protocol_version)
    }

    /// Fetches the node's protocol version and chainspec and stores both. Call once after the node
    /// connection is established, to hydrate the cache before the first request arrives.
    pub async fn hydrate_chainspec(&self, node_client: &dyn NodeClient) -> Result<(), ClientError> {
        let protocol_version = node_client.read_node_status().await?.protocol_version;
        self.refresh_chainspec(node_client, protocol_version).await
    }

    async fn refresh_chainspec(
        &self,
        node_client: &dyn NodeClient,
        protocol_version: ProtocolVersion,
    ) -> Result<(), ClientError> {
        match Self::fetch_chainspec(node_client, protocol_version).await {
            Ok(entry) => {
                *self.chainspec.lock().await = Some(entry);
                info!(%protocol_version, "node state cache: chainspec cached");
                Ok(())
            }
            Err(err) => {
                // Wipe any previously cached chainspec: it was fetched under a different protocol
                // version and we could not confirm it still applies, so callers must fall back to
                // the node rather than be served a stale config.
                *self.chainspec.lock().await = None;
                Err(err)
            }
        }
    }

    async fn fetch_chainspec(
        node_client: &dyn NodeClient,
        protocol_version: ProtocolVersion,
    ) -> Result<ChainspecEntry, ClientError> {
        let raw = node_client.read_chainspec_bytes().await?;
        let text = std::str::from_utf8(raw.chainspec_bytes()).map_err(|err| {
            ClientError::Deserialization(format!("chainspec bytes are not valid utf8: {err}"))
        })?;
        let chainspec: Chainspec = toml::from_str(text)
            .map_err(|err| ClientError::Deserialization(format!("chainspec toml: {err}")))?;
        Ok(ChainspecEntry {
            protocol_version,
            chainspec: Arc::new(chainspec),
            raw: Arc::new(raw),
        })
    }

    /// Reacts to an observed protocol version - from the SSE `ApiVersion` event or the fallback
    /// poller. Re-fetches and re-parses the chainspec whenever the version differs from what is
    /// cached (or nothing is cached yet).
    pub async fn on_protocol_version(
        &self,
        node_client: &dyn NodeClient,
        protocol_version: ProtocolVersion,
    ) {
        if self.cached_protocol_version().await == Some(protocol_version) {
            return;
        }
        info!(
            %protocol_version,
            "node state cache: protocol version changed, refreshing chainspec"
        );
        if let Err(err) = self.refresh_chainspec(node_client, protocol_version).await {
            warn!(
                %err,
                "node state cache: failed to refresh chainspec after protocol version change"
            );
        }
    }
}

/// Background task: when the SSE server is disabled there is no `ApiVersion` event to react to, so
/// poll the node's protocol version periodically and refresh the cached chainspec on change.
pub(crate) async fn protocol_version_poll_loop(
    node_client: Arc<dyn NodeClient>,
    cache: Arc<NodeStateCache>,
) -> Result<(), anyhow::Error> {
    let mut ticker = tokio::time::interval(PROTOCOL_VERSION_POLL_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        match node_client.read_node_status().await {
            Ok(status) => {
                cache
                    .on_protocol_version(node_client.as_ref(), status.protocol_version)
                    .await
            }
            Err(err) => warn!(%err, "node state cache: protocol version poll failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;
    use casper_binary_port::{BinaryResponseAndRequest, Command};
    use casper_types::{Block, TestBlockBuilder, testing::TestRng};

    use super::*;
    use crate::rpcs::eth::syncing::SyncingResult;

    const V2_0_0: ProtocolVersion = ProtocolVersion::from_parts(2, 0, 0);
    const V2_1_0: ProtocolVersion = ProtocolVersion::from_parts(2, 1, 0);

    /// A node client whose every binary port request fails, so `read_chainspec_bytes` errors.
    struct BinaryPortDown;

    #[async_trait]
    impl NodeClient for BinaryPortDown {
        async fn send_request(
            &self,
            _req: Command,
        ) -> Result<BinaryResponseAndRequest, ClientError> {
            Err(ClientError::ConnectionLost)
        }
    }

    /// A node client that returns chainspec bytes that are not a valid `Chainspec`.
    struct GarbageChainspec;

    #[async_trait]
    impl NodeClient for GarbageChainspec {
        async fn send_request(
            &self,
            _req: Command,
        ) -> Result<BinaryResponseAndRequest, ClientError> {
            unreachable!("read_chainspec_bytes is overridden")
        }

        async fn read_chainspec_bytes(&self) -> Result<ChainspecRawBytes, ClientError> {
            Ok(ChainspecRawBytes::new(
                b"not a chainspec".to_vec().into(),
                None,
                None,
            ))
        }
    }

    #[tokio::test]
    async fn latest_block_not_tracked_without_sse() {
        let cache = NodeStateCache::new_for_test(false, Duration::from_secs(5));
        assert!(!cache.tracks_latest_block());
        assert!(cache.trusted_latest_block().await.is_none());
        assert!(cache.trusted_latest_block_header().await.is_none());
    }

    #[test]
    fn zero_ttl_never_trusts() {
        let cache = NodeStateCache::new_for_test(true, Duration::ZERO);
        assert!(!cache.is_fresh(Instant::now()));
    }

    #[test]
    fn fresh_within_ttl_stale_after() {
        let cache = NodeStateCache::new_for_test(true, Duration::from_secs(2));
        assert!(cache.is_fresh(Instant::now()));
        assert!(!cache.is_fresh(Instant::now() - Duration::from_secs(3)));
    }

    #[tokio::test]
    async fn observe_block_fills_both_slots_and_respects_ttl() {
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().build(rng));

        let fresh = NodeStateCache::new_for_test(true, Duration::from_secs(30));
        fresh.observe_block(Arc::new(block.clone())).await;
        assert_eq!(
            fresh.trusted_latest_block().await.map(|b| *b.hash()),
            Some(*block.hash())
        );
        assert_eq!(
            fresh.trusted_latest_block_header().await,
            Some(block.clone_header())
        );

        let stale = NodeStateCache::new_for_test(true, Duration::ZERO);
        stale.observe_block(Arc::new(block.clone())).await;
        assert!(stale.trusted_latest_block().await.is_none());
        assert!(stale.trusted_latest_block_header().await.is_none());
    }

    #[tokio::test]
    async fn observe_block_keeps_the_highest_block() {
        let rng = &mut TestRng::new();
        let low = Block::V2(TestBlockBuilder::new().height(5).build(rng));
        let high = Block::V2(TestBlockBuilder::new().height(9).build(rng));
        let higher = Block::V2(TestBlockBuilder::new().height(12).build(rng));

        let cache = NodeStateCache::new_for_test(true, Duration::from_secs(30));

        cache.observe_block(Arc::new(high.clone())).await;
        // An out-of-order / historical replay of an older block must be ignored...
        cache.observe_block(Arc::new(low)).await;
        assert_eq!(
            cache.trusted_latest_block().await.map(|b| b.height()),
            Some(9)
        );
        assert_eq!(
            cache
                .trusted_latest_block_header()
                .await
                .map(|h| h.height()),
            Some(9)
        );

        // ...but a strictly newer block replaces it.
        cache.observe_block(Arc::new(higher)).await;
        assert_eq!(
            cache.trusted_latest_block().await.map(|b| b.height()),
            Some(12)
        );
        assert_eq!(
            cache
                .trusted_latest_block_header()
                .await
                .map(|h| h.height()),
            Some(12)
        );
    }

    #[tokio::test]
    async fn refresh_chainspec_wipes_stale_entry_when_fetch_fails() {
        let cache = NodeStateCache::new_for_test(false, Duration::ZERO);
        cache.seed_chainspec_for_test(V2_0_0).await;
        assert!(cache.chainspec().await.is_some());

        let err = cache
            .refresh_chainspec(&BinaryPortDown, V2_1_0)
            .await
            .expect_err("fetch should fail");
        assert!(matches!(err, ClientError::ConnectionLost));

        assert!(cache.chainspec().await.is_none());
        assert!(cache.chainspec_raw().await.is_none());
        assert!(cache.cached_protocol_version().await.is_none());
    }

    #[tokio::test]
    async fn refresh_chainspec_wipes_stale_entry_when_bytes_do_not_parse() {
        let cache = NodeStateCache::new_for_test(false, Duration::ZERO);
        cache.seed_chainspec_for_test(V2_0_0).await;

        cache
            .refresh_chainspec(&GarbageChainspec, V2_1_0)
            .await
            .expect_err("parse should fail");

        assert!(cache.chainspec().await.is_none());
        assert!(cache.chainspec_raw().await.is_none());
    }

    #[tokio::test]
    async fn on_protocol_version_wipes_entry_when_refresh_fails() {
        let cache = NodeStateCache::new_for_test(false, Duration::ZERO);
        cache.seed_chainspec_for_test(V2_0_0).await;

        // Version changed -> refresh is attempted, fails -> entry wiped.
        cache.on_protocol_version(&BinaryPortDown, V2_1_0).await;
        assert!(cache.chainspec().await.is_none());
    }

    #[tokio::test]
    async fn on_protocol_version_keeps_entry_when_version_unchanged() {
        let cache = NodeStateCache::new_for_test(false, Duration::ZERO);
        cache.seed_chainspec_for_test(V2_0_0).await;

        // Same version -> no refresh attempted -> a failing client must not disturb the entry.
        cache.on_protocol_version(&BinaryPortDown, V2_0_0).await;
        assert!(cache.chainspec().await.is_some());
    }

    #[tokio::test]
    async fn syncing_status_round_trips() {
        let cache = NodeStateCache::new_for_test(false, Duration::ZERO);
        assert!(cache.syncing_status().await.is_none());
        cache
            .set_syncing_status(Some(SyncingResult::NotSyncing(false)))
            .await;
        assert_eq!(
            cache.syncing_status().await,
            Some(SyncingResult::NotSyncing(false))
        );
    }
}
