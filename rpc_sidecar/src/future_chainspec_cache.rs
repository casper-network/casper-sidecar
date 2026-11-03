//! Future protocol documents discovered via SSE and reconciled with the node's snapshot.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use casper_types::{ChainspecRawBytes, ProtocolVersion};
use tokio::sync::Mutex;
use tracing::warn;

use crate::{ClientError, NodeClient};

// The node's upgrade watcher also scans approximately every 30 seconds. A bounded
// lifetime ensures missed announcements, edits, removals and failed polls cannot
// cause an indefinitely stale document to be served.
const REFRESH_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Default)]
struct State {
    generation: u64,
    entries: BTreeMap<ProtocolVersion, (Instant, ChainspecRawBytes)>,
}

#[derive(Default)]
pub(crate) struct FutureChainspecCache {
    state: Mutex<State>,
}

impl FutureChainspecCache {
    pub(crate) async fn invalidate(&self) {
        let mut state = self.state.lock().await;
        state.generation = state.generation.wrapping_add(1);
        state.entries.clear();
    }

    pub(crate) async fn read(
        &self,
        node: &dyn NodeClient,
        version: ProtocolVersion,
    ) -> Result<Option<ChainspecRawBytes>, ClientError> {
        let generation = {
            let state = self.state.lock().await;
            if let Some((observed, raw)) = state.entries.get(&version)
                && observed.elapsed() < REFRESH_INTERVAL
            {
                return Ok(Some(raw.clone()));
            }
            state.generation
        };
        let raw = node.read_future_chainspec(version).await?;
        let mut state = self.state.lock().await;
        if state.generation == generation {
            match &raw {
                Some(raw) => {
                    state.entries.insert(version, (Instant::now(), raw.clone()));
                }
                None => {
                    state.entries.remove(&version);
                }
            }
        }
        Ok(raw)
    }

    /// Refresh the version announced by the node. An older in-flight fetch cannot
    /// overwrite this observation, and a failed fetch never leaves its old entry trusted.
    pub(crate) async fn observe(&self, node: &dyn NodeClient, version: ProtocolVersion) {
        let generation = {
            let mut state = self.state.lock().await;
            state.generation = state.generation.wrapping_add(1);
            state.entries.remove(&version);
            state.generation
        };
        match node.read_future_chainspec(version).await {
            Ok(Some(raw)) => {
                let mut state = self.state.lock().await;
                if state.generation == generation {
                    state.entries.insert(version, (Instant::now(), raw));
                }
            }
            Ok(None) => {} // The version was removed before we fetched it.
            Err(error) => warn!(%version, %error, "failed to cache staged chainspec"),
        }
    }

    /// Refresh all staged documents, including edits and deletions that do not emit SSE.
    pub(crate) async fn reconcile(&self, node: &dyn NodeClient) {
        let generation = {
            let mut state = self.state.lock().await;
            state.generation = state.generation.wrapping_add(1);
            state.generation
        };
        let result = async {
            let versions = node.read_staged_protocol_versions().await?;
            let mut entries = BTreeMap::new();
            for version in versions {
                if let Some(raw) = node.read_future_chainspec(version).await? {
                    entries.insert(version, (Instant::now(), raw));
                }
            }
            Ok::<_, ClientError>(entries)
        }
        .await;
        let mut state = self.state.lock().await;
        if state.generation != generation {
            return; // A newer SSE event or refresh superseded this snapshot.
        }
        match result {
            Ok(entries) => state.entries = entries,
            Err(error) => {
                state.entries.clear();
                warn!(%error, "failed to reconcile staged chainspec cache");
            }
        }
    }
}

/// The periodic snapshot also covers configurations without an SSE listener.
pub(crate) async fn refresh_loop(
    node: Arc<dyn NodeClient>,
    cache: Arc<FutureChainspecCache>,
) -> Result<(), anyhow::Error> {
    let mut ticker = tokio::time::interval(REFRESH_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        cache.reconcile(node.as_ref()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        binary_port_cache::HeedBinaryPortCache,
        caching_node_client::{CachingNodeClient, cache_update_loop},
        node_state_cache::NodeStateCache,
    };
    use async_trait::async_trait;
    use casper_binary_port::{BinaryResponseAndRequest, Command};
    use casper_event_types::SidecarEvent;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::{broadcast, oneshot};

    const V1: ProtocolVersion = ProtocolVersion::from_parts(2, 1, 0);
    const V2: ProtocolVersion = ProtocolVersion::from_parts(2, 2, 0);

    type Pause = (oneshot::Sender<()>, oneshot::Receiver<()>);

    #[derive(Default)]
    struct MockNode {
        documents: Mutex<BTreeMap<ProtocolVersion, Vec<u8>>>,
        failed: AtomicBool,
        reads: AtomicUsize,
        pause_next: Mutex<Option<Pause>>,
    }

    impl MockNode {
        async fn set(&self, version: ProtocolVersion, data: &[u8]) {
            self.documents.lock().await.insert(version, data.to_vec());
        }
    }

    #[async_trait]
    impl NodeClient for MockNode {
        async fn send_request(&self, _: Command) -> Result<BinaryResponseAndRequest, ClientError> {
            unreachable!("the mock overrides the relevant queries")
        }

        async fn read_staged_protocol_versions(&self) -> Result<Vec<ProtocolVersion>, ClientError> {
            if self.failed.load(Ordering::SeqCst) {
                return Err(ClientError::ConnectionLost);
            }
            Ok(self.documents.lock().await.keys().copied().collect())
        }

        async fn read_future_chainspec(
            &self,
            version: ProtocolVersion,
        ) -> Result<Option<ChainspecRawBytes>, ClientError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if self.failed.load(Ordering::SeqCst) {
                return Err(ClientError::ConnectionLost);
            }
            let raw = self
                .documents
                .lock()
                .await
                .get(&version)
                .cloned()
                .map(|bytes| ChainspecRawBytes::new(bytes.into(), None, None));
            let pause = self.pause_next.lock().await.take();
            if let Some((started, resume)) = pause {
                let _ = started.send(());
                let _ = resume.await;
            }
            Ok(raw)
        }
    }

    fn bytes(raw: &ChainspecRawBytes) -> &[u8] {
        raw.chainspec_bytes()
    }

    #[tokio::test]
    async fn announcements_cache_documents_and_reconciliation_handles_edits_and_removals() {
        let node = MockNode::default();
        let cache = FutureChainspecCache::default();
        node.set(V1, b"original").await;
        cache.observe(&node, V1).await;
        assert_eq!(
            bytes(&cache.read(&node, V1).await.unwrap().unwrap()),
            b"original"
        );
        assert_eq!(node.reads.load(Ordering::SeqCst), 1);

        node.set(V1, b"edited").await;
        node.set(V2, b"second").await;
        cache.reconcile(&node).await;
        assert_eq!(
            bytes(&cache.read(&node, V1).await.unwrap().unwrap()),
            b"edited"
        );
        assert_eq!(
            bytes(&cache.read(&node, V2).await.unwrap().unwrap()),
            b"second"
        );

        node.documents.lock().await.remove(&V1);
        cache.reconcile(&node).await;
        assert!(cache.read(&node, V1).await.unwrap().is_none());
        assert!(!cache.state.lock().await.entries.contains_key(&V1));
    }

    #[tokio::test]
    async fn refresh_failures_and_expired_entries_fall_back_to_the_node() {
        let node = MockNode::default();
        let cache = FutureChainspecCache::default();
        node.set(V1, b"old").await;
        cache.observe(&node, V1).await;
        node.failed.store(true, Ordering::SeqCst);
        cache.reconcile(&node).await;
        assert!(cache.read(&node, V1).await.is_err());
        assert!(cache.state.lock().await.entries.is_empty());

        node.failed.store(false, Ordering::SeqCst);
        node.set(V1, b"new").await;
        cache.state.lock().await.entries.insert(
            V1,
            (
                Instant::now() - REFRESH_INTERVAL,
                ChainspecRawBytes::new(b"old".to_vec().into(), None, None),
            ),
        );
        assert_eq!(
            bytes(&cache.read(&node, V1).await.unwrap().unwrap()),
            b"new"
        );
    }

    #[tokio::test]
    async fn older_in_flight_fetch_does_not_overwrite_new_announcement() {
        let node = Arc::new(MockNode::default());
        let cache = Arc::new(FutureChainspecCache::default());
        node.set(V1, b"old").await;
        let (started_tx, started_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *node.pause_next.lock().await = Some((started_tx, resume_rx));
        let task = tokio::spawn({
            let node = node.clone();
            let cache = cache.clone();
            async move { cache.read(node.as_ref(), V1).await.unwrap() }
        });
        started_rx.await.unwrap();
        node.set(V1, b"new").await;
        cache.observe(node.as_ref(), V1).await;
        resume_tx.send(()).unwrap();
        assert_eq!(bytes(&task.await.unwrap().unwrap()), b"old");
        assert_eq!(
            bytes(&cache.read(node.as_ref(), V1).await.unwrap().unwrap()),
            b"new"
        );
    }

    #[tokio::test]
    async fn refresh_loop_hydrates_when_sse_is_disabled() {
        let node = Arc::new(MockNode::default());
        let state = NodeStateCache::new_for_test(false, Duration::ZERO);
        node.set(V1, b"installed before startup").await;
        let task = tokio::spawn(refresh_loop(node, state.future_chainspecs.clone()));
        wait_for_cached(&state.future_chainspecs, V1, b"installed before startup").await;
        task.abort();
    }

    #[tokio::test]
    async fn event_loop_caches_announcements_and_reconciles_after_reconnect() {
        let node = Arc::new(MockNode::default());
        let state = Arc::new(NodeStateCache::new_for_test(true, Duration::ZERO));
        let client = Arc::new(CachingNodeClient::new(
            node.clone(),
            None::<Arc<HeedBinaryPortCache>>,
            state.clone(),
        ));
        let (tx, rx) = broadcast::channel(16);
        let task = tokio::spawn(cache_update_loop(client.clone(), rx));

        node.set(V1, b"announced").await;
        tx.send(SidecarEvent::StagedProtocol(V1)).unwrap();
        wait_for_cached(&state.future_chainspecs, V1, b"announced").await;
        assert_eq!(
            bytes(&client.read_future_chainspec(V1).await.unwrap().unwrap()),
            b"announced"
        );
        assert_eq!(node.reads.load(Ordering::SeqCst), 1);

        // The node can miss announcements during a disconnect, or edit a document
        // in place without an announcement. Reconnection must reconcile both.
        node.set(V1, b"edited").await;
        node.set(V2, b"missed").await;
        tx.send(SidecarEvent::SseConnected).unwrap();
        wait_for_cached(&state.future_chainspecs, V1, b"edited").await;
        wait_for_cached(&state.future_chainspecs, V2, b"missed").await;
        assert_eq!(
            client.read_staged_protocol_versions().await.unwrap(),
            vec![V1, V2]
        );
        task.abort();
    }

    async fn wait_for_cached(
        cache: &FutureChainspecCache,
        version: ProtocolVersion,
        expected: &[u8],
    ) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if cache
                    .state
                    .lock()
                    .await
                    .entries
                    .get(&version)
                    .is_some_and(|(_, raw)| bytes(raw) == expected)
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cache was not updated");
    }
}
