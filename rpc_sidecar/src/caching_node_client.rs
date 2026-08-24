use crate::{
    ClientError, NodeClient,
    binary_port_cache::{BinaryPortCache, InFlightDataHandling},
    node_client::RestNodeStatus,
    parse_response,
};
use anyhow::Error;
use async_trait::async_trait;
use casper_binary_port::{
    BinaryResponseAndRequest, Command, InformationRequest, TransactionWithExecutionInfo,
};
use casper_event_types::SidecarEvent;
use casper_types::{
    BlockHeader, BlockIdentifier, BlockWithSignatures, TransactionHash, execution::ExecutionResult,
};
use metrics::binary_port_cache as cache_metrics;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tracing::warn;

pub struct CachingNodeClient<T: NodeClient + Send + Sync, C: BinaryPortCache + InFlightDataHandling>
{
    inner_client: Arc<T>,
    binary_port_cache: Option<Arc<C>>,
}

impl<T: NodeClient + Send + Sync, C: BinaryPortCache + InFlightDataHandling>
    CachingNodeClient<T, C>
{
    pub(crate) fn new(inner_client: Arc<T>, binary_port_cache: Option<Arc<C>>) -> Self {
        Self {
            inner_client,
            binary_port_cache,
        }
    }
}

#[async_trait]
impl<T: NodeClient + Send + Sync, C: BinaryPortCache + InFlightDataHandling> NodeClient
    for CachingNodeClient<T, C>
{
    async fn send_request(&self, req: Command) -> Result<BinaryResponseAndRequest, ClientError> {
        self.inner_client.send_request(req).await
    }

    async fn read_rest_node_status(&self) -> Result<RestNodeStatus, ClientError> {
        self.inner_client.read_rest_node_status().await
    }

    async fn read_block_with_signatures(
        &self,
        block_identifier: Option<BlockIdentifier>,
    ) -> Result<Option<BlockWithSignatures>, ClientError> {
        // The persistent binary port cache is keyed by identifier, so it has no entry for
        // "whatever the latest block is" - always fall through to the node for that case.
        let Some(id) = block_identifier else {
            let resp = self
                .read_info(InformationRequest::BlockWithSignatures(None))
                .await?;
            return parse_response::<BlockWithSignatures>(&resp.into());
        };
        if let Some(cache) = &self.binary_port_cache {
            match cache.get_block_with_signatures(id).await {
                Ok(envelope) => {
                    if let Some(block) = envelope.into_option() {
                        cache_metrics::record_cache_lookup("block_with_signatures", true);
                        return Ok(Some(block));
                    }
                    cache_metrics::record_cache_lookup("block_with_signatures", false);
                }
                Err(err) => {
                    cache_metrics::record_cache_lookup("block_with_signatures", false);
                    warn!(%err, "binary port cache: get_block_with_signatures failed");
                }
            }
        }
        let resp = self
            .read_info(InformationRequest::BlockWithSignatures(Some(id)))
            .await?;
        let block = parse_response::<BlockWithSignatures>(&resp.into())?;
        if let (Some(cache), Some(block)) = (&self.binary_port_cache, &block)
            && let Err(err) = cache.put_block_with_signatures(block).await
        {
            warn!(%err, "binary port cache: put_block_with_signatures failed");
        }
        Ok(block)
    }

    async fn read_block_header(
        &self,
        block_identifier: Option<BlockIdentifier>,
    ) -> Result<Option<BlockHeader>, ClientError> {
        let Some(id) = block_identifier else {
            let resp = self
                .read_info(InformationRequest::BlockHeader(None))
                .await?;
            return parse_response::<BlockHeader>(&resp.into());
        };
        if let Some(cache) = &self.binary_port_cache {
            match cache.get_block_header(id).await {
                Ok(envelope) => {
                    if let Some(header) = envelope.into_option() {
                        cache_metrics::record_cache_lookup("block_header", true);
                        return Ok(Some(header));
                    }
                    cache_metrics::record_cache_lookup("block_header", false);
                }
                Err(err) => {
                    cache_metrics::record_cache_lookup("block_header", false);
                    warn!(%err, "binary port cache: get_block_header failed");
                }
            }
        }
        let resp = self
            .read_info(InformationRequest::BlockHeader(Some(id)))
            .await?;
        let header = parse_response::<BlockHeader>(&resp.into())?;
        if let (Some(cache), Some(header)) = (&self.binary_port_cache, &header)
            && let Err(err) = cache.put_block_header(header).await
        {
            warn!(%err, "binary port cache: put_block_header failed");
        }
        Ok(header)
    }

    async fn read_transaction_with_execution_info(
        &self,
        hash: TransactionHash,
        with_finalized_approvals: bool,
    ) -> Result<Option<TransactionWithExecutionInfo>, ClientError> {
        if let Some(cache) = &self.binary_port_cache {
            match cache
                .get_transaction_with_execution_info(hash, with_finalized_approvals)
                .await
            {
                Ok(envelope) => {
                    let hit = envelope.into_option();
                    if hit.is_some() {
                        cache_metrics::record_cache_lookup("transaction_with_execution_info", true);
                        return Ok(hit);
                    }
                    cache_metrics::record_cache_lookup("transaction_with_execution_info", false);
                }
                Err(err) => {
                    cache_metrics::record_cache_lookup("transaction_with_execution_info", false);
                    warn!(%err, "binary port cache: get_transaction_with_execution_info failed")
                }
            }
        }
        let resp = self
            .read_info(InformationRequest::Transaction {
                hash,
                with_finalized_approvals,
            })
            .await?;
        let Some(transaction) = parse_response::<TransactionWithExecutionInfo>(&resp.into())?
        else {
            return Ok(None);
        };
        let (transaction, execution_info) = transaction.into_inner();
        // A transaction without execution info yet is still pending, not immutable data - it
        // will transition to `Some(..)` once executed, so caching it now would risk permanently
        // serving a stale "no result yet" answer.
        if let (Some(cache), Some(execution_info)) = (&self.binary_port_cache, &execution_info) {
            let to_cache = TransactionWithExecutionInfo::new(
                transaction.clone(),
                Some(execution_info.clone()),
            );
            if let Err(err) = cache
                .put_transaction_with_execution_info(hash, with_finalized_approvals, &to_cache)
                .await
            {
                warn!(%err, "binary port cache: put_transaction_with_execution_info failed");
            }
        }
        Ok(Some(TransactionWithExecutionInfo::new(
            transaction,
            execution_info,
        )))
    }

    async fn read_transaction_execution_result(
        &self,
        hash: TransactionHash,
    ) -> Result<Option<ExecutionResult>, ClientError> {
        if let Some(cache) = &self.binary_port_cache {
            match cache.get_transaction_execution_result(hash).await {
                Ok(envelope) => {
                    if let Some(result) = envelope.into_option() {
                        cache_metrics::record_cache_lookup("transaction_execution_result", true);
                        return Ok(Some(result));
                    }
                    cache_metrics::record_cache_lookup("transaction_execution_result", false);
                }
                Err(err) => {
                    cache_metrics::record_cache_lookup("transaction_execution_result", false);
                    warn!(%err, "binary port cache: get_transaction_execution_result failed");
                }
            }
        }
        // No data found in cache, fallback to asking the node directly
        let with_info = self
            .read_transaction_with_execution_info(hash, false)
            .await?;
        let execution_result = with_info
            .and_then(|transaction| transaction.into_inner().1)
            .and_then(|execution_info| execution_info.execution_result);
        if let (Some(cache), Some(result)) = (&self.binary_port_cache, &execution_result)
            && let Err(err) = cache.put_transaction_execution_result(hash, result).await
        {
            warn!(%err, "binary port cache: put_transaction_execution_result failed");
        }
        Ok(execution_result)
    }
}

pub(crate) async fn cache_update_loop<
    T: NodeClient + Send + Sync + 'static,
    C: BinaryPortCache + InFlightDataHandling + 'static,
>(
    client: Arc<CachingNodeClient<T, C>>,
    mut sidecar_event_receiver: Receiver<SidecarEvent>,
) -> Result<(), Error> {
    loop {
        match sidecar_event_receiver.recv().await {
            Ok(msg) => {
                if let Some(handler) = client.binary_port_cache.clone()
                    && let Err(err) = handler.handle_sidecar_event(msg).await
                {
                    warn!(%err, "binary port cache: failed to handle sidecar event");
                }
            }
            Err(x) => match x {
                tokio::sync::broadcast::error::RecvError::Closed => {
                    anyhow::bail!(
                        "In cache_update_loop: internal broadcast mechanism of sidecar events failed."
                    );
                }
                tokio::sync::broadcast::error::RecvError::Lagged(_) => {
                    warn!("lag detected in cache_update_loop");
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CachingNodeClient, cache_update_loop};
    use crate::binary_port_cache::{BinaryPortCache, HeedBinaryPortCache};
    use crate::node_client::RestNodeStatus;
    use crate::{ClientError, NodeClient, rpcs::test_utils::BinaryPortMock};
    use async_trait::async_trait;
    use casper_binary_port::{BinaryResponseAndRequest, Command, InformationRequest};
    use casper_event_types::SidecarEvent;
    use casper_types::{
        AvailableBlockRange, Block, BlockSignatures, BlockSynchronizerStatus, BlockWithSignatures,
        EraId, TestBlockBuilder, testing::TestRng,
    };
    use rand::Rng;
    use std::{sync::Arc, time::Duration};
    use tokio::sync::broadcast;

    /// Inner client whose `read_rest_node_status` is distinguishable from the trait's default
    /// (which reports the request as unsupported), so a test can prove `CachingNodeClient`
    /// forwards to it rather than silently falling back to that default.
    struct RestStatusOnlyMock {
        status: RestNodeStatus,
    }

    #[async_trait]
    impl NodeClient for RestStatusOnlyMock {
        async fn send_request(
            &self,
            req: Command,
        ) -> Result<BinaryResponseAndRequest, ClientError> {
            unimplemented!("not needed for this test, got: {:?}", req)
        }

        async fn read_rest_node_status(&self) -> Result<RestNodeStatus, ClientError> {
            Ok(self.status.clone())
        }
    }

    /// `CachingNodeClient` only overrides the methods it needs to add caching behavior to; every
    /// other `NodeClient` method must be forwarded to the inner client explicitly, since an
    /// un-overridden method resolves to the trait's own default rather than delegating to the
    /// wrapped client. `read_rest_node_status`'s default reports the request as unsupported, so a
    /// missing forward would silently break `eth_syncing` in production.
    #[tokio::test]
    async fn read_rest_node_status_is_forwarded_to_inner_client() {
        let status = RestNodeStatus {
            reactor_state: "KeepUp".to_string(),
            available_block_range: AvailableBlockRange::new(5, 100),
            block_sync: BlockSynchronizerStatus::new(None, None),
        };
        let inner = Arc::new(RestStatusOnlyMock {
            status: status.clone(),
        });
        let under_test = CachingNodeClient::new(inner, None::<Arc<HeedBinaryPortCache>>);

        let got = under_test
            .read_rest_node_status()
            .await
            .expect("should forward to inner client rather than hit the unsupported default");

        assert_eq!(got, status);
    }

    /// `read_block_with_signatures(None)` ("give me the latest block") has no persistent-cache
    /// entry to serve from - the binary port cache is keyed by identifier, not "latest" - so it
    /// must fall through to the node client on every call, never serving a stale answer from an
    /// earlier call.
    #[tokio::test]
    async fn latest_block_always_falls_through_to_node_client() {
        let (_tx, rx) = broadcast::channel(16);
        let rng = &mut TestRng::new();
        let binary_port_mock = Arc::new(BinaryPortMock::new());
        let under_test = Arc::new(CachingNodeClient::new(
            binary_port_mock.clone(),
            None::<Arc<HeedBinaryPortCache>>,
        ));
        let node_client_to_move = under_test.clone();
        tokio::spawn(async move {
            cache_update_loop(node_client_to_move, rx).await.unwrap();
        });

        for _ in 0..2 {
            let block_v2 = TestBlockBuilder::new().build(rng);
            let block = Block::V2(block_v2);
            let signatures = BlockSignatures::random(rng);
            let block_with_signatures = BlockWithSignatures::new(block.clone(), signatures);
            binary_port_mock
                .add_block_with_signatures(
                    block_with_signatures.clone(),
                    InformationRequest::BlockWithSignatures(None),
                )
                .await;
            let got = under_test.read_block_with_signatures(None).await;
            assert_eq!(got, Ok(Some(block_with_signatures)));
        }
        binary_port_mock.verify_no_lingering().await;
    }

    /// Builds a persistent cache + in-flight handler pair backed by the same store, wired up to
    /// `node_client` (mirrors what `new_binary_port_cache` + `CachingNodeClient::new` do in
    /// production).
    fn new_persistent_cache(
        node_client: Arc<BinaryPortMock>,
    ) -> (Arc<HeedBinaryPortCache>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::BinaryPortCacheConfig::test_default(dir.path().to_path_buf());
        let store = crate::binary_port_cache::new_binary_port_cache(&config, node_client).unwrap();
        (store.clone(), dir)
    }

    #[tokio::test]
    async fn binary_port_cache_populates_on_miss_and_serves_on_hit() {
        let rng = &mut TestRng::new();
        let (_tx, rx) = broadcast::channel(16);
        let binary_port_mock = Arc::new(BinaryPortMock::new());
        let (persistent_cache, _dir) = new_persistent_cache(binary_port_mock.clone());
        let under_test = Arc::new(CachingNodeClient::new(
            binary_port_mock.clone(),
            Some(persistent_cache),
        ));
        let node_client_to_move = under_test.clone();
        tokio::spawn(async move {
            cache_update_loop(node_client_to_move, rx).await.unwrap();
        });

        let block_v2 = TestBlockBuilder::new().build(rng);
        let block = Block::V2(block_v2);
        let signatures = BlockSignatures::random(rng);
        let block_with_signatures = BlockWithSignatures::new(block.clone(), signatures);
        let id = casper_types::BlockIdentifier::Height(block.height());
        binary_port_mock
            .add_block_with_signatures(
                block_with_signatures.clone(),
                InformationRequest::BlockWithSignatures(Some(id)),
            )
            .await;

        // first call is a persistent-cache miss, consumes the single mock response
        let got = under_test.read_block_with_signatures(Some(id)).await;
        assert_eq!(got, Ok(Some(block_with_signatures.clone())));

        // second call must be served entirely from the persistent cache - no mock response left
        let got = under_test.read_block_with_signatures(Some(id)).await;
        assert_eq!(got, Ok(Some(block_with_signatures)));
        binary_port_mock.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn binary_port_cache_hit_across_identifier_forms() {
        let rng = &mut TestRng::new();
        let (_tx, rx) = broadcast::channel(16);
        let binary_port_mock = Arc::new(BinaryPortMock::new());
        let (persistent_cache, _dir) = new_persistent_cache(binary_port_mock.clone());
        let under_test = Arc::new(CachingNodeClient::new(
            binary_port_mock.clone(),
            Some(persistent_cache),
        ));
        let node_client_to_move = under_test.clone();
        tokio::spawn(async move {
            cache_update_loop(node_client_to_move, rx).await.unwrap();
        });

        let block_v2 = TestBlockBuilder::new().build(rng);
        let block = Block::V2(block_v2);
        let header = block.clone_header();
        let height_id = casper_types::BlockIdentifier::Height(block.height());
        let hash_id = casper_types::BlockIdentifier::Hash(*block.hash());
        binary_port_mock
            .add_block_header_req_res(
                header.clone(),
                InformationRequest::BlockHeader(Some(height_id)),
            )
            .await;

        let got = under_test.read_block_header(Some(height_id)).await;
        assert_eq!(got, Ok(Some(header.clone())));

        // populated via Height, must also be servable via Hash without hitting the mock again
        let got = under_test.read_block_header(Some(hash_id)).await;
        assert_eq!(got, Ok(Some(header)));
        binary_port_mock.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn no_persistent_cache_falls_through_every_time() {
        let rng = &mut TestRng::new();
        let (_tx, rx) = broadcast::channel(16);
        let binary_port_mock = Arc::new(BinaryPortMock::new());
        let under_test = Arc::new(CachingNodeClient::new(
            binary_port_mock.clone(),
            None::<Arc<HeedBinaryPortCache>>,
        ));
        let node_client_to_move = under_test.clone();
        tokio::spawn(async move {
            cache_update_loop(node_client_to_move, rx).await.unwrap();
        });

        let block_v2 = TestBlockBuilder::new().build(rng);
        let block = Block::V2(block_v2);
        let header = block.clone_header();
        let id = casper_types::BlockIdentifier::Height(block.height());
        for _ in 0..2 {
            binary_port_mock
                .add_block_header_req_res(header.clone(), InformationRequest::BlockHeader(Some(id)))
                .await;
        }

        assert_eq!(
            under_test.read_block_header(Some(id)).await,
            Ok(Some(header.clone()))
        );
        assert_eq!(
            under_test.read_block_header(Some(id)).await,
            Ok(Some(header))
        );
        binary_port_mock.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn transaction_cache_keys_include_finalized_approvals_flag() {
        let rng = &mut TestRng::new();
        let (_tx, rx) = broadcast::channel(16);
        let binary_port_mock = Arc::new(BinaryPortMock::new());
        let (persistent_cache, _dir) = new_persistent_cache(binary_port_mock.clone());
        let under_test = Arc::new(CachingNodeClient::new(
            binary_port_mock.clone(),
            Some(persistent_cache),
        ));
        let node_client_to_move = under_test.clone();
        tokio::spawn(async move {
            cache_update_loop(node_client_to_move, rx).await.unwrap();
        });

        let transaction = casper_types::Transaction::random(rng);
        let hash = transaction.hash();
        let execution_info = casper_types::ExecutionInfo {
            block_hash: casper_types::BlockHash::random(rng),
            block_height: rng.r#gen(),
            execution_result: Some(casper_types::execution::ExecutionResult::random(rng)),
        };
        // `TransactionWithExecutionInfo` isn't `Clone`, so build fresh (but equal) instances for
        // each use from the `Clone`-able `Transaction`/`ExecutionInfo` parts instead.
        let with_info = || {
            casper_binary_port::TransactionWithExecutionInfo::new(
                transaction.clone(),
                Some(execution_info.clone()),
            )
        };

        binary_port_mock
            .add_transaction_with_execution_info_req_res(
                with_info(),
                InformationRequest::Transaction {
                    hash,
                    with_finalized_approvals: true,
                },
            )
            .await;
        binary_port_mock
            .add_transaction_with_execution_info_req_res(
                with_info(),
                InformationRequest::Transaction {
                    hash,
                    with_finalized_approvals: false,
                },
            )
            .await;

        let got_true = under_test
            .read_transaction_with_execution_info(hash, true)
            .await;
        assert_eq!(got_true, Ok(Some(with_info())));
        // different `with_finalized_approvals` value must NOT hit the entry cached above
        let got_false = under_test
            .read_transaction_with_execution_info(hash, false)
            .await;
        assert_eq!(got_false, Ok(Some(with_info())));
        binary_port_mock.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn pending_transaction_is_not_cached() {
        let rng = &mut TestRng::new();
        let (_tx, rx) = broadcast::channel(16);
        let binary_port_mock = Arc::new(BinaryPortMock::new());
        let (persistent_cache, _dir) = new_persistent_cache(binary_port_mock.clone());
        let under_test = Arc::new(CachingNodeClient::new(
            binary_port_mock.clone(),
            Some(persistent_cache),
        ));
        let node_client_to_move = under_test.clone();
        tokio::spawn(async move {
            cache_update_loop(node_client_to_move, rx).await.unwrap();
        });

        let transaction = casper_types::Transaction::random(rng);
        let hash = transaction.hash();
        let pending =
            || casper_binary_port::TransactionWithExecutionInfo::new(transaction.clone(), None);

        for _ in 0..2 {
            binary_port_mock
                .add_transaction_with_execution_info_req_res(
                    pending(),
                    InformationRequest::Transaction {
                        hash,
                        with_finalized_approvals: true,
                    },
                )
                .await;
        }

        // both calls must hit the mock - a pending (execution_info: None) result is never cached
        assert_eq!(
            under_test
                .read_transaction_with_execution_info(hash, true)
                .await,
            Ok(Some(pending()))
        );
        assert_eq!(
            under_test
                .read_transaction_with_execution_info(hash, true)
                .await,
            Ok(Some(pending()))
        );
        binary_port_mock.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn transaction_execution_result_hits_its_own_cache_before_falling_back() {
        let rng = &mut TestRng::new();
        let (tx, rx) = broadcast::channel(16);
        let binary_port_mock = Arc::new(BinaryPortMock::new());
        let (persistent_cache, _dir) = new_persistent_cache(binary_port_mock.clone());
        let under_test = Arc::new(CachingNodeClient::new(
            binary_port_mock.clone(),
            Some(persistent_cache.clone()),
        ));
        let node_client_to_move = under_test.clone();
        tokio::spawn(async move {
            cache_update_loop(node_client_to_move, rx).await.unwrap();
        });

        let transaction = casper_types::Transaction::random(rng);
        let hash = transaction.hash();
        let execution_result = casper_types::execution::ExecutionResult::random(rng);

        // Simulate the SSE event this cache is proactively populated from - no node request is
        // registered on `binary_port_mock` at all, so a hit here proves no fallback fetch
        // happened.
        tx.send(SidecarEvent::TransactionProcessed {
            transaction_hash: hash,
            block_hash: casper_types::BlockHash::random(rng),
            execution_result: Arc::new(execution_result.clone()),
        })
        .unwrap();
        // `cache_update_loop` processes the event on its own spawned task. Poll the cache
        // directly (not through `read_transaction_execution_result`) while waiting for it to
        // land - that method falls back to a node fetch on a miss, which would spuriously panic
        // the mock (empty request queue) while the event is still in flight.
        let mut num_of_tries = 20;
        while matches!(
            persistent_cache
                .get_transaction_execution_result(hash)
                .await
                .unwrap(),
            crate::binary_port_cache::CacheEnvelope::DontHave
        ) {
            num_of_tries -= 1;
            assert!(num_of_tries > 0, "execution result was never cached");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            under_test.read_transaction_execution_result(hash).await,
            Ok(Some(execution_result))
        );
        binary_port_mock.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn transaction_execution_result_falls_back_to_transaction_with_execution_info() {
        let rng = &mut TestRng::new();
        let (_tx, rx) = broadcast::channel(16);
        let binary_port_mock = Arc::new(BinaryPortMock::new());
        let (persistent_cache, _dir) = new_persistent_cache(binary_port_mock.clone());
        let under_test = Arc::new(CachingNodeClient::new(
            binary_port_mock.clone(),
            Some(persistent_cache),
        ));
        let node_client_to_move = under_test.clone();
        tokio::spawn(async move {
            cache_update_loop(node_client_to_move, rx).await.unwrap();
        });

        let transaction = casper_types::Transaction::random(rng);
        let hash = transaction.hash();
        let execution_info = casper_types::ExecutionInfo {
            block_hash: casper_types::BlockHash::random(rng),
            block_height: rng.r#gen(),
            execution_result: Some(casper_types::execution::ExecutionResult::random(rng)),
        };
        let with_info = || {
            casper_binary_port::TransactionWithExecutionInfo::new(
                transaction.clone(),
                Some(execution_info.clone()),
            )
        };

        // No `TransactionProcessed` event was ever observed for this hash - the only way to
        // serve this is via the general-purpose (`with_finalized_approvals = false`) lookup.
        binary_port_mock
            .add_transaction_with_execution_info_req_res(
                with_info(),
                InformationRequest::Transaction {
                    hash,
                    with_finalized_approvals: false,
                },
            )
            .await;

        assert_eq!(
            under_test.read_transaction_execution_result(hash).await,
            Ok(execution_info.execution_result.clone())
        );
        // the fallback result gets backfilled into the execution-result cache, so a second call
        // must not hit the mock again.
        assert_eq!(
            under_test.read_transaction_execution_result(hash).await,
            Ok(execution_info.execution_result)
        );
        binary_port_mock.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn sse_block_with_era_end_populates_validators_cache() {
        let (tx, rx) = broadcast::channel(16);
        let rng = &mut TestRng::new();
        let binary_port_mock = Arc::new(BinaryPortMock::new());
        let (persistent_cache, _dir) = new_persistent_cache(binary_port_mock.clone());
        let under_test = Arc::new(CachingNodeClient::new(
            binary_port_mock.clone(),
            Some(persistent_cache.clone()),
        ));
        let node_client_to_move = under_test.clone();
        tokio::spawn(async move {
            cache_update_loop(node_client_to_move, rx).await.unwrap();
        });

        let era_id = EraId::from(5);
        let block_v2 = TestBlockBuilder::new()
            .switch_block(true)
            .era(era_id)
            .height(50)
            .build(rng);
        let block = Block::V2(block_v2);
        let expected_weights = block
            .clone_era_end()
            .expect("test block should be a switch block")
            .next_era_validator_weights()
            .clone();
        tx.send(SidecarEvent::BlockAdded {
            block: Arc::new(block.clone()),
        })
        .unwrap();

        // the switch block's `next_era_validator_weights` belong to the *following* era
        let next_era_id = era_id.successor();
        let mut num_of_tries = 20;
        loop {
            match persistent_cache.get_validators(next_era_id).await.unwrap() {
                crate::binary_port_cache::CacheEnvelope::Have(data) => {
                    assert_eq!(data.validators, expected_weights);
                    break;
                }
                _ => {
                    num_of_tries -= 1;
                    assert!(
                        num_of_tries > 0,
                        "expected validators to be cached from the SSE-driven block fetch"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }

        binary_port_mock.verify_no_lingering().await;
    }
}
