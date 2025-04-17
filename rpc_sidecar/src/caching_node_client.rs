use crate::{ClientError, NodeClient, parse_response};
use anyhow::Error;
use async_trait::async_trait;
use casper_binary_port::{BinaryResponseAndRequest, Command, InformationRequest};
use casper_event_types::SidecarEvent;
use casper_types::{BlockIdentifier, BlockWithSignatures};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{RwLock, broadcast::Receiver},
    time::timeout,
};
use tracing::info;

const CACHE_FETCH_TIMEOUT: Duration = Duration::from_millis(5);

pub struct CachingNodeClient<T: NodeClient + Send + Sync> {
    inner_client: Arc<T>,
    block_with_signatures_cache: Arc<RwLock<Option<BlockWithSignatures>>>,
}

impl<T: NodeClient + Send + Sync> CachingNodeClient<T> {
    pub(crate) fn new(inner_client: Arc<T>) -> Self {
        let block_with_signatures_cache = Arc::new(RwLock::new(None));
        Self {
            inner_client,
            block_with_signatures_cache,
        }
    }

    async fn get_block_from_cache(&self) -> Option<BlockWithSignatures> {
        match timeout(CACHE_FETCH_TIMEOUT, self.block_with_signatures_cache.read()).await {
            Ok(maybe_block) => maybe_block.clone(),
            Err(_) => None,
        }
    }

    #[cfg(test)]
    async fn inner_cached_block(&self) -> Option<BlockWithSignatures> {
        let guard = self.block_with_signatures_cache.read().await;
        (*guard).clone()
    }
}

#[async_trait]
impl<T: NodeClient + Send + Sync> NodeClient for CachingNodeClient<T> {
    async fn send_request(&self, req: Command) -> Result<BinaryResponseAndRequest, ClientError> {
        self.inner_client.send_request(req).await
    }

    async fn read_block_with_signatures(
        &self,
        block_identifier: Option<BlockIdentifier>,
    ) -> Result<Option<BlockWithSignatures>, ClientError> {
        if block_identifier.is_none() {
            if let Some(block) = self.get_block_from_cache().await {
                return Ok(Some(block));
            }
        }
        let resp = self
            .read_info(InformationRequest::BlockWithSignatures(block_identifier))
            .await?;
        parse_response::<BlockWithSignatures>(&resp.into())
    }
}

pub(crate) async fn cache_update_loop<T: NodeClient + Send + Sync>(
    client: Arc<CachingNodeClient<T>>,
    mut sidecar_event_receiver: Receiver<SidecarEvent>,
) -> Result<(), Error> {
    loop {
        match sidecar_event_receiver.recv().await {
            Ok(msg) => match msg {
                SidecarEvent::BlockAdded { height, .. } => {
                    let guard = client.block_with_signatures_cache.read().await;
                    if let Some(block) = guard.as_ref() {
                        let known_height = block.block().height();
                        if height <= known_height {
                            //We know of a heigher block than the sse is reporting to us.
                            // This may happen if the node that we listen to is still catching up.
                            // In this case we just skip this one.
                            continue;
                        }
                    }
                    drop(guard);
                    let block_identifier = Some(BlockIdentifier::Height(height));
                    let res = client
                        .inner_client
                        .read_block_with_signatures(block_identifier)
                        .await;

                    let mut guard = client.block_with_signatures_cache.write().await;
                    match res {
                        Ok(Some(block)) => {
                            let height = block.block().height();
                            if let Some(block) = guard.as_ref() {
                                let known_height = block.block().height();
                                if height <= known_height {
                                    //It seems that for some reason we got an older block than we already have, let's just skip it
                                    continue;
                                }
                            }
                            *guard = Some(block);
                        }
                        Ok(None) => {
                            //This should generally NOT happen since this loops reacts to BlockAdded events. But for completeness we are handling this.
                            // To be sure we set the cache to None so we will fall through to underlying client
                            *guard = None;
                        }
                        Err(_) => {
                            // We can't trust the cache since we might have fallen behind, resetting it to None so we will fall through to underlying client
                            *guard = None;
                        }
                    }
                }
            },
            Err(x) => match x {
                tokio::sync::broadcast::error::RecvError::Closed => {
                    let mut guard = client.block_with_signatures_cache.write().await;
                    *guard = None;
                    anyhow::bail!(
                        "In cache_update_loop: internal broadcast mechanism of sidecar events failed."
                    );
                }
                tokio::sync::broadcast::error::RecvError::Lagged(_) => {
                    let mut guard = client.block_with_signatures_cache.write().await;
                    *guard = None;
                    info!("lag detected in cache_update_loop");
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CachingNodeClient, cache_update_loop};
    use crate::{NodeClient, rpcs::test_utils::BinaryPortMock};
    use casper_binary_port::InformationRequest;
    use casper_event_types::SidecarEvent;
    use casper_types::{
        Block, BlockSignatures, BlockWithSignatures, TestBlockBuilder, testing::TestRng,
    };
    use std::{sync::Arc, time::Duration};
    use tokio::sync::broadcast;

    #[tokio::test]
    async fn if_no_block_was_cached_should_fetch_directly() {
        let (_tx, rx) = broadcast::channel(16);
        let rng = &mut TestRng::new();
        let binary_port_mock = Arc::new(BinaryPortMock::new());
        let under_test = Arc::new(CachingNodeClient::new(binary_port_mock.clone()));
        let node_client_to_move = under_test.clone();
        tokio::spawn(async move {
            cache_update_loop(node_client_to_move, rx).await.unwrap();
        });
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
        //Give some time for inner messages propagation
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(under_test.inner_cached_block().await, None);
        let got = under_test.read_block_with_signatures(None).await;
        assert_eq!(got, Ok(Some(block_with_signatures)));
        binary_port_mock.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn incoming_sidecar_event_should_trigger_block_fetch() {
        let (tx, rx) = broadcast::channel(16);
        let rng = &mut TestRng::new();
        let binary_port_mock = Arc::new(BinaryPortMock::new());
        let under_test = Arc::new(CachingNodeClient::new(binary_port_mock.clone()));
        let node_client_to_move = under_test.clone();
        tokio::spawn(async move {
            cache_update_loop(node_client_to_move, rx).await.unwrap();
        });
        let block_v2 = TestBlockBuilder::new().build(rng);
        let block = Block::V2(block_v2);
        let signatures = BlockSignatures::random(rng);
        let block_with_signatures = BlockWithSignatures::new(block.clone(), signatures);
        binary_port_mock
            .add_block_with_signatures(
                block_with_signatures.clone(),
                InformationRequest::BlockWithSignatures(Some(
                    casper_types::BlockIdentifier::Height(block.height()),
                )),
            )
            .await;
        tx.send(SidecarEvent::BlockAdded {
            block_hash: *block.hash(),
            height: block.height(),
        })
        .unwrap();
        wait_for_inner_block(under_test.clone()).await;
        assert_eq!(
            under_test.inner_cached_block().await,
            Some(block_with_signatures)
        );

        binary_port_mock.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn none_cache_forces_real_fetch() {
        let (tx, rx) = broadcast::channel(16);
        let rng = &mut TestRng::new();
        let binary_port_mock = Arc::new(BinaryPortMock::new());
        let under_test = Arc::new(CachingNodeClient::new(binary_port_mock.clone()));
        let node_client_to_move = under_test.clone();
        tokio::spawn(async move {
            cache_update_loop(node_client_to_move, rx).await.unwrap();
        });
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
        drop(tx);
        //Give some time for inner messages propagation
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(under_test.inner_cached_block().await, None);
        let got = under_test.read_block_with_signatures(None).await;
        assert_eq!(got, Ok(Some(block_with_signatures)));
        binary_port_mock.verify_no_lingering().await;
    }

    async fn wait_for_inner_block(client: Arc<CachingNodeClient<BinaryPortMock>>) {
        let mut num_of_tries = 5;

        while num_of_tries > 0 {
            num_of_tries -= 1;
            if client.inner_cached_block().await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
