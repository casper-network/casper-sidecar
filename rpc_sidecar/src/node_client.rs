use crate::{config::ExponentialBackoffConfig, NodeClientConfig, SUPPORTED_PROTOCOL_VERSION};
use anyhow::Error as AnyhowError;
use async_trait::async_trait;
use metrics::rpc::{inc_disconnect, observe_reconnect_time};
use serde::de::DeserializeOwned;
use std::{
    convert::{TryFrom, TryInto},
    future::Future,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use casper_binary_port::{
    BinaryRequest, BinaryRequestHeader, BinaryResponse, BinaryResponseAndRequest,
    ConsensusValidatorChanges, DictionaryItemIdentifier, DictionaryQueryResult, ErrorCode,
    GetRequest, GetTrieFullResult, GlobalStateQueryResult, GlobalStateRequest, InformationRequest,
    NodeStatus, PayloadEntity, RecordId, SpeculativeExecutionResult, TransactionWithExecutionInfo,
};
use casper_types::{
    bytesrepr::{self, FromBytes, ToBytes},
    AvailableBlockRange, BlockHash, BlockHeader, BlockIdentifier, ChainspecRawBytes, Digest,
    GlobalStateIdentifier, Key, KeyTag, Peers, ProtocolVersion, SignedBlock, StoredValue,
    Transaction, TransactionHash, Transfer,
};
use juliet::{
    io::IoCoreBuilder,
    protocol::ProtocolBuilder,
    rpc::{JulietRpcClient, JulietRpcServer, RpcBuilder},
    ChannelConfiguration, ChannelId,
};
use std::{
    fmt::{self, Display, Formatter},
    time::Instant,
};
use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::{Notify, RwLock},
};
use tracing::{error, field, info, warn};

#[async_trait]
pub trait NodeClient: Send + Sync {
    async fn send_request(&self, req: BinaryRequest) -> Result<BinaryResponseAndRequest, Error>;

    async fn read_record(
        &self,
        record_id: RecordId,
        key: &[u8],
    ) -> Result<BinaryResponseAndRequest, Error> {
        let get = GetRequest::Record {
            record_type_tag: record_id.into(),
            key: key.to_vec(),
        };
        self.send_request(BinaryRequest::Get(get)).await
    }

    async fn read_info(&self, req: InformationRequest) -> Result<BinaryResponseAndRequest, Error> {
        let get = req.try_into().expect("should always be able to convert");
        self.send_request(BinaryRequest::Get(get)).await
    }

    async fn query_global_state(
        &self,
        state_identifier: Option<GlobalStateIdentifier>,
        base_key: Key,
        path: Vec<String>,
    ) -> Result<Option<GlobalStateQueryResult>, Error> {
        let req = GlobalStateRequest::Item {
            state_identifier,
            base_key,
            path,
        };
        let resp = self
            .send_request(BinaryRequest::Get(GetRequest::State(req)))
            .await?;
        parse_response::<GlobalStateQueryResult>(&resp.into())
    }

    async fn query_global_state_by_tag(
        &self,
        state_identifier: Option<GlobalStateIdentifier>,
        key_tag: KeyTag,
    ) -> Result<Vec<StoredValue>, Error> {
        let get = GlobalStateRequest::AllItems {
            state_identifier,
            key_tag,
        };
        let resp = self
            .send_request(BinaryRequest::Get(GetRequest::State(get)))
            .await?;
        parse_response::<Vec<StoredValue>>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_trie_bytes(&self, trie_key: Digest) -> Result<Option<Vec<u8>>, Error> {
        let req = GlobalStateRequest::Trie { trie_key };
        let resp = self
            .send_request(BinaryRequest::Get(GetRequest::State(req)))
            .await?;
        let res = parse_response::<GetTrieFullResult>(&resp.into())?.ok_or(Error::EmptyEnvelope)?;
        Ok(res.into_inner().map(<Vec<u8>>::from))
    }

    async fn query_dictionary_item(
        &self,
        state_identifier: Option<GlobalStateIdentifier>,
        identifier: DictionaryItemIdentifier,
    ) -> Result<Option<DictionaryQueryResult>, Error> {
        let get = GlobalStateRequest::DictionaryItem {
            state_identifier,
            identifier,
        };
        let resp = self
            .send_request(BinaryRequest::Get(GetRequest::State(get)))
            .await?;
        parse_response::<DictionaryQueryResult>(&resp.into())
    }

    async fn try_accept_transaction(&self, transaction: Transaction) -> Result<(), Error> {
        let request = BinaryRequest::TryAcceptTransaction { transaction };
        let response = self.send_request(request).await?;

        if response.is_success() {
            return Ok(());
        } else {
            return Err(Error::from_error_code(response.error_code()));
        }
    }

    async fn exec_speculatively(
        &self,
        transaction: Transaction,
    ) -> Result<SpeculativeExecutionResult, Error> {
        let request = BinaryRequest::TrySpeculativeExec { transaction };
        let resp = self.send_request(request).await?;
        parse_response::<SpeculativeExecutionResult>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_block_transfers(&self, hash: BlockHash) -> Result<Option<Vec<Transfer>>, Error> {
        let key = hash.to_bytes().expect("should always serialize a digest");
        let resp = self.read_record(RecordId::Transfer, &key).await?;
        parse_response_bincode::<Vec<Transfer>>(&resp.into())
    }

    async fn read_block_header(
        &self,
        block_identifier: Option<BlockIdentifier>,
    ) -> Result<Option<BlockHeader>, Error> {
        let resp = self
            .read_info(InformationRequest::BlockHeader(block_identifier))
            .await?;
        parse_response::<BlockHeader>(&resp.into())
    }

    async fn read_signed_block(
        &self,
        block_identifier: Option<BlockIdentifier>,
    ) -> Result<Option<SignedBlock>, Error> {
        let resp = self
            .read_info(InformationRequest::SignedBlock(block_identifier))
            .await?;
        parse_response::<SignedBlock>(&resp.into())
    }

    async fn read_transaction_with_execution_info(
        &self,
        hash: TransactionHash,
        with_finalized_approvals: bool,
    ) -> Result<Option<TransactionWithExecutionInfo>, Error> {
        let resp = self
            .read_info(InformationRequest::Transaction {
                hash,
                with_finalized_approvals,
            })
            .await?;
        parse_response::<TransactionWithExecutionInfo>(&resp.into())
    }

    async fn read_peers(&self) -> Result<Peers, Error> {
        let resp = self.read_info(InformationRequest::Peers).await?;
        parse_response::<Peers>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_available_block_range(&self) -> Result<AvailableBlockRange, Error> {
        let resp = self
            .read_info(InformationRequest::AvailableBlockRange)
            .await?;
        parse_response::<AvailableBlockRange>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_chainspec_bytes(&self) -> Result<ChainspecRawBytes, Error> {
        let resp = self
            .read_info(InformationRequest::ChainspecRawBytes)
            .await?;
        parse_response::<ChainspecRawBytes>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_validator_changes(&self) -> Result<ConsensusValidatorChanges, Error> {
        let resp = self
            .read_info(InformationRequest::ConsensusValidatorChanges)
            .await?;
        parse_response::<ConsensusValidatorChanges>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_latest_switch_block_header(&self) -> Result<Option<BlockHeader>, Error> {
        let resp = self
            .read_info(InformationRequest::LatestSwitchBlockHeader)
            .await?;
        parse_response::<BlockHeader>(&resp.into())
    }

    async fn read_node_status(&self) -> Result<NodeStatus, Error> {
        let resp = self.read_info(InformationRequest::NodeStatus).await?;
        parse_response::<NodeStatus>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("request error: {0}")]
    RequestFailed(String),
    #[error("failed to deserialize the envelope of a response: {0}")]
    EnvelopeDeserialization(String),
    #[error("failed to deserialize a response: {0}")]
    Deserialization(String),
    #[error("failed to serialize a request: {0}")]
    Serialization(String),
    #[error("unexpectedly received no response body")]
    NoResponseBody,
    #[error("unexpectedly received an empty envelope")]
    EmptyEnvelope,
    #[error("unexpected payload variant received in the response: {0}")]
    UnexpectedVariantReceived(u8),
    #[error("attempted to use a function that's disabled on the node")]
    FunctionIsDisabled,
    #[error("could not find the provided state root hash")]
    UnknownStateRootHash,
    #[error("the provided global state query failed to execute")]
    QueryFailedToExecute,
    #[error("could not execute the provided transaction")]
    InvalidTransaction,
    #[error("speculative execution has failed: {0}")]
    SpecExecutionFailed(String),
    #[error("received a response with an unsupported protocol version: {0}")]
    UnsupportedProtocolVersion(ProtocolVersion),
    #[error("received an unexpected node error: {message} ({code})")]
    UnexpectedNodeError { message: String, code: u8 },
}

impl Error {
    fn from_error_code(code: u8) -> Self {
        match ErrorCode::try_from(code) {
            Ok(ErrorCode::FunctionDisabled) => Self::FunctionIsDisabled,
            Ok(ErrorCode::InvalidTransaction) => Self::InvalidTransaction,
            Ok(ErrorCode::RootNotFound) => Self::UnknownStateRootHash,
            Ok(ErrorCode::FailedQuery) => Self::QueryFailedToExecute,
            Ok(err @ (ErrorCode::WasmPreprocessing | ErrorCode::InvalidItemVariant)) => {
                Self::SpecExecutionFailed(err.to_string())
            }
            Ok(err) => Self::UnexpectedNodeError {
                message: err.to_string(),
                code,
            },
            Err(err) => Self::UnexpectedNodeError {
                message: err.to_string(),
                code,
            },
        }
    }
}

const CHANNEL_COUNT: usize = 1;

#[derive(Debug)]
pub struct JulietNodeClient {
    client: Arc<RwLock<JulietRpcClient<CHANNEL_COUNT>>>,
    shutdown: Arc<Notify>,
}

impl JulietNodeClient {
    pub async fn new(
        config: NodeClientConfig,
    ) -> Result<(Self, impl Future<Output = Result<(), AnyhowError>>), AnyhowError> {
        let protocol_builder = ProtocolBuilder::<1>::with_default_channel_config(
            ChannelConfiguration::default()
                .with_request_limit(config.request_limit)
                .with_max_request_payload_size(config.max_request_size_bytes)
                .with_max_response_payload_size(config.max_response_size_bytes),
        );
        let io_builder = IoCoreBuilder::new(protocol_builder)
            .buffer_size(ChannelId::new(0), config.request_buffer_size);
        let rpc_builder = RpcBuilder::new(io_builder);

        let stream =
            Self::connect_with_retries(config.address, &config.exponential_backoff).await?;
        let (reader, writer) = stream.into_split();
        let (client, server) = rpc_builder.build(reader, writer);
        let client = Arc::new(RwLock::new(client));
        let shutdown = Arc::new(Notify::new());
        let server_loop = Self::server_loop(
            config.address,
            config.exponential_backoff.clone(),
            rpc_builder,
            Arc::clone(&client),
            server,
            shutdown.clone(),
        );

        Ok((Self { client, shutdown }, server_loop))
    }

    async fn reconnect(
        addr: SocketAddr,
        config: ExponentialBackoffConfig,
        rpc_builder: &RpcBuilder<CHANNEL_COUNT>,
    ) -> Result<
        (
            JulietRpcClient<CHANNEL_COUNT>,
            JulietRpcServer<CHANNEL_COUNT, OwnedReadHalf, OwnedWriteHalf>,
        ),
        AnyhowError,
    > {
        let disconnected_start = Instant::now();
        inc_disconnect();
        error!("node connection closed, will attempt to reconnect");
        let (reader, writer) = Self::connect_with_retries(addr, &config)
            .await?
            .into_split();
        let (new_client, new_server) = rpc_builder.build(reader, writer);

        info!("connection with the node has been re-established");
        observe_reconnect_time(disconnected_start.elapsed());
        Ok((new_client, new_server))
    }

    async fn server_loop(
        addr: SocketAddr,
        config: ExponentialBackoffConfig,
        rpc_builder: RpcBuilder<CHANNEL_COUNT>,
        client: Arc<RwLock<JulietRpcClient<CHANNEL_COUNT>>>,
        mut server: JulietRpcServer<CHANNEL_COUNT, OwnedReadHalf, OwnedWriteHalf>,
        shutdown: Arc<Notify>,
    ) -> Result<(), AnyhowError> {
        loop {
            tokio::select! {
                req = server.next_request() => match req {
                    Err(err) => {
                        warn!(%addr, err=display_error(&err), "binary port client handler error");
                        let (new_client, new_server) = Self::reconnect(addr, config.clone(), &rpc_builder).await?;
                        *client.write().await = new_client;
                        server = new_server;
                    }
                    Ok(None) => {
                        let (new_client, new_server) = Self::reconnect(addr, config.clone(), &rpc_builder).await?;
                        *client.write().await = new_client;
                        server = new_server;
                    }
                    Ok(Some(_)) => {
                        error!("node client received a request from the node, it's going to be ignored")
                    }
                },
                _ = shutdown.notified() => {
                    info!("node client shutdown has been requested");
                    return Ok(())
                }
            }
        }
    }

    async fn connect_with_retries(
        addr: SocketAddr,
        config: &ExponentialBackoffConfig,
    ) -> Result<TcpStream, AnyhowError> {
        let mut wait = config.initial_delay_ms;
        let mut current_attempt = 1;
        loop {
            match TcpStream::connect(addr).await {
                Ok(server) => return Ok(server),
                Err(err) => {
                    warn!(%err, "failed to connect to the node, waiting {wait}ms before retrying");
                    current_attempt += 1;
                    if !config.max_attempts.can_attempt(current_attempt) {
                        anyhow::bail!(
                            "Couldn't connect to node {} after {} attempts",
                            addr,
                            current_attempt - 1
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(wait)).await;
                    wait = (wait * config.coefficient).min(config.max_delay_ms);
                }
            }
        }
    }
}

#[async_trait]
impl NodeClient for JulietNodeClient {
    async fn send_request(&self, req: BinaryRequest) -> Result<BinaryResponseAndRequest, Error> {
        let payload = encode_request(&req).expect("should always serialize a request");
        let request_guard = self
            .client
            .read()
            .await
            .create_request(ChannelId::new(0))
            .with_payload(payload.into())
            .queue_for_sending()
            .await;
        let response = request_guard
            .wait_for_response()
            .await
            .map_err(|err| Error::RequestFailed(err.to_string()))?
            .ok_or(Error::NoResponseBody)?;
        let resp = bytesrepr::deserialize_from_slice(&response)
            .map_err(|err| Error::EnvelopeDeserialization(err.to_string()))?;
        handle_response(resp, &self.shutdown)
    }
}

fn handle_response(
    resp: BinaryResponseAndRequest,
    shutdown: &Notify,
) -> Result<BinaryResponseAndRequest, Error> {
    let version = resp.response().protocol_version();

    if version.is_compatible_with(&SUPPORTED_PROTOCOL_VERSION) {
        Ok(resp)
    } else {
        info!("received a response with incompatible major version from the node {version}, shutting down");
        shutdown.notify_one();
        Err(Error::UnsupportedProtocolVersion(version))
    }
}

fn encode_request(req: &BinaryRequest) -> Result<Vec<u8>, bytesrepr::Error> {
    let header = BinaryRequestHeader::new(SUPPORTED_PROTOCOL_VERSION, req.tag());
    let mut bytes = Vec::with_capacity(header.serialized_length() + req.serialized_length());
    header.write_bytes(&mut bytes)?;
    req.write_bytes(&mut bytes)?;
    Ok(bytes)
}

fn parse_response<A>(resp: &BinaryResponse) -> Result<Option<A>, Error>
where
    A: FromBytes + PayloadEntity,
{
    if resp.is_not_found() {
        return Ok(None);
    }
    if !resp.is_success() {
        return Err(Error::from_error_code(resp.error_code()));
    }
    match resp.returned_data_type_tag() {
        Some(found) if found == u8::from(A::PAYLOAD_TYPE) => {
            bytesrepr::deserialize_from_slice(resp.payload())
                .map(Some)
                .map_err(|err| Error::Deserialization(err.to_string()))
        }
        Some(other) => Err(Error::UnexpectedVariantReceived(other)),
        _ => Ok(None),
    }
}

fn parse_response_bincode<A>(resp: &BinaryResponse) -> Result<Option<A>, Error>
where
    A: DeserializeOwned + PayloadEntity,
{
    if resp.is_not_found() {
        return Ok(None);
    }
    if !resp.is_success() {
        return Err(Error::from_error_code(resp.error_code()));
    }
    match resp.returned_data_type_tag() {
        Some(found) if found == u8::from(A::PAYLOAD_TYPE) => bincode::deserialize(resp.payload())
            .map(Some)
            .map_err(|err| Error::Deserialization(err.to_string())),
        Some(other) => Err(Error::UnexpectedVariantReceived(other)),
        _ => Ok(None),
    }
}

/// Wraps an error to ensure it gets properly captured by tracing.
pub(crate) fn display_error<'a, T>(err: &'a T) -> field::DisplayValue<ErrFormatter<'a, T>>
where
    T: std::error::Error + 'a,
{
    field::display(ErrFormatter(err))
}

/// An error formatter.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ErrFormatter<'a, T>(pub &'a T);

impl<'a, T> Display for ErrFormatter<'a, T>
where
    T: std::error::Error,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut opt_source: Option<&(dyn std::error::Error)> = Some(self.0);

        while let Some(source) = opt_source {
            write!(f, "{}", source)?;
            opt_source = source.source();

            if opt_source.is_some() {
                f.write_str(": ")?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::testing::{get_port, start_mock_binary_port_responding_with_stored_value};

    use super::*;
    use casper_types::testing::TestRng;
    use casper_types::{CLValue, SemVer};
    use futures::FutureExt;
    use tokio::time::sleep;

    #[tokio::test]
    async fn should_reject_bad_major_version() {
        let notify = Notify::new();
        let bad_version = ProtocolVersion::from_parts(10, 0, 0);

        let result = handle_response(
            BinaryResponseAndRequest::new(
                BinaryResponse::from_value(AvailableBlockRange::RANGE_0_0, bad_version),
                &[],
            ),
            &notify,
        );

        assert_eq!(result, Err(Error::UnsupportedProtocolVersion(bad_version)));
        assert_eq!(notify.notified().now_or_never(), Some(()))
    }

    #[tokio::test]
    async fn should_accept_different_minor_version() {
        let notify = Notify::new();
        let version = ProtocolVersion::new(SemVer {
            minor: SUPPORTED_PROTOCOL_VERSION.value().minor + 1,
            ..SUPPORTED_PROTOCOL_VERSION.value()
        });

        let result = handle_response(
            BinaryResponseAndRequest::new(
                BinaryResponse::from_value(AvailableBlockRange::RANGE_0_0, version),
                &[],
            ),
            &notify,
        );

        assert_eq!(
            result,
            Ok(BinaryResponseAndRequest::new(
                BinaryResponse::from_value(AvailableBlockRange::RANGE_0_0, version),
                &[],
            ))
        );
        assert_eq!(notify.notified().now_or_never(), None)
    }

    #[tokio::test]
    async fn should_accept_different_patch_version() {
        let notify = Notify::new();
        let version = ProtocolVersion::new(SemVer {
            patch: SUPPORTED_PROTOCOL_VERSION.value().patch + 1,
            ..SUPPORTED_PROTOCOL_VERSION.value()
        });

        let result = handle_response(
            BinaryResponseAndRequest::new(
                BinaryResponse::from_value(AvailableBlockRange::RANGE_0_0, version),
                &[],
            ),
            &notify,
        );

        assert_eq!(
            result,
            Ok(BinaryResponseAndRequest::new(
                BinaryResponse::from_value(AvailableBlockRange::RANGE_0_0, version),
                &[],
            ))
        );
        assert_eq!(notify.notified().now_or_never(), None)
    }

    #[tokio::test]
    async fn given_client_and_no_node_should_fail_after_tries() {
        let config = NodeClientConfig::finite_retries_config(1111, 2);
        let res = JulietNodeClient::new(config).await;

        assert!(res.is_err());
        let error_message = res.err().unwrap().to_string();

        assert!(error_message.starts_with("Couldn't connect to node"));
        assert!(error_message.ends_with(" after 2 attempts"));
    }

    #[tokio::test]
    async fn given_client_and_node_should_connect_and_do_request() {
        let port = get_port();
        let mut rng = TestRng::new();
        let _mock_server_handle = start_mock_binary_port_responding_with_stored_value(port).await;
        let config = NodeClientConfig::finite_retries_config(port, 2);
        let (c, server_loop) = JulietNodeClient::new(config).await.unwrap();
        tokio::spawn(async move {
            server_loop.await.unwrap();
        });

        let res = query_global_state_for_string_value(&mut rng, &c)
            .await
            .unwrap();

        assert_eq!(res, StoredValue::CLValue(CLValue::from_t("Foo").unwrap()))
    }

    #[tokio::test]
    async fn given_client_should_try_until_node_starts() {
        let mut rng = TestRng::new();
        let port = get_port();
        tokio::spawn(async move {
            sleep(Duration::from_secs(5)).await;
            let _mock_server_handle =
                start_mock_binary_port_responding_with_stored_value(port).await;
        });
        let config = NodeClientConfig::finite_retries_config(port, 5);
        let (client, server_loop) = JulietNodeClient::new(config).await.unwrap();
        tokio::spawn(async move {
            server_loop.await.unwrap();
        });

        let res = query_global_state_for_string_value(&mut rng, &client)
            .await
            .unwrap();

        assert_eq!(res, StoredValue::CLValue(CLValue::from_t("Foo").unwrap()))
    }

    async fn query_global_state_for_string_value(
        rng: &mut TestRng,
        client: &JulietNodeClient,
    ) -> Result<StoredValue, Error> {
        let state_root_hash = Digest::random(rng);
        let base_key = Key::ChecksumRegistry;
        client
            .query_global_state(
                Some(GlobalStateIdentifier::StateRootHash(state_root_hash)),
                base_key,
                vec![],
            )
            .await?
            .ok_or(Error::NoResponseBody)
            .map(|query_res| query_res.into_inner().0)
    }
}
