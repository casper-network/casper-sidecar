use std::{convert::TryFrom, future::Future, net::SocketAddr, sync::Arc, time::Duration};

use async_trait::async_trait;
use serde::de::DeserializeOwned;

use crate::{config::ExponentialBackoffConfig, NodeClientConfig};
use casper_types_ver_2_0::{
    binary_port::{
        binary_request::{BinaryRequest, BinaryRequestHeader},
        db_id::DbId,
        get::GetRequest,
        global_state_query_result::GlobalStateQueryResult,
        non_persistent_data_request::NonPersistedDataRequest,
        payload_type::PayloadEntity,
        type_wrappers::{
            ConsensusValidatorChanges, GetTrieFullResult, HighestBlockSequenceCheckResult,
            SpeculativeExecutionResult, StoredValues,
        },
        ErrorCode as BinaryPortError, NodeStatus, BINARY_PROTOCOL_VERSION,
    },
    bytesrepr::{self, FromBytes, ToBytes},
    execution::{ExecutionResult, ExecutionResultV1},
    AvailableBlockRange, BinaryResponse, BinaryResponseAndRequest, BlockBody, BlockBodyV1,
    BlockHash, BlockHashAndHeight, BlockHeader, BlockHeaderV1, BlockIdentifier, BlockSignatures,
    ChainspecRawBytes, Deploy, Digest, FinalizedApprovals, FinalizedDeployApprovals, Key, KeyTag,
    Peers, ProtocolVersion, SemVer, Timestamp, Transaction, TransactionHash, Transfer,
};
use juliet::{
    io::IoCoreBuilder,
    protocol::ProtocolBuilder,
    rpc::{JulietRpcClient, JulietRpcServer, RpcBuilder},
    ChannelConfiguration, ChannelId,
};
use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::RwLock,
};
use tracing::{error, info, warn};

#[async_trait]
pub trait NodeClient: Send + Sync {
    async fn send_request(&self, req: BinaryRequest) -> Result<BinaryResponseAndRequest, Error>;

    async fn read_from_db(&self, db: DbId, key: &[u8]) -> Result<BinaryResponseAndRequest, Error> {
        let get = GetRequest::Db {
            db_tag: db as u8,
            key: key.to_vec(),
        };
        self.send_request(BinaryRequest::Get(get)).await
    }

    async fn read_from_mem(
        &self,
        req: NonPersistedDataRequest,
    ) -> Result<BinaryResponseAndRequest, Error> {
        let get = GetRequest::NonPersistedData(req);
        self.send_request(BinaryRequest::Get(get)).await
    }

    async fn read_trie_bytes(&self, trie_key: Digest) -> Result<Option<Vec<u8>>, Error> {
        let get = GetRequest::Trie { trie_key };
        let resp = self.send_request(BinaryRequest::Get(get)).await?;
        let res = parse_response::<GetTrieFullResult>(&resp.into())?.ok_or(Error::EmptyEnvelope)?;
        Ok(res.into_inner().map(<Vec<u8>>::from))
    }

    async fn query_global_state(
        &self,
        state_root_hash: Digest,
        base_key: Key,
        path: Vec<String>,
    ) -> Result<Option<GlobalStateQueryResult>, Error> {
        let get = GetRequest::State {
            state_root_hash,
            base_key,
            path,
        };
        let resp = self.send_request(BinaryRequest::Get(get)).await?;
        parse_response::<GlobalStateQueryResult>(&resp.into())
    }

    async fn query_global_state_by_tag(
        &self,
        state_root_hash: Digest,
        tag: KeyTag,
    ) -> Result<StoredValues, Error> {
        let get = GetRequest::AllValues {
            state_root_hash,
            key_tag: tag,
        };
        let resp = self.send_request(BinaryRequest::Get(get)).await?;
        parse_response::<StoredValues>(&resp.into())?.ok_or(Error::EmptyEnvelope)
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
        state_root_hash: Digest,
        block_time: Timestamp,
        protocol_version: ProtocolVersion,
        transaction: Transaction,
        exec_at_block: BlockHeader,
    ) -> Result<SpeculativeExecutionResult, Error> {
        let request = BinaryRequest::TrySpeculativeExec {
            transaction,
            state_root_hash,
            block_time,
            protocol_version,
            speculative_exec_at_block: exec_at_block,
        };
        let resp = self.send_request(request).await?;
        parse_response::<SpeculativeExecutionResult>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_transaction(&self, hash: TransactionHash) -> Result<Option<Transaction>, Error> {
        let key = hash.to_bytes().expect("should always serialize a digest");
        let resp = self.read_from_db(DbId::Transaction, &key).await?;
        parse_response_versioned::<Deploy, Transaction>(&resp.into())
    }

    async fn read_finalized_approvals(
        &self,
        hash: TransactionHash,
    ) -> Result<Option<FinalizedApprovals>, Error> {
        let key = hash.to_bytes().expect("should always serialize a digest");
        let resp = self
            .read_from_db(DbId::FinalizedTransactionApprovals, &key)
            .await?;
        parse_response_versioned::<FinalizedDeployApprovals, FinalizedApprovals>(&resp.into())
    }

    async fn read_block_header(&self, hash: BlockHash) -> Result<Option<BlockHeader>, Error> {
        let key = hash.to_bytes().expect("should always serialize a digest");
        let resp = self.read_from_db(DbId::BlockHeader, &key).await?;
        parse_response_versioned::<BlockHeaderV1, BlockHeader>(&resp.into())
    }

    async fn read_block_body(&self, hash: Digest) -> Result<Option<BlockBody>, Error> {
        let key = hash.to_bytes().expect("should always serialize a digest");
        let resp = self.read_from_db(DbId::BlockBody, &key).await?;
        parse_response_versioned::<BlockBodyV1, BlockBody>(&resp.into())
    }

    async fn read_block_signatures(
        &self,
        hash: BlockHash,
    ) -> Result<Option<BlockSignatures>, Error> {
        let key = hash.to_bytes().expect("should always serialize a digest");
        let resp = self.read_from_db(DbId::BlockMetadata, &key).await?;
        parse_response_bincode::<BlockSignatures>(&resp.into())
    }

    async fn read_block_transfers(&self, hash: BlockHash) -> Result<Option<Vec<Transfer>>, Error> {
        let key = hash.to_bytes().expect("should always serialize a digest");
        let resp = self.read_from_db(DbId::Transfer, &key).await?;
        parse_response_bincode::<Vec<Transfer>>(&resp.into())
    }

    async fn read_execution_result(
        &self,
        hash: TransactionHash,
    ) -> Result<Option<ExecutionResult>, Error> {
        let key = hash.to_bytes().expect("should always serialize a digest");
        let resp = self.read_from_db(DbId::ExecutionResult, &key).await?;
        parse_response_versioned::<ExecutionResultV1, ExecutionResult>(&resp.into())
    }

    async fn read_transaction_block_info(
        &self,
        transaction_hash: TransactionHash,
    ) -> Result<Option<BlockHashAndHeight>, Error> {
        let req = NonPersistedDataRequest::TransactionHash2BlockHashAndHeight { transaction_hash };
        let resp = self.read_from_mem(req).await?;
        parse_response::<BlockHashAndHeight>(&resp.into())
    }

    async fn read_highest_completed_block_info(&self) -> Result<Option<BlockHashAndHeight>, Error> {
        let resp = self
            .read_from_mem(NonPersistedDataRequest::HighestCompleteBlock)
            .await?;
        parse_response::<BlockHashAndHeight>(&resp.into())
    }

    async fn read_block_hash_from_height(&self, height: u64) -> Result<Option<BlockHash>, Error> {
        let req = NonPersistedDataRequest::BlockHeight2Hash { height };
        let resp = self.read_from_mem(req).await?;
        parse_response::<BlockHash>(&resp.into())
    }

    async fn does_exist_in_completed_blocks(&self, block_hash: BlockHash) -> Result<bool, Error> {
        let block_identifier = BlockIdentifier::Hash(block_hash);
        let req = NonPersistedDataRequest::CompletedBlocksContain { block_identifier };
        let resp = self.read_from_mem(req).await?;
        parse_response::<HighestBlockSequenceCheckResult>(&resp.into())?
            .map(bool::from)
            .ok_or(Error::EmptyEnvelope)
    }

    async fn read_peers(&self) -> Result<Peers, Error> {
        let resp = self.read_from_mem(NonPersistedDataRequest::Peers).await?;
        parse_response::<Peers>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_available_block_range(&self) -> Result<AvailableBlockRange, Error> {
        let resp = self
            .read_from_mem(NonPersistedDataRequest::AvailableBlockRange)
            .await?;
        parse_response::<AvailableBlockRange>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_chainspec_bytes(&self) -> Result<ChainspecRawBytes, Error> {
        let resp = self
            .read_from_mem(NonPersistedDataRequest::ChainspecRawBytes)
            .await?;
        parse_response::<ChainspecRawBytes>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_validator_changes(&self) -> Result<ConsensusValidatorChanges, Error> {
        let resp = self
            .read_from_mem(NonPersistedDataRequest::ConsensusValidatorChanges)
            .await?;
        parse_response::<ConsensusValidatorChanges>(&resp.into())?.ok_or(Error::EmptyEnvelope)
    }

    async fn read_node_status(&self) -> Result<NodeStatus, Error> {
        let resp = self
            .read_from_mem(NonPersistedDataRequest::NodeStatus)
            .await?;
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
    #[error("could not execute the provided deploy")]
    InvalidDeploy,
    #[error("speculative execution has failed: {0}")]
    SpecExecutionFailed(String),
    #[error("received a response with an unsupported protocol version: {0}")]
    UnsupportedProtocolVersion(SemVer),
    #[error("received an unexpected node error: {message} ({code})")]
    UnexpectedNodeError { message: String, code: u8 },
}

impl Error {
    fn from_error_code(code: u8) -> Self {
        match BinaryPortError::try_from(code) {
            Ok(BinaryPortError::FunctionDisabled) => Self::FunctionIsDisabled,
            Ok(BinaryPortError::InvalidDeploy) => Self::InvalidDeploy,
            Ok(BinaryPortError::RootNotFound) => Self::UnknownStateRootHash,
            Ok(BinaryPortError::QueryFailedToExecute) => Self::QueryFailedToExecute,
            Ok(
                err @ (BinaryPortError::WasmPreprocessing
                | BinaryPortError::InvalidDeployItemVariant),
            ) => Self::SpecExecutionFailed(err.to_string()),
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
}

impl JulietNodeClient {
    pub async fn new(config: &NodeClientConfig) -> (Self, impl Future<Output = ()> + '_) {
        let protocol_builder = ProtocolBuilder::<1>::with_default_channel_config(
            ChannelConfiguration::default()
                .with_request_limit(config.request_limit)
                .with_max_request_payload_size(config.max_request_size_bytes)
                .with_max_response_payload_size(config.max_response_size_bytes),
        );
        let io_builder = IoCoreBuilder::new(protocol_builder)
            .buffer_size(ChannelId::new(0), config.request_buffer_size);
        let rpc_builder = RpcBuilder::new(io_builder);

        let stream = Self::connect_with_retries(config.address, &config.exponential_backoff).await;
        let (reader, writer) = stream.into_split();
        let (client, server) = rpc_builder.build(reader, writer);
        let client = Arc::new(RwLock::new(client));
        let server_loop = Self::server_loop(
            config.address,
            &config.exponential_backoff,
            rpc_builder,
            Arc::clone(&client),
            server,
        );

        (Self { client }, server_loop)
    }

    async fn server_loop(
        addr: SocketAddr,
        config: &ExponentialBackoffConfig,
        rpc_builder: RpcBuilder<CHANNEL_COUNT>,
        client: Arc<RwLock<JulietRpcClient<CHANNEL_COUNT>>>,
        mut server: JulietRpcServer<CHANNEL_COUNT, OwnedReadHalf, OwnedWriteHalf>,
    ) {
        loop {
            match server.next_request().await {
                Ok(None) | Err(_) => {
                    error!("node connection closed, will attempt to reconnect");
                    let (reader, writer) =
                        Self::connect_with_retries(addr, config).await.into_split();
                    let (new_client, new_server) = rpc_builder.build(reader, writer);

                    info!("connection with the node has been re-established");
                    *client.write().await = new_client;
                    server = new_server;
                }
                Ok(Some(_)) => {
                    error!("node client received a request from the node, it's going to be ignored")
                }
            }
        }
    }

    async fn connect_with_retries(
        addr: SocketAddr,
        config: &ExponentialBackoffConfig,
    ) -> TcpStream {
        let mut wait = config.initial_delay_ms;
        loop {
            match TcpStream::connect(addr).await {
                Ok(server) => break server,
                Err(err) => {
                    warn!(%err, "failed to connect to the node, waiting {wait}ms before retrying");
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
        bytesrepr::deserialize_from_slice(&response)
            .map_err(|err| Error::EnvelopeDeserialization(err.to_string()))
    }
}

fn encode_request(req: &BinaryRequest) -> Result<Vec<u8>, bytesrepr::Error> {
    let header = BinaryRequestHeader::new(BINARY_PROTOCOL_VERSION);
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
    validate_response(resp)?;
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

fn parse_response_versioned<V1, V2>(resp: &BinaryResponse) -> Result<Option<V2>, Error>
where
    V1: DeserializeOwned + PayloadEntity,
    V2: FromBytes + PayloadEntity + From<V1>,
{
    if resp.is_not_found() {
        return Ok(None);
    }
    validate_response(resp)?;
    match resp.returned_data_type_tag() {
        Some(found) if found == u8::from(V1::PAYLOAD_TYPE) => bincode::deserialize(resp.payload())
            .map(|val| Some(V2::from(val)))
            .map_err(|err| Error::Deserialization(err.to_string())),
        Some(found) if found == u8::from(V2::PAYLOAD_TYPE) => {
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
    validate_response(resp)?;
    match resp.returned_data_type_tag() {
        Some(found) if found == u8::from(A::PAYLOAD_TYPE) => bincode::deserialize(resp.payload())
            .map(Some)
            .map_err(|err| Error::Deserialization(err.to_string())),
        Some(other) => Err(Error::UnexpectedVariantReceived(other)),
        _ => Ok(None),
    }
}

fn validate_response(resp: &BinaryResponse) -> Result<(), Error> {
    if !resp
        .protocol_version()
        .is_major_compatible(BINARY_PROTOCOL_VERSION)
    {
        return Err(Error::UnsupportedProtocolVersion(resp.protocol_version()));
    }
    if !resp.is_success() {
        return Err(Error::from_error_code(resp.error_code()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn should_reject_bad_major_version() {
        struct ClientMock;

        #[async_trait]
        impl NodeClient for ClientMock {
            async fn send_request(
                &self,
                _req: BinaryRequest,
            ) -> Result<BinaryResponseAndRequest, Error> {
                Ok(BinaryResponseAndRequest::new(
                    BinaryResponse::from_value_with_protocol_version(
                        AvailableBlockRange::RANGE_0_0,
                        SemVer::new(10, 0, 0),
                    ),
                    &[],
                ))
            }
        }

        let result = ClientMock.read_available_block_range().await;
        assert_eq!(
            result,
            Err(Error::UnsupportedProtocolVersion(SemVer::new(10, 0, 0)))
        );
    }

    #[tokio::test]
    async fn should_accept_different_minor_version() {
        struct ClientMock;

        #[async_trait]
        impl NodeClient for ClientMock {
            async fn send_request(
                &self,
                _req: BinaryRequest,
            ) -> Result<BinaryResponseAndRequest, Error> {
                Ok(BinaryResponseAndRequest::new(
                    BinaryResponse::from_value_with_protocol_version(
                        AvailableBlockRange::RANGE_0_0,
                        SemVer {
                            minor: BINARY_PROTOCOL_VERSION.minor + 1,
                            ..BINARY_PROTOCOL_VERSION
                        },
                    ),
                    &[],
                ))
            }
        }

        let result = ClientMock.read_available_block_range().await;
        assert_eq!(result, Ok(AvailableBlockRange::RANGE_0_0));
    }

    #[tokio::test]
    async fn should_accept_different_patch_version() {
        struct ClientMock;

        #[async_trait]
        impl NodeClient for ClientMock {
            async fn send_request(
                &self,
                _req: BinaryRequest,
            ) -> Result<BinaryResponseAndRequest, Error> {
                Ok(BinaryResponseAndRequest::new(
                    BinaryResponse::from_value_with_protocol_version(
                        AvailableBlockRange::RANGE_0_0,
                        SemVer {
                            patch: BINARY_PROTOCOL_VERSION.patch + 1,
                            ..BINARY_PROTOCOL_VERSION
                        },
                    ),
                    &[],
                ))
            }
        }

        let result = ClientMock.read_available_block_range().await;
        assert_eq!(result, Ok(AvailableBlockRange::RANGE_0_0));
    }
}
