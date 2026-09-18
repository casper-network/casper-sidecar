mod heed_store;
pub(crate) use heed_store::HeedBinaryPortCache;

use async_trait::async_trait;
use casper_event_types::SidecarEvent;
use casper_types::{
    Block, BlockHash, BlockHeader, BlockIdentifier, BlockWithSignatures, EraId, ExecutionInfo,
    FinalitySignature, PublicKey, Transaction, TransactionHash, U512,
    bytesrepr::{self, Bytes, FromBytes, ToBytes, U8_SERIALIZED_LENGTH, U32_SERIALIZED_LENGTH},
};
use datasize::DataSize;
use serde::Deserialize;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use thiserror::Error;

use crate::{ClientError, NodeClient};

/// Default upper bound on the total on-disk size of the binary port cache's LMDB environment.
const DEFAULT_BINARY_PORT_CACHE_MAX_SIZE_BYTES: usize = 512 * 1024 * 1024;

fn default_binary_port_cache_max_size_bytes() -> usize {
    DEFAULT_BINARY_PORT_CACHE_MAX_SIZE_BYTES
}

/// enum representing th knowledge of sidecar about a block. The caching mechanism can "find out" about a block
/// via either fetching BlockWithSignatures from the node directly, but it can also assembly the data from the SSE events
/// it listens to.
#[derive(Clone)]
pub(crate) enum BlockWithSignaturesBuiltInFlight {
    /// Variant responsible for holding data about
    /// a block for which either there are not enough
    /// finality signatures known to sidecar OR sidecar
    /// doesn't have enough knowledge about the eras
    /// validators to determine if the signatures have enough weight.
    /// This variant esentially should be treated as a "None" because we can't serve incomplete data.
    /// Only once finality signatures are good enough we can change this variant to BlockWithSignaturesBuiltInFlight::BlockWithSignatures which can then be served to the public.
    /// This variant should not be ever used if sidecar is caching response from the node - only when it's accumulating knowledge from SSE.
    /// Please note that it's not sidecars role to verify any of the signatures - we only need to know when we have enough signatures that we can consider the cache entry saturated
    NotSureBlock {
        block: Option<Block>,
        signatures: Vec<FinalitySignature>,
    },
    /// Complete info about a block obtained either by storing output from nodes binary port response OR accumulated by listening to SSE events and confronting them with era validators knowledge
    BlockWithSignatures(BlockWithSignatures),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ValidatorsData {
    pub(crate) validators: BTreeMap<PublicKey, U512>,
    pub(crate) total_stake: U512,
}

impl ValidatorsData {
    pub(crate) fn new(validators: BTreeMap<PublicKey, U512>) -> Self {
        let total_stake = validators
            .values()
            .fold(U512::zero(), |acc, weight| acc + *weight);
        Self {
            validators,
            total_stake,
        }
    }
}

impl ToBytes for ValidatorsData {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        let mut buffer = bytesrepr::allocate_buffer(self)?;
        self.write_bytes(&mut buffer)?;
        Ok(buffer)
    }

    fn serialized_length(&self) -> usize {
        self.validators.serialized_length() + self.total_stake.serialized_length()
    }

    fn write_bytes(&self, writer: &mut Vec<u8>) -> Result<(), bytesrepr::Error> {
        self.validators.write_bytes(writer)?;
        self.total_stake.write_bytes(writer)?;
        Ok(())
    }
}

impl FromBytes for ValidatorsData {
    fn from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), bytesrepr::Error> {
        let (validators, remainder) = BTreeMap::<PublicKey, U512>::from_bytes(bytes)?;
        let (total_stake, remainder) = U512::from_bytes(remainder)?;
        Ok((
            Self {
                validators,
                total_stake,
            },
            remainder,
        ))
    }
}

/// Everything the cache currently knows about one transaction, assembled piecemeal (and
/// out of order) from two independent SSE signals - `TransactionAccepted` (the transaction's
/// body, seen before execution) and `TransactionProcessed` (its execution result, seen after) -
/// plus, on a cache miss, a direct node fetch.
///
/// Each field is independently `None` ("not learned yet") vs `Some` ("learned"), because the two
/// pieces become known at different, unordered times: `TransactionProcessed` can in principle
/// even race `TransactionAccepted` for the same hash. `execution_info` is additionally double
/// `Option`ed: the outer layer is this cache's own "have I learned anything about execution
/// status" bit, while the inner `Option<ExecutionInfo>` mirrors the domain-level meaning already
/// used elsewhere in the API ("no execution info" - i.e. accepted but not yet executed - is
/// itself a piece of information, distinct from "the cache hasn't looked/heard yet"). Nothing in
/// this module currently constructs `Some(None)`: unlike the outer "haven't heard yet" `None`,
/// asserting "confirmed still pending" would need some invalidation story once execution
/// actually happens, which doesn't exist yet - see `CachingNodeClient`'s equivalent caution
/// before this type existed. The variant is kept for callers (e.g. a future node-fetch path) that
/// can respect that contract.
///
/// A cache entry only exists once at least one of these fields has actual content - see
/// `BinaryPortCache::merge_transaction`.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) struct TransactionKnowledge {
    pub(crate) transaction: Option<Transaction>,
    pub(crate) execution_info: Option<Option<ExecutionInfo>>,
}

impl TransactionKnowledge {
    /// Combines already-known knowledge with newly-learned pieces. `None` for either parameter
    /// of the "new" side means "nothing new learned about that piece this time" - it never erases
    /// existing knowledge, only adds to it.
    pub(crate) fn merged(
        self,
        new_transaction: Option<Transaction>,
        new_execution_info: Option<Option<ExecutionInfo>>,
    ) -> Self {
        Self {
            transaction: new_transaction.or(self.transaction),
            execution_info: new_execution_info.or(self.execution_info),
        }
    }
}

impl ToBytes for TransactionKnowledge {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        let mut buffer = bytesrepr::allocate_buffer(self)?;
        self.write_bytes(&mut buffer)?;
        Ok(buffer)
    }

    fn serialized_length(&self) -> usize {
        self.transaction.serialized_length() + self.execution_info.serialized_length()
    }

    fn write_bytes(&self, writer: &mut Vec<u8>) -> Result<(), bytesrepr::Error> {
        self.transaction.write_bytes(writer)?;
        self.execution_info.write_bytes(writer)?;
        Ok(())
    }
}

impl FromBytes for TransactionKnowledge {
    fn from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), bytesrepr::Error> {
        let (transaction, remainder) = Option::<Transaction>::from_bytes(bytes)?;
        let (execution_info, remainder) = Option::<Option<ExecutionInfo>>::from_bytes(remainder)?;
        Ok((
            Self {
                transaction,
                execution_info,
            },
            remainder,
        ))
    }
}

impl BlockWithSignaturesBuiltInFlight {
    pub(crate) fn block_height_and_hash(&self) -> Option<(u64, BlockHash)> {
        match self {
            BlockWithSignaturesBuiltInFlight::NotSureBlock { block, signatures } => {
                block.as_ref().map(|b| (b.height(), *b.hash())).or_else(|| {
                    signatures.first().and_then(|first_signature| {
                        match first_signature {
                            FinalitySignature::V1(_finality_signature_v1) => {
                                // Ignore v1, we don't want to support cache assembly for old blocks
                                None
                            }
                            FinalitySignature::V2(finality_signature_v2) => Some((
                                finality_signature_v2.block_height(),
                                *finality_signature_v2.block_hash(),
                            )),
                        }
                    })
                })
            }
            BlockWithSignaturesBuiltInFlight::BlockWithSignatures(block) => {
                let block = block.block();
                Some((block.height(), *block.hash()))
            }
        }
    }
}

const BLOCK_WITH_SIGNATURES_PARTS_TAG: u8 = 0;
const ALREADY_KNOWN_TAG: u8 = 1;

/// `FinalitySignature` has no `bytesrepr` support in `casper_types`, so each one is encoded with
/// `bincode` (already used elsewhere in this crate for binary port payloads, e.g.
/// `node_client::parse_response_bincode`) and the resulting blob is framed like `bytesrepr` frames
/// a `Vec<u8>`, i.e. via the blanket `Vec<T: ToBytes>`/`Vec<T: FromBytes>` impls over
/// `Vec<Bytes>`. Each blob must be the `Bytes` newtype, not a raw `Vec<u8>`: the generic
/// `Vec<T>` (de)serialization recurses per-element, so a `Vec<Vec<u8>>` would (de)serialize each
/// blob one byte at a time through `Vec<u8>`'s own impl - which trips a debug-only
/// `ensure_efficient_serialization` assertion in `casper_types::bytesrepr` (it exists precisely
/// to steer callers away from that) as soon as a blob is non-empty. `Bytes` (de)serializes as a
/// single length-prefixed slice instead, with no such recursion.
fn finality_signature_to_bytes(signature: &FinalitySignature) -> Result<Bytes, bytesrepr::Error> {
    bincode::serialize(signature)
        .map(Bytes::from)
        .map_err(|_| bytesrepr::Error::Formatting)
}

fn finality_signature_from_bytes(bytes: &[u8]) -> Result<FinalitySignature, bytesrepr::Error> {
    bincode::deserialize(bytes).map_err(|_| bytesrepr::Error::Formatting)
}

impl ToBytes for BlockWithSignaturesBuiltInFlight {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        let mut buffer = bytesrepr::allocate_buffer(self)?;
        self.write_bytes(&mut buffer)?;
        Ok(buffer)
    }

    fn serialized_length(&self) -> usize {
        U8_SERIALIZED_LENGTH
            + match self {
                BlockWithSignaturesBuiltInFlight::NotSureBlock { block, signatures } => {
                    block.serialized_length()
                        + U32_SERIALIZED_LENGTH
                        + signatures
                            .iter()
                            .map(|signature| {
                                U32_SERIALIZED_LENGTH
                                    + bincode::serialized_size(signature).unwrap_or(0) as usize
                            })
                            .sum::<usize>()
                }
                BlockWithSignaturesBuiltInFlight::BlockWithSignatures(value) => {
                    value.serialized_length()
                }
            }
    }

    fn write_bytes(&self, writer: &mut Vec<u8>) -> Result<(), bytesrepr::Error> {
        match self {
            BlockWithSignaturesBuiltInFlight::NotSureBlock { block, signatures } => {
                writer.push(BLOCK_WITH_SIGNATURES_PARTS_TAG);
                block.write_bytes(writer)?;
                //TODO maybe FinalitySignatures should have bytesrepr also?
                let signature_blobs = signatures
                    .iter()
                    .map(finality_signature_to_bytes)
                    .collect::<Result<Vec<Bytes>, _>>()?;
                signature_blobs.write_bytes(writer)?;
            }
            BlockWithSignaturesBuiltInFlight::BlockWithSignatures(value) => {
                writer.push(ALREADY_KNOWN_TAG);
                value.write_bytes(writer)?;
            }
        }
        Ok(())
    }
}

impl FromBytes for BlockWithSignaturesBuiltInFlight {
    fn from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), bytesrepr::Error> {
        let (tag, remainder) = u8::from_bytes(bytes)?;
        match tag {
            BLOCK_WITH_SIGNATURES_PARTS_TAG => {
                let (block, remainder) = Option::<Block>::from_bytes(remainder)?;
                let (signature_blobs, remainder) = Vec::<Bytes>::from_bytes(remainder)?;
                let signatures = signature_blobs
                    .iter()
                    .map(|blob| finality_signature_from_bytes(blob))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((
                    BlockWithSignaturesBuiltInFlight::NotSureBlock { block, signatures },
                    remainder,
                ))
            }
            ALREADY_KNOWN_TAG => {
                let (value, remainder) = BlockWithSignatures::from_bytes(remainder)?;
                Ok((
                    BlockWithSignaturesBuiltInFlight::BlockWithSignatures(value),
                    remainder,
                ))
            }
            _ => Err(bytesrepr::Error::Formatting),
        }
    }
}

/// Configuration for the persistent, LMDB-backed cache of identifier-addressed, immutable
/// historical data (block headers, blocks with signatures, transactions with execution info)
/// read over the node's binary port. A missing/`None` value at the `RpcServerConfig` level
/// disables the persistent cache entirely.
#[derive(Clone, DataSize, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BinaryPortCacheConfig {
    /// Directory backing the LMDB environment. Created on startup if it doesn't already exist.
    pub path: PathBuf,
    /// Upper bound, in bytes, on the LMDB environment's size. This is a hard ceiling fixed when
    /// the environment is opened; exceeding it causes cache writes to fail (logged and ignored)
    /// but never affects reads or RPC serving.
    #[serde(default = "default_binary_port_cache_max_size_bytes")]
    pub max_size_bytes: usize,
}

#[cfg(any(feature = "testing", test))]
impl BinaryPortCacheConfig {
    pub fn test_default(path: PathBuf) -> Self {
        Self {
            path,
            max_size_bytes: 16 * 1024 * 1024,
        }
    }
}

/// The tri-state result of a persistent cache lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CacheEnvelope<V> {
    /// I have it.
    Have(V),
    /// I don't have it - unknown, go ask the node.
    DontHave,
}

impl<V> CacheEnvelope<V> {
    pub(crate) fn into_option(self) -> Option<V> {
        match self {
            CacheEnvelope::Have(v) => Some(v),
            CacheEnvelope::DontHave => None,
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum CacheError {
    #[error("Bytesrepr error")]
    Bytesrepr(#[from] bytesrepr::Error),
    #[error("LMDB error")]
    Heed(#[from] heed::Error),
    #[error("background cache task panicked: {0}")]
    TaskJoin(#[from] tokio::task::JoinError),
}

/// Persistent cache for a fixed set of identifier-addressed, immutable resources read over the
/// binary port. Implementations must hide their storage engine entirely - this trait, plus
/// [`CacheEnvelope`], is the only thing the rest of the crate depends on.
#[async_trait]
pub(crate) trait BinaryPortCache: Send + Sync {
    async fn get_block_header(
        &self,
        id: BlockIdentifier,
    ) -> Result<CacheEnvelope<BlockHeader>, CacheError>;

    async fn put_block_header(&self, value: &BlockHeader) -> Result<(), CacheError>;

    async fn get_block_with_signatures(
        &self,
        id: BlockIdentifier,
    ) -> Result<CacheEnvelope<BlockWithSignatures>, CacheError>;

    async fn put_block_with_signatures(
        &self,
        value: &BlockWithSignatures,
    ) -> Result<(), CacheError>;

    /// Returns everything currently known about `hash` - possibly just the transaction body,
    /// possibly just its execution info, possibly both, possibly neither field (a
    /// `CacheEnvelope::DontHave` instead, if nothing at all is known yet). Callers decide for
    /// themselves whether the returned knowledge is enough to answer the request they're serving,
    /// or whether they still need to fall back to the node (and then call
    /// [`Self::merge_transaction`] with whatever they learned).
    async fn get_transaction(
        &self,
        hash: TransactionHash,
    ) -> Result<CacheEnvelope<TransactionKnowledge>, CacheError>;

    /// Merges newly-learned pieces into whatever is already known about `hash`, creating the
    /// entry if this is the first thing ever learned about it. `None` for either parameter means
    /// "nothing new learned about that piece this call" and leaves the existing value (if any)
    /// untouched - it never erases already-known information. Locks internally against
    /// concurrent merges for the same hash, so callers never need their own read-decide-write
    /// dance (contrast [`Self::put_block_parts`], which pushes that requirement onto callers).
    async fn merge_transaction(
        &self,
        hash: TransactionHash,
        transaction: Option<Transaction>,
        execution_info: Option<Option<ExecutionInfo>>,
    ) -> Result<(), CacheError>;

    async fn get_block_parts(
        &self,
        block_identifier: BlockIdentifier,
    ) -> Result<CacheEnvelope<BlockWithSignaturesBuiltInFlight>, CacheError>;

    async fn put_block_parts(
        &self,
        in_flight: &BlockWithSignaturesBuiltInFlight,
    ) -> Result<(), CacheError>;

    async fn get_validators(
        &self,
        era_id: EraId,
    ) -> Result<CacheEnvelope<ValidatorsData>, CacheError>;
}

/// Reacts to `SidecarEvent`s observed over the sidecar's SSE stream, advancing whatever in-flight
/// (not-yet-fully-known) persistent cache state a given event bears on: a switch block's
/// validator weights becoming known, a chainspec-derived finality threshold fraction becoming
/// known, or a finality signature that may or may not push a block over its finality threshold.
/// All bookkeeping this requires - which blocks are still pending a validator set, recording
/// newly learned validators/finality thresholds, etc. - is private to the implementation;
/// callers just forward every event they see and don't need to know which of them matter.
#[async_trait]
pub(crate) trait InFlightDataHandling: Send + Sync {
    async fn handle_sidecar_event(self: Arc<Self>, event: SidecarEvent) -> Result<(), CacheError>;

    /// Resolves the validator set active in `era_id`: a cache hit if already known (e.g. from a
    /// switch block seen live over SSE, via [`Self::handle_sidecar_event`]), otherwise the
    /// binary port's latest-switch-block fallback - which only resolves the era following the
    /// *latest* switch block known to the node - caching the result for next time.
    async fn resolve_validators(
        &self,
        era_id: EraId,
    ) -> Result<Option<ValidatorsData>, ClientError>;
}

/// Derives the validator set active in `era_id` from `node_client`'s latest switch block header,
/// without touching any cache. Only resolves `era_id` when it is the era following the *latest*
/// switch block known to the node - i.e. the era the node is currently in or about to enter; any
/// other era returns `Ok(None)`, since the binary port has no request to fetch the validators of
/// an arbitrary era directly.
pub(crate) async fn validators_from_latest_switch_block<T: NodeClient + Send + Sync + ?Sized>(
    node_client: &T,
    era_id: EraId,
) -> Result<Option<ValidatorsData>, ClientError> {
    let Some(header) = node_client.read_latest_switch_block_header().await? else {
        return Ok(None);
    };
    if header.next_block_era_id() != era_id {
        return Ok(None);
    }
    let Some(weights) = header.next_era_validator_weights() else {
        return Ok(None);
    };
    Ok(Some(ValidatorsData::new(weights.clone())))
}

/// Builds the persistent cache backend (currently LMDB/`heed`) behind the [`BinaryPortCache`] and
/// [`InFlightDataHandling`] trait objects - [`HeedBinaryPortCache`] implements both directly, so
/// a single `Arc` can be handed out under either trait object and both see the same underlying
/// store (crucial since [`InFlightDataHandling::handle_sidecar_event`] writes validators that
/// [`BinaryPortCache::get_validators`] must then read back).
pub(crate) fn new_binary_port_cache(
    config: &BinaryPortCacheConfig,
    node_client: Arc<dyn NodeClient>,
) -> anyhow::Result<Arc<HeedBinaryPortCache>> {
    Ok(Arc::new(HeedBinaryPortCache::open(config, node_client)?))
}

/// How often [`HeedBinaryPortCache::prune_old_eras`] runs.
const ERA_PRUNE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// Runs forever, periodically pruning [`HeedBinaryPortCache`]'s era-scoped tables of entries for
/// eras nothing has asked about in a while - see `HeedBinaryPortCache::prune_old_eras`. Errors
/// are logged and never stop the loop: a failed prune pass just means those tables keep growing
/// a bit longer, not something worth tearing the sidecar down over (unlike e.g.
/// `cache_update_loop`'s SSE bus, whose loss really does mean the cache can go stale).
pub(crate) async fn prune_loop(store: Arc<HeedBinaryPortCache>) -> anyhow::Result<()> {
    let mut interval = tokio::time::interval(ERA_PRUNE_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        if let Err(err) = store.prune_old_eras().await {
            tracing::warn!(%err, "binary port cache: prune_old_eras failed");
        }
    }
}
