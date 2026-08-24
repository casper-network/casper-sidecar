use std::{
    ops::Mul,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use casper_binary_port::TransactionWithExecutionInfo;
use casper_event_types::SidecarEvent;
use casper_types::{
    Block, BlockHash, BlockHeader, BlockIdentifier, BlockSignatures, BlockSignaturesV2,
    BlockWithSignatures, EraId, FinalitySignature, ProtocolVersion, TransactionHash, U512,
    bytesrepr::{self, FromBytes, ToBytes},
    execution::ExecutionResult,
};
use heed::{Database, Env, EnvOpenOptions, RwTxn, types::Bytes};
use metrics::binary_port_cache as cache_metrics;
use num_rational::Ratio;
use serde::Deserialize;
use tracing::warn;

use crate::{ClientError, NodeClient};

use super::{
    BinaryPortCache, BinaryPortCacheConfig, BlockWithSignaturesBuiltInFlight, CacheEnvelope,
    CacheError, InFlightDataHandling, ValidatorsData, validators_from_latest_switch_block,
};

/// Number of named databases the environment must have room for. Fixed at env-creation time by
/// LMDB; keep some headroom over the 7 tables created below.
const MAX_DBS: u32 = 12;

/// Number of stripes in each [`StripedLocks`] table. A fixed-size array (rather than one entry
/// per key) keeps memory bounded over a long-running process's lifetime at the cost of
/// unrelated keys occasionally sharing a stripe and serializing against each other - harmless
/// for the short critical sections these locks guard.
const LOCK_STRIPE_COUNT: usize = 256;

/// How far behind the most recently asked-about era `era_horizon` trails it. Keeps a margin of
/// this many eras' worth of `blocks_by_era`/`validators_by_era` entries around the "current" era
/// so a slightly-delayed deferred recheck (see `add_pending_block_for_era`) still finds the
/// validators/pending-heights it needs, instead of racing the pruner.
const ERA_HORIZON_MARGIN: u64 = 10;

/// A fixed-size table of async mutexes, indexed by `key % stripe count`. Used to serialize
/// read-modify-write sequences (spanning multiple independent LMDB transactions, with async work
/// possibly in between) against each other on a per-key basis, without the unbounded memory
/// growth of a lock-per-key map.
///
/// Guards taken from *different* `StripedLocks` instances (e.g. `block_locks` and `era_locks`)
/// are never nested in the wrong order anywhere in this module - only `block_locks` may be held
/// while acquiring an `era_locks` guard, never the reverse - so the two tables can't deadlock
/// against each other.
struct StripedLocks {
    stripes: Vec<tokio::sync::Mutex<()>>,
}

impl StripedLocks {
    fn new() -> Self {
        Self {
            stripes: (0..LOCK_STRIPE_COUNT)
                .map(|_| tokio::sync::Mutex::new(()))
                .collect(),
        }
    }

    /// Acquires the stripe for `key`, blocking until it's available. The returned guard must be
    /// held for the entire read-modify-write section it protects.
    async fn lock(&self, key: u64) -> tokio::sync::MutexGuard<'_, ()> {
        let index = (key as usize) % self.stripes.len();
        self.stripes[index].lock().await
    }
}

const BLOCK_HEADER_BY_HEIGHT_DB: &str = "block_header_by_height";
// Backs both `get_block_with_signatures`/`put_block_with_signatures` and
// `get_block_parts`/`put_block_parts`: both resources are keyed by block height and store a
// `BlockWithSignaturesBuiltInFlight` (a fully-known block is just the `BlockWithSignatures`
// variant of that enum), so they share a single table instead of duplicating block content.
const BLOCK_WITH_SIGNATURES_BY_HEIGHT_DB: &str = "block_with_signatures_by_height";
const TRANSACTION_WITH_EXECUTION_INFO_DB: &str = "transaction_with_execution_info_by_hash";
const TRANSACTION_EXECUTION_RESULT_BY_HASH_DB: &str = "transaction_execution_result_by_hash";
const BLOCK_HASH_TO_HEIGHT_INDEX_DB: &str = "block_hash_to_height_index";
const BLOCKS_BY_ERA_DB: &str = "blocks_by_era";
const VALIDATORS_BY_ERA_DB: &str = "validators_by_era";
const FINALITY_THRESHOLD_FRACTION_BY_PROTOCOL_VERSION_DB: &str =
    "finality_threshold_fraction_by_protocol_version";

/// Persistent LMDB-backed store that is both the crate's [`BinaryPortCache`] (plain
/// identifier-addressed immutable data) and its [`InFlightDataHandling`] (data assembled/derived
/// from the sidecar's own SSE stream) - the same instance implements both traits so that
/// validators/finality thresholds written while reacting to an SSE event are immediately visible
/// through `get_validators`, without needing to coordinate two separate stores.
pub(crate) struct HeedBinaryPortCache {
    env: Env,
    block_header_by_height: Database<Bytes, Bytes>,
    block_with_signatures_by_height: Database<Bytes, Bytes>,
    transaction_with_execution_info_by_hash: Database<Bytes, Bytes>,
    transaction_execution_result_by_hash: Database<Bytes, Bytes>,
    block_hash_to_height_index: Database<Bytes, Bytes>,
    blocks_by_era: Database<Bytes, Bytes>,
    validators_by_era: Database<Bytes, Bytes>,
    finality_threshold_fraction_by_protocol_version: Database<Bytes, Bytes>,
    node_client: Arc<dyn NodeClient>,
    /// Serializes read-modify-write sequences against `block_with_signatures_by_height`, keyed
    /// by block height. See the module-level race between the SSE-driven assembly path
    /// (`handle_finality_signature`/`handle_block_added`) and the read-through cache-aside in
    /// `CachingNodeClient::read_block_with_signatures` (which calls `put_block_with_signatures`
    /// on a miss): without this, one path can read a stale value, do its (possibly async) work,
    /// and then write back over a more-complete value the other path stored in the meantime.
    block_locks: StripedLocks,
    /// Serializes read-modify-write sequences against `blocks_by_era`'s pending-heights list,
    /// keyed by era id. Without this, two concurrent events pending on the same era's
    /// not-yet-known validators (e.g. two `FinalitySignature`s for different blocks in that era)
    /// can each read the same list, append their own height, and write back - the second write
    /// clobbers the first, silently dropping a height from the pending list.
    era_locks: StripedLocks,
    /// Lower bound (exclusive) below which `blocks_by_era`/`validators_by_era` entries are
    /// eligible for pruning by [`Self::prune_old_eras`]. Advanced by [`Self::note_era_asked`]
    /// to `era_id - ERA_HORIZON_MARGIN` every time `era_id`'s validators are looked up, so it
    /// tracks (with a safety margin) the highest era anything has actually asked about -
    /// monotonically non-decreasing, so a burst of lookups for an older era (e.g. a deferred
    /// recheck) can never move it backwards and unprune something concurrently deleted.
    era_horizon: AtomicU64,
}

impl HeedBinaryPortCache {
    /// Plain constructor: assembles an already-opened `Env` and its already-created `Database`
    /// handles into a `HeedBinaryPortCache`. Infallible - all the fallible I/O (creating the
    /// cache directory, opening the LMDB environment, creating the named databases) lives in
    /// [`Self::open`].
    #[allow(clippy::too_many_arguments)]
    fn new(
        env: Env,
        block_header_by_height: Database<Bytes, Bytes>,
        block_with_signatures_by_height: Database<Bytes, Bytes>,
        transaction_with_execution_info_by_hash: Database<Bytes, Bytes>,
        transaction_execution_result_by_hash: Database<Bytes, Bytes>,
        block_hash_to_height_index: Database<Bytes, Bytes>,
        blocks_by_era: Database<Bytes, Bytes>,
        validators_by_era: Database<Bytes, Bytes>,
        finality_threshold_fraction_by_protocol_version: Database<Bytes, Bytes>,
        node_client: Arc<dyn NodeClient>,
    ) -> Self {
        Self {
            env,
            block_header_by_height,
            block_with_signatures_by_height,
            transaction_with_execution_info_by_hash,
            transaction_execution_result_by_hash,
            block_hash_to_height_index,
            blocks_by_era,
            validators_by_era,
            finality_threshold_fraction_by_protocol_version,
            node_client,
            block_locks: StripedLocks::new(),
            era_locks: StripedLocks::new(),
            era_horizon: AtomicU64::new(0),
        }
    }

    pub(crate) fn open(
        config: &BinaryPortCacheConfig,
        node_client: Arc<dyn NodeClient>,
    ) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&config.path).map_err(|err| {
            anyhow::anyhow!(
                "failed to create binary port cache directory {:?}: {err}",
                config.path
            )
        })?;

        let env = open_env(&config.path, config.max_size_bytes)?;

        let mut wtxn = env.write_txn()?;
        let block_header_by_height =
            env.create_database(&mut wtxn, Some(BLOCK_HEADER_BY_HEIGHT_DB))?;
        let block_with_signatures_by_height =
            env.create_database(&mut wtxn, Some(BLOCK_WITH_SIGNATURES_BY_HEIGHT_DB))?;
        let transaction_with_execution_info_by_hash =
            env.create_database(&mut wtxn, Some(TRANSACTION_WITH_EXECUTION_INFO_DB))?;
        let transaction_execution_result_by_hash =
            env.create_database(&mut wtxn, Some(TRANSACTION_EXECUTION_RESULT_BY_HASH_DB))?;
        let block_hash_to_height_index =
            env.create_database(&mut wtxn, Some(BLOCK_HASH_TO_HEIGHT_INDEX_DB))?;
        let blocks_by_era = env.create_database(&mut wtxn, Some(BLOCKS_BY_ERA_DB))?;
        let validators_by_era = env.create_database(&mut wtxn, Some(VALIDATORS_BY_ERA_DB))?;
        let finality_threshold_fraction_by_protocol_version = env.create_database(
            &mut wtxn,
            Some(FINALITY_THRESHOLD_FRACTION_BY_PROTOCOL_VERSION_DB),
        )?;
        wtxn.commit()?;

        Ok(Self::new(
            env,
            block_header_by_height,
            block_with_signatures_by_height,
            transaction_with_execution_info_by_hash,
            transaction_execution_result_by_hash,
            block_hash_to_height_index,
            blocks_by_era,
            validators_by_era,
            finality_threshold_fraction_by_protocol_version,
            node_client,
        ))
    }
}

/// Opens (creating if needed) the LMDB environment at `path`.
///
/// # Safety
/// `EnvOpenOptions::open` requires that no other process (or, within this process, no other
/// `Env` with a mismatched `map_size`/`max_dbs`) touches the same directory concurrently. We
/// rely on `config.path` being a directory dedicated to this sidecar's binary port cache, and on
/// this function being the sole call site for `open` in the crate, so those settings are always
/// consistent for a given process lifetime.
fn open_env(path: &Path, max_size_bytes: usize) -> anyhow::Result<Env> {
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(max_size_bytes)
            .max_dbs(MAX_DBS)
            .open(path)
    }
    .map_err(|err| anyhow::anyhow!("failed to open binary port cache env at {path:?}: {err}"))?;
    Ok(env)
}

fn transaction_key(
    hash: &TransactionHash,
    with_finalized_approvals: bool,
) -> Result<Vec<u8>, bytesrepr::Error> {
    let mut key = hash.to_bytes()?;
    key.push(with_finalized_approvals as u8);
    Ok(key)
}

/// Shared read path for `BlockIdentifier`-keyed resources: resolves `Hash` to a `Height` via the
/// hash-to-height index, then looks the value up in the by-height db, within a single read
/// transaction.
fn get_by_block_identifier<V: FromBytes>(
    env: &Env,
    by_height_db: Database<Bytes, Bytes>,
    block_hash_to_height_index: Database<Bytes, Bytes>,
    id: BlockIdentifier,
) -> Result<CacheEnvelope<V>, CacheError> {
    let rtxn = env.read_txn()?;

    let height_bytes: Vec<u8> = match id {
        BlockIdentifier::Hash(hash) => {
            match block_hash_to_height_index.get(&rtxn, &hash.to_bytes()?)? {
                Some(raw) => {
                    let (height, remainder) = u64::from_bytes(raw)?;
                    //TODO log error
                    if !remainder.is_empty() {
                        return Err(CacheError::Bytesrepr(bytesrepr::Error::LeftOverBytes));
                    }
                    height.to_bytes()?
                }
                None => return Ok(CacheEnvelope::DontHave),
            }
        }
        BlockIdentifier::Height(height) => height.to_bytes()?,
    };

    match by_height_db.get(&rtxn, height_bytes.as_slice())? {
        Some(raw) => {
            let value = bytesrepr::deserialize_from_slice::<_, V>(raw)?;
            Ok(CacheEnvelope::Have(value))
        }
        None => Ok(CacheEnvelope::DontHave),
    }
}

/// `block_with_signatures_by_height` stores `BlockWithSignaturesBuiltInFlight` so it can be
/// shared with `get_block_parts`/`put_block_parts`. `get_block_with_signatures` can only serve a
/// fully-known block, so a `NotSureBlock` entry (still being assembled from SSE events) is
/// reported the same as a miss.
fn block_with_signatures_from_envelope(
    envelope: CacheEnvelope<BlockWithSignaturesBuiltInFlight>,
) -> CacheEnvelope<BlockWithSignatures> {
    match envelope {
        CacheEnvelope::Have(BlockWithSignaturesBuiltInFlight::BlockWithSignatures(value)) => {
            CacheEnvelope::Have(value)
        }
        CacheEnvelope::Have(BlockWithSignaturesBuiltInFlight::NotSureBlock { .. }) => {
            CacheEnvelope::DontHave
        }
        CacheEnvelope::DontHave => CacheEnvelope::DontHave,
    }
}

impl HeedBinaryPortCache {
    /// Records the `hash` -> `height` mapping in `block_hash_to_height_index`, within the
    /// caller's write transaction (so it can be committed atomically alongside whatever else
    /// that transaction is writing). This is the only direction ever read (by
    /// `get_by_block_identifier`'s `BlockIdentifier::Hash` case), so it's the only direction
    /// the index stores.
    fn update_block_hash_to_height_index(
        block_hash_to_height_index: Database<Bytes, Bytes>,
        wtxn: &mut RwTxn,
        hash: BlockHash,
        height: u64,
    ) -> Result<(), CacheError> {
        let hash_bytes = hash.to_bytes()?;
        let height_bytes = height.to_bytes()?;
        block_hash_to_height_index.put(wtxn, &hash_bytes, &height_bytes)?;
        Ok(())
    }
}

/// Shared write path: atomically writes the value under its height key and records the
/// hash -> height index entry, in one `RwTxn`.
fn put_block_value(
    env: &Env,
    by_height_db: Database<Bytes, Bytes>,
    block_hash_to_height_index: Database<Bytes, Bytes>,
    hash: BlockHash,
    height: u64,
    value_bytes: Vec<u8>,
) -> Result<(), CacheError> {
    let mut wtxn = env.write_txn()?;
    let height_bytes = height.to_bytes()?;
    by_height_db.put(&mut wtxn, &height_bytes, &value_bytes)?;
    HeedBinaryPortCache::update_block_hash_to_height_index(
        block_hash_to_height_index,
        &mut wtxn,
        hash,
        height,
    )?;
    wtxn.commit()?;
    Ok(())
}

#[async_trait]
impl BinaryPortCache for HeedBinaryPortCache {
    async fn get_block_header(
        &self,
        id: BlockIdentifier,
    ) -> Result<CacheEnvelope<BlockHeader>, CacheError> {
        let env = self.env.clone();
        let block_hash_to_height_index = self.block_hash_to_height_index;
        let by_height = self.block_header_by_height;
        tokio::task::spawn_blocking(move || {
            get_by_block_identifier::<BlockHeader>(&env, by_height, block_hash_to_height_index, id)
        })
        .await
        .map_err(CacheError::from)?
    }

    async fn put_block_header(&self, value: &BlockHeader) -> Result<(), CacheError> {
        let env = self.env.clone();
        let by_height = self.block_header_by_height;
        let block_hash_to_height_index = self.block_hash_to_height_index;
        let hash = value.block_hash();
        let height = value.height();
        let value_bytes = value.to_bytes()?;
        tokio::task::spawn_blocking(move || {
            put_block_value(
                &env,
                by_height,
                block_hash_to_height_index,
                hash,
                height,
                value_bytes,
            )
        })
        .await
        .map_err(CacheError::from)?
    }

    async fn get_block_with_signatures(
        &self,
        id: BlockIdentifier,
    ) -> Result<CacheEnvelope<BlockWithSignatures>, CacheError> {
        let env = self.env.clone();
        let block_hash_to_height_index = self.block_hash_to_height_index;
        let by_height = self.block_with_signatures_by_height;
        tokio::task::spawn_blocking(move || {
            let envelope = get_by_block_identifier::<BlockWithSignaturesBuiltInFlight>(
                &env,
                by_height,
                block_hash_to_height_index,
                id,
            )?;
            Ok(block_with_signatures_from_envelope(envelope))
        })
        .await
        .map_err(CacheError::from)?
    }

    async fn put_block_with_signatures(
        &self,
        value: &BlockWithSignatures,
    ) -> Result<(), CacheError> {
        let env = self.env.clone();
        let by_height = self.block_with_signatures_by_height;
        let block_hash_to_height_index = self.block_hash_to_height_index;
        let hash = *value.block().hash();
        let height = value.block().height();
        let in_flight = BlockWithSignaturesBuiltInFlight::BlockWithSignatures(value.clone());
        let value_bytes = in_flight.to_bytes()?;
        // Hold this height's lock across the write so it can't race the SSE assembly path
        // (`handle_finality_signature`/`handle_block_added`, via `put_block_parts`), which reads
        // the current entry, does async work, and writes back over the same key - see
        // `block_locks`'s docs.
        let _guard = self.block_locks.lock(height).await;
        tokio::task::spawn_blocking(move || {
            put_block_value(
                &env,
                by_height,
                block_hash_to_height_index,
                hash,
                height,
                value_bytes,
            )
        })
        .await
        .map_err(CacheError::from)??;
        cache_metrics::inc_blocks_fetched_from_node();
        Ok(())
    }

    async fn get_transaction_with_execution_info(
        &self,
        hash: TransactionHash,
        with_finalized_approvals: bool,
    ) -> Result<CacheEnvelope<TransactionWithExecutionInfo>, CacheError> {
        let env = self.env.clone();
        let db = self.transaction_with_execution_info_by_hash;
        tokio::task::spawn_blocking(
            move || -> Result<CacheEnvelope<TransactionWithExecutionInfo>, CacheError> {
                let key = transaction_key(&hash, with_finalized_approvals)?;
                let rtxn = env.read_txn()?;
                match db.get(&rtxn, key.as_slice())? {
                    Some(raw) => {
                        let value = bytesrepr::deserialize_from_slice(raw)?;
                        Ok(CacheEnvelope::Have(value))
                    }
                    None => Ok(CacheEnvelope::DontHave),
                }
            },
        )
        .await
        .map_err(CacheError::from)?
    }

    async fn put_transaction_with_execution_info(
        &self,
        hash: TransactionHash,
        with_finalized_approvals: bool,
        value: &TransactionWithExecutionInfo,
    ) -> Result<(), CacheError> {
        let key = transaction_key(&hash, with_finalized_approvals)?;
        let value_bytes = value.to_bytes()?;
        let env = self.env.clone();
        let db = self.transaction_with_execution_info_by_hash;
        tokio::task::spawn_blocking(move || -> Result<(), CacheError> {
            let mut wtxn = env.write_txn()?;
            db.put(&mut wtxn, key.as_slice(), &value_bytes)?;
            wtxn.commit()?;
            Ok(())
        })
        .await
        .map_err(CacheError::from)?
    }

    async fn get_transaction_execution_result(
        &self,
        hash: TransactionHash,
    ) -> Result<CacheEnvelope<ExecutionResult>, CacheError> {
        let env = self.env.clone();
        let db = self.transaction_execution_result_by_hash;
        tokio::task::spawn_blocking(
            move || -> Result<CacheEnvelope<ExecutionResult>, CacheError> {
                let key = hash.to_bytes()?;
                let rtxn = env.read_txn()?;
                match db.get(&rtxn, key.as_slice())? {
                    Some(raw) => {
                        let value = bytesrepr::deserialize_from_slice(raw)?;
                        Ok(CacheEnvelope::Have(value))
                    }
                    None => Ok(CacheEnvelope::DontHave),
                }
            },
        )
        .await
        .map_err(CacheError::from)?
    }

    async fn put_transaction_execution_result(
        &self,
        hash: TransactionHash,
        value: &ExecutionResult,
    ) -> Result<(), CacheError> {
        let key = hash.to_bytes()?;
        let value_bytes = value.to_bytes()?;
        let env = self.env.clone();
        let db = self.transaction_execution_result_by_hash;
        tokio::task::spawn_blocking(move || -> Result<(), CacheError> {
            let mut wtxn = env.write_txn()?;
            db.put(&mut wtxn, key.as_slice(), &value_bytes)?;
            wtxn.commit()?;
            Ok(())
        })
        .await
        .map_err(CacheError::from)?
    }

    async fn get_block_parts(
        &self,
        block_identifier: BlockIdentifier,
    ) -> Result<CacheEnvelope<BlockWithSignaturesBuiltInFlight>, CacheError> {
        let env = self.env.clone();
        let block_hash_to_height_index = self.block_hash_to_height_index;
        let by_height = self.block_with_signatures_by_height;
        tokio::task::spawn_blocking(move || {
            get_by_block_identifier::<BlockWithSignaturesBuiltInFlight>(
                &env,
                by_height,
                block_hash_to_height_index,
                block_identifier,
            )
        })
        .await
        .map_err(CacheError::from)?
    }

    /// Writes `in_flight` under its block's height/hash, first giving it a chance to
    /// self-upgrade from `NotSureBlock` to `BlockWithSignatures` if validators for its era are
    /// already known and sufficient.
    ///
    /// # Locking
    /// This does *not* acquire `block_locks` itself. Every call site in this module first reads
    /// the block's current cache entry (`get_block_parts`) and then, possibly after further
    /// async work, calls this to write the result - the read and this write must be treated as
    /// one atomic unit, so callers acquire `self.block_locks.lock(height)` before their read and
    /// hold the guard across this call. Locking here too would deadlock against that (a
    /// `tokio::sync::Mutex` isn't reentrant).
    async fn put_block_parts(
        &self,
        in_flight: &BlockWithSignaturesBuiltInFlight,
    ) -> Result<(), CacheError> {
        let (height, hash) = match in_flight.block_height_and_hash() {
            Some(tuple) => tuple,
            None => {
                // this BlockWithSignaturesBuiltInFlight is empty
                return Ok(());
            }
        };

        let mut to_store = in_flight.clone();
        if let BlockWithSignaturesBuiltInFlight::NotSureBlock { block, signatures } = &to_store
            && let (Some(block), false) = (block.clone(), signatures.is_empty())
        {
            let signatures = signatures.clone();
            let era_id = block.era_id();
            match self.get_validators(era_id).await? {
                CacheEnvelope::Have(validators) => {
                    to_store = self
                        .finalize_if_sufficient(block, signatures, &validators)
                        .await?;
                }
                CacheEnvelope::DontHave => {
                    // Validators for this era aren't known yet: remember this block so it
                    // can be rechecked once they are (see `recheck_pending_blocks_for_era`).
                    // Don't eagerly fetch them here - `handle_finality_signature`, the other
                    // caller that can reach this branch, already does that itself (via
                    // `spawn_validators_fetch_and_recheck`) once, right before calling this;
                    // doing it again here just duplicates that node round-trip.
                    self.add_pending_block_for_era(era_id, block.height())
                        .await?;
                }
            }
        }

        let env = self.env.clone();
        let by_height = self.block_with_signatures_by_height;
        let block_hash_to_height_index = self.block_hash_to_height_index;
        let value_bytes = to_store.to_bytes()?;
        tokio::task::spawn_blocking(move || {
            put_block_value(
                &env,
                by_height,
                block_hash_to_height_index,
                hash,
                height,
                value_bytes,
            )
        })
        .await
        .map_err(CacheError::from)?
    }

    async fn get_validators(
        &self,
        era_id: EraId,
    ) -> Result<CacheEnvelope<ValidatorsData>, CacheError> {
        // Every era-scoped lookup in this cache funnels through here (directly or via
        // `resolve_validators`), so this is the single chokepoint for "someone asked about
        // era_id" that `prune_old_eras` relies on to know which eras are still of interest.
        self.note_era_asked(era_id);
        let env = self.env.clone();
        let db = self.validators_by_era;
        tokio::task::spawn_blocking(
            move || -> Result<CacheEnvelope<ValidatorsData>, CacheError> {
                let rtxn = env.read_txn()?;
                let key = era_id.to_bytes()?;
                match db.get(&rtxn, key.as_slice())? {
                    Some(raw) => {
                        let (value, remainder) = ValidatorsData::from_bytes(raw)?;
                        if !remainder.is_empty() {
                            return Err(CacheError::Bytesrepr(bytesrepr::Error::LeftOverBytes));
                        }
                        Ok(CacheEnvelope::Have(value))
                    }
                    None => Ok(CacheEnvelope::DontHave),
                }
            },
        )
        .await
        .map_err(CacheError::from)?
    }
}

impl HeedBinaryPortCache {
    /// Blocks in `era_id` that are still `NotSureBlock` and are waiting on that era's validators
    /// weights to become known before their finality can be (re)checked. See
    /// [`Self::add_pending_block_for_era`].
    async fn get_pending_blocks_for_era(
        &self,
        era_id: EraId,
    ) -> Result<CacheEnvelope<Vec<u64>>, CacheError> {
        let env = self.env.clone();
        let db = self.blocks_by_era;
        tokio::task::spawn_blocking(move || -> Result<CacheEnvelope<Vec<u64>>, CacheError> {
            let rtxn = env.read_txn()?;
            let key = era_id.to_bytes()?;
            match db.get(&rtxn, key.as_slice())? {
                Some(raw) => {
                    let (value, remainder) = Vec::<u64>::from_bytes(raw)?;
                    if !remainder.is_empty() {
                        return Err(CacheError::Bytesrepr(bytesrepr::Error::LeftOverBytes));
                    }
                    Ok(CacheEnvelope::Have(value))
                }
                None => Ok(CacheEnvelope::DontHave),
            }
        })
        .await
        .map_err(CacheError::from)?
    }

    /// Records `height` as a block in `era_id` awaiting a finality recheck once validator
    /// weights for that era become known. Idempotent - appending a height already present is a
    /// no-op.
    async fn add_pending_block_for_era(
        &self,
        era_id: EraId,
        height: u64,
    ) -> Result<(), CacheError> {
        // Held across the read-modify-write below so two concurrent calls for the same era
        // (e.g. two blocks whose signatures both arrive before that era's validators are known)
        // can't both read the same list, append their own height, and have the second write
        // clobber the first - see `era_locks`'s docs.
        let _guard = self.era_locks.lock(era_id.value()).await;
        let env = self.env.clone();
        let db = self.blocks_by_era;
        tokio::task::spawn_blocking(move || -> Result<(), CacheError> {
            let key = era_id.to_bytes()?;
            let mut wtxn = env.write_txn()?;
            let mut heights = match db.get(&wtxn, key.as_slice())? {
                Some(raw) => {
                    let (value, remainder) = Vec::<u64>::from_bytes(raw)?;
                    if !remainder.is_empty() {
                        return Err(CacheError::Bytesrepr(bytesrepr::Error::LeftOverBytes));
                    }
                    value
                }
                None => Vec::new(),
            };
            if !heights.contains(&height) {
                heights.push(height);
                db.put(&mut wtxn, key.as_slice(), &heights.to_bytes()?)?;
                wtxn.commit()?;
            }
            Ok(())
        })
        .await
        .map_err(CacheError::from)?
    }

    async fn put_validators(&self, era_id: EraId, blob: &ValidatorsData) -> Result<(), CacheError> {
        let env = self.env.clone();
        let db = self.validators_by_era;
        let blob = blob.clone();
        tokio::task::spawn_blocking(move || -> Result<(), CacheError> {
            let key = era_id.to_bytes()?;
            let value_bytes = blob.to_bytes()?;
            let mut wtxn = env.write_txn()?;
            db.put(&mut wtxn, &key, &value_bytes)?;
            wtxn.commit()?;
            Ok(())
        })
        .await
        .map_err(CacheError::from)?
    }

    async fn get_finality_threshold_fraction(
        &self,
        protocol_version: ProtocolVersion,
    ) -> Result<CacheEnvelope<Ratio<u64>>, CacheError> {
        let env = self.env.clone();
        let db = self.finality_threshold_fraction_by_protocol_version;
        tokio::task::spawn_blocking(move || -> Result<CacheEnvelope<Ratio<u64>>, CacheError> {
            let rtxn = env.read_txn()?;
            let key = protocol_version.to_bytes()?;
            match db.get(&rtxn, key.as_slice())? {
                Some(raw) => {
                    let (value, remainder) = Ratio::<u64>::from_bytes(raw)?;
                    if !remainder.is_empty() {
                        return Err(CacheError::Bytesrepr(bytesrepr::Error::LeftOverBytes));
                    }
                    Ok(CacheEnvelope::Have(value))
                }
                None => Ok(CacheEnvelope::DontHave),
            }
        })
        .await
        .map_err(CacheError::from)?
    }

    async fn put_finality_threshold_fraction(
        &self,
        protocol_version: ProtocolVersion,
        fraction: Ratio<u64>,
    ) -> Result<(), CacheError> {
        let env = self.env.clone();
        let db = self.finality_threshold_fraction_by_protocol_version;
        tokio::task::spawn_blocking(move || -> Result<(), CacheError> {
            let key = protocol_version.to_bytes()?;
            let value_bytes = fraction.to_bytes()?;
            let mut wtxn = env.write_txn()?;
            db.put(&mut wtxn, &key, &value_bytes)?;
            wtxn.commit()?;
            Ok(())
        })
        .await
        .map_err(CacheError::from)?
    }
}

/// The `[core]` section of `chainspec.toml`, as much of it as this crate needs. Deserializing
/// only this instead of the whole chainspec (`casper_types::Chainspec` doesn't even implement
/// `Deserialize`, and `CoreConfig` requires every one of its ~30 fields with
/// `deny_unknown_fields`) means unrelated chainspec churn across protocol versions can't break
/// this.
#[derive(Deserialize)]
struct CoreSection {
    finality_threshold_fraction: Ratio<u64>,
}

#[derive(Deserialize)]
struct ChainspecToml {
    core: CoreSection,
}

fn parse_finality_threshold_fraction(chainspec_bytes: &[u8]) -> anyhow::Result<Ratio<u64>> {
    let text = std::str::from_utf8(chainspec_bytes)?;
    let parsed: ChainspecToml = toml::from_str(text)?;
    Ok(parsed.core.finality_threshold_fraction)
}

/// Sums the stake of the unique validators (deduped by public key, guarding against a
/// replayed/duplicated SSE signature double-counting one validator's weight) who signed `block`
/// among `signatures`, and checks whether that clears `fraction` of the era's total stake.
/// Mirrors casper-node's own "weak" sufficiency check (`validator_matrix.rs::signature_weight`,
/// the tier the spec asks for) rather than reimplementing full consensus finality.
fn is_sufficient_weight(
    block: &Block,
    signatures: &[FinalitySignature],
    validators: &ValidatorsData,
    fraction: Ratio<u64>,
) -> bool {
    let signed_weight = signatures
        .iter()
        .filter_map(|fs| match fs {
            FinalitySignature::V2(v2) if v2.block_hash() == block.hash() => validators
                .validators
                .get(v2.public_key())
                .map(|weight| (v2.public_key(), weight)),
            _ => None,
        })
        .collect::<std::collections::BTreeMap<_, _>>()
        .into_values()
        .fold(U512::zero(), |acc, weight| acc + weight);
    signed_weight * U512::from(*fraction.denom())
        > validators.total_stake * U512::from(*fraction.numer())
}

/// Builds a `BlockSignatures` for `block` from the accumulated V2 finality signatures. Returns
/// `None` if `signatures` has no V2 entry for `block` (should not happen once
/// `is_sufficient_weight` returns `true`, since that check requires at least one such entry to
/// have contributed nonzero weight).
fn build_block_signatures(
    block: &Block,
    signatures: &[FinalitySignature],
) -> Option<BlockSignatures> {
    let first_v2 = signatures.iter().find_map(|fs| match fs {
        FinalitySignature::V2(v2) if v2.block_hash() == block.hash() => Some(v2),
        _ => None,
    })?;
    let mut block_signatures = BlockSignatures::V2(BlockSignaturesV2::new(
        *block.hash(),
        block.height(),
        block.era_id(),
        first_v2.chain_name_hash(),
    ));
    for fs in signatures {
        if let FinalitySignature::V2(v2) = fs
            && v2.block_hash() == block.hash()
        {
            block_signatures.insert_signature(v2.public_key().clone(), *v2.signature());
        }
    }
    Some(block_signatures)
}

/// In-flight (SSE-driven) assembly logic - handling a single event may need several of these
/// cache reads/writes plus a background recheck, so each event handler is its own method rather
/// than being folded into `handle_sidecar_event`'s `match` directly.
impl HeedBinaryPortCache {
    /// Handles a freshly-observed `BlockAdded` SSE event: if `block` turns out to be a switch
    /// block, caches the validator weights it carries for the era that follows it and rechecks
    /// any blocks in that era that were left pending on them.
    async fn handle_block_added(&self, input_block: &Block) -> Result<(), CacheError> {
        let header = input_block.clone_header();
        if let Some(weights) = header.next_era_validator_weights() {
            let next_era_id = header.next_block_era_id();
            let validators = ValidatorsData::new(weights.clone());
            self.put_validators(next_era_id, &validators).await?;
            self.recheck_pending_blocks_for_era(next_era_id, &validators)
                .await;
        }
        // Held for the whole read-decide-write below so a concurrent write for this block -
        // a finality signature, or the read-through cache-aside's `put_block_with_signatures` -
        // can't read a stale value out from under us or have its own write clobbered by ours.
        // See `block_locks`'s docs and `put_block_parts`'s locking precondition.
        let _guard = self.block_locks.lock(input_block.height()).await;
        match self
            .get_block_parts(BlockIdentifier::Height(input_block.height()))
            .await?
        {
            CacheEnvelope::Have(parts) => match parts {
                BlockWithSignaturesBuiltInFlight::NotSureBlock { block, signatures } => {
                    match block {
                        Some(_) => {
                            // Do nothing. We somehow found out about this block prior to this event.
                            Ok(())
                        }
                        None => {
                            let not_sure_with_block =
                                BlockWithSignaturesBuiltInFlight::NotSureBlock {
                                    block: Some(input_block.clone()),
                                    signatures,
                                };
                            self.put_block_parts(&not_sure_with_block).await
                        }
                    }
                }
                BlockWithSignaturesBuiltInFlight::BlockWithSignatures(_) => {
                    // Do nothing, we already know enough about this block.
                    Ok(())
                }
            },
            CacheEnvelope::DontHave => {
                cache_metrics::inc_new_entry("block_added");
                self.put_block_parts(&BlockWithSignaturesBuiltInFlight::NotSureBlock {
                    block: Some(input_block.clone()),
                    signatures: Vec::new(),
                })
                .await
            }
        }
    }

    /// Handles a freshly-observed `ApiVersion` SSE event: fetches and parses the chainspec for
    /// `version`'s `finality_threshold_fraction`, then caches it. A no-op (beyond logging) if the
    /// value is already cached or on any fetch/parse failure - this is best-effort background
    /// enrichment, not something callers wait on.
    async fn cache_finality_threshold_fraction(&self, version: ProtocolVersion) {
        match self.get_finality_threshold_fraction(version).await {
            Ok(CacheEnvelope::Have(_)) => return,
            Ok(_) => {}
            Err(err) => warn!(%err, "binary port cache: get_finality_threshold_fraction failed"),
        }
        let chainspec_bytes = match self.node_client.read_chainspec_bytes().await {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!(%err, "binary port cache: failed to fetch chainspec for finality_threshold_fraction");
                return;
            }
        };
        let fraction = match parse_finality_threshold_fraction(chainspec_bytes.chainspec_bytes()) {
            Ok(fraction) => fraction,
            Err(err) => {
                warn!(%err, "binary port cache: failed to parse finality_threshold_fraction from chainspec");
                return;
            }
        };
        if let Err(err) = self
            .put_finality_threshold_fraction(version, fraction)
            .await
        {
            warn!(%err, "binary port cache: put_finality_threshold_fraction failed");
        }
    }

    async fn handle_transaction_processed(
        &self,
        transaction_hash: TransactionHash,
        execution_result: &ExecutionResult,
    ) -> Result<(), CacheError> {
        cache_metrics::inc_new_entry("transaction_processed");
        self.put_transaction_execution_result(transaction_hash, execution_result)
            .await
    }

    /// Handles a `FinalitySignature` SSE event, incrementally assembling `BlockWithSignatures` in
    /// the persistent cache. See the module-level cache design: a block is stored as
    /// `BlockWithSignaturesBuiltInFlight::NotSureBlock` until enough validator stake has signed
    /// it.
    async fn handle_finality_signature(
        self: &Arc<Self>,
        finality_signature: FinalitySignature,
    ) -> Result<(), CacheError> {
        // V1 signatures don't carry a block height and aren't supported for cache assembly,
        // matching `BlockWithSignaturesBuiltInFlight::block_height_and_hash`'s handling
        // elsewhere in this module.
        let FinalitySignature::V2(v2) = &finality_signature else {
            return Ok(());
        };
        let block_hash = *finality_signature.block_hash();
        let block_height = v2.block_height();
        let era_id = finality_signature.era_id();

        // Held for the whole read-decide-write below (through every `put_block_parts` call in
        // this function) so a concurrent write for the same block - another finality signature,
        // `handle_block_added`, or the read-through cache-aside's `put_block_with_signatures` -
        // can't read a stale value out from under us or have its own write clobbered by ours.
        // See `block_locks`'s docs and `put_block_parts`'s locking precondition.
        let _guard = self.block_locks.lock(block_height).await;

        let (block, mut signatures) = match self
            .get_block_parts(BlockIdentifier::Hash(block_hash))
            .await?
        {
            CacheEnvelope::Have(BlockWithSignaturesBuiltInFlight::BlockWithSignatures(_)) => {
                // Already final - nothing to do.
                return Ok(());
            }
            CacheEnvelope::Have(BlockWithSignaturesBuiltInFlight::NotSureBlock {
                block,
                signatures,
            }) => (block, signatures),
            CacheEnvelope::DontHave => {
                cache_metrics::inc_new_entry("finality_signature");
                (None, Vec::new())
            }
        };
        signatures.push(finality_signature);

        let Some(block) = block else {
            // Don't have the block itself yet - per spec, just persist the signature and stop;
            // we can't determine finality (or even the block's protocol version) without it.
            return self
                .put_block_parts(&BlockWithSignaturesBuiltInFlight::NotSureBlock {
                    block: None,
                    signatures,
                })
                .await;
        };

        let validators = self.get_validators(era_id).await?.into_option();
        let Some(validators) = validators else {
            // Validators for this era aren't known yet - remember this block so it can be
            // rechecked once they are (see `recheck_pending_blocks_for_era`), and kick off a
            // background fetch for them.
            self.add_pending_block_for_era(era_id, block.height())
                .await?;
            self.spawn_validators_fetch_and_recheck(era_id);
            return self
                .put_block_parts(&BlockWithSignaturesBuiltInFlight::NotSureBlock {
                    block: Some(block),
                    signatures,
                })
                .await;
        };

        let updated = self
            .finalize_if_sufficient(block, signatures, &validators)
            .await?;
        self.put_block_parts(&updated).await
    }

    /// Given a known `block` and the `NotSureBlock` signatures accumulated for it so far, checks
    /// whether `validators`' signed weight now clears `block.protocol_version()`'s
    /// `finality_threshold_fraction` and, if so, upgrades to `BlockWithSignatures`. If the
    /// threshold fraction for that protocol version isn't cached yet, this is treated the same as
    /// "not sufficient yet" - the block stays a `NotSureBlock` until it shows up (see
    /// `cache_finality_threshold_fraction`).
    async fn finalize_if_sufficient(
        &self,
        block: Block,
        signatures: Vec<FinalitySignature>,
        validators: &ValidatorsData,
    ) -> Result<BlockWithSignaturesBuiltInFlight, CacheError> {
        let not_sure = |block, signatures| BlockWithSignaturesBuiltInFlight::NotSureBlock {
            block: Some(block),
            signatures,
        };
        let fraction = match self
            .get_finality_threshold_fraction(block.protocol_version())
            .await?
        {
            CacheEnvelope::Have(fraction) => fraction,
            _ => return Ok(not_sure(block, signatures)),
        };
        if !is_sufficient_weight(&block, &signatures, validators, fraction.mul(2)) {
            return Ok(not_sure(block, signatures));
        }
        match build_block_signatures(&block, &signatures) {
            Some(block_signatures) => {
                cache_metrics::inc_blocks_finalized();
                Ok(BlockWithSignaturesBuiltInFlight::BlockWithSignatures(
                    BlockWithSignatures::new(block, block_signatures),
                ))
            }
            None => Ok(not_sure(block, signatures)),
        }
    }

    /// Best-effort background fetch of `era_id`'s validators (via [`InFlightDataHandling::
    /// resolve_validators`], which only resolves the era following the latest switch block - a
    /// hard limitation of the binary port, not new here) followed by a recheck of any blocks left
    /// pending on them.
    fn spawn_validators_fetch_and_recheck(self: &Arc<Self>, era_id: EraId) {
        let this = self.clone();
        tokio::spawn(async move {
            match this.resolve_validators(era_id).await {
                Ok(Some(validators)) => {
                    this.recheck_pending_blocks_for_era(era_id, &validators)
                        .await
                }
                Ok(None) => {}
                Err(err) => {
                    warn!(%err, "binary port cache: failed to fetch validators for era {era_id}")
                }
            }
        });
    }

    /// Re-runs the check for every block height recorded as pending for `era_id` now
    /// that `validators` is known - upgrading any that now clear the threshold from
    /// `NotSureBlock` to `BlockWithSignatures`. Blocks that have enough finality
    /// signature awareness, still unknown, or still insufficient are left untouched
    async fn recheck_pending_blocks_for_era(&self, era_id: EraId, validators: &ValidatorsData) {
        let heights = match self.get_pending_blocks_for_era(era_id).await {
            Ok(CacheEnvelope::Have(heights)) => heights,
            Ok(_) => return,
            Err(err) => {
                warn!(%err, "binary port cache: get_pending_blocks_for_era failed");
                return;
            }
        };
        for height in heights {
            if let Err(err) = self.recheck_block_finality(height, validators).await {
                warn!(%err, "binary port cache: failed to recheck block finality for height {height}");
            }
        }
    }

    async fn recheck_block_finality(
        &self,
        height: u64,
        validators: &ValidatorsData,
    ) -> Result<(), CacheError> {
        // Held for the whole read-decide-write below - see `block_locks`'s docs and
        // `put_block_parts`'s locking precondition. This can run concurrently with
        // `handle_finality_signature`'s own background recheck for the same height (spawned via
        // `spawn_validators_fetch_and_recheck`), so it needs the same protection they do.
        let _guard = self.block_locks.lock(height).await;
        match self
            .get_block_parts(BlockIdentifier::Height(height))
            .await?
        {
            CacheEnvelope::Have(BlockWithSignaturesBuiltInFlight::NotSureBlock {
                block: Some(block),
                signatures,
            }) => {
                let updated = self
                    .finalize_if_sufficient(block, signatures, validators)
                    .await?;
                self.put_block_parts(&updated).await
            }
            // Already final, block still unknown, or no entry at all - nothing to do.
            _ => Ok(()),
        }
    }

    /// Advances `era_horizon` to `era_id - ERA_HORIZON_MARGIN` (saturating, never past 0) if
    /// that's further along than where it already is. Called from the `get_validators`
    /// chokepoint, so the horizon tracks (with a safety margin) the highest era anything has
    /// actually asked about.
    fn note_era_asked(&self, era_id: EraId) {
        let candidate = era_id.value().saturating_sub(ERA_HORIZON_MARGIN);
        self.era_horizon.fetch_max(candidate, Ordering::Relaxed);
    }

    /// Deletes every `blocks_by_era`/`validators_by_era` entry whose era is older than the
    /// current `era_horizon`, i.e. more than [`ERA_HORIZON_MARGIN`] eras behind the highest era
    /// anything has asked about via [`Self::note_era_asked`]. Run periodically by
    /// [`super::prune_loop`] to keep those two tables from growing unboundedly over a
    /// long-running process's lifetime (see `add_pending_block_for_era`'s docs).
    ///
    /// A no-op while `era_horizon` is still at its initial `0` (nothing asked about yet), so a
    /// freshly-started sidecar never prunes eras it hasn't had a chance to learn about.
    pub(crate) async fn prune_old_eras(&self) -> Result<(), CacheError> {
        let horizon = self.era_horizon.load(Ordering::Relaxed);
        if horizon == 0 {
            return Ok(());
        }
        let env = self.env.clone();
        let blocks_by_era = self.blocks_by_era;
        let validators_by_era = self.validators_by_era;
        tokio::task::spawn_blocking(move || -> Result<(), CacheError> {
            let mut wtxn = env.write_txn()?;
            for db in [blocks_by_era, validators_by_era] {
                prune_stale_era_keys(db, &mut wtxn, horizon)?;
            }
            wtxn.commit()?;
            Ok(())
        })
        .await
        .map_err(CacheError::from)?
    }
}

/// Deletes every entry in `db` (a `Database<Bytes, Bytes>` keyed by `EraId::to_bytes()`) whose
/// era is strictly less than `horizon`. Collects the stale keys before deleting rather than
/// deleting while iterating, since `heed`/LMDB cursors don't support mutating the database they're
/// currently iterating over.
fn prune_stale_era_keys(
    db: Database<Bytes, Bytes>,
    wtxn: &mut RwTxn,
    horizon: u64,
) -> Result<(), CacheError> {
    let stale_keys = db
        .iter(wtxn)?
        .filter_map(|entry| entry.ok())
        .filter_map(|(key, _value)| match EraId::from_bytes(key) {
            Ok((era_id, remainder)) if remainder.is_empty() && era_id.value() < horizon => {
                Some(key.to_vec())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    for key in stale_keys {
        db.delete(wtxn, key.as_slice())?;
    }
    Ok(())
}

#[async_trait]
impl InFlightDataHandling for HeedBinaryPortCache {
    async fn handle_sidecar_event(self: Arc<Self>, event: SidecarEvent) -> Result<(), CacheError> {
        match event {
            SidecarEvent::BlockAdded { block } => {
                cache_metrics::inc_handle_call("block_added");
                self.handle_block_added(&block).await
            }
            SidecarEvent::ApiVersion(version) => {
                cache_metrics::inc_handle_call("api_version");
                let this = self.clone();
                tokio::spawn(async move {
                    this.cache_finality_threshold_fraction(version).await;
                });
                Ok(())
            }
            SidecarEvent::FinalitySignature(finality_signature) => {
                cache_metrics::inc_handle_call("finality_signature");
                self.handle_finality_signature(*finality_signature).await
            }
            SidecarEvent::TransactionProcessed {
                transaction_hash,
                execution_result,
                ..
            } => {
                cache_metrics::inc_handle_call("transaction_processed");
                self.handle_transaction_processed(transaction_hash, &execution_result)
                    .await
            }
        }
    }

    async fn resolve_validators(
        &self,
        era_id: EraId,
    ) -> Result<Option<ValidatorsData>, ClientError> {
        match self.get_validators(era_id).await {
            Ok(envelope) => {
                if let Some(validators) = envelope.into_option() {
                    return Ok(Some(validators));
                }
            }
            Err(err) => warn!(%err, "binary port cache: get_validators failed"),
        }
        let Some(validators) =
            validators_from_latest_switch_block(self.node_client.as_ref(), era_id).await?
        else {
            return Ok(None);
        };
        if let Err(err) = self.put_validators(era_id, &validators).await {
            warn!(%err, "binary port cache: put_validators failed");
        }
        Ok(Some(validators))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpcs::test_utils::BinaryPortMock;
    use casper_binary_port::InformationRequest;
    use casper_types::{
        ChainNameDigest, ChainspecRawBytes, EvmTransactionHash, FinalitySignatureV2, PublicKey,
        SecretKey, TestBlockBuilder, Transaction, testing::TestRng,
    };
    use rand::Rng;
    use std::{collections::BTreeMap, time::Duration};

    fn new_store() -> (HeedBinaryPortCache, Arc<BinaryPortMock>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let config = BinaryPortCacheConfig::test_default(dir.path().to_path_buf());
        let node_client = Arc::new(BinaryPortMock::new());
        let store = HeedBinaryPortCache::open(&config, node_client.clone()).unwrap();
        (store, node_client, dir)
    }

    fn new_handler() -> (
        Arc<HeedBinaryPortCache>,
        Arc<BinaryPortMock>,
        tempfile::TempDir,
    ) {
        let (store, node_client, dir) = new_store();
        (Arc::new(store), node_client, dir)
    }

    fn random_block_header(rng: &mut TestRng) -> BlockHeader {
        let block = Block::V2(TestBlockBuilder::new().build(rng));
        block.take_header()
    }

    #[tokio::test]
    async fn block_header_roundtrip_by_hash_and_height() {
        let (store, _node_client, _dir) = new_store();
        let rng = &mut TestRng::new();
        let header = random_block_header(rng);

        assert_eq!(
            store
                .get_block_header(BlockIdentifier::Hash(header.block_hash()))
                .await
                .unwrap(),
            CacheEnvelope::DontHave
        );

        store.put_block_header(&header).await.unwrap();

        assert_eq!(
            store
                .get_block_header(BlockIdentifier::Hash(header.block_hash()))
                .await
                .unwrap(),
            CacheEnvelope::Have(header.clone())
        );
        assert_eq!(
            store
                .get_block_header(BlockIdentifier::Height(header.height()))
                .await
                .unwrap(),
            CacheEnvelope::Have(header)
        );
    }

    #[tokio::test]
    async fn block_with_signatures_roundtrip_by_hash_and_height() {
        let (store, _node_client, _dir) = new_store();
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().build(rng));
        let signatures = BlockSignatures::random(rng);
        let block_with_signatures = BlockWithSignatures::new(block.clone(), signatures);

        store
            .put_block_with_signatures(&block_with_signatures)
            .await
            .unwrap();

        assert_eq!(
            store
                .get_block_with_signatures(BlockIdentifier::Hash(*block.hash()))
                .await
                .unwrap(),
            CacheEnvelope::Have(block_with_signatures.clone())
        );
        assert_eq!(
            store
                .get_block_with_signatures(BlockIdentifier::Height(block.height()))
                .await
                .unwrap(),
            CacheEnvelope::Have(block_with_signatures)
        );
    }

    #[tokio::test]
    async fn tables_for_different_resources_do_not_collide() {
        let (store, _node_client, _dir) = new_store();
        let rng = &mut TestRng::new();
        let header = random_block_header(rng);
        store.put_block_header(&header).await.unwrap();

        assert_eq!(
            store
                .get_block_with_signatures(BlockIdentifier::Hash(header.block_hash()))
                .await
                .unwrap(),
            CacheEnvelope::DontHave
        );
    }

    #[tokio::test]
    async fn transaction_cache_is_keyed_by_finalized_approvals_flag() {
        let (store, _node_client, _dir) = new_store();
        let rng = &mut TestRng::new();
        let transaction = Transaction::random(rng);
        let hash = transaction.hash();
        let with_info = TransactionWithExecutionInfo::new(transaction, None);

        store
            .put_transaction_with_execution_info(hash, true, &with_info)
            .await
            .unwrap();

        assert_eq!(
            store
                .get_transaction_with_execution_info(hash, true)
                .await
                .unwrap(),
            CacheEnvelope::Have(with_info)
        );
        assert_eq!(
            store
                .get_transaction_with_execution_info(hash, false)
                .await
                .unwrap(),
            CacheEnvelope::DontHave
        );
    }

    #[tokio::test]
    async fn persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let config = BinaryPortCacheConfig::test_default(dir.path().to_path_buf());
        let rng = &mut TestRng::new();
        let header = random_block_header(rng);

        {
            let store =
                HeedBinaryPortCache::open(&config, Arc::new(BinaryPortMock::new())).unwrap();
            store.put_block_header(&header).await.unwrap();
        }

        let reopened = HeedBinaryPortCache::open(&config, Arc::new(BinaryPortMock::new())).unwrap();
        assert_eq!(
            reopened
                .get_block_header(BlockIdentifier::Hash(header.block_hash()))
                .await
                .unwrap(),
            CacheEnvelope::Have(header)
        );
    }

    #[tokio::test]
    async fn creates_missing_cache_directory() {
        let dir = tempfile::tempdir().unwrap();
        let nested_path = dir.path().join("nested").join("cache-dir");
        let config = BinaryPortCacheConfig::test_default(nested_path.clone());

        HeedBinaryPortCache::open(&config, Arc::new(BinaryPortMock::new())).unwrap();

        assert!(nested_path.is_dir());
    }

    #[tokio::test]
    async fn validators_roundtrip_by_era_and_are_not_confused_across_eras() {
        let (store, _node_client, _dir) = new_store();
        let rng = &mut TestRng::new();
        let mut validators = BTreeMap::new();
        validators.insert(PublicKey::random(rng), U512::from(100));
        validators.insert(PublicKey::random(rng), U512::from(200));
        let data = ValidatorsData::new(validators);
        let era_id = EraId::from(7);

        assert_eq!(
            store.get_validators(era_id).await.unwrap(),
            CacheEnvelope::DontHave
        );

        store.put_validators(era_id, &data).await.unwrap();

        assert_eq!(
            store.get_validators(era_id).await.unwrap(),
            CacheEnvelope::Have(data)
        );
        assert_eq!(
            store.get_validators(EraId::from(8)).await.unwrap(),
            CacheEnvelope::DontHave
        );
    }

    #[tokio::test]
    async fn block_parts_roundtrip_by_hash_and_height() {
        let (store, _node_client, _dir) = new_store();
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().build(rng));
        let in_flight = BlockWithSignaturesBuiltInFlight::NotSureBlock {
            block: Some(block.clone()),
            signatures: vec![],
        };

        assert!(matches!(
            store
                .get_block_parts(BlockIdentifier::Height(block.height()))
                .await
                .unwrap(),
            CacheEnvelope::DontHave
        ));

        store.put_block_parts(&in_flight).await.unwrap();

        let by_height = store
            .get_block_parts(BlockIdentifier::Height(block.height()))
            .await
            .unwrap();
        assert!(matches!(
            by_height,
            CacheEnvelope::Have(BlockWithSignaturesBuiltInFlight::NotSureBlock {
                block: Some(ref stored_block),
                ..
            }) if stored_block == &block
        ));

        let by_hash = store
            .get_block_parts(BlockIdentifier::Hash(*block.hash()))
            .await
            .unwrap();
        assert!(matches!(
            by_hash,
            CacheEnvelope::Have(BlockWithSignaturesBuiltInFlight::NotSureBlock {
                block: Some(ref stored_block),
                ..
            }) if stored_block == &block
        ));
    }

    #[tokio::test]
    async fn block_with_signatures_and_block_parts_share_storage() {
        let (store, _node_client, _dir) = new_store();
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().build(rng));
        let signatures = BlockSignatures::random(rng);
        let block_with_signatures = BlockWithSignatures::new(block.clone(), signatures);

        // A block still being assembled from SSE events is not a complete
        // `BlockWithSignatures`, so it must not be surfaced by `get_block_with_signatures`.
        let not_sure = BlockWithSignaturesBuiltInFlight::NotSureBlock {
            block: Some(block.clone()),
            signatures: vec![],
        };
        store.put_block_parts(&not_sure).await.unwrap();
        assert_eq!(
            store
                .get_block_with_signatures(BlockIdentifier::Height(block.height()))
                .await
                .unwrap(),
            CacheEnvelope::DontHave
        );

        // Once the full `BlockWithSignatures` is known, it's visible through both accessors,
        // since they share the same underlying table.
        store
            .put_block_with_signatures(&block_with_signatures)
            .await
            .unwrap();
        assert_eq!(
            store
                .get_block_with_signatures(BlockIdentifier::Hash(*block.hash()))
                .await
                .unwrap(),
            CacheEnvelope::Have(block_with_signatures.clone())
        );
        assert!(matches!(
            store
                .get_block_parts(BlockIdentifier::Height(block.height()))
                .await
                .unwrap(),
            CacheEnvelope::Have(BlockWithSignaturesBuiltInFlight::BlockWithSignatures(ref stored))
                if stored == &block_with_signatures
        ));
    }

    #[tokio::test]
    async fn pending_blocks_for_era_append_and_dedup() {
        let (store, _node_client, _dir) = new_store();
        let era_id = EraId::from(3);

        assert!(matches!(
            store.get_pending_blocks_for_era(era_id).await.unwrap(),
            CacheEnvelope::DontHave
        ));

        store.add_pending_block_for_era(era_id, 10).await.unwrap();
        match store.get_pending_blocks_for_era(era_id).await.unwrap() {
            CacheEnvelope::Have(heights) => assert_eq!(heights, vec![10]),
            _ => panic!("expected a cache hit"),
        }

        // appending to an already-populated era adds to the existing vector
        store.add_pending_block_for_era(era_id, 20).await.unwrap();
        match store.get_pending_blocks_for_era(era_id).await.unwrap() {
            CacheEnvelope::Have(heights) => assert_eq!(heights, vec![10, 20]),
            _ => panic!("expected a cache hit"),
        }

        // re-adding an already-present height is a no-op, not a duplicate
        store.add_pending_block_for_era(era_id, 10).await.unwrap();
        match store.get_pending_blocks_for_era(era_id).await.unwrap() {
            CacheEnvelope::Have(heights) => assert_eq!(heights, vec![10, 20]),
            _ => panic!("expected a cache hit"),
        }

        // a different era is unaffected
        assert!(matches!(
            store
                .get_pending_blocks_for_era(EraId::from(4))
                .await
                .unwrap(),
            CacheEnvelope::DontHave
        ));
    }

    /// `prune_old_eras` should leave `blocks_by_era`/`validators_by_era` alone until something
    /// has actually asked about an era (a fresh `era_horizon` of `0` means "don't know enough
    /// yet"), then delete only entries older than `ERA_HORIZON_MARGIN` eras behind whatever was
    /// last asked about, keeping everything within that margin.
    #[tokio::test]
    async fn prune_old_eras_deletes_only_entries_behind_the_horizon() {
        let (store, _node_client, _dir) = new_store();
        let validators = ValidatorsData::new(BTreeMap::new());

        let old_era = EraId::from(5);
        let recent_era = EraId::from(95);
        store.put_validators(old_era, &validators).await.unwrap();
        store.put_validators(recent_era, &validators).await.unwrap();
        store.add_pending_block_for_era(old_era, 1).await.unwrap();
        store
            .add_pending_block_for_era(recent_era, 2)
            .await
            .unwrap();

        // nothing has been asked about yet - horizon is still 0, so pruning is a no-op even
        // though `old_era`'s entries would otherwise be well behind `recent_era`.
        store.prune_old_eras().await.unwrap();
        assert!(matches!(
            store.get_validators(old_era).await.unwrap(),
            CacheEnvelope::Have(_)
        ));

        // `get_validators(old_era)` above already asked about `old_era` (advancing the horizon
        // to `old_era - 10`, i.e. still 0 via saturation), so ask about `recent_era` too -
        // advancing the horizon to `recent_era - ERA_HORIZON_MARGIN` = 85.
        store.get_validators(recent_era).await.unwrap();
        store.prune_old_eras().await.unwrap();

        assert!(matches!(
            store.get_validators(old_era).await.unwrap(),
            CacheEnvelope::DontHave
        ));
        assert!(matches!(
            store.get_pending_blocks_for_era(old_era).await.unwrap(),
            CacheEnvelope::DontHave
        ));
        // `recent_era` (95) is within `ERA_HORIZON_MARGIN` (10) of itself, so it survives.
        assert!(matches!(
            store.get_validators(recent_era).await.unwrap(),
            CacheEnvelope::Have(_)
        ));
        assert!(matches!(
            store.get_pending_blocks_for_era(recent_era).await.unwrap(),
            CacheEnvelope::Have(_)
        ));
    }

    /// Regression test for a lost-update race on `blocks_by_era`: many blocks in the same era
    /// becoming pending at roughly the same time (e.g. several `FinalitySignature`s arriving for
    /// different blocks before that era's validators are known) each read-modify-write the same
    /// era's height list. Without `era_locks` serializing that read-modify-write, two calls that
    /// both read the list before either writes back would each append their own height to the
    /// *same* stale snapshot, and the second commit would silently drop the first one's height.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_add_pending_block_for_era_does_not_lose_updates() {
        let (store, _node_client, _dir) = new_handler();
        let era_id = EraId::from(3);
        const NUM_HEIGHTS: u64 = 64;

        let tasks: Vec<_> = (0..NUM_HEIGHTS)
            .map(|height| {
                let store = store.clone();
                tokio::spawn(async move { store.add_pending_block_for_era(era_id, height).await })
            })
            .collect();
        for task in tasks {
            task.await.unwrap().unwrap();
        }

        let mut heights = match store.get_pending_blocks_for_era(era_id).await.unwrap() {
            CacheEnvelope::Have(heights) => heights,
            CacheEnvelope::DontHave => panic!("expected pending heights to be recorded"),
        };
        heights.sort_unstable();
        assert_eq!(heights, (0..NUM_HEIGHTS).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn finality_threshold_fraction_roundtrip_by_protocol_version() {
        let (store, _node_client, _dir) = new_store();
        let version = ProtocolVersion::from_parts(2, 0, 0);
        let other_version = ProtocolVersion::from_parts(2, 1, 0);
        let fraction = Ratio::new(1u64, 3u64);

        assert!(matches!(
            store
                .get_finality_threshold_fraction(version)
                .await
                .unwrap(),
            CacheEnvelope::DontHave
        ));

        store
            .put_finality_threshold_fraction(version, fraction)
            .await
            .unwrap();

        assert_eq!(
            store
                .get_finality_threshold_fraction(version)
                .await
                .unwrap(),
            CacheEnvelope::Have(fraction)
        );
        assert!(matches!(
            store
                .get_finality_threshold_fraction(other_version)
                .await
                .unwrap(),
            CacheEnvelope::DontHave
        ));
    }

    #[test]
    fn parses_finality_threshold_fraction_from_chainspec_core_section() {
        let toml = br#"
[protocol]
version = "2.0.0"

[core]
finality_threshold_fraction = [1, 3]
"#;
        let fraction = parse_finality_threshold_fraction(toml).unwrap();
        assert_eq!(fraction, Ratio::new(1, 3));
    }

    #[tokio::test]
    async fn resolve_validators_falls_back_to_binary_port_and_then_caches() {
        let (store, node_client, _dir) = new_handler();
        let rng = &mut TestRng::new();

        let era_id = EraId::from(5);
        let next_era_id = era_id.successor();
        let block_v2 = TestBlockBuilder::new()
            .switch_block(true)
            .era(era_id)
            .height(50)
            .build(rng);
        let header = Block::V2(block_v2).take_header();
        let expected_weights = header
            .next_era_validator_weights()
            .expect("switch block header should carry next era validator weights")
            .clone();

        node_client
            .add_block_header_req_res(header, InformationRequest::LatestSwitchBlockHeader)
            .await;

        let validators = store
            .resolve_validators(next_era_id)
            .await
            .unwrap()
            .expect("should resolve validators via the binary port fallback");
        assert_eq!(validators.validators, expected_weights);

        // second call must be served entirely from the persistent cache - no mock response left
        let validators_again = store
            .resolve_validators(next_era_id)
            .await
            .unwrap()
            .expect("should resolve validators via the persistent cache");
        assert_eq!(validators_again.validators, expected_weights);

        assert!(matches!(
            store.get_validators(next_era_id).await.unwrap(),
            CacheEnvelope::Have(_)
        ));
        node_client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn api_version_event_caches_finality_threshold_fraction() {
        let (store, node_client, _dir) = new_handler();

        let version = ProtocolVersion::from_parts(2, 0, 0);
        let chainspec_toml = b"[core]\nfinality_threshold_fraction = [1, 3]\n".to_vec();
        node_client
            .add_chainspec_req_res(ChainspecRawBytes::new(chainspec_toml.into(), None, None))
            .await;

        store
            .clone()
            .handle_sidecar_event(SidecarEvent::ApiVersion(version))
            .await
            .unwrap();

        // the chainspec fetch runs on a spawned background task
        let mut num_of_tries = 20;
        while !matches!(
            store
                .get_finality_threshold_fraction(version)
                .await
                .unwrap(),
            CacheEnvelope::Have(_)
        ) {
            num_of_tries -= 1;
            assert!(
                num_of_tries > 0,
                "finality_threshold_fraction was never cached"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert_eq!(
            store
                .get_finality_threshold_fraction(version)
                .await
                .unwrap(),
            CacheEnvelope::Have(Ratio::new(1, 3))
        );
        node_client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn transaction_processed_event_caches_execution_result_without_a_node_fetch() {
        let (store, node_client, _dir) = new_handler();
        let rng = &mut TestRng::new();

        let transaction_hash = TransactionHash::from(EvmTransactionHash::from_raw(rng.r#gen()));
        let block_hash = casper_types::BlockHash::random(rng);
        let execution_result = casper_types::execution::ExecutionResult::random(rng);

        // no request/response is registered on `node_client` - the event alone must be enough.
        store
            .clone()
            .handle_sidecar_event(SidecarEvent::TransactionProcessed {
                transaction_hash,
                block_hash,
                execution_result: Arc::new(execution_result.clone()),
            })
            .await
            .unwrap();

        assert_eq!(
            store
                .get_transaction_execution_result(transaction_hash)
                .await
                .unwrap(),
            CacheEnvelope::Have(execution_result)
        );
        // the general-purpose (transaction-plus-execution-info) cache is untouched - the event
        // doesn't carry a full transaction body to populate it with.
        assert_eq!(
            store
                .get_transaction_with_execution_info(transaction_hash, true)
                .await
                .unwrap(),
            CacheEnvelope::DontHave
        );
        node_client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn finality_signatures_accumulate_and_finalize_once_weight_is_sufficient() {
        let (store, node_client, _dir) = new_handler();
        let rng = &mut TestRng::new();

        let era_id = EraId::from(5);
        let block_v2 = TestBlockBuilder::new().era(era_id).height(50).build(rng);
        let block = Block::V2(block_v2);

        // validator A holds far too little stake alone to clear a 1/3 threshold; A + B together
        // clear it easily
        let secret_a = SecretKey::random(rng);
        let secret_b = SecretKey::random(rng);
        let public_a = PublicKey::from(&secret_a);
        let public_b = PublicKey::from(&secret_b);
        let mut weights = BTreeMap::new();
        weights.insert(public_a, U512::from(1));
        weights.insert(public_b, U512::from(8));
        let validators = ValidatorsData::new(weights);

        store.put_validators(era_id, &validators).await.unwrap();
        store
            .put_finality_threshold_fraction(block.protocol_version(), Ratio::new(1, 3))
            .await
            .unwrap();
        store
            .put_block_parts(&BlockWithSignaturesBuiltInFlight::NotSureBlock {
                block: Some(block.clone()),
                signatures: vec![],
            })
            .await
            .unwrap();

        let chain_name_hash = ChainNameDigest::random(rng);
        let fs_a = FinalitySignatureV2::create(
            *block.hash(),
            block.height(),
            era_id,
            chain_name_hash,
            &secret_a,
        );
        store
            .clone()
            .handle_sidecar_event(SidecarEvent::FinalitySignature(Box::new(
                FinalitySignature::V2(fs_a),
            )))
            .await
            .unwrap();

        // A alone (weight 1 of 9) doesn't clear a 1/3 threshold - still not final
        match store
            .get_block_parts(BlockIdentifier::Height(block.height()))
            .await
            .unwrap()
        {
            CacheEnvelope::Have(BlockWithSignaturesBuiltInFlight::NotSureBlock {
                block: Some(_),
                signatures,
            }) => assert_eq!(signatures.len(), 1),
            _ => panic!("expected the block to still be unresolved"),
        }

        let fs_b = FinalitySignatureV2::create(
            *block.hash(),
            block.height(),
            era_id,
            chain_name_hash,
            &secret_b,
        );
        store
            .clone()
            .handle_sidecar_event(SidecarEvent::FinalitySignature(Box::new(
                FinalitySignature::V2(fs_b),
            )))
            .await
            .unwrap();

        // A + B (weight 9 of 9) clears the threshold
        match store
            .get_block_parts(BlockIdentifier::Height(block.height()))
            .await
            .unwrap()
        {
            CacheEnvelope::Have(BlockWithSignaturesBuiltInFlight::BlockWithSignatures(bws)) => {
                assert_eq!(bws.block(), &block);
                assert_eq!(bws.block_signatures().len(), 2);
            }
            _ => panic!("expected the block to be finalized"),
        }
        // the shared table means the "complete" accessor sees it too
        assert!(matches!(
            store
                .get_block_with_signatures(BlockIdentifier::Height(block.height()))
                .await
                .unwrap(),
            CacheEnvelope::Have(_)
        ));

        node_client.verify_no_lingering().await;
    }

    /// Regression test: `is_sufficient_weight` used to dedup signers by *stake value* (via a
    /// `BTreeSet<&U512>`) rather than by public key, so two distinct validators who happen to
    /// hold equal stake collapsed into a single counted weight. Here A, B and C each hold equal
    /// stake and together clear a 1/3 threshold (3 of 3), but any one alone does not (1 of 3) -
    /// so if the bug were still present, deduping A's and B's equal weights down to one would
    /// leave the block permanently unresolved even after all three sign.
    #[tokio::test]
    async fn finality_signatures_from_equal_weight_validators_all_count() {
        let (store, node_client, _dir) = new_handler();
        let rng = &mut TestRng::new();

        let era_id = EraId::from(5);
        let block_v2 = TestBlockBuilder::new().era(era_id).height(50).build(rng);
        let block = Block::V2(block_v2);

        let secret_a = SecretKey::random(rng);
        let secret_b = SecretKey::random(rng);
        let secret_c = SecretKey::random(rng);
        let public_a = PublicKey::from(&secret_a);
        let public_b = PublicKey::from(&secret_b);
        let public_c = PublicKey::from(&secret_c);
        let mut weights = BTreeMap::new();
        weights.insert(public_a, U512::from(1));
        weights.insert(public_b, U512::from(1));
        weights.insert(public_c, U512::from(1));
        let validators = ValidatorsData::new(weights);

        store.put_validators(era_id, &validators).await.unwrap();
        store
            .put_finality_threshold_fraction(block.protocol_version(), Ratio::new(1, 3))
            .await
            .unwrap();
        store
            .put_block_parts(&BlockWithSignaturesBuiltInFlight::NotSureBlock {
                block: Some(block.clone()),
                signatures: vec![],
            })
            .await
            .unwrap();

        let chain_name_hash = ChainNameDigest::random(rng);
        for secret in [&secret_a, &secret_b, &secret_c] {
            let fs = FinalitySignatureV2::create(
                *block.hash(),
                block.height(),
                era_id,
                chain_name_hash,
                secret,
            );
            store
                .clone()
                .handle_sidecar_event(SidecarEvent::FinalitySignature(Box::new(
                    FinalitySignature::V2(fs),
                )))
                .await
                .unwrap();
        }

        match store
            .get_block_parts(BlockIdentifier::Height(block.height()))
            .await
            .unwrap()
        {
            CacheEnvelope::Have(BlockWithSignaturesBuiltInFlight::BlockWithSignatures(bws)) => {
                assert_eq!(bws.block(), &block);
                assert_eq!(bws.block_signatures().len(), 3);
            }
            _ => panic!("expected the block to be finalized"),
        }

        node_client.verify_no_lingering().await;
    }

    /// Regression test for the race between the SSE-driven assembly path and the read-through
    /// cache-aside populate in `CachingNodeClient::read_block_with_signatures` (which calls
    /// `put_block_with_signatures` on a miss): both can be mid-flight for the same block at once
    /// - one reading the current `NotSureBlock`, doing async work, and writing back; the other
    /// unconditionally overwriting with a freshly node-fetched `BlockWithSignatures`. Without
    /// `block_locks` serializing the two, whichever finishes last wins regardless of which one
    /// actually has more complete information, so the SSE path can clobber a just-arrived
    /// complete block back down to `NotSureBlock`. The fix guarantees the opposite: no matter
    /// which of the two runs "first", the block must end up fully known afterwards.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_write_through_and_sse_assembly_never_downgrades_a_finalized_block() {
        let (store, node_client, _dir) = new_handler();
        let rng = &mut TestRng::new();

        let era_id = EraId::from(5);
        let block_v2 = TestBlockBuilder::new().era(era_id).height(50).build(rng);
        let block = Block::V2(block_v2);
        let block_with_signatures =
            BlockWithSignatures::new(block.clone(), BlockSignatures::random(rng));

        // Seed the cache the way the SSE path would have it mid-assembly: block known, no
        // signatures yet.
        store
            .put_block_parts(&BlockWithSignaturesBuiltInFlight::NotSureBlock {
                block: Some(block.clone()),
                signatures: vec![],
            })
            .await
            .unwrap();

        // The signature event drives `handle_finality_signature` into its
        // validators-unknown branch, which spawns a background validators fetch; give it a
        // switch block header for an unrelated era so that fetch resolves cleanly instead of
        // panicking on an empty mock queue (mirrors
        // `deferred_validators_recheck_upgrades_pending_block_once_known`).
        let unrelated_header = Block::V2(
            TestBlockBuilder::new()
                .switch_block(true)
                .era(EraId::from(1))
                .height(1)
                .build(rng),
        )
        .take_header();
        node_client
            .add_block_header_req_res(
                unrelated_header,
                InformationRequest::LatestSwitchBlockHeader,
            )
            .await;

        let chain_name_hash = ChainNameDigest::random(rng);
        let secret_key = SecretKey::random(rng);
        let fs = FinalitySignatureV2::create(
            *block.hash(),
            block.height(),
            era_id,
            chain_name_hash,
            &secret_key,
        );

        // Race the two writers against each other for real, on a multi-threaded runtime.
        let sse_store = store.clone();
        let sse_task = tokio::spawn(async move {
            sse_store
                .handle_sidecar_event(SidecarEvent::FinalitySignature(Box::new(
                    FinalitySignature::V2(fs),
                )))
                .await
        });
        let write_through_store = store.clone();
        let bws = block_with_signatures.clone();
        let write_through_task =
            tokio::spawn(async move { write_through_store.put_block_with_signatures(&bws).await });

        sse_task.await.unwrap().unwrap();
        write_through_task.await.unwrap().unwrap();

        match store
            .get_block_parts(BlockIdentifier::Height(block.height()))
            .await
            .unwrap()
        {
            CacheEnvelope::Have(BlockWithSignaturesBuiltInFlight::BlockWithSignatures(bws)) => {
                assert_eq!(bws.block(), &block);
            }
            CacheEnvelope::Have(BlockWithSignaturesBuiltInFlight::NotSureBlock { .. }) => {
                panic!("block was downgraded back to NotSureBlock by the race")
            }
            CacheEnvelope::DontHave => panic!("block was lost entirely by the race"),
        }

        // Whether the mocked validators response above is ever consumed depends on which of the
        // two racing writes lands first: if `put_block_with_signatures` wins,
        // `handle_finality_signature` sees an already-final block and returns immediately
        // without spawning the background fetch at all. Either way is a legitimate outcome, so
        // this intentionally doesn't assert on `node_client`'s leftover mock state - only the
        // invariant checked above (never downgraded) matters here.
    }

    #[tokio::test]
    async fn finality_signature_before_block_known_is_stored_without_block() {
        let (store, node_client, _dir) = new_handler();
        let rng = &mut TestRng::new();

        let era_id = EraId::from(9);
        let block_hash = casper_types::BlockHash::random(rng);
        let block_height = 123u64;
        let secret_key = SecretKey::random(rng);
        let chain_name_hash = ChainNameDigest::random(rng);
        let fs = FinalitySignatureV2::create(
            block_hash,
            block_height,
            era_id,
            chain_name_hash,
            &secret_key,
        );

        // no validators are seeded or mocked: without the block itself, per spec, this should
        // only persist the signature and never try to look validators up
        store
            .clone()
            .handle_sidecar_event(SidecarEvent::FinalitySignature(Box::new(
                FinalitySignature::V2(fs),
            )))
            .await
            .unwrap();

        match store
            .get_block_parts(BlockIdentifier::Hash(block_hash))
            .await
            .unwrap()
        {
            CacheEnvelope::Have(BlockWithSignaturesBuiltInFlight::NotSureBlock {
                block: None,
                signatures,
            }) => assert_eq!(signatures.len(), 1),
            _ => panic!("expected the signature to be stored without a known block"),
        }
        assert!(matches!(
            store
                .get_block_parts(BlockIdentifier::Height(block_height))
                .await
                .unwrap(),
            CacheEnvelope::Have(BlockWithSignaturesBuiltInFlight::NotSureBlock { block: None, .. })
        ));

        node_client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn deferred_validators_recheck_upgrades_pending_block_once_known() {
        let (store, node_client, _dir) = new_handler();
        let rng = &mut TestRng::new();

        let era_id = EraId::from(5);
        let block_v2 = TestBlockBuilder::new().era(era_id).height(50).build(rng);
        let block = Block::V2(block_v2);
        store
            .put_finality_threshold_fraction(block.protocol_version(), Ratio::new(1, 3))
            .await
            .unwrap();
        store
            .put_block_parts(&BlockWithSignaturesBuiltInFlight::NotSureBlock {
                block: Some(block.clone()),
                signatures: vec![],
            })
            .await
            .unwrap();

        // validators for `era_id` are unknown, so the signature below triggers an automatic
        // background fetch; give it a switch block header for an unrelated era so it resolves
        // to "no validators found" instead of hitting an empty mock queue
        let unrelated_header = Block::V2(
            TestBlockBuilder::new()
                .switch_block(true)
                .era(EraId::from(1))
                .height(1)
                .build(rng),
        )
        .take_header();
        node_client
            .add_block_header_req_res(
                unrelated_header,
                InformationRequest::LatestSwitchBlockHeader,
            )
            .await;

        let secret_key = SecretKey::random(rng);
        let public_key = PublicKey::from(&secret_key);
        let chain_name_hash = ChainNameDigest::random(rng);
        let fs = FinalitySignatureV2::create(
            *block.hash(),
            block.height(),
            era_id,
            chain_name_hash,
            &secret_key,
        );
        store
            .clone()
            .handle_sidecar_event(SidecarEvent::FinalitySignature(Box::new(
                FinalitySignature::V2(fs),
            )))
            .await
            .unwrap();

        // the height should be recorded as pending right away - `add_pending_block_for_era` runs
        // synchronously before the background validators fetch is spawned
        match store.get_pending_blocks_for_era(era_id).await.unwrap() {
            CacheEnvelope::Have(heights) => assert_eq!(heights, vec![block.height()]),
            other => panic!("expected the height to be recorded as pending, got {other:?}"),
        }
        match store
            .get_block_parts(BlockIdentifier::Height(block.height()))
            .await
            .unwrap()
        {
            CacheEnvelope::Have(BlockWithSignaturesBuiltInFlight::NotSureBlock {
                block: Some(_),
                ..
            }) => {}
            _ => panic!("expected the block to still be unresolved"),
        }
        // give the background fetch spawned above time to actually drain its one mock
        // response before the switch-block flow below queues and expects a different one
        tokio::time::sleep(Duration::from_millis(150)).await;

        // validators for `era_id` become known via a switch block for the *previous* era,
        // without ever sending another `FinalitySignature` event
        let previous_era = EraId::from(era_id.value() - 1);
        let mut weights = BTreeMap::new();
        weights.insert(public_key, U512::from(100));
        let switch_block_v2 = TestBlockBuilder::new()
            .switch_block(true)
            .era(previous_era)
            .height(block.height() - 1)
            .validator_weights(weights)
            .build(rng);
        let switch_block = Block::V2(switch_block_v2);
        store
            .clone()
            .handle_sidecar_event(SidecarEvent::BlockAdded {
                block: Arc::new(switch_block.clone()),
            })
            .await
            .unwrap();

        let mut num_of_tries = 20;
        while !matches!(
            store
                .get_block_parts(BlockIdentifier::Height(block.height()))
                .await
                .unwrap(),
            CacheEnvelope::Have(BlockWithSignaturesBuiltInFlight::BlockWithSignatures(_))
        ) {
            num_of_tries -= 1;
            assert!(
                num_of_tries > 0,
                "pending block was never upgraded after validators became known"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        node_client.verify_no_lingering().await;
    }
}
