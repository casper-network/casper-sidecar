use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::{fmt, iter};

use derive_more::Into;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::deploy::{Approval, DeployHash};
use crate::digest::Digest;
use casper_types::bytesrepr::ToBytes;
use casper_types::testing::TestRng;
use casper_types::{
    bytesrepr, EraId, ProtocolVersion, PublicKey, SecretKey, Signature, Timestamp, U512,
};
#[cfg(feature = "sse-data-testing")]
use rand::{Rng, RngCore};

const BLOCK_REWARD: u64 = 1_000_000_000_000;

/// An error that can arise when creating a block from a finalized block and other components
#[derive(Error, Debug)]
pub enum BlockCreationError {
    /// `EraEnd`s need both an `EraReport` present and a map of the next era validator weights.
    /// If one of them is not present while trying to construct an `EraEnd` we must emit an
    /// error.
    #[error(
        "Cannot create EraEnd unless we have both an EraReport and next era validators. \
         Era report: {maybe_era_report:?}, \
         Next era validator weights: {maybe_next_era_validator_weights:?}"
    )]
    CouldNotCreateEraEnd {
        /// An optional `EraReport` we tried to use to construct an `EraEnd`
        maybe_era_report: Option<EraReport>,
        /// An optional map of the next era validator weights used to construct an `EraEnd`
        maybe_next_era_validator_weights: Option<BTreeMap<PublicKey, U512>>,
    },
}

/// A cryptographic hash identifying a [`Block`](struct.Block.html).
#[derive(
    Copy, Clone, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug, Into,
)]
#[serde(deny_unknown_fields)]
pub struct BlockHash(Digest);

impl BlockHash {
    /// Constructs a new `BlockHash`.
    pub fn new(hash: Digest) -> Self {
        BlockHash(hash)
    }

    /// Returns the wrapped inner hash.
    pub fn inner(&self) -> &Digest {
        &self.0
    }

    /// Creates a random block hash.
    pub fn random(rng: &mut TestRng) -> Self {
        let hash = rng.gen::<[u8; Digest::LENGTH]>().into();
        BlockHash(hash)
    }
}

impl ToBytes for BlockHash {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        self.0.to_bytes()
    }

    fn serialized_length(&self) -> usize {
        self.0.serialized_length()
    }
}

impl Display for BlockHash {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "block-hash({})", self.0,)
    }
}

/// The header portion of a [`Block`](struct.Block.html).
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct BlockHeader {
    parent_hash: BlockHash,
    state_root_hash: Digest,
    body_hash: Digest,
    random_bit: bool,
    accumulated_seed: Digest,
    era_end: Option<EraEnd>,
    timestamp: Timestamp,
    era_id: EraId,
    height: u64,
    protocol_version: ProtocolVersion,
}

impl BlockHeader {
    pub fn hash(&self) -> BlockHash {
        let serialized_header = Self::serialize(self)
            .unwrap_or_else(|error| panic!("should serialize block header: {}", error));
        BlockHash::new(Digest::hash(&serialized_header))
    }

    // Serialize the block header.
    fn serialize(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        self.to_bytes()
    }
}

impl ToBytes for BlockHeader {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        let mut buffer = bytesrepr::allocate_buffer(self)?;
        buffer.extend(self.parent_hash.to_bytes()?);
        buffer.extend(self.state_root_hash.to_bytes()?);
        buffer.extend(self.body_hash.to_bytes()?);
        buffer.extend(self.random_bit.to_bytes()?);
        buffer.extend(self.accumulated_seed.to_bytes()?);
        buffer.extend(self.era_end.to_bytes()?);
        buffer.extend(self.timestamp.to_bytes()?);
        buffer.extend(self.era_id.to_bytes()?);
        buffer.extend(self.height.to_bytes()?);
        buffer.extend(self.protocol_version.to_bytes()?);
        Ok(buffer)
    }

    fn serialized_length(&self) -> usize {
        self.parent_hash.serialized_length()
            + self.state_root_hash.serialized_length()
            + self.body_hash.serialized_length()
            + self.random_bit.serialized_length()
            + self.accumulated_seed.serialized_length()
            + self.era_end.serialized_length()
            + self.timestamp.serialized_length()
            + self.era_id.serialized_length()
            + self.height.serialized_length()
            + self.protocol_version.serialized_length()
    }
}

/// The hash of a deploy (or transfer) together with signatures approving it for execution.
#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeployWithApprovals {
    /// The hash of the deploy.
    deploy_hash: DeployHash,
    /// The approvals for the deploy.
    approvals: BTreeSet<Approval>,
}

impl DeployWithApprovals {
    /// Creates a new `DeployWithApprovals`.
    pub fn new(deploy_hash: DeployHash, approvals: BTreeSet<Approval>) -> Self {
        Self {
            deploy_hash,
            approvals,
        }
    }

    /// Returns the deploy hash.
    pub fn deploy_hash(&self) -> &DeployHash {
        &self.deploy_hash
    }

    /// Returns the approvals.
    pub fn approvals(&self) -> &BTreeSet<Approval> {
        &self.approvals
    }
}

#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct BlockPayload {
    deploys: Vec<DeployWithApprovals>,
    transfers: Vec<DeployWithApprovals>,
    accusations: Vec<PublicKey>,
    random_bit: bool,
}

impl BlockPayload {
    pub(crate) fn new(
        deploys: Vec<DeployWithApprovals>,
        transfers: Vec<DeployWithApprovals>,
        accusations: Vec<PublicKey>,
        random_bit: bool,
    ) -> Self {
        BlockPayload {
            deploys,
            transfers,
            accusations,
            random_bit,
        }
    }

    /// An iterator over deploy hashes included in the block, excluding transfers.
    pub(crate) fn deploy_hashes(&self) -> impl Iterator<Item = &DeployHash> + Clone {
        self.deploys.iter().map(|dwa| dwa.deploy_hash())
    }

    /// An iterator over transfer hashes included in the block.
    pub(crate) fn transfer_hashes(&self) -> impl Iterator<Item = &DeployHash> + Clone {
        self.transfers.iter().map(|dwa| dwa.deploy_hash())
    }
}

/// The piece of information that will become the content of a future block after it was finalized
/// and before execution happened yet.
#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FinalizedBlock {
    deploy_hashes: Vec<DeployHash>,
    transfer_hashes: Vec<DeployHash>,
    timestamp: Timestamp,
    random_bit: bool,
    era_report: Option<EraReport>,
    era_id: EraId,
    height: u64,
    proposer: PublicKey,
}

impl FinalizedBlock {
    pub(crate) fn new(
        block_payload: BlockPayload,
        era_report: Option<EraReport>,
        timestamp: Timestamp,
        era_id: EraId,
        height: u64,
        proposer: PublicKey,
    ) -> Self {
        FinalizedBlock {
            deploy_hashes: block_payload.deploy_hashes().cloned().collect(),
            transfer_hashes: block_payload.transfer_hashes().cloned().collect(),
            timestamp,
            random_bit: block_payload.random_bit,
            era_report,
            era_id,
            height,
            proposer,
        }
    }

    /// Generates a random instance using a `TestRng`.
    pub fn random(rng: &mut TestRng) -> Self {
        let era = rng.gen_range(0..5);
        let height = era * 10 + rng.gen_range(0..10);
        let is_switch = rng.gen_bool(0.1);

        FinalizedBlock::random_with_specifics(rng, EraId::from(era), height, is_switch)
    }

    /// Generates a random instance using a `TestRng`, but using the specified era ID and height.
    pub fn random_with_specifics(
        rng: &mut TestRng,
        era_id: EraId,
        height: u64,
        is_switch: bool,
    ) -> Self {
        let deploy_count = rng.gen_range(0..11);
        let deploys = iter::repeat_with(|| {
            DeployWithApprovals::new(
                DeployHash::new(rng.gen::<[u8; Digest::LENGTH]>().into()),
                BTreeSet::new(),
            )
        })
        .take(deploy_count)
        .collect();
        let random_bit = rng.gen();
        // TODO - make Timestamp deterministic.
        let timestamp = Timestamp::now();
        let block_payload = BlockPayload::new(deploys, vec![], vec![], random_bit);

        let era_report = if is_switch {
            let equivocators_count = rng.gen_range(0..5);
            let rewards_count = rng.gen_range(0..5);
            let inactive_count = rng.gen_range(0..5);
            Some(EraReport {
                equivocators: iter::repeat_with(|| {
                    PublicKey::from(&SecretKey::ed25519_from_bytes(rng.gen::<[u8; 32]>()).unwrap())
                })
                .take(equivocators_count)
                .collect(),
                rewards: iter::repeat_with(|| {
                    let pub_key = PublicKey::from(
                        &SecretKey::ed25519_from_bytes(rng.gen::<[u8; 32]>()).unwrap(),
                    );
                    let reward = rng.gen_range(1..(BLOCK_REWARD + 1));
                    (pub_key, reward)
                })
                .take(rewards_count)
                .collect(),
                inactive_validators: iter::repeat_with(|| {
                    PublicKey::from(&SecretKey::ed25519_from_bytes(rng.gen::<[u8; 32]>()).unwrap())
                })
                .take(inactive_count)
                .collect(),
            })
        } else {
            None
        };
        let secret_key: SecretKey = SecretKey::ed25519_from_bytes(rng.gen::<[u8; 32]>()).unwrap();
        let public_key = PublicKey::from(&secret_key);

        FinalizedBlock::new(
            block_payload,
            era_report,
            timestamp,
            era_id,
            height,
            public_key,
        )
    }
}

#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Block {
    hash: BlockHash,
    pub header: BlockHeader,
    body: BlockBody,
}

impl Block {
    fn hash_block_body(block_body: &BlockBody) -> Digest {
        block_body.hash()
    }

    pub(crate) fn new(
        parent_hash: BlockHash,
        parent_seed: Digest,
        state_root_hash: Digest,
        finalized_block: FinalizedBlock,
        next_era_validator_weights: Option<BTreeMap<PublicKey, U512>>,
        protocol_version: ProtocolVersion,
    ) -> Result<Self, BlockCreationError> {
        let body = BlockBody::new(
            finalized_block.proposer.clone(),
            finalized_block.deploy_hashes,
            finalized_block.transfer_hashes,
        );

        let body_hash = Self::hash_block_body(&body);

        let era_end = match (finalized_block.era_report, next_era_validator_weights) {
            (None, None) => None,
            (Some(era_report), Some(next_era_validator_weights)) => {
                Some(EraEnd::new(era_report, next_era_validator_weights))
            }
            (maybe_era_report, maybe_next_era_validator_weights) => {
                return Err(BlockCreationError::CouldNotCreateEraEnd {
                    maybe_era_report,
                    maybe_next_era_validator_weights,
                })
            }
        };

        let accumulated_seed = Digest::hash_pair(parent_seed, [finalized_block.random_bit as u8]);

        let header = BlockHeader {
            parent_hash,
            state_root_hash,
            body_hash,
            random_bit: finalized_block.random_bit,
            accumulated_seed,
            era_end,
            timestamp: finalized_block.timestamp,
            era_id: finalized_block.era_id,
            height: finalized_block.height,
            protocol_version,
        };

        Ok(Block {
            hash: header.hash(),
            header,
            body,
        })
    }

    /// The hash of this block's header.
    pub fn hash(&self) -> &BlockHash {
        &self.hash
    }

    pub fn random(rng: &mut TestRng) -> Self {
        let era = rng.gen_range(1..6);
        let height = era * 10 + rng.gen_range(0..10);
        let is_switch = rng.gen_bool(0.1);

        Block::random_with_specifics(
            rng,
            EraId::from(era),
            height,
            ProtocolVersion::V1_0_0,
            is_switch,
        )
    }

    /// Generates a random instance using a `TestRng`, but using the specified values.
    pub fn random_with_specifics(
        rng: &mut TestRng,
        era_id: EraId,
        height: u64,
        protocol_version: ProtocolVersion,
        is_switch: bool,
    ) -> Self {
        let parent_hash = BlockHash::new(rng.gen::<[u8; Digest::LENGTH]>().into());
        let state_root_hash = rng.gen::<[u8; Digest::LENGTH]>().into();
        let finalized_block = FinalizedBlock::random_with_specifics(rng, era_id, height, is_switch);
        let parent_seed = rng.gen::<[u8; Digest::LENGTH]>().into();
        let next_era_validator_weights = finalized_block
            .era_report
            .as_ref()
            .map(|_| BTreeMap::<PublicKey, U512>::default());

        Block::new(
            parent_hash,
            parent_seed,
            state_root_hash,
            finalized_block,
            next_era_validator_weights,
            protocol_version,
        )
        .expect("Could not create random block with specifics")
    }
}

impl ToBytes for Block {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        let mut buffer = bytesrepr::allocate_buffer(self)?;
        buffer.extend(self.hash.to_bytes()?);
        buffer.extend(self.header.to_bytes()?);
        buffer.extend(self.body.to_bytes()?);
        Ok(buffer)
    }

    fn serialized_length(&self) -> usize {
        self.hash.serialized_length()
            + self.header.serialized_length()
            + self.body.serialized_length()
    }
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct BlockBody {
    proposer: PublicKey,
    deploy_hashes: Vec<DeployHash>,
    transfer_hashes: Vec<DeployHash>,
}

impl BlockBody {
    /// Creates a new body from deploy and transfer hashes.
    pub(crate) fn new(
        proposer: PublicKey,
        deploy_hashes: Vec<DeployHash>,
        transfer_hashes: Vec<DeployHash>,
    ) -> Self {
        BlockBody {
            proposer,
            deploy_hashes,
            transfer_hashes,
        }
    }

    pub fn hash(&self) -> Digest {
        let serialized_body = self
            .to_bytes()
            .unwrap_or_else(|error| panic!("should serialize block body: {}", error));
        Digest::hash(&serialized_body)
    }

    /// Block proposer.
    pub fn proposer(&self) -> &PublicKey {
        &self.proposer
    }

    /// Retrieves the deploy hashes within the block.
    pub(crate) fn deploy_hashes(&self) -> &Vec<DeployHash> {
        &self.deploy_hashes
    }

    /// Retrieves the transfer hashes within the block.
    pub(crate) fn transfer_hashes(&self) -> &Vec<DeployHash> {
        &self.transfer_hashes
    }
}

impl ToBytes for BlockBody {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        let mut buffer = bytesrepr::allocate_buffer(self)?;
        buffer.extend(self.proposer.to_bytes()?);
        buffer.extend(self.deploy_hashes.to_bytes()?);
        buffer.extend(self.transfer_hashes.to_bytes()?);
        Ok(buffer)
    }

    fn serialized_length(&self) -> usize {
        self.proposer.serialized_length()
            + self.deploy_hashes.serialized_length()
            + self.transfer_hashes.serialized_length()
    }
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct EraReport {
    pub(crate) equivocators: Vec<PublicKey>,
    pub(crate) rewards: BTreeMap<PublicKey, u64>,
    pub(crate) inactive_validators: Vec<PublicKey>,
}

impl ToBytes for EraReport {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        let mut buffer = bytesrepr::allocate_buffer(self)?;
        buffer.extend(self.equivocators.to_bytes()?);
        buffer.extend(self.rewards.to_bytes()?);
        buffer.extend(self.inactive_validators.to_bytes()?);
        Ok(buffer)
    }

    fn serialized_length(&self) -> usize {
        self.equivocators.serialized_length()
            + self.rewards.serialized_length()
            + self.inactive_validators.serialized_length()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FinalitySignature {
    /// Hash of a block this signature is for.
    pub block_hash: BlockHash,
    /// Era in which the block was created in.
    pub era_id: EraId,
    /// Signature over the block hash.
    pub signature: Signature,
    /// Public key of the signing validator.
    pub public_key: PublicKey,
}

// #TODO -> replace this with SecretKey::random once casper-types exposes test code
pub fn random_secret_key(rng: &mut TestRng) -> SecretKey {
    let mut bytes = [0u8; 32];
    rng.fill_bytes(&mut bytes[..]);
    SecretKey::ed25519_from_bytes(bytes).unwrap()
}

impl FinalitySignature {
    pub fn random_for_block(block_hash: BlockHash, era_id: u64, test_rng: &mut TestRng) -> Self {
        let mut bytes = block_hash.inner().into_vec();
        bytes.extend_from_slice(&era_id.to_le_bytes());

        let (_, pub_key) = {
            let secret_key = random_secret_key(test_rng);
            let public_key = PublicKey::from(&secret_key);
            (secret_key, public_key)
        };
        let signature = Signature::System;

        FinalitySignature::new(block_hash, EraId::new(era_id), signature, pub_key)
    }

    pub fn new(
        block_hash: BlockHash,
        era_id: EraId,
        signature: Signature,
        public_key: PublicKey,
    ) -> Self {
        FinalitySignature {
            block_hash,
            era_id,
            signature,
            public_key,
        }
    }
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
/// A struct to contain information related to the end of an era and validator weights for the
/// following era.
pub struct EraEnd {
    /// The era end information.
    era_report: EraReport,
    /// The validator weights for the next era.
    next_era_validator_weights: BTreeMap<PublicKey, U512>,
}

impl EraEnd {
    pub fn new(
        era_report: EraReport,
        next_era_validator_weights: BTreeMap<PublicKey, U512>,
    ) -> Self {
        EraEnd {
            era_report,
            next_era_validator_weights,
        }
    }

    pub fn era_report(&self) -> &EraReport {
        &self.era_report
    }
}

impl ToBytes for EraEnd {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        let mut buffer = bytesrepr::allocate_buffer(self)?;
        buffer.extend(self.era_report.to_bytes()?);
        buffer.extend(self.next_era_validator_weights.to_bytes()?);
        Ok(buffer)
    }

    fn serialized_length(&self) -> usize {
        self.era_report.serialized_length() + self.next_era_validator_weights.serialized_length()
    }
}

/// A storage representation of finality signatures with the associated block hash.
#[derive(Clone, Debug, PartialOrd, Ord, Hash, Serialize, Deserialize, Eq, PartialEq)]
pub struct BlockSignatures {
    /// The block hash for a given block.
    pub(crate) block_hash: BlockHash,
    /// The era id for the given set of finality signatures.
    pub(crate) era_id: EraId,
    /// The signatures associated with the block hash.
    pub(crate) proofs: BTreeMap<PublicKey, Signature>,
}

impl BlockSignatures {
    #[cfg(test)]
    pub(crate) fn new(block_hash: BlockHash, era_id: EraId) -> Self {
        BlockSignatures {
            block_hash,
            era_id,
            proofs: BTreeMap::new(),
        }
    }
}

pub mod json_compatibility {
    use super::*;
    use casper_types::PublicKey;

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct Reward {
        validator: PublicKey,
        amount: u64,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct ValidatorWeight {
        validator: PublicKey,
        weight: U512,
    }

    /// Equivocation and reward information to be included in the terminal block.
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct JsonEraReport {
        equivocators: Vec<PublicKey>,
        rewards: Vec<Reward>,
        inactive_validators: Vec<PublicKey>,
    }

    impl From<EraReport> for JsonEraReport {
        fn from(era_report: EraReport) -> Self {
            JsonEraReport {
                equivocators: era_report.equivocators,
                rewards: era_report
                    .rewards
                    .into_iter()
                    .map(|(validator, amount)| Reward { validator, amount })
                    .collect(),
                inactive_validators: era_report.inactive_validators,
            }
        }
    }

    impl From<JsonEraReport> for EraReport {
        fn from(era_report: JsonEraReport) -> Self {
            let equivocators = era_report.equivocators;
            let rewards = era_report
                .rewards
                .into_iter()
                .map(|reward| (reward.validator, reward.amount))
                .collect();
            let inactive_validators = era_report.inactive_validators;
            EraReport {
                equivocators,
                rewards,
                inactive_validators,
            }
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct JsonEraEnd {
        era_report: JsonEraReport,
        next_era_validator_weights: Vec<ValidatorWeight>,
    }

    impl From<EraEnd> for JsonEraEnd {
        fn from(data: EraEnd) -> Self {
            let json_era_end = JsonEraReport::from(data.era_report);
            let json_validator_weights = data
                .next_era_validator_weights
                .iter()
                .map(|(validator, weight)| ValidatorWeight {
                    validator: validator.clone(),
                    weight: *weight,
                })
                .collect();
            JsonEraEnd {
                era_report: json_era_end,
                next_era_validator_weights: json_validator_weights,
            }
        }
    }

    impl From<JsonEraEnd> for EraEnd {
        fn from(json_data: JsonEraEnd) -> Self {
            let era_report = EraReport::from(json_data.era_report);
            let validator_weights = json_data
                .next_era_validator_weights
                .iter()
                .map(|validator_weight| {
                    (validator_weight.validator.clone(), validator_weight.weight)
                })
                .collect();
            EraEnd::new(era_report, validator_weights)
        }
    }

    /// JSON representation of a block header.
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct JsonBlockHeader {
        /// The parent hash.
        pub parent_hash: BlockHash,
        /// The state root hash.
        pub state_root_hash: Digest,
        /// The body hash.
        pub body_hash: Digest,
        /// Randomness bit.
        pub random_bit: bool,
        /// Accumulated seed.
        pub accumulated_seed: Digest,
        /// The era end.
        pub era_end: Option<JsonEraEnd>,
        /// The block timestamp.
        pub timestamp: Timestamp,
        /// The block era id.
        pub era_id: EraId,
        /// The block height.
        pub height: u64,
        /// The protocol version.
        pub protocol_version: ProtocolVersion,
    }

    impl From<BlockHeader> for JsonBlockHeader {
        fn from(block_header: BlockHeader) -> Self {
            JsonBlockHeader {
                parent_hash: block_header.parent_hash,
                state_root_hash: block_header.state_root_hash,
                body_hash: block_header.body_hash,
                random_bit: block_header.random_bit,
                accumulated_seed: block_header.accumulated_seed,
                era_end: block_header.era_end.map(JsonEraEnd::from),
                timestamp: block_header.timestamp,
                era_id: block_header.era_id,
                height: block_header.height,
                protocol_version: block_header.protocol_version,
            }
        }
    }

    impl From<JsonBlockHeader> for BlockHeader {
        fn from(block_header: JsonBlockHeader) -> Self {
            BlockHeader {
                parent_hash: block_header.parent_hash,
                state_root_hash: block_header.state_root_hash,
                body_hash: block_header.body_hash,
                random_bit: block_header.random_bit,
                accumulated_seed: block_header.accumulated_seed,
                era_end: block_header.era_end.map(EraEnd::from),
                timestamp: block_header.timestamp,
                era_id: block_header.era_id,
                height: block_header.height,
                protocol_version: block_header.protocol_version,
            }
        }
    }

    /// A JSON-friendly representation of `Body`
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct JsonBlockBody {
        proposer: PublicKey,
        deploy_hashes: Vec<DeployHash>,
        transfer_hashes: Vec<DeployHash>,
    }

    impl From<BlockBody> for JsonBlockBody {
        fn from(body: BlockBody) -> Self {
            JsonBlockBody {
                proposer: body.proposer().clone(),
                deploy_hashes: body.deploy_hashes().clone(),
                transfer_hashes: body.transfer_hashes().clone(),
            }
        }
    }

    impl From<JsonBlockBody> for BlockBody {
        fn from(json_body: JsonBlockBody) -> Self {
            BlockBody {
                proposer: json_body.proposer,
                deploy_hashes: json_body.deploy_hashes,
                transfer_hashes: json_body.transfer_hashes,
            }
        }
    }

    /// A JSON-friendly representation of `Block`.
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct JsonBlock {
        /// `BlockHash`
        pub hash: BlockHash,
        /// JSON-friendly block header.
        pub header: JsonBlockHeader,
        /// JSON-friendly block body.
        pub body: JsonBlockBody,
        /// JSON-friendly list of proofs for this block.
        pub proofs: Vec<JsonProof>,
    }

    impl JsonBlock {
        /// Create a new JSON Block with a Linear chain block and its associated signatures.
        pub fn new(block: Block, maybe_signatures: Option<BlockSignatures>) -> Self {
            let hash = *block.hash();
            let header = JsonBlockHeader::from(block.header.clone());
            let body = JsonBlockBody::from(block.body);
            let proofs = maybe_signatures
                .map(|signatures| signatures.proofs.into_iter().map(JsonProof::from).collect())
                .unwrap_or_default();

            JsonBlock {
                hash,
                header,
                body,
                proofs,
            }
        }

        /// Returns the hashes of the `Deploy`s included in the `Block`.
        pub fn deploy_hashes(&self) -> &Vec<DeployHash> {
            &self.body.deploy_hashes
        }

        /// Returns the hashes of the transfer `Deploy`s included in the `Block`.
        pub fn transfer_hashes(&self) -> &Vec<DeployHash> {
            &self.body.transfer_hashes
        }
    }

    impl From<JsonBlock> for Block {
        fn from(block: JsonBlock) -> Self {
            Block {
                hash: block.hash,
                header: BlockHeader::from(block.header),
                body: BlockBody::from(block.body),
            }
        }
    }

    /// A JSON-friendly representation of a proof, i.e. a block's finality signature.
    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct JsonProof {
        public_key: PublicKey,
        signature: Signature,
    }

    impl From<(PublicKey, Signature)> for JsonProof {
        fn from((public_key, signature): (PublicKey, Signature)) -> JsonProof {
            JsonProof {
                public_key,
                signature,
            }
        }
    }

    impl From<JsonProof> for (PublicKey, Signature) {
        fn from(proof: JsonProof) -> (PublicKey, Signature) {
            (proof.public_key, proof.signature)
        }
    }

    #[test]
    fn block_json_roundtrip() {
        let mut rng = TestRng::new();
        let block: Block = Block::random(&mut rng);
        let empty_signatures = BlockSignatures::new(*block.hash(), block.header.era_id);
        let json_block = JsonBlock::new(block.clone(), Some(empty_signatures));
        let block_deserialized = Block::from(json_block);
        assert_eq!(block, block_deserialized);
    }
}
