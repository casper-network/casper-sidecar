use std::{
    collections::BTreeMap,
    fmt::{self, Display, Formatter},
    hash::Hash,
    iter,
};

#[cfg(feature = "sse-data-testing")]
use rand::Rng;
use serde::{Deserialize, Serialize};

use casper_types::{
    bytesrepr, EraId, ProtocolVersion, PublicKey, SecretKey, Signature, Timestamp, U512,
};
#[cfg(feature = "sse-data-testing")]
use casper_types::{bytesrepr::ToBytes, crypto, testing::TestRng};

use crate::{DeployHash, Digest};

/// A cryptographic hash identifying a [`Block`].
#[derive(
    Copy, Clone, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug,
)]
#[serde(deny_unknown_fields)]
pub struct BlockHash(Digest);

impl BlockHash {
    /// Returns the wrapped inner hash.
    pub fn inner(&self) -> &Digest {
        &self.0
    }
}

impl Display for BlockHash {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "block-hash({})", self.0,)
    }
}

#[cfg(feature = "sse-data-testing")]
impl BlockHash {
    /// Creates a random block hash.
    pub fn random(rng: &mut TestRng) -> Self {
        let hash = Digest::from(rng.gen::<[u8; Digest::LENGTH]>());
        BlockHash(hash)
    }
}

#[cfg(feature = "sse-data-testing")]
impl ToBytes for BlockHash {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        self.0.to_bytes()
    }

    fn serialized_length(&self) -> usize {
        self.0.serialized_length()
    }
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct EraReport {
    equivocators: Vec<PublicKey>,
    rewards: BTreeMap<PublicKey, u64>,
    inactive_validators: Vec<PublicKey>,
}

#[cfg(feature = "sse-data-testing")]
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

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
/// A struct to contain information related to the end of an era and validator weights for the
/// following era.
pub struct EraEnd {
    /// The era end information.
    era_report: EraReport,
    /// The validator weights for the next era.
    next_era_validator_weights: BTreeMap<PublicKey, U512>,
}

#[cfg(feature = "sse-data-testing")]
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

/// The header portion of a [`Block`].
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

#[cfg(feature = "sse-data-testing")]
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

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct BlockBody {
    proposer: PublicKey,
    deploy_hashes: Vec<DeployHash>,
    transfer_hashes: Vec<DeployHash>,
}

#[cfg(feature = "sse-data-testing")]
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

#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Block {
    hash: BlockHash,
    header: BlockHeader,
    body: BlockBody,
}

#[cfg(feature = "sse-data-testing")]
impl Block {
    /// The hash of this block's header.
    pub fn hash(&self) -> &BlockHash {
        &self.hash
    }

    pub fn random_with_data(
        rng: &mut TestRng,
        maybe_deploy_hashes: Option<Vec<DeployHash>>,
        maybe_height: Option<u64>,
    ) -> Self {
        const BLOCK_REWARD: u64 = 1_000_000_000_000;

        // Create the block body.
        let proposer = PublicKey::random(rng);
        let deploy_hashes = maybe_deploy_hashes.unwrap_or_else(|| {
            let deploy_count = rng.gen_range(0..11);
            iter::repeat_with(|| DeployHash::new(Digest::random(rng)))
                .take(deploy_count)
                .collect()
        });
        let transfer_count = rng.gen_range(0..11);
        let transfer_hashes = iter::repeat_with(|| DeployHash::new(Digest::random(rng)))
            .take(transfer_count)
            .collect();
        let body = BlockBody {
            proposer,
            deploy_hashes,
            transfer_hashes,
        };

        // Create the block header.
        let parent_hash = BlockHash(Digest::random(rng));
        let state_root_hash = Digest::random(rng);
        let serialized_body = body
            .to_bytes()
            .unwrap_or_else(|error| panic!("should serialize block body: {}", error));
        let body_hash = Digest::hash(serialized_body);
        let random_bit = rng.gen();
        let accumulated_seed = Digest::random(rng);
        let is_switch = rng.gen_bool(0.1);
        let era_end = if is_switch {
            let equivocators_count = rng.gen_range(0..5);
            let rewards_count = rng.gen_range(0..5);
            let inactive_count = rng.gen_range(0..5);
            let era_report = EraReport {
                equivocators: iter::repeat_with(|| PublicKey::random(rng))
                    .take(equivocators_count)
                    .collect(),
                rewards: iter::repeat_with(|| {
                    let public_key = PublicKey::random(rng);
                    let reward = rng.gen_range(1..(BLOCK_REWARD + 1));
                    (public_key, reward)
                })
                .take(rewards_count)
                .collect(),
                inactive_validators: iter::repeat_with(|| PublicKey::random(rng))
                    .take(inactive_count)
                    .collect(),
            };
            let validator_count = rng.gen_range(0..11);
            let next_era_validator_weights =
                iter::repeat_with(|| (PublicKey::random(rng), rng.gen()))
                    .take(validator_count)
                    .collect();
            Some(EraEnd {
                era_report,
                next_era_validator_weights,
            })
        } else {
            None
        };
        let timestamp = Timestamp::now();
        let era = rng.gen_range(1..6);
        let height = maybe_height.unwrap_or_else(|| era * 10 + rng.gen_range(0..10));
        let header = BlockHeader {
            parent_hash,
            state_root_hash,
            body_hash,
            random_bit,
            accumulated_seed,
            era_end,
            timestamp,
            era_id: EraId::new(era),
            height,
            protocol_version: ProtocolVersion::V1_0_0,
        };

        // Create the block hash.
        let serialized_header = header
            .to_bytes()
            .unwrap_or_else(|error| panic!("should serialize block header: {}", error));
        let hash = BlockHash(Digest::hash(serialized_header));

        Block { hash, header, body }
    }

    pub fn random(rng: &mut TestRng) -> Self {
        Self::random_with_data(rng, None, None)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FinalitySignature {
    block_hash: BlockHash,
    era_id: EraId,
    signature: Signature,
    public_key: PublicKey,
}

impl FinalitySignature {
    /// Hash of a block this signature is for.
    pub fn block_hash(&self) -> &BlockHash {
        &self.block_hash
    }

    /// Era in which the block was created in.
    pub fn era_id(&self) -> EraId {
        self.era_id
    }

    /// Signature over the block hash.
    pub fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Public key of the signing validator.
    pub fn public_key(&self) -> &PublicKey {
        &self.public_key
    }
}

#[cfg(feature = "sse-data-testing")]
impl FinalitySignature {
    pub fn random_for_block(block_hash: BlockHash, era_id: u64, rng: &mut TestRng) -> Self {
        let mut bytes = block_hash.inner().into_vec();
        bytes.extend_from_slice(&era_id.to_le_bytes());
        let secret_key = SecretKey::random(rng);
        let public_key = PublicKey::from(&secret_key);
        let signature = crypto::sign(bytes, &secret_key, &public_key);

        FinalitySignature {
            block_hash,
            era_id: EraId::new(era_id),
            signature,
            public_key,
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
            let next_era_validator_weights = json_data
                .next_era_validator_weights
                .iter()
                .map(|validator_weight| {
                    (validator_weight.validator.clone(), validator_weight.weight)
                })
                .collect();
            EraEnd {
                era_report,
                next_era_validator_weights,
            }
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
                proposer: body.proposer.clone(),
                deploy_hashes: body.deploy_hashes.clone(),
                transfer_hashes: body.transfer_hashes,
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
        /// Creates a new JSON block with no proofs from a linear chain block.
        pub fn new_unsigned(block: Block) -> Self {
            JsonBlock {
                hash: block.hash,
                header: JsonBlockHeader::from(block.header.clone()),
                body: JsonBlockBody::from(block.body),
                proofs: Vec::new(),
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

        pub fn random_with_data(
            rng: &mut TestRng,
            deploy_hashes: Option<Vec<DeployHash>>,
            height: Option<u64>,
        ) -> Self {
            let block = Block::random_with_data(rng, deploy_hashes, height);
            let proofs_count = rng.gen_range(0..11);
            let proofs = iter::repeat_with(|| {
                let finality_signature = FinalitySignature::random_for_block(
                    block.hash,
                    block.header.era_id.value(),
                    rng,
                );
                JsonProof {
                    public_key: finality_signature.public_key,
                    signature: finality_signature.signature,
                }
            })
            .take(proofs_count)
            .collect();
            JsonBlock {
                hash: block.hash,
                header: JsonBlockHeader::from(block.header.clone()),
                body: JsonBlockBody::from(block.body),
                proofs,
            }
        }

        #[cfg(feature = "sse-data-testing")]
        pub fn random(rng: &mut TestRng) -> Self {
            let block = Block::random(rng);
            let proofs_count = rng.gen_range(0..11);
            let proofs = iter::repeat_with(|| {
                let finality_signature = FinalitySignature::random_for_block(
                    block.hash,
                    block.header.era_id.value(),
                    rng,
                );
                JsonProof {
                    public_key: finality_signature.public_key,
                    signature: finality_signature.signature,
                }
            })
            .take(proofs_count)
            .collect();
            JsonBlock {
                hash: block.hash,
                header: JsonBlockHeader::from(block.header.clone()),
                body: JsonBlockBody::from(block.body),
                proofs,
            }
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
}
