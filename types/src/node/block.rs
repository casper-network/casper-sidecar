

/// The output of the hash function.
#[derive(Copy, Clone, DataSize, Ord, PartialOrd, Eq, PartialEq, Hash, Default, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(with = "String", description = "Hex-encoded hash digest.")]
pub struct Digest(#[schemars(skip, with = "String")] [u8; 32]);

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
    #[cfg(any(feature = "testing", test))]
    pub fn random(rng: &mut TestRng) -> Self {
        let hash = rng.gen::<[u8; Digest::LENGTH]>().into();
        BlockHash(hash)
    }
}


#[derive(DataSize, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Block {
    hash: BlockHash,
    header: BlockHeader,
    body: BlockBody,
}

impl Block {
    pub(crate) fn new(
        parent_hash: BlockHash,
        parent_seed: Digest,
        state_root_hash: Digest,
        finalized_block: FinalizedBlock,
        next_era_validator_weights: Option<BTreeMap<PublicKey, U512>>,
        protocol_version: ProtocolVersion,
    ) -> Result<Self, BlockCreationError> {
        let body = BlockBody::new(
            *finalized_block.proposer,
            finalized_block.deploy_hashes,
            finalized_block.transfer_hashes,
        );

        let body_hash = body.hash();

        let era_end = match (finalized_block.era_report, next_era_validator_weights) {
            (None, None) => None,
            (Some(era_report), Some(next_era_validator_weights)) => {
                Some(EraEnd::new(*era_report, next_era_validator_weights))
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
            block_hash: OnceCell::new(),
        };

        Ok(Block {
            hash: header.block_hash(),
            header,
            body,
        })
    }
}