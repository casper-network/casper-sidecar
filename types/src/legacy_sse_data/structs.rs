use casper_types::{
    BlockHash, BlockHeaderV1, DeployHash, Digest, EraEndV1, EraId, ProtocolVersion, PublicKey,
    Timestamp,
};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub struct BlockV1 {
    pub(super) hash: BlockHash,
    pub(super) header: BlockHeaderV1,
    pub(super) body: BlockBodyV1,
}

impl BlockV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        parent_hash: BlockHash,
        state_root_hash: Digest,
        body_hash: Digest,
        random_bit: bool,
        accumulated_seed: Digest,
        era_end: Option<EraEndV1>,
        timestamp: Timestamp,
        era_id: EraId,
        height: u64,
        protocol_version: ProtocolVersion,
        proposer: PublicKey,
        block_hash: BlockHash,
        deploy_hashes: Vec<DeployHash>,
        transfer_hashes: Vec<DeployHash>,
    ) -> Self {
        let body = BlockBodyV1::new(proposer, deploy_hashes, transfer_hashes);

        let header = BlockHeaderV1::new(
            parent_hash,
            state_root_hash,
            body_hash,
            random_bit,
            accumulated_seed,
            era_end,
            timestamp,
            era_id,
            height,
            protocol_version,
            OnceCell::from(block_hash),
        );
        Self::new_from_header_and_body(header, body)
    }

    pub fn new_from_header_and_body(header: BlockHeaderV1, body: BlockBodyV1) -> Self {
        let hash = header.block_hash();
        BlockV1 { hash, header, body }
    }

    pub fn from(hash: BlockHash, header: &BlockHeaderV1, body: &casper_types::BlockBodyV1) -> Self {
        let legacy_body = BlockBodyV1::new(
            body.proposer().clone(),
            body.deploy_hashes().to_vec(),
            body.transfer_hashes().to_vec(),
        );
        BlockV1 {
            hash,
            header: header.clone(),
            body: legacy_body,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub struct BlockBodyV1 {
    pub(super) proposer: PublicKey,
    pub(super) deploy_hashes: Vec<DeployHash>,
    pub(super) transfer_hashes: Vec<DeployHash>,
}

impl BlockBodyV1 {
    pub(crate) fn new(
        proposer: PublicKey,
        deploy_hashes: Vec<DeployHash>,
        transfer_hashes: Vec<DeployHash>,
    ) -> Self {
        BlockBodyV1 {
            proposer,
            deploy_hashes,
            transfer_hashes,
        }
    }
}
