use std::sync::Arc;

use derive_new::new;
use serde::{Deserialize, Serialize};

use casper_node::types::{BlockHash, Deploy, DeployHash, FinalitySignature as FinSig, JsonBlock};
use casper_types::{
    AsymmetricType, EraId, ExecutionEffect, ExecutionResult, ProtocolVersion, PublicKey, TimeDiff,
    Timestamp,
};

/// The version of this node's API server.  This event will always be the first sent to a new
/// client, and will have no associated event ID provided.
#[derive(Debug, Serialize, Deserialize, new)]
pub struct ApiVersion(ProtocolVersion);

/// The given block has been added to the linear chain and stored locally.
#[derive(Debug, Serialize, Deserialize, new)]
pub struct BlockAdded {
    block_hash: BlockHash,
    block: Box<JsonBlock>,
}

impl BlockAdded {
    pub fn hex_encoded_hash(&self) -> String {
        hex::encode(self.block_hash.inner())
    }

    pub fn get_height(&self) -> u64 {
        self.block.header.height
    }
}

/// The given deploy has been newly-accepted by this node.
#[derive(Debug, Serialize, Deserialize, new)]
pub struct DeployAccepted {
    #[serde(flatten)]
    // It's an Arc to not create multiple copies of the same deploy for multiple subscribers.
    deploy: Arc<Deploy>,
}

impl DeployAccepted {
    pub fn hex_encoded_hash(&self) -> String {
        hex::encode(self.deploy.id().inner())
    }
}

/// The given deploy has been executed, committed and forms part of the given block.
#[derive(Debug, Serialize, Deserialize, new)]
pub struct DeployProcessed {
    deploy_hash: Box<DeployHash>,
    account: Box<PublicKey>,
    timestamp: Timestamp,
    ttl: TimeDiff,
    dependencies: Vec<DeployHash>,
    block_hash: Box<BlockHash>,
    execution_result: Box<ExecutionResult>,
}

impl DeployProcessed {
    pub fn hex_encoded_hash(&self) -> String {
        hex::encode(self.deploy_hash.inner())
    }
}

/// The given deploy has expired.
#[derive(Debug, Serialize, Deserialize, new)]
pub struct DeployExpired {
    deploy_hash: DeployHash,
}

impl DeployExpired {
    pub fn hex_encoded_hash(&self) -> String {
        hex::encode(self.deploy_hash.inner())
    }
}

/// Generic representation of validator's fault in an era.
#[derive(Debug, Serialize, Deserialize, new)]
pub struct Fault {
    pub era_id: EraId,
    pub public_key: PublicKey,
    pub timestamp: Timestamp,
}

/// New finality signature received.
#[derive(Debug, Serialize, Deserialize, new)]
pub struct FinalitySignature(Box<FinSig>);

impl FinalitySignature {
    pub fn inner(&self) -> FinSig {
        *self.0.clone()
    }

    pub fn hex_encoded_block_hash(&self) -> String {
        hex::encode(self.0.block_hash.inner())
    }

    pub fn hex_encoded_public_key(&self) -> String {
        self.0.public_key.to_hex()
    }
}

/// The execution effects produced by a `StepRequest`.
#[derive(Debug, Serialize, Deserialize, new)]
pub struct Step {
    pub era_id: EraId,
    execution_effect: ExecutionEffect,
}
