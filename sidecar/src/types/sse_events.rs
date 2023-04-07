use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};

use derive_new::new;
#[cfg(test)]
use rand::Rng;
use serde::{Deserialize, Serialize};

#[cfg(test)]
use casper_event_types::block::random_secret_key;
#[cfg(test)]
use casper_event_types::block::Block;
use casper_event_types::{
    block::{json_compatibility::JsonBlock, BlockHash, FinalitySignature as FinSig},
    deploy::{Deploy, DeployHash},
};
#[cfg(test)]
use casper_types::testing::TestRng;

use casper_types::{
    AsymmetricType, EraId, ExecutionEffect, ExecutionResult, ProtocolVersion, PublicKey, TimeDiff,
    Timestamp,
};

/// The version of this node's API server.  This event will always be the first sent to a new
/// client, and will have no associated event ID provided.
#[derive(Clone, Debug, Serialize, Deserialize, new)]
pub struct ApiVersion(ProtocolVersion);

/// The given block has been added to the linear chain and stored locally.
#[derive(Clone, Debug, Serialize, Deserialize, new)]
pub struct BlockAdded {
    block_hash: BlockHash,
    block: Box<JsonBlock>,
}

#[cfg(test)]
impl BlockAdded {
    pub fn random(rng: &mut TestRng) -> Self {
        let block = Block::random(rng);
        Self {
            block_hash: *block.hash(),
            block: Box::new(JsonBlock::new(block, None)),
        }
    }
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
#[derive(Clone, Debug, Serialize, Deserialize, new)]
pub struct DeployAccepted {
    #[serde(flatten)]
    // It's an Arc to not create multiple copies of the same deploy for multiple subscribers.
    deploy: Arc<Deploy>,
}

impl DeployAccepted {
    #[cfg(test)]
    pub fn random(rng: &mut TestRng) -> Self {
        let secret_key = random_secret_key(rng);
        let deploy = Deploy::random(rng, secret_key);
        Self {
            deploy: Arc::new(deploy),
        }
    }

    #[cfg(test)]
    pub fn deploy_hash(&self) -> DeployHash {
        self.deploy.id().to_owned()
    }

    pub fn hex_encoded_hash(&self) -> String {
        hex::encode(self.deploy.id().inner())
    }
}

/// The given deploy has been executed, committed and forms part of the given block.
#[derive(Clone, Debug, Serialize, Deserialize, new)]
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
    #[cfg(test)]
    pub fn random(rng: &mut TestRng, with_deploy_hash: Option<DeployHash>) -> Self {
        let secret_key = random_secret_key(rng);
        let deploy = Deploy::random(rng, secret_key);
        Self {
            deploy_hash: Box::new(with_deploy_hash.unwrap_or(deploy.hash)),
            account: Box::new(deploy.header().account().clone()),
            timestamp: deploy.header().timestamp(),
            ttl: deploy.header().ttl(),
            dependencies: deploy.header().dependencies().clone(),
            block_hash: Box::new(BlockHash::random(rng)),
            execution_result: Box::new(rng.gen()),
        }
    }

    pub fn hex_encoded_hash(&self) -> String {
        hex::encode(self.deploy_hash.inner())
    }
}

/// The given deploy has expired.
#[derive(Clone, Debug, Serialize, Deserialize, new)]
pub struct DeployExpired {
    deploy_hash: DeployHash,
}

impl DeployExpired {
    #[cfg(test)]
    pub fn random(rng: &mut TestRng, with_deploy_hash: Option<DeployHash>) -> Self {
        let secret_key = random_secret_key(rng);

        let deploy = Deploy::random(rng, secret_key);
        Self {
            deploy_hash: with_deploy_hash.unwrap_or(deploy.hash),
        }
    }

    pub fn hex_encoded_hash(&self) -> String {
        hex::encode(self.deploy_hash.inner())
    }
}

/// Generic representation of validator's fault in an era.
#[derive(Clone, Debug, Serialize, Deserialize, new)]
pub struct Fault {
    pub era_id: EraId,
    pub public_key: PublicKey,
    pub timestamp: Timestamp,
}

impl Fault {
    #[cfg(test)]
    pub fn random(rng: &mut TestRng) -> Self {
        Self {
            era_id: EraId::new(rng.gen()),
            public_key: PublicKey::random(rng),
            timestamp: Timestamp::random(rng),
        }
    }
}

impl Display for Fault {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}

/// New finality signature received.
#[derive(Clone, Debug, Serialize, Deserialize, new)]
pub struct FinalitySignature(Box<FinSig>);

impl FinalitySignature {
    #[cfg(test)]
    pub fn random(rng: &mut TestRng) -> Self {
        //#TODO check if this is right
        Self(Box::new(FinSig::random_for_block(
            BlockHash::random(rng),
            rng.gen(),
            rng,
        )))
    }

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
#[derive(Clone, Debug, Serialize, Deserialize, new)]
pub struct Step {
    pub era_id: EraId,
    execution_effect: ExecutionEffect,
}

impl Step {
    #[cfg(test)]
    pub fn random(rng: &mut TestRng) -> Self {
        let execution_effect = match rng.gen::<ExecutionResult>() {
            ExecutionResult::Success { effect, .. } | ExecutionResult::Failure { effect, .. } => {
                effect
            }
        };
        Self {
            era_id: EraId::new(rng.gen()),
            execution_effect,
        }
    }
}
