use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};

#[cfg(test)]
use casper_event_types::Digest;
use casper_event_types::{BlockHash, Deploy, DeployHash, FinalitySignature as FinSig, JsonBlock};
#[cfg(test)]
use casper_types::testing::TestRng;
use derive_new::new;
#[cfg(test)]
use rand::Rng;
use serde::{Deserialize, Serialize};

use casper_types::{
    AsymmetricType, EraId, ExecutionResult, ProtocolVersion, PublicKey, TimeDiff, Timestamp,
};
use serde_json::value::RawValue;

/// The version of this node's API server.  This event will always be the first sent to a new
/// client, and will have no associated event ID provided.
#[derive(Clone, Debug, Serialize, Deserialize, new)]
pub struct ApiVersion(ProtocolVersion);

/// The given block has been added to the linear chain and stored locally.
#[derive(Clone, Debug, Serialize, Deserialize, new)]
pub struct BlockAdded {
    block_hash: BlockHash,
    pub block: Box<JsonBlock>,
}

impl BlockAdded {
    pub fn get_timestamp(&self) -> Timestamp {
        self.block.header.timestamp
    }

    pub fn get_all_deploy_hashes(&self) -> Vec<String> {
        let deploy_hashes = self.block.deploy_hashes();
        let transfer_hashes = self.block.transfer_hashes();
        deploy_hashes
            .iter()
            .chain(transfer_hashes.iter())
            .map(|hash_struct| hex::encode(hash_struct.inner()))
            .collect()
    }

    pub fn hex_encoded_hash(&self) -> String {
        hex::encode(self.block_hash.inner())
    }

    pub fn get_height(&self) -> u64 {
        self.block.header.height
    }

    #[cfg(test)]
    pub fn random_with_data(
        rng: &mut TestRng,
        deploy_hashes: Vec<DeployHash>,
        height: u64,
    ) -> Self {
        let block = JsonBlock::random_with_data(rng, Some(deploy_hashes), Some(height));
        Self {
            block_hash: block.hash,
            block: Box::new(block),
        }
    }

    #[cfg(test)]
    pub fn random(rng: &mut TestRng) -> Self {
        let block = JsonBlock::random(rng);
        Self {
            block_hash: block.hash,
            block: Box::new(block),
        }
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
        Self {
            deploy: Arc::new(Deploy::random(rng)),
        }
    }

    #[cfg(test)]
    pub fn deploy_hash(&self) -> DeployHash {
        self.deploy.hash().to_owned()
    }

    pub fn hex_encoded_hash(&self) -> String {
        hex::encode(self.deploy.hash().inner())
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
    pub fn random(
        rng: &mut TestRng,
        with_deploy_hash: Option<DeployHash>,
        with_block_hash: Option<BlockHash>,
    ) -> Self {
        let deploy = Deploy::random(rng);
        Self {
            deploy_hash: Box::new(with_deploy_hash.unwrap_or(*deploy.hash())),
            account: Box::new(deploy.header().account().clone()),
            timestamp: deploy.header().timestamp(),
            ttl: deploy.header().ttl(),
            dependencies: deploy.header().dependencies().clone(),
            block_hash: Box::new(with_block_hash.unwrap_or(BlockHash::random(rng))),
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
        Self {
            deploy_hash: with_deploy_hash.unwrap_or_else(|| DeployHash::new(Digest::random(rng))),
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
        hex::encode(self.0.block_hash().inner())
    }

    pub fn hex_encoded_public_key(&self) -> String {
        self.0.public_key().to_hex()
    }
}

/// The execution effects produced by a `StepRequest`.
#[derive(Clone, Debug, Serialize, Deserialize, new)]
pub struct Step {
    pub era_id: EraId,
    execution_effect: Box<RawValue>,
}

impl Step {
    #[cfg(test)]
    pub fn random(rng: &mut TestRng) -> Self {
        use serde_json::value::to_raw_value;

        let execution_effect = match rng.gen::<ExecutionResult>() {
            ExecutionResult::Success { effect, .. } | ExecutionResult::Failure { effect, .. } => {
                effect
            }
        };
        Self {
            era_id: EraId::new(rng.gen()),
            execution_effect: to_raw_value(&execution_effect).unwrap(),
        }
    }
}
