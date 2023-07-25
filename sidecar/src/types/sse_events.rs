#[cfg(test)]
use casper_event_types::Digest;
use casper_event_types::{BlockHash, Deploy, DeployHash, FinalitySignature as FinSig, JsonBlock};
#[cfg(test)]
use casper_types::testing::TestRng;
use casper_types::{
    AsymmetricType, EraId, ExecutionResult, ProtocolVersion, PublicKey, TimeDiff, Timestamp,
};
use derive_new::new;
#[cfg(test)]
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};
use utoipa::ToSchema;

/// The version of this node's API server.  This event will always be the first sent to a new
/// client, and will have no associated event ID provided.
#[derive(Clone, Debug, Serialize, Deserialize, new)]
pub struct ApiVersion(ProtocolVersion);

/// The given block has been added to the linear chain and stored locally.
#[derive(Clone, Debug, Serialize, Deserialize, new, ToSchema)]
pub struct BlockAdded {
    block_hash: BlockHash,
    block: Box<JsonBlock>,
}

#[cfg(test)]
impl BlockAdded {
    pub fn random(rng: &mut TestRng) -> Self {
        let block = JsonBlock::random(rng);
        Self {
            block_hash: block.hash,
            block: Box::new(block),
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
#[derive(Clone, Debug, Serialize, Deserialize, new, ToSchema)]
pub struct DeployAccepted {
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
#[derive(Clone, Debug, Serialize, Deserialize, new, ToSchema)]
pub struct DeployProcessed {
    deploy_hash: Box<DeployHash>,
    #[schema(value_type = String)]
    account: Box<PublicKey>,
    #[schema(value_type = String)]
    timestamp: Timestamp,
    #[schema(value_type = String)]
    ttl: TimeDiff,
    dependencies: Vec<DeployHash>,
    block_hash: Box<BlockHash>,
    execution_result: Box<ExecutionResult>,
}

impl DeployProcessed {
    #[cfg(test)]
    pub fn random(rng: &mut TestRng, with_deploy_hash: Option<DeployHash>) -> Self {
        let deploy = Deploy::random(rng);
        Self {
            deploy_hash: Box::new(with_deploy_hash.unwrap_or(*deploy.hash())),
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
#[derive(Clone, Debug, Serialize, Deserialize, new, ToSchema)]
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
#[derive(Clone, Debug, Serialize, Deserialize, new, ToSchema)]
pub struct Fault {
    #[schema(value_type = u64)]
    pub era_id: EraId,
    /// "Hex-encoded cryptographic public key, including the algorithm tag prefix."
    #[schema(value_type = String)]
    pub public_key: PublicKey,
    #[schema(value_type = String)]
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
#[derive(Clone, Debug, Serialize, Deserialize, new, ToSchema)]
pub struct Step {
    #[schema(value_type = u64)]
    pub era_id: EraId,
    #[schema(value_type = ExecutionEffect)]
    //This technically is not amorphic, but this field is potentially > 30MB of size. By not parsing it we make the process of intaking these messages much quicker and less memory consuming
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
