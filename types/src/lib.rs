#[cfg(any(feature = "sse-data-testing", test))]
mod testing;

use std::sync::Arc;

#[cfg(feature = "sse-data-testing")]
use rand::Rng;
use serde::{Deserialize, Serialize};

#[cfg(feature = "sse-data-testing")]
use casper_node::types::Block;
use casper_node::types::{BlockHash, Deploy, DeployHash, FinalitySignature, JsonBlock};
#[cfg(feature = "sse-data-testing")]
use casper_types::testing::TestRng;
use casper_types::{
    EraId, ExecutionEffect, ExecutionResult, ProtocolVersion, PublicKey, TimeDiff, Timestamp,
};

/// A filter for event types a client has subscribed to receive.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum EventFilter {
    BlockAdded,
    DeployAccepted,
    DeployProcessed,
    DeployExpired,
    Fault,
    FinalitySignature,
    Step,
}

/// The "data" field of the events sent on the event stream to clients.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub enum SseData {
    /// The version of this node's API server.  This event will always be the first sent to a new
    /// client, and will have no associated event ID provided.
    ApiVersion(ProtocolVersion),
    /// The given block has been added to the linear chain and stored locally.
    BlockAdded {
        block_hash: BlockHash,
        block: Box<JsonBlock>,
    },
    /// The given deploy has been newly-accepted by this node.
    DeployAccepted {
        #[serde(flatten)]
        // It's an Arc to not create multiple copies of the same deploy for multiple subscribers.
        deploy: Arc<Deploy>,
    },
    /// The given deploy has been executed, committed and forms part of the given block.
    DeployProcessed {
        deploy_hash: Box<DeployHash>,
        account: Box<PublicKey>,
        timestamp: Timestamp,
        ttl: TimeDiff,
        dependencies: Vec<DeployHash>,
        block_hash: Box<BlockHash>,
        execution_result: Box<ExecutionResult>,
    },
    /// The given deploy has expired.
    DeployExpired { deploy_hash: DeployHash },
    /// Generic representation of validator's fault in an era.
    Fault {
        era_id: EraId,
        public_key: PublicKey,
        timestamp: Timestamp,
    },
    /// New finality signature received.
    FinalitySignature(Box<FinalitySignature>),
    /// The execution effects produced by a `StepRequest`.
    Step {
        era_id: EraId,
        execution_effect: ExecutionEffect,
    },
    /// The node is about to shut down.
    Shutdown,
}

impl SseData {
    pub fn should_include(&self, filter: &[EventFilter]) -> bool {
        match self {
            SseData::ApiVersion(_) | SseData::Shutdown => true,
            SseData::BlockAdded { .. } => filter.contains(&EventFilter::BlockAdded),
            SseData::DeployAccepted { .. } => filter.contains(&EventFilter::DeployAccepted),
            SseData::DeployProcessed { .. } => filter.contains(&EventFilter::DeployProcessed),
            SseData::DeployExpired { .. } => filter.contains(&EventFilter::DeployExpired),
            SseData::Fault { .. } => filter.contains(&EventFilter::Fault),
            SseData::FinalitySignature(_) => filter.contains(&EventFilter::FinalitySignature),
            SseData::Step { .. } => filter.contains(&EventFilter::Step),
        }
    }
}

#[cfg(any(feature = "sse-data-testing", test))]
impl SseData {
    /// Returns a random `SseData::ApiVersion`.
    pub fn random_api_version(rng: &mut TestRng) -> Self {
        let protocol_version = ProtocolVersion::from_parts(
            rng.gen_range(0..10),
            rng.gen::<u8>() as u32,
            rng.gen::<u8>() as u32,
        );
        SseData::ApiVersion(protocol_version)
    }

    /// Returns a random `SseData::BlockAdded`.
    pub fn random_block_added(rng: &mut TestRng) -> Self {
        let block = Block::random(rng);
        SseData::BlockAdded {
            block_hash: *block.hash(),
            block: Box::new(JsonBlock::new(block, None)),
        }
    }

    /// Returns a random `SseData::DeployAccepted`, along with the random `Deploy`.
    pub fn random_deploy_accepted(rng: &mut TestRng) -> (Self, Deploy) {
        let deploy = Deploy::random(rng);
        let event = SseData::DeployAccepted {
            deploy: Arc::new(deploy.clone()),
        };
        (event, deploy)
    }

    /// Returns a random `SseData::DeployProcessed`.
    pub fn random_deploy_processed(rng: &mut TestRng) -> Self {
        let deploy = Deploy::random(rng);
        SseData::DeployProcessed {
            deploy_hash: Box::new(*deploy.id()),
            account: Box::new(deploy.header().account().clone()),
            timestamp: deploy.header().timestamp(),
            ttl: deploy.header().ttl(),
            dependencies: deploy.header().dependencies().clone(),
            block_hash: Box::new(BlockHash::random(rng)),
            execution_result: Box::new(rng.gen()),
        }
    }

    /// Returns a random `SseData::DeployExpired`
    pub fn random_deploy_expired(rng: &mut TestRng) -> Self {
        let deploy = testing::create_expired_deploy(Timestamp::now(), rng);
        SseData::DeployExpired {
            deploy_hash: *deploy.id(),
        }
    }

    /// Returns a random `SseData::Fault`.
    pub fn random_fault(rng: &mut TestRng) -> Self {
        SseData::Fault {
            era_id: EraId::new(rng.gen()),
            public_key: PublicKey::random(rng),
            timestamp: Timestamp::random(rng),
        }
    }

    /// Returns a random `SseData::FinalitySignature`.
    pub fn random_finality_signature(rng: &mut TestRng) -> Self {
        SseData::FinalitySignature(Box::new(FinalitySignature::random_for_block(
            BlockHash::random(rng),
            rng.gen(),
        )))
    }

    /// Returns a random `SseData::Step`.
    pub fn random_step(rng: &mut TestRng) -> Self {
        let execution_effect = match rng.gen::<ExecutionResult>() {
            ExecutionResult::Success { effect, .. } | ExecutionResult::Failure { effect, .. } => {
                effect
            }
        };
        SseData::Step {
            era_id: EraId::new(rng.gen()),
            execution_effect,
        }
    }
}
