//! Common types used when dealing with serialization/deserialization of data from nodes,
//! also a "contemporary" data model which is based on 1.4.x node specification

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

#[cfg(feature = "sse-data-testing")]
use super::testing;
use crate::{BlockHash, Deploy, DeployHash, FinalitySignature, JsonBlock};
#[cfg(feature = "sse-data-testing")]
use casper_types::testing::TestRng;

use casper_types::{
    EraId, ExecutionEffect, ExecutionResult, ProtocolVersion, PublicKey, TimeDiff, Timestamp,
};
#[cfg(feature = "sse-data-testing")]
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SseDataDeserializeError {
    #[error("Couldn't deserialize {0}")]
    DeserializationError(String),
}

pub(crate) fn to_error(msg: String) -> SseDataDeserializeError {
    SseDataDeserializeError::DeserializationError(msg)
}

/// Deserializes a string which should contain json data and returns a result of either SseData (which is 1.4.x compliant) or an SseDataDeserializeError
///
/// * `json_raw`: string slice which should contain raw json data.
pub fn deserialize(json_raw: &str) -> Result<SseData, SseDataDeserializeError> {
    serde_json::from_str::<SseData>(json_raw).map_err(|err| {
        let error_message = format!("Serde Error: {}", err);
        to_error(error_message)
    })
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

#[cfg(feature = "sse-data-testing")]
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
        let block = JsonBlock::random(rng);
        SseData::BlockAdded {
            block_hash: block.hash,
            block: Box::new(block),
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
            deploy_hash: Box::new(*deploy.hash()),
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
            deploy_hash: *deploy.hash(),
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
            rng,
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

#[cfg(feature = "sse-data-testing")]
pub mod test_support {
    pub const BLOCK_HASH_1: &str =
        "ca52062424e9d5631a34b7b401e123927ce29d4bd10bc97c7df0aa752f131bb7";
    pub const BLOCK_HASH_2: &str =
        "1a73fbaca8c655de21547c9b73e486f259af5d9f57860ca14141bbd20784189b";
    pub const BLOCK_HASH_3: &str =
        "4c561b6d1e9a955810c6ec6326bf197891e85c3b2cff9521e33ce769d9af78e5";
    pub fn example_block_added_1_4_10(block_hash: &str, height: &str) -> String {
        let raw_block_added = format!("{{\"BlockAdded\":{{\"block_hash\":\"{block_hash}\",\"block\":{{\"hash\":\"{block_hash}\",\"header\":{{\"parent_hash\":\"4a28718301a83a43563ec42a184294725b8dd188aad7a9fceb8a2fa1400c680e\",\"state_root_hash\":\"63274671f2a860e39bb029d289e688526e4828b70c79c678649748e5e376cb07\",\"body_hash\":\"6da90c09f3fc4559d27b9fff59ab2453be5752260b07aec65e0e3a61734f656a\",\"random_bit\":true,\"accumulated_seed\":\"c8b4f30a3e3e082f4f206f972e423ffb23d152ca34241ff94ba76189716b61da\",\"era_end\":{{\"era_report\":{{\"equivocators\":[],\"rewards\":[{{\"validator\":\"01026ca707c348ed8012ac6a1f28db031fadd6eb67203501a353b867a08c8b9a80\",\"amount\":1559401400039}},{{\"validator\":\"010427c1d1227c9d2aafe8c06c6e6b276da8dcd8fd170ca848b8e3e8e1038a6dc8\",\"amount\":25895190891}}],\"inactive_validators\":[]}},\"next_era_validator_weights\":[{{\"validator\":\"01026ca707c348ed8012ac6a1f28db031fadd6eb67203501a353b867a08c8b9a80\",\"weight\":\"50538244651768072\"}},{{\"validator\":\"010427c1d1227c9d2aafe8c06c6e6b276da8dcd8fd170ca848b8e3e8e1038a6dc8\",\"weight\":\"839230678448335\"}}]}},\"timestamp\":\"2021-04-08T05:14:14.912Z\",\"era_id\":90,\"height\":{height},\"protocol_version\":\"1.0.0\"}},\"body\":{{\"proposer\":\"012bac1d0ff9240ff0b7b06d555815640497861619ca12583ddef434885416e69b\",\"deploy_hashes\":[],\"transfer_hashes\":[]}},\"proofs\":[]}}}}}}");
        super::deserialize(&raw_block_added).unwrap(); // deserializing to make sure that the raw json string is in correct form
        raw_block_added
    }

    pub fn example_finality_signature_1_4_10(block_hash: &str) -> String {
        let raw_block_added = format!("{{\"FinalitySignature\":{{\"block_hash\":\"{block_hash}\",\"era_id\":8538,\"signature\":\"0157368db32b578c1cf97256c3012d50afc5745fe22df2f4be1efd0bdf82b63ce072b4726fdfb7c026068b38aaa67ea401b49d969ab61ae587af42c64de8914101\",\"public_key\":\"0138e64f04c03346e94471e340ca7b94ba3581e5697f4d1e59f5a31c0da720de45\"}}}}");
        super::deserialize(&raw_block_added).unwrap(); // deserializing to make sure that the raw json string is in correct form
        raw_block_added
    }
}
