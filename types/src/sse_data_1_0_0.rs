//! Types used to deserialize data from nodes that are in legacy version (prior to 1.0.0-1.2.0)
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use casper_types::{
    EraId, ExecutionEffect, ExecutionResult, ProtocolVersion, PublicKey, TimeDiff, Timestamp,
};

use crate::{
    block::{json_compatibility::JsonBlock, Block, BlockHash, FinalitySignature},
    deploy::{Deploy, DeployHash},
    sse_data::{self, to_error, SseDataDeserializeError},
};

/// Deserializes a string which should contain json data and returns a result of either SseData (which is 1.0.0 compliant) or an SseDataDeserializeError
///
/// * `json_raw`: string slice which should contain raw json data.
pub fn deserialize(raw_data: &str) -> Result<super::sse_data::SseData, SseDataDeserializeError> {
    serde_json::from_str::<SseData>(raw_data)
        .map_err(|err| {
            let error_message = format!("Serde Error: {}", err);
            to_error(error_message)
        })
        .map(super::sse_data::SseData::from)
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
        block: Box<Block>,
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

impl From<SseData> for sse_data::SseData {
    fn from(v1_0_0_data: SseData) -> sse_data::SseData {
        match v1_0_0_data {
            SseData::BlockAdded { block_hash, block } => {
                let json_block = JsonBlock::new(*block, None);
                sse_data::SseData::BlockAdded {
                    block_hash,
                    block: Box::new(json_block),
                }
            }
            SseData::ApiVersion(version) => sse_data::SseData::ApiVersion(version),
            SseData::DeployAccepted { deploy } => sse_data::SseData::DeployAccepted { deploy },
            SseData::DeployProcessed {
                deploy_hash,
                account,
                timestamp,
                ttl,
                dependencies,
                block_hash,
                execution_result,
            } => sse_data::SseData::DeployProcessed {
                deploy_hash,
                account,
                timestamp,
                ttl,
                dependencies,
                block_hash,
                execution_result,
            },
            SseData::DeployExpired { deploy_hash } => {
                sse_data::SseData::DeployExpired { deploy_hash }
            }
            SseData::Fault {
                era_id,
                public_key,
                timestamp,
            } => sse_data::SseData::Fault {
                era_id,
                public_key,
                timestamp,
            },
            SseData::FinalitySignature(finality_signature) => {
                sse_data::SseData::FinalitySignature(finality_signature)
            }
            SseData::Step {
                era_id,
                execution_effect,
            } => sse_data::SseData::Step {
                era_id,
                execution_effect,
            },
            SseData::Shutdown => sse_data::SseData::Shutdown,
        }
    }
}

#[cfg(any(feature = "sse-data-testing", test))]
pub mod test_support {
    pub fn example_block_added_1_0_0(block_hash: &str, height: &str) -> String {
        let raw_block_added = format!("{{\"BlockAdded\": {{ \"block_hash\": \"{block_hash}\", \"block\": {{ \"hash\": \"{block_hash}\", \"header\": {{ \"parent_hash\": \"4a28718301a83a43563ec42a184294725b8dd188aad7a9fceb8a2fa1400c680e\", \"state_root_hash\": \"63274671f2a860e39bb029d289e688526e4828b70c79c678649748e5e376cb07\", \"body_hash\": \"6da90c09f3fc4559d27b9fff59ab2453be5752260b07aec65e0e3a61734f656a\", \"random_bit\": true, \"accumulated_seed\": \"c8b4f30a3e3e082f4f206f972e423ffb23d152ca34241ff94ba76189716b61da\", \"era_end\": {{ \"era_report\": {{ \"equivocators\": [], \"rewards\": {{ \"01026ca707c348ed8012ac6a1f28db031fadd6eb67203501a353b867a08c8b9a80\": 1559401400039, \"010427c1d1227c9d2aafe8c06c6e6b276da8dcd8fd170ca848b8e3e8e1038a6dc8\": 25895190891 }}, \"inactive_validators\": [] }}, \"next_era_validator_weights\": {{ \"01026ca707c348ed8012ac6a1f28db031fadd6eb67203501a353b867a08c8b9a80\": \"50538244651768072\", \"010427c1d1227c9d2aafe8c06c6e6b276da8dcd8fd170ca848b8e3e8e1038a6dc8\": \"839230678448335\" }} }}, \"timestamp\": \"2021-04-08T05:14:14.912Z\", \"era_id\": 90, \"height\": {height}, \"protocol_version\": \"1.0.0\" }}, \"body\": {{ \"proposer\": \"012bac1d0ff9240ff0b7b06d555815640497861619ca12583ddef434885416e69b\", \"deploy_hashes\": [], \"transfer_hashes\": [] }} }} }}}}");
        let _ = super::deserialize(&raw_block_added).expect("malformed raw json"); // deserializing to make sure that the raw json string is in correct form
        raw_block_added
    }

    pub fn shutdown() -> String {
        let raw_shutdown = "\"Shutdown\"".to_string();
        let _ = super::deserialize(&raw_shutdown).expect("malformed raw json"); // deserializing to make sure that the raw json string is in correct form
        raw_shutdown
    }
}
