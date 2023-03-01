use anyhow::Error;
use casper_node::types::{Block, BlockHash, Deploy, DeployHash, FinalitySignature, JsonBlock};
use casper_types::{
    EraId, ExecutionEffect, ExecutionResult, ProtocolVersion, PublicKey, TimeDiff, Timestamp,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::sse_data;
pub fn deserialize(raw_data: &str) -> Result<super::sse_data::SseData, Error> {
    serde_json::from_str::<SseData>(raw_data)
        .map_err(|err| {
            let error_message = format!("Serde Error: {}", err);
            Error::msg(error_message)
        })
        .and_then(|el| el.to_contemporary_representation())
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

impl SseData {
    fn to_contemporary_representation(&self) -> Result<super::sse_data::SseData, Error> {
        match self {
            Self::BlockAdded { block_hash, block } => {
                let json_block = JsonBlock::new((**block).clone(), None);
                let block_added = sse_data::SseData::BlockAdded {
                    block_hash: *block_hash,
                    block: Box::new(json_block),
                };
                Ok(block_added)
            }
            el => {
                let raw = serde_json::to_string(el).map_err(|err| {
                    let error_message = format!("Serde Error: {}", err);
                    Error::msg(error_message)
                })?;
                sse_data::deserialize(&raw)
            }
        }
    }
}
