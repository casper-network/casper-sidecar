//! Common types used when dealing with serialization/deserialization of data from nodes,
//! also a "contemporary" data model which is based on 2.0.x node specification

/// A filter for event types a client has subscribed to receive.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum EventFilter {
    ApiVersion,
    SidecarVersion,
    BlockAdded,
    TransactionAccepted,
    TransactionProcessed,
    TransactionExpired,
    Fault,
    FinalitySignature,
    Step,
}

#[cfg(feature = "sse-data-testing")]
use super::testing;
#[cfg(feature = "sse-data-testing")]
use casper_types::ChainNameDigest;
use casper_types::{
    contract_messages::Messages, execution::ExecutionResult, Block, BlockHash, EraId,
    FinalitySignature, InitiatorAddr, ProtocolVersion, PublicKey, TimeDiff, Timestamp, Transaction,
    TransactionHash,
};
#[cfg(feature = "sse-data-testing")]
use casper_types::{execution::ExecutionResultV2, testing::TestRng, TestBlockBuilder};
#[cfg(feature = "sse-data-testing")]
use rand::Rng;
use serde::{Deserialize, Serialize};
#[cfg(feature = "sse-data-testing")]
use serde_json::value::to_raw_value;
use serde_json::value::RawValue;
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

/// Deserializes a string which should contain json data and returns a result of either SseData (which is 2.0.x compliant) or an SseDataDeserializeError
///
/// * `json_raw`: string slice which should contain raw json data.
pub fn deserialize(json_raw: &str) -> Result<(SseData, bool), SseDataDeserializeError> {
    serde_json::from_str::<SseData>(json_raw)
        .map(|el| (el, false))
        .map_err(|err| {
            let error_message = format!("Serde Error: {}", err);
            to_error(error_message)
        })
}

/// The "data" field of the events sent on the event stream to clients.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum SseData {
    /// The version of this node's API server.  This event will always be the first sent to a new
    /// client, and will have no associated event ID provided.
    ApiVersion(ProtocolVersion),
    /// This event is specific only to the Sidecar. For now it is put here but ultimately we will
    /// need to split the way we store events going to outbound so we can discern between events
    /// coming from inbound and the sidecar itself
    SidecarVersion(ProtocolVersion),
    /// The given block has been added to the linear chain and stored locally.
    BlockAdded {
        block_hash: BlockHash,
        block: Box<Block>,
    },
    /// The given transaction has been newly-accepted by this node.
    TransactionAccepted(Arc<Transaction>),
    /// The given transaction has been executed, committed and forms part of the given block.
    TransactionProcessed {
        transaction_hash: Box<TransactionHash>,
        initiator_addr: Box<InitiatorAddr>,
        timestamp: Timestamp,
        ttl: TimeDiff,
        block_hash: Box<BlockHash>,
        execution_result: Box<ExecutionResult>,
        messages: Messages,
    },
    /// The given transaction has expired.
    TransactionExpired { transaction_hash: TransactionHash },
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
        execution_effects: Box<RawValue>,
    },
    /// The node is about to shut down.
    Shutdown,
}

impl SseData {
    pub fn type_label(&self) -> &str {
        match self {
            SseData::ApiVersion(_) => "ApiVersion",
            SseData::SidecarVersion(_) => "SidecarVersion",
            SseData::BlockAdded { .. } => "BlockAdded",
            SseData::TransactionAccepted(_) => "TransactionAccepted",
            SseData::TransactionProcessed { .. } => "TransactionProcessed",
            SseData::TransactionExpired { .. } => "TransactionExpired",
            SseData::Fault { .. } => "Fault",
            SseData::FinalitySignature(_) => "FinalitySignature",
            SseData::Step { .. } => "Step",
            SseData::Shutdown => "Shutdown",
        }
    }
    pub fn should_include(&self, filter: &[EventFilter]) -> bool {
        match self {
            SseData::Shutdown => true,
            //Keeping the rest part as explicit match so that if a new variant is added, it will be caught by the compiler            SseData::Shutdown
            SseData::SidecarVersion(_) => filter.contains(&EventFilter::SidecarVersion),
            SseData::ApiVersion(_) => filter.contains(&EventFilter::ApiVersion),
            SseData::BlockAdded { .. } => filter.contains(&EventFilter::BlockAdded),
            SseData::TransactionAccepted { .. } => {
                filter.contains(&EventFilter::TransactionAccepted)
            }
            SseData::TransactionProcessed { .. } => {
                filter.contains(&EventFilter::TransactionProcessed)
            }
            SseData::TransactionExpired { .. } => filter.contains(&EventFilter::TransactionExpired),
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
            rng.gen_range(2..10),
            rng.gen::<u8>() as u32,
            rng.gen::<u8>() as u32,
        );
        SseData::ApiVersion(protocol_version)
    }

    /// Returns a random `SseData::BlockAdded`.
    pub fn random_block_added(rng: &mut TestRng) -> Self {
        let block = TestBlockBuilder::new().build(rng);
        SseData::BlockAdded {
            block_hash: *block.hash(),
            block: Box::new(block.into()),
        }
    }

    /// Returns a random `SseData::DeployAccepted`, along with the random `Deploy`.
    pub fn random_transaction_accepted(rng: &mut TestRng) -> (Self, Transaction) {
        let transaction = Transaction::random(rng);
        let event = SseData::TransactionAccepted(Arc::new(transaction.clone()));
        (event, transaction)
    }

    /// Returns a random `SseData::DeployProcessed`.
    pub fn random_transaction_processed(rng: &mut TestRng) -> Self {
        let transaction = Transaction::random(rng);
        let timestamp = match &transaction {
            Transaction::Deploy(deploy) => deploy.header().timestamp(),
            Transaction::V1(v1_transaction) => v1_transaction.timestamp(),
        };
        let ttl = match &transaction {
            Transaction::Deploy(deploy) => deploy.header().ttl(),
            Transaction::V1(v1_transaction) => v1_transaction.ttl(),
        };

        SseData::TransactionProcessed {
            transaction_hash: Box::new(TransactionHash::random(rng)),
            initiator_addr: Box::new(transaction.initiator_addr().clone()),
            timestamp,
            ttl,
            block_hash: Box::new(BlockHash::random(rng)),
            execution_result: Box::new(ExecutionResult::random(rng)),
            messages: rng.random_vec(1..5),
        }
    }

    /// Returns a random `SseData::DeployExpired`
    pub fn random_transaction_expired(rng: &mut TestRng) -> Self {
        let transaction = testing::create_expired_transaction(Timestamp::now(), rng);
        SseData::TransactionExpired {
            transaction_hash: transaction.hash(),
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
        let block_hash = BlockHash::random(rng);
        let block_height = rng.gen::<u64>();
        let era_id = EraId::random(rng);
        let chain_name_digest = ChainNameDigest::random(rng);
        SseData::FinalitySignature(Box::new(FinalitySignature::random_for_block(
            block_hash,
            block_height,
            era_id,
            chain_name_digest,
            rng,
        )))
    }

    /// Returns a random `SseData::Step`.
    pub fn random_step(rng: &mut TestRng) -> Self {
        let execution_effects = match ExecutionResultV2::random(rng) {
            ExecutionResultV2::Success { effects, .. }
            | ExecutionResultV2::Failure { effects, .. } => effects,
        };
        SseData::Step {
            era_id: EraId::new(rng.gen()),
            execution_effects: to_raw_value(&execution_effects).unwrap(),
        }
    }

    /// Returns a random `SseData::SidecarVersion`.
    pub fn random_sidecar_version(rng: &mut TestRng) -> Self {
        let protocol_version = ProtocolVersion::from_parts(
            rng.gen_range(2..10),
            rng.gen::<u8>() as u32,
            rng.gen::<u8>() as u32,
        );
        SseData::SidecarVersion(protocol_version)
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
    pub const BLOCK_HASH_4: &str =
        "000625a798318315a4f401828f6d53371a623d79653db03a79a4cfbdd1e4ae53";

    pub fn example_api_version() -> String {
        "{\"ApiVersion\":\"2.0.0\"}".to_string()
    }

    pub fn shutdown() -> String {
        "\"Shutdown\"".to_string()
    }

    pub fn example_block_added_2_0_0(hash: &str, height: &str) -> String {
        let raw_block_added = format!("{{\"BlockAdded\":{{\"block_hash\":\"{hash}\",\"block\":{{\"Version2\":{{\"hash\":\"{hash}\",\"header\":{{\"parent_hash\":\"e38f28265439296d106cf111869cd17a3ca114707ae2c82b305bf830f90a36a5\",\"state_root_hash\":\"e7ec15c0700717850febb2a0a67ee5d3a55ddb121b1fc70e5bcf154e327fe6c6\",\"body_hash\":\"5ad04cda6912de119d776045d44a4266e05eb768d4c1652825cc19bce7030d2c\",\"random_bit\":false,\"accumulated_seed\":\"bbcabbb76ac8714a37e928b7f0bde4caeddf5e446e51a36ceab9a34f5e983b92\",\"era_end\":null,\"timestamp\":\"2024-02-22T08:18:44.352Z\",\"era_id\":2,\"height\":{height},\"protocol_version\":\"2.0.0\"}},\"body\":{{\"proposer\":\"01302f30e5a5a00b2a0afbfbe9e63b3a9feb278d5f1944ba5efffa15fbb2e8a2e6\",\"transfer\":[],\"staking\":[],\"install_upgrade\":[],\"standard\":[{{\"Deploy\":\"2e3083dbf5344c82efeac5e1a079bfd94acc1dfb454da0d92970f2e18e3afa9f\"}}],\"rewarded_signatures\":[[248],[0],[0]]}}}}}}}}}}");
        super::deserialize(&raw_block_added).unwrap(); // deserializing to make sure that the raw json string is in correct form
        raw_block_added
    }

    pub fn example_finality_signature_2_0_0(hash: &str) -> String {
        let raw_block_added = format!("{{\"FinalitySignature\":{{\"block_hash\":\"{hash}\",\"era_id\":2,\"signature\":\"01ff6089c9b187f38ba61b518082db22552fb4762d505773e8221f6593c45e0602de560c4690b035dbacba9ab9dbe63e97d928970a515ea6a25fb920b3e9099d05\",\"public_key\":\"01914182c7d11ef13dccdbf1470648af3c3cd7f570bc351f0c14112370b19b8331\"}}}}");
        super::deserialize(&raw_block_added).unwrap(); // deserializing to make sure that the raw json string is in correct form
        raw_block_added
    }
}
