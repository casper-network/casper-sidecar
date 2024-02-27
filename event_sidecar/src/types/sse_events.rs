#[cfg(test)]
use casper_types::ChainNameDigest;
use casper_types::FinalitySignature as FinSig;
use casper_types::{
    contract_messages::Messages, execution::ExecutionResult, AsymmetricType, Block, BlockHash,
    EraId, InitiatorAddr, ProtocolVersion, PublicKey, TimeDiff, Timestamp, Transaction,
    TransactionHash,
};
#[cfg(test)]
use casper_types::{
    execution::{execution_result_v1::ExecutionResultV1, Effects, ExecutionResultV2},
    testing::TestRng,
    TestBlockBuilder, TestBlockV1Builder,
};
use derive_new::new;
use hex::ToHex;
#[cfg(test)]
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};
use utoipa::ToSchema;

use crate::sql::tables::transaction_type::TransactionTypeId;

/// The version of this node's API server.  This event will always be the first sent to a new
/// client, and will have no associated event ID provided.
#[derive(Clone, Debug, Serialize, Deserialize, new)]
pub struct ApiVersion(ProtocolVersion);

/// The given block has been added to the linear chain and stored locally.
#[derive(Clone, Debug, Serialize, Deserialize, new, ToSchema)]
pub struct BlockAdded {
    block_hash: BlockHash,
    block: Box<Block>,
}

#[cfg(test)]
pub fn random_execution_result(rng: &mut TestRng) -> ExecutionResult {
    match rng.gen_range(0..2) {
        0 => {
            let result_v1: ExecutionResultV1 = rng.gen();
            ExecutionResult::V1(result_v1)
        }
        1 => {
            let result_v2: ExecutionResultV2 = rng.gen();
            ExecutionResult::V2(result_v2)
        }
        _ => panic!("Unexpected value"),
    }
}

#[cfg(test)]
impl BlockAdded {
    pub fn random(rng: &mut TestRng) -> Self {
        let block = match rng.gen_range(0..2) {
            0 => {
                let block_v1 = TestBlockV1Builder::default().build(rng);
                Block::V1(block_v1)
            }
            1 => {
                let block_v2 = TestBlockBuilder::default().build(rng);
                Block::V2(block_v2)
            }
            _ => panic!("Unexpected value"),
        };
        Self {
            block_hash: *block.hash(),
            block: Box::new(block),
        }
    }
}

impl BlockAdded {
    pub fn hex_encoded_hash(&self) -> String {
        hex::encode(self.block_hash.inner())
    }

    pub fn get_height(&self) -> u64 {
        self.block.height()
    }
}

/// The given transaction has been newly-accepted by this node.
#[derive(Clone, Debug, Serialize, Deserialize, new, ToSchema)]
pub struct TransactionAccepted {
    // It's an Arc to not create multiple copies of the same transaction for multiple subscribers.
    transaction: Arc<Transaction>,
}

impl TransactionAccepted {
    pub fn identifier(&self) -> String {
        transaction_hash_to_identifier(&self.transaction.hash())
    }

    pub fn transaction_type_id(&self) -> TransactionTypeId {
        match *self.transaction {
            Transaction::Deploy(_) => TransactionTypeId::Deploy,
            Transaction::V1(_) => TransactionTypeId::Version1,
        }
    }

    #[cfg(test)]
    pub fn api_transaction_type_id(&self) -> crate::types::database::TransactionTypeId {
        match *self.transaction {
            Transaction::Deploy(_) => crate::types::database::TransactionTypeId::Deploy,
            Transaction::V1(_) => crate::types::database::TransactionTypeId::Version1,
        }
    }

    #[cfg(test)]
    pub fn random(rng: &mut TestRng) -> Self {
        Self {
            transaction: Arc::new(Transaction::random(rng)),
        }
    }

    #[cfg(test)]
    pub fn transaction_hash(&self) -> TransactionHash {
        self.transaction.hash().to_owned()
    }

    pub fn hex_encoded_hash(&self) -> String {
        let hex_fmt: String = match self.transaction.hash() {
            TransactionHash::Deploy(deploy) => deploy.encode_hex(),
            TransactionHash::V1(transaction) => transaction.encode_hex(),
        };
        hex_fmt
    }
}

/// The given transaction has been executed, committed and forms part of the given block.
#[derive(Clone, Debug, Serialize, Deserialize, new, ToSchema)]
pub struct TransactionProcessed {
    transaction_hash: Box<TransactionHash>,
    #[schema(value_type = String)]
    initiator_addr: Box<InitiatorAddr>,
    #[schema(value_type = String)]
    timestamp: Timestamp,
    #[schema(value_type = String)]
    ttl: TimeDiff,
    block_hash: Box<BlockHash>,
    //#[data_size(skip)]
    execution_result: Box<ExecutionResult>,
    messages: Messages,
}

impl TransactionProcessed {
    pub fn identifier(&self) -> String {
        transaction_hash_to_identifier(&self.transaction_hash)
    }

    pub fn transaction_type_id(&self) -> TransactionTypeId {
        match *self.transaction_hash.as_ref() {
            TransactionHash::Deploy(_) => TransactionTypeId::Deploy,
            TransactionHash::V1(_) => TransactionTypeId::Version1,
        }
    }

    #[cfg(test)]
    pub fn api_transaction_type_id(&self) -> crate::types::database::TransactionTypeId {
        match *self.transaction_hash.as_ref() {
            TransactionHash::Deploy(_) => crate::types::database::TransactionTypeId::Deploy,
            TransactionHash::V1(_) => crate::types::database::TransactionTypeId::Version1,
        }
    }

    #[cfg(test)]
    pub fn random(rng: &mut TestRng, with_transaction_hash: Option<TransactionHash>) -> Self {
        let transaction = Transaction::random(rng);
        let ttl = match &transaction {
            Transaction::Deploy(deploy) => deploy.ttl(),
            Transaction::V1(transaction) => transaction.ttl(),
        };
        let timestamp = match &transaction {
            Transaction::Deploy(deploy) => deploy.timestamp(),
            Transaction::V1(transaction) => transaction.timestamp(),
        };
        let initiator_addr = Box::new(transaction.initiator_addr());
        Self {
            transaction_hash: Box::new(with_transaction_hash.unwrap_or(transaction.hash())),
            initiator_addr,
            timestamp,
            ttl,
            block_hash: Box::new(BlockHash::random(rng)),
            execution_result: Box::new(random_execution_result(rng)),
            messages: rng.random_vec(1..5),
        }
    }

    pub fn hex_encoded_hash(&self) -> String {
        match *self.transaction_hash.as_ref() {
            TransactionHash::Deploy(deploy_hash) => deploy_hash.encode_hex(),
            TransactionHash::V1(v1_hash) => v1_hash.encode_hex(),
        }
    }
}

/// The given transaction has expired.
#[derive(Clone, Debug, Serialize, Deserialize, new, ToSchema)]
pub struct TransactionExpired {
    transaction_hash: TransactionHash,
}

impl TransactionExpired {
    pub fn identifier(&self) -> String {
        transaction_hash_to_identifier(&self.transaction_hash)
    }

    pub fn transaction_type_id(&self) -> TransactionTypeId {
        match self.transaction_hash {
            TransactionHash::Deploy(_) => TransactionTypeId::Deploy,
            TransactionHash::V1(_) => TransactionTypeId::Version1,
        }
    }

    #[cfg(test)]
    pub fn api_transaction_type_id(&self) -> crate::types::database::TransactionTypeId {
        match self.transaction_hash {
            TransactionHash::Deploy(_) => crate::types::database::TransactionTypeId::Deploy,
            TransactionHash::V1(_) => crate::types::database::TransactionTypeId::Version1,
        }
    }

    #[cfg(test)]
    pub fn random(rng: &mut TestRng, with_transaction_hash: Option<TransactionHash>) -> Self {
        Self {
            transaction_hash: with_transaction_hash.unwrap_or_else(|| TransactionHash::random(rng)),
        }
    }

    pub fn hex_encoded_hash(&self) -> String {
        match self.transaction_hash {
            TransactionHash::Deploy(deploy_hash) => deploy_hash.encode_hex(),
            TransactionHash::V1(v1_hash) => v1_hash.encode_hex(),
        }
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

impl From<FinalitySignature> for FinSig {
    fn from(val: FinalitySignature) -> Self {
        *val.0
    }
}

impl FinalitySignature {
    #[cfg(test)]
    pub fn random(rng: &mut TestRng) -> Self {
        let block_hash = BlockHash::random(rng);
        let block_height = rng.gen::<u64>();
        let era_id = EraId::random(rng);
        let chain_name_digest = ChainNameDigest::random(rng);
        Self(Box::new(FinSig::random_for_block(
            block_hash,
            block_height,
            era_id,
            chain_name_digest,
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

        let execution_effect = Effects::random(rng);
        Self {
            era_id: EraId::new(rng.gen()),
            execution_effect: to_raw_value(&execution_effect).unwrap(),
        }
    }
}

fn transaction_hash_to_identifier(transaction_hash: &TransactionHash) -> String {
    match transaction_hash {
        TransactionHash::Deploy(deploy) => hex::encode(deploy.inner()),
        TransactionHash::V1(transaction) => hex::encode(transaction.inner()),
    }
}
