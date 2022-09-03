use crate::types::enums::Network;
use casper_node::types::{BlockHash, Deploy, DeployHash, JsonBlock};
use casper_types::{EraId, ExecutionEffect, ExecutionResult, PublicKey, TimeDiff, Timestamp};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Deserialize)]
pub struct Config {
    pub connection: ConnectionConfig,
    pub storage: StorageConfig,
    pub rest_server: ServerConfig,
    pub sse_server: ServerConfig,
}

#[derive(Deserialize)]
pub struct ServerConfig {
    pub ip_address: String,
    pub port: u16,
}

#[derive(Deserialize)]
pub struct StorageConfig {
    pub db_path: String,
    pub sse_cache: String,
}

#[derive(Deserialize)]
pub struct ConnectionConfig {
    pub network: Network,
    pub node: Node,
}

#[derive(Deserialize)]
pub struct Node {
    pub testnet: NodeConfig,
    pub mainnet: NodeConfig,
    pub local: NodeConfig,
}

#[derive(Clone, Deserialize)]
pub struct NodeConfig {
    pub ip_address: String,
    pub sse_port: u16,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BlockAdded {
    pub block_hash: BlockHash,
    pub block: JsonBlock,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeployAccepted {
    pub deploy: Arc<Deploy>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DeployProcessed {
    pub deploy_hash: Box<DeployHash>,
    pub account: Box<PublicKey>,
    pub timestamp: Timestamp,
    pub ttl: TimeDiff,
    pub dependencies: Vec<DeployHash>,
    pub block_hash: Box<BlockHash>,
    pub execution_result: Box<ExecutionResult>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeployExpired {
    pub deploy_hash: DeployHash,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Step {
    pub era_id: EraId,
    pub execution_effect: ExecutionEffect,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Fault {
    pub era_id: EraId,
    pub public_key: PublicKey,
    pub timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Faults(Vec<Fault>);

impl From<Vec<Fault>> for Faults {
    fn from(vec_faults: Vec<Fault>) -> Self {
        Self(vec_faults)
    }
}
