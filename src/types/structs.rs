use crate::types::enums::Network;
use casper_node::types::{BlockHash, DeployHash, JsonBlock};
use casper_types::{EraId, ExecutionEffect, ExecutionResult, PublicKey, TimeDiff, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct Config {
    pub connection: ConnectionConfig,
    pub storage: StorageConfig,
    pub rest_server: ServerConfig,
    pub sse_server: ServerConfig,
}

#[derive(Deserialize)]
pub struct ServerConfig {
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

#[derive(Deserialize)]
pub struct NodeConfig {
    pub ip_address: String,
    pub sse_port: u16,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BlockAdded {
    pub block_hash: String,
    pub block: JsonBlock,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeployAccepted {
    pub deploy: DeployHash,
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
