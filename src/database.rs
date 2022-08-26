use anyhow::Error;
use async_trait::async_trait;
use serde::Serialize;

use casper_node::types::{Block, Deploy, FinalitySignature};

use crate::types::structs::{
    BlockAdded, DeployAccepted, DeployExpired, DeployProcessed, Fault, Step,
};

#[derive(Debug, Serialize)]
pub struct AggregateDeployInfo {
    pub(crate) deploy_hash: String,
    pub(crate) deploy_accepted: Option<String>,
    pub(crate) deploy_processed: Option<String>,
    pub(crate) deploy_expired: bool,
}

#[async_trait]
pub(crate) trait DatabaseWriter {
    async fn save_block_added(&self, block_added: BlockAdded) -> Result<usize, Error>;
    async fn save_deploy_accepted(&self, deploy_accepted: DeployAccepted) -> Result<usize, Error>;
    async fn save_deploy_processed(
        &self,
        deploy_processed: DeployProcessed,
    ) -> Result<usize, Error>;
    async fn save_deploy_expired(&self, deploy_expired: DeployExpired) -> Result<usize, Error>;
    async fn save_step(&self, step: Step) -> Result<usize, Error>;
    async fn save_fault(&self, fault: Fault) -> Result<usize, Error>;
    async fn save_finality_signature(
        &self,
        finality_signature: FinalitySignature,
    ) -> Result<usize, Error>;
}

#[async_trait]
pub(crate) trait DatabaseReader {
    async fn get_latest_block(&self) -> Result<Block, DatabaseRequestError>;
    async fn get_block_by_height(&self, height: u64) -> Result<Block, DatabaseRequestError>;
    async fn get_block_by_hash(&self, hash: &str) -> Result<Block, DatabaseRequestError>;
    async fn get_latest_deploy_aggregate(
        &self,
    ) -> Result<AggregateDeployInfo, DatabaseRequestError>;
    async fn get_deploy_by_hash_aggregate(
        &self,
        hash: &str,
    ) -> Result<AggregateDeployInfo, DatabaseRequestError>;
    async fn get_deploy_accepted_by_hash(&self, hash: &str)
        -> Result<Deploy, DatabaseRequestError>;
    async fn get_deploy_processed_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployProcessed, DatabaseRequestError>;
    async fn get_deploy_expired_by_hash(&self, hash: &str) -> Result<bool, DatabaseRequestError>;
    async fn get_step_by_era(&self, era_id: u64) -> Result<Step, DatabaseRequestError>;
    async fn get_fault_by_public_key(
        &self,
        public_key: &str,
    ) -> Result<Fault, DatabaseRequestError>;
}

#[derive(Debug)]
pub(crate) enum DatabaseRequestError {
    DBConnectionFailed(Error),
    NotFound,
    InvalidParam(Error),
    Serialisation(Error),
    Unhandled(Error),
}
