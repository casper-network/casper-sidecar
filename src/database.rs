use anyhow::Error;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::types::sse_events::{
    BlockAdded, DeployAccepted, DeployExpired, DeployProcessed, Fault, FinalitySignature, Step,
};

#[derive(Debug, Deserialize, Serialize)]
pub struct AggregateDeployInfo {
    pub(crate) deploy_hash: String,
    pub(crate) deploy_accepted: Option<DeployAccepted>,
    pub(crate) deploy_processed: Option<DeployProcessed>,
    pub(crate) deploy_expired: bool,
}

#[async_trait]
pub trait DatabaseWriter {
    async fn save_block_added(
        &self,
        block_added: BlockAdded,
        event_id: u64,
        event_source_address: String,
    ) -> Result<usize, Error>;
    async fn save_deploy_accepted(
        &self,
        deploy_accepted: DeployAccepted,
        event_id: u64,
        event_source_address: String,
    ) -> Result<(), Error>;
    async fn save_deploy_processed(
        &self,
        deploy_processed: DeployProcessed,
        event_id: u64,
        event_source_address: String,
    ) -> Result<(), Error>;
    async fn save_deploy_expired(
        &self,
        deploy_expired: DeployExpired,
        event_id: u64,
        event_source_address: String,
    ) -> Result<(), Error>;
    async fn save_step(
        &self,
        step: Step,
        event_id: u64,
        event_source_address: String,
    ) -> Result<usize, Error>;
    async fn save_fault(
        &self,
        fault: Fault,
        event_id: u64,
        event_source_address: String,
    ) -> Result<usize, Error>;
    async fn save_finality_signature(
        &self,
        finality_signature: FinalitySignature,
    ) -> Result<usize, Error>;
}

#[async_trait]
pub trait DatabaseReader {
    async fn get_latest_block(&self) -> Result<BlockAdded, DatabaseRequestError>;
    async fn get_block_by_height(&self, height: u64) -> Result<BlockAdded, DatabaseRequestError>;
    async fn get_block_by_hash(&self, hash: &str) -> Result<BlockAdded, DatabaseRequestError>;
    async fn get_latest_deploy_aggregate(
        &self,
    ) -> Result<AggregateDeployInfo, DatabaseRequestError>;
    async fn get_deploy_by_hash_aggregate(
        &self,
        hash: &str,
    ) -> Result<AggregateDeployInfo, DatabaseRequestError>;
    async fn get_deploy_accepted_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployAccepted, DatabaseRequestError>;
    async fn get_deploy_processed_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployProcessed, DatabaseRequestError>;
    async fn get_deploy_expired_by_hash(&self, hash: &str) -> Result<bool, DatabaseRequestError>;
    async fn get_step_by_era(&self, era_id: u64) -> Result<Step, DatabaseRequestError>;
    async fn get_faults_by_public_key(
        &self,
        public_key: &str,
    ) -> Result<Vec<Fault>, DatabaseRequestError>;
    async fn get_faults_by_era(&self, era: u64) -> Result<Vec<Fault>, DatabaseRequestError>;
}

#[derive(Debug)]
pub enum DatabaseRequestError {
    NotFound,
    InvalidParam(Error),
    Serialisation(Error),
    Unhandled(Error),
}
