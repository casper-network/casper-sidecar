use anyhow::Error;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use casper_node::types::FinalitySignature as FinSig;

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

/// Describes a reference for the writing interface of an 'Event Store' database.
/// There is a one-to-one relationship between each method and each event that can be received from the node.
/// Each method takes the `data` and `id` fields as well as the source IP address (useful for tying the node-specific `id` to the relevant node).
///
/// For a reference implementation using Sqlite see, [SqliteDb](crate::SqliteDb)
#[async_trait]
pub trait DatabaseWriter {
    /// Save a BlockAdded event to the database.
    ///
    /// * `block_added`: the [BlockAdded] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_block_added(
        &self,
        block_added: BlockAdded,
        event_id: u64,
        event_source_address: String,
    ) -> Result<usize, Error>;
    /// Save a DeployAccepted event to the database.
    ///
    /// * `deploy_accepted`: the [DeployAccepted] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_deploy_accepted(
        &self,
        deploy_accepted: DeployAccepted,
        event_id: u64,
        event_source_address: String,
    ) -> Result<usize, Error>;
    /// Save a DeployProcessed event to the database.
    ///
    /// * `deploy_accepted`: the [DeployProcessed] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_deploy_processed(
        &self,
        deploy_processed: DeployProcessed,
        event_id: u64,
        event_source_address: String,
    ) -> Result<usize, Error>;
    /// Save a DeployExpired event to the database.
    ///
    /// * `deploy_expired`: the [DeployExpired] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_deploy_expired(
        &self,
        deploy_expired: DeployExpired,
        event_id: u64,
        event_source_address: String,
    ) -> Result<usize, Error>;
    /// Save a Fault event to the database.
    ///
    /// * `fault`: the [Fault] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_fault(
        &self,
        fault: Fault,
        event_id: u64,
        event_source_address: String,
    ) -> Result<usize, Error>;
    /// Save a FinalitySignature event to the database.
    ///
    /// * `finality_signature`: the [FinalitySignature] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_finality_signature(
        &self,
        finality_signature: FinalitySignature,
        event_id: u64,
        event_source_address: String,
    ) -> Result<usize, Error>;
    /// Save a Step event to the database.
    ///
    /// * `step`: the [Step] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_step(
        &self,
        step: Step,
        event_id: u64,
        event_source_address: String,
    ) -> Result<usize, Error>;
}

// todo document
#[async_trait]
pub trait DatabaseReader {
    async fn get_latest_block(&self) -> Result<BlockAdded, DatabaseRequestError>;
    async fn get_block_by_height(&self, height: u64) -> Result<BlockAdded, DatabaseRequestError>;
    async fn get_block_by_hash(&self, hash: &str) -> Result<BlockAdded, DatabaseRequestError>;
    async fn get_latest_deploy_aggregate(
        &self,
    ) -> Result<AggregateDeployInfo, DatabaseRequestError>;
    async fn get_deploy_aggregate_by_hash(
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
    async fn get_finality_signatures_by_block(
        &self,
        block_hash: &str,
    ) -> Result<Vec<FinSig>, DatabaseRequestError>;
}

#[derive(Debug)]
pub enum DatabaseRequestError {
    NotFound,
    InvalidParam(Error),
    Serialisation(Error),
    Unhandled(Error),
}
