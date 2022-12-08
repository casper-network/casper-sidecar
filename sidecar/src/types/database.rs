use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use casper_node::types::FinalitySignature as FinSig;

use crate::types::sse_events::{
    BlockAdded, DeployAccepted, DeployExpired, DeployProcessed, Fault, FinalitySignature, Step,
};

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
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError>;
    /// Save a DeployAccepted event to the database.
    ///
    /// * `deploy_accepted`: the [DeployAccepted] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_deploy_accepted(
        &self,
        deploy_accepted: DeployAccepted,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError>;
    /// Save a DeployProcessed event to the database.
    ///
    /// * `deploy_accepted`: the [DeployProcessed] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_deploy_processed(
        &self,
        deploy_processed: DeployProcessed,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError>;
    /// Save a DeployExpired event to the database.
    ///
    /// * `deploy_expired`: the [DeployExpired] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_deploy_expired(
        &self,
        deploy_expired: DeployExpired,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError>;
    /// Save a Fault event to the database.
    ///
    /// * `fault`: the [Fault] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_fault(
        &self,
        fault: Fault,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError>;
    /// Save a FinalitySignature event to the database.
    ///
    /// * `finality_signature`: the [FinalitySignature] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_finality_signature(
        &self,
        finality_signature: FinalitySignature,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError>;
    /// Save a Step event to the database.
    ///
    /// * `step`: the [Step] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_step(
        &self,
        step: Step,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError>;
}

#[derive(Debug)]
pub struct UniqueConstraintError {
    pub table: String,
    pub error: sqlx::Error,
}

/// The database failed to insert a record(s).
#[derive(Debug)]
pub enum DatabaseWriteError {
    /// The insert failed prior to execution because the data could not be serialised.
    Serialisation(serde_json::Error),
    /// The insert failed prior to execution because the SQL could not be constructed.
    SqlConstruction(sea_query::error::Error),
    /// The insert was rejected by the database because it would break a unique constraint.
    UniqueConstraint(UniqueConstraintError),
    /// The insert was rejected by the database.
    Database(sqlx::Error),
    /// An error occurred somewhere unexpected.
    Unhandled(anyhow::Error),
}

impl ToString for DatabaseWriteError {
    fn to_string(&self) -> String {
        format!("{:?}", self)
    }
}

impl From<serde_json::Error> for DatabaseWriteError {
    fn from(serde_err: serde_json::Error) -> Self {
        Self::Serialisation(serde_err)
    }
}

impl From<sea_query::error::Error> for DatabaseWriteError {
    fn from(sea_query_err: sea_query::error::Error) -> Self {
        Self::SqlConstruction(sea_query_err)
    }
}

impl From<sqlx::Error> for DatabaseWriteError {
    fn from(sqlx_err: sqlx::Error) -> Self {
        if let Some(db_err) = sqlx_err.as_database_error() {
            if let Some(code) = db_err.code() {
                match code.as_ref() {
                    "1555" | "2067" => {
                        // The message looks something like this:
                        // UNIQUE constraint failed: DeployProcessed.deploy_hash

                        let table = db_err.message().split(':').collect::<Vec<&str>>()[1]
                            .split('.')
                            .collect::<Vec<&str>>()[0]
                            .trim()
                            .to_string();
                        return Self::UniqueConstraint(UniqueConstraintError {
                            table,
                            error: sqlx_err,
                        });
                    }
                    _ => {}
                }
            }
        }
        Self::Database(sqlx_err)
    }
}

impl From<anyhow::Error> for DatabaseWriteError {
    fn from(anyhow_err: anyhow::Error) -> Self {
        Self::Unhandled(anyhow_err)
    }
}

/// Describes a reference for the reading interface of an 'Event Store' database.
///
/// For a reference implementation using Sqlite see, [SqliteDb](crate::SqliteDb)
#[async_trait]
pub trait DatabaseReader {
    /// Returns the latest [BlockAdded] by height from the database.
    async fn get_latest_block(&self) -> Result<BlockAdded, DatabaseReadError>;
    /// Returns the [BlockAdded] corresponding to the provided [height].
    async fn get_block_by_height(&self, height: u64) -> Result<BlockAdded, DatabaseReadError>;
    /// Returns the [BlockAdded] corresponding to the provided hex-encoded [hash].
    async fn get_block_by_hash(&self, hash: &str) -> Result<BlockAdded, DatabaseReadError>;
    /// Returns an aggregate of the deploy's events corresponding to the given hex-encoded `hash`
    async fn get_deploy_aggregate_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployAggregate, DatabaseReadError>;
    /// Returns the [DeployAccepted] corresponding to the given hex-encoded `hash`
    async fn get_deploy_accepted_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployAccepted, DatabaseReadError>;
    /// Returns the [DeployProcessed] corresponding to the given hex-encoded `hash`
    async fn get_deploy_processed_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployProcessed, DatabaseReadError>;
    /// Returns a boolean representing the expired state of the deploy corresponding to the given hex-encoded `hash`.
    ///
    /// * If there is a record present it will return `true` meaning the [Deploy] has expired.
    /// * If there is no record present it will return [NotFound](DatabaseRequestError::NotFound) rather than `false`.
    /// This is because the lack of a record does not definitely mean it hasn't expired. The deploy could have expired
    /// prior to sidecar's start point. Calling [get_deploy_aggregate_by_hash] can help in this case, if there is a [DeployAccepted]
    /// without a corresponding [DeployExpired] then you can assert that it truly has not expired.
    async fn get_deploy_expired_by_hash(&self, hash: &str) -> Result<bool, DatabaseReadError>;
    /// Returns all [Fault]s that correspond to the given hex-encoded [public_key]
    async fn get_faults_by_public_key(
        &self,
        public_key: &str,
    ) -> Result<Vec<Fault>, DatabaseReadError>;
    /// Returns all [Fault]s that occurred in the given [era]
    async fn get_faults_by_era(&self, era: u64) -> Result<Vec<Fault>, DatabaseReadError>;
    /// Returns all [FinalitySignature](casper_node::types::FinalitySignature)s for the given hex-encoded `block_hash`.
    async fn get_finality_signatures_by_block(
        &self,
        block_hash: &str,
    ) -> Result<Vec<FinSig>, DatabaseReadError>;
    /// Returns the [Step] event for the given era.
    async fn get_step_by_era(&self, era: u64) -> Result<Step, DatabaseReadError>;
}

/// The database was unable to fulfil the request.
#[derive(Debug)]
pub enum DatabaseReadError {
    /// The requested record was not present in the database.
    NotFound,
    /// An error occurred serialising or deserialising data from the database.
    Serialisation(serde_json::Error),
    /// An error occurred somewhere unexpected.
    Unhandled(anyhow::Error),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DeployAggregate {
    pub(crate) deploy_hash: String,
    pub(crate) deploy_accepted: Option<DeployAccepted>,
    pub(crate) deploy_processed: Option<DeployProcessed>,
    pub(crate) deploy_expired: bool,
}
