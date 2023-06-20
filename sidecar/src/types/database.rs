use crate::{
    sql::tables,
    types::sse_events::{
        BlockAdded, DeployAccepted, DeployExpired, DeployProcessed, Fault, FinalitySignature, Step,
    },
};
use anyhow::Error;
use async_trait::async_trait;
use casper_event_types::FinalitySignature as FinSig;
use casper_types::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sqlx::FromRow;
use std::sync::Arc;

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

    // Save data about shutdown to the database
    async fn save_shutdown(
        &self,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError>;

    /// Executes migration and stores current migration version
    ///
    /// * `migration`: migration to execute
    async fn execute_migration(&self, migration: Migration) -> Result<(), DatabaseWriteError>;

    /// Syncs deploy aggregates
    async fn update_pending_deploy_aggregates(&self) -> Result<usize, DatabaseWriteError>;
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
    /// Returns tuple of: 1. vector of paginated DeployAggregates that match the filter and a total number of DeployAggregates that match the filter outside the pagination.
    async fn list_deploy_aggregate(
        &self,
        filter: DeployAggregateFilter,
    ) -> Result<(Vec<DeployAggregate>, u32), DatabaseReadError>;

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

    /// Returns the [DeployExpired] corresponding to the given hex-encoded `hash`
    async fn get_deploy_expired_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployExpired, DatabaseReadError>;
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

    /// Returns number of events stored in db
    async fn get_number_of_events(&self) -> Result<u64, DatabaseReadError>;

    /// Gets the newest migration version.
    async fn get_newest_migration_version(&self) -> Result<Option<(u32, bool)>, DatabaseReadError>;
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
    /// The insert was rejected by the database.
    Database(sqlx::Error),
}

impl From<sqlx::Error> for DatabaseReadError {
    fn from(sqlx_err: sqlx::Error) -> Self {
        Self::Database(sqlx_err)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DeployAggregate {
    pub(crate) deploy_hash: String,
    pub(crate) deploy_accepted: Option<Box<RawValue>>,
    pub(crate) deploy_processed: Option<Box<RawValue>>,
    pub(crate) deploy_expired: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) block_timestamp: Option<Timestamp>,
}

#[derive(Debug, Deserialize, FromRow)]
pub struct DeployAggregateEntity {
    #[sqlx(rename = "deploy_hash")]
    pub(crate) deploy_hash: String,
    #[sqlx(rename = "deploy_accepted_raw")]
    pub(crate) deploy_accepted_raw: String,
    #[sqlx(rename = "deploy_processed_raw")]
    pub(crate) deploy_processed_raw: Option<String>,
    #[sqlx(rename = "is_expired")]
    pub(crate) is_expired: bool,
    #[sqlx(rename = "block_timestamp")]
    pub(crate) block_timestamp: Option<i64>,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeployAggregateSortColumn {
    BlockTimestamp,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    Desc,
    Asc,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DeployAggregateFilter {
    pub exclude_expired: bool,
    pub exclude_not_processed: bool,
    pub limit: u32,
    pub offset: u32,
    pub sort_column: Option<DeployAggregateSortColumn>,
    pub sort_order: Option<SortOrder>,
}
impl DeployAggregateFilter {
    #[cfg(test)]
    pub(crate) fn paginate(offset: u32, limit: u32) -> DeployAggregateFilter {
        DeployAggregateFilter {
            exclude_expired: false,
            exclude_not_processed: false,
            limit,
            offset,
            sort_column: None,
            sort_order: None,
        }
    }
}

#[allow(dead_code)] //Allowing dead code here because the Raw enum is used only in ITs
pub enum StatementWrapper {
    TableCreateStatement(Box<sea_query::TableCreateStatement>),
    IndexCreateStatement(Box<sea_query::IndexCreateStatement>),
    SelectStatement(sea_query::SelectStatement),
    InsertStatement(sea_query::InsertStatement),
    Raw(String),
}

#[async_trait]
pub trait MigrationScriptExecutor: Send + Sync {
    async fn execute(
        &self,
        transaction: Arc<dyn TransactionWrapper>,
    ) -> Result<(), DatabaseWriteError>;
}

#[async_trait]
pub trait TransactionWrapper: Send + Sync {
    async fn execute(&self, sql: &str) -> Result<(), DatabaseWriteError>;
}

#[derive(Clone)]
pub struct Migration {
    // version is optional to denote the special case of the first migration
    // that creates migrations table. No other migration should have version None.
    pub version: Option<u32>,
    pub statement_producers: fn() -> Result<Vec<StatementWrapper>, Error>,
    pub script_executor: Option<Arc<dyn MigrationScriptExecutor>>,
}

impl Migration {
    pub fn get_all_migrations() -> Vec<Migration> {
        vec![Migration::migration_1(), Migration::migration_2()]
    }

    pub fn initial() -> Migration {
        Migration {
            version: None,
            statement_producers: || {
                Ok(vec![StatementWrapper::TableCreateStatement(Box::new(
                    tables::migration::create_table_stmt(),
                ))])
            },
            script_executor: None,
        }
    }

    pub fn migration_2() -> Migration {
        Migration {
            version: Some(2),
            statement_producers: || {
                Ok(vec![
                    StatementWrapper::TableCreateStatement(Box::new(
                        tables::deploy_aggregate::create_table_stmt(),
                    )),
                    StatementWrapper::IndexCreateStatement(Box::new(
                        tables::deploy_aggregate::create_deploy_aggregate_block_hash_index(),
                    )),
                    StatementWrapper::IndexCreateStatement(Box::new(
                        tables::deploy_aggregate::create_deploy_aggregate_is_accepted_and_timestamp_index(),
                    )),
                    StatementWrapper::IndexCreateStatement(Box::new(
                        tables::deploy_aggregate::create_deploy_aggregate_is_processed_index(),
                    )),
                    StatementWrapper::TableCreateStatement(Box::new(
                        tables::pending_deploy_aggregations::create_table_stmt(),
                    )),
                ])
            },
            script_executor: None,
        }
    }

    pub fn migration_1() -> Migration {
        Migration {
            version: Some(1),
            statement_producers: || {
                let insert_types_stmt =
                    tables::event_type::create_initialise_stmt().map_err(|err| {
                        Error::msg(format!("Error building create_initialise_stmt: {:?}", err))
                    })?;
                let init_stmt = StatementWrapper::InsertStatement(insert_types_stmt);
                Ok(vec![
                    // Synthetic tables
                    StatementWrapper::TableCreateStatement(Box::new(
                        tables::event_type::create_table_stmt(),
                    )),
                    StatementWrapper::TableCreateStatement(Box::new(
                        tables::event_log::create_table_stmt(),
                    )),
                    StatementWrapper::TableCreateStatement(Box::new(
                        tables::deploy_event::create_table_stmt(),
                    )),
                    // Raw Event tables
                    StatementWrapper::TableCreateStatement(Box::new(
                        tables::block_added::create_table_stmt(),
                    )),
                    StatementWrapper::TableCreateStatement(Box::new(
                        tables::deploy_accepted::create_table_stmt(),
                    )),
                    StatementWrapper::TableCreateStatement(Box::new(
                        tables::deploy_processed::create_table_stmt(),
                    )),
                    StatementWrapper::TableCreateStatement(Box::new(
                        tables::deploy_expired::create_table_stmt(),
                    )),
                    StatementWrapper::TableCreateStatement(Box::new(
                        tables::fault::create_table_stmt(),
                    )),
                    StatementWrapper::TableCreateStatement(Box::new(
                        tables::finality_signature::create_table_stmt(),
                    )),
                    StatementWrapper::TableCreateStatement(Box::new(
                        tables::step::create_table_stmt(),
                    )),
                    StatementWrapper::TableCreateStatement(Box::new(
                        tables::shutdown::create_table_stmt(),
                    )),
                    init_stmt,
                ])
            },
            script_executor: None,
        }
    }

    pub fn get_version(&self) -> Option<u32> {
        self.version
    }

    pub fn get_migrations(&self) -> Result<Vec<StatementWrapper>, Error> {
        (self.statement_producers)()
    }
}
