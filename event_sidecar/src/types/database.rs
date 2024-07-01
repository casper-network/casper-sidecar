use crate::{
    database::{
        postgresql_database::PostgreSqlDatabase,
        sqlite_database::SqliteDatabase,
        types::{DDLConfiguration, SseEnvelope},
    },
    sql::tables::{self, transaction_type::TransactionTypeId as SqlTransactionTypeId},
    types::sse_events::{
        BlockAdded, Fault, FinalitySignature, Step, TransactionAccepted, TransactionExpired,
        TransactionProcessed,
    },
    StorageConfig,
};
use anyhow::{Context, Error};
use async_trait::async_trait;
use casper_types::FinalitySignature as FinSig;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::{
    fmt::{Display, Formatter},
    path::Path,
};
use tokio::sync::OnceCell;
use utoipa::ToSchema;

pub enum TransactionTypeId {
    Deploy,
    Version1,
}

impl From<&TransactionTypeId> for u8 {
    fn from(transaction_type: &TransactionTypeId) -> u8 {
        let sql_transaction_type = match transaction_type {
            TransactionTypeId::Deploy => SqlTransactionTypeId::Deploy,
            TransactionTypeId::Version1 => SqlTransactionTypeId::Version1,
        };
        sql_transaction_type as u8
    }
}

#[cfg(test)]
impl Display for TransactionTypeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionTypeId::Deploy => write!(f, "deploy"),
            TransactionTypeId::Version1 => write!(f, "version1"),
        }
    }
}

#[derive(Clone)]
pub enum Database {
    SqliteDatabaseWrapper(SqliteDatabase),
    PostgreSqlDatabaseWrapper(PostgreSqlDatabase),
}

/// Wrapper for a Database that is lazily initialized.
/// Using this structure ensures that the database connection is not being initialized
/// if the db-dependant components are configured, but disabled.
#[derive(Clone)]
pub struct LazyDatabaseWrapper {
    config: StorageConfig,
    resource: Arc<OnceCell<Result<Database, DatabaseInitializationError>>>,
}

impl LazyDatabaseWrapper {
    pub fn new(config: StorageConfig) -> Self {
        let cell = Arc::new(tokio::sync::OnceCell::new());
        Self {
            config,
            resource: cell,
        }
    }

    pub async fn acquire(&self) -> &Result<Database, DatabaseInitializationError> {
        let config_ref = &self.config;
        self.resource
            .get_or_init(|| async move { Database::build(&config_ref.clone()).await })
            .await
    }

    #[cfg(any(feature = "testing", test))]
    pub fn for_tests() -> Self {
        let db = Database::for_tests();
        let cell = Arc::new(tokio::sync::OnceCell::from(Ok(db)));
        Self {
            config: StorageConfig::default(),
            resource: cell,
        }
    }
}

impl Database {
    pub async fn build(config: &StorageConfig) -> Result<Database, DatabaseInitializationError> {
        if let Some(sqlite_config) = &config.sqlite_config {
            let sqlite_database =
                SqliteDatabase::new(Path::new(&config.storage_folder), sqlite_config.clone())
                    .await
                    .context("Error instantiating sqlite database")
                    .map_err(DatabaseInitializationError::from)?;
            return Ok(Database::SqliteDatabaseWrapper(sqlite_database));
        } else if let Some(postgresql) = &config.postgresql_config {
            let postgres_database = PostgreSqlDatabase::new(postgresql.clone())
                .await
                .context("Error instantiating postgres database")
                .map_err(DatabaseInitializationError::from)?;
            return Ok(Database::PostgreSqlDatabaseWrapper(postgres_database));
        }
        Err("Tried to build database without any enabled database configuration".into())
    }

    #[cfg(any(feature = "testing", test))]
    pub fn for_tests() -> Database {
        let sqlite_database = SqliteDatabase::new_in_memory_no_migrations(100).unwrap();
        Database::SqliteDatabaseWrapper(sqlite_database)
    }
}

/// Describes a reference for the writing interface of an 'Event Store' database.
/// There is a one-to-one relationship between each method and each event that can be received from the node.
/// Each method takes the `data` and `id` fields as well as the source IP address (useful for tying the node-specific `id` to the relevant node).
///
/// For a reference implementation using Sqlite see, [SqliteDatabase](crate::sqlite_database::SqliteDatabase)
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
        api_version: String,
        network_name: String,
    ) -> Result<u64, DatabaseWriteError>;
    /// Save a DeployAccepted event to the database.
    ///
    /// * `transaction_accepted`: the [DeployAccepted] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_transaction_accepted(
        &self,
        transaction_accepted: TransactionAccepted,
        event_id: u32,
        event_source_address: String,
        api_version: String,
        network_name: String,
    ) -> Result<u64, DatabaseWriteError>;
    /// Save a DeployProcessed event to the database.
    ///
    /// * `transaction_accepted`: the [DeployProcessed] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_transaction_processed(
        &self,
        transaction_processed: TransactionProcessed,
        event_id: u32,
        event_source_address: String,
        api_version: String,
        network_name: String,
    ) -> Result<u64, DatabaseWriteError>;
    /// Save a DeployExpired event to the database.
    ///
    /// * `transaction_expired`: the [DeployExpired] from the `data` field.
    /// * `event_id`: the node-specific assigned `id`.
    /// * `event_source_address`: the IP address of the source node.
    async fn save_transaction_expired(
        &self,
        transaction_expired: TransactionExpired,
        event_id: u32,
        event_source_address: String,
        api_version: String,
        network_name: String,
    ) -> Result<u64, DatabaseWriteError>;
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
        api_version: String,
        network_name: String,
    ) -> Result<u64, DatabaseWriteError>;
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
        api_version: String,
        network_name: String,
    ) -> Result<u64, DatabaseWriteError>;
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
        api_version: String,
        network_name: String,
    ) -> Result<u64, DatabaseWriteError>;

    // Save data about shutdown to the database
    async fn save_shutdown(
        &self,
        event_id: u32,
        event_source_address: String,
        api_version: String,
        network_name: String,
    ) -> Result<u64, DatabaseWriteError>;

    /// Executes migration and stores current migration version
    ///
    /// * `migration`: migration to execute
    async fn execute_migration(&self, migration: Migration) -> Result<(), DatabaseWriteError>;
}

#[derive(Debug)]
pub struct UniqueConstraintError {
    pub table: String,
    pub error: sqlx::Error,
}

#[derive(Debug, Clone)]
pub struct DatabaseInitializationError {
    reason: String,
}

impl Display for DatabaseInitializationError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DatabaseInitializationError: {}", self.reason)
    }
}

impl From<&DatabaseInitializationError> for Error {
    fn from(value: &DatabaseInitializationError) -> Self {
        Error::msg(value.to_string())
    }
}

impl DatabaseInitializationError {
    pub fn from(error: Error) -> Self {
        Self {
            reason: error.to_string(),
        }
    }
}

impl From<&str> for DatabaseInitializationError {
    fn from(reason: &str) -> Self {
        Self {
            reason: reason.to_string(),
        }
    }
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
                    "23505" => {
                        //"23505" is postgresql unique constraint violation violation
                        let table = db_err
                            .table()
                            .map(|table_name| table_name.to_string())
                            .unwrap_or(db_err.message().to_string());
                        return Self::UniqueConstraint(UniqueConstraintError {
                            table,
                            error: sqlx_err,
                        });
                    }
                    "1555" | "2067" => {
                        // The message looks something like this:
                        // UNIQUE constraint failed: DeployProcessed.transaction_hash

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
/// For a reference implementation using Sqlite see, [SqliteDatabase](crate::sqlite_database::SqliteDatabase)
#[async_trait]
pub trait DatabaseReader {
    /// Returns the latest [BlockAdded] by height from the database.
    async fn get_latest_block(&self) -> Result<SseEnvelope<BlockAdded>, DatabaseReadError>;
    /// Returns the [BlockAdded] corresponding to the provided `height`.
    ///
    /// * `height` - Height of the block which should be retrieved
    async fn get_block_by_height(
        &self,
        height: u64,
    ) -> Result<SseEnvelope<BlockAdded>, DatabaseReadError>;
    /// Returns the [BlockAdded] corresponding to the provided hex-encoded `hash`.
    ///
    /// * `hash` - hash which identifies the block
    async fn get_block_by_hash(
        &self,
        hash: &str,
    ) -> Result<SseEnvelope<BlockAdded>, DatabaseReadError>;
    /// Returns an aggregate of the transaction's events corresponding to the given hex-encoded `hash`
    ///
    /// * `hash` - transaction hash of which the aggregate data should be fetched
    async fn get_transaction_aggregate_by_identifier(
        &self,
        transaction_type: &TransactionTypeId,
        hash: &str,
    ) -> Result<TransactionAggregate, DatabaseReadError>;
    /// Returns the [DeployAccepted] corresponding to the given hex-encoded `hash`
    ///
    /// * `hash` - transaction hash which identifies the transaction accepted
    async fn get_transaction_accepted_by_hash(
        &self,
        transaction_type: &TransactionTypeId,
        hash: &str,
    ) -> Result<SseEnvelope<TransactionAccepted>, DatabaseReadError>;
    /// Returns the [DeployProcessed] corresponding to the given hex-encoded `hash`
    ///
    /// * `hash` - transaction hash which identifies the transaction pocessed
    async fn get_transaction_processed_by_hash(
        &self,
        transaction_type: &TransactionTypeId,
        hash: &str,
    ) -> Result<SseEnvelope<TransactionProcessed>, DatabaseReadError>;

    /// Returns the [DeployExpired] corresponding to the given hex-encoded `hash`
    ///
    /// * `hash` - transaction hash which identifies the transaction expired
    async fn get_transaction_expired_by_hash(
        &self,
        transaction_type: &TransactionTypeId,
        hash: &str,
    ) -> Result<SseEnvelope<TransactionExpired>, DatabaseReadError>;
    /// Returns all [Fault]s that correspond to the given hex-encoded `public_key`
    ///
    /// * `public_key` - key which identifies the fault
    async fn get_faults_by_public_key(
        &self,
        public_key: &str,
    ) -> Result<Vec<SseEnvelope<Fault>>, DatabaseReadError>;
    /// Returns all [Fault]s that occurred in the given `era`
    ///
    /// * `era` - number of era for which faults should be fetched
    async fn get_faults_by_era(
        &self,
        era: u64,
    ) -> Result<Vec<SseEnvelope<Fault>>, DatabaseReadError>;
    /// Returns all [FinalitySignature](casper_event_types::FinalitySignature)s for the given hex-encoded `block_hash`.
    ///
    /// * `block_hash` - block hash for which finality signatures should be fetched
    async fn get_finality_signatures_by_block(
        &self,
        block_hash: &str,
    ) -> Result<Vec<SseEnvelope<FinSig>>, DatabaseReadError>;
    /// Returns the [Step] event for the given era.
    ///
    /// * `era` - identifier of era
    async fn get_step_by_era(&self, era: u64) -> Result<SseEnvelope<Step>, DatabaseReadError>;

    /// Returns number of events stored in db.
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
}

#[derive(Debug, Deserialize, Serialize, Clone, ToSchema)]
pub struct TransactionAggregate {
    pub(crate) transaction_hash: String,
    #[schema(value_type = TransactionAcceptedEnveloped)]
    pub(crate) transaction_accepted: Option<SseEnvelope<TransactionAccepted>>,
    #[schema(value_type = TransactionProcessedEnveloped)]
    pub(crate) transaction_processed: Option<SseEnvelope<TransactionProcessed>>,
    pub(crate) transaction_expired: bool,
}

#[allow(dead_code)] //Allowing dead code here because the Raw enum is used only in ITs
pub enum StatementWrapper {
    TableCreateStatement(Box<sea_query::TableCreateStatement>),
    InsertStatement(sea_query::InsertStatement),
    Raw(String),
}

/// Trait used to abstract a transaction of a underlying relational database
#[async_trait]
pub trait TransactionWrapper: Send + Sync {
    /// Execute the *sql* param in transaction
    async fn execute(&self, sql: &str) -> Result<(), DatabaseWriteError>;
}

/// Trait used to abstract a set of instructions necessary to perform a migration.
/// The trait exists because some transactions might require code to be executed.
/// The migraqtion script in here is second step to the `statement_producers` in [Migration] structure.
/// They are executed in one transaction, so as long as this transaction is used we can guarantee
/// that either everything went OK (both the statements and the script) or no changes were applied.
#[async_trait]
pub trait MigrationScriptExecutor: Send + Sync {
    /// Executes the code necessary to perform the migration.
    async fn execute(
        &self,
        transaction: Arc<dyn TransactionWrapper>,
    ) -> Result<(), DatabaseWriteError>;
}

/// Envelops data required for a migration
#[derive(Clone)]
pub struct Migration {
    /// Version of the migration. It is optional to denote the special case of the first migration
    /// that creates migrations table. No other migration should have version None.
    pub version: Option<u32>,
    /// A function producing sql statement wrappers that are necessary for the migration to be complete
    pub statement_producers: fn(DDLConfiguration) -> Result<Vec<StatementWrapper>, Error>,
    /// Some migrations might required to run arbitrary code (this is so we don't have to rely on
    /// DB-specific scripting SQL extensions). To achieve that a migration should have script_executor
    pub script_executor: Option<Arc<dyn MigrationScriptExecutor>>,
}

impl Migration {
    pub fn get_all_migrations() -> Vec<Migration> {
        vec![Migration::migration_1()]
    }

    pub fn initial() -> Migration {
        Migration {
            version: None,
            statement_producers: |_| {
                Ok(vec![StatementWrapper::TableCreateStatement(Box::new(
                    tables::migration::create_table_stmt(),
                ))])
            },
            script_executor: None,
        }
    }

    pub fn migration_1() -> Migration {
        Migration {
            version: Some(1),
            statement_producers: |config: DDLConfiguration| {
                let insert_event_types_stmt = tables::event_type::create_initialise_stmt()
                    .map_err(|err| {
                        Error::msg(format!(
                            "Error building event types insert statement: {:?}",
                            err
                        ))
                    })?;
                let insert_transaction_types_stmt =
                    tables::transaction_type::create_initialise_stmt().map_err(|err| {
                        Error::msg(format!(
                            "Error building transaction types insert statement: {:?}",
                            err
                        ))
                    })?;
                Ok(migration_1_ddl_statements(
                    config,
                    insert_event_types_stmt,
                    insert_transaction_types_stmt,
                ))
            },
            script_executor: None,
        }
    }

    pub fn get_version(&self) -> Option<u32> {
        self.version
    }

    pub fn get_migrations(
        &self,
        database_specific_configuration: DDLConfiguration,
    ) -> Result<Vec<StatementWrapper>, Error> {
        (self.statement_producers)(database_specific_configuration)
    }
}

fn migration_1_ddl_statements(
    config: DDLConfiguration,
    insert_event_types_stmt: sea_query::InsertStatement,
    insert_transaction_types_stmt: sea_query::InsertStatement,
) -> Vec<StatementWrapper> {
    vec![
        // Synthetic tables
        StatementWrapper::TableCreateStatement(Box::new(tables::event_type::create_table_stmt())),
        StatementWrapper::TableCreateStatement(Box::new(
            tables::transaction_type::create_table_stmt(),
        )),
        StatementWrapper::TableCreateStatement(Box::new(tables::event_log::create_table_stmt())),
        StatementWrapper::TableCreateStatement(Box::new(
            tables::transaction_event::create_table_stmt(),
        )),
        // Raw Event tables
        StatementWrapper::TableCreateStatement(Box::new(tables::block_added::create_table_stmt())),
        StatementWrapper::TableCreateStatement(Box::new(
            tables::transaction_accepted::create_table_stmt(),
        )),
        StatementWrapper::TableCreateStatement(Box::new(
            tables::transaction_processed::create_table_stmt(),
        )),
        StatementWrapper::TableCreateStatement(Box::new(
            tables::transaction_expired::create_table_stmt(),
        )),
        StatementWrapper::TableCreateStatement(Box::new(tables::fault::create_table_stmt(
            config.db_supports_unsigned,
        ))),
        StatementWrapper::TableCreateStatement(Box::new(
            tables::finality_signature::create_table_stmt(),
        )),
        StatementWrapper::TableCreateStatement(Box::new(tables::step::create_table_stmt(
            config.db_supports_unsigned,
        ))),
        StatementWrapper::TableCreateStatement(Box::new(tables::shutdown::create_table_stmt())),
        StatementWrapper::InsertStatement(insert_event_types_stmt),
        StatementWrapper::InsertStatement(insert_transaction_types_stmt),
    ]
}
