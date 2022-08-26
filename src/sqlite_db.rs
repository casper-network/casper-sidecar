use super::types::structs::{Fault, Step};
use crate::database::{AggregateDeployInfo, DatabaseReader, DatabaseRequestError, DatabaseWriter};
use crate::types::enums::DeployAtState;
use crate::DeployProcessed;
use anyhow::{Context, Error};
use async_trait::async_trait;
use casper_node::types::{Block, Deploy};
use casper_types::{AsymmetricType, ExecutionEffect, PublicKey, Timestamp};
use lazy_static::lazy_static;
use rusqlite::{named_params, params, types::Value as SqlValue, Connection, OpenFlags, Row};
use sea_query::{
    BlobSize, ColumnDef, ForeignKey, ForeignKeyAction, Iden, SqliteQueryBuilder, Table,
};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Write};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const DB_FILENAME: &str = "raw_sse_data.db3";

enum TableBlockAdded {
    Name,
    Height,
    BlockHash,
    Raw,
    EventLogId,
}

impl Iden for TableBlockAdded {
    fn unquoted(&self, s: &mut dyn Write) {
        write!(
            s,
            "{}",
            match self {
                TableBlockAdded::Name => "BlockAdded",
                TableBlockAdded::Height => "height",
                TableBlockAdded::BlockHash => "block_hash",
                TableBlockAdded::Raw => "raw",
                TableBlockAdded::EventLogId => "event_log_id",
            }
        )
        .unwrap();
    }
}

enum Tables {
    EventLog,
    EventType,
    EventSource,
    EventEventSource,
    DeployEvent,
    BlockAdded,
    DeployAccepted,
    DeployProcessed,
    AggregateDeployInfo,
    Step,
    Fault,
    FinalitySignature,
}

impl Display for Tables {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let string_repr = match self {
            Tables::EventLog => "event_log",
            Tables::EventType => "event_type",
            Tables::EventSource => "event_source",
            Tables::EventEventSource => "event_event_source",
            Tables::DeployEvent => "deploy_event",
            Tables::BlockAdded => "BlockAdded",
            Tables::DeployAccepted => "DeployAccepted",
            Tables::DeployProcessed => "DeployProcessed",
            Tables::AggregateDeployInfo => "agg_DeployInfo",
            Tables::Step => "Step",
            Tables::Fault => "Fault",
            Tables::FinalitySignature => "FinalitySignature",
        };
        write!(f, "{}", string_repr)
    }
}

lazy_static! {
    static ref BLOCK_COLUMNS: Vec<String> =
        ["height", "hash", "block"].map(ToString::to_string).into();
    static ref DEPLOY_COLUMNS: Vec<String> =
        ["hash", "timestamp", "accepted", "processed", "expired"]
            .map(ToString::to_string)
            .into();
    static ref STEP_COLUMNS: Vec<String> = ["era", "effect"].map(ToString::to_string).into();
    static ref FAULT_COLUMNS: Vec<String> = ["era", "public_key", "timestamp"]
        .map(ToString::to_string)
        .into();
}

#[derive(Clone)]
pub struct SqliteDb {
    db: Arc<Mutex<Connection>>,
    pub file_path: PathBuf,
}

impl SqliteDb {
    pub fn ref_new(path: &Path) -> Result<SqliteDb, Error> {
        fs::create_dir_all(path)?;
        let file_path = path.join(DB_FILENAME);
        let connection = Connection::open(&file_path)?;
    }

    fn create_block_added_table(connection: &Connection) -> rusqlite::Result<usize> {
        let table_stmt = Table::create()
            .table(TableBlockAdded::Name)
            .if_not_exists()
            .col(
                ColumnDef::new(TableBlockAdded::Height)
                    .integer()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(TableBlockAdded::BlockHash)
                    .string()
                    .not_null()
                    .unique_key(),
            )
            // todo investigate boundaries of the blobsize variants.
            .col(
                ColumnDef::new(TableBlockAdded::Raw)
                    .blob(BlobSize::Tiny)
                    .not_null(),
            )
            .col(
                ColumnDef::new(TableBlockAdded::EventLogId)
                    .integer()
                    .not_null(),
            )
            .foreign_key(
                ForeignKey::create()
                    .name("FK_event_log_id")
                    .from(TableBlockAdded::Name, TableBlockAdded::EventLogId)
                    .to("event_log", "event_log_id")
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict),
            );

        connection.execute(&table_stmt.to_string(SqliteQueryBuilder), [])
    }

    pub fn new(path: &Path) -> Result<SqliteDb, Error> {
        fs::create_dir_all(path)?;
        let file_path = path.join(DB_FILENAME);
        let connection = Connection::open(&file_path)?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS event_log (
                    event_log_key   INTEGER NOT NULL PRIMARY KEY,
                    event_type_id   INTEGER NOT NULL,
                    event_source_id INTEGER NOT NULL,
                    event_id        INTEGER NOT NULL,
                    timestamp       DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (event_type_id)
                        REFERENCES event_type (event_type_id)
                            ON UPDATE RESTRICT
                            ON DELETE RESTRICT
                    FOREIGN KEY (event_source_id)
                        REFERENCES event_source (event_source_id)
                            ON UPDATE RESTRICT
                            ON DELETE RESTRICT                        
                )",
                [],
            )
            .context("Error creating event_log table")?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS event_type (
                    event_type_id   INTEGER NOT NULL PRIMARY KEY,
                    event_type_name      VARCHAR(255) NOT NULL
            )",
                [],
            )
            .context("Error creating event_type table")?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS event_source (
                    event_source_id   INTEGER NOT NULL PRIMARY KEY,
                    event_source      VARCHAR(255) NOT NULL
            )",
                [],
            )
            .context("Error creating event_source table")?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS event_event_source (
                event_id            INTEGER NOT NULL PRIMARY KEY,
                event_source_id     INTEGER NOT NULL
            )",
                [],
            )
            .context("Error creating event_event_source table")?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS deploy_event (
                event_log_id    INTEGER NOT NULL,
                deploy_hash     VARCHAR(255) NOT NULL
            )",
                [],
            )
            .context("Error creating deploy_event table")?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS deploy_event_type (
                deploy_event_type_id    INTEGER NOT NULL,
                deploy_event_type       VARCHAR(255) NOT NULL
            )",
                [],
            )
            .context("Error creating deploy_event_type table")?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS BlockAdded (
                height          INTEGER NOT NULL PRIMARY KEY,
                block_hash      VARCHAR(255) NOT NULL,
                raw             BLOB NOT NULL,
                event_log_id    INTEGER NOT NULL,
                FOREIGN KEY (event_log_id)
                    REFERENCES event_log (event_log_id)
                        ON UPDATE RESTRICT
                        ON DELETE RESTRICT
            )",
                [],
            )
            .context("Error creating BlockAdded table")?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS DeployProcessed (
                deploy_hash     VARCHAR(255) NOT NULL PRIMARY KEY,
                raw             BLOB NOT NULL,
                event_log_id    INTEGER NOT NULL,
                FOREIGN KEY (event_log_id)
                    REFERENCES event_log (event_log_id)
                        ON UPDATE RESTRICT
                        ON DELETE RESTRICT                
            )",
                [],
            )
            .context("Error creating DeployProcessed table")?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS DeployAccepted (
                deploy_hash     VARCHAR(255) NOT NULL PRIMARY KEY,
                raw             BLOB NOT NULL,
                event_log_id    INTEGER NOT NULL,
                FOREIGN KEY (event_log_id)
                    REFERENCES event_log (event_log_id)
                        ON UPDATE RESTRICT
                        ON DELETE RESTRICT                
            )",
                [],
            )
            .context("Error creating DeployAccepted table")?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS DeployExpired (
                deploy_hash     VARCHAR(255) NOT NULL PRIMARY KEY,
                raw             BLOB NOT NULL,
                event_log_id    INTEGER NOT NULL,
                FOREIGN KEY (event_log_id)
                    REFERENCES event_log (event_log_id)
                        ON UPDATE RESTRICT
                        ON DELETE RESTRICT                
            )",
                [],
            )
            .context("Error creating DeployExpired table")?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS Step (
                era             INTEGER NOT NULL PRIMARY KEY,
                raw             BLOB NOT NULL,
                event_log_id    INTEGER NOT NULL,
                FOREIGN KEY (event_log_id)
                    REFERENCES event_log (event_log_id)
                        ON UPDATE RESTRICT
                        ON DELETE RESTRICT    
        )",
                [],
            )
            .context("Error creating Step table")?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS Fault (
                era             INTEGER NOT NULL PRIMARY KEY,
                public_key      VARCHAR(255) NOT NULL,
                raw             BLOB NOT NULL,
                event_log_id    INTEGER NOT NULL,
                PRIMARY KEY (era, public_key),
                FOREIGN KEY (event_log_id)
                    REFERENCES event_log (event_log_id)
                        ON UPDATE RESTRICT
                        ON DELETE RESTRICT 

        )",
                [],
            )
            .context("Error creating Fault table")?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS FinalitySignature (
                block_hash      VARCHAR(255) NOT NULL,
                public_key      VARCHAR(255) NOT NULL,
                raw             BLOB NOT NULL,
                event_log_id    INTEGER NOT NULL,
                FOREIGN KEY (event_log_id)
                    REFERENCES event_log (event_log_id)
                        ON UPDATE RESTRICT
                        ON DELETE RESTRICT 
            )",
                [],
            )
            .context("Error creating FinalitySignature table")?;

        Ok(SqliteDb {
            db: Arc::new(Mutex::new(connection)),
            file_path,
        })
    }

    pub fn new_read_only(path: &Path) -> Result<SqliteDb, Error> {
        let db = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let file_path = path.join(DB_FILENAME);

        Ok(SqliteDb {
            db: Arc::new(Mutex::new(db)),
            file_path,
        })
    }

    // Helper for the DatabaseWriter for inserting / updating records - internal only - not to be exposed in the API to other components
    fn insert_data(&self, data: Entity) -> Result<usize, Error> {
        let db_connection = self.db.lock().map_err(|err| Error::msg(err.to_string()))?;

        return match data {
            Entity::Block(block) => {
                // The same pattern is used for each entity, I have therefore only
                // commented on it once - here.

                // Extract and format the data for each column
                let height = block.height().to_string();
                let hash = hex::encode(block.hash().inner());
                let block = serde_json::to_string(&block)?;

                // Generate the SQL for INSERTing the new row
                let insert_command = create_insert_stmt(Table::Blocks, &BLOCK_COLUMNS);

                // Interpolate the positional parameters into the above statement and execute
                db_connection
                    .execute(&insert_command, params![height, hash, block])
                    .map_err(Error::from)
            }

            Entity::Deploy(deploy) => {
                match deploy {
                    DeployAtState::Accepted(deploy) => {
                        let hash = hex::encode(deploy.id().inner());
                        let timestamp = deploy.header().timestamp().to_string();
                        let json_deploy = serde_json::to_string(&deploy)?;

                        let insert_command = create_insert_stmt(Table::Deploys, &DEPLOY_COLUMNS);

                        // Params are DeployHash, Timestamp, DeployAccepted, DeployProcessed, Expired
                        db_connection
                            .execute(
                                &insert_command,
                                params![
                                    hash,
                                    timestamp,
                                    json_deploy,
                                    SqlValue::Null,
                                    SqlValue::Integer(0)
                                ],
                            )
                            .map_err(Error::from)
                    }
                    DeployAtState::Processed(deploy_processed) => {
                        let hash = hex::encode(deploy_processed.deploy_hash.inner());
                        let json_deploy = serde_json::to_string(&deploy_processed)?;

                        let update_command = "UPDATE deploys SET processed = ? WHERE hash = ?";

                        db_connection
                            .execute(update_command, params![json_deploy, hash])
                            .map_err(Error::from)
                    }
                    DeployAtState::Expired(deploy_hash) => {
                        let hash = hex::encode(deploy_hash.inner());

                        let update_command =
                            format!("UPDATE deploys SET expired = {} WHERE hash = ?", 1i64);

                        db_connection
                            .execute(&update_command, params![hash])
                            .map_err(Error::from)
                    }
                }
            }
            Entity::Fault(fault) => {
                let era = fault.era_id.value().to_string();
                let public_key = fault.public_key.to_hex();
                let timestamp = fault.timestamp.to_string();

                let insert_command = create_insert_stmt(Table::Faults, &FAULT_COLUMNS);

                db_connection
                    .execute(&insert_command, params![era, public_key, timestamp])
                    .map_err(Error::from)
            }
            Entity::Step(step) => {
                let era = step.era_id.value().to_string();
                let execution_effect = serde_json::to_string(&step.execution_effect)
                    .context("Error serializing Execution Effect")?;

                let insert_command = create_insert_stmt(Table::Steps, &STEP_COLUMNS);

                db_connection
                    .execute(&insert_command, params![era, execution_effect])
                    .map_err(Error::from)
            }
        };
    }
}

#[async_trait]
impl DatabaseWriter for SqliteDb {
    async fn save_block_added(&self, block: Block) -> Result<usize, Error> {
        let db_connection = self.db.lock().map_err(|err| Error::msg(err.to_string()))?;

        // self.insert_data(Entity::Block(block))
        // Extract and format the data for each column
        let height = block.height().to_string();
        let hash = hex::encode(block.hash().inner());
        let block = serde_json::to_string(&block)?;

        // Generate the SQL for INSERTing the new row
        let insert_command = create_insert_stmt(Table::Blocks, &BLOCK_COLUMNS);

        // Interpolate the positional parameters into the above statement and execute
        db_connection
            .execute(&insert_command, params![height, hash, block])
            .map_err(Error::from)
    }

    async fn save_or_update_deploy(&self, deploy: DeployAtState) -> Result<usize, Error> {
        self.insert_data(Entity::Deploy(deploy))
    }

    async fn save_step(&self, step: Step) -> Result<usize, Error> {
        self.insert_data(Entity::Step(step))
    }

    async fn save_fault(&self, fault: Fault) -> Result<usize, Error> {
        self.insert_data(Entity::Fault(fault))
    }
}

#[async_trait]
impl DatabaseReader for SqliteDb {
    async fn get_latest_block(&self) -> Result<Block, DatabaseRequestError> {
        let db = self.db.lock().map_err(|error| {
            DatabaseRequestError::DBConnectionFailed(Error::msg(error.to_string()))
        })?;

        db.query_row_and_then(
            "SELECT block FROM blocks ORDER BY height DESC LIMIT 1",
            [],
            |row| {
                let block_string: String = row.get(0)?;
                deserialize_data::<Block>(&block_string)
            },
        )
        .map_err(wrap_query_error)
    }

    async fn get_block_by_height(&self, height: u64) -> Result<Block, DatabaseRequestError> {
        let db = self.db.lock().map_err(|error| {
            DatabaseRequestError::DBConnectionFailed(Error::msg(error.to_string()))
        })?;

        db.query_row_and_then(
            "SELECT block FROM blocks WHERE height = ?",
            [height],
            |row| {
                let block_string: String = row.get(0)?;
                deserialize_data::<Block>(&block_string)
            },
        )
        .map_err(wrap_query_error)
    }

    async fn get_block_by_hash(&self, hash: &str) -> Result<Block, DatabaseRequestError> {
        let hash_regex = regex::Regex::new("^([A-Fa-f0-9]){64}$")
            .map_err(|err| DatabaseRequestError::Unhandled(Error::from(err)))?;
        if !hash_regex.is_match(hash) {
            return Err(DatabaseRequestError::InvalidParam(Error::msg(format!(
                "Expected hex-encoded block hash (64 chars), received: {} (length: {})",
                hash,
                hash.len()
            ))));
        }

        let db = self.db.lock().map_err(|error| {
            DatabaseRequestError::DBConnectionFailed(Error::msg(error.to_string()))
        })?;

        db.query_row_and_then("SELECT block FROM blocks WHERE hash = ?", [hash], |row| {
            let block_string: String = row.get(0).map_err(SqliteDbError::Rusqlite)?;
            deserialize_data::<Block>(&block_string)
        })
        .map_err(wrap_query_error)
    }

    async fn get_latest_deploy(&self) -> Result<AggregateDeployInfo, DatabaseRequestError> {
        let db = self.db.lock().map_err(|error| {
            DatabaseRequestError::DBConnectionFailed(Error::msg(error.to_string()))
        })?;

        db.query_row_and_then(
            "SELECT * FROM deploys ORDER BY timestamp DESC LIMIT 1",
            [],
            |row| extract_aggregate_deploy_info(row),
        )
        .map_err(wrap_query_error)
    }

    async fn get_deploy_by_hash(
        &self,
        hash: &str,
    ) -> Result<AggregateDeployInfo, DatabaseRequestError> {
        let hash_regex = regex::Regex::new("^([0-9A-Fa-f]){64}$")
            .map_err(|err| DatabaseRequestError::Unhandled(Error::from(err)))?;
        if !hash_regex.is_match(hash) {
            return Err(DatabaseRequestError::InvalidParam(Error::msg(format!(
                "Expected hex-encoded deploy hash (64 chars), received: {} (length: {})",
                hash,
                hash.len()
            ))));
        }
        let db = self.db.lock().map_err(|error| {
            DatabaseRequestError::DBConnectionFailed(Error::msg(error.to_string()))
        })?;

        db.query_row_and_then("SELECT * FROM deploys WHERE hash = ?", [hash], |row| {
            extract_aggregate_deploy_info(row)
        })
        .map_err(wrap_query_error)
    }

    async fn get_deploy_accepted_by_hash(
        &self,
        hash: &str,
    ) -> Result<Deploy, DatabaseRequestError> {
        let hash_regex = regex::Regex::new("^([0-9A-Fa-f]){64}$")
            .map_err(|err| DatabaseRequestError::Unhandled(Error::from(err)))?;
        if !hash_regex.is_match(hash) {
            return Err(DatabaseRequestError::InvalidParam(Error::msg(format!(
                "Expected hex-encoded deploy hash (64 chars), received: {} (length: {})",
                hash,
                hash.len()
            ))));
        }
        let db = self.db.lock().map_err(|error| {
            DatabaseRequestError::DBConnectionFailed(Error::msg(error.to_string()))
        })?;

        db.query_row_and_then(
            "SELECT accepted FROM deploys WHERE hash = ?",
            [hash],
            |row| {
                let deploy_string: String = row.get(0)?;
                deserialize_data::<Deploy>(&deploy_string)
            },
        )
        .map_err(wrap_query_error)
    }

    async fn get_deploy_processed_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployProcessed, DatabaseRequestError> {
        let hash_regex = regex::Regex::new("^([0-9A-Fa-f]){64}$")
            .map_err(|err| DatabaseRequestError::Unhandled(Error::from(err)))?;
        if !hash_regex.is_match(hash) {
            return Err(DatabaseRequestError::InvalidParam(Error::msg(format!(
                "Expected hex-encoded deploy hash (64 chars), received: {} (length: {})",
                hash,
                hash.len()
            ))));
        }
        let db = self.db.lock().map_err(|error| {
            DatabaseRequestError::DBConnectionFailed(Error::msg(error.to_string()))
        })?;

        db.query_row_and_then(
            "SELECT processed FROM deploys WHERE hash = ?",
            [hash],
            |row| {
                let deploy_string: String = row.get(0)?;
                deserialize_data::<DeployProcessed>(&deploy_string)
            },
        )
        .map_err(wrap_query_error)
    }

    async fn get_deploy_expired_by_hash(&self, hash: &str) -> Result<bool, DatabaseRequestError> {
        let hash_regex = regex::Regex::new("^([0-9A-Fa-f]){64}$")
            .map_err(|err| DatabaseRequestError::Unhandled(Error::from(err)))?;
        if !hash_regex.is_match(hash) {
            return Err(DatabaseRequestError::InvalidParam(Error::msg(format!(
                "Expected hex-encoded deploy hash (64 chars), received: {} (length: {})",
                hash,
                hash.len()
            ))));
        }
        let db = self.db.lock().map_err(|error| {
            DatabaseRequestError::DBConnectionFailed(Error::msg(error.to_string()))
        })?;

        db.query_row_and_then(
            "SELECT expired FROM deploys WHERE hash = ?",
            [hash],
            |row| {
                let expired: u8 = row.get(0)?;
                integer_to_bool(expired).map_err(|err| SqliteDbError::Internal(err))
            },
        )
        .map_err(wrap_query_error)
    }

    async fn get_step_by_era(&self, era: u64) -> Result<Step, DatabaseRequestError> {
        let db = self.db.lock().map_err(|error| {
            DatabaseRequestError::DBConnectionFailed(Error::msg(error.to_string()))
        })?;

        db.query_row_and_then("SELECT effect FROM steps WHERE era = ?", [era], |row| {
            let effect_string: String = row.get(0)?;
            let effect = deserialize_data::<ExecutionEffect>(&effect_string)?;
            Ok(Step {
                era_id: era.into(),
                execution_effect: effect,
            })
        })
        .map_err(wrap_query_error)
    }

    // todo this should be taking a compound identifier maybe including Era to ensure it gets a single row
    async fn get_fault_by_public_key(
        &self,
        public_key: &str,
    ) -> Result<Fault, DatabaseRequestError> {
        let db = self.db.lock().map_err(|error| {
            DatabaseRequestError::DBConnectionFailed(Error::msg(error.to_string()))
        })?;

        db.query_row_and_then(
            "SELECT * FROM faults WHERE public_key = ?",
            [public_key],
            |row| {
                let era: u64 = row.get(0)?;
                let public_key = PublicKey::from_hex(public_key)
                    .map_err(|err| SqliteDbError::Internal(Error::from(err)))?;
                let timestamp_string: String = row.get(2)?;
                let timestamp = deserialize_data::<Timestamp>(&timestamp_string)?;

                Ok(Fault {
                    era_id: era.into(),
                    public_key,
                    timestamp,
                })
            },
        )
        .map_err(wrap_query_error)
    }
}

enum SqliteDbError {
    Rusqlite(rusqlite::Error),
    SerdeJson(serde_json::Error),
    Internal(Error),
}

impl From<rusqlite::Error> for SqliteDbError {
    fn from(error: rusqlite::Error) -> Self {
        SqliteDbError::Rusqlite(error)
    }
}

fn wrap_query_error(error: SqliteDbError) -> DatabaseRequestError {
    match error {
        SqliteDbError::Rusqlite(err) => match err.to_string().as_str() {
            "Query returned no rows" => DatabaseRequestError::NotFound,
            _ => DatabaseRequestError::Unhandled(Error::from(err)),
        },
        SqliteDbError::SerdeJson(err) => DatabaseRequestError::Serialisation(Error::from(err)),
        SqliteDbError::Internal(err) => DatabaseRequestError::Unhandled(err),
    }
}

fn deserialize_data<'de, T: Deserialize<'de>>(data: &'de str) -> Result<T, SqliteDbError> {
    serde_json::from_str::<T>(data).map_err(SqliteDbError::SerdeJson)
}

fn extract_aggregate_deploy_info(deploy_row: &Row) -> Result<AggregateDeployInfo, SqliteDbError> {
    let mut aggregate_deploy: AggregateDeployInfo = AggregateDeployInfo {
        deploy_hash: "".to_string(),
        deploy_accepted: None,
        deploy_processed: None,
        deploy_expired: false,
    };

    aggregate_deploy.deploy_hash = deploy_row.get(0)?;
    aggregate_deploy.accepted = deploy_row.get::<usize, Option<String>>(2)?;
    aggregate_deploy.processed = deploy_row.get::<usize, Option<String>>(3)?;
    aggregate_deploy.expired = match deploy_row.get::<usize, u8>(4) {
        Ok(int) => integer_to_bool(int).map_err(SqliteDbError::Internal)?,
        Err(err) => return Err(SqliteDbError::Rusqlite(err)),
    };

    Ok(aggregate_deploy)
}

fn integer_to_bool(integer: u8) -> Result<bool, Error> {
    match integer {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(Error::msg("Invalid bool number in DB")),
    }
}

#[allow(clippy::large_enum_variant)]
enum Entity {
    Block(Block),
    Deploy(DeployAtState),
    Fault(Fault),
    Step(Step),
}

#[test]
fn should_successfully_create_connection() {
    let path_to_storage = Path::new("./target/storage");
    SqliteDb::new(path_to_storage).unwrap();
}
