use super::types::structs::{Fault, Step};
use crate::database::{AggregateDeployInfo, DatabaseReader, DatabaseRequestError, DatabaseWriter};
use crate::sql::tables;
use crate::sql::tables::event_type::EventTypeId;
use crate::types::enums::DeployAtState;
use crate::types::structs::{BlockAdded, DeployAccepted, DeployExpired};
use crate::DeployProcessed;
use anyhow::{Context, Error};
use async_trait::async_trait;
use casper_node::types::{Block, Deploy, FinalitySignature};
use casper_types::{AsymmetricType, ExecutionEffect, PublicKey, Timestamp};
use itertools::Itertools;
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
use tracing::info;

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
    pub fn new(path: &Path, node_ip_address: String) -> Result<SqliteDb, Error> {
        fs::create_dir_all(path)?;
        let file_path = path.join(DB_FILENAME);
        let connection = Connection::open(&file_path)?;

        // Synthetic table creation statements
        let create_table_sql_event_type = tables::event_type::create_table_stmt();
        let create_table_sql_event_log = tables::event_log::create_table_stmt();
        let create_table_sql_event_source = tables::event_source::create_table_stmt();
        let create_table_sql_event_source_of_event =
            tables::event_source_of_event::create_table_stmt();

        // Raw Event table creation statements
        let create_table_sql_block_added = tables::block_added::create_table_stmt();
        let create_table_sql_deploy_accepted = tables::deploy_accepted::create_table_stmt();
        let create_table_sql_deploy_processed = tables::deploy_processed::create_table_stmt();
        let create_table_sql_deploy_expired = tables::deploy_expired::create_table_stmt();
        let create_table_sql_fault = tables::fault::create_table_stmt();
        let create_table_sql_step = tables::step::create_table_stmt();

        let batched_created_table = vec![
            create_table_sql_event_type,
            create_table_sql_event_source,
            create_table_sql_event_source_of_event,
            create_table_sql_event_log,
            create_table_sql_block_added,
            create_table_sql_deploy_accepted,
            create_table_sql_deploy_expired,
            create_table_sql_deploy_processed,
            create_table_sql_fault,
            create_table_sql_step,
        ]
        .iter()
        .map(|stmt| stmt.to_string(SqliteQueryBuilder))
        .join(";");

        connection
            .execute_batch(&batched_created_table)
            .context("Error creating tables")?;

        // Initialisation statements
        let initialise_sql_event_type = tables::event_type::create_initialise_stmt()?;
        let initialise_sql_event_source =
            tables::event_source::create_insert_stmt(node_ip_address)?;

        let batched_initialise_tables =
            vec![initialise_sql_event_type, initialise_sql_event_source]
                .iter()
                .map(|stmt| stmt.to_string(SqliteQueryBuilder))
                .join(";");

        connection
            .execute_batch(&batched_initialise_tables)
            .context("Error creating tables")?;

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
    // fn insert_data(&self, data: Entity) -> Result<usize, Error> {
    //     let db_connection = self.db.lock().map_err(|err| Error::msg(err.to_string()))?;
    //
    //     return match data {
    //         Entity::Block(block) => {
    //             // The same pattern is used for each entity, I have therefore only
    //             // commented on it once - here.
    //
    //             // Extract and format the data for each column
    //             let height = block.height().to_string();
    //             let hash = hex::encode(block.hash().inner());
    //             let block = serde_json::to_string(&block)?;
    //
    //             // Generate the SQL for INSERTing the new row
    //             let insert_command = create_insert_stmt(Table::Blocks, &BLOCK_COLUMNS);
    //
    //             // Interpolate the positional parameters into the above statement and execute
    //             db_connection
    //                 .execute(&insert_command, params![height, hash, block])
    //                 .map_err(Error::from)
    //         }
    //
    //         Entity::Deploy(deploy) => {
    //             match deploy {
    //                 DeployAtState::Accepted(deploy) => {
    //                     let hash = hex::encode(deploy.id().inner());
    //                     let timestamp = deploy.header().timestamp().to_string();
    //                     let json_deploy = serde_json::to_string(&deploy)?;
    //
    //                     let insert_command = create_insert_stmt(Table::Deploys, &DEPLOY_COLUMNS);
    //
    //                     // Params are DeployHash, Timestamp, DeployAccepted, DeployProcessed, Expired
    //                     db_connection
    //                         .execute(
    //                             &insert_command,
    //                             params![
    //                                 hash,
    //                                 timestamp,
    //                                 json_deploy,
    //                                 SqlValue::Null,
    //                                 SqlValue::Integer(0)
    //                             ],
    //                         )
    //                         .map_err(Error::from)
    //                 }
    //                 DeployAtState::Processed(deploy_processed) => {
    //                     let hash = hex::encode(deploy_processed.deploy_hash.inner());
    //                     let json_deploy = serde_json::to_string(&deploy_processed)?;
    //
    //                     let update_command = "UPDATE deploys SET processed = ? WHERE hash = ?";
    //
    //                     db_connection
    //                         .execute(update_command, params![json_deploy, hash])
    //                         .map_err(Error::from)
    //                 }
    //                 DeployAtState::Expired(deploy_hash) => {
    //                     let hash = hex::encode(deploy_hash.inner());
    //
    //                     let update_command =
    //                         format!("UPDATE deploys SET expired = {} WHERE hash = ?", 1i64);
    //
    //                     db_connection
    //                         .execute(&update_command, params![hash])
    //                         .map_err(Error::from)
    //                 }
    //             }
    //         }
    //         Entity::Fault(fault) => {
    //             let era = fault.era_id.value().to_string();
    //             let public_key = fault.public_key.to_hex();
    //             let timestamp = fault.timestamp.to_string();
    //
    //             let insert_command = create_insert_stmt(Table::Faults, &FAULT_COLUMNS);
    //
    //             db_connection
    //                 .execute(&insert_command, params![era, public_key, timestamp])
    //                 .map_err(Error::from)
    //         }
    //         Entity::Step(step) => {
    //             let era = step.era_id.value().to_string();
    //             let execution_effect = serde_json::to_string(&step.execution_effect)
    //                 .context("Error serializing Execution Effect")?;
    //
    //             let insert_command = create_insert_stmt(Table::Steps, &STEP_COLUMNS);
    //
    //             db_connection
    //                 .execute(&insert_command, params![era, execution_effect])
    //                 .map_err(Error::from)
    //         }
    //     };
    // }
}

#[async_trait]
impl DatabaseWriter for SqliteDb {
    async fn save_block_added(
        &self,
        block_added: BlockAdded,
        event_id: u64,
        event_source_address: String,
    ) -> Result<usize, Error> {
        let db_connection = self.db.lock().map_err(|err| Error::msg(err.to_string()))?;

        info!("Db connected");

        let serialised_data = serde_json::to_string(&block_added)?;
        let encoded_hash = hex::encode(block_added.block_hash.inner());

        info!("Data parsed for {}", encoded_hash);

        let stmt = tables::event_source::create_select_id_by_address_stmt(event_source_address);
        let event_source_id =
            db_connection.query_row_and_then(&stmt.to_string(SqliteQueryBuilder), [], |row| {
                row.get::<&str, u8>("event_source_id")
            })?;

        info!("Event Source ID: {}", event_source_id);

        let insert_to_event_log_stmt = tables::event_log::create_insert_stmt(
            EventTypeId::BlockAdded as u8,
            event_source_id,
            event_id,
        )
        .context("Error constructing SQL statement")?;

        db_connection
            .execute(&insert_to_event_log_stmt.to_string(SqliteQueryBuilder), [])
            .map_err(Error::from)?;

        let get_max_id_stmt = tables::event_log::create_get_max_id_stmt();

        let id: u64 = db_connection.query_row_and_then(
            &get_max_id_stmt.to_string(SqliteQueryBuilder),
            [],
            |row| {
                row.get("event_log_id")
                    .map_err(|err| Error::msg(err.to_string()))
            },
        )?;

        let insert_to_block_added_stmt = tables::block_added::create_insert_stmt(
            block_added.block.header.height,
            encoded_hash,
            serialised_data,
            id,
        )
        .context("Error constructing SQL statement")?;

        db_connection
            .execute(
                &insert_to_block_added_stmt.to_string(SqliteQueryBuilder),
                [],
            )
            .map_err(Error::from)
    }

    async fn save_deploy_accepted(&self, deploy_accepted: DeployAccepted) -> Result<usize, Error> {
        Ok(2)
    }

    async fn save_deploy_processed(
        &self,
        deploy_processed: DeployProcessed,
    ) -> Result<usize, Error> {
        Ok(2)
    }

    async fn save_deploy_expired(&self, deploy_expired: DeployExpired) -> Result<usize, Error> {
        Ok(2)
    }

    async fn save_step(&self, step: Step) -> Result<usize, Error> {
        Ok(2)
    }

    async fn save_fault(&self, fault: Fault) -> Result<usize, Error> {
        Ok(2)
    }

    async fn save_finality_signature(
        &self,
        finality_signature: FinalitySignature,
    ) -> Result<usize, Error> {
        Ok(2)
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

    async fn get_latest_deploy_aggregate(
        &self,
    ) -> Result<AggregateDeployInfo, DatabaseRequestError> {
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

    async fn get_deploy_by_hash_aggregate(
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
    aggregate_deploy.deploy_accepted = deploy_row.get::<usize, Option<String>>(2)?;
    aggregate_deploy.deploy_processed = deploy_row.get::<usize, Option<String>>(3)?;
    aggregate_deploy.deploy_expired = match deploy_row.get::<usize, u8>(4) {
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
    let path_to_storage = Path::new("./target/test");
    SqliteDb::new(path_to_storage, "127.0.0.1".to_string()).unwrap();
}
