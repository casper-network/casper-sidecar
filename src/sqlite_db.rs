use std::{
    fmt::{Display, Formatter},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use casper_node::types::{Block, Deploy};
use casper_types::{AsymmetricType, ExecutionEffect, PublicKey, Timestamp};

use anyhow::{Context, Error};
use async_trait::async_trait;
use lazy_static::lazy_static;
use rusqlite::{params, types::Value as SqlValue, Connection, OpenFlags, Row};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::types::{
    enums::DeployAtState,
    structs::{DeployProcessed, Fault, Step},
};

const DB_FILENAME: &str = "raw_sse_data.db3";
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

#[async_trait]
pub(crate) trait DatabaseWriter {
    async fn save_block(&self, block: Block) -> Result<usize, Error>;
    async fn save_or_update_deploy(&self, deploy: DeployAtState) -> Result<usize, Error>;
    async fn save_step(&self, step: Step) -> Result<usize, Error>;
    async fn save_fault(&self, fault: Fault) -> Result<usize, Error>;
}

#[async_trait]
pub(crate) trait DatabaseReader {
    async fn get_latest_block(&self) -> Result<Block, DatabaseRequestError>;
    async fn get_block_by_height(&self, height: u64) -> Result<Block, DatabaseRequestError>;
    async fn get_block_by_hash(&self, hash: &str) -> Result<Block, DatabaseRequestError>;
    async fn get_latest_deploy(&self) -> Result<AggregateDeployInfo, DatabaseRequestError>;
    async fn get_deploy_by_hash(
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

impl SqliteDb {
    pub fn new(path: &Path) -> Result<SqliteDb, Error> {
        fs::create_dir_all(path)?;
        let file_path = path.join(DB_FILENAME);
        let db = Connection::open(&file_path)?;

        // todo: make these statements more programmatic by making the above COLUMNS Key-Values of Column Name and SQL Type
        // todo: and then interpolate them into the SQL Strings by iterating over the COLUMN vecs.
        db.execute(
            "CREATE TABLE IF NOT EXISTS blocks (
                height      INTEGER PRIMARY KEY,
                hash        STRING NOT NULL,
                block       STRING NOT NULL
            )",
            [],
        )
        .context("failed to create blocks table in database")?;
        debug!("SQLite - Blocks table initialised");

        db.execute(
            "CREATE TABLE IF NOT EXISTS deploys (
                hash        STRING PRIMARY KEY,
                timestamp   STRING,
                accepted    STRING,
                processed   STRING,
                expired     INTEGER
            )",
            [],
        )
        .context("failed to create deploys table in database")?;
        debug!("SQLite - Deploys table initialised");

        db.execute(
            "CREATE TABLE IF NOT EXISTS steps (
                era      INTEGER PRIMARY KEY,
                effect      STRING NOT NULL
        )",
            [],
        )
        .context("failed to create steps table in database")?;
        debug!("SQLite - Steps table initialised");

        db.execute(
            "CREATE TABLE IF NOT EXISTS faults (
                era      INTEGER,
                public_key  STRING NOT NULL,
                timestamp   STRING NOT NULL,
                PRIMARY KEY (era, public_key)
        )",
            [],
        )
        .context("failed to create faults table in database")?;
        debug!("SQLite - Faults table initialised");

        Ok(SqliteDb {
            db: Arc::new(Mutex::new(db)),
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
    async fn save_block(&self, block: Block) -> Result<usize, Error> {
        self.insert_data(Entity::Block(block))
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
            let block_string: String = row.get(0).map_err(DatabaseError::Rusqlite)?;
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
            extract_aggregate_deploy_info,
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
                integer_to_bool(expired).map_err(DatabaseError::Internal)
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

    // This will only ever return one result even if there were multiple faults.
    // todo Add another query/rest interface for specifying era as well as public key.
    async fn get_fault_by_public_key(
        &self,
        public_key: &str,
    ) -> Result<Fault, DatabaseRequestError> {
        let public_key_regex = regex::Regex::new("^([0-9A-Fa-f]{2}){33,34}$")
            .map_err(|err| DatabaseRequestError::Unhandled(Error::from(err)))?;
        if !public_key_regex.is_match(public_key) {
            return Err(DatabaseRequestError::InvalidParam(Error::msg(format!(
                "Expected hex-encoded public key (66/68 chars), received: {} (length: {})",
                public_key,
                public_key.len()
            ))));
        }

        let db = self.db.lock().map_err(|error| {
            DatabaseRequestError::DBConnectionFailed(Error::msg(error.to_string()))
        })?;

        db.query_row_and_then(
            "SELECT * FROM faults WHERE public_key = ?",
            [public_key],
            |row| {
                let era: u64 = row.get(0)?;
                let public_key = PublicKey::from_hex(public_key)
                    .map_err(|err| DatabaseError::Internal(Error::from(err)))?;
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

#[derive(Debug)]
pub(crate) enum DatabaseRequestError {
    DBConnectionFailed(Error),
    NotFound,
    InvalidParam(Error),
    Serialisation(Error),
    Unhandled(Error),
}

enum DatabaseError {
    Rusqlite(rusqlite::Error),
    SerdeJson(serde_json::Error),
    Internal(Error),
}

impl From<rusqlite::Error> for DatabaseError {
    fn from(error: rusqlite::Error) -> Self {
        DatabaseError::Rusqlite(error)
    }
}

fn wrap_query_error(error: DatabaseError) -> DatabaseRequestError {
    match error {
        DatabaseError::Rusqlite(err) => match err.to_string().as_str() {
            "Query returned no rows" => DatabaseRequestError::NotFound,
            _ => DatabaseRequestError::Unhandled(Error::from(err)),
        },
        DatabaseError::SerdeJson(err) => DatabaseRequestError::Serialisation(Error::from(err)),
        DatabaseError::Internal(err) => DatabaseRequestError::Unhandled(err),
    }
}

fn deserialize_data<'de, T: Deserialize<'de>>(data: &'de str) -> Result<T, DatabaseError> {
    serde_json::from_str::<T>(data).map_err(DatabaseError::SerdeJson)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AggregateDeployInfo {
    pub(crate) deploy_hash: String,
    pub(crate) accepted: Option<String>,
    // Once processed it will contain a stringified JSON representation of the DeployProcessed event.
    pub(crate) processed: Option<String>,
    pub(crate) expired: bool,
}

fn extract_aggregate_deploy_info(deploy_row: &Row) -> Result<AggregateDeployInfo, DatabaseError> {
    let mut aggregate_deploy: AggregateDeployInfo = AggregateDeployInfo {
        deploy_hash: "".to_string(),
        accepted: None,
        processed: None,
        expired: false,
    };

    aggregate_deploy.deploy_hash = deploy_row.get(0)?;
    aggregate_deploy.accepted = deploy_row.get::<usize, Option<String>>(2)?;
    aggregate_deploy.processed = deploy_row.get::<usize, Option<String>>(3)?;
    aggregate_deploy.expired = match deploy_row.get::<usize, u8>(4) {
        Ok(int) => integer_to_bool(int).map_err(DatabaseError::Internal)?,
        Err(err) => return Err(DatabaseError::Rusqlite(err)),
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

enum Table {
    Blocks,
    Deploys,
    Faults,
    Steps,
}

impl Display for Table {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            Table::Blocks => write!(f, "blocks"),
            Table::Deploys => write!(f, "deploys"),
            Table::Faults => write!(f, "faults"),
            Table::Steps => write!(f, "steps"),
        }
    }
}

fn create_insert_stmt(table: Table, keys: &[String]) -> String {
    let mut keys_string = String::new();
    let mut indices = String::new();

    let mut count = 0;
    keys.iter().for_each(|key| {
        count += 1;
        if count == 1 {
            keys_string = key.to_string();
            indices = format!("?{}", count);
        } else {
            keys_string = format!("{}, {}", keys_string, key);
            indices = format!("{}, ?{}", indices, count);
        }
    });

    let insert_command = format!(
        "INSERT INTO {table} ({keys}) VALUES ({indices})",
        table = table,
        keys = keys_string,
        indices = indices
    );

    insert_command
}

#[test]
fn check_create_insert_stmt() {
    let parameters = vec![
        String::from("first"),
        String::from("second"),
        String::from("third"),
    ];

    let result = create_insert_stmt(Table::Blocks, &parameters);

    assert_eq!(
        result,
        String::from("INSERT INTO blocks (first, second, third) VALUES (?1, ?2, ?3)")
    )
}
