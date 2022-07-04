use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};
use super::types::structs::{Fault, Step};
use anyhow::{Context, Error};
use casper_node::types::Block;
use rusqlite::{params, named_params, Connection, OpenFlags, types::Value as SqlValue, Row};
use std::sync::{Arc, Mutex};
use casper_types::AsymmetricType;
use serde::{Serialize, Deserialize};
use tracing::{debug, warn};
use crate::types::enums::DeployAtState;

const DB_FILENAME: &str = "raw_sse_data.db3";

#[derive(Clone)]
pub struct Database {
    db: Arc<Mutex<Connection>>,
    pub file_path: PathBuf,
    block_columns: Vec<String>,
    deploy_columns: Vec<String>,
    step_columns: Vec<String>,
    fault_columns: Vec<String>
}

impl Database {
    pub fn new(path: &Path) -> Result<Database, Error> {
        fs::create_dir_all(path)?;
        let file_path = path.join(DB_FILENAME);
        let db = Connection::open(&file_path)?;

        let block_columns: Vec<String> = ["height", "hash", "block"].map(|x| x.to_string()).into();
        let deploy_columns: Vec<String> = ["hash", "timestamp", "accepted", "processed", "added", "expired"].map(|x| x.to_string()).into();
        let step_columns: Vec<String> = ["era", "effect"].map(|x| x.to_string()).into();
        let fault_columns: Vec<String> = ["era", "public_key", "timestamp"].map(|x| x.to_string()).into();

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


        // The deploys table contains information on Deploys at all stages of their lifecycle.
        // For a deploy that completes it's lifecycle it should contain:
        //      hash ->The hex-encoded hash of the deploy
        //      timestamp ->The deploy's timestamp (used for ordering)
        //      accepted -> A "boolean" value of 1 (accepted) or 0 (not accepted).
        //      processed -> The raw DeployProcessed event data.
        //      added -> The hex-encoded hash of the containing block.
        //      expired -> A "boolean" value of 1 (expired) or 0 (not expired).
        // +-----------+----------------+-----------------+
        // |  Column   | Initialised To |  Updated When   |
        // +-----------+----------------+-----------------+
        // | hash      | <Deploy Hash>  | DeployAccepted  |
        // | timestamp | Null           | DeployProcessed |
        // | accepted  | 1              | DeployAccepted  |
        // | processed | Null           | DeployProcessed |
        // | added     | Null           | BlockAdded      |
        // | expired   | 0              | DeployExpired   |
        // +-----------+----------------+-----------------+
        db.execute(
            "CREATE TABLE IF NOT EXISTS deploys (
                hash        STRING PRIMARY KEY,
                timestamp   STRING,
                accepted    INTEGER,
                processed   STRING,
                added       STRING,
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

        Ok(Database {
            db: Arc::new(Mutex::new(db)),
            file_path,
            block_columns,
            deploy_columns,
            step_columns,
            fault_columns
        })
    }

    pub async fn save_block(&self, block: Block) -> Result<usize, Error> {
        self.insert_data(Entity::Block(block))
    }

    pub async fn save_or_update_deploy(&self, deploy: DeployAtState) -> Result<usize, Error> {
        self.insert_data(Entity::Deploy(deploy))
    }

    pub async fn save_step(&self, step: Step) -> Result<usize, Error> {
        self.insert_data(Entity::Step(step))
    }

    pub async fn save_fault(&self, fault: Fault) -> Result<usize, Error> {
        self.insert_data(Entity::Fault(fault))
    }

    // Catch-all Insert function - internal only - not exposed in the API to other components
    fn insert_data(&self, data: Entity) -> Result<usize, Error> {
        let db_connection = self.db.lock().map_err(|err| {
            Error::msg(err.to_string())
        })?;

        return match data {
            Entity::Block(block) => {
                // The same pattern is used for each entity, I have therefore only
                // commented on it once - here.

                // Extract and format the data for each column
                let height = block.height().to_string();
                let hash = hex::encode(block.hash().inner());
                let block = serde_json::to_string(&block)?;

                // Generate the SQL for INSERTing the new row
                let insert_command = create_insert_stmt(Table::Blocks, &self.block_columns);

                // Interpolate the positional parameters into the above statement and execute
                db_connection.execute(&insert_command, params![height, hash, block]).context("Error saving block")
            }

            Entity::Deploy(deploy) => {
                match deploy {
                    DeployAtState::Accepted((deploy_hash, timestamp)) => {
                        let hash = hex::encode(deploy_hash.inner());
                        let timestamp = timestamp.to_string();

                        let insert_command = create_insert_stmt(Table::Deploys, &self.deploy_columns);

                        db_connection.execute(&insert_command, params![hash, timestamp, SqlValue::Integer(1), SqlValue::Null, SqlValue::Null, SqlValue::Integer(0)])
                            .context("Error inserting deploy")
                    }
                    DeployAtState::Processed(deploy_processed) => {
                        let hash = hex::encode(deploy_processed.deploy_hash.inner());
                        let timestamp = deploy_processed.timestamp.to_string();
                        let json_deploy = serde_json::to_string(&deploy_processed)?;

                        let update_command = "UPDATE deploys SET timestamp = ?, processed = ? WHERE hash = ?";

                        db_connection.execute(update_command,params![timestamp, json_deploy, hash])
                            .context(format!("Error updating deploy: {}", hash))
                    }
                    DeployAtState::Added((deploy_hashes, block_hash)) => {
                        let deploy_hashes: Vec<String> = deploy_hashes.iter().map(|x| {
                            hex::encode(x.inner())
                        }).collect();

                        // todo handle case where the block contains deploys that the DB doesn't yet have. This would really only occur on initial connection to the stream.

                        let block_hash = hex::encode(block_hash.inner());

                        let update_command = "UPDATE deploys SET added = ? WHERE hash = ?";

                        // todo check if there's a more efficient way to do this in a single query execution
                        for deploy_hash in &deploy_hashes {
                            db_connection.execute(&update_command,params![block_hash, deploy_hash])
                                .context(format!("Error updating deploy: {}", deploy_hash))?;
                        }
                        Ok(deploy_hashes.len())
                    }
                    DeployAtState::Expired(deploy_hash) => {
                        let hash = hex::encode(deploy_hash.inner());

                        let update_command = format!("UPDATE deploys SET expired = {} WHERE hash = ?", 1i64);

                        db_connection.execute(&update_command, params![hash])
                            .context("Error updating deploy")
                    }
                }
            }
            Entity::Fault(fault) => {
                let era = fault.era_id.value().to_string();
                let public_key = fault.public_key.to_hex();
                let timestamp = fault.timestamp.to_string();

                let insert_command = create_insert_stmt(Table::Faults, &self.fault_columns);

                db_connection.execute(&insert_command, params![era, public_key, timestamp]).context("Error saving deploy")
            }
            Entity::Step(step) => {
                let era = step.era_id.value().to_string();
                let execution_effect = serde_json::to_string(&step.execution_effect).context("Error serializing Execution Effect")?;

                let insert_command = create_insert_stmt(Table::Steps, &self.step_columns);

                db_connection.execute(&insert_command, params![era, execution_effect]).context("Error saving step")
            }
        }
    }
}

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
            Table::Steps => write!(f, "steps")
        }
    }
}

fn create_insert_stmt(table: Table, keys: &Vec<String>) -> String {
    let mut keys_string = String::new();
    let mut indices = String::new();

    let mut count = 0;
    keys.iter().for_each(|key| {
        count += 1;
        if count == 1 {
            keys_string = format!("{}", key);
            indices = format!("?{}", count);
        } else {
            keys_string = format!("{}, {}", keys_string, key);
            indices = format!("{}, ?{}", indices, count);
        }
    });

    let insert_command = format!("INSERT INTO {table} ({keys}) VALUES ({indices})",
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
        String::from("third")
    ];

    let result = create_insert_stmt(Table::Blocks, &parameters);

    assert_eq!(result, String::from("INSERT INTO blocks (first, second, third) VALUES (?1, ?2, ?3)"))
}

#[derive(Debug, Serialize)]
pub struct AggregateDeployInfo {
    deploy_hash: String,
    accepted: bool,
    // If processed it will contain a stringified JSON representation of the DeployProcessed event.
    processed: Option<String>,
    // If included in a block it will have the hex-encoded hash of the containing Block.
    added: Option<String>,
    expired: bool
}


#[derive(Clone)]
pub struct ReadOnlyDatabase {
    db: Arc<Mutex<Connection>>,
}

impl ReadOnlyDatabase {
    pub fn new(path: &Path) -> Result<ReadOnlyDatabase, Error> {
        Ok(ReadOnlyDatabase {
            db: Arc::new(Mutex::new(Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?)),
        })
    }

    pub async fn get_latest_block(&self) -> Result<Block, Error> {
        let db = self
            .db
            .lock()
            .map_err(|error| Error::msg(error.to_string()))?;
        let mut stmt = db.prepare("SELECT * FROM blocks ORDER BY height DESC LIMIT 1")?;
        let mut rows = stmt.query([])?;
        let mut maybe_block: Option<String> = None;
        while let Some(row) = rows.next()? {
            // column 2 represents the block data itself
            maybe_block = row.get(2)?;
        }

        match maybe_block {
            None => Err(Error::msg("Failed to retrieve latest block")),
            Some(block) => {
                deserialize_data::<Block>(&block)
            }
        }
    }

    pub async fn get_block_by_height(&self, height: u64) -> Result<Block, Error> {
        let db = self
            .db
            .lock()
            .map_err(|error| Error::msg(error.to_string()))?;
        let mut stmt = db.prepare("SELECT block FROM blocks where height = :height")?;
        let mut rows = stmt.query(named_params! { ":height": height })?;
        let mut maybe_block: Option<String> = None;
        while let Some(row) = rows.next()? {
            maybe_block = row.get(0)?;
        }

        match maybe_block {
            None => Err(Error::msg(format!("Failed to retrieve block at height: {}", height))),
            Some(block) => deserialize_data::<Block>(&block)
        }
    }

    pub async fn get_block_by_hash(&self, hash: &str) -> Result<Block, Error> {
        let db = self
            .db
            .lock()
            .map_err(|error| Error::msg(error.to_string()))?;
        let mut stmt = db.prepare("SELECT block FROM blocks where hash = :hash")?;
        let mut rows = stmt.query(named_params! { ":hash": hash })?;
        let mut maybe_block: Option<String> = None;
        while let Some(row) = rows.next()? {
            maybe_block = row.get(0)?;
        }

        match maybe_block {
            None => Err(Error::msg(format!("Failed to retrieve block with hash: {}", hash))),
            Some(block) => deserialize_data::<Block>(&block)
        }
    }

    pub async fn get_latest_deploy(&self) -> Result<AggregateDeployInfo, Error> {
        let db = self
            .db
            .lock()
            .map_err(|error| Error::msg(error.to_string()))?;
        // todo Double check that this does actually return the latest as expected
        let mut stmt = db.prepare("SELECT * FROM deploys ORDER BY timestamp DESC LIMIT 1")?;
        let mut rows = stmt.query([])?;

        match rows.next()? {
            None => Err(Error::msg("No records found for query")),
            Some(row) => {
                extract_aggregate_deploy_info(row)
            }
        }
    }

    pub async fn get_deploy_by_hash(&self, hash: &str) -> Result<AggregateDeployInfo, Error> {
        let db = self
            .db
            .lock()
            .map_err(|error| Error::msg(error.to_string()))?;
        let mut stmt = db.prepare("SELECT * FROM deploys where hash = :hash")?;
        let mut rows = stmt.query(named_params! { ":hash": hash })?;

        match rows.next()? {
            None => Err(Error::msg("No records found for query")),
            Some(row) => {
                extract_aggregate_deploy_info(row)
            }
        }
    }

    pub async fn get_step_by_era(&self, era_id: u64) -> Result<Step, Error> {
        let db = self
            .db
            .lock()
            .map_err(|error| Error::msg(error.to_string()))?;
        let mut stmt = db.prepare("SELECT effect FROM steps where era_id = :era_id")?;
        let mut rows = stmt.query(named_params! { ":era_id": era_id })?;
        let mut maybe_step: Option<String> = None;
        while let Some(row) = rows.next()? {
            maybe_step = row.get(0)?;
        }

        match maybe_step {
            None => Err(Error::msg(format!("Failed to retrieve step at era: {}", era_id))),
            Some(step) => deserialize_data::<Step>(&step)
        }
    }

    pub async fn get_fault_by_public_key(&self, public_key: &str) -> Result<Fault, Error> {
        let db = self
            .db
            .lock()
            .map_err(|error| Error::msg(error.to_string()))?;
        let mut stmt = db.prepare("SELECT * FROM faults where public_key = :public_key")?;
        let mut rows = stmt.query(named_params! { ":public_key": public_key })?;
        let mut maybe_fault: Option<String> = None;
        while let Some(row) = rows.next()? {
            let era_id: u64 = row.get(0)?;
            let timestamp: String = row.get(2)?;
            let fault_string = format!("{{\"era_id\":{},\"public_key\":\"{}\",\"timestamp\":{}}}",
                                      era_id,
                                      public_key,
                                      timestamp
            );
            warn!("{}", fault_string);
            maybe_fault = Some(fault_string);
        }

        match maybe_fault {
            None => Err(Error::msg(format!("Failed to retrieve faults with public key: {}", public_key))),
            Some(fault) => deserialize_data::<Fault>(&fault)
        }
    }}

fn deserialize_data<'de, T: Deserialize<'de>>(data: &'de str) -> Result<T, Error> {
    serde_json::from_str::<T>(data).map_err(|err| {
        Error::from(err)
    })
}

fn extract_aggregate_deploy_info(deploy_row: &Row) -> Result<AggregateDeployInfo, Error> {
    let mut aggregate_deploy: AggregateDeployInfo = AggregateDeployInfo {
        deploy_hash: "uninitialised".to_string(),
        accepted: false,
        processed: None,
        added: None,
        expired: false
    };

    aggregate_deploy.deploy_hash = deploy_row.get(0)?;
    aggregate_deploy.accepted = match deploy_row.get::<usize, i64>(2) {
        Ok(x) => match x {
            0 => false,
            1 => true,
            _ => return Err(Error::msg("Invalid bool number in DB"))
        }
        Err(err) => return Err(Error::from(err))
    };
    aggregate_deploy.processed = match deploy_row.get::<usize, Option<String>>(3) {
        Ok(x) => x,
        Err(err) => return Err(Error::from(err))
    };
    aggregate_deploy.added = match deploy_row.get::<usize, Option<String>>(4) {
        Ok(x) => x,
        Err(err) => return Err(Error::from(err))
    };
    aggregate_deploy.expired = match deploy_row.get::<usize, i64>(5) {
        Ok(x) => match x {
            0 => false,
            1 => true,
            _ => return Err(Error::msg("Invalid bool number in DB"))
        }
        Err(err) => return Err(Error::from(err))
    };

    Ok(aggregate_deploy)
}
