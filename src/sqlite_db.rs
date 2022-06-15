use std::fs;
use std::path::{Path, PathBuf};
use super::types::structs::{DeployProcessed, Fault, Step};
use anyhow::{Context, Error};
use casper_node::types::Block;
use rusqlite::{params, Connection, OpenFlags, named_params};
use std::sync::{Arc, Mutex};
use tracing::{trace, error};

const DB_FILENAME: &str = "raw_sse_data.db3";

#[derive(Clone)]
pub struct Database {
    db: Arc<Mutex<Connection>>,
    pub file_path: PathBuf
}

impl Database {
    pub fn new(path: &Path) -> Result<Database, Error> {
        fs::create_dir_all(path)?;
        let file_path = path.join(DB_FILENAME);
        let db = Connection::open(&file_path)?;

        db.execute(
            "CREATE TABLE IF NOT EXISTS blocks (
                height      INTEGER PRIMARY KEY,
                hash        STRING NOT NULL,
                block       BLOB NOT NULL
            )",
            [],
        )
        .context("failed to create blocks table in database")?;
        trace!("SQLite - Blocks table initialised");

        db.execute(
            "CREATE TABLE IF NOT EXISTS deploys (
                hash        STRING PRIMARY KEY,
                timestamp   STRING NOT NULL,
                deploy      BLOB NOT NULL
            )",
            [],
        )
        .context("failed to create deploys table in database")?;
        trace!("SQLite - Deploys table initialised");

        db.execute(
            "CREATE TABLE IF NOT EXISTS steps (
                era_id      INTEGER PRIMARY KEY,
                effect      BLOB NOT NULL
        )",
            [],
        )
        .context("failed to create steps table in database")?;
        trace!("SQLite - Steps table initialised");

        db.execute(
            "CREATE TABLE IF NOT EXISTS faults (
                era_id      INTEGER,
                public_key  STRING NOT NULL,
                timestamp   STRING NOT NULL,
                PRIMARY KEY (era_id, public_key)
        )",
            [],
        )
        .context("failed to create faults table in database")?;
        trace!("SQLite - Faults table initialised");

        Ok(Database {
            db: Arc::new(Mutex::new(db)),
            file_path
        })
    }

    pub async fn save_block(&self, block: &Block) -> Result<usize, Error> {
        let height = block.height();
        let hash = hex::encode(block.hash().inner());
        let serialised_block = serde_json::to_string(block)
            .map_err(Error::msg)
            .with_context(|| format!("Serialisation failed for block at height: {}", height))?;
        let db = self
            .db
            .lock()
            .map_err(|error| Error::msg(error.to_string()))?;
        db.execute(
            "INSERT INTO blocks (height, hash, block) VALUES (?1, ?2, ?3)",
            params![height, hash, serialised_block],
        )
        .with_context(|| format!("failed to save block at height: {}", height))
    }

    #[allow(unused)]
    pub async fn save_deploy(&self, deploy: &DeployProcessed) -> Result<usize, Error> {
        let hash = hex::encode(deploy.deploy_hash.value());
        let timestamp = deploy.timestamp.to_string();
        let serialised_deploy =
            serde_json::to_string(deploy).map_err(|error| Error::msg(error.to_string()))?;
        let db = self
            .db
            .lock()
            .map_err(|error| Error::msg(error.to_string()))?;
        db.execute(
            "INSERT INTO deploys (hash, timestamp, deploy) VALUES (?1, ?2, ?3)",
            params![hash, timestamp, serialised_deploy],
        )
        .context("failed to save deploy")
    }

    #[allow(unused)]
    pub async fn save_step(&self, step: &Step) -> Result<usize, Error> {
        let serialised_effect =
            serde_json::to_string(&step.execution_effect).map_err(Error::msg)?;
        let db = self
            .db
            .lock()
            .map_err(|error| Error::msg(error.to_string()))?;
        db.execute(
            "INSERT INTO steps (era_id, effect) VALUES (?1, ?2)",
            params![u64::from(step.era_id), serialised_effect],
        )
        .context("failed to save step")
    }

    #[allow(unused)]
    pub async fn save_fault(&self, fault: &Fault) -> Result<usize, Error> {
        // 'from' trait implementation is preferred to calling .value()
        let era = u64::from(fault.era_id);
        let public_key = serde_json::to_string(&fault.public_key)
            .map_err(|error| Error::msg(error.to_string()))?;
        let timestamp = serde_json::to_string(&fault.timestamp)
            .map_err(|error| Error::msg(error.to_string()))?;
        let db = self
            .db
            .lock()
            .map_err(|error| Error::msg(error.to_string()))?;
        db.execute(
            "INSERT INTO faults (era_id, public_key, timestamp) VALUES (?1, ?2, ?3)",
            params![era, public_key, timestamp],
        )
        .context("failed to save fault")
    }
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

    pub async fn get_latest_block(&self) -> Result<String, Error> {
        let db = self
            .db
            .lock()
            .map_err(|error| Error::msg(error.to_string()))?;
        let mut stmt = db.prepare("SELECT * FROM blocks ORDER BY height DESC LIMIT 1")?;
        let mut rows = stmt.query([])?;
        let mut block: Option<String> = None;
        while let Some(row) = rows.next()? {
            block = row.get(2)?;
        }

        block.ok_or(Error::msg("No latest block found"))
    }

    pub async fn get_block_by_height(&self, height: u64) -> Result<String, Error> {
        let db = self
            .db
            .lock()
            .map_err(|error| Error::msg(error.to_string()))?;
        let mut stmt = db.prepare("SELECT block FROM blocks where height = :height")?;
        let mut rows = stmt.query(named_params! { ":height": height })?;
        let mut block: Option<String> = None;
        while let Some(row) = rows.next()? {
            block = row.get(0)?;
        }

        block.ok_or(Error::msg(format!(
            "Unable to find block at height: {}",
            height
        )))
    }

    pub async fn get_block_by_hash(&self, hash: &str) -> Result<String, Error> {
        let db = self
            .db
            .lock()
            .map_err(|error| Error::msg(error.to_string()))?;
        let mut stmt = db.prepare("SELECT block FROM blocks where hash = :hash")?;
        let mut rows = stmt.query(named_params! { ":hash": hash })?;
        let mut block: Option<String> = None;
        while let Some(row) = rows.next()? {
            block = row.get(0)?;
        }

        block.ok_or(Error::msg(format!(
            "Unable to find block with hash: {}",
            hash
        )))
    }

    pub async fn get_latest_deploy(&self) -> Result<String, Error> {
        let db = self
            .db
            .lock()
            .map_err(|error| Error::msg(error.to_string()))?;
        let mut stmt = db.prepare("SELECT * FROM deploys ORDER BY timestamp DESC LIMIT 1")?;
        let mut rows = stmt.query([])?;
        let mut deploy: Option<String> = None;
        while let Some(row) = rows.next()? {
            deploy = row.get(2)?;
        }

        deploy.ok_or(Error::msg("No latest deploy found"))
    }

    pub async fn get_deploy_by_hash(&self, hash: &str) -> Result<String, Error> {
        let db = self
            .db
            .lock()
            .map_err(|error| Error::msg(error.to_string()))?;
        let mut stmt = db.prepare("SELECT deploy FROM deploys where hash = :hash")?;
        let mut rows = stmt.query(named_params! { ":hash": hash })?;
        let mut deploy: Option<String> = None;
        while let Some(row) = rows.next()? {
            deploy = row.get(0)?;
        }

        deploy.ok_or(Error::msg(format!(
            "Unable to find deploy with hash: {}",
            hash
        )))
    }

    pub async fn get_step_by_era(&self, era_id: u64) -> Result<String, Error> {
        let db = self
            .db
            .lock()
            .map_err(|error| Error::msg(error.to_string()))?;
        let mut stmt = db.prepare("SELECT effect FROM steps where era_id = :era_id")?;
        let mut rows = stmt.query(named_params! { ":era_id": era_id })?;
        let mut step_effect: Option<String> = None;
        while let Some(row) = rows.next()? {
            step_effect = row.get(0)?;
        }

        step_effect.ok_or(Error::msg(format!(
            "Unable to find step for era: {}",
            era_id
        )))
    }

    // TODO: Add fault retrieval
}


