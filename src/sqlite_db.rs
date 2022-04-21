use super::types::structs::{DeployProcessed, Fault, Step};
use anyhow::{Context, Error};
use casper_node::types::Block;
use rusqlite::{params, Connection};
use std::sync::{Arc, Mutex};
use tracing::trace;

#[derive(Clone)]
pub struct Database {
    db: Arc<Mutex<Connection>>,
}

impl Database {
    pub fn new(path: &str) -> Result<Database, Error> {
        let db = Connection::open(path)?;

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
