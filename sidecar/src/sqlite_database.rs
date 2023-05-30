mod errors;
mod reader;
#[cfg(test)]
mod tests;
mod writer;
use crate::{
    migration_manager::MigrationManager,
    sql::tables,
    types::{config::SqliteConfig, database::DatabaseWriteError},
};
use anyhow::{Context, Error};
use itertools::Itertools;
use sea_query::SqliteQueryBuilder;
#[cfg(test)]
use sqlx::Row;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions},
    ConnectOptions, Executor, Sqlite, Transaction,
};
use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

/// This pragma queries or sets the [write-ahead log](https://www.sqlite.org/wal.html) [auto-checkpoint](https://www.sqlite.org/wal.html#ckpt) interval.
const WAL_AUTOCHECKPOINT_KEY: &str = "wal_autocheckpoint";

/// [SqliteDatabase] can be cloned to allow multiple components access to the database.
/// The [SqlitePool] is cloned using an [Arc](std::sync::Arc) so each cloned instance of [SqliteDatabase] shares the same connection pool.
#[derive(Clone)]
pub struct SqliteDatabase {
    pub connection_pool: SqlitePool,
    pub file_path: PathBuf,
}

impl SqliteDatabase {
    pub async fn new(database_dir: &Path, config: SqliteConfig) -> Result<SqliteDatabase, Error> {
        fs::create_dir_all(database_dir)?;

        match database_dir.join(config.file_name).to_str() {
            None => Err(Error::msg("Error handling path to database")),
            Some(path) => {
                let connection_pool = SqlitePoolOptions::new()
                    .max_connections(config.max_connections_in_pool)
                    .connect_lazy_with(
                        SqliteConnectOptions::from_str(path)?
                            .create_if_missing(true)
                            .journal_mode(SqliteJournalMode::Wal)
                            .pragma(
                                WAL_AUTOCHECKPOINT_KEY,
                                config.wal_autocheckpointing_interval.to_string(),
                            )
                            .disable_statement_logging()
                            .to_owned(),
                    );

                let sqlite_db = SqliteDatabase {
                    connection_pool,
                    file_path: Path::new(&path).into(),
                };
                MigrationManager::apply_all_migrations(sqlite_db.clone()).await?;

                Ok(sqlite_db)
            }
        }
    }

    async fn store_version_based_on_result(
        &self,
        maybe_version: Option<u32>,
        res: Result<(), DatabaseWriteError>,
    ) -> Result<(), DatabaseWriteError> {
        let db_connection = &self.connection_pool;
        match maybe_version {
            None => res,
            Some(version) => match res {
                Ok(_) => {
                    let insert_version_stmt = tables::migration::create_insert_stmt(version, true)?
                        .to_string(SqliteQueryBuilder);
                    db_connection.execute(insert_version_stmt.as_str()).await?;
                    Ok(())
                }
                Err(e) => {
                    let insert_version_stmt =
                        tables::migration::create_insert_stmt(version, false)?
                            .to_string(SqliteQueryBuilder);
                    db_connection.execute(insert_version_stmt.as_str()).await?;
                    Err(e)
                }
            },
        }
    }

    #[cfg(test)]
    pub async fn new_in_memory(max_connections: u32) -> Result<SqliteDatabase, Error> {
        let sqlite_db = Self::new_in_memory_no_migrations(max_connections).await?;
        MigrationManager::apply_all_migrations(sqlite_db.clone()).await?;
        Ok(sqlite_db)
    }

    #[cfg(test)]
    pub async fn new_in_memory_no_migrations(
        max_connections: u32,
    ) -> Result<SqliteDatabase, Error> {
        let connection_pool = SqlitePoolOptions::new()
            .max_connections(max_connections)
            .connect_lazy_with(
                SqliteConnectOptions::from_str(":memory:")?
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .disable_statement_logging()
                    .to_owned(),
            );

        let sqlite_db = SqliteDatabase {
            connection_pool,
            file_path: Path::new("in_memory").into(),
        };
        Ok(sqlite_db)
    }

    #[cfg(test)]
    pub async fn get_number_of_tables(&self) -> u32 {
        self.connection_pool
        .fetch_one("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name != 'android_metadata' AND name != 'sqlite_sequence';")
        .await
        .unwrap()
        .get::<u32, usize>(0)
    }

    pub async fn initialise_with_tables(&self) -> Result<(), Error> {
        let create_table_stmts = vec![
            // Synthetic tables
            tables::event_type::create_table_stmt(),
            tables::event_log::create_table_stmt(),
            tables::deploy_event::create_table_stmt(),
            tables::block_deploys::create_table_stmt(),
            // Raw Event tables
            tables::block_added::create_table_stmt(),
            tables::deploy_accepted::create_table_stmt(),
            tables::deploy_processed::create_table_stmt(),
            tables::deploy_expired::create_table_stmt(),
            tables::fault::create_table_stmt(),
            tables::finality_signature::create_table_stmt(),
            tables::step::create_table_stmt(),
            tables::shutdown::create_table_stmt(),
            tables::deploy_aggregate::create_table_stmt(),
        ]
        .iter()
        .map(|stmt| stmt.to_string(SqliteQueryBuilder))
        .join(";");

        self.connection_pool
            .execute(create_table_stmts.as_str())
            .await?;

        let create_indexes_stmts = vec![
            tables::block_deploys::create_block_deploys_deploy_hash_index(),
            tables::block_deploys::create_block_deploys_block_hash_index(),
        ]
        .iter()
        .map(|stmt| stmt.to_string(SqliteQueryBuilder))
        .join(";");
        self.connection_pool
            .execute(create_indexes_stmts.as_str())
            .await?;

        let initialise_event_type =
            tables::event_type::create_initialise_stmt()?.to_string(SqliteQueryBuilder);

        // The result is swallowed because this call may fail if the tables were already created and populated.
        let _ = self
            .connection_pool
            .execute(initialise_event_type.as_str())
            .await;

        Ok(())
    }

    async fn get_transaction(&self) -> Result<Transaction<Sqlite>, sqlx::Error> {
        self.connection_pool.begin().await
    }

    async fn save_deploy_aggregate<'c>(
        &self,
        transaction: &mut Transaction<'c, Sqlite>,
        deploy_hash: String,
        raw_deploy_accepted: String,
    ) -> Result<(), sqlx::Error> {
        let insert_stmt =
            tables::deploy_aggregate::create_insert_stmt(deploy_hash, raw_deploy_accepted)?
                .to_string(SqliteQueryBuilder);
        transaction.execute(insert_stmt.as_str()).await.map(|_| ())
    }

    async fn update_deploy_aggregate<'c>(
        &self,
        transaction: &mut Transaction<'c, Sqlite>,
        deploy_hash: String,
    ) -> Result<(), sqlx::Error> {
        let deploy_accepted = self.get_deploy_accepted_by_hash(deploy_hash.as_str()).await;
        if deploy_accepted.is_err() {
            //This means no DeployAccepted was observed, no point in proceeding since we build aggregates only for deploys with DeployAccepted present
            return Ok(());
        }
        let maybe_deploy_processed = match self
            .get_deploy_processed_by_hash(deploy_hash.as_str())
            .await
        {
            Ok(deploy_processed) => Some(deploy_processed),
            Err(_) => None,
        };
        let maybe_deploy_expired_raw =
            match self.get_deploy_expired_by_hash(deploy_hash.as_str()).await {
                Ok(deploy_expired) => Some(serde_json::to_string(&deploy_expired).unwrap()),
                Err(_) => None,
            };

        let (maybe_deploy_processed_raw, maybe_block) = match maybe_deploy_processed {
            None => (None, None),
            Some(deploy_processed) => {
                let deploy_hash = deploy_processed.hex_encoded_block_hash();
                let maybe_block = match self.get_block_by_hash(deploy_hash.as_str()).await {
                    Ok(block) => Some(block),
                    Err(_) => None,
                };
                let deploy_processed_raw = Some(serde_json::to_string(&deploy_processed).unwrap());
                (deploy_processed_raw, maybe_block)
            }
        };
        let maybe_block_data =
            maybe_block.map(|b| (b.hex_encoded_hash(), b.block.header.timestamp.millis()));
        let stmt = tables::deploy_aggregate::create_update_stmt(
            deploy_hash,
            maybe_deploy_processed_raw,
            maybe_deploy_expired_raw,
            maybe_block_data,
        ).to_string(SqliteQueryBuilder);
        transaction.execute(stmt.as_str()).await.map(|_| ())
    }
}
