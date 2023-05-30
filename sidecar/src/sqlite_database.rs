mod errors;
mod reader;
#[cfg(test)]
mod tests;
mod writer;
use crate::{
    migration_manager::MigrationManager,
    sql::tables::{self},
    types::{
        config::SqliteConfig,
        database::{DatabaseReadError, DatabaseWriteError},
        sse_events::{BlockAdded, DeployAccepted, DeployExpired, DeployProcessed},
    },
};
use anyhow::Error;
use itertools::Itertools;
use sea_query::SqliteQueryBuilder;
use serde::Deserialize;
#[cfg(test)]
use sqlx::Row;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteRow},
    ConnectOptions, Executor, Sqlite, Transaction,
};
use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use self::errors::{wrap_query_error, SqliteDbError};

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
            tables::deploy_aggregate::create_table_stmt(),
            tables::assemble_deploy_aggregate::create_table_stmt(),
            // Raw Event tables
            tables::block_added::create_table_stmt(),
            tables::deploy_accepted::create_table_stmt(),
            tables::deploy_processed::create_table_stmt(),
            tables::deploy_expired::create_table_stmt(),
            tables::fault::create_table_stmt(),
            tables::finality_signature::create_table_stmt(),
            tables::step::create_table_stmt(),
            tables::shutdown::create_table_stmt(),
        ]
        .iter()
        .map(|stmt| stmt.to_string(SqliteQueryBuilder))
        .join(";");

        self.connection_pool
            .execute(create_table_stmts.as_str())
            .await?;
        let create_indexes_stmts = vec![
            tables::deploy_aggregate::create_deploy_aggregate_block_hash_timestamp_index(),
            tables::deploy_aggregate::create_deploy_aggregate_block_hash_index(),
            tables::assemble_deploy_aggregate::create_assemble_deploy_aggregate_block_hash_index(),
            tables::assemble_deploy_aggregate::create_assemble_deploy_aggregate_deploy_hash_index(),
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
    ) -> Result<(), DatabaseWriteError> {
        let insert_stmt =
            tables::deploy_aggregate::create_insert_stmt(deploy_hash, raw_deploy_accepted)?
                .to_string(SqliteQueryBuilder);
        transaction
            .execute(insert_stmt.as_str())
            .await
            .map(|_| ())
            .map_err(|err| DatabaseWriteError::Unhandled(Error::from(err)))
    }

    async fn save_assemble_deploy_aggregate_command<'c>(
        &self,
        transaction: &mut Transaction<'c, Sqlite>,
        deploy_hash: String,
        block_data: Option<(String, u64)>,
    ) -> Result<(), DatabaseWriteError> {
        let (maybe_block_hash, maybe_timestamp) = match block_data {
            None => (None, None),
            Some((block_hash, block_timestamp)) => (Some(block_hash), Some(block_timestamp)),
        };
        let insert_stmt = tables::assemble_deploy_aggregate::create_insert_stmt(
            deploy_hash,
            maybe_block_hash,
            maybe_timestamp,
        )?
        .to_string(SqliteQueryBuilder);
        transaction
            .execute(insert_stmt.as_str())
            .await
            .map(|_| ())
            .map_err(|err| DatabaseWriteError::Unhandled(Error::from(err)))
    }

    async fn get_deploy_processed_inner_by_hash<'c>(
        &self,
        executor: &mut Transaction<'c, Sqlite>,
        hash: &str,
    ) -> Result<DeployProcessed, DatabaseReadError> {
        let stmt = tables::deploy_processed::create_get_by_hash_stmt(hash.to_string())
            .to_string(SqliteQueryBuilder);

        executor
            .fetch_optional(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
            .and_then(|maybe_row| match maybe_row {
                None => Err(DatabaseReadError::NotFound),
                Some(row) => {
                    let raw = row
                        .try_get::<String, &str>("raw")
                        .map_err(|sqlx_error| wrap_query_error(sqlx_error.into()))?;
                    deserialize_data::<DeployProcessed>(&raw).map_err(wrap_query_error)
                }
            })
    }

    async fn get_deploy_expired_inner_by_hash<'c>(
        &self,
        executor: &mut Transaction<'c, Sqlite>,
        hash: &str,
    ) -> Result<DeployExpired, DatabaseReadError> {
        let stmt = tables::deploy_expired::create_get_by_hash_stmt(hash.to_string())
            .to_string(SqliteQueryBuilder);

        executor
            .fetch_optional(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
            .and_then(|maybe_row| match maybe_row {
                None => Err(DatabaseReadError::NotFound),
                Some(row) => {
                    let raw = row
                        .try_get::<String, &str>("raw")
                        .map_err(|sqlx_error| wrap_query_error(sqlx_error.into()))?;
                    deserialize_data::<DeployExpired>(&raw).map_err(wrap_query_error)
                }
            })
    }

    async fn get_block_inner_by_hash<'c>(
        &self,
        executor: &mut Transaction<'c, Sqlite>,
        hash: &str,
    ) -> Result<BlockAdded, DatabaseReadError> {
        let stmt = tables::block_added::create_get_by_hash_stmt(hash.to_string())
            .to_string(SqliteQueryBuilder);

        executor
            .fetch_optional(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
            .and_then(|maybe_row| match maybe_row {
                None => Err(DatabaseReadError::NotFound),
                Some(row) => parse_block_from_row(row),
            })
    }

    async fn get_deploy_accepted_inner_by_hash<'c>(
        &self,
        executor: &mut Transaction<'c, Sqlite>,
        hash: &str,
    ) -> Result<DeployAccepted, DatabaseReadError> {
        let stmt = tables::deploy_accepted::create_get_by_hash_stmt(hash.to_string())
            .to_string(SqliteQueryBuilder);

        executor
            .fetch_optional(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
            .and_then(|maybe_row| match maybe_row {
                None => Err(DatabaseReadError::NotFound),
                Some(row) => {
                    let raw = row
                        .try_get::<String, &str>("raw")
                        .map_err(|sqlx_error| wrap_query_error(sqlx_error.into()))?;
                    deserialize_data::<DeployAccepted>(&raw).map_err(wrap_query_error)
                }
            })
    }
}

fn deserialize_data<'de, T: Deserialize<'de>>(data: &'de str) -> Result<T, SqliteDbError> {
    serde_json::from_str::<T>(data).map_err(SqliteDbError::SerdeJson)
}

fn parse_block_from_row(row: SqliteRow) -> Result<BlockAdded, DatabaseReadError> {
    let raw_data = row
        .try_get::<String, &str>("raw")
        .map_err(|sqlx_err| wrap_query_error(sqlx_err.into()))?;
    deserialize_data::<BlockAdded>(&raw_data).map_err(wrap_query_error)
}
