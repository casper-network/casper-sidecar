mod reader;
#[cfg(test)]
mod tests;
mod writer;
use super::migration_manager::MigrationManager;
#[cfg(test)]
use crate::types::config::StorageConfig;
use crate::{
    sql::tables,
    types::{config::SqliteConfig, database::DatabaseWriteError},
};
use anyhow::Error;
use sea_query::SqliteQueryBuilder;
#[cfg(any(feature = "testing", test))]
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
                            .disable_statement_logging(),
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

    async fn get_transaction(&self) -> Result<Transaction<Sqlite>, sqlx::Error> {
        self.connection_pool.begin().await
    }
}

impl SqliteDatabase {
    #[cfg(test)]
    pub async fn new_from_config(storage_config: &StorageConfig) -> Result<SqliteDatabase, Error> {
        match storage_config {
            StorageConfig::SqliteDbConfig {
                storage_path,
                sqlite_config,
            } => SqliteDatabase::new(Path::new(storage_path), sqlite_config.clone()).await,
            StorageConfig::PostgreSqlDbConfig { .. } => Err(Error::msg(
                "can't build Sqlite database from postgres config",
            )),
        }
    }

    #[cfg(any(feature = "testing", test))]
    pub async fn new_in_memory(max_connections: u32) -> Result<SqliteDatabase, Error> {
        let sqlite_db = Self::new_in_memory_no_migrations(max_connections)?;
        MigrationManager::apply_all_migrations(sqlite_db.clone()).await?;
        Ok(sqlite_db)
    }

    pub fn new_in_memory_no_migrations(max_connections: u32) -> Result<SqliteDatabase, Error> {
        let connection_pool = SqlitePoolOptions::new()
            .max_connections(max_connections)
            .connect_lazy_with(
                SqliteConnectOptions::from_str(":memory:")?
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .disable_statement_logging(),
            );

        let sqlite_db = SqliteDatabase {
            connection_pool,
            file_path: Path::new("in_memory").into(),
        };
        Ok(sqlite_db)
    }

    pub async fn get_number_of_tables(&self) -> u32 {
        self.connection_pool
        .fetch_one("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name != 'android_metadata' AND name != 'sqlite_sequence';")
        .await
        .unwrap()
        .get::<u32, usize>(0)
    }
}
