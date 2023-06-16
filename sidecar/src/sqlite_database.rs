mod errors;
mod reader;
#[cfg(test)]
mod tests;
mod writer;
#[cfg(test)]
use crate::types::database::DeployAggregateEntity;
use crate::{
    migration_manager::MigrationManager,
    sql::tables,
    types::{config::SqliteConfig, database::DatabaseWriteError},
};
use anyhow::Error;
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

    async fn get_transaction(&self) -> Result<Transaction<Sqlite>, sqlx::Error> {
        self.connection_pool.begin().await
    }

    #[cfg(test)]
    pub async fn new_in_memory(max_connections: u32) -> Result<SqliteDatabase, Error> {
        let sqlite_db = Self::new_in_memory_no_migrations(max_connections).await?;
        MigrationManager::apply_all_migrations(sqlite_db.clone()).await?;
        Ok(sqlite_db)
    }

    async fn save_assemble_deploy_aggregate_command<'c>(
        &self,
        transaction: &mut Transaction<'c, Sqlite>,
        deploy_hash: String,
        maybe_block_data: Option<(String, u64)>,
    ) -> Result<(), DatabaseWriteError> {
        let insert_stmt =
            tables::pending_deploy_aggregations::create_insert_stmt(deploy_hash, maybe_block_data)?
                .to_string(SqliteQueryBuilder);
        transaction
            .execute(insert_stmt.as_str())
            .await
            .map(|_| ())
            .map_err(|err| DatabaseWriteError::Unhandled(Error::from(err)))
    }

    ///This function is temporary. DeployAggregateEntitis should be assembled automatically when
    /// DeployAccepted, DeployProcessed, DeployExpired and/or BlockAdded entities are being observed.
    /// But this functionality will be introduced in the next PR since this one is big enough
    #[cfg(test)]
    pub async fn save_deploy_aggregate(
        &self,
        entity: DeployAggregateEntity,
        block_hash: Option<String>,
    ) -> Result<usize, DatabaseWriteError> {
        let db_connection = &self.connection_pool;
        let insert_version_stmt = tables::deploy_aggregate::create_insert_stmt(
            entity.deploy_hash,
            Some(entity.deploy_accepted_raw),
            entity.deploy_processed_raw,
            entity.is_expired,
            block_hash,
            entity.block_timestamp.map(|el| el as u64),
        )?
        .to_string(SqliteQueryBuilder);
        db_connection.execute(insert_version_stmt.as_str()).await?;
        Ok(0)
    }

    ///This function is temporary. DeployAggregateEntitis should be assembled automatically when
    /// DeployAccepted, DeployProcessed, DeployExpired and/or BlockAdded entities are being observed.
    /// But this functionality will be introduced in the next PR since this one is big enough
    #[cfg(test)]
    pub async fn update_deploy_aggregate(
        &self,
        deploy_hash: String,
        deploy_accepted_raw: Option<String>,
        deploy_processed_raw: Option<String>,
        is_expired: bool,
        block_hash: Option<String>,
        block_timestamp: Option<u64>,
    ) -> Result<usize, DatabaseWriteError> {
        let db_connection = &self.connection_pool;
        let update_stmt = tables::deploy_aggregate::create_update_stmt(
            deploy_hash,
            deploy_accepted_raw,
            deploy_processed_raw,
            is_expired,
            block_hash,
            block_timestamp,
        )
        .to_string(SqliteQueryBuilder);
        db_connection.execute(update_stmt.as_str()).await?;
        Ok(0)
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
}
