mod errors;
mod reader;
#[cfg(test)]
mod tests;
mod writer;

use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::Error;
use itertools::Itertools;
use sea_query::SqliteQueryBuilder;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions},
    ConnectOptions, Executor,
};

use crate::sql::tables;

const MAX_WRITE_CONNECTIONS: u32 = 10;
const MAX_READ_CONNECTIONS: u32 = 100;
/// This pragma queries or sets the [write-ahead log](https://www.sqlite.org/wal.html) [auto-checkpoint](https://www.sqlite.org/wal.html#ckpt) interval.
const WAL_AUTOCHECKPOINT_KEY: &str = "wal_autocheckpoint";

#[derive(Clone)]
pub struct SqliteDatabase {
    pub connection_pool: SqlitePool,
    pub file_path: PathBuf,
}

impl SqliteDatabase {
    pub async fn new(
        database_dir: &Path,
        database_file_name: String,
        auto_checkpoint_interval: u16,
    ) -> Result<SqliteDatabase, Error> {
        fs::create_dir_all(database_dir)?;

        match database_dir.join(database_file_name).to_str() {
            None => Err(Error::msg("Error handling path to database")),
            Some(path) => {
                let connection_pool = SqlitePoolOptions::new()
                    .max_connections(MAX_WRITE_CONNECTIONS)
                    .connect_lazy_with(
                        SqliteConnectOptions::from_str(path)?
                            .create_if_missing(true)
                            .journal_mode(SqliteJournalMode::Wal)
                            .pragma(WAL_AUTOCHECKPOINT_KEY, auto_checkpoint_interval.to_string())
                            .disable_statement_logging()
                            .to_owned(),
                    );

                let sqlite_db = SqliteDatabase {
                    connection_pool,
                    file_path: Path::new(&path).into(),
                };

                sqlite_db.initialise_with_tables().await?;

                Ok(sqlite_db)
            }
        }
    }

    pub fn new_read_only(path: &Path) -> Result<SqliteDatabase, Error> {
        let connection_pool = SqlitePoolOptions::new()
            .max_connections(MAX_READ_CONNECTIONS)
            .connect_lazy_with(
                SqliteConnectOptions::from_str(path.to_str().unwrap())?
                    .read_only(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .disable_statement_logging()
                    .to_owned(),
            );

        Ok(SqliteDatabase {
            connection_pool,
            file_path: path.into(),
        })
    }

    #[cfg(test)]
    pub async fn new_in_memory() -> Result<SqliteDatabase, Error> {
        let connection_pool = SqlitePoolOptions::new()
            .max_connections(MAX_WRITE_CONNECTIONS)
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

        sqlite_db.initialise_with_tables().await?;

        Ok(sqlite_db)
    }

    pub async fn initialise_with_tables(&self) -> Result<(), Error> {
        let create_table_stmts = vec![
            // Synthetic tables
            tables::event_type::create_table_stmt(),
            tables::event_log::create_table_stmt(),
            tables::deploy_event::create_table_stmt(),
            // Raw Event tables
            tables::block_added::create_table_stmt(),
            tables::deploy_accepted::create_table_stmt(),
            tables::deploy_processed::create_table_stmt(),
            tables::deploy_expired::create_table_stmt(),
            tables::fault::create_table_stmt(),
            tables::finality_signature::create_table_stmt(),
            tables::step::create_table_stmt(),
        ]
        .iter()
        .map(|stmt| stmt.to_string(SqliteQueryBuilder))
        .join(";");

        self.connection_pool
            .execute(create_table_stmts.as_str())
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
}
