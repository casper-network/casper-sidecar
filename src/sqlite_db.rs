use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use casper_node::types::FinalitySignature as FinSig;
use casper_types::AsymmetricType;

use anyhow::Error;
use async_trait::async_trait;
use itertools::Itertools;
use sea_query::SqliteQueryBuilder;
use serde::Deserialize;
use sqlx::{
    sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteQueryResult,
        SqliteRow,
    },
    ConnectOptions, Executor, Row,
};

use crate::{
    database::{AggregateDeployInfo, DatabaseReader, DatabaseRequestError, DatabaseWriter},
    sql::{tables, tables::event_type::EventTypeId},
    types::sse_events::*,
};

const MAX_WRITE_CONNECTIONS: u32 = 10;
const MAX_READ_CONNECTIONS: u32 = 100;

#[derive(Clone)]
pub struct SqliteDb {
    // todo allow checkpointing to be configurable + log size
    // todo return and share ref
    pub connection_pool: SqlitePool,
    pub file_path: PathBuf,
}

impl SqliteDb {
    pub async fn new(database_dir: &Path, database_file_name: String) -> Result<SqliteDb, Error> {
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
                            .disable_statement_logging()
                            .to_owned(),
                    );

                let sqlite_db = SqliteDb {
                    connection_pool,
                    file_path: Path::new(&path).into(),
                };

                sqlite_db.initialise_with_tables().await?;

                Ok(sqlite_db)
            }
        }
    }

    pub fn new_read_only(path: &Path) -> Result<SqliteDb, Error> {
        let connection_pool = SqlitePoolOptions::new()
            .max_connections(MAX_READ_CONNECTIONS)
            .connect_lazy_with(
                SqliteConnectOptions::from_str(path.to_str().unwrap())?
                    .read_only(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .disable_statement_logging()
                    .to_owned(),
            );

        Ok(SqliteDb {
            connection_pool,
            file_path: path.into(),
        })
    }

    #[cfg(test)]
    pub async fn new_in_memory() -> Result<SqliteDb, Error> {
        let connection_pool = SqlitePoolOptions::new()
            .max_connections(MAX_WRITE_CONNECTIONS)
            .connect_lazy_with(
                SqliteConnectOptions::from_str(":memory:")?
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .disable_statement_logging()
                    .to_owned(),
            );

        let sqlite_db = SqliteDb {
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

fn handle_sqlite_result(result: Result<SqliteQueryResult, sqlx::Error>) -> Result<usize, Error> {
    result
        .map_err(Error::from)
        .map(|sqlite_res| sqlite_res.rows_affected() as usize)
}

#[async_trait]
impl DatabaseWriter for SqliteDb {
    async fn save_block_added(
        &self,
        block_added: BlockAdded,
        event_id: u64,
        event_source_address: String,
    ) -> Result<usize, Error> {
        let db_connection = &self.connection_pool;

        let json = serde_json::to_string(&block_added)?;
        let encoded_hash = block_added.hex_encoded_hash();

        let insert_to_event_log_stmt = tables::event_log::create_insert_stmt(
            EventTypeId::BlockAdded as u8,
            &event_source_address,
            event_id,
        )?
        .to_string(SqliteQueryBuilder);

        let row = db_connection
            .fetch_one(insert_to_event_log_stmt.as_str())
            .await
            .unwrap();

        let event_log_id = row.try_get::<i64, usize>(0)?;

        let insert_stmt = tables::block_added::create_insert_stmt(
            block_added.get_height(),
            encoded_hash,
            json,
            event_log_id as u64,
        )?
        .to_string(SqliteQueryBuilder);

        handle_sqlite_result(db_connection.execute(insert_stmt.as_str()).await)
    }

    async fn save_deploy_accepted(
        &self,
        deploy_accepted: DeployAccepted,
        event_id: u64,
        event_source_address: String,
    ) -> Result<usize, Error> {
        let db_connection = &self.connection_pool;

        let json = serde_json::to_string(&deploy_accepted)?;
        let encoded_hash = deploy_accepted.hex_encoded_hash();

        let insert_to_event_log_stmt = tables::event_log::create_insert_stmt(
            EventTypeId::DeployAccepted as u8,
            &event_source_address,
            event_id,
        )?
        .to_string(SqliteQueryBuilder);

        let event_log_id = db_connection
            .fetch_one(insert_to_event_log_stmt.as_str())
            .await?
            .try_get::<i64, usize>(0)?;

        let batched_insert_stmts = vec![
            tables::deploy_accepted::create_insert_stmt(
                encoded_hash.clone(),
                json,
                event_log_id as u64,
            )?,
            tables::deploy_event::create_insert_stmt(event_log_id as u64, encoded_hash)?,
        ]
        .iter()
        .map(|stmt| stmt.to_string(SqliteQueryBuilder))
        .join(";");

        handle_sqlite_result(db_connection.execute(batched_insert_stmts.as_str()).await)
    }

    async fn save_deploy_processed(
        &self,
        deploy_processed: DeployProcessed,
        event_id: u64,
        event_source_address: String,
    ) -> Result<usize, Error> {
        let db_connection = &self.connection_pool;

        let json = serde_json::to_string(&deploy_processed)?;
        let encoded_hash = deploy_processed.hex_encoded_hash();

        let insert_to_event_log_stmt = tables::event_log::create_insert_stmt(
            EventTypeId::DeployProcessed as u8,
            &event_source_address,
            event_id,
        )?
        .to_string(SqliteQueryBuilder);

        let event_log_id = db_connection
            .fetch_one(insert_to_event_log_stmt.as_str())
            .await?
            .try_get::<i64, usize>(0)?;

        let batched_insert_stmts = vec![
            tables::deploy_processed::create_insert_stmt(
                encoded_hash.clone(),
                json,
                event_log_id as u64,
            )?,
            tables::deploy_event::create_insert_stmt(event_log_id as u64, encoded_hash)?,
        ]
        .iter()
        .map(|stmt| stmt.to_string(SqliteQueryBuilder))
        .join(";");

        handle_sqlite_result(db_connection.execute(batched_insert_stmts.as_str()).await)
    }

    async fn save_deploy_expired(
        &self,
        deploy_expired: DeployExpired,
        event_id: u64,
        event_source_address: String,
    ) -> Result<usize, Error> {
        let db_connection = &self.connection_pool;

        let encoded_hash = deploy_expired.hex_encoded_hash();

        let insert_to_event_log_stmt = tables::event_log::create_insert_stmt(
            EventTypeId::DeployExpired as u8,
            &event_source_address,
            event_id,
        )?
        .to_string(SqliteQueryBuilder);

        let event_log_id = db_connection
            .fetch_one(insert_to_event_log_stmt.as_str())
            .await?
            .try_get::<i64, usize>(0)?;

        let batched_insert_stmts = vec![
            tables::deploy_expired::create_insert_stmt(encoded_hash.clone(), event_log_id as u64)?,
            tables::deploy_event::create_insert_stmt(event_log_id as u64, encoded_hash)?,
        ]
        .iter()
        .map(|stmt| stmt.to_string(SqliteQueryBuilder))
        .join(";");

        handle_sqlite_result(db_connection.execute(batched_insert_stmts.as_str()).await)
    }

    async fn save_fault(
        &self,
        fault: Fault,
        event_id: u64,
        event_source_address: String,
    ) -> Result<usize, Error> {
        let db_connection = &self.connection_pool;

        let json = serde_json::to_string(&fault)?;
        let era_id = u64::from(fault.era_id);
        let public_key = fault.public_key.to_hex();

        let insert_to_event_log_stmt = tables::event_log::create_insert_stmt(
            EventTypeId::Fault as u8,
            &event_source_address,
            event_id,
        )?
        .to_string(SqliteQueryBuilder);

        let event_log_id = db_connection
            .fetch_one(insert_to_event_log_stmt.as_str())
            .await?
            .try_get::<i64, usize>(0)?;

        let insert_stmt =
            tables::fault::create_insert_stmt(era_id, public_key, json, event_log_id as u64)?
                .to_string(SqliteQueryBuilder);

        handle_sqlite_result(db_connection.execute(insert_stmt.as_str()).await)
    }

    async fn save_finality_signature(
        &self,
        finality_signature: FinalitySignature,
        event_id: u64,
        event_source_address: String,
    ) -> Result<usize, Error> {
        let db_connection = &self.connection_pool;

        let json = serde_json::to_string(&finality_signature)?;
        let block_hash = finality_signature.hex_encoded_block_hash();
        let public_key = finality_signature.hex_encoded_public_key();

        let insert_to_event_log_stmt = tables::event_log::create_insert_stmt(
            EventTypeId::Fault as u8,
            &event_source_address,
            event_id,
        )?
        .to_string(SqliteQueryBuilder);

        let event_log_id = db_connection
            .fetch_one(insert_to_event_log_stmt.as_str())
            .await?
            .try_get::<i64, usize>(0)?;

        let insert_stmt = tables::finality_signature::create_insert_stmt(
            block_hash,
            public_key,
            json,
            event_log_id as u64,
        )?
        .to_string(SqliteQueryBuilder);

        handle_sqlite_result(db_connection.execute(insert_stmt.as_str()).await)
    }

    async fn save_step(
        &self,
        step: Step,
        event_id: u64,
        event_source_address: String,
    ) -> Result<usize, Error> {
        let db_connection = &self.connection_pool;

        let json = serde_json::to_string(&step)?;
        let era_id = u64::from(step.era_id);

        let insert_to_event_log_stmt = tables::event_log::create_insert_stmt(
            EventTypeId::Step as u8,
            &event_source_address,
            event_id,
        )?
        .to_string(SqliteQueryBuilder);

        let event_log_id = db_connection
            .fetch_one(insert_to_event_log_stmt.as_str())
            .await?
            .try_get::<i64, usize>(0)?;

        let insert_stmt = tables::step::create_insert_stmt(era_id, json, event_log_id as u64)?
            .to_string(SqliteQueryBuilder);

        handle_sqlite_result(db_connection.execute(insert_stmt.as_str()).await)
    }
}

fn parse_block_from_row(row: SqliteRow) -> Result<BlockAdded, DatabaseRequestError> {
    let raw_data = row
        .try_get::<String, &str>("raw")
        .map_err(|sqlx_err| wrap_query_error(sqlx_err.into()))?;
    deserialize_data::<BlockAdded>(&raw_data).map_err(wrap_query_error)
}

async fn fetch_optional_with_error_check(
    connection: &SqlitePool,
    stmt: String,
) -> Result<SqliteRow, DatabaseRequestError> {
    connection
        .fetch_optional(stmt.as_str())
        .await
        .map_err(|sql_err| DatabaseRequestError::Unhandled(Error::from(sql_err)))
        .and_then(|maybe_row| match maybe_row {
            None => Err(DatabaseRequestError::NotFound),
            Some(row) => Ok(row),
        })
}

#[async_trait]
impl DatabaseReader for SqliteDb {
    async fn get_latest_block(&self) -> Result<BlockAdded, DatabaseRequestError> {
        let db_connection = &self.connection_pool;

        let stmt = tables::block_added::create_get_latest_stmt().to_string(SqliteQueryBuilder);

        let row = fetch_optional_with_error_check(db_connection, stmt).await?;

        parse_block_from_row(row)
    }

    async fn get_block_by_height(&self, height: u64) -> Result<BlockAdded, DatabaseRequestError> {
        let db_connection = &self.connection_pool;

        let stmt =
            tables::block_added::create_get_by_height_stmt(height).to_string(SqliteQueryBuilder);

        let row = fetch_optional_with_error_check(db_connection, stmt).await?;

        parse_block_from_row(row)
    }

    async fn get_block_by_hash(&self, hash: &str) -> Result<BlockAdded, DatabaseRequestError> {
        let hash_regex = regex::Regex::new("^([A-Fa-f0-9]){64}$")
            .map_err(|err| DatabaseRequestError::Unhandled(Error::from(err)))?;
        if !hash_regex.is_match(hash) {
            return Err(DatabaseRequestError::InvalidParam(Error::msg(format!(
                "Expected hex-encoded block hash (64 chars), received: {} (length: {})",
                hash,
                hash.len()
            ))));
        }

        let db_connection = &self.connection_pool;

        let stmt = tables::block_added::create_get_by_hash_stmt(hash.to_string())
            .to_string(SqliteQueryBuilder);

        db_connection
            .fetch_optional(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseRequestError::Unhandled(Error::from(sql_err)))
            .and_then(|maybe_row| match maybe_row {
                None => Err(DatabaseRequestError::NotFound),
                Some(row) => parse_block_from_row(row),
            })
    }

    async fn get_latest_deploy_aggregate(
        &self,
    ) -> Result<AggregateDeployInfo, DatabaseRequestError> {
        let db_connection = &self.connection_pool;

        let stmt =
            tables::deploy_event::create_get_latest_deploy_hash().to_string(SqliteQueryBuilder);

        let latest_deploy_hash = db_connection
            .fetch_optional(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseRequestError::Unhandled(Error::from(sql_err)))
            .and_then(|maybe_row| match maybe_row {
                None => Err(DatabaseRequestError::NotFound),
                Some(row) => row
                    .try_get::<String, &str>("deploy_hash")
                    .map_err(|sqlx_err| wrap_query_error(sqlx_err.into())),
            })?;

        self.get_deploy_aggregate_by_hash(&latest_deploy_hash).await
    }

    async fn get_deploy_aggregate_by_hash(
        &self,
        hash: &str,
    ) -> Result<AggregateDeployInfo, DatabaseRequestError> {
        check_hash_is_correct_format(hash)?;
        // We may return here with NotFound because if there's no accepted record then theoretically there should be no other records for the given hash.
        let deploy_accepted = self.get_deploy_accepted_by_hash(hash).await?;

        // However we handle the Err case for DeployProcessed explicitly as we don't want to return NotFound when we've got a DeployAccepted to return
        match self.get_deploy_processed_by_hash(hash).await {
            Ok(deploy_processed) => Ok(AggregateDeployInfo {
                deploy_hash: hash.to_string(),
                deploy_accepted: Some(deploy_accepted),
                deploy_processed: Some(deploy_processed),
                deploy_expired: false,
            }),
            Err(err) => {
                // If the error is anything other than NotFound return the error.
                if !matches!(DatabaseRequestError::NotFound, _err) {
                    return Err(err);
                }
                match self.get_deploy_expired_by_hash(hash).await {
                    Ok(deploy_has_expired) => Ok(AggregateDeployInfo {
                        deploy_hash: hash.to_string(),
                        deploy_accepted: Some(deploy_accepted),
                        deploy_processed: None,
                        deploy_expired: deploy_has_expired,
                    }),
                    Err(err) => {
                        // If the error is anything other than NotFound return the error.
                        if !matches!(DatabaseRequestError::NotFound, _err) {
                            return Err(err);
                        }
                        Ok(AggregateDeployInfo {
                            deploy_hash: hash.to_string(),
                            deploy_accepted: Some(deploy_accepted),
                            deploy_processed: None,
                            deploy_expired: false,
                        })
                    }
                }
            }
        }
    }

    async fn get_deploy_accepted_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployAccepted, DatabaseRequestError> {
        check_hash_is_correct_format(hash)?;

        let db_connection = &self.connection_pool;

        let stmt = tables::deploy_accepted::create_get_by_hash_stmt(hash.to_string())
            .to_string(SqliteQueryBuilder);

        db_connection
            .fetch_optional(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseRequestError::Unhandled(Error::from(sql_err)))
            .and_then(|maybe_row| match maybe_row {
                None => Err(DatabaseRequestError::NotFound),
                Some(row) => {
                    let raw = row
                        .try_get::<String, &str>("raw")
                        .map_err(|sqlx_error| wrap_query_error(sqlx_error.into()))?;
                    deserialize_data::<DeployAccepted>(&raw).map_err(wrap_query_error)
                }
            })
    }

    async fn get_deploy_processed_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployProcessed, DatabaseRequestError> {
        check_hash_is_correct_format(hash)?;

        let db_connection = &self.connection_pool;

        let stmt = tables::deploy_processed::create_get_by_hash_stmt(hash.to_string())
            .to_string(SqliteQueryBuilder);

        db_connection
            .fetch_optional(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseRequestError::Unhandled(Error::from(sql_err)))
            .and_then(|maybe_row| match maybe_row {
                None => Err(DatabaseRequestError::NotFound),
                Some(row) => {
                    let raw = row
                        .try_get::<String, &str>("raw")
                        .map_err(|sqlx_error| wrap_query_error(sqlx_error.into()))?;
                    deserialize_data::<DeployProcessed>(&raw).map_err(wrap_query_error)
                }
            })
    }

    // todo Note for documentation: calling the deploy_expired directly can only tell the client that the deploy has expired OR that the record wasn't found - this doesn't necessarily mean it hasn't expired.
    // todo Calling the aggregate_deploy endpoint can confirm if a deploy hasn't expired (given there is a corresponding DeployAccepted record present)
    async fn get_deploy_expired_by_hash(&self, hash: &str) -> Result<bool, DatabaseRequestError> {
        check_hash_is_correct_format(hash)?;

        let db_connection = &self.connection_pool;

        let stmt = tables::deploy_expired::create_get_by_hash_stmt(hash.to_string())
            .to_string(SqliteQueryBuilder);

        db_connection
            .fetch_optional(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseRequestError::Unhandled(Error::from(sql_err)))
            .and_then(|maybe_row| match maybe_row {
                None => Err(DatabaseRequestError::NotFound),
                Some(_) => Ok(true),
            })
    }

    async fn get_step_by_era(&self, era: u64) -> Result<Step, DatabaseRequestError> {
        let db_connection = &self.connection_pool;

        let stmt = tables::step::create_get_by_era_stmt(era).to_string(SqliteQueryBuilder);

        db_connection
            .fetch_optional(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseRequestError::Unhandled(Error::from(sql_err)))
            .and_then(|maybe_row| match maybe_row {
                None => Err(DatabaseRequestError::NotFound),
                Some(row) => {
                    let raw = row
                        .try_get::<String, &str>("raw")
                        .map_err(|sqlx_error| wrap_query_error(sqlx_error.into()))?;
                    deserialize_data::<Step>(&raw).map_err(wrap_query_error)
                }
            })
    }

    async fn get_faults_by_public_key(
        &self,
        public_key: &str,
    ) -> Result<Vec<Fault>, DatabaseRequestError> {
        check_public_key_is_correct_format(public_key)?;

        let db_connection = &self.connection_pool;

        let stmt = tables::fault::create_get_faults_by_public_key_stmt(public_key.to_string())
            .to_string(SqliteQueryBuilder);

        db_connection
            .fetch_all(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseRequestError::Unhandled(Error::from(sql_err)))
            .and_then(parse_faults_from_rows)
    }

    async fn get_faults_by_era(&self, era: u64) -> Result<Vec<Fault>, DatabaseRequestError> {
        let db_connection = &self.connection_pool;

        let stmt = tables::fault::create_get_faults_by_era_stmt(era).to_string(SqliteQueryBuilder);

        db_connection
            .fetch_all(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseRequestError::Unhandled(Error::from(sql_err)))
            .and_then(parse_faults_from_rows)
    }

    async fn get_finality_signatures_by_block(
        &self,
        block_hash: &str,
    ) -> Result<Vec<FinSig>, DatabaseRequestError> {
        check_hash_is_correct_format(block_hash)?;

        let db_connection = &self.connection_pool;

        let stmt = tables::finality_signature::create_get_finality_signatures_by_block_stmt(
            block_hash.to_string(),
        )
        .to_string(SqliteQueryBuilder);

        db_connection
            .fetch_all(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseRequestError::Unhandled(Error::from(sql_err)))
            .and_then(parse_finality_signatures_from_rows)
    }
}

fn parse_faults_from_rows(rows: Vec<SqliteRow>) -> Result<Vec<Fault>, DatabaseRequestError> {
    let mut faults = Vec::new();
    for row in rows {
        let raw = row
            .try_get::<String, &str>("raw")
            .map_err(|err| wrap_query_error(err.into()))?;

        let fault = deserialize_data::<Fault>(&raw).map_err(wrap_query_error)?;
        faults.push(fault);
    }

    if faults.is_empty() {
        return Err(DatabaseRequestError::NotFound);
    }
    Ok(faults)
}

fn parse_finality_signatures_from_rows(
    rows: Vec<SqliteRow>,
) -> Result<Vec<FinSig>, DatabaseRequestError> {
    let mut finality_signatures = Vec::new();
    for row in rows {
        let raw = row
            .try_get::<String, &str>("raw")
            .map_err(|err| wrap_query_error(err.into()))?;

        let finality_signature =
            deserialize_data::<FinalitySignature>(&raw).map_err(wrap_query_error)?;
        finality_signatures.push(finality_signature.inner());
    }

    if finality_signatures.is_empty() {
        return Err(DatabaseRequestError::NotFound);
    }
    Ok(finality_signatures)
}

fn check_hash_is_correct_format(hash: &str) -> Result<(), DatabaseRequestError> {
    let hash_regex = regex::Regex::new("^([0-9A-Fa-f]){64}$")
        .map_err(|err| DatabaseRequestError::Unhandled(Error::from(err)))?;
    if !hash_regex.is_match(hash) {
        return Err(DatabaseRequestError::InvalidParam(Error::msg(format!(
            "Expected hex-encoded hash (64 chars), received: {} (length: {})",
            hash,
            hash.len()
        ))));
    }
    Ok(())
}

fn check_public_key_is_correct_format(public_key_hex: &str) -> Result<(), DatabaseRequestError> {
    let public_key_regex = regex::Regex::new("^([0-9A-Fa-f]{2}){33,34}$")
        .map_err(|err| DatabaseRequestError::Unhandled(Error::from(err)))?;
    if !public_key_regex.is_match(public_key_hex) {
        Err(DatabaseRequestError::InvalidParam(Error::msg(format!(
            "Expected hex-encoded public key (66/68 chars), received: {} (length: {})",
            public_key_hex,
            public_key_hex.len()
        ))))
    } else {
        Ok(())
    }
}

enum SqliteDbError {
    Sqlx(sqlx::Error),
    SerdeJson(serde_json::Error),
}

impl From<sqlx::Error> for SqliteDbError {
    fn from(error: sqlx::Error) -> Self {
        SqliteDbError::Sqlx(error)
    }
}

fn wrap_query_error(error: SqliteDbError) -> DatabaseRequestError {
    match error {
        SqliteDbError::Sqlx(err) => match err.to_string().as_str() {
            "Query returned no rows" => DatabaseRequestError::NotFound,
            _ => DatabaseRequestError::Unhandled(Error::from(err)),
        },
        SqliteDbError::SerdeJson(err) => DatabaseRequestError::Serialisation(Error::from(err)),
    }
}

fn deserialize_data<'de, T: Deserialize<'de>>(data: &'de str) -> Result<T, SqliteDbError> {
    serde_json::from_str::<T>(data).map_err(SqliteDbError::SerdeJson)
}

#[cfg(test)]
mod tests {
    use casper_types::testing::TestRng;
    use casper_types::AsymmetricType;

    use crate::{
        database::{DatabaseReader, DatabaseWriter},
        sqlite_db::SqliteDb,
        types::sse_events::*,
    };

    #[tokio::test]
    async fn should_save_and_retrieve_block_added() {
        let mut test_rng = TestRng::new();

        let sqlite_db = SqliteDb::new_in_memory().await.unwrap();
        let block_added = BlockAdded::random(&mut test_rng);

        sqlite_db
            .save_block_added(block_added.clone(), 1, "127.0.0.1".to_string())
            .await
            .expect("Error saving block_added");

        sqlite_db
            .get_latest_block()
            .await
            .expect("Error getting latest block_added");

        sqlite_db
            .get_block_by_hash(&block_added.hex_encoded_hash())
            .await
            .expect("Error getting block_added by hash");

        sqlite_db
            .get_block_by_height(block_added.get_height())
            .await
            .expect("Error getting block_added by height");
    }

    #[tokio::test]
    async fn should_save_and_retrieve_deploy_accepted() {
        let mut test_rng = TestRng::new();

        let sqlite_db = SqliteDb::new_in_memory().await.unwrap();
        let deploy_accepted = DeployAccepted::random(&mut test_rng);

        sqlite_db
            .save_deploy_accepted(deploy_accepted.clone(), 1, "127.0.0.1".to_string())
            .await
            .expect("Error saving deploy_accepted");

        sqlite_db
            .get_deploy_accepted_by_hash(&deploy_accepted.hex_encoded_hash())
            .await
            .expect("Error getting deploy_accepted by hash");
    }

    #[tokio::test]
    async fn should_save_and_retrieve_deploy_processed() {
        let mut test_rng = TestRng::new();

        let sqlite_db = SqliteDb::new_in_memory().await.unwrap();
        let deploy_processed = DeployProcessed::random(&mut test_rng, None);

        sqlite_db
            .save_deploy_processed(deploy_processed.clone(), 1, "127.0.0.1".to_string())
            .await
            .expect("Error saving deploy_processed");

        sqlite_db
            .get_deploy_processed_by_hash(&deploy_processed.hex_encoded_hash())
            .await
            .expect("Error getting deploy_processed by hash");
    }

    #[tokio::test]
    async fn should_save_and_retrieve_deploy_expired() {
        let mut test_rng = TestRng::new();

        let sqlite_db = SqliteDb::new_in_memory().await.unwrap();
        let deploy_expired = DeployExpired::random(&mut test_rng, None);

        sqlite_db
            .save_deploy_expired(deploy_expired.clone(), 1, "127.0.0.1".to_string())
            .await
            .expect("Error saving deploy_expired");

        sqlite_db
            .get_deploy_expired_by_hash(&deploy_expired.hex_encoded_hash())
            .await
            .expect("Error getting deploy_expired by hash");
    }

    #[tokio::test]
    async fn should_retrieve_deploy_aggregate_of_accepted() {
        let mut test_rng = TestRng::new();

        let sqlite_db = SqliteDb::new_in_memory().await.unwrap();
        let deploy_accepted = DeployAccepted::random(&mut test_rng);

        sqlite_db
            .save_deploy_accepted(deploy_accepted.clone(), 1, "127.0.0.1".to_string())
            .await
            .expect("Error saving deploy_accepted");

        sqlite_db
            .get_latest_deploy_aggregate()
            .await
            .expect("Error getting latest deploy aggregate");

        sqlite_db
            .get_deploy_aggregate_by_hash(&deploy_accepted.hex_encoded_hash())
            .await
            .expect("Error getting deploy aggregate by hash");
    }

    #[tokio::test]
    async fn should_retrieve_deploy_aggregate_of_processed() {
        let mut test_rng = TestRng::new();

        let sqlite_db = SqliteDb::new_in_memory().await.unwrap();
        let deploy_accepted = DeployAccepted::random(&mut test_rng);
        let deploy_processed =
            DeployProcessed::random(&mut test_rng, Some(deploy_accepted.deploy_hash()));

        sqlite_db
            .save_deploy_accepted(deploy_accepted.clone(), 1, "127.0.0.1".to_string())
            .await
            .expect("Error saving deploy_accepted");

        sqlite_db
            .save_deploy_processed(deploy_processed, 2, "127.0.0.1".to_string())
            .await
            .expect("Error saving deploy_processed");

        sqlite_db
            .get_latest_deploy_aggregate()
            .await
            .expect("Error getting latest deploy aggregate");

        sqlite_db
            .get_deploy_aggregate_by_hash(&deploy_accepted.hex_encoded_hash())
            .await
            .expect("Error getting deploy aggregate by hash");
    }

    #[tokio::test]
    async fn should_retrieve_deploy_aggregate_of_expired() {
        let mut test_rng = TestRng::new();

        let sqlite_db = SqliteDb::new_in_memory().await.unwrap();
        let deploy_accepted = DeployAccepted::random(&mut test_rng);
        let deploy_expired =
            DeployExpired::random(&mut test_rng, Some(deploy_accepted.deploy_hash()));

        sqlite_db
            .save_deploy_accepted(deploy_accepted.clone(), 1, "127.0.0.1".to_string())
            .await
            .expect("Error saving deploy_accepted");

        sqlite_db
            .save_deploy_expired(deploy_expired, 2, "127.0.0.1".to_string())
            .await
            .expect("Error saving deploy_expired");

        sqlite_db
            .get_latest_deploy_aggregate()
            .await
            .expect("Error getting latest deploy aggregate");

        sqlite_db
            .get_deploy_aggregate_by_hash(&deploy_accepted.hex_encoded_hash())
            .await
            .expect("Error getting deploy aggregate by hash");
    }

    #[tokio::test]
    async fn should_save_and_retrieve_fault() {
        let mut test_rng = TestRng::new();

        let sqlite_db = SqliteDb::new_in_memory().await.unwrap();
        let fault = Fault::random(&mut test_rng);

        sqlite_db
            .save_fault(fault.clone(), 1, "127.0.0.1".to_string())
            .await
            .expect("Error saving fault");

        sqlite_db
            .get_faults_by_era(fault.era_id.value())
            .await
            .expect("Error getting faults by era");

        sqlite_db
            .get_faults_by_public_key(&fault.public_key.to_hex())
            .await
            .expect("Error getting faults by public key");
    }

    #[tokio::test]
    async fn should_save_and_retrieve_finality_signature() {
        let mut test_rng = TestRng::new();

        let sqlite_db = SqliteDb::new_in_memory().await.unwrap();
        let finality_signature = FinalitySignature::random(&mut test_rng);

        sqlite_db
            .save_finality_signature(finality_signature.clone(), 1, "127.0.0.1".to_string())
            .await
            .expect("Error saving finality_signature");

        sqlite_db
            .get_finality_signatures_by_block(&finality_signature.hex_encoded_block_hash())
            .await
            .expect("Error getting finality signatures by block_hash");
    }

    #[tokio::test]
    async fn should_save_and_retrieve_step() {
        let mut test_rng = TestRng::new();

        let sqlite_db = SqliteDb::new_in_memory().await.unwrap();
        let step = Step::random(&mut test_rng);

        sqlite_db
            .save_step(step.clone(), 1, "127.0.0.1".to_string())
            .await
            .expect("Error saving step");

        sqlite_db
            .get_step_by_era(step.era_id.value())
            .await
            .expect("Error getting step by era");
    }
}
