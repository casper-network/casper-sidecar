use crate::database::{AggregateDeployInfo, DatabaseReader, DatabaseRequestError, DatabaseWriter};
use crate::sql::tables;
use crate::sql::tables::event_type::EventTypeId;
use crate::types::sse_events::{
    BlockAdded, DeployAccepted, DeployExpired, DeployProcessed, Fault, FinalitySignature, Step,
};
use crate::SseData;
use anyhow::{Context, Error};
use async_trait::async_trait;
use casper_node::types::Deploy;
use casper_types::AsymmetricType;
use futures_util::{StreamExt, TryStreamExt};
use itertools::Itertools;
use sea_query::SqliteQueryBuilder;
use serde::Deserialize;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteError, SqliteJournalMode, SqliteQueryResult, SqliteRow,
};
use sqlx::{
    pool::PoolConnection,
    sqlite::{SqlitePool, SqlitePoolOptions},
    ConnectOptions, Executor, Row, Sqlite,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

const MAX_WRITE_CONNECTIONS: u32 = 10;
const MAX_READ_CONNECTIONS: u32 = 100;

#[derive(Clone)]
pub struct SqliteDb {
    pub connection_pool: SqlitePool,
    pub file_path: PathBuf,
}

impl SqliteDb {
    pub async fn new(
        storage_dir: &Path,
        storage_file_name: String,
        node_ip_address: String,
    ) -> Result<SqliteDb, Error> {
        fs::create_dir_all(storage_dir)?;

        let relative_path = storage_dir
            .join(storage_file_name)
            .to_str()
            .unwrap()
            .to_string();

        let mut initial_connection = SqliteConnectOptions::from_str(&relative_path)?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .disable_statement_logging()
            .connect()
            .await?;

        let create_table_stmts = vec![
            // Synthetic tables
            tables::event_type::create_table_stmt(),
            tables::event_log::create_table_stmt(),
            tables::event_source::create_table_stmt(),
            tables::event_source_of_event::create_table_stmt(),
            tables::deploy_event::create_table_stmt(),
            tables::deploy_event_type::create_table_stmt(),
            // Raw Event tables
            tables::block_added::create_table_stmt(),
            tables::deploy_accepted::create_table_stmt(),
            tables::deploy_processed::create_table_stmt(),
            tables::deploy_expired::create_table_stmt(),
            tables::fault::create_table_stmt(),
            tables::step::create_table_stmt(),
        ]
        .iter()
        .map(|stmt| stmt.to_string(SqliteQueryBuilder))
        .join(";");

        initial_connection
            .execute(create_table_stmts.as_str())
            .await?;

        // todo this is dumb it should be on a per event basis not on the initialisation of the struct/db
        // Check if node_ip_address is already in the DB to avoid duplicating the IP under different event_source_id's
        let check_for_node_ip_stmt =
            tables::event_source::create_select_id_by_address_stmt(node_ip_address.clone())
                .to_string(SqliteQueryBuilder);
        let node_ip_not_stored = initial_connection
            .fetch_optional(check_for_node_ip_stmt.as_str())
            .await?
            .is_none();

        if node_ip_not_stored {
            let add_node_ip_sql = tables::event_source::create_insert_stmt(node_ip_address)?
                .to_string(SqliteQueryBuilder);
            initial_connection.execute(add_node_ip_sql.as_str()).await?;
        }

        // Initialisation statements
        let initialise_sql_event_type = tables::event_type::create_initialise_stmt()?;
        let initialise_sql_deploy_event_type = tables::deploy_event_type::create_initialise_stmt()?;

        let batched_initialise_tables =
            vec![initialise_sql_event_type, initialise_sql_deploy_event_type]
                .iter()
                .map(|stmt| stmt.to_string(SqliteQueryBuilder))
                .join(";");

        // The result is swallowed because this call may fail if the tables were already created and populated.
        let _ = initial_connection
            .execute(batched_initialise_tables.as_str())
            .await;

        let connection_pool = SqlitePoolOptions::new()
            .max_connections(MAX_WRITE_CONNECTIONS)
            .connect_lazy_with(
                SqliteConnectOptions::from_str(&relative_path)?
                    .journal_mode(SqliteJournalMode::Wal)
                    .disable_statement_logging()
                    .to_owned(),
            );

        Ok(SqliteDb {
            connection_pool,
            file_path: Path::new(&relative_path).into(),
        })
    }

    pub fn new_read_only(path: &Path) -> Result<SqliteDb, Error> {
        let connection_pool = SqlitePoolOptions::new()
            .max_connections(MAX_READ_CONNECTIONS)
            .connect_lazy_with(
                SqliteConnectOptions::from_str(path.to_str().unwrap())?
                    .read_only(true)
                    .journal_mode(SqliteJournalMode::Wal), // .disable_statement_logging()
                                                           // .to_owned(),
            );

        Ok(SqliteDb {
            connection_pool,
            file_path: path.into(),
        })
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

        let insert_to_block_added_stmt = tables::block_added::create_insert_stmt(
            block_added.get_height(),
            encoded_hash,
            json,
            event_log_id as u64,
        )?
        .to_string(SqliteQueryBuilder);

        handle_sqlite_result(
            db_connection
                .execute(insert_to_block_added_stmt.as_str())
                .await,
        )
    }

    async fn save_deploy_accepted(
        &self,
        deploy_accepted: DeployAccepted,
        event_id: u64,
        event_source_address: String,
    ) -> Result<(), Error> {
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

        while let Some(result) = db_connection
            .execute_many(batched_insert_stmts.as_str())
            .next()
            .await
        {
            if let Err(sqlite_err) = result {
                return Err(Error::from(sqlite_err));
            }
        }

        Ok(())
    }

    async fn save_deploy_processed(
        &self,
        deploy_processed: DeployProcessed,
        event_id: u64,
        event_source_address: String,
    ) -> Result<(), Error> {
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

        while let Some(result) = db_connection
            .execute_many(batched_insert_stmts.as_str())
            .next()
            .await
        {
            if let Err(sqlite_err) = result {
                return Err(Error::from(sqlite_err));
            }
        }

        Ok(())
    }

    async fn save_deploy_expired(
        &self,
        deploy_expired: DeployExpired,
        event_id: u64,
        event_source_address: String,
    ) -> Result<(), Error> {
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

        while let Some(result) = db_connection
            .execute_many(batched_insert_stmts.as_str())
            .next()
            .await
        {
            if let Err(sqlite_err) = result {
                return Err(Error::from(sqlite_err));
            }
        }

        Ok(())
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

        let insert_to_step_stmt =
            tables::step::create_insert_stmt(era_id, json, event_log_id as u64)?
                .to_string(SqliteQueryBuilder);

        handle_sqlite_result(db_connection.execute(insert_to_step_stmt.as_str()).await)
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

        let insert_to_step_stmt =
            tables::fault::create_insert_stmt(era_id, public_key, json, event_log_id as u64)?
                .to_string(SqliteQueryBuilder);

        handle_sqlite_result(db_connection.execute(insert_to_step_stmt.as_str()).await)
    }

    async fn save_finality_signature(
        &self,
        finality_signature: FinalitySignature,
    ) -> Result<usize, Error> {
        Err(Error::msg("Not implemented yet"))
    }
}

fn parse_block_from_row(row: SqliteRow) -> Result<BlockAdded, DatabaseRequestError> {
    let raw_data = row
        .try_get::<String, &str>("raw")
        .map_err(|sqlx_err| wrap_query_error(sqlx_err.into()))?;
    deserialize_data::<BlockAdded>(&raw_data).map_err(wrap_query_error)
}

#[async_trait]
impl DatabaseReader for SqliteDb {
    async fn get_latest_block(&self) -> Result<BlockAdded, DatabaseRequestError> {
        let db_connection = &self.connection_pool;

        let stmt = tables::block_added::create_get_latest_stmt().to_string(SqliteQueryBuilder);

        db_connection
            .fetch_optional(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseRequestError::Unhandled(Error::from(sql_err)))
            .and_then(|maybe_row| match maybe_row {
                None => Err(DatabaseRequestError::NotFound),
                Some(row) => parse_block_from_row(row),
            })
    }

    async fn get_block_by_height(&self, height: u64) -> Result<BlockAdded, DatabaseRequestError> {
        let db_connection = &self.connection_pool;

        let stmt =
            tables::block_added::create_get_by_height_stmt(height).to_string(SqliteQueryBuilder);

        db_connection
            .fetch_optional(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseRequestError::Unhandled(Error::from(sql_err)))
            .and_then(|maybe_row| match maybe_row {
                None => Err(DatabaseRequestError::NotFound),
                Some(row) => parse_block_from_row(row),
            })
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
                None => return Err(DatabaseRequestError::NotFound),
                Some(row) => row
                    .try_get::<String, &str>("deploy_hash")
                    .map_err(|sqlx_err| wrap_query_error(sqlx_err.into())),
            })?;

        self.get_deploy_by_hash_aggregate(&latest_deploy_hash).await
    }

    async fn get_deploy_by_hash_aggregate(
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
            Err(_) => {
                // Other errors could be handled here but for now I think it's better to just lump them together and mark the DeployProcessed as if it is NotFound.
                match self.get_deploy_expired_by_hash(hash).await {
                    Ok(deploy_has_expired) => Ok(AggregateDeployInfo {
                        deploy_hash: hash.to_string(),
                        deploy_accepted: Some(deploy_accepted),
                        deploy_processed: None,
                        deploy_expired: deploy_has_expired,
                    }),
                    Err(_) => {
                        // Other errors could be handled here but for now I think it's better to just lump them together and mark the DeployExpired as if it is NotFound.
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

        let stmt = tables::deploy_processed::create_get_by_hash_stmt(hash.to_string())
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
    Internal(Error),
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
        SqliteDbError::Internal(err) => DatabaseRequestError::Unhandled(err),
    }
}

fn deserialize_data<'de, T: Deserialize<'de>>(data: &'de str) -> Result<T, SqliteDbError> {
    serde_json::from_str::<T>(data).map_err(SqliteDbError::SerdeJson)
}

// fn extract_aggregate_deploy_info(deploy_row: &Row) -> Result<AggregateDeployInfo, SqliteDbError> {
//     let mut aggregate_deploy: AggregateDeployInfo = AggregateDeployInfo {
//         deploy_hash: "".to_string(),
//         deploy_accepted: None,
//         deploy_processed: None,
//         deploy_expired: false,
//     };
//
//     aggregate_deploy.deploy_hash = deploy_row.get(0)?;
//     aggregate_deploy.deploy_accepted = deploy_row.get::<usize, Option<String>>(2)?;
//     aggregate_deploy.deploy_processed = deploy_row.get::<usize, Option<String>>(3)?;
//     aggregate_deploy.deploy_expired = match deploy_row.get::<usize, u8>(4) {
//         Ok(int) => integer_to_bool(int).map_err(SqliteDbError::Internal)?,
//         Err(err) => return Err(SqliteDbError::Rusqlite(err)),
//     };
//
//     Ok(aggregate_deploy)
// }

fn integer_to_bool(integer: u8) -> Result<bool, Error> {
    match integer {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(Error::msg("Invalid bool number in DB")),
    }
}
