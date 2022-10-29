use anyhow::Error;
use async_trait::async_trait;
use sea_query::SqliteQueryBuilder;
use serde::Deserialize;
use sqlx::{sqlite::SqliteRow, Executor, Row, SqlitePool};

use casper_node::types::FinalitySignature as FinSig;

use super::{
    errors::{wrap_query_error, SqliteDbError},
    SqliteDatabase,
};
use crate::{
    sql::tables,
    types::{
        database::{DatabaseReadError, DatabaseReader, DeployAggregate},
        sse_events::*,
    },
};

#[async_trait]
impl DatabaseReader for SqliteDatabase {
    async fn get_latest_block(&self) -> Result<BlockAdded, DatabaseReadError> {
        let db_connection = &self.connection_pool;

        let stmt = tables::block_added::create_get_latest_stmt().to_string(SqliteQueryBuilder);

        let row = fetch_optional_with_error_check(db_connection, stmt).await?;

        parse_block_from_row(row)
    }

    async fn get_block_by_height(&self, height: u64) -> Result<BlockAdded, DatabaseReadError> {
        let db_connection = &self.connection_pool;

        let stmt =
            tables::block_added::create_get_by_height_stmt(height).to_string(SqliteQueryBuilder);

        let row = fetch_optional_with_error_check(db_connection, stmt).await?;

        parse_block_from_row(row)
    }

    async fn get_block_by_hash(&self, hash: &str) -> Result<BlockAdded, DatabaseReadError> {
        let db_connection = &self.connection_pool;

        let stmt = tables::block_added::create_get_by_hash_stmt(hash.to_string())
            .to_string(SqliteQueryBuilder);

        db_connection
            .fetch_optional(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
            .and_then(|maybe_row| match maybe_row {
                None => Err(DatabaseReadError::NotFound),
                Some(row) => parse_block_from_row(row),
            })
    }

    async fn get_latest_deploy_aggregate(&self) -> Result<DeployAggregate, DatabaseReadError> {
        let db_connection = &self.connection_pool;

        let stmt =
            tables::deploy_event::create_get_latest_deploy_hash().to_string(SqliteQueryBuilder);

        let latest_deploy_hash = db_connection
            .fetch_optional(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
            .and_then(|maybe_row| match maybe_row {
                None => Err(DatabaseReadError::NotFound),
                Some(row) => row
                    .try_get::<String, &str>("deploy_hash")
                    .map_err(|sqlx_err| wrap_query_error(sqlx_err.into())),
            })?;

        self.get_deploy_aggregate_by_hash(&latest_deploy_hash).await
    }

    async fn get_deploy_aggregate_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployAggregate, DatabaseReadError> {
        // We may return here with NotFound because if there's no accepted record then theoretically there should be no other records for the given hash.
        let deploy_accepted = self.get_deploy_accepted_by_hash(hash).await?;

        // However we handle the Err case for DeployProcessed explicitly as we don't want to return NotFound when we've got a DeployAccepted to return
        match self.get_deploy_processed_by_hash(hash).await {
            Ok(deploy_processed) => Ok(DeployAggregate {
                deploy_hash: hash.to_string(),
                deploy_accepted: Some(deploy_accepted),
                deploy_processed: Some(deploy_processed),
                deploy_expired: false,
            }),
            Err(err) => {
                // If the error is anything other than NotFound return the error.
                if !matches!(DatabaseReadError::NotFound, _err) {
                    return Err(err);
                }
                match self.get_deploy_expired_by_hash(hash).await {
                    Ok(deploy_has_expired) => Ok(DeployAggregate {
                        deploy_hash: hash.to_string(),
                        deploy_accepted: Some(deploy_accepted),
                        deploy_processed: None,
                        deploy_expired: deploy_has_expired,
                    }),
                    Err(err) => {
                        // If the error is anything other than NotFound return the error.
                        if !matches!(DatabaseReadError::NotFound, _err) {
                            return Err(err);
                        }
                        Ok(DeployAggregate {
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
    ) -> Result<DeployAccepted, DatabaseReadError> {
        let db_connection = &self.connection_pool;

        let stmt = tables::deploy_accepted::create_get_by_hash_stmt(hash.to_string())
            .to_string(SqliteQueryBuilder);

        db_connection
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

    async fn get_deploy_processed_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployProcessed, DatabaseReadError> {
        let db_connection = &self.connection_pool;

        let stmt = tables::deploy_processed::create_get_by_hash_stmt(hash.to_string())
            .to_string(SqliteQueryBuilder);

        db_connection
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

    async fn get_deploy_expired_by_hash(&self, hash: &str) -> Result<bool, DatabaseReadError> {
        let db_connection = &self.connection_pool;

        let stmt = tables::deploy_expired::create_get_by_hash_stmt(hash.to_string())
            .to_string(SqliteQueryBuilder);

        db_connection
            .fetch_optional(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
            .and_then(|maybe_row| match maybe_row {
                None => Err(DatabaseReadError::NotFound),
                Some(_) => Ok(true),
            })
    }

    async fn get_step_by_era(&self, era: u64) -> Result<Step, DatabaseReadError> {
        let db_connection = &self.connection_pool;

        let stmt = tables::step::create_get_by_era_stmt(era).to_string(SqliteQueryBuilder);

        db_connection
            .fetch_optional(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
            .and_then(|maybe_row| match maybe_row {
                None => Err(DatabaseReadError::NotFound),
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
    ) -> Result<Vec<Fault>, DatabaseReadError> {
        let db_connection = &self.connection_pool;

        let stmt = tables::fault::create_get_faults_by_public_key_stmt(public_key.to_string())
            .to_string(SqliteQueryBuilder);

        db_connection
            .fetch_all(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
            .and_then(parse_faults_from_rows)
    }

    async fn get_faults_by_era(&self, era: u64) -> Result<Vec<Fault>, DatabaseReadError> {
        let db_connection = &self.connection_pool;

        let stmt = tables::fault::create_get_faults_by_era_stmt(era).to_string(SqliteQueryBuilder);

        db_connection
            .fetch_all(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
            .and_then(parse_faults_from_rows)
    }

    async fn get_finality_signatures_by_block(
        &self,
        block_hash: &str,
    ) -> Result<Vec<FinSig>, DatabaseReadError> {
        let db_connection = &self.connection_pool;

        let stmt = tables::finality_signature::create_get_finality_signatures_by_block_stmt(
            block_hash.to_string(),
        )
        .to_string(SqliteQueryBuilder);

        db_connection
            .fetch_all(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
            .and_then(parse_finality_signatures_from_rows)
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

async fn fetch_optional_with_error_check(
    connection: &SqlitePool,
    stmt: String,
) -> Result<SqliteRow, DatabaseReadError> {
    connection
        .fetch_optional(stmt.as_str())
        .await
        .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
        .and_then(|maybe_row| match maybe_row {
            None => Err(DatabaseReadError::NotFound),
            Some(row) => Ok(row),
        })
}

fn parse_faults_from_rows(rows: Vec<SqliteRow>) -> Result<Vec<Fault>, DatabaseReadError> {
    let mut faults = Vec::new();
    for row in rows {
        let raw = row
            .try_get::<String, &str>("raw")
            .map_err(|err| wrap_query_error(err.into()))?;

        let fault = deserialize_data::<Fault>(&raw).map_err(wrap_query_error)?;
        faults.push(fault);
    }

    if faults.is_empty() {
        return Err(DatabaseReadError::NotFound);
    }
    Ok(faults)
}

fn parse_finality_signatures_from_rows(
    rows: Vec<SqliteRow>,
) -> Result<Vec<FinSig>, DatabaseReadError> {
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
        return Err(DatabaseReadError::NotFound);
    }
    Ok(finality_signatures)
}
