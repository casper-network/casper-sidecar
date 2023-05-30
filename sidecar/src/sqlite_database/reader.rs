use anyhow::{Context, Error};
use async_trait::async_trait;
use casper_types::Timestamp;
use sea_query::SqliteQueryBuilder;
use serde::Deserialize;
use sqlx::{sqlite::SqliteRow, Executor, Row, SqlitePool};
use std::convert::TryFrom;

use casper_event_types::FinalitySignature as FinSig;

use super::{
    errors::{wrap_query_error, SqliteDbError},
    SqliteDatabase,
};
use crate::{
    sql::tables,
    types::{
        database::{
            DatabaseReadError, DatabaseReader, DeployAggregate, DeployAggregateFilter,
            DeployAggregateJoin,
        },
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

    async fn get_deploy_aggregate_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployAggregate, DatabaseReadError> {
        // We may return here with NotFound because if there's no accepted record then theoretically there should be no other records for the given hash.
        let deploy_accepted = self.get_deploy_accepted_by_hash(hash).await?;
        let maybe_block = self.get_block_by_deploy_hash(hash).await?;
        let maybe_block_timestamp = maybe_block.map(|block| block.block.header.timestamp);

        // However we handle the Err case for DeployProcessed explicitly as we don't want to return NotFound when we've got a DeployAccepted to return
        match self.get_deploy_processed_by_hash(hash).await {
            Ok(deploy_processed) => Ok(DeployAggregate {
                deploy_hash: hash.to_string(),
                deploy_accepted: Some(deploy_accepted),
                deploy_processed: Some(deploy_processed),
                deploy_expired: false,
                block_timestamp: maybe_block_timestamp,
            }),
            Err(err) => {
                // If the error is anything other than NotFound return the error.
                if !matches!(DatabaseReadError::NotFound, _err) {
                    return Err(err);
                }
                match self.get_deploy_expired_by_hash(hash).await {
                    Ok(_) => Ok(DeployAggregate {
                        deploy_hash: hash.to_string(),
                        deploy_accepted: Some(deploy_accepted),
                        deploy_processed: None,
                        deploy_expired: true,
                        block_timestamp: maybe_block_timestamp,
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
                            block_timestamp: maybe_block_timestamp,
                        })
                    }
                }
            }
        }
    }

    async fn list_deploy_aggregate(
        &self,
        filter: DeployAggregateFilter,
    ) -> Result<(Vec<DeployAggregate>, u32), DatabaseReadError> {
        let db_connection = &self.connection_pool;
        let item_count = count_aggregates(filter.clone(), db_connection).await?;

        let stmt = tables::deploy_aggregate::create_list_by_filter_query(filter)
            .to_string(SqliteQueryBuilder);
        let data = sqlx::query_as::<_, DeployAggregateJoin>(&stmt)
            .fetch_all(db_connection)
            .await
            .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
            .and_then(|joins| {
                let vector_of_deploy_aggregates = joins
                    .into_iter()
                    .map(to_deploy_aggregate)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(vector_of_deploy_aggregates)
            })?;
        Ok((data, item_count))
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

    async fn get_block_by_deploy_hash(
        &self,
        deploy_hash: &str,
    ) -> Result<Option<BlockAdded>, DatabaseReadError> {
        let db_connection = &self.connection_pool;
        let get_block_deploy_stmt =
            tables::block_deploys::create_get_by_deploy_hash_stmt(deploy_hash.to_string())
                .to_string(SqliteQueryBuilder);
        let block_hash_row = db_connection
            .fetch_optional(get_block_deploy_stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))?;
        match block_hash_row.map(|row| {
            row.try_get::<String, &str>("block_hash")
                .map_err(|sqlx_error| wrap_query_error(sqlx_error.into()))
        }) {
            None => Ok(None),
            Some(res) => {
                let block_hash = res?;
                self.get_block_by_hash(block_hash.as_str()).await.map(Some)
            }
        }
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

    async fn get_deploy_expired_by_hash(
        &self,
        hash: &str,
    ) -> Result<DeployExpired, DatabaseReadError> {
        let db_connection = &self.connection_pool;

        let stmt = tables::deploy_expired::create_get_by_hash_stmt(hash.to_string())
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
                    deserialize_data::<DeployExpired>(&raw).map_err(wrap_query_error)
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

    async fn get_number_of_events(&self) -> Result<u64, DatabaseReadError> {
        let db_connection = &self.connection_pool;

        let stmt = tables::event_log::count().to_string(SqliteQueryBuilder);

        db_connection
            .fetch_one(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
            .and_then(|row| {
                row.try_get::<i64, _>(0)
                    .map(|i| i as u64) //this should never be negative
                    .map_err(|sqlx_error| wrap_query_error(sqlx_error.into()))
            })
    }

    async fn get_newest_migration_version(&self) -> Result<Option<(u32, bool)>, DatabaseReadError> {
        let db_connection = &self.connection_pool;

        let stmt =
            tables::migration::create_get_newest_migration_stmt().to_string(SqliteQueryBuilder);

        db_connection
            .fetch_optional(stmt.as_str())
            .await
            .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
            .and_then(parse_migration_row)
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

fn parse_migration_row(
    maybe_row: Option<SqliteRow>,
) -> Result<Option<(u32, bool)>, DatabaseReadError> {
    match maybe_row {
        None => Ok(None),
        Some(row) => {
            let version = row
                .try_get::<u32, &str>("version")
                .map_err(|err| wrap_query_error(err.into()))?;
            let is_success = row
                .try_get::<bool, &str>("is_success")
                .map_err(|err| wrap_query_error(err.into()))?;
            Ok(Some((version, is_success)))
        }
    }
}

async fn count_aggregates(
    filter: DeployAggregateFilter,
    connection: &SqlitePool,
) -> Result<u32, DatabaseReadError> {
    let stmt = tables::deploy_aggregate::create_count_aggregate_deploys_query(filter)
        .to_string(SqliteQueryBuilder);
    connection
        .fetch_one(stmt.as_str())
        .await
        .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
        .and_then(|row| {
            let count: u32 = row
                .try_get("count")
                .map_err(|sqlx_error| wrap_query_error(sqlx_error.into()))?;
            Ok(count)
        })
}

fn to_deploy_aggregate(join: DeployAggregateJoin) -> Result<DeployAggregate, DatabaseReadError> {
    let deploy_accepted =
        deserialize_data::<DeployAccepted>(&join.deploy_accepted_raw).map_err(wrap_query_error)?;
    let deploy_processed = match &join.deploy_processed_raw {
        Some(raw) => {
            let data = deserialize_data::<DeployProcessed>(raw).map_err(wrap_query_error)?;
            Ok(Some(data))
        }
        None => Ok(None),
    }?;
    let is_expired = join.is_expired;
    let maybe_timestamp = match join.block_timestamp {
        None => None,
        Some(utc_millis) => {
            let u64_timestamp = u64::try_from(utc_millis)
                .context(format!(
                    "Error while trying to map {} to Timestamp",
                    utc_millis
                ))
                .map_err(DatabaseReadError::Unhandled)?;
            Some(Timestamp::from(u64_timestamp))
        }
    };
    Ok(DeployAggregate {
        deploy_hash: join.deploy_hash,
        deploy_accepted: Some(deploy_accepted),
        deploy_processed,
        deploy_expired: is_expired,
        block_timestamp: maybe_timestamp,
    })
}
