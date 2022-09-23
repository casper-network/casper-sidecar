use casper_types::AsymmetricType;

use anyhow::{Context, Error};
use async_trait::async_trait;
use itertools::Itertools;
use sea_query::SqliteQueryBuilder;
use sqlx::{sqlite::SqliteQueryResult, Executor, Row};

use super::SqliteDatabase;
use crate::{
    sql::{tables, tables::event_type::EventTypeId},
    types::{database::DatabaseWriter, sse_events::*},
};

#[async_trait]
impl DatabaseWriter for SqliteDatabase {
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

        let event_log_id = db_connection
            .fetch_one(insert_to_event_log_stmt.as_str())
            .await
            .context("Error inserting row to event_log")?
            .try_get::<i64, usize>(0)
            .context("Error parsing event_log_id from row")?;

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
            .await
            .context("Error inserting row to event_log")?
            .try_get::<i64, usize>(0)
            .context("Error parsing event_log_id from row")?;

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
            .await
            .context("Error inserting row to event_log")?
            .try_get::<i64, usize>(0)
            .context("Error parsing event_log_id from row")?;

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
            .await
            .context("Error inserting row to event_log")?
            .try_get::<i64, usize>(0)
            .context("Error parsing event_log_id from row")?;

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
            .await
            .context("Error inserting row to event_log")?
            .try_get::<i64, usize>(0)
            .context("Error parsing event_log_id from row")?;

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
            .await
            .context("Error inserting row to event_log")?
            .try_get::<i64, usize>(0)
            .context("Error parsing event_log_id from row")?;

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
            .await
            .context("Error inserting row to event_log")?
            .try_get::<i64, usize>(0)
            .context("Error parsing event_log_id from row")?;

        let insert_stmt = tables::step::create_insert_stmt(era_id, json, event_log_id as u64)?
            .to_string(SqliteQueryBuilder);

        handle_sqlite_result(db_connection.execute(insert_stmt.as_str()).await)
    }
}

fn handle_sqlite_result(result: Result<SqliteQueryResult, sqlx::Error>) -> Result<usize, Error> {
    result
        .map_err(Error::from)
        .map(|sqlite_res| sqlite_res.rows_affected() as usize)
}
