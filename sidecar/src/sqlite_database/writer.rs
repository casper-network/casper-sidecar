use anyhow::Context;
use async_trait::async_trait;
use itertools::Itertools;
use miniz_oxide::deflate::compress_to_vec;
use sea_query::SqliteQueryBuilder;
#[cfg(test)]
use sqlx::sqlite::SqliteRow;
use sqlx::{sqlite::SqliteQueryResult, Executor, Row};

use casper_types::AsymmetricType;

use super::SqliteDatabase;
use crate::{
    sql::{tables, tables::event_type::EventTypeId},
    types::{
        database::{DatabaseWriteError, DatabaseWriter},
        sse_events::*,
    },
};

// This can be set from 0-10 inclusive.
const COMPRESSION_LEVEL: u8 = 10;

#[async_trait]
impl DatabaseWriter for SqliteDatabase {
    async fn save_block_added(
        &self,
        block_added: BlockAdded,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
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
            .await?
            .try_get::<u32, usize>(0)
            .context("Error parsing event_log_id from row")?;

        let insert_stmt = tables::block_added::create_insert_stmt(
            block_added.get_height(),
            encoded_hash,
            json,
            event_log_id,
        )?
        .to_string(SqliteQueryBuilder);

        handle_sqlite_result(db_connection.execute(insert_stmt.as_str()).await)
    }

    async fn save_deploy_accepted(
        &self,
        deploy_accepted: DeployAccepted,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
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
            .try_get::<u32, usize>(0)
            .context("Error parsing event_log_id from row")?;

        let batched_insert_stmts = vec![
            tables::deploy_accepted::create_insert_stmt(encoded_hash.clone(), json, event_log_id)?,
            tables::deploy_event::create_insert_stmt(event_log_id, encoded_hash)?,
        ]
        .iter()
        .map(|stmt| stmt.to_string(SqliteQueryBuilder))
        .join(";");

        handle_sqlite_result(db_connection.execute(batched_insert_stmts.as_str()).await)
    }

    async fn save_deploy_processed(
        &self,
        deploy_processed: DeployProcessed,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
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
            .try_get::<u32, usize>(0)
            .context("Error parsing event_log_id from row")?;

        let batched_insert_stmts = vec![
            tables::deploy_processed::create_insert_stmt(encoded_hash.clone(), json, event_log_id)?,
            tables::deploy_event::create_insert_stmt(event_log_id, encoded_hash)?,
        ]
        .iter()
        .map(|stmt| stmt.to_string(SqliteQueryBuilder))
        .join(";");

        handle_sqlite_result(db_connection.execute(batched_insert_stmts.as_str()).await)
    }

    async fn save_deploy_expired(
        &self,
        deploy_expired: DeployExpired,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
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
            .try_get::<u32, usize>(0)
            .context("Error parsing event_log_id from row")?;

        let batched_insert_stmts = vec![
            tables::deploy_expired::create_insert_stmt(encoded_hash.clone(), event_log_id)?,
            tables::deploy_event::create_insert_stmt(event_log_id, encoded_hash)?,
        ]
        .iter()
        .map(|stmt| stmt.to_string(SqliteQueryBuilder))
        .join(";");

        handle_sqlite_result(db_connection.execute(batched_insert_stmts.as_str()).await)
    }

    async fn save_fault(
        &self,
        fault: Fault,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
        let db_connection = &self.connection_pool;

        let json = serde_json::to_string(&fault)?;
        let era_id = fault.era_id.value();
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
            .try_get::<u32, usize>(0)
            .context("Error parsing event_log_id from row")?;

        let insert_stmt =
            tables::fault::create_insert_stmt(era_id, public_key, json, event_log_id)?
                .to_string(SqliteQueryBuilder);

        handle_sqlite_result(db_connection.execute(insert_stmt.as_str()).await)
    }

    async fn save_finality_signature(
        &self,
        finality_signature: FinalitySignature,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
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
            .try_get::<u32, usize>(0)
            .context("Error parsing event_log_id from row")?;

        let insert_stmt = tables::finality_signature::create_insert_stmt(
            block_hash,
            public_key,
            json,
            event_log_id,
        )?
        .to_string(SqliteQueryBuilder);

        handle_sqlite_result(db_connection.execute(insert_stmt.as_str()).await)
    }

    async fn save_step(
        &self,
        step: Step,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
        let db_connection = &self.connection_pool;

        let insert_to_event_log_stmt = tables::event_log::create_insert_stmt(
            EventTypeId::Step as u8,
            &event_source_address,
            event_id,
        )?
        .to_string(SqliteQueryBuilder);

        let event_log_id = db_connection
            .fetch_one(insert_to_event_log_stmt.as_str())
            .await?
            .try_get::<u32, usize>(0)
            .context("Error parsing event_log_id from row")?;

        let insert_stmt = if self.use_compression {
            let bytes = serde_json::to_vec(&step)?;
            let compressed = compress_to_vec(&bytes, COMPRESSION_LEVEL);
            let era_id = step.era_id.value();

            tables::step::create_insert_stmt_compressed(era_id, compressed, event_log_id)?
                .to_string(SqliteQueryBuilder)
        } else {
            let json = serde_json::to_string(&step)?;
            let era_id = step.era_id.value();

            tables::step::create_insert_stmt(era_id, json, event_log_id)?
                .to_string(SqliteQueryBuilder)
        };

        handle_sqlite_result(db_connection.execute(insert_stmt.as_str()).await)
    }
}

#[cfg(test)]
impl SqliteDatabase {
    pub(super) async fn fetch_one(&self, sql: &str) -> SqliteRow {
        self.connection_pool
            .fetch_one(sql)
            .await
            .expect("Error executing provided SQL")
    }
}

fn handle_sqlite_result(
    result: Result<SqliteQueryResult, sqlx::Error>,
) -> Result<usize, DatabaseWriteError> {
    result
        .map(|ok_query_result| ok_query_result.rows_affected() as usize)
        .map_err(std::convert::From::from)
}

#[cfg(test)]
mod compression_checks {
    use casper_types::testing::TestRng;

    use miniz_oxide::{deflate::compress_to_vec, inflate::decompress_to_vec};
    use serde_json::Value;

    use crate::Step;

    #[test]
    #[ignore]
    fn check_compression_roundtrip() {
        let mut test_rng = TestRng::new();

        for _ in 0..300 {
            let step = Step::random(&mut test_rng);
            let step_bytes = serde_json::to_vec(&step).unwrap();

            let compressed = compress_to_vec(&step_bytes, 10);

            let decompressed = decompress_to_vec(&compressed).unwrap();
            let decompressed_json = serde_json::from_slice::<Value>(&decompressed).unwrap();
            let decompressed_step = serde_json::from_value::<Step>(decompressed_json).unwrap();

            assert_eq!(step, decompressed_step);
        }
    }
}
