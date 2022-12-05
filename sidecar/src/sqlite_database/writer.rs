use anyhow::Context;
use async_trait::async_trait;
use itertools::Itertools;
use miniz_oxide::deflate::compress_to_vec;
use sea_query::SqliteQueryBuilder;
#[cfg(test)]
use sqlx::sqlite::SqliteRow;
use sqlx::{sqlite::SqliteQueryResult, Executor, Row};

use casper_types::AsymmetricType;
use tokio::time::Instant;

use super::SqliteDatabase;
use crate::{
    sql::{tables, tables::event_type::EventTypeId},
    types::{
        database::{DatabaseWriteError, DatabaseWriter},
        sse_events::*,
    },
};

// This can be set from 0-10 inclusive.
const COMPRESSION_LEVEL: u8 = 1;

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
            EventTypeId::FinalitySignature as u8,
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

        let mut start = Instant::now();
        let insert_stmt = if self.use_compression {
            let bytes = bincode::serialize(&step).unwrap();
            // println!("Serialised: {:.3}s", (Instant::now() - start).as_secs_f32());
            start = Instant::now();
            let compressed = compress_to_vec(&bytes, COMPRESSION_LEVEL);
            // println!("Compressed: {:.3}s", (Instant::now() - start).as_secs_f32());
            let era_id = step.era_id.value();
            start = Instant::now();
            tables::step::create_insert_stmt_compressed(era_id, compressed, event_log_id)?
                .to_string(SqliteQueryBuilder)
        } else {
            let json = serde_json::to_string(&step)?;
            let era_id = step.era_id.value();

            tables::step::create_insert_stmt(era_id, json, event_log_id)?
                .to_string(SqliteQueryBuilder)
        };

        // println!(
        //     "Created step statement: {:.3}s",
        //     (Instant::now() - start).as_secs_f32()
        // );

        let start_of_write = Instant::now();
        let handled_result =
            handle_sqlite_result(db_connection.execute(insert_stmt.as_str()).await);

        // println!(
        //     "Wrote step to db: {:.3}",
        //     (Instant::now() - start_of_write).as_secs_f32()
        // );

        handled_result
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
    use std::fs;

    use casper_event_types::SseData;
    use casper_types::testing::TestRng;

    use crate::sqlite_database::SqliteDatabase;
    use datasize::data_size;
    use miniz_oxide::{deflate::compress_to_vec, inflate::decompress_to_vec};
    use serde_json::Value;
    use tokio::time::Instant;

    use crate::types::database::DatabaseWriter;
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

    #[test]
    #[ignore]
    fn benchmark_compression() {
        let file_string =
            fs::read_to_string("/home/george/casper/casperlabs/step_events/mainnet_step_7006.json")
                .unwrap();
        let sse_data = serde_json::from_str::<SseData>(&file_string).unwrap();

        if let SseData::Step {
            era_id,
            execution_effect,
        } = sse_data
        {
            let step = Step {
                era_id,
                execution_effect,
            };

            println!("_______________ serde_json ____________");

            let start_of_to_vec = Instant::now();
            let bytes = serde_json::to_vec(&step).unwrap();
            let end_of_to_vec = Instant::now();
            println!(
                "\nTime to convert to bytes: {:.2}s\n",
                (end_of_to_vec - start_of_to_vec).as_secs_f32()
            );

            let uncompressed_step = data_size(&bytes);
            println!("Original Size: {}MB\n", uncompressed_step / 1024 / 1024);

            for i in 1..=10 {
                let start = Instant::now();
                let compressed = compress_to_vec(&bytes, i);
                let time_to_compress = Instant::now() - start;
                let compressed_step = data_size(&compressed);
                println!(
                    "Compressed Step in {:.2}s at level {}\t:: Compressed Size: {}MB\t:: Reduction => {}MB",
                    time_to_compress.as_secs_f32(),
                    i,
                    compressed_step / 1024 / 1024,
                    (uncompressed_step - compressed_step) / 1024 / 1024
                );
            }

            println!("\n\n_______________ bincode ____________");

            let start_of_to_vec = Instant::now();
            let bytes = bincode::serialize(&step).unwrap();
            let end_of_to_vec = Instant::now();
            println!(
                "\nTime to convert to bytes: {:.2}s\n",
                (end_of_to_vec - start_of_to_vec).as_secs_f32()
            );

            let uncompressed_step = data_size(&bytes);
            println!("Original Size: {}MB\n", uncompressed_step / 1024 / 1024);

            for i in 1..=10 {
                let start = Instant::now();
                let compressed = compress_to_vec(&bytes, i);
                let time_to_compress = Instant::now() - start;
                let compressed_step = data_size(&compressed);
                println!(
                    "Compressed Step in {:.2}s at level {}\t:: Compressed Size: {}MB\t:: Reduction => {}MB",
                    time_to_compress.as_secs_f32(),
                    i,
                    compressed_step / 1024 / 1024,
                    (uncompressed_step - compressed_step) / 1024 / 1024
                );
            }
        }
    }

    #[tokio::test]
    async fn time_to_write_uncompressed_step() {
        println!();
        let mut sqlite_database = SqliteDatabase::new_in_memory(100).await.unwrap();
        sqlite_database.use_compression = false;

        let file_string =
            fs::read_to_string("/home/george/casper/casperlabs/step_events/mainnet_step_7006.json")
                .unwrap();
        let sse_data = serde_json::from_str::<SseData>(&file_string).unwrap();

        if let SseData::Step {
            era_id,
            execution_effect,
        } = sse_data
        {
            let step = Step {
                era_id,
                execution_effect,
            };
            let start_of_uncompressed_write = Instant::now();
            sqlite_database
                .save_step(step, 1, "127.0.0.1".to_string())
                .await
                .unwrap();
            let end_of_uncompressed_write = Instant::now();

            println!(
                "Time to write uncompressed step: {:.2}s",
                (end_of_uncompressed_write - start_of_uncompressed_write).as_secs_f32()
            );
        }
        println!()
    }

    #[tokio::test]
    async fn time_to_write_compressed_step() {
        let sqlite_database = SqliteDatabase::new_in_memory(100).await.unwrap();

        let file_string =
            fs::read_to_string("/home/george/casper/casperlabs/step_events/mainnet_step_7006.json")
                .unwrap();
        let sse_data = serde_json::from_str::<SseData>(&file_string).unwrap();

        if let SseData::Step {
            era_id,
            execution_effect,
        } = sse_data
        {
            let step = Step {
                era_id,
                execution_effect,
            };
            let start_of_compressed_write = Instant::now();
            sqlite_database
                .save_step(step, 1, "127.0.0.1".to_string())
                .await
                .unwrap();
            let end_of_compressed_write = Instant::now();

            println!(
                "Time to write compressed step: {:.2}s",
                (end_of_compressed_write - start_of_compressed_write).as_secs_f32()
            );
        }
    }
}
