#[macro_export]
macro_rules! database_writer_implementation {
    ($extended_type:ty,
        $database_type:ty,
        $query_result_type:ty,
        $query_materializer_expr:expr,
        $database_specific_configuration: expr) => {
use anyhow::Context;
use async_trait::async_trait;
use casper_types::AsymmetricType;
#[cfg(feature = "additional-metrics")]
use casper_event_types::metrics;
use itertools::Itertools;
use tokio::sync::Mutex;
use $crate::{
    sql::{tables, tables::event_type::EventTypeId},
    types::{
        database::{
            DatabaseWriteError, DatabaseWriter, Migration, StatementWrapper, TransactionWrapper,
        },
        sse_events::*,
    },
};
#[cfg(feature = "additional-metrics")]
use std::time::Instant;
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use sqlx::{Executor, Row, Transaction};

#[derive(Debug)]
struct TransactionWrapperType<'a> {
    transaction_mutex: Arc<Mutex<Transaction<'a, $database_type>>>,
}

#[async_trait]
impl TransactionWrapper for TransactionWrapperType<'_> {
    async fn execute(&self, sql: &str) -> Result<(), DatabaseWriteError> {
        let mut lock = self.transaction_mutex.lock().await;
        lock.execute(sql)
            .await
            .map(|_| ())
            .map_err(DatabaseWriteError::from)
    }
}

#[async_trait]
impl DatabaseWriter for $extended_type {
    async fn save_block_added(
        &self,
        block_added: BlockAdded,
        event_id: u32,
        event_source_address: String,
    ) -> Result<u64, DatabaseWriteError> {
        #[cfg(feature = "additional-metrics")]
        let start = Instant::now();
        let mut transaction = self.get_transaction().await?;
        let json = serde_json::to_string(&block_added)?;
        let encoded_hash = block_added.hex_encoded_hash();
        let event_log_id = save_event_log(
                EventTypeId::BlockAdded as u8,
                &event_source_address,
                event_id,
                &encoded_hash,
                &mut transaction,
            )
            .await?;

        let insert_stmt = tables::block_added::create_insert_stmt(
            block_added.get_height(),
            encoded_hash,
            json,
            event_log_id,
        )?
        .to_string($query_materializer_expr);

        let res = handle_result(transaction.execute(insert_stmt.as_str()).await);
        if res.is_ok() {
            transaction.commit().await?;
        }
        #[cfg(feature = "additional-metrics")]
        observe_db_operation_time("save_block_added", start);
        res
    }

    async fn save_deploy_accepted(
        &self,
        deploy_accepted: DeployAccepted,
        event_id: u32,
        event_source_address: String,
    ) -> Result<u64, DatabaseWriteError> {
        #[cfg(feature = "additional-metrics")]
        let start = Instant::now();
        let mut transaction = self.get_transaction().await?;
        let json = serde_json::to_string(&deploy_accepted)?;
        let encoded_hash = deploy_accepted.hex_encoded_hash();

        let event_log_id = save_event_log(
                EventTypeId::DeployAccepted as u8,
                &event_source_address,
                event_id,
                &encoded_hash,
                &mut transaction,
            )
            .await?;

        let batched_insert_stmts = vec![
            tables::deploy_accepted::create_insert_stmt(encoded_hash.clone(), json, event_log_id)?,
            tables::deploy_event::create_insert_stmt(event_log_id, encoded_hash)?,
        ]
        .iter()
        .map(|stmt| stmt.to_string($query_materializer_expr))
        .join(";");

        let res = handle_result(transaction.execute(batched_insert_stmts.as_str()).await);
        if res.is_ok() {
            transaction.commit().await?;
        }
        #[cfg(feature = "additional-metrics")]
        observe_db_operation_time("save_deploy_accepted", start);
        res
    }

    async fn save_deploy_processed(
        &self,
        deploy_processed: DeployProcessed,
        event_id: u32,
        event_source_address: String,
    ) -> Result<u64, DatabaseWriteError> {
        #[cfg(feature = "additional-metrics")]
        let start = Instant::now();
        let mut transaction = self.get_transaction().await?;
        let json = serde_json::to_string(&deploy_processed)?;
        let encoded_hash = deploy_processed.hex_encoded_hash();
        let event_log_id = save_event_log(
                EventTypeId::DeployProcessed as u8,
                &event_source_address,
                event_id,
                &encoded_hash,
                &mut transaction,
            )
            .await?;

        let batched_insert_stmts = vec![
            tables::deploy_processed::create_insert_stmt(encoded_hash.clone(), json, event_log_id)?,
            tables::deploy_event::create_insert_stmt(event_log_id, encoded_hash)?,
        ]
        .iter()
        .map(|stmt| stmt.to_string($query_materializer_expr))
        .join(";");

        let res = handle_result(transaction.execute(batched_insert_stmts.as_str()).await);
        if res.is_ok() {
            transaction.commit().await?;
        }
        #[cfg(feature = "additional-metrics")]
        observe_db_operation_time("save_deploy_processed", start);
        res
    }

    async fn save_deploy_expired(
        &self,
        deploy_expired: DeployExpired,
        event_id: u32,
        event_source_address: String,
    ) -> Result<u64, DatabaseWriteError> {
        #[cfg(feature = "additional-metrics")]
        let start = Instant::now();
        let mut transaction = self.get_transaction().await?;
        let json = serde_json::to_string(&deploy_expired)?;
        let encoded_hash = deploy_expired.hex_encoded_hash();
        let event_log_id = save_event_log(
                EventTypeId::DeployExpired as u8,
                &event_source_address,
                event_id,
                &encoded_hash,
                &mut transaction,
            )
            .await?;

        let batched_insert_stmts = vec![
            tables::deploy_expired::create_insert_stmt(encoded_hash.clone(), event_log_id, json)?,
            tables::deploy_event::create_insert_stmt(event_log_id, encoded_hash)?,
        ]
        .iter()
        .map(|stmt| stmt.to_string($query_materializer_expr))
        .join(";");

        let res = handle_result(transaction.execute(batched_insert_stmts.as_str()).await);
        if res.is_ok() {
            transaction.commit().await?;
        }
        #[cfg(feature = "additional-metrics")]
        observe_db_operation_time("save_deploy_expired", start);
        res
    }

    async fn save_fault(
        &self,
        fault: Fault,
        event_id: u32,
        event_source_address: String,
    ) -> Result<u64, DatabaseWriteError> {
        #[cfg(feature = "additional-metrics")]
        let start = Instant::now();
        let mut transaction = self.get_transaction().await?;
        let json = serde_json::to_string(&fault)?;
        let era_id = fault.era_id.value();
        let public_key = fault.public_key.to_hex();
        let event_key = format!("{era_id} {public_key}");
        let event_log_id = save_event_log(
                EventTypeId::Fault as u8,
                &event_source_address,
                event_id,
                &event_key,
                &mut transaction,
            )
            .await?;

        let insert_stmt =
            tables::fault::create_insert_stmt(era_id, public_key, json, event_log_id)?
                .to_string($query_materializer_expr);
        let res = handle_result(transaction.execute(insert_stmt.as_str()).await);
        if res.is_ok() {
            transaction.commit().await?;
        }
        #[cfg(feature = "additional-metrics")]
        observe_db_operation_time("save_fault", start);
        res
    }

    async fn save_finality_signature(
        &self,
        finality_signature: FinalitySignature,
        event_id: u32,
        event_source_address: String,
    ) -> Result<u64, DatabaseWriteError> {
        #[cfg(feature = "additional-metrics")]
        let start = Instant::now();
        let mut transaction = self.get_transaction().await?;
        let json = serde_json::to_string(&finality_signature)?;
        let block_hash = finality_signature.hex_encoded_block_hash();
        let public_key = finality_signature.hex_encoded_public_key();
        let event_key = format!("{block_hash} {public_key}");

        let event_log_id = save_event_log(
                EventTypeId::FinalitySignature as u8,
                &event_source_address,
                event_id,
                &event_key,
                &mut transaction,
            )
            .await?;

        let insert_stmt = tables::finality_signature::create_insert_stmt(
            block_hash,
            public_key,
            json,
            event_log_id,
        )?
        .to_string($query_materializer_expr);

        let res = handle_result(transaction.execute(insert_stmt.as_str()).await);
        if res.is_ok() {
            transaction.commit().await?;
        }
        #[cfg(feature = "additional-metrics")]
        observe_db_operation_time("save_finality_signature", start);
        res
    }

    async fn save_step(
        &self,
        step: Step,
        event_id: u32,
        event_source_address: String,
    ) -> Result<u64, DatabaseWriteError> {
        #[cfg(feature = "additional-metrics")]
        let start = Instant::now();
        let mut transaction = self.get_transaction().await?;
        let json = serde_json::to_string(&step)?;
        let era_id = step.era_id.value();

        let event_log_id = save_event_log(
                EventTypeId::Step as u8,
                &event_source_address,
                event_id,
                &era_id.to_string(),
                &mut transaction,
            )
            .await?;

        let insert_stmt = tables::step::create_insert_stmt(era_id, json, event_log_id)?
            .to_string($query_materializer_expr);

        let res = handle_result(transaction.execute(insert_stmt.as_str()).await);
        if res.is_ok() {
            transaction.commit().await?;
        }
        #[cfg(feature = "additional-metrics")]
        observe_db_operation_time("save_step", start);
        res
    }

    async fn save_shutdown(
        &self,
        event_id: u32,
        event_source_address: String,
    ) -> Result<u64, DatabaseWriteError> {
        #[cfg(feature = "additional-metrics")]
        let start = Instant::now();
        let mut transaction = self.get_transaction().await?;
        let unix_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let event_key = format!("{}-{}", event_source_address, unix_timestamp);

        let event_log_id = save_event_log(
                EventTypeId::Shutdown as u8,
                &event_source_address,
                event_id,
                &event_key,
                &mut transaction,
            )
            .await?;

        let insert_stmt = tables::shutdown::create_insert_stmt(event_source_address, event_log_id)?
            .to_string($query_materializer_expr);
        let res = handle_result(transaction.execute(insert_stmt.as_str()).await);
        if res.is_ok() {
            transaction.commit().await?;
        }
        #[cfg(feature = "additional-metrics")]
        observe_db_operation_time("save_shutdown", start);
        res
    }

    async fn execute_migration(&self, migration: Migration) -> Result<(), DatabaseWriteError> {
        let transaction = self.connection_pool.begin().await?;
        let transaction_shared = Arc::new(Mutex::new(transaction));
        let maybe_version = migration.get_version();
        let res = {
            let wrapper = TransactionWrapperType {
                transaction_mutex: transaction_shared.clone(),
            };
            let wrapper_arc = Arc::new(wrapper);
            let sql = {
                let sqls = materialize_statements(migration.get_migrations($database_specific_configuration)?);
                sqls.iter().join(";")
            };
            match wrapper_arc.clone().execute(sql.as_str()).await {
                Ok(_) => {
                    if let Some(script_executor) = migration.script_executor {
                        script_executor.execute(wrapper_arc.clone()).await
                    } else {
                        Ok(())
                    }
                }
                Err(err) => Err(err).map_err(std::convert::From::from),
            }
        };
        let tx = Arc::try_unwrap(transaction_shared)
        .expect("Failed unwrapping transaction before commit. It seems some code is storing Arcs to the transaction?")
        .into_inner();
        if res.is_ok() {
            tx.commit().await?;
        } else {
            tx.rollback().await?
        }
        self.store_version_based_on_result(maybe_version, res).await
    }
}

fn materialize_statements(wrappers: Vec<StatementWrapper>) -> Vec<String> {
    wrappers
        .iter()
        .map(|wrapper| match wrapper {
            StatementWrapper::TableCreateStatement(statement) => {
                statement.to_string($query_materializer_expr)
            }
            StatementWrapper::InsertStatement(statement) => statement.to_string($query_materializer_expr),
            StatementWrapper::Raw(sql) => sql.to_string(),
        })
        .collect()
}

fn handle_result(
    result: Result<$query_result_type, sqlx::Error>,
) -> Result<u64, DatabaseWriteError> {
    result
        .map(|ok_query_result| ok_query_result.rows_affected())
        .map_err(std::convert::From::from)
}

async fn save_event_log(
    event_type_id: u8,
    event_source_address: &str,
    event_id: u32,
    event_key: &str,
    transaction: &mut Transaction<'_, $database_type>,
) -> Result<u64, DatabaseWriteError> {
    let insert_to_event_log_stmt = tables::event_log::create_insert_stmt(
        event_type_id,
        event_source_address,
        event_id,
        event_key,
    )?
    .to_string($query_materializer_expr);
    let event_log_id = transaction
        .fetch_one(insert_to_event_log_stmt.as_str())
        .await?
        .try_get::<i64, usize>(0)
        .context("save_block_added: Error parsing event_log_id from row")?
        as u64;
    Ok(event_log_id)
}

#[cfg(feature = "additional-metrics")]
fn observe_db_operation_time(operation_name: &str, start: Instant) {
    let duration = start.elapsed();
    metrics::DB_OPERATION_TIMES
        .with_label_values(&[operation_name])
        .observe(duration.as_nanos() as f64);
}

    }
}
