use super::SqliteDatabase;
use crate::{
    sql::{
        tables,
        tables::{
            assemble_deploy_aggregate::AssembleDeployAggregateEntity, event_type::EventTypeId,
        },
    },
    types::{
        database::{DatabaseWriteError, DatabaseWriter, Migration, StatementWrapper},
        sse_events::*,
        transaction::{
            MaterializedTransactionStatementWrapper, TransactionStatement,
            TransactionStatementResult, TransactionWrapper,
        },
    },
};
use anyhow::{Context, Error};
use async_trait::async_trait;
use casper_types::AsymmetricType;
use itertools::Itertools;
use sea_query::SqliteQueryBuilder;
#[cfg(test)]
use sqlx::sqlite::SqliteRow;
use sqlx::{
    sqlite::SqliteQueryResult, Database, Executor, Row, Sqlite, Transaction, Value, ValueRef,
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

#[async_trait]
impl DatabaseWriter for SqliteDatabase {
    async fn save_block_added(
        &self,
        block_added: BlockAdded,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
        let mut transaction = self.get_transaction().await?;
        let json = serde_json::to_string(&block_added)?;
        let encoded_hash = block_added.hex_encoded_hash();

        let insert_to_event_log_stmt = tables::event_log::create_insert_stmt(
            EventTypeId::BlockAdded as u8,
            &event_source_address,
            event_id,
            &encoded_hash,
        )?
        .to_string(SqliteQueryBuilder);

        let event_log_id = transaction
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

        let block_hash = block_added.hex_encoded_hash();
        let timestamp = block_added.block.header.timestamp.millis();
        let res = handle_sqlite_result(transaction.execute(insert_stmt.as_str()).await);
        if res.is_ok() {
            /*for hash in block_added.get_all_deploy_hashes() {
                self.save_assemble_deploy_aggregate_command(
                    &mut transaction,
                    hash,
                    Some((block_hash.clone(), timestamp)),
                )
                .await?;
            }*/
            transaction.commit().await?;
        }
        res
    }

    async fn save_deploy_accepted(
        &self,
        deploy_accepted: DeployAccepted,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
        let mut transaction = self.get_transaction().await?;
        let json = serde_json::to_string(&deploy_accepted)?;
        let encoded_hash = deploy_accepted.hex_encoded_hash();

        let insert_to_event_log_stmt = tables::event_log::create_insert_stmt(
            EventTypeId::DeployAccepted as u8,
            &event_source_address,
            event_id,
            &encoded_hash,
        )?
        .to_string(SqliteQueryBuilder);

        let event_log_id = transaction
            .fetch_one(insert_to_event_log_stmt.as_str())
            .await?
            .try_get::<u32, usize>(0)
            .context("Error parsing event_log_id from row")?;

        let batched_insert_stmts = vec![
            tables::deploy_accepted::create_insert_stmt(
                encoded_hash.clone(),
                json.clone(),
                event_log_id,
            )?,
            tables::deploy_event::create_insert_stmt(event_log_id, encoded_hash.clone())?,
        ]
        .iter()
        .map(|stmt| stmt.to_string(SqliteQueryBuilder))
        .join(";");

        let res = handle_sqlite_result(transaction.execute(batched_insert_stmts.as_str()).await);
        if res.is_ok() {
            //self.save_assemble_deploy_aggregate_command(&mut transaction, encoded_hash, None)
            //   .await?;
            transaction.commit().await?;
        }
        res
    }

    async fn save_deploy_processed(
        &self,
        deploy_processed: DeployProcessed,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
        let mut transaction = self.get_transaction().await?;
        let json = serde_json::to_string(&deploy_processed)?;
        let encoded_hash = deploy_processed.hex_encoded_hash();

        let insert_to_event_log_stmt = tables::event_log::create_insert_stmt(
            EventTypeId::DeployProcessed as u8,
            &event_source_address,
            event_id,
            &encoded_hash,
        )?
        .to_string(SqliteQueryBuilder);

        let event_log_id = transaction
            .fetch_one(insert_to_event_log_stmt.as_str())
            .await?
            .try_get::<u32, usize>(0)
            .context("Error parsing event_log_id from row")?;

        let batched_insert_stmts = vec![
            tables::deploy_processed::create_insert_stmt(encoded_hash.clone(), json, event_log_id)?,
            tables::deploy_event::create_insert_stmt(event_log_id, encoded_hash.clone())?,
        ]
        .iter()
        .map(|stmt| stmt.to_string(SqliteQueryBuilder))
        .join(";");

        let res = handle_sqlite_result(transaction.execute(batched_insert_stmts.as_str()).await);
        if res.is_ok() {
            //self.save_assemble_deploy_aggregate_command(&mut transaction, encoded_hash, None)
            //    .await?;
            transaction.commit().await?;
        }
        res
    }

    async fn save_deploy_expired(
        &self,
        deploy_expired: DeployExpired,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
        let mut transaction = self.get_transaction().await?;
        let json = serde_json::to_string(&deploy_expired)?;
        let encoded_hash = deploy_expired.hex_encoded_hash();

        let insert_to_event_log_stmt = tables::event_log::create_insert_stmt(
            EventTypeId::DeployExpired as u8,
            &event_source_address,
            event_id,
            &encoded_hash,
        )?
        .to_string(SqliteQueryBuilder);

        let event_log_id = transaction
            .fetch_one(insert_to_event_log_stmt.as_str())
            .await?
            .try_get::<u32, usize>(0)
            .context("Error parsing event_log_id from row")?;

        let batched_insert_stmts = vec![
            tables::deploy_expired::create_insert_stmt(encoded_hash.clone(), event_log_id, json)?,
            tables::deploy_event::create_insert_stmt(event_log_id, encoded_hash.clone())?,
        ]
        .iter()
        .map(|stmt| stmt.to_string(SqliteQueryBuilder))
        .join(";");

        let res = handle_sqlite_result(transaction.execute(batched_insert_stmts.as_str()).await);
        if res.is_ok() {
            //self.save_assemble_deploy_aggregate_command(&mut transaction, encoded_hash, None)
            //    .await?;
            transaction.commit().await?;
        }
        res
    }

    async fn save_fault(
        &self,
        fault: Fault,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
        let mut transaction = self.get_transaction().await?;
        let json = serde_json::to_string(&fault)?;
        let era_id = fault.era_id.value();
        let public_key = fault.public_key.to_hex();

        let event_key = format!("{era_id} {public_key}");

        let insert_to_event_log_stmt = tables::event_log::create_insert_stmt(
            EventTypeId::Fault as u8,
            &event_source_address,
            event_id,
            &event_key,
        )?
        .to_string(SqliteQueryBuilder);

        let event_log_id = transaction
            .fetch_one(insert_to_event_log_stmt.as_str())
            .await?
            .try_get::<u32, usize>(0)
            .context("Error parsing event_log_id from row")?;

        let insert_stmt =
            tables::fault::create_insert_stmt(era_id, public_key, json, event_log_id)?
                .to_string(SqliteQueryBuilder);

        let res = handle_sqlite_result(transaction.execute(insert_stmt.as_str()).await);
        if res.is_ok() {
            transaction.commit().await?;
        }
        res
    }

    async fn save_finality_signature(
        &self,
        finality_signature: FinalitySignature,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
        let mut transaction = self.get_transaction().await?;
        let json = serde_json::to_string(&finality_signature)?;
        let block_hash = finality_signature.hex_encoded_block_hash();
        let public_key = finality_signature.hex_encoded_public_key();

        let event_key = format!("{block_hash} {public_key}");

        let insert_to_event_log_stmt = tables::event_log::create_insert_stmt(
            EventTypeId::FinalitySignature as u8,
            &event_source_address,
            event_id,
            &event_key,
        )?
        .to_string(SqliteQueryBuilder);

        let event_log_id = transaction
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

        let res = handle_sqlite_result(transaction.execute(insert_stmt.as_str()).await);
        if res.is_ok() {
            transaction.commit().await?;
        }
        res
    }

    async fn save_step(
        &self,
        step: Step,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
        let mut transaction = self.get_transaction().await?;
        let json = serde_json::to_string(&step)?;
        let era_id = step.era_id.value();

        let insert_to_event_log_stmt = tables::event_log::create_insert_stmt(
            EventTypeId::Step as u8,
            &event_source_address,
            event_id,
            &era_id.to_string(),
        )?
        .to_string(SqliteQueryBuilder);

        let event_log_id = transaction
            .fetch_one(insert_to_event_log_stmt.as_str())
            .await?
            .try_get::<u32, usize>(0)
            .context("Error parsing event_log_id from row")?;

        let insert_stmt = tables::step::create_insert_stmt(era_id, json, event_log_id)?
            .to_string(SqliteQueryBuilder);

        let res = handle_sqlite_result(transaction.execute(insert_stmt.as_str()).await);
        if res.is_ok() {
            transaction.commit().await?;
        }
        res
    }

    async fn save_shutdown(
        &self,
        event_id: u32,
        event_source_address: String,
    ) -> Result<usize, DatabaseWriteError> {
        let mut transaction = self.get_transaction().await?;
        let unix_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let event_key = format!("{}-{}", event_source_address, unix_timestamp);

        let insert_to_event_log_stmt = tables::event_log::create_insert_stmt(
            EventTypeId::Shutdown as u8,
            &event_source_address,
            event_id,
            &event_key,
        )?
        .to_string(SqliteQueryBuilder);

        let event_log_id = transaction
            .fetch_one(insert_to_event_log_stmt.as_str())
            .await?
            .try_get::<u32, usize>(0)
            .context("Error parsing event_log_id from row")?;

        let insert_stmt = tables::shutdown::create_insert_stmt(event_source_address, event_log_id)?
            .to_string(SqliteQueryBuilder);
        let res = handle_sqlite_result(transaction.execute(insert_stmt.as_str()).await);
        if res.is_ok() {
            transaction.commit().await?;
        }

        res
    }

    async fn execute_migration(&self, migration: Migration) -> Result<(), DatabaseWriteError> {
        let transaction = self.connection_pool.begin().await?;
        let transaction_shared = Arc::new(Mutex::new(transaction));
        let maybe_version = migration.get_version();
        let res = {
            let wrapper = SqliteTransactionWrapper {
                transaction_mutex: transaction_shared.clone(),
            };
            let wrapper_arc = Arc::new(wrapper);
            let sql = {
                let sqls = materialize_statements(migration.get_migrations()?);
                sqls.iter().join(";")
            };
            let stmt = MaterializedTransactionStatementWrapper::Raw(sql);
            match wrapper_arc.clone().execute(stmt).await {
                Ok(_) => {
                    if let Some(script_executor) = migration.script_executor {
                        script_executor
                            .execute(wrapper_arc.clone())
                            .await
                            .map(|_| ())
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

    async fn update_pending_deploy_aggregates(&self) -> Result<usize, DatabaseWriteError> {
        let mut transaction = self.get_transaction().await?;

        let fetch_assemble_deploy_aggregate_orders_sql =
            tables::assemble_deploy_aggregate::select_oldest_stmt(1000)
                .to_string(SqliteQueryBuilder);
        let (batch_update_sql, batch_delete_sql, number_of_ids) =
            sqlx::query_as::<_, AssembleDeployAggregateEntity>(
                &fetch_assemble_deploy_aggregate_orders_sql,
            )
            .fetch_all(&mut transaction)
            .await
            .map_err(|sql_err| DatabaseWriteError::Unhandled(Error::from(sql_err)))
            .map(|entities| {
                let number_of_ids = entities.len();
                let mut deduplicated_entities = HashMap::<String, Option<(String, u64)>>::new();
                let deduplicated_entities_ref = &mut deduplicated_entities;
                let ids = entities.iter().map(|el| el.get_id()).collect();
                entities.iter().for_each(|el| {
                    let deploy_hash = el.deploy_hash();
                    let block_data = el.get_block_data();
                    match deduplicated_entities_ref.get(&deploy_hash) {
                        None | Some(None) => {
                            deduplicated_entities_ref.insert(deploy_hash, block_data);
                        }
                        Some(_) => {}
                    }
                });
                let batch_update_sql = deduplicated_entities
                    .into_iter()
                    .map(|(key, value)| {
                        tables::deploy_aggregate::create_update_stmt(key, value)
                            .unwrap()
                            .to_string(SqliteQueryBuilder)
                    })
                    .join(";");
                let batch_delete_sql = tables::assemble_deploy_aggregate::delete_stmt(ids)
                    .to_string(SqliteQueryBuilder);
                (batch_update_sql, batch_delete_sql, number_of_ids)
            })?;
        if number_of_ids > 0 {
            transaction.execute(batch_update_sql.as_str()).await?;
            transaction.execute(batch_delete_sql.as_str()).await?;

            transaction.commit().await?;
        }
        Ok(number_of_ids)
    }
}

#[derive(Debug)]
struct SqliteTransactionWrapper<'a> {
    transaction_mutex: Arc<Mutex<Transaction<'a, Sqlite>>>,
}

#[async_trait]
impl TransactionWrapper for SqliteTransactionWrapper<'_> {
    fn materialize(
        &self,
        statement: TransactionStatement,
    ) -> MaterializedTransactionStatementWrapper {
        match statement {
            TransactionStatement::SelectStatement(x) => {
                MaterializedTransactionStatementWrapper::SelectStatement(
                    x.to_string(SqliteQueryBuilder),
                )
            }
            TransactionStatement::InsertStatement(x) => {
                MaterializedTransactionStatementWrapper::InsertStatement(
                    x.to_string(SqliteQueryBuilder),
                )
            }
            TransactionStatement::Raw(sql) => MaterializedTransactionStatementWrapper::Raw(sql),
            TransactionStatement::MultiInsertStatement(inserts) => {
                let sqls = inserts
                    .into_iter()
                    .map(|el| el.to_string(SqliteQueryBuilder))
                    .join(";");
                MaterializedTransactionStatementWrapper::InsertStatement(sqls)
            }
        }
    }

    async fn execute(
        &self,
        sql_wrapper: MaterializedTransactionStatementWrapper,
    ) -> Result<TransactionStatementResult, DatabaseWriteError> {
        let mut lock = self.transaction_mutex.lock().await;
        match sql_wrapper {
            MaterializedTransactionStatementWrapper::SelectStatement(sql) => lock
                .fetch_all(sql.as_str())
                .await
                .map_err(|err| anyhow::Error::from(err))
                .and_then(|rows| {
                    let results: Result<Vec<String>, anyhow::Error> = rows
                        .into_iter()
                        .map(|row| {
                            row.try_get_raw(0)
                                .map_err(|err| anyhow::Error::from(err))
                                .and_then(|value_ref| {
                                    if value_ref.is_null() {
                                        return Ok("".to_string());
                                    } else {
                                        let owned = value_ref.to_owned();
                                        match value_ref.type_info().to_string().as_str() {
                                            "INTEGER" => Ok(owned.decode::<i64>().to_string()),
                                            "TEXT" => Ok(owned.decode::<String>()),
                                            t => Err(anyhow::Error::msg(format!(
                                                "Don't know how to turn type {} to string",
                                                t
                                            ))),
                                        }
                                    }
                                })
                        })
                        .collect();
                    results.map(|r| TransactionStatementResult::SelectResult(r))
                })
                .map_err(DatabaseWriteError::from),
            MaterializedTransactionStatementWrapper::InsertStatement(sql) => lock
                .execute(sql.as_str())
                .await
                .map_err(|err| {
                    println!("sss {}", err);
                    err
                })
                .and_then(|el| {
                    Ok(TransactionStatementResult::InsertStatement(el.rows_affected()))
                })
                .map_err(DatabaseWriteError::from),
            MaterializedTransactionStatementWrapper::Raw(sql) => lock
                .execute(sql.as_str())
                .await
                .map_err(DatabaseWriteError::from)
                .map(|_| TransactionStatementResult::Raw()),
        }
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

fn materialize_statements(wrappers: Vec<StatementWrapper>) -> Vec<String> {
    wrappers
        .iter()
        .map(|wrapper| match wrapper {
            StatementWrapper::TableCreateStatement(statement) => {
                statement.to_string(SqliteQueryBuilder)
            }
            StatementWrapper::InsertStatement(statement) => statement.to_string(SqliteQueryBuilder),
            StatementWrapper::IndexCreateStatement(statement) => {
                statement.to_string(SqliteQueryBuilder)
            }
            StatementWrapper::SelectStatement(statement) => statement.to_string(SqliteQueryBuilder),
            StatementWrapper::Raw(sql) => sql.to_string(),
        })
        .collect()
}
