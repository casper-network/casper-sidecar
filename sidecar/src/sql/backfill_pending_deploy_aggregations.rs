use crate::{
    sql::tables,
    types::{
        database::{DatabaseWriteError, MigrationScriptExecutor},
        sse_events::BlockAdded,
        transaction::{TransactionStatement, TransactionStatementResult, TransactionWrapper},
    },
};
use anyhow::Error;
use async_trait::async_trait;
use std::sync::Arc;

pub struct BackfillPendingDeployAggregations {}

impl BackfillPendingDeployAggregations {
    pub fn new() -> Arc<Self> {
        Arc::new(BackfillPendingDeployAggregations {})
    }

    async fn pending_deploy_aggregations_from_deploy_accepted(
        &self,
        transaction: Arc<dyn TransactionWrapper>,
    ) -> Result<TransactionStatementResult, DatabaseWriteError> {
        let count_deploy_accepted_materialized = {
            let select_statement = tables::deploy_accepted::create_count_deploy_accepted();
            transaction.materialize(TransactionStatement::SelectStatement(select_statement))
        };
        let res = transaction
            .execute(count_deploy_accepted_materialized)
            .await?;
        let are_there_any_deploy_accepted =
            self.determine_if_there_are_deploy_accepted(res).await?;
        if are_there_any_deploy_accepted {
            let materialized = {
                let insert_statement =
                    tables::pending_deploy_aggregations::create_insert_from_deploy_accepted()?;
                transaction.materialize(TransactionStatement::InsertStatement(insert_statement))
            };
            transaction.execute(materialized).await
        } else {
            Ok(TransactionStatementResult::U64(0))
        }
    }

    async fn pending_deploy_aggregations_from_block_added(
        &self,
        transaction: Arc<dyn TransactionWrapper>,
    ) -> Result<i32, DatabaseWriteError> {
        let limit = 10000;
        let mut offset = 0;
        let mut processed = 0;
        loop {
            let processed_in_batch = do_one_batch(transaction.clone(), offset, limit).await?;
            if processed_in_batch == 0 {
                break;
            }
            processed += processed_in_batch;
            offset += limit;
        }
        Ok(processed)
    }

    async fn determine_if_there_are_deploy_accepted(
        &self,
        res: TransactionStatementResult,
    ) -> Result<bool, DatabaseWriteError> {
        match res {
            TransactionStatementResult::RawData(data) => {
                if data.len() != 1 {
                    return Err(DatabaseWriteError::Unhandled(Error::msg(
                        "Expected exactly one result from counting DeployAggregate query",
                    )));
                }
                Ok(data.get(0).unwrap().as_str() != "0")
            }
            _ => Err(DatabaseWriteError::Unhandled(Error::msg(
                "Unexpected TransactionResult while executing BackfillAggregateDeployData",
            ))),
        }
    }
}

#[async_trait]
impl MigrationScriptExecutor for BackfillPendingDeployAggregations {
    async fn execute(
        &self,
        transaction: Arc<dyn TransactionWrapper>,
    ) -> Result<(), DatabaseWriteError> {
        self.pending_deploy_aggregations_from_deploy_accepted(transaction.clone())
            .await?;
        self.pending_deploy_aggregations_from_block_added(transaction.clone())
            .await?;
        Ok(())
    }
}

fn build_query_based_on_fetched_raw_block_added(
    data: Vec<String>,
) -> Result<Option<TransactionStatement>, Error> {
    let mut inserts_data = vec![];
    for raw_block_added in data {
        let block_added = serde_json::from_str::<BlockAdded>(&raw_block_added)?;
        let block_hash = block_added.hex_encoded_hash();
        let timestamp = block_added.get_timestamp().millis();
        let all_deploy_hashes = block_added.get_all_deploy_hashes();
        if !all_deploy_hashes.is_empty() {
            for deploy_hash in all_deploy_hashes {
                inserts_data.push((deploy_hash, Some((block_hash.clone(), timestamp))))
            }
        }
    }
    if inserts_data.is_empty() {
        Ok(None)
    } else {
        let insert_stmt =
            tables::pending_deploy_aggregations::create_multi_insert_stmt(inserts_data)?;
        let multi_insert_stmt = TransactionStatement::InsertStatement(insert_stmt);
        Ok(Some(multi_insert_stmt))
    }
}

async fn do_one_batch(
    transaction: Arc<dyn TransactionWrapper>,
    offset: u64,
    limit: u64,
) -> Result<i32, DatabaseWriteError> {
    let materialized = {
        let select_statement = tables::block_added::create_list_stmt(offset, limit);
        let select_statement = TransactionStatement::SelectStatement(select_statement);
        transaction.materialize(select_statement)
    };
    let results = transaction.execute(materialized).await?;
    match results {
        TransactionStatementResult::RawData(data) => {
            let data_size = data.len() as i32;
            if data_size == 0 {
                return Ok(0);
            }
            let maybe_materialized = {
                let maybe_statement = build_query_based_on_fetched_raw_block_added(data)?;
                maybe_statement.map(|statement| transaction.materialize(statement))
            };
            if let Some(materialized) = maybe_materialized {
                let _ = transaction.execute(materialized).await?;
            }
            Ok(data_size)
        }
        _ => Err(DatabaseWriteError::Unhandled(Error::msg(
            "Unexpected TransactionResult while executing BackfillAggregateDeployData",
        ))),
    }
}
