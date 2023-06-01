use std::sync::Arc;

use anyhow::Error;
use async_trait::async_trait;

use crate::{
    sql::tables,
    types::{
        database::{DatabaseWriteError, MigrationScriptExecutor},
        sse_events::BlockAdded,
        transaction::{TransactionStatement, TransactionStatementResult, TransactionWrapper},
    },
};

pub struct BackfillAggregateDeployData {}

impl BackfillAggregateDeployData {
    pub fn new() -> Arc<Self> {
        Arc::new(BackfillAggregateDeployData {})
    }

    async fn update_pending_deploy_aggregates(&self) -> Result<(), DatabaseWriteError> {
        Ok(())
    }

    async fn backfill_assemble_commands_from_deploy_accepted(
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
        let are_there_any_deploy_accepted = self.determine_if_there_are_deploy_accepted(res).await?;
        if are_there_any_deploy_accepted {
            let materialized = {
                let insert_statement =
                    tables::assemble_deploy_aggregate::create_insert_from_deploy_accepted()?;
                transaction.materialize(TransactionStatement::InsertStatement(insert_statement))
            };
            transaction.execute(materialized).await
        } else {
            return Ok(TransactionStatementResult::InsertStatement(0))
        }
    }

    async fn backfill_assemble_commands_from_block_added(
        &self,
        transaction: Arc<dyn TransactionWrapper>,
    ) -> Result<i32, DatabaseWriteError> {
        let limit = 5000;
        let mut offset = 0;
        let processed = 0;
        loop {
            let materialized = {
                let select_statement = tables::block_added::create_list_stmt(offset, limit);
                let select_statement = TransactionStatement::SelectStatement(select_statement);
                transaction.materialize(select_statement)
            };
            let results = transaction.execute(materialized).await?;
            match results {
                TransactionStatementResult::SelectResult(data) => {
                    if data.is_empty() {
                        break;
                    }
                    let materialized = {
                        let mut insert_stmts = vec![];
                        for raw_block_added in data {
                            let block_added = serde_json::from_str::<BlockAdded>(&raw_block_added)?;
                            let block_hash = block_added.hex_encoded_hash();
                            let timestamp = block_added.get_tmestamp().millis();
                            for deploy_hash in block_added.get_all_deploy_hashes() {
                                let insert_stmt =
                                    tables::assemble_deploy_aggregate::create_insert_stmt(
                                        deploy_hash,
                                        Some((block_hash.clone(), timestamp)),
                                    )?;
                                insert_stmts.push(insert_stmt)
                            }
                        }
                        let multi_insert_stmt =
                            TransactionStatement::MultiInsertStatement(insert_stmts);
                        transaction.materialize(multi_insert_stmt)
                    };
                    let _ = transaction.execute(materialized).await?;
                    processed += data.len();
                }
                TransactionStatementResult::InsertStatement(_) => {
                    return Err(DatabaseWriteError::Unhandled(Error::msg(
                        "Unexpected InsertStatement while executing BackfillAggregateDeployData",
                    )))
                }
                TransactionStatementResult::Raw() => {
                    return Err(DatabaseWriteError::Unhandled(Error::msg(
                        "Unexpected Raw while executing BackfillAggregateDeployData",
                    )))
                }
            }
            offset += limit;
        }
        Ok(processed)
    }

    async fn determine_if_there_are_deploy_accepted(&self, res: TransactionStatementResult) -> Result<bool, DatabaseWriteError> {
        match res {
            TransactionStatementResult::SelectResult(data) => {
                if data.len() != 1 {
                    return Err(DatabaseWriteError::Unhandled(Error::msg(
                        "Expected exactly one result from counting DeployAggregate query",
                    )));
                }
                return Ok(data.get(0).unwrap().to_owned() != "0".to_string())
            }
            TransactionStatementResult::InsertStatement(_) => {
                return Err(DatabaseWriteError::Unhandled(Error::msg(
                    "Unexpected InsertStatement while executing BackfillAggregateDeployData",
                )))
            }
            TransactionStatementResult::Raw() => {
                return Err(DatabaseWriteError::Unhandled(Error::msg(
                    "Unexpected Raw while executing BackfillAggregateDeployData",
                )))
            }
        }
    }
}

#[async_trait]
impl MigrationScriptExecutor for BackfillAggregateDeployData {
    async fn execute(
        &self,
        transaction: Arc<dyn TransactionWrapper>,
    ) -> Result<(), DatabaseWriteError> {
        self.backfill_assemble_commands_from_deploy_accepted(transaction.clone())
            .await?;
        self.backfill_assemble_commands_from_block_added(transaction.clone())
            .await?;
        self.update_pending_deploy_aggregates().await?;
        Ok(())
    }
}
