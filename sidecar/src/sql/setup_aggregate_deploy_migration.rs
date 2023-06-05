use std::{collections::HashSet, sync::Arc, time::Instant};

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
        let are_there_any_deploy_accepted =
            self.determine_if_there_are_deploy_accepted(res).await?;
        if are_there_any_deploy_accepted {
            let materialized = {
                let insert_statement =
                    tables::assemble_deploy_aggregate::create_insert_from_deploy_accepted()?;
                transaction.materialize(TransactionStatement::InsertStatement(insert_statement))
            };
            transaction.execute(materialized).await
        } else {
            return Ok(TransactionStatementResult::InsertStatement(0));
        }
    }

    async fn backfill_assemble_commands_from_block_added(
        &self,
        transaction: Arc<dyn TransactionWrapper>,
    ) -> Result<i32, DatabaseWriteError> {
        let limit = 10000;
        let mut offset = 0;
        let mut processed = 0;
        let mut deploy_hashes = HashSet::new();
        loop {
            println!("Fetching blocks offset {}", offset);
            let mut start = Instant::now();
            let materialized = {
                let select_statement = tables::block_added::create_list_stmt(offset, limit);
                let select_statement = TransactionStatement::SelectStatement(select_statement);
                transaction.materialize(select_statement)
            };
            let results = transaction.execute(materialized).await?;
            match results {
                TransactionStatementResult::SelectResult(data) => {
                    let millis = start.elapsed().as_millis() as f64 / 1000.0;
                    println!("Fetching blocks took {} [s]", millis);

                    start = Instant::now();
                    let data_size = data.len() as i32;
                    if data_size == 0 {
                        break;
                    }
                    let maybe_materialized = {
                        let mut inserts_data = vec![];
                        for raw_block_added in data {
                            let block_added = serde_json::from_str::<BlockAdded>(&raw_block_added)?;
                            let block_hash = block_added.hex_encoded_hash();
                            let timestamp = block_added.get_tmestamp().millis();
                            let all_deploy_hashes = block_added.get_all_deploy_hashes();
                            if !all_deploy_hashes.is_empty() {
                                for deploy_hash in all_deploy_hashes {
                                    deploy_hashes.insert(deploy_hash.clone());
                                    inserts_data.push((deploy_hash, Some((block_hash.clone(), timestamp))))
                                }
                            }
                        }
                        if inserts_data.is_empty() {
                            None
                        } else {
                            println!("Number of inserts {}", inserts_data.len());
                            let insert_stmt = tables::assemble_deploy_aggregate::create_multi_insert_stmt(inserts_data)?;                            
                            let multi_insert_stmt =
                                TransactionStatement::InsertStatement(insert_stmt);
                            Some(transaction.materialize(multi_insert_stmt))
                        }
                    };
                    if let Some(materialized) = maybe_materialized {
                        let millis = start.elapsed().as_millis() as f64 / 1000.0;
                        println!("Preparing inserts took {} [s]", millis);
                        start = Instant::now();
                        let _ = transaction.execute(materialized).await?;
                        let millis = start.elapsed().as_millis() as f64 / 1000.0;
                        println!("Executing inserts took {} [s]", millis);
                    }
                    processed += data_size;
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
        println!(
            "Found {} unique deploy hashes in blocks",
            deploy_hashes.len()
        );
        Ok(processed as i32)
    }

    async fn determine_if_there_are_deploy_accepted(
        &self,
        res: TransactionStatementResult,
    ) -> Result<bool, DatabaseWriteError> {
        match res {
            TransactionStatementResult::SelectResult(data) => {
                if data.len() != 1 {
                    return Err(DatabaseWriteError::Unhandled(Error::msg(
                        "Expected exactly one result from counting DeployAggregate query",
                    )));
                }
                return Ok(data.get(0).unwrap().to_owned() != "0".to_string());
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
        let start = Instant::now();
        println!("Starting to backfill assemble commands from deploy accepted");
        self.backfill_assemble_commands_from_deploy_accepted(transaction.clone())
            .await?;
        println!("Done backfilling assemble commands from deploy accepted");
        self.backfill_assemble_commands_from_block_added(transaction.clone())
            .await?;
        println!("Done backfilling assemble commands from blocks");
        self.update_pending_deploy_aggregates().await?;
        let took = start.elapsed();
        let millis = took.as_millis() as f64 / 1000.0;
        println!("AAAA IT took {} to do part 1", millis);
        Ok(())
    }
}
